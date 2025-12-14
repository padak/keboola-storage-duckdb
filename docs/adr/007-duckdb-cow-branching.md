# ADR-007: Copy-on-Write Branching pro Dev Branches

## Status

Proposed (nahrazuje ADR-003)

## Datum

2024-12-15

## Kontext

ADR-003 definoval dev branches jako samostatne DuckDB soubory s plnym kopiovanim dat. Pro vetsi projekty (stovky GB) je tento pristup:

- **Pomaly**: Kopirovani 500GB trva minuty
- **Nakladny**: Kazda branch = plna kopie dat
- **Neefektivni**: Vetsina tabulek se v branch nemeni

### Inspirace

- **QuackFS** (github.com/vinimdocarmo/quackfs): FUSE-based differential storage pro DuckDB s time-travel. Zajimava myslenka, ale PoC zavislost na PostgreSQL + S3 + FUSE.
- **Git**: Lazy copy, uklada jen rozdily
- **ZFS/Btrfs**: Copy-on-Write snapshoty na urovni filesystemu

### Pozadavky

1. Rychle vytvoreni branch (sekundy, ne minuty)
2. Efektivni vyuziti uloziste (neplytva mistem)
3. Zadna zavislost na specialnim filesystemu
4. Kompatibilita s existujici architekturou

## Rozhodnuti

**Implementovat Copy-on-Write na aplikacni urovni v Python API vrstve.**

### Princip

```
Branch READ:
  - Pokud tabulka NENI v branch -> cti z main (ATTACH READ_ONLY)
  - Pokud tabulka JE v branch -> cti z branch

Branch WRITE:
  - Pokud tabulka NENI v branch -> zkopiruj ji z main, pak zapis
  - Pokud tabulka JE v branch -> zapis primo
```

### Architektura

```
┌─────────────────────────────────────────────────────────────┐
│                    BranchConnection                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   ┌─────────────────┐                                       │
│   │  BranchState    │  tracking:                            │
│   │  (metadata)     │  - copied_tables: set[(bucket,table)] │
│   │                 │  - deleted_tables: set[(bucket,table)]│
│   │                 │  - created_at: timestamp              │
│   └────────┬────────┘                                       │
│            │                                                 │
│            ▼                                                 │
│   ┌────────────────────────────────────────────────┐        │
│   │              Query Router                       │        │
│   │                                                 │        │
│   │  is_local(bucket, table)?                      │        │
│   │     YES ──► branch.duckdb                      │        │
│   │     NO  ──► main.duckdb (ATTACH READ_ONLY)    │        │
│   └────────────────────────────────────────────────┘        │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Struktura souboru

```
/data/duckdb/
├── project_123_main.duckdb              # hlavni databaze
├── project_123_branch_456.duckdb        # branch (jen zmenene tabulky)
├── project_123_branch_456.meta.json     # branch metadata
└── ...
```

### Metadata format

```json
{
  "branch_id": "456",
  "project_id": "123",
  "created_at": "2024-12-15T10:30:00Z",
  "created_by": "user@example.com",
  "base_snapshot": "2024-12-15T10:30:00Z",
  "copied_tables": [
    {"bucket": "in_c_sales", "table": "orders"},
    {"bucket": "in_c_sales", "table": "customers"}
  ],
  "deleted_tables": [
    {"bucket": "in_c_temp", "table": "old_data"}
  ],
  "description": "Feature branch for new reporting"
}
```

## Duvody

### Proc ne filesystem-level CoW (ZFS/Btrfs)?

1. **Zavislost na OS/storage**: Ne vsechny produkce maji ZFS
2. **Slozita orchestrace**: Snapshoty mimo nasi kontrolu
3. **Portabilita**: Chceme fungovat na AWS, GCP, on-prem

### Proc ne QuackFS?

1. **PoC stav**: Neni production-ready
2. **Zavislosti**: PostgreSQL + S3 + FUSE
3. **Komplexita**: Dalsi komponenta k provozovani

### Proc aplikacni CoW?

1. **Zadne zavislosti**: Cista Python/DuckDB implementace
2. **Portabilni**: Funguje vsude kde bezi DuckDB
3. **Kontrola**: Plna kontrola nad logikou
4. **Testovatelnost**: Snadno unit-testovatelne

## Implementace

### Vytvoreni branch

```python
def create_branch(project_id: str, branch_id: str) -> BranchState:
    """Vytvor branch - jen prazdny soubor + schema"""
    main_path = f"project_{project_id}_main.duckdb"
    branch_path = f"project_{project_id}_branch_{branch_id}.duckdb"

    # Pripoj main pro cteni schemat
    main_conn = duckdb.connect(main_path, read_only=True)
    branch_conn = duckdb.connect(branch_path)

    # Zkopiruj pouze SCHEMA definice (zadna data)
    schemas = main_conn.execute("""
        SELECT DISTINCT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('main', 'information_schema', 'pg_catalog')
    """).fetchall()

    for (schema_name,) in schemas:
        branch_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    main_conn.close()
    branch_conn.close()

    # Uloz metadata
    state = BranchState(
        project_id=project_id,
        branch_id=branch_id,
        created_at=datetime.now().isoformat(),
        base_snapshot=datetime.now().isoformat()
    )
    save_branch_metadata(state)

    return state
```

### Copy-on-Write logika

```python
class BranchConnection:
    """Connection wrapper s CoW logikou"""

    def __init__(self, main_path: Path, branch_path: Path, state: BranchState):
        self.main_path = main_path
        self.branch_path = branch_path
        self.state = state
        self.conn = duckdb.connect(str(branch_path))
        self._main_attached = False

    def _ensure_main_attached(self):
        """Lazy ATTACH main databaze"""
        if not self._main_attached:
            self.conn.execute(
                f"ATTACH '{self.main_path}' AS main_db (READ_ONLY)"
            )
            self._main_attached = True

    def _copy_table_on_write(self, bucket: str, table: str):
        """Zkopiruj tabulku z main pred prvnim zapisem"""
        if self.state.is_local(bucket, table):
            return  # Uz je lokalni

        self._ensure_main_attached()

        # Atomicka kopie schema + data
        self.conn.execute(f"""
            CREATE TABLE {bucket}.{table} AS
            SELECT * FROM main_db.{bucket}.{table}
        """)

        self.state.mark_copied(bucket, table)
        save_branch_metadata(self.state)

    def execute_read(self, bucket: str, table: str, sql: str):
        """Cti data z branch nebo main"""
        if self.state.is_local(bucket, table):
            return self.conn.execute(sql).fetchall()
        else:
            self._ensure_main_attached()
            # Prepis dotaz na main_db
            rewritten = rewrite_to_main_db(sql, bucket, table)
            return self.conn.execute(rewritten).fetchall()

    def execute_write(self, bucket: str, table: str, sql: str):
        """Zapis - CoW pokud potreba"""
        self._copy_table_on_write(bucket, table)
        self.conn.execute(sql)
```

### Merge branch

```python
def merge_branch(project_id: str, branch_id: str, strategy: str = "replace"):
    """Merge zmenene tabulky zpet do main"""
    state = load_branch_metadata(project_id, branch_id)

    main_conn = duckdb.connect(f"project_{project_id}_main.duckdb")
    branch_path = f"project_{project_id}_branch_{branch_id}.duckdb"

    main_conn.execute(f"ATTACH '{branch_path}' AS branch_db (READ_ONLY)")

    # Merge pouze ZMENENE tabulky
    for bucket, table in state.copied_tables:
        if strategy == "replace":
            main_conn.execute(f"""
                CREATE OR REPLACE TABLE {bucket}.{table} AS
                SELECT * FROM branch_db.{bucket}.{table}
            """)
        elif strategy == "upsert":
            # TODO: Implementovat upsert (vyzaduje PK)
            pass

    # Aplikuj DELETE operace
    for bucket, table in state.deleted_tables:
        main_conn.execute(f"DROP TABLE IF EXISTS {bucket}.{table}")

    main_conn.execute("DETACH branch_db")
    main_conn.close()
```

### Smazani branch

```python
def delete_branch(project_id: str, branch_id: str):
    """Smaz branch - jen soubory"""
    branch_path = Path(f"project_{project_id}_branch_{branch_id}.duckdb")
    meta_path = Path(f"project_{project_id}_branch_{branch_id}.meta.json")

    branch_path.unlink(missing_ok=True)
    meta_path.unlink(missing_ok=True)
```

## Otevrene otazky (TODO)

### 1. Izolace vs Fresh Data

**Problem**: Co kdyz se main zmeni po vytvoreni branch?

```
T0: create_branch (customers ma 100 radku)
T1: main dostane +50 radku do customers
T2: branch cte customers - vidi 100 nebo 150?
```

**Moznosti**:
- **A) Snapshot izolace**: Branch vidi stav z T0 (jako Git) - DOPORUCENO
- **B) Live view**: Branch vidi aktualni main

**Implementace snapshot izolace**: Pri vytvoreni branch ulozit LSN nebo timestamp, pouzit pro ATTACH WITH TIME TRAVEL (pokud DuckDB podporuje) nebo exportovat do Parquet.

### 2. Konfliktni zmeny pri merge

**Problem**: Main i branch zmenily stejnou tabulku.

**Moznosti**:
- **Replace**: Branch prepise main (jednoduche)
- **Upsert**: Merge na urovni radku (potrebuje PK)
- **Fail**: Odmitni merge, vyzaduj manualni reseni
- **Three-way merge**: Porovnej s base snapshot

### 3. Velke tabulky - lazy copy optimalizace

**Problem**: Prvni write do 100GB tabulky = 100GB kopie.

**Moznosti**:
- **Parquet intermediary**: Exportovat main tabulku do Parquet, branch cte z Parquet + vlastni delta
- **Partition-level CoW**: Kopirovat jen affected partitions
- **Accept tradeoff**: Pro branch development je jednorizova kopie OK

### 4. ATTACH limity

**Problem**: DuckDB ma limit na pocet ATTACHed databazi.

**Reseni**: Pouzivat jeden ATTACH na main, lazy attach/detach.

### 5. Transakce across databases

**Problem**: DuckDB nepodporuje cross-database transakce.

**Reseni**: Pro CoW logiku pouzivat sekvenční operace s manualni rollback logikou.

## Dusledky

### Pozitivni

- **Rychle vytvoreni branch**: Sekundy misto minut
- **Efektivni storage**: Ulozeny jen rozdily
- **Zadne zavislosti**: Funguje vsude
- **Inkrementalni merge**: Kopiruje jen zmenene tabulky

### Negativni

- **Komplexnejsi logika**: Query routing, metadata management
- **Prvni write penalty**: Kopie tabulky pri prvnim zapisu
- **Izolace vyzaduje extra praci**: Snapshot semantika neni automaticka

### Neutralni

- **Nahrazuje ADR-003**: Stary pristup zustava jako fallback pro male projekty

## Reference

- ADR-003: Dev branches jako samostatne DuckDB soubory (nahrazeno)
- ADR-004: Snapshoty jako Parquet soubory
- [QuackFS](https://github.com/vinimdocarmo/quackfs): Inspirace pro differential storage
- [DuckDB ATTACH](https://duckdb.org/docs/sql/statements/attach.html)
