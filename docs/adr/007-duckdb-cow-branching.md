# ADR-007: Copy-on-Write Branching pro Dev Branches

## Status

APPROVED (nahrazuje ADR-003)

## Zdroj

Chovani odvozeno z analyzy Keboola Storage produkce (Snowflake backend):
- Knowledge sharing session: Martin Zajic - branchovana storage (2024-11-28)
- Devin AI analyza kodu: `DevBranchCreate.php`, `DevBranchDelete.php`, `MergeConfigurationsService`
- Feature flag: `storage-branches` (real branched storage)

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

### Princip (Live View + CoW)

**DULEZITE**: Dev branch vidi LIVE data z main, ne zamrzly snapshot!

```
Branch READ:
  - Pokud tabulka NENI v branch -> cti AKTUALNI data z main (ATTACH READ_ONLY)
  - Pokud tabulka JE v branch -> cti z branch (izolovaná kopie)

Branch WRITE (Copy-on-Write):
  - Pokud tabulka NENI v branch -> zkopiruj AKTUALNI stav z main, pak zapis
  - Pokud tabulka JE v branch -> zapis primo do branch kopie

Branch DELETE (po merge):
  - Merge = POUZE konfigurace (ne tabulky!)
  - Vsechny tabulky v branch se SMAZOU
  - Data z branch se NEPREPISUJI do main
```

**Priklad:**
```
T0: create_branch("dev-123")
T1: main.customers ma 100 radku
T2: branch cte customers -> vidi 100 radku (live z main)
T3: main dostane +50 radku (celkem 150)
T4: branch cte customers -> vidi 150 radku (stale live!)
T5: branch ZAPISE do customers (CoW: zkopiruje 150 radku)
T6: main dostane +20 radku (celkem 170)
T7: branch cte customers -> vidi 150 radku (izolovaná kopie)
T8: merge_branch("dev-123") -> merge JEN konfigurace
T9: delete_branch("dev-123") -> smaze branch + vsechny branch tabulky
    (zmeny v customers z branche jsou ZTRACENY!)
```

### Architektura

```
┌─────────────────────────────────────────────────────────────┐
│                    BranchConnection                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   ┌─────────────────┐                                       │
│   │  BranchState    │  tracking:                            │
│   │  (metadata)     │  - copied_tables: set[(bucket,table)] │
│   │                 │  - deleted_tables: set[(bucket,table)]│
│   │                 │  - created_at: timestamp              │
│   └────────┬────────┘                                       │
│            │                                                │
│            ▼                                                │
│   ┌────────────────────────────────────────────────┐        │
│   │              Query Router                       │       │
│   │                                                 │       │
│   │  is_local(bucket, table)?                      │        │
│   │     YES ──► branch.duckdb                      │        │
│   │     NO  ──► main.duckdb (ATTACH READ_ONLY)    │         │
│   └────────────────────────────────────────────────┘        │
│                                                             │
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

### Merge branch (POUZE konfigurace!)

**DULEZITE**: V Keboola produkci merge NEMERGE tabulky! Mergují se pouze konfigurace komponent.

```python
def merge_branch(project_id: str, branch_id: str):
    """
    Merge branch = merge POUZE konfigurace (ne tabulky!)

    V DuckDB API implementaci:
    - Konfigurace jsou mimo scope (reseno v Connection)
    - Tato funkce pouze validuje stav pred smazanim
    """
    state = load_branch_metadata(project_id, branch_id)

    # Informovat uzivatele o tabulkach, ktere budou smazany
    if state.copied_tables:
        logger.warning(
            f"Branch {branch_id} obsahuje {len(state.copied_tables)} "
            f"modifikovanych tabulek. Tyto tabulky budou SMAZANY pri delete_branch!"
        )
        logger.warning(f"Tabulky: {state.copied_tables}")

    # V DuckDB API neprovadime merge tabulek - to neni nase zodpovednost
    # Konfigurace merguje Connection vrstva

    return {
        "status": "ready_for_delete",
        "tables_to_be_deleted": list(state.copied_tables),
        "warning": "Tabulky z branch budou smazany. Export data pred smazanim pokud je potrebujete."
    }
```

### Alternativa: Export tabulek pred smazanim (volitelne)

```python
def export_branch_tables(project_id: str, branch_id: str, output_dir: Path):
    """Exportuj modifikovane tabulky z branch pred smazanim"""
    state = load_branch_metadata(project_id, branch_id)
    branch_path = Path(f"project_{project_id}_branch_{branch_id}.duckdb")

    conn = duckdb.connect(str(branch_path), read_only=True)

    for bucket, table in state.copied_tables:
        output_file = output_dir / f"{bucket}_{table}.parquet"
        conn.execute(f"""
            COPY {bucket}.{table} TO '{output_file}' (FORMAT PARQUET)
        """)
        logger.info(f"Exported {bucket}.{table} to {output_file}")

    conn.close()
    return list(state.copied_tables)
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

## Vyresene otazky (na zaklade Keboola produkce)

### 1. Izolace vs Fresh Data - VYRESENO

**Rozhodnuti: B) Live view**

Branch vidi AKTUALNI data z main, dokud do tabulky nezapise. Toto je chovani Keboola produkce.

**Duvody:**
- Vyvojar pracuje s aktualnimi daty (ne zastaralymi)
- Jednodussi implementace (neni potreba time-travel)
- Konzistentni s ocekavanim uzivatelu

### 2. Konfliktni zmeny pri merge - VYRESENO

**Rozhodnuti: ZADNY MERGE TABULEK**

Keboola `MergeConfigurationsService` merguje POUZE konfigurace, NE tabulky!

Pri delete branch:
- Vsechny tabulky v branch se smazou
- Data z branch se NEKOPIRUJI zpet do main
- Pokud uzivatel chce data z branch, musi je MANUALNE exportovat pred smazanim

**Duvody:**
- Jednoducha a bezpecna logika
- Zadne riziko prepisu produkce
- Uzivatel ma plnou kontrolu

### 3. Velke tabulky - lazy copy - AKCEPTOVANO

**Rozhodnuti: Accept tradeoff**

Prvni write do velke tabulky = plna kopie. Pro dev branch development je to OK.

**Budouci optimalizace (post-MVP):**
- Partition-level CoW pro velmi velke tabulky
- Parquet intermediary pro read-heavy workloads

### 4. ATTACH limity - VYRESENO

**Reseni**: Jeden ATTACH na main project database, lazy attach/detach.

S ADR-009 (per-table files) je limit mene relevantni - attachujeme jednotlive tabulky.

### 5. Transakce across databases - VYRESENO

**Reseni**: Sekvencni operace s rollback logikou.

Pro CoW kopii:
1. Vytvor novou tabulku v branch
2. INSERT SELECT z main
3. Pokud fail -> DROP nove vytvorena tabulka

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
