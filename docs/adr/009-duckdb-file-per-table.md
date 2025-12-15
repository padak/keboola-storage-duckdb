# ADR-009: 1 DuckDB soubor per tabulka

## Status

**Accepted** - schvaleno 2024-12-16

Supersedes: ADR-002

## Datum

2024-12-16 (proposed), 2024-12-16 (accepted)

## Validace

Rozhodnuti bylo validovano pomoci:
- **Codex (GPT-5)** - architekturni analyza a doporuceni
- **DuckDB ATTACH test** - Codex otestoval 4,096 soucasnych ATTACH bez chyby
- **Industry patterns review** - Delta Lake, Iceberg, MotherDuck pouzivaji stejny pattern

## Kontext

Soucasny design (ADR-002) definuje:
- 1 projekt = 1 DuckDB soubor
- Bucket = DuckDB schema
- Tabulka = tabulka ve schema

### Problem se soucasnym designem

DuckDB ma **single-writer** omezeni - do jednoho souboru muze zapisovat pouze jedno spojeni. To znamena:

```
Import do orders    ──┐
Import do customers ──┼──► Write Queue ──► project.duckdb
Import do products  ──┘
                         (serializovane!)
```

Pri importu do vice tabulek soucasne jsou vsechny operace serializovane, i kdyz by mohly bezet paralelne.

### Keboola Storage specifika

Po analyze jsme zjistili:
1. **Transakce pres vice tabulek** - v praxi se nepouzivaji
2. **SQL transformace** - user cte z vice tabulek, pise do workspace
3. **Storage neni aplikacni DB** - je to ETL storage, ne OLTP

```
STORAGE (read-only)           WORKSPACE (read-write)
┌─────────────────┐           ┌─────────────────┐
│ orders          │──READ────►│                 │
│ customers       │──READ────►│  SQL transform  │
│ products        │──READ────►│                 │
└─────────────────┘           └─────────────────┘
```

## Rozhodnuti

**Zmenit architekturu na 1 DuckDB soubor per tabulka.**

### Nova struktura

```
/data/duckdb/
├── metadata.duckdb                    # Centralni metadata (beze zmeny)
│
├── project_123/
│   ├── in_c_sales/                    # Bucket = adresar
│   │   ├── orders.duckdb              # Tabulka = soubor
│   │   ├── customers.duckdb
│   │   └── products.duckdb
│   │
│   ├── out_c_reports/
│   │   └── summary.duckdb
│   │
│   └── _workspaces/
│       └── ws_456.duckdb              # Workspace = soubor (read-write)
│
├── project_123_branch_789/            # Dev branch = kopie pouze zmenenych
│   └── in_c_sales/
│       └── orders.duckdb              # Jen tabulky zmenene v branch
│
└── project_124/
    └── ...
```

### Jak funguje cteni z vice tabulek

```python
# Workspace session - user SQL transformace
conn = duckdb.connect("workspace_456.duckdb")

# ATTACH vsech potrebnych tabulek (read-only)
conn.execute("ATTACH 'in_c_sales/orders.duckdb' AS orders (READ_ONLY)")
conn.execute("ATTACH 'in_c_sales/customers.duckdb' AS customers (READ_ONLY)")
conn.execute("ATTACH 'in_c_sales/products.duckdb' AS products (READ_ONLY)")

# User query - JOIN pres ATTACH
result = conn.execute("""
    SELECT o.*, c.name as customer_name
    FROM orders.main.orders o
    JOIN customers.main.customers c ON o.customer_id = c.id
""")

# Write do workspace (lokalni tabulka)
conn.execute("""
    CREATE TABLE output AS
    SELECT ...
""")
```

### Jak funguje paralelni import

```python
# Paralelni import - zadna fronta!
async def import_tables(tables: list[ImportJob]):
    tasks = []
    for job in tables:
        # Kazda tabulka ma vlastni soubor = vlastni writer
        task = asyncio.create_task(import_single_table(job))
        tasks.append(task)

    # Vsechny importy bezi soucasne
    await asyncio.gather(*tasks)

async def import_single_table(job: ImportJob):
    # Primo otevrit soubor tabulky - zadna fronta
    conn = duckdb.connect(f"{job.bucket}/{job.table}.duckdb")
    conn.execute(f"COPY main.data FROM '{job.file}' ...")
    conn.close()
```

## Duvody

### Proc zmena?

1. **Paralelni import** - hlavni duvod, masivni zrychleni
2. **Prirozeny CoW pro dev branches** - kopiruj jen zmenene soubory
3. **Jednodussi snapshoty** - snapshot tabulky = kopie souboru
4. **Granularni locking** - zamek per tabulka, ne per projekt
5. **Lepsi skalovatelnost** - vic tabulek = vic paralelismu

### Proc to funguje pro Keboola?

1. Transakce pres tabulky nejsou potreba
2. JOINy jsou jen pro cteni (workspaces)
3. ETL pattern: read many, write one

## Dusledky

### Pozitivni

- Import N tabulek = N paralelne (vs serializovane)
- Dev branches: kopie jen zmenenych tabulek (skutecny CoW)
- Snapshoty: `cp table.duckdb table_snapshot.duckdb`
- Backup/restore: granularni per tabulka
- Zadna write queue per projekt

### Negativni

- Vice souboru (projekt s 500 tabulkami = 500 souboru)
- JOINy vyzaduji ATTACH (slozitejsi query routing)
- File handle management
- Slozitejsi implementace workspace sessions

### Neutralni

- Metadata.duckdb zustava beze zmeny
- API zustava stejne (implementace se meni)

---

## VYRESENE OTAZKY (validace Codex GPT-5)

### 1. ATTACH limity - VYRESENO

**Otazka:** Kolik databazi lze ATTACH soucasne?

**Odpoved (Codex test):**
- DuckDB 1.4.2 **nema pevny ATTACH limit**
- Codex otestoval **4,096 soucasnych ATTACH** bez chyby
- Limit je efektivne `available_memory` + OS resources
- Kazdy ATTACH = ~2-3 file descriptors + par MB metadata overhead

**Overeno:**
- [x] Otestovat ATTACH 100, 500, 1000 databazi - **4096 OK**
- [x] Zmerit memory overhead per ATTACH - **par MB per ATTACH**
- [x] Zjistit chovani pri dosazeni limitu - **OS file descriptor limit**

**Doporuceni:** Zvysit `ulimit -n` na 65536, monitorovat FD usage.

---

### 2. Schema vs Main - VYRESENO

**Otazka:** Kazda tabulka je v `main` schema - jak reprezentovat bucket?

**Rozhodnuti: Varianta A (bucket = adresar)**

```
project_123/
├── in_c_sales/              # Bucket = adresar
│   ├── orders.duckdb        # Tabulka = soubor, data v main.data
│   └── customers.duckdb
└── out_c_reports/
    └── summary.duckdb
```

**Duvody:**
- Nejjednodussi, nejprehlednejsi
- Prirozena organizace na filesystemu
- Snadna navigace a debugging

---

### 3. Workspace session management - VYRESENO

**Otazka:** Jak spravovat ATTACH pro workspace session?

**Rozhodnuti: Varianta A (Eager ATTACH) s cachovani**

```python
class WorkspaceSession:
    def __init__(self, project_id: str):
        self.conn = duckdb.connect(f"workspace_{uuid4()}.duckdb")
        self.attached_tables: set[str] = set()

    def ensure_attached(self, tables: list[TableRef]):
        """Attach tables needed for query."""
        for table in tables:
            if table.full_path not in self.attached_tables:
                self.conn.execute(
                    f"ATTACH '{table.file_path}' AS {table.alias} (READ_ONLY)"
                )
                self.attached_tables.add(table.full_path)
```

**Codex doporuceni:**
- Cache ATTACH statements per session (ne DETACH po kazdem query)
- ATTACH pouze tabulky potrebne pro aktualni query
- Memory overhead je minimalni diky sdileni connection-level memory limitu

---

### 4. File descriptors / handles - VYRESENO

**Otazka:** Kolik file descriptors potrebujeme?

**Codex analyza:**
- Kazdy ATTACH = **2-3 file descriptors** (base + WAL + checkpoint)
- Projekt s 500 tabulkami = ~1500 FD
- 10 soucasnych workspace sessions = ~15000 FD (worst case)

**Reseni:**
```bash
# V Docker/systemd konfiguraci
ulimit -n 65536

# Monitoring
duckdb_file_descriptors{project_id="X"} gauge
```

**Overeno:**
- [x] Zmerit skutecnou FD spotrebu per ATTACH - **2-3 FD**
- [x] Otestovat chovani pri vycerpani limitu - **graceful error**
- [x] Dokumentovat potrebne `ulimit` nastaveni - **65536 recommended**

---

### 5. Pristupova prava (permissions) - VYRESENO

**Otazka:** Jak resit pristup k jednotlivym tabulkam?

**Rozhodnuti: Project-level access pro MVP**

- Filesystem permissions: `700` na project adresare
- API-level: Hierarchicky API key model (viz ADR plan)
- Per-table permissions: **future feature** (post-MVP)

**Bucket sharing:**
- App-layer enforcement
- ATTACH z jineho projektu s READ_ONLY flag

---

### 6. Bucket sharing s novou architekturou - VYRESENO

**Otazka:** Jak funguje bucket sharing kdyz bucket = adresar?

**Rozhodnuti: Varianta B (metadata-based routing)**

```python
def get_table_path(project_id: str, bucket: str, table: str) -> Path:
    """Resolve table path, handling linked buckets."""
    bucket_info = metadata_db.get_bucket(project_id, bucket)

    if bucket_info.is_linked:
        # Linked bucket - resolve to source
        return Path(
            f"{bucket_info.source_project}/{bucket_info.source_bucket}/{table}.duckdb"
        )
    else:
        return Path(f"{project_id}/{bucket}/{table}.duckdb")
```

**Pro workspace session:**
```python
# Automaticky ATTACH vsech tabulek z linked bucketu
for table in get_bucket_tables(source_project, source_bucket):
    workspace.attach(table, read_only=True)
```

---

### 7. Atomic operace - VYRESENO

**Otazka:** Jak zajistit atomicitu pri operacich nad tabulkou?

**Rozhodnuti: Varianta A (two-phase: staging -> move)**

```python
async def create_table(project_id: str, bucket: str, table: str, columns: list):
    staging_path = f"_staging/{uuid4()}.duckdb"
    final_path = f"{project_id}/{bucket}/{table}.duckdb"

    try:
        # 1. Create in staging
        conn = duckdb.connect(staging_path)
        conn.execute(f"CREATE TABLE main.data ({columns_sql})")
        conn.close()

        # 2. Register in metadata
        metadata_db.register_table(project_id, bucket, table)

        # 3. Atomic move to final location
        os.rename(staging_path, final_path)

    except Exception:
        # Cleanup staging on any failure
        if os.path.exists(staging_path):
            os.unlink(staging_path)
        raise
```

**Duvody:**
- `os.rename()` je atomicka operace na POSIX
- Staging soubor nikdy neni videt jako "valid" tabulka
- Jasny stav pri padu

---

### 8. Table rename / move - VYRESENO

**Otazka:** Jak prejmenovavat/presouvat tabulky?

**Rozhodnuti: Eventual consistency s graceful error handling**

```python
async def rename_table(project: str, bucket: str, old_name: str, new_name: str):
    old_path = f"{project}/{bucket}/{old_name}.duckdb"
    new_path = f"{project}/{bucket}/{new_name}.duckdb"

    # 1. Update metadata first (source of truth)
    metadata_db.rename_table(project, bucket, old_name, new_name)

    # 2. Rename file
    os.rename(old_path, new_path)

    # Note: Active sessions with old ATTACH will get error on next query
    # This is acceptable - sessions are short-lived for ETL workloads
```

**Chovani pri aktivni session:**
- Stare sessions s ATTACH dostanou "file not found" error
- Workspace session se refreshne a znovu ATTACH
- Pro MVP akceptovatelne (ETL sessions jsou kratke)

---

### 9. Migrace ze soucasneho designu - PLAN

**Otazka:** Jak migrovat existujici data?

**Rozhodnuti: Neni potreba migrace - zacname znovu**

Soucasny stav:
- Implementovano: Project/Bucket/Table CRUD + Preview
- Data: pouze testovaci (pytest)
- Produkce: zatim neni

**Pristup:**
1. Refaktorovat kod PRED dalsim vyvojem
2. Testy upravi na novy format
3. Zadna migrace existujicich dat (nejsou)

**Codex doporuceni:**
> "Migrating from Option A to B later is non-trivial... Doing it upfront is easier than retrofitting after a large fleet exists."

**Timeline:**
- Refaktor ted (pred Write Queue, Import/Export)
- Zadny tech debt do budoucna

---

### 10. Performance benchmarky - TODO

**K otestovani po refaktoru:**

| Test | Metrika | Ocekavany vysledek |
|------|---------|-------------------|
| Import 1 tabulky | Cas | Stejne jako dnes |
| Import 10 tabulek paralelne | Cas | **~10x rychlejsi** |
| ATTACH 100 tabulek | Memory | ~200-300 MB |
| ATTACH 500 tabulek | Memory | ~1-1.5 GB |
| Query s 10 JOINy (ATTACH) | Cas | ~stejne jako nativni |
| Workspace session startup | Cas | +few ms per ATTACH |

**Codex poznamka k performance:**
> "Cross-database JOINs are bound into one logical plan... scan/filter/join performance is nearly identical to two tables inside one database."

---

## Dalsi kroky - DONE

1. [x] **Rozhodnout** - ACCEPTED pro MVP
2. [x] **Otestovat ATTACH limity** - Codex: 4096 OK
3. [ ] **Refaktorovat** implementaci (viz plan v duckdb-driver-plan.md)
4. [x] **Aktualizovat ADR-002** - marked as Superseded

---

## Industry Patterns (Codex)

> "Modern analytical storage layers (Lakehouse systems like Delta Lake/Iceberg/Hudi, BigQuery, Athena/Glue catalogs) store data per table and rely on centralized metadata to stitch them together. DuckDB Cloud and MotherDuck similarly use multiple files/object chunks per table, attaching them into sessions on demand. Table-granular files with catalog-driven attachment is therefore an established pattern for ETL/analytics workloads."

---

## Reference

- ADR-002: 1 projekt = 1 DuckDB soubor (**SUPERSEDED** by this ADR)
- ADR-007: Copy-on-Write branching (zjednoduseno s per-table soubory)
- ADR-008: Centralni metadata databaze (beze zmeny)
- [DuckDB ATTACH dokumentace](https://duckdb.org/docs/sql/statements/attach.html)
- [DuckDB Concurrency](https://duckdb.org/docs/connect/concurrency.html)
