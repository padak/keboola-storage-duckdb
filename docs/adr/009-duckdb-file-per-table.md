# ADR-009: 1 DuckDB soubor per tabulka

## Status

**Proposed** - ceka na rozhodnuti

## Datum

2024-12-16

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

## OTEVRENE OTAZKY

### 1. ATTACH limity

**Otazka:** Kolik databazi lze ATTACH soucasne?

**Co vime:**
- DuckDB dokumentace neuvadi pevny limit
- Zavisi na `max_memory` a dostupnych file descriptors
- Realne testovano: stovky ATTACH funguji

**K overeni:**
- [ ] Otestovat ATTACH 100, 500, 1000 databazi
- [ ] Zmerit memory overhead per ATTACH
- [ ] Zjistit chovani pri dosazeni limitu

**Navrh:** Test s realnym workloadem, nastavit rozumny limit (napr. 500).

---

### 2. Schema vs Main

**Otazka:** Kazda tabulka je v `main` schema - jak reprezentovat bucket?

**Moznosti:**

**A) Bucket = adresar na filesystemu**
```
in_c_sales/orders.duckdb     # Tabulka "orders" v bucketu "in_c_sales"
in_c_sales/customers.duckdb
```

**B) Bucket = schema uvnitr souboru**
```
orders.duckdb obsahuje: in_c_sales.orders (schema.table)
```

**C) Bucket v ATTACH aliasu**
```sql
ATTACH 'orders.duckdb' AS in_c_sales_orders;
SELECT * FROM in_c_sales_orders.main.data;
```

**Doporuceni:** Varianta A (bucket = adresar) - nejjednodussi, nejprehlednejsi.

---

### 3. Workspace session management

**Otazka:** Jak spravovat ATTACH pro workspace session?

**Problem:**
- User potrebuje pristup k N tabulkam
- ATTACH je session-specific
- Session muze bezet hodiny

**Moznosti:**

**A) Eager ATTACH** - pripojit vsechny tabulky na zacatku
```python
# Pri vytvoreni workspace - ATTACH vsech tabulek v projektu
for table in project.tables:
    conn.execute(f"ATTACH '{table.path}' AS {table.alias} (READ_ONLY)")
```
- Pro: Jednoduche
- Proti: Velke projekty = hodne ATTACH = memory

**B) Lazy ATTACH** - pripojit az pri prvnim dotazu
```python
# Parser detekuje potrebne tabulky a ATTACH on-demand
def execute_query(sql):
    needed_tables = parse_table_references(sql)
    for table in needed_tables:
        if not is_attached(table):
            attach(table)
    return conn.execute(sql)
```
- Pro: Efektivni memory
- Proti: Slozitejsi implementace, SQL parsing

**C) Explicit ATTACH** - user si ridi sam
```sql
-- User musi explicitne ATTACH pred pouzitim
ATTACH 'orders.duckdb' AS orders;
SELECT * FROM orders.main.orders;
```
- Pro: Nejjednodussi implementace
- Proti: Horsi UX

**Doporuceni:** Zacat s A (eager), optimalizovat na B pokud bude problem.

---

### 4. File descriptors / handles

**Otazka:** Kolik file descriptors potrebujeme?

**Odhad:**
- Kazdy ATTACH = 1+ file descriptor
- Projekt s 500 tabulkami = 500+ FD
- 10 soucasnych workspace sessions = 5000+ FD

**Defaultni limity:**
- Linux: 1024 (soft), 65536 (hard)
- macOS: 256 (soft), unlimited (hard)

**K overeni:**
- [ ] Zmerit skutecnou FD spotrebu per ATTACH
- [ ] Otestovat chovani pri vycerpani limitu
- [ ] Dokumentovat potrebne `ulimit` nastaveni

**Navrh:** Zvysit `ulimit -n` na 65536, monitorovat FD usage.

---

### 5. Pristupova prava (permissions)

**Otazka:** Jak resit pristup k jednotlivym tabulkam?

**Soucasny model:**
- Project API key = pristup ke vsemu v projektu
- Zadna granularita per bucket/tabulka

**S novou architekturou:**
- Filesystem permissions per soubor (700, 750, ...)
- Moznost granularniho pristupu

**Otazky:**
- Chceme per-table permissions?
- Staci nam project-level pristup?
- Jak to souvisi s bucket sharing?

**Navrh:** Pro MVP zustat u project-level, granularita jako future feature.

---

### 6. Bucket sharing s novou architekturou

**Otazka:** Jak funguje bucket sharing kdyz bucket = adresar?

**Soucasne (ADR):**
```sql
ATTACH 'source_project.duckdb' AS source (READ_ONLY);
CREATE VIEW target_bucket.orders AS SELECT * FROM source.source_bucket.orders;
```

**Nove:**
```sql
-- Target project workspace
ATTACH 'source_project/in_c_sales/orders.duckdb' AS shared_orders (READ_ONLY);
ATTACH 'source_project/in_c_sales/customers.duckdb' AS shared_customers (READ_ONLY);
-- ... pro kazdou tabulku v bucket
```

**Problem:** Bucket s 100 tabulkami = 100 ATTACH statements

**Moznosti:**

**A) Symlinks na filesystem urovni**
```
target_project/linked_sales/ -> source_project/in_c_sales/
```

**B) Metadata-based routing**
```python
# Pri ATTACH detekovat linked bucket a ATTACH ze source
if bucket.is_linked:
    path = bucket.source_path
```

**C) Akceptovat vice ATTACH**
- Automaticky generovat ATTACH pro vsechny tabulky v bucket

**Doporuceni:** Varianta B (metadata routing) - nejflexibilnejsi.

---

### 7. Atomic operace

**Otazka:** Jak zajistit atomicitu pri operacich nad tabulkou?

**Scenar:**
```
1. Vytvorit novou tabulku (novy soubor)
2. Zaregistrovat v metadata.duckdb
-- Co kdyz krok 2 selze?
```

**Moznosti:**

**A) Two-phase: staging -> move**
```python
# 1. Vytvorit do staging
create_table("_staging/orders_new.duckdb")

# 2. Zaregistrovat v metadata
metadata_db.register_table(...)

# 3. Presunout do finalniho umisteni
os.rename("_staging/orders_new.duckdb", "in_c_sales/orders.duckdb")
```

**B) Compensation pattern**
```python
try:
    create_table("in_c_sales/orders.duckdb")
    metadata_db.register_table(...)
except:
    os.unlink("in_c_sales/orders.duckdb")  # rollback
    raise
```

**Doporuceni:** Varianta A (staging) - bezpecnejsi, jasnejsi stav.

---

### 8. Table rename / move

**Otazka:** Jak prejmenovavat/presouvat tabulky?

**Soucasne:** `ALTER TABLE schema.old_name RENAME TO new_name`

**Nove:** Prejmenovat soubor + aktualizovat metadata

```python
def rename_table(project, bucket, old_name, new_name):
    old_path = f"{project}/{bucket}/{old_name}.duckdb"
    new_path = f"{project}/{bucket}/{new_name}.duckdb"

    # 1. Zkontrolovat ze neni ATTACH nikde
    if is_table_in_use(old_path):
        raise TableInUseError()

    # 2. Prejmenovat soubor
    os.rename(old_path, new_path)

    # 3. Aktualizovat metadata
    metadata_db.rename_table(...)
```

**Problem:** Co kdyz je tabulka ATTACH v nejake session?

**Navrh:**
- Zavest "table lock" v metadata pro DDL operace
- Nebo akceptovat eventual consistency (stare sessions uvidí chybu)

---

### 9. Migrace ze soucasneho designu

**Otazka:** Jak migrovat existujici data?

```python
def migrate_project_to_per_table(project_id: str):
    """Migrate from single-file to per-table architecture."""
    old_db = f"project_{project_id}.duckdb"
    new_dir = f"project_{project_id}/"

    conn = duckdb.connect(old_db, read_only=True)

    # Pro kazdy bucket (schema)
    for schema in get_schemas(conn):
        os.makedirs(f"{new_dir}/{schema}", exist_ok=True)

        # Pro kazdou tabulku
        for table in get_tables(conn, schema):
            # Export do noveho souboru
            new_conn = duckdb.connect(f"{new_dir}/{schema}/{table}.duckdb")
            new_conn.execute(f"""
                CREATE TABLE main.data AS
                SELECT * FROM read_parquet('{old_db}', schema='{schema}', table='{table}')
            """)
            new_conn.close()

    conn.close()
```

**Otazky:**
- [ ] Podporovat obe architektury soucasne?
- [ ] Migrace za behu nebo s downtime?
- [ ] Rollback strategie?

---

### 10. Performance benchmarky

**K otestovani:**

| Test | Metrika | Soucasny | Novy (ocekavany) |
|------|---------|----------|------------------|
| Import 1 tabulky | Cas | X s | X s (stejne) |
| Import 10 tabulek paralelne | Cas | 10X s | X s (10x rychlejsi) |
| ATTACH 100 tabulek | Memory | N/A | ? MB |
| ATTACH 500 tabulek | Memory | N/A | ? MB |
| Query s 10 JOINy | Cas | X ms | ? ms |
| Workspace session startup | Cas | X ms | ? ms |

---

## Dalsi kroky

1. **Rozhodnout** zda jit do teto zmeny pro MVP nebo post-MVP
2. **Otestovat** ATTACH limity a performance
3. **Prototyp** - implementovat zakladni verzi a zmerit
4. **Aktualizovat** ADR-002, ADR-007 pokud schvaleno

## Reference

- ADR-002: 1 projekt = 1 DuckDB soubor (nahrazeno timto ADR pokud schvaleno)
- ADR-007: Copy-on-Write branching (zjednoduseno s per-table soubory)
- ADR-008: Centralni metadata databaze (beze zmeny)
- [DuckDB ATTACH dokumentace](https://duckdb.org/docs/sql/statements/attach.html)
- [DuckDB Concurrency](https://duckdb.org/docs/connect/concurrency.html)
