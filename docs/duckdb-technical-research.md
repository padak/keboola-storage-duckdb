# DuckDB Technical Research pro Keboola Storage

## 1. Jak funguje import souboru v Storage API

### Flow importu CSV do tabulky

```
1. Klient       POST /files/prepare
                ↓
2. Storage API  Vytvori zaznam v Elasticsearch
                Vrati presigned URL pro S3/GCS
                ↓
3. Klient       Upload CSV na presigned URL
                ↓
4. Klient       POST /tables/{id}/import-async
                { dataFileId: 12345, incremental: false, ... }
                ↓
5. Storage API  Vytvori Job v message queue
                ↓
6. Worker       Vezme job, nacte file metadata z Elasticsearch
                ↓
7. Driver       Dostane TableImportFromFileCommand s:
                - FilePath (root, path, fileName)
                - CSV options (delimiter, enclosure)
                - Import options (incremental, columns)
                ↓
8. Driver       Stahne CSV z cloud storage
                Importuje do cilove tabulky
                ↓
9. Response     TableImportResponse (row count, columns, size)
```

### Co driver dostane

```protobuf
message TableImportFromFileCommand {
    FilePath source = 1;           // gs://bucket/projects/123/file.csv
    ObjectInfo destination = 2;    // schema.table
    ImportOptions options = 3;     // incremental, columns, etc.
    CsvOptions csv = 4;           // delimiter, enclosure
}

message FilePath {
    string root = 1;      // "my-bucket"
    string path = 2;      // "projects/123/456/"
    string fileName = 3;  // "data.csv"
}
```

### Kompatibilita s DuckDB

**DuckDB nativne podporuje cteni z cloud storage:**

```sql
-- S3
COPY my_table FROM 's3://bucket/path/file.csv' (HEADER);

-- GCS
COPY my_table FROM 'gs://bucket/path/file.csv' (HEADER);

-- S credentials
SET s3_access_key_id = 'xxx';
SET s3_secret_access_key = 'yyy';
```

**Zaver:** Import flow je plne kompatibilni. DuckDB muze primo cist z S3/GCS.

---

## 2. Kompletni seznam funkci Storage Driveru

### Povinne operace (33)

| Kategorie | Operace | Popis |
|-----------|---------|-------|
| **Backend** | InitBackend | Validace spojeni a permissions |
| | RemoveBackend | Cleanup (muze byt no-op) |
| **Project** | CreateProject | Vytvorit projekt/databazi |
| | UpdateProject | Upravit nastaveni |
| | DropProject | Smazat projekt |
| **Bucket** | CreateBucket | Vytvorit bucket (= schema) |
| | DropBucket | Smazat bucket |
| | ShareBucket | Sdileni pristupu |
| | UnshareBucket | Zrusit sdileni |
| | LinkBucket | Propojit bucket |
| | UnlinkBucket | Odpojit bucket |
| | GrantBucketAccessToReadOnlyRole | Read-only pristup |
| | RevokeBucketAccessFromReadOnlyRole | Zrusit read-only |
| **Table CRUD** | CreateTable | Vytvorit tabulku |
| | DropTable | Smazat tabulku |
| | CreateTableFromTimeTravel | Snapshot (pokud podporovano) |
| **Table Schema** | AddColumn | Pridat sloupec |
| | DropColumn | Smazat sloupec |
| | AlterColumn | Upravit sloupec |
| | AddPrimaryKey | Pridat PK (dedup columns) |
| | DropPrimaryKey | Odebrat PK |
| | DeleteTableRows | Smazat radky s WHERE |
| **Import/Export** | ImportTableFromFile | Import z CSV/Parquet |
| | ImportTableFromTable | Kopie mezi tabulkami |
| | ExportTableToFile | Export do CSV/Parquet |
| **Info** | PreviewTable | Nahled dat (max 1000 rows) |
| | ProfileTable | Statistiky sloupcu |
| | ObjectInfo | Metadata objektu |
| | ExecuteQuery | Spustit SQL |
| **Workspace** | CreateWorkspace | Izolovaný workspace |
| | DropWorkspace | Smazat workspace |
| | ClearWorkspace | Vycistit workspace |
| | DropWorkspaceObject | Smazat objekt |
| | ResetWorkspacePassword | Rotace credentials |

### Volitelne operace (2)

| Operace | Popis |
|---------|-------|
| CreateDevBranch | Dev branches (empty handler OK) |
| DropDevBranch | Dev branches (empty handler OK) |

---

## 3. DuckDB Limity a Concurrency

### Velikost souboru

| Metrika | Limit |
|---------|-------|
| Max velikost DuckDB souboru | **Prakticky neomezeno** |
| Testovano v produkci | Až 10 TB |
| BLOB typ | Max 4 GB per objekt |
| S3 file limit | 5 TB (AWS omezeni) |

### Concurrency model

**DuckDB je single-writer, multi-reader:**

| Rezim | Popis |
|-------|-------|
| 1 proces R/W | Jeden proces ma exkluzivni pristup |
| N procesu READ_ONLY | Vice procesu muze cist soucasne |
| N procesu R/W | **NEPODPOROVANO** |

```python
# Multi-reader example
import duckdb

# Kazdy proces musi otevrit jako READ_ONLY
conn = duckdb.connect('data.duckdb', read_only=True)
```

### Dulezite implikace pro architekturu

1. **Pokud potrebujeme paralelni zapis** → potrebujeme jeden "writer" proces
2. **Pokud potrebujeme paralelni cteni** → vsichni ctenari musi byt READ_ONLY
3. **Nase API sluzba** bude single-writer = to je OK!

---

## 4. ATTACH - Cross-database JOINy

### DuckDB podporuje pripojeni vice databazi

```sql
-- Pripojit vice DuckDB souboru
ATTACH 'sales.duckdb' AS sales_db;
ATTACH 'hr.duckdb' AS hr_db (READ_ONLY);

-- Cross-database JOIN
SELECT
    o.order_id,
    e.employee_name
FROM sales_db.main.orders AS o
JOIN hr_db.main.employees AS e
    ON o.manager_id = e.id;
```

### Omezeni

| Operace | Podporovano |
|---------|-------------|
| Read z vice DB | Ano |
| JOIN mezi DB | Ano |
| Write do vice DB v jedne transakci | **NE** |

```sql
-- TOTO NEFUNGUJE:
BEGIN;
INSERT INTO db1.main.table1 VALUES (1);
INSERT INTO db2.main.table2 VALUES (2);  -- ERROR!
COMMIT;
```

---

## 5. Organizace DuckDB souboru - MOZNOSTI

### Varianta A: Jeden soubor = Cely backend

```
backend.duckdb
├── project_123/           (schema)
│   ├── bucket_abc/        (schema)
│   │   ├── table1
│   │   └── table2
│   └── bucket_def/        (schema)
│       └── table3
└── project_456/           (schema)
    └── bucket_xyz/
        └── table4
```

**Vyhody:**
- Jednoduche
- Joiny mezi buckety/projekty nativne
- Jeden soubor k zalohovani

**Nevyhody:**
- Single point of failure
- Vsichni tenanti sdili jeden soubor
- Velky soubor = delsi recovery

### Varianta B: Jeden soubor = Jeden projekt

```
project_123.duckdb
├── bucket_abc/            (schema)
│   ├── table1
│   └── table2
└── bucket_def/            (schema)
    └── table3

project_456.duckdb
└── bucket_xyz/            (schema)
    └── table4
```

**Vyhody:**
- Izolace mezi projekty
- Mensi soubory
- Paralelni operace mezi projekty

**Nevyhody:**
- Cross-project joiny vyzaduji ATTACH
- Vice souboru ke sprave

### Varianta C: Jeden soubor = Jeden bucket

```
project_123_bucket_abc.duckdb
├── table1
└── table2

project_123_bucket_def.duckdb
└── table3
```

**Vyhody:**
- Maximalni izolace
- Nejmensi soubory
- Nejrychlejsi recovery

**Nevyhody:**
- Kazdy JOIN mezi buckety = ATTACH
- Hodne souboru

---

## 6. DOPORUCENI: Varianta B (1 projekt = 1 DuckDB)

### Proc?

1. **Izolace**: Projekty jsou hlavni tenant boundary v Keboola
2. **Cross-bucket joiny**: Buckety v projektu jsou v jednom souboru = nativni JOIN
3. **Cross-project joiny**: Mozne pres ATTACH (read-only)
4. **Velikost**: Rozumna velikost souboru per projekt
5. **Recovery**: Pad jednoho projektu neovlivni ostatni

### Mapovani konceptu

| Keboola | DuckDB |
|---------|--------|
| Backend | Adresar se soubory |
| Project | 1 DuckDB soubor (`project_{id}.duckdb`) |
| Bucket | Schema v DuckDB |
| Table | Table v schema |
| Workspace | Schema s prefixem `WORKSPACE_` |

### Struktura souboru

```
/data/duckdb/
├── project_123.duckdb
│   ├── in_c_customers/           # bucket schema
│   │   ├── customers
│   │   └── orders
│   ├── out_c_reports/            # bucket schema
│   │   └── monthly_sales
│   └── WORKSPACE_456/            # workspace schema
│       └── temp_analysis
├── project_124.duckdb
└── project_125.duckdb
```

### Cross-project JOIN (kdyz je potreba)

```python
# V Python API service
conn = duckdb.connect('project_123.duckdb')
conn.execute("ATTACH 'project_124.duckdb' AS other (READ_ONLY)")

result = conn.execute("""
    SELECT a.*, b.category
    FROM main.in_c_customers.orders AS a
    JOIN other.in_c_products.categories AS b
        ON a.category_id = b.id
""").fetchall()

conn.execute("DETACH other")
```

---

## 7. File Import strategie

### DuckDB nativni import

```sql
-- CSV
COPY schema.table FROM 's3://bucket/file.csv' (
    HEADER true,
    DELIMITER ',',
    QUOTE '"'
);

-- Parquet (nejefektivnejsi!)
COPY schema.table FROM 's3://bucket/file.parquet';

-- Sliced files (wildcards)
COPY schema.table FROM 's3://bucket/path/*.csv' (HEADER);
```

### Doporuceny flow pro nasi API

```
1. Storage API posle:
   - S3/GCS URL souboru
   - Credentials (presigned nebo service account)
   - CSV options

2. DuckDB API Service:
   - Nastavi credentials: SET s3_access_key_id = '...'
   - Vytvori staging tabulku (optional)
   - COPY ... FROM 's3://...'
   - Transformace/deduplikace
   - COPY do finalni tabulky
   - Vrati statistiky
```

---

## 8. Aktualizovana architektura

```
┌─────────────────────────────┐
│     Keboola Storage API     │
│         (PHP)               │
│                             │
│  ┌───────────────────────┐  │
│  │ DuckdbDriverClient    │  │
│  │ (HTTP klient)         │  │
│  └───────────┬───────────┘  │
└──────────────┼──────────────┘
               │ REST/JSON
               ▼
┌─────────────────────────────┐
│   DuckDB API Service        │
│   (Python + FastAPI)        │
│                             │
│  ┌───────────────────────┐  │
│  │ Connection Manager    │  │ ◄── Single writer per project
│  │ (per-project pools)   │  │
│  └───────────┬───────────┘  │
│              │              │
│  ┌───────────▼───────────┐  │
│  │    DuckDB Files       │  │
│  │                       │  │
│  │  project_123.duckdb   │  │
│  │  project_124.duckdb   │  │
│  │  ...                  │  │
│  └───────────────────────┘  │
└─────────────────────────────┘
               │
               ▼ (COPY FROM)
┌─────────────────────────────┐
│   Cloud Storage (S3/GCS)    │
│   - Staged CSV/Parquet      │
└─────────────────────────────┘
```

---

## 9. Otevrene otazky

### Reseno

- [x] Jak funguje import flow? → S3 staging, driver cte primo
- [x] Jaky format souboru? → 1 projekt = 1 DuckDB file
- [x] Cross-bucket joiny? → Nativne (stejny soubor)
- [x] Cross-project joiny? → ATTACH (read-only)
- [x] Concurrency? → Single-writer API service

### K doreseni behem implementace

- [ ] Credential management pro S3/GCS pristup
- [ ] Backup/restore strategie pro DuckDB soubory
- [ ] Monitoring a metriky (velikost souboru, query times)
- [ ] Connection pooling per projekt
- [ ] Graceful handling pri padu sluzby
