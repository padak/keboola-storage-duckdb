# DuckDB Storage Backend pro Keboola - Implementacni plan v3

> **Cil:** On-premise Keboola bez Snowflake a bez S3

## Architektura

```
┌────────────────────────────────────────────────────────────────┐
│                    ON-PREMISE KEBOOLA                          │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│   ┌──────────────────┐         ┌────────────────────────────┐  │
│   │ Keboola Connection│◄───REST─►│   DuckDB API Service      │  │
│   │ (PHP)            │         │   (Python + FastAPI)       │  │
│   │                  │         │                            │  │
│   │ - Thin HTTP client│        │   - Write Queue (async)    │  │
│   │ - Credentials mgmt│        │   - Read Pool (parallel)   │  │
│   └──────────────────┘         │   - All handlers           │  │
│                                └─────────────┬──────────────┘  │
│                                              │                 │
│   ┌──────────────────────────────────────────┴────────────────┐│
│   │                     LOCAL FILESYSTEM                      ││
│   │                                                           ││
│   │   /data/duckdb/              /data/files/                 ││
│   │   ├── proj_1_main.duckdb     ├── project_1/               ││
│   │   ├── proj_1_branch_x.duckdb │   └── *.csv, *.json        ││
│   │   └── proj_2_main.duckdb     └── project_2/               ││
│   │                                                           ││
│   │   /data/snapshots/           /data/metadata/              ││
│   │   └── project_1/             └── files.duckdb             ││
│   │       └── *.parquet                                       ││
│   └───────────────────────────────────────────────────────────┘│
└────────────────────────────────────────────────────────────────┘
```

## Architektonicka rozhodnuti (ADR)

| ADR | Rozhodnuti |
|-----|------------|
| 001 | Python microservice misto PHP FFI driver |
| 002 | 1 projekt = 1 DuckDB soubor, bucket = schema |
| 003 | Dev branches = separate DuckDB files |
| 004 | Snapshoty = Parquet export |
| 005 | Write serialization = async fronta per projekt |
| 006 | Storage Files = lokalni filesystem + metadata v DuckDB |

---

## Struktura souboru

```
/data/
├── duckdb/                              # Storage Tables
│   ├── project_123_main.duckdb          # Default branch
│   ├── project_123_branch_456.duckdb    # Dev branch
│   └── project_124_main.duckdb
│
├── files/                               # Storage Files (nahrada S3)
│   ├── project_123/
│   │   ├── staging/                     # Upload staging
│   │   └── 2024/12/11/*.csv
│   └── project_124/
│
├── snapshots/                           # Table snapshots
│   └── project_123/
│       └── snap_001/
│           ├── metadata.json
│           └── *.parquet
│
└── metadata/
    └── files.duckdb                     # File registry
```

---

## Komponenty k implementaci

### 1. DuckDB API Service (Python)

```
duckdb-api-service/
├── pyproject.toml
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── src/
│   ├── main.py                    # FastAPI app
│   ├── config.py                  # ENV konfigurace
│   ├── database.py                # DuckDB connection manager
│   ├── write_queue.py             # Async write serialization
│   ├── routers/
│   │   ├── backend.py             # /backend/init, /backend/remove
│   │   ├── buckets.py             # /buckets CRUD + sharing
│   │   ├── tables.py              # /tables CRUD + import/export
│   │   ├── workspaces.py          # /workspaces CRUD
│   │   ├── branches.py            # /branches CRUD + merge
│   │   ├── snapshots.py           # /snapshots CRUD + restore
│   │   ├── files.py               # /files upload/download
│   │   └── query.py               # /query execute
│   ├── services/
│   │   ├── bucket_service.py
│   │   ├── table_service.py
│   │   ├── import_service.py
│   │   ├── export_service.py
│   │   ├── branch_service.py
│   │   ├── snapshot_service.py
│   │   ├── file_service.py
│   │   └── workspace_service.py
│   └── models/
│       ├── requests.py            # Pydantic models
│       └── responses.py
└── tests/
```

### 2. PHP Driver v Connection

```
connection/Package/StorageDriverDuckdb/
├── composer.json
├── services.yaml
├── src/
│   ├── DuckdbDriverClient.php      # Implements ClientInterface
│   ├── DuckdbApiClient.php         # HTTP client
│   └── DuckdbCredentialsHelper.php
└── tests/
```

---

## Implementacni faze

### Faze 1: Zaklad API
- [ ] FastAPI app s healthcheck
- [ ] DuckDB connection manager
- [ ] Docker + docker-compose
- [ ] Zakladni konfigurace

### Faze 2: Write Queue
- [ ] Async write serialization
- [ ] Per-project queues
- [ ] Priority support
- [ ] Timeout handling

### Faze 3: Bucket operace
- [ ] POST /buckets (CREATE SCHEMA)
- [ ] DELETE /buckets/{name}
- [ ] GET /buckets (list)
- [ ] Bucket sharing (ATTACH + Views)

### Faze 4: Table CRUD
- [ ] POST /tables (CREATE TABLE)
- [ ] DELETE /tables/{schema}/{table}
- [ ] GET /tables/{schema}/{table}/info
- [ ] PUT /tables/{schema}/{table}/columns

### Faze 5: Import/Export
- [ ] POST /tables/.../import/file
- [ ] POST /tables/.../import/table
- [ ] POST /tables/.../export
- [ ] Podpora CSV + Parquet

### Faze 6: Dev Branches
- [ ] POST /branches (create = copy file)
- [ ] DELETE /branches/{id}
- [ ] POST /branches/{id}/merge
- [ ] ATTACH pro cross-branch queries

### Faze 7: Snapshots
- [ ] POST /snapshots (Parquet export)
- [ ] GET /snapshots
- [ ] POST /snapshots/{id}/restore
- [ ] Auto-snapshot pred DROP

### Faze 8: Storage Files
- [ ] File metadata schema
- [ ] POST /files/upload
- [ ] GET /files/{id}
- [ ] Staging directory management

### Faze 9: Workspaces
- [ ] POST /workspaces
- [ ] DELETE /workspaces/{id}
- [ ] Workspace isolation (schema)

### Faze 10: PHP Integration
- [ ] DuckdbDriverClient
- [ ] DuckdbApiClient (HTTP)
- [ ] Integration do DriverClientFactory
- [ ] Testy

---

## Technologie

### Python API
```
duckdb>=1.0.0
fastapi>=0.109.0
uvicorn>=0.27.0
pydantic>=2.0.0
httpx>=0.26.0
python-dotenv>=1.0.0
```

### PHP Client
```json
{
    "require": {
        "guzzlehttp/guzzle": "^7.0",
        "keboola/storage-driver-common": "^7.8.0"
    }
}
```

---

## Keboola Features - Implementace

| Feature | DuckDB implementace |
|---------|---------------------|
| Storage Tables | DuckDB soubory (1 projekt = 1 file) |
| Buckets | DuckDB schemas |
| Dev Branches | Separate DuckDB files per branch |
| Bucket Sharing | ATTACH (READ_ONLY) + Views |
| Linked Buckets | Views na ATTACH-nuty zdroj |
| Snapshots | Parquet export + metadata JSON |
| Time Travel | Snapshot-based restore |
| Storage Files | Lokalni FS + metadata v DuckDB |
| Query Service | Write queue serialization |
| Workspaces | Izolovane schemas (WORKSPACE_*) |

---

## Limity a kapacita

| Metrika | Hodnota |
|---------|---------|
| Max tabulek per projekt | Neomezeno (testovano 5000+) |
| Max velikost projektu | 500GB+ (testovano 10TB) |
| Komprese dat | 75-95% (ZSTD) |
| Query latence | Milisekundy |
| Concurrent readers | Neomezeno |
| Concurrent writers | 1 per projekt (queue) |

---

## Vyhody oprem DuckDB vs Cloud

| Aspekt | Snowflake/BigQuery | DuckDB On-Prem |
|--------|-------------------|----------------|
| Naklady | $$$/mesic | $0 |
| Latence | Sekundy | Milisekundy |
| Offline | Ne | Ano |
| Data suverenita | Cloud | On-prem |
| Vendor lock-in | Vysoky | Zadny |
| Python integrace | Externi | Native (zero-copy) |

---

## Dokumentace

```
docs/
├── duckdb-driver-plan.md           # Tento soubor
├── duckdb-technical-research.md    # Technicke detaily DuckDB
├── duckdb-keboola-features.md      # Mapovani Keboola features
├── zajca.md                        # Puvodni pozadavky
└── adr/
    ├── 001-duckdb-microservice-architecture.md
    ├── 002-duckdb-file-organization.md
    ├── 003-duckdb-branch-strategy.md
    ├── 004-duckdb-snapshots.md
    ├── 005-duckdb-write-serialization.md
    └── 006-duckdb-on-prem-storage.md
```

---

## Dalsi kroky (zitra)

1. [ ] Vytvorit `duckdb-api-service/` strukturu
2. [ ] Implementovat FastAPI skeleton
3. [ ] Pridat DuckDB connection manager
4. [ ] Implementovat write queue
5. [ ] Prvni endpointy (health, backend/init)
