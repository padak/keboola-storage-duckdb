# DuckDB Storage Backend pro Keboola - Implementacni plan v3

> **Cil:** On-premise Keboola bez Snowflake a bez S3

## Architektura

```
┌──────────────────────────────────────────────────────────────────────┐
│                         ON-PREMISE KEBOOLA                           │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌────────────────────────┐           ┌────────────────────────────┐ │
│  │  Keboola Connection    │           │   DuckDB API Service       │ │
│  │  (PHP)                 │◄──REST───►│   (Python + FastAPI)       │ │
│  │                        │           │                            │ │
│  │  - Thin HTTP client    │           │   - Write Queue (async)    │ │
│  │  - Credentials mgmt    │           │   - Read Pool (parallel)   │ │
│  └────────────────────────┘           │   - All handlers           │ │
│                                       └─────────────┬──────────────┘ │
│                                                     │                │
│  ┌──────────────────────────────────────────────────┴───────────────┐│
│  │                       LOCAL FILESYSTEM                           ││
│  │                                                                  ││
│  │  /data/duckdb/                        /data/files/               ││
│  │  ├── project_123_main.duckdb          ├── project_123/           ││
│  │  ├── project_123_branch_456.duckdb    │   └── *.csv, *.json      ││
│  │  └── project_124_main.duckdb          └── project_124/           ││
│  │                                                                  ││
│  │  /data/snapshots/                     /data/metadata/            ││
│  │  └── project_123/                     └── files.duckdb           ││
│  │      └── *.parquet                                               ││
│  └──────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────┘
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
├── duckdb/                                    # Storage Tables
│   ├── project_123_main.duckdb                # Default branch
│   ├── project_123_branch_456.duckdb          # Dev branch
│   └── project_124_main.duckdb
│
├── files/                                     # Storage Files (nahrada S3)
│   ├── project_123/
│   │   ├── staging/                           # Upload staging
│   │   └── 2024/12/11/*.csv
│   └── project_124/
│
├── snapshots/                                 # Table snapshots
│   └── project_123/
│       └── snap_001/
│           ├── metadata.json
│           └── *.parquet
│
└── metadata/
    └── files.duckdb                           # File registry
```

---

## Driver Commands - Kompletni seznam (35)

### Backend (2)
| Command | Popis | Faze |
|---------|-------|------|
| InitBackend | Validace spojeni a permissions | 1 |
| RemoveBackend | Cleanup | 1 |

### Project (3)
| Command | Popis | Faze |
|---------|-------|------|
| CreateProject | Vytvorit DuckDB soubor pro projekt | 2 |
| UpdateProject | Upravit nastaveni projektu | 2 |
| DropProject | Smazat projekt (soubor) | 2 |

### Bucket (8)
| Command | Popis | Faze |
|---------|-------|------|
| CreateBucket | CREATE SCHEMA | 3 |
| DropBucket | DROP SCHEMA CASCADE | 3 |
| ShareBucket | ATTACH (READ_ONLY) + Views | 3 |
| UnshareBucket | Zrusit ATTACH | 3 |
| LinkBucket | Vytvorit linked bucket (views) | 3 |
| UnlinkBucket | Smazat linked views | 3 |
| GrantBucketAccessToReadOnlyRole | Read-only pristup | 3 |
| RevokeBucketAccessFromReadOnlyRole | Zrusit read-only | 3 |

### Table CRUD (4)
| Command | Popis | Faze |
|---------|-------|------|
| CreateTable | CREATE TABLE | 4 |
| DropTable | DROP TABLE | 4 |
| CreateTableFromTimeTravel | Restore ze snapshotu | 7 |
| ObjectInfo | Metadata tabulky | 4 |

### Table Schema (6)
| Command | Popis | Faze |
|---------|-------|------|
| AddColumn | ALTER TABLE ADD COLUMN | 5 |
| DropColumn | ALTER TABLE DROP COLUMN | 5 |
| AlterColumn | ALTER TABLE ALTER COLUMN | 5 |
| AddPrimaryKey | Pridat PK (dedup columns) | 5 |
| DropPrimaryKey | Odebrat PK | 5 |
| DeleteTableRows | DELETE WHERE | 5 |

### Table Aliases (4)
| Command | Popis | Faze |
|---------|-------|------|
| CreateTableAlias | CREATE VIEW AS SELECT | 5 |
| UpdateAliasFilter | Upravit WHERE v alias view | 5 |
| RemoveAliasFilter | Odstranit filter z alias | 5 |
| SetAliasColumnsAutoSync | Nastavit auto-sync sloupcu | 5 |

### Import/Export (3)
| Command | Popis | Faze |
|---------|-------|------|
| ImportTableFromFile | COPY FROM (CSV/Parquet) | 6 |
| ImportTableFromTable | INSERT INTO SELECT | 6 |
| ExportTableToFile | COPY TO (CSV/Parquet) | 6 |

### Info/Preview (2)
| Command | Popis | Faze |
|---------|-------|------|
| PreviewTable | SELECT * LIMIT 1000 | 4 |
| ProfileTable | Statistiky sloupcu | 5 |

### Workspace (5)
| Command | Popis | Faze |
|---------|-------|------|
| CreateWorkspace | CREATE SCHEMA WORKSPACE_* | 9 |
| DropWorkspace | DROP SCHEMA WORKSPACE_* | 9 |
| ClearWorkspace | DROP all tables in workspace | 9 |
| DropWorkspaceObject | DROP konkretni objekt | 9 |
| ResetWorkspacePassword | N/A (neni relevantni) | 9 |

### Dev Branches (3) - volitelne
| Command | Popis | Faze |
|---------|-------|------|
| CreateDevBranch | Kopie DuckDB souboru | 8 |
| DropDevBranch | Smazat branch soubor | 8 |
| PullTableToBranch | ATTACH + INSERT SELECT | 8 |

### Query (1)
| Command | Popis | Faze |
|---------|-------|------|
| ExecuteQuery | Spustit SQL (pres write queue) | 4 |

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
│   │   ├── project.py             # /projects CRUD
│   │   ├── buckets.py             # /buckets CRUD + sharing
│   │   ├── tables.py              # /tables CRUD + import/export
│   │   ├── aliases.py             # /tables/.../alias
│   │   ├── workspaces.py          # /workspaces CRUD
│   │   ├── branches.py            # /branches CRUD + merge + pull
│   │   ├── snapshots.py           # /snapshots CRUD + restore
│   │   ├── files.py               # /files upload/download
│   │   └── query.py               # /query execute
│   ├── services/
│   │   ├── project_service.py
│   │   ├── bucket_service.py
│   │   ├── table_service.py
│   │   ├── alias_service.py
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
    ├── test_backend.py
    ├── test_project.py
    ├── test_buckets.py
    ├── test_tables.py
    ├── test_aliases.py
    ├── test_import_export.py
    ├── test_branches.py
    ├── test_snapshots.py
    ├── test_files.py
    ├── test_workspaces.py
    └── test_query.py
```

### 2. PHP Driver v Connection

```
connection/Package/StorageDriverDuckdb/
├── composer.json
├── services.yaml
├── src/
│   ├── DuckdbDriverClient.php      # Implements ClientInterface
│   ├── DuckdbApiClient.php         # HTTP client pro API Service
│   ├── DuckdbCredentialsHelper.php
│   └── Handler/
│       ├── Backend/
│       │   ├── InitBackendHandler.php
│       │   └── RemoveBackendHandler.php
│       ├── Project/
│       │   ├── CreateProjectHandler.php
│       │   ├── UpdateProjectHandler.php
│       │   └── DropProjectHandler.php
│       ├── Bucket/
│       │   ├── CreateBucketHandler.php
│       │   ├── DropBucketHandler.php
│       │   ├── ShareBucketHandler.php
│       │   └── ...
│       ├── Table/
│       │   ├── CreateTableHandler.php
│       │   ├── DropTableHandler.php
│       │   ├── ImportFromFileHandler.php
│       │   └── ...
│       └── Workspace/
│           └── ...
└── tests/
```

---

## Implementacni faze

### Faze 1: Zaklad API + Backend
- [ ] FastAPI app s healthcheck
- [ ] DuckDB connection manager
- [ ] Docker + docker-compose
- [ ] Zakladni konfigurace (ENV)
- [ ] POST /backend/init
- [ ] POST /backend/remove

### Faze 2: Project operace
- [ ] POST /projects (vytvorit DuckDB soubor)
- [ ] PUT /projects/{id} (update metadata)
- [ ] DELETE /projects/{id} (smazat soubor)
- [ ] GET /projects/{id}/info

### Faze 3: Bucket operace
- [ ] POST /buckets (CREATE SCHEMA)
- [ ] DELETE /buckets/{name}
- [ ] GET /buckets (list schemas)
- [ ] POST /buckets/{name}/share (ATTACH + Views)
- [ ] DELETE /buckets/{name}/share (DETACH)
- [ ] POST /buckets/{name}/link (linked bucket)
- [ ] DELETE /buckets/{name}/link
- [ ] POST /buckets/{name}/grant-readonly
- [ ] DELETE /buckets/{name}/grant-readonly

### Faze 4: Table CRUD + Query
- [ ] POST /tables (CREATE TABLE)
- [ ] DELETE /tables/{schema}/{table}
- [ ] GET /tables/{schema}/{table}/info (ObjectInfo)
- [ ] GET /tables/{schema}/{table}/preview (LIMIT 1000)
- [ ] POST /query (write queue integration)
- [ ] Async write queue implementace

### Faze 5: Table Schema + Aliases + Profile
- [ ] POST /tables/{schema}/{table}/columns (AddColumn)
- [ ] DELETE /tables/{schema}/{table}/columns/{name} (DropColumn)
- [ ] PUT /tables/{schema}/{table}/columns/{name} (AlterColumn)
- [ ] POST /tables/{schema}/{table}/primary-key
- [ ] DELETE /tables/{schema}/{table}/primary-key
- [ ] DELETE /tables/{schema}/{table}/rows (DELETE WHERE)
- [ ] POST /tables/{schema}/{table}/alias (CreateTableAlias)
- [ ] PUT /tables/{schema}/{table}/alias-filter
- [ ] DELETE /tables/{schema}/{table}/alias-filter
- [ ] PUT /tables/{schema}/{table}/alias-columns-sync
- [ ] POST /tables/{schema}/{table}/profile (ProfileTable)

### Faze 6: Import/Export
- [ ] POST /tables/{schema}/{table}/import/file
- [ ] POST /tables/{schema}/{table}/import/table
- [ ] POST /tables/{schema}/{table}/export
- [ ] Podpora CSV + Parquet
- [ ] Sliced files (wildcards)

### Faze 7: Snapshots
- [ ] POST /snapshots (Parquet export)
- [ ] GET /snapshots (list)
- [ ] GET /snapshots/{id}
- [ ] DELETE /snapshots/{id}
- [ ] POST /snapshots/{id}/restore (CreateTableFromTimeTravel)
- [ ] Auto-snapshot pred DROP TABLE

### Faze 8: Dev Branches
- [ ] POST /branches (create = copy file)
- [ ] DELETE /branches/{id}
- [ ] GET /branches/{id}/info
- [ ] POST /branches/{id}/merge
- [ ] POST /branches/{id}/tables/{table}/pull (PullTableToBranch)
- [ ] ATTACH pro cross-branch queries

### Faze 9: Workspaces
- [ ] POST /workspaces (CREATE SCHEMA WORKSPACE_*)
- [ ] DELETE /workspaces/{id}
- [ ] POST /workspaces/{id}/clear
- [ ] DELETE /workspaces/{id}/objects/{name}
- [ ] POST /workspaces/{id}/load (import dat do workspace)
- [ ] POST /branch/{branch_id}/workspaces (workspace v dev branch)
- [ ] POST /workspaces/{id}/query (execute v workspace kontextu)

### Faze 10: Storage Files (on-prem)
- [ ] File metadata schema v DuckDB
- [ ] POST /files/prepare (staging path)
- [ ] POST /files/upload
- [ ] GET /files/{id}
- [ ] DELETE /files/{id}
- [ ] Staging directory management
- [ ] Cleanup stale staging files

### Faze 11: PHP Integration
- [ ] DuckdbDriverClient (implements ClientInterface)
- [ ] DuckdbApiClient (HTTP klient)
- [ ] Vsechny Handlers (viz struktura vyse)
- [ ] Integration do DriverClientFactory
- [ ] E2E testy

### Faze 12: Production Readiness
- [ ] Snapshot retention policy
- [ ] Monitoring a metriky (Prometheus)
- [ ] Logging (strukturovane)
- [ ] Health checks
- [ ] Graceful shutdown
- [ ] Benchmark: 5000 tabulek / 500GB
- [ ] Dokumentace API (OpenAPI)

---

## Technologie

### Python API Service
```
duckdb>=1.0.0
fastapi>=0.109.0
uvicorn>=0.27.0
pydantic>=2.0.0
httpx>=0.26.0
python-dotenv>=1.0.0
prometheus-client>=0.19.0
structlog>=24.0.0
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

| Feature | DuckDB implementace | Slozitost |
|---------|---------------------|-----------|
| Storage Tables | DuckDB soubory (1 projekt = 1 file) | Nizka |
| Buckets | DuckDB schemas | Nizka |
| Dev Branches | Separate DuckDB files per branch | Stredni |
| Bucket Sharing | ATTACH (READ_ONLY) + Views | Stredni |
| Linked Buckets | Views na ATTACH-nuty zdroj | Stredni |
| Table Aliases | Views s optional WHERE | Nizka |
| Snapshots | Parquet export + metadata JSON | Nizka |
| Time Travel | Snapshot-based restore | Stredni |
| Storage Files | Lokalni FS + metadata v DuckDB | Stredni |
| Query Service | Write queue serialization | Vysoka |
| Workspaces | Izolovane schemas (WORKSPACE_*) | Nizka |
| Table Profile | SUMMARIZE / statistiky | Nizka |

---

## API Endpoints - Kompletni prehled

### Backend
```
POST   /backend/init                    # InitBackend
POST   /backend/remove                  # RemoveBackend
```

### Projects
```
POST   /projects                        # CreateProject
PUT    /projects/{id}                   # UpdateProject
DELETE /projects/{id}                   # DropProject
GET    /projects/{id}                   # ProjectInfo
```

### Buckets
```
POST   /projects/{id}/buckets           # CreateBucket
GET    /projects/{id}/buckets           # ListBuckets
DELETE /projects/{id}/buckets/{name}    # DropBucket
POST   /projects/{id}/buckets/{name}/share          # ShareBucket
DELETE /projects/{id}/buckets/{name}/share          # UnshareBucket
POST   /projects/{id}/buckets/{name}/link           # LinkBucket
DELETE /projects/{id}/buckets/{name}/link           # UnlinkBucket
POST   /projects/{id}/buckets/{name}/grant-readonly # GrantReadOnly
DELETE /projects/{id}/buckets/{name}/grant-readonly # RevokeReadOnly
```

### Tables
```
POST   /projects/{id}/tables                              # CreateTable
GET    /projects/{id}/tables/{schema}/{table}             # ObjectInfo
DELETE /projects/{id}/tables/{schema}/{table}             # DropTable
GET    /projects/{id}/tables/{schema}/{table}/preview     # PreviewTable
POST   /projects/{id}/tables/{schema}/{table}/columns     # AddColumn
DELETE /projects/{id}/tables/{schema}/{table}/columns/{n} # DropColumn
PUT    /projects/{id}/tables/{schema}/{table}/columns/{n} # AlterColumn
POST   /projects/{id}/tables/{schema}/{table}/primary-key # AddPrimaryKey
DELETE /projects/{id}/tables/{schema}/{table}/primary-key # DropPrimaryKey
DELETE /projects/{id}/tables/{schema}/{table}/rows        # DeleteTableRows
POST   /projects/{id}/tables/{schema}/{table}/profile     # ProfileTable
POST   /projects/{id}/tables/{schema}/{table}/import/file # ImportFromFile
POST   /projects/{id}/tables/{schema}/{table}/import/table# ImportFromTable
POST   /projects/{id}/tables/{schema}/{table}/export      # ExportToFile
```

### Table Aliases
```
POST   /projects/{id}/tables/{schema}/{table}/alias       # CreateTableAlias
PUT    /projects/{id}/tables/{schema}/{table}/alias-filter# UpdateAliasFilter
DELETE /projects/{id}/tables/{schema}/{table}/alias-filter# RemoveAliasFilter
PUT    /projects/{id}/tables/{schema}/{table}/alias-sync  # SetColumnsAutoSync
```

### Snapshots
```
POST   /projects/{id}/snapshots                    # CreateSnapshot
GET    /projects/{id}/snapshots                    # ListSnapshots
GET    /projects/{id}/snapshots/{snap_id}          # SnapshotDetail
DELETE /projects/{id}/snapshots/{snap_id}          # DeleteSnapshot
POST   /projects/{id}/snapshots/{snap_id}/restore  # RestoreFromSnapshot
```

### Dev Branches
```
POST   /projects/{id}/branches                     # CreateDevBranch
GET    /projects/{id}/branches                     # ListBranches
GET    /projects/{id}/branches/{branch_id}         # BranchDetail
DELETE /projects/{id}/branches/{branch_id}         # DropDevBranch
POST   /projects/{id}/branches/{branch_id}/merge   # MergeBranch
POST   /projects/{id}/branches/{branch_id}/tables/{table}/pull # PullTable
```

### Workspaces
```
POST   /projects/{id}/workspaces                   # CreateWorkspace
GET    /projects/{id}/workspaces                   # ListWorkspaces
GET    /projects/{id}/workspaces/{ws_id}           # WorkspaceDetail
DELETE /projects/{id}/workspaces/{ws_id}           # DropWorkspace
POST   /projects/{id}/workspaces/{ws_id}/clear     # ClearWorkspace
DELETE /projects/{id}/workspaces/{ws_id}/objects/{name} # DropWorkspaceObject
POST   /projects/{id}/workspaces/{ws_id}/load      # LoadDataToWorkspace
POST   /projects/{id}/workspaces/{ws_id}/query     # ExecuteInWorkspace

# Dev branch workspaces
POST   /projects/{id}/branches/{branch_id}/workspaces           # CreateBranchWorkspace
POST   /projects/{id}/branches/{branch_id}/workspaces/{ws}/query# QueryInBranchWorkspace
```

### Files (on-prem)
```
POST   /projects/{id}/files/prepare     # GetStagingPath
POST   /projects/{id}/files             # RegisterUploadedFile
GET    /projects/{id}/files/{file_id}   # GetFile
DELETE /projects/{id}/files/{file_id}   # DeleteFile
```

### Query
```
POST   /projects/{id}/query             # ExecuteQuery (via write queue)
```

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

## Vyhody on-prem DuckDB vs Cloud

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
├── duckdb-api-endpoints.md         # Storage API endpointy
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

## Dalsi kroky

1. [ ] Vytvorit `duckdb-api-service/` strukturu
2. [ ] Implementovat FastAPI skeleton + healthcheck
3. [ ] Pridat DuckDB connection manager
4. [ ] Implementovat write queue (ADR-005)
5. [ ] Prvni endpointy: /backend/init, /projects
6. [ ] Docker + docker-compose pro lokalni vyvoj
7. [ ] Zakladni testy

---

## Budouci rozsireni (nice-to-have)

- [ ] MCP server pro AI agenty (Claude, GPT)
- [ ] Vector search extension (VSS) pro embeddings
- [ ] Delta Lake / Iceberg integrace
- [ ] Streaming import (Apache Arrow)
- [ ] Horizontal scaling (sharding per project groups)
