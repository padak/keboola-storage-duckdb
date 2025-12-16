# DuckDB Storage Backend pro Keboola - Implementacni plan v6.6

> **Cil:** On-premise Keboola bez Snowflake a bez S3

---

## AKTUALNI STAV (2024-12-16)

### Co je hotovo

| Krok | Status | Poznamka |
|------|--------|----------|
| Lokalni Connection setup | DONE | Bezi na https://localhost:8700 |
| S3 File Storage | DONE | `padak-kbc-services-s3-files-storage-bucket` |
| GCS File Storage | DONE | `kbc-padak-files-storage` |
| Snowflake Backend | DONE | `vceecnu-bz34672.snowflakecomputing.com` |
| BigQuery Backend | DONE | GCP folder `393339196668` |
| BigQuery driver studium | **DONE** | Viz `bigquery-driver-research.md` |
| DuckDB API Service skeleton | **DONE** | `duckdb-api-service/` - FastAPI, Docker, testy |
| Centralni metadata databaze | **DONE** | ADR-008, `metadata.duckdb` |
| Project CRUD API | **DONE** | 20 novych testu (32 total) |
| Bucket CRUD API | **DONE** | 37 novych testu (69 total, vcetne sharing/linking) |
| Table CRUD + Preview | **DONE** | 29 novych testu (98 total) |
| **ADR-009 Refaktor** | **DONE** | **Per-table soubory, 98 testu PASS** |
| **Auth + Write Queue** | **DONE** | **TableLockManager + API keys, 144 testu PASS** |
| **Idempotency Middleware** | **DONE** | **X-Idempotency-Key header, 165 testu PASS** |
| **Prometheus /metrics** | **DONE** | **180 testu PASS** |
| **Table Schema Operations** | **DONE** | **33 novych testu (213 total)** |
| **Import/Export API** | **DONE** | **17 novych testu (230 total)** |
| **Files API (on-prem)** | **DONE** | **20 novych testu (250 total)** |
| **Snapshots API** | **DONE** | **34 novych testu (284 total), hierarchicka konfigurace** |
| Schema Migrations | TODO | Verzovani v DB + migrace pri startu |

### Kde jsme

```
[DONE] Rozjet lokalni Connection
       ↓
[DONE] Pridat BigQuery backend (referencni implementace)
       ↓
[DONE] Prostudovat BigQuery driver kod
       ↓
[DONE] Vytvorit DuckDB API Service skeleton
       ↓
[DONE] Pridat Centralni metadata databazi (ADR-008)
       ↓
[DONE] Pridat Project CRUD do Python API
       ↓
[DONE] Pridat Bucket CRUD do Python API (vcetne share/link/readonly)
       ↓
[DONE] Pridat Table CRUD + Preview do Python API
       ↓
[DONE] ADR-009 schvaleno (Codex GPT-5 validace, 4096 ATTACH test OK)
       ↓
[DONE] REFAKTOR NA ADR-009 (1 soubor per tabulka) - 98 testu PASS
       ↓
[DONE] Auth + Write Queue (TableLockManager + API keys) - 144 testu PASS
       ↓
[DONE] Idempotency Middleware - 165 testu PASS
       ↓
[DONE] Prometheus /metrics endpoint - 180 testu PASS
       ↓
[DONE] Table Schema Operations - 213 testu PASS
       ↓
[DONE] Files API - 230 testu PASS
       ↓
[DONE] Import/Export API - 250 testu PASS
       ↓
[DONE] Snapshots API (hierarchicka konfigurace) - 284 testu PASS
       ↓
[NOW]  *** Dev Branches ***
       ↓
[NEXT] Workspaces
       ↓
[LAST] PHP Driver Package (az bude Python API kompletni)
```

### Stav implementace podle fazi

| Faze | Popis | Stav | Pokryti |
|------|-------|------|---------|
| 1 | Backend + Observability | **100% - DONE** | **Prometheus /metrics implementovan** |
| 2 | Projects | **100%** | Hotovo |
| 3 | Buckets + Sharing | **100%** | Hotovo |
| 4 | Table CRUD + Preview | **100%** | Hotovo |
| **4.5** | **REFAKTOR ADR-009** | **100% - DONE** | **Per-table soubory, 98 testu** |
| **5** | **Auth + Write Queue** | **100% - DONE** | **TableLockManager + API keys, 144 testu** |
| **5.6** | **Idempotency Middleware** | **100% - DONE** | **165 testu PASS** |
| **5.7** | **Prometheus /metrics** | **100% - DONE** | **180 testu PASS** |
| **6** | **Table Schema Operations** | **100% - DONE** | **33 testu, 213 total** |
| **7** | **Import/Export** | **100% - DONE** | **17 testu, 230 total** |
| **8** | **Files API (on-prem)** | **100% - DONE** | **20 testu, 250 total** |
| **9** | **Snapshots + Settings** | **100% - DONE** | **34 testu, 284 total** |
| 10 | Dev Branches | **0%** | Zjednoduseno s ADR-009 |
| 11 | Workspaces | **0%** | Specifikace 30% |
| 12 | PHP Driver | **0%** | Ceka na Python API |

### Dalsi kroky (prioritizovane)

> **ZMENA STRATEGIE:** Nejprve dokoncime Python API, pak PHP Driver

1. ~~**Prostudovat BigQuery driver kod**~~ - DONE
   - [x] Detaily viz `bigquery-driver-research.md`

2. ~~**Project/Bucket/Table CRUD**~~ - DONE
   - [x] 98 pytest testu celkem
   - [x] Bucket sharing/linking
   - [x] Table preview s primary keys

3. ~~**Dospecifikovat a implementovat Write Queue**~~ - DONE
   - [x] Rozhodnout: Queue durability → **In-memory** (klient ceka, retry na strane Keboola)
   - [x] Rozhodnout: Batch vs single statement → **Single SQL**
   - [x] Rozhodnout: Idempotency → **X-Idempotency-Key header** (TTL 10 min)
   - [x] TableLockManager implementovan (per-table mutex)
   - [x] Idempotency middleware implementovan (21 testu)
   - [ ] Endpoint `POST /projects/{id}/query`
   - [ ] Connection pooling per project
   - [ ] Metriky: queue_depth, wait_time

3.5. ~~**Auth Middleware**~~ - DONE (144 testu PASS)
   - [x] `ADMIN_API_KEY` v ENV pro vytvareni projektu
   - [x] `PROJECT_ADMIN_API_KEY` generovan pri POST /projects
   - [x] SHA256 hash klicu v metadata.duckdb
   - [x] FastAPI dependencies: `require_admin`, `require_project_access`
   - [x] Vsechny endpointy chraneny (krome /health)

4. **Implementovat Table Schema Operations** (P1)
   - [ ] `POST /tables/{table}/columns` - AddColumn
   - [ ] `DELETE /tables/{table}/columns/{name}` - DropColumn
   - [ ] `PUT /tables/{table}/columns/{name}` - AlterColumn
   - [ ] `POST /tables/{table}/primary-key` - AddPrimaryKey
   - [ ] `DELETE /tables/{table}/primary-key` - DropPrimaryKey
   - [ ] `DELETE /tables/{table}/rows` - DeleteTableRows

5. **Dospecifikovat a implementovat Import/Export** (P0 - kriticke pro MVP)
   - [x] Rozhodnout: Staging table strategy → **Temp schema `_staging_{uuid}`**
   - [x] Rozhodnout: Dedup SQL → **INSERT ON CONFLICT**
   - [x] Rozhodnout: Incremental mode → **Full MERGE** (INSERT/UPDATE/DELETE)
   - [ ] Implementovat 3-stage pipeline
   - [ ] CSV + Parquet podpora
   - [ ] Sliced files (wildcards)

6. **Dospecifikovat a implementovat Files API** (P1)
   - [ ] Rozhodnout: Upload mechanism (multipart vs filesystem)
   - [ ] Rozhodnout: Staging cleanup policy (TTL)
   - [ ] Implementovat endpoints
   - [ ] File quotas per project

7. **Implementovat Snapshots** (P2)
   - [ ] Snapshot registry v metadata.duckdb
   - [ ] Parquet export s ZSTD
   - [ ] Retention policy
   - [ ] Auto-snapshot pred DROP TABLE

8. **PHP Driver Package** (LAST - az bude Python API hotove)
   - [ ] `DuckdbDriverClient` (implements `ClientInterface`)
   - [ ] `HandlerFactory` pro dispatch commands
   - [ ] Vsechny handlery

---

## Poznatky z BigQuery driveru (2024-12-15)

> **Detailni dokumentace:** Viz `bigquery-driver-research.md`

### Klicove poznatky pro DuckDB

| Aspekt | BigQuery | DuckDB (plan) |
|--------|----------|---------------|
| **Project** | GCP project (cloud resource) | `.duckdb` soubor (local file) |
| **Bucket** | BigQuery Dataset | DuckDB Schema |
| **InitBackend** | 4 validace (folder, permissions, IAM, billing) | Ping Python API + check storage |
| **CreateProject** | 11-step GCP setup | Vytvorit soubor |
| **Primary Key** | Metadata only (not enforced) | Nativni constraint (enforced) |
| **Import pipeline** | 3-stage (staging->transform->cleanup) | Stejny pattern |
| **Sharing** | Analytics Hub Listings | ATTACH (READ_ONLY) + Views |
| **File formats** | CSV only | CSV + Parquet nativne |

### Proc Python microservice misto pure PHP?

1. **DuckDB nema nativni PHP extension** (FFI je komplikovane)
2. **Python ma oficialni DuckDB binding** (rychle, stabilni)
3. **FastAPI umoznuje snadny development** (hot reload, typing)
4. **Oddeleni concerns** - PHP = protokol, Python = storage engine

---

## KRITICKA KOMPONENTA: Protocol Buffers

> **Toto je zaklad cele integrace driveru do Keboola Connection!**

### Proc jsou Protocol Buffers dulezite?

Connection komunikuje s drivery pres **protobuf messages**:
- Kazdy driver MUSI implementovat `ClientInterface` z `keboola/storage-driver-common`
- Vsechny commands jsou protobuf Message objekty (ne JSON, ne REST)
- Vsechny responses jsou protobuf Message objekty

### Komunikacni protokoly

| Komunikace | Protokol | Duvod |
|------------|----------|-------|
| Connection <-> PHP Driver | Protobuf | Definovano storage-driver-common |
| PHP Driver <-> Python API | REST/JSON | Jednodussi, debugging, flexibilita |

> **Poznamka:** PHP <-> Python pouziva REST/JSON misto protobuf pro jednodussi
> vyvoj a debugging. Pydantic modely v Pythonu jsou single source of truth
> pro API schema. PHP DTOs jsou generovany z OpenAPI spec, aby nedochazelo k driftu.

### Komunikacni tok

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           CONNECTION (PHP)                               │
│                                                                          │
│  TableInfoService / ImportService / ...                                  │
│         │                                                                │
│         ▼                                                                │
│  DriverClientFactory::getClientForBackend('duckdb')                     │
│         │                                                                │
│         ▼                                                                │
│  DriverClientWrapper                                                     │
│         │                                                                │
│         │ client->runCommand(                                            │
│         │   GenericBackendCredentials,  // protobuf Message              │
│         │   CreateTableCommand,          // protobuf Message              │
│         │   array $features,                                             │
│         │   RuntimeOptions               // protobuf Message              │
│         │ ): DriverResponse              // protobuf Message              │
│         │                                                                │
│         ▼                                                                │
└─────────┬───────────────────────────────────────────────────────────────┘
          │ PROTOBUF MESSAGES
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                DUCKDB DRIVER (implements ClientInterface)                │
│                                                                          │
│  DuckdbDriverClient::runCommand()                                        │
│         │                                                                │
│         ▼                                                                │
│  HandlerFactory::create($command)                                        │
│         │                                                                │
│         ├── CreateTableCommand::class => CreateTableHandler              │
│         ├── TableImportFromFileCommand::class => ImportTableHandler      │
│         ├── ObjectInfoCommand::class => ObjectInfoHandler                │
│         └── ... (33+ handleru)                                           │
│                │                                                         │
│                │ REST/JSON (interni komunikace)                          │
│                ▼                                                         │
│  ┌─────────────────────────────────────────┐                            │
│  │   DuckDB API Service (Python)           │                            │
│  │   - FastAPI                             │                            │
│  │   - Zpracovava SQL operace              │                            │
│  └─────────────────────────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────┘
```

### Kriticka zavislost: storage-driver-common

```json
{
    "require": {
        "keboola/storage-driver-common": "^7.8.0"
    }
}
```

Tento balicek obsahuje:

**Commands (Input):**
```
Keboola\StorageDriver\Command\
├── Backend\InitBackendCommand
├── Backend\RemoveBackendCommand
├── Project\CreateProjectCommand
├── Project\UpdateProjectCommand
├── Project\DropProjectCommand
├── Project\CreateDevBranchCommand
├── Project\DropDevBranchCommand
├── Bucket\CreateBucketCommand
├── Bucket\DropBucketCommand
├── Bucket\ShareBucketCommand
├── Bucket\UnshareBucketCommand
├── Bucket\LinkBucketCommand
├── Bucket\UnlinkBucketCommand
├── Bucket\GrantBucketAccessToReadOnlyRoleCommand
├── Bucket\RevokeBucketAccessFromReadOnlyRoleCommand
├── Table\CreateTableCommand
├── Table\DropTableCommand
├── Table\AddColumnCommand
├── Table\DropColumnCommand
├── Table\AlterColumnCommand
├── Table\AddPrimaryKeyCommand
├── Table\DropPrimaryKeyCommand
├── Table\DeleteTableRowsCommand
├── Table\TableImportFromFileCommand
├── Table\TableImportFromTableCommand
├── Table\TableExportToFileCommand
├── Table\PreviewTableCommand
├── Table\CreateProfileTableCommand
├── Table\CreateTableFromTimeTravelCommand
├── Workspace\CreateWorkspaceCommand
├── Workspace\DropWorkspaceCommand
├── Workspace\ClearWorkspaceCommand
├── Workspace\DropWorkspaceObjectCommand
├── Workspace\ResetWorkspacePasswordCommand
├── Info\ObjectInfoCommand
└── ExecuteQuery\ExecuteQueryCommand
```

**Responses (Output):**
```
Keboola\StorageDriver\Command\*\*Response
├── ObjectInfoResponse
├── TableImportResponse
├── TableExportToFileResponse
├── CreateWorkspaceResponse
├── InitBackendResponse
└── ... (pro kazdy command)
```

**Runtime Messages:**
```
Keboola\StorageDriver\Credentials\GenericBackendCredentials
Keboola\StorageDriver\Command\Common\RuntimeOptions
Keboola\StorageDriver\Command\Common\DriverResponse
```

### Handler priklad (z BigQuery driveru)

```php
<?php
// ImportTableFromFileHandler.php

final class ImportTableFromFileHandler extends BaseHandler
{
    public function __invoke(
        Message $credentials,        // GenericBackendCredentials
        Message $command,            // TableImportFromFileCommand
        array $features,
        Message $runtimeOptions,     // RuntimeOptions
    ): ?Message {                   // TableImportResponse
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableImportFromFileCommand);

        // Extrakce dat z protobuf
        $filePath = $command->getFilePath();
        $destination = $command->getDestination();
        $path = ProtobufHelper::repeatedStringToArray($destination->getPath());

        // ... zpracovani ...

        // Vytvoreni protobuf response
        $response = new TableImportResponse();
        $response->setTableRowsCount($rowCount);
        $response->setTableSizeBytes($sizeBytes);
        $response->setImportedRowsCount($importedRows);

        return $response;
    }
}
```

---

## Architektura

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         ON-PREMISE KEBOOLA                                │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                    KEBOOLA CONNECTION (PHP)                         │  │
│  │                                                                     │  │
│  │  Services (TableInfoService, ImportService, ...)                    │  │
│  │         │                                                           │  │
│  │         ▼                                                           │  │
│  │  DriverClientFactory → DriverClientWrapper                          │  │
│  │         │                                                           │  │
│  │         │ runCommand(credentials, command, features, runtimeOptions)│  │
│  │         │              ↑ PROTOBUF MESSAGES ↑                        │  │
│  │         ▼                                                           │  │
│  │  ┌──────────────────────────────────────────────────────────────┐   │  │
│  │  │  StorageDriverDuckdb Package                                 │   │  │
│  │  │                                                              │   │  │
│  │  │  DuckdbDriverClient (implements ClientInterface)             │   │  │
│  │  │         │                                                    │   │  │
│  │  │         ▼                                                    │   │  │
│  │  │  HandlerFactory::create($command)                            │   │  │
│  │  │         │                                                    │   │  │
│  │  │         ├── InitBackendHandler                               │   │  │
│  │  │         ├── CreateTableHandler                               │   │  │
│  │  │         ├── ImportTableFromFileHandler                       │   │  │
│  │  │         ├── ObjectInfoHandler                                │   │  │
│  │  │         └── ... (33+ handlers)                               │   │  │
│  │  │                │                                             │   │  │
│  │  │                │ DuckdbApiClient (HTTP/REST)                 │   │  │
│  │  │                ▼                                             │   │  │
│  │  └────────────────┼─────────────────────────────────────────────┘   │  │
│  └───────────────────┼─────────────────────────────────────────────────┘  │
│                      │                                                    │
│                      │ REST/JSON                                          │
│                      ▼                                                    │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                  DuckDB API Service (Python)                        │  │
│  │                                                                     │  │
│  │  FastAPI App                                                        │  │
│  │  ├── /backend/init, /backend/remove                                 │  │
│  │  ├── /projects CRUD                                                 │  │
│  │  ├── /tables CRUD + import/export                                   │  │
│  │  ├── /workspaces CRUD                                               │  │
│  │  ├── /branches CRUD                                                 │  │
│  │  └── /query (via write queue)                                       │  │
│  │                                                                     │  │
│  │  Write Queue (per project) → DuckDB Connection Manager              │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                      │                                                    │
│                      ▼                                                    │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                       LOCAL FILESYSTEM                              │  │
│  │                                                                     │  │
│  │  /data/duckdb/                        /data/files/                  │  │
│  │  ├── project_123_main.duckdb          ├── project_123/              │  │
│  │  ├── project_123_branch_456.duckdb    │   └── *.csv, *.parquet      │  │
│  │  └── project_124_main.duckdb          └── project_124/              │  │
│  │                                                                     │  │
│  │  /data/snapshots/                     /data/metadata/               │  │
│  │  └── project_123/snap_001/*.parquet   └── files.duckdb              │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

## Architektonicka rozhodnuti (ADR)

| ADR | Rozhodnuti | Status |
|-----|------------|--------|
| 001 | Python microservice misto PHP FFI driver | Accepted |
| 002 | ~~1 projekt = 1 DuckDB soubor, bucket = schema~~ | **Superseded by 009** |
| 003 | Dev branches = separate DuckDB files | Superseded by 007 |
| 004 | Snapshoty = Parquet export | Accepted |
| 005 | ~~Write serialization = async fronta per projekt~~ | **Simplified by 009** |
| 006 | Storage Files = lokalni filesystem + metadata v DuckDB | Accepted |
| 007 | Copy-on-Write branching = lazy table-level copy | Accepted |
| 008 | Centralni metadata databaze (`metadata.duckdb`) | Accepted |
| **009** | **1 DuckDB soubor per tabulka** | **Accepted (2024-12-16)** |

> **ADR-009 Impact:** Architektura zmenena na 1 soubor per tabulka. Paralelni zapis
> do ruznych tabulek, zjednodusena Write Queue, prirozeny CoW pro dev branches.
> Validovano Codex GPT-5 (4096 ATTACH test OK).

---

## Struktura souboru (ADR-009)

```
/data/
├── metadata.duckdb                            # Centralni metadata (ADR-008)
│   ├── projects                               # Registry projektu
│   ├── tables                                 # Registry tabulek (NOVE)
│   ├── files                                  # File storage metadata
│   ├── operations_log                         # Audit trail
│   └── stats                                  # Agregovane statistiky
│
├── duckdb/                                    # Storage Tables (ADR-009: per-table)
│   ├── project_123/                           # Projekt = adresar
│   │   ├── in_c_sales/                        # Bucket = adresar
│   │   │   ├── orders.duckdb                  # Tabulka = soubor
│   │   │   └── customers.duckdb
│   │   ├── out_c_reports/
│   │   │   └── summary.duckdb
│   │   └── _workspaces/                       # Workspace soubory
│   │       └── ws_789.duckdb
│   │
│   ├── project_123_branch_456/                # Dev branch = kopie adresare
│   │   └── in_c_sales/
│   │       └── orders.duckdb                  # Jen zmenene tabulky
│   │
│   ├── project_124/
│   │   └── ...
│   │
│   └── _staging/                              # Staging pro atomicke operace
│       └── {uuid}.duckdb
│
├── files/                                     # Storage Files (nahrada S3)
│   ├── project_123/
│   │   ├── staging/                           # Upload staging
│   │   └── 2024/12/11/*.csv
│   └── project_124/
│
└── snapshots/                                 # Table snapshots
    └── project_123/
        └── snap_orders_20241216/
            ├── metadata.json
            └── data.parquet
```

> **Zmena oproti puvodnimu:** Misto `project_123.duckdb` (jeden soubor) mame
> `project_123/bucket/table.duckdb` (adresar s per-table soubory).

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

**Aktualne implementovano (180 testu):**
```
duckdb-api-service/
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── pytest.ini
├── src/
│   ├── __init__.py
│   ├── main.py                    # FastAPI app [DONE]
│   ├── config.py                  # ENV konfigurace [DONE]
│   ├── database.py                # MetadataDB + ProjectDBManager [DONE]
│   ├── auth.py                    # API key generation/verification [DONE]
│   ├── dependencies.py            # FastAPI auth dependencies [DONE]
│   ├── metrics.py                 # Prometheus metrics definitions [DONE]
│   ├── middleware/
│   │   ├── __init__.py
│   │   ├── idempotency.py         # X-Idempotency-Key handling [DONE]
│   │   └── metrics.py             # Request instrumentation [DONE]
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── backend.py             # /health, /backend/* [DONE]
│   │   ├── projects.py            # /projects CRUD [DONE]
│   │   ├── buckets.py             # /buckets CRUD [DONE]
│   │   ├── bucket_sharing.py      # share/link/readonly [DONE]
│   │   ├── tables.py              # /tables CRUD + preview [DONE]
│   │   └── metrics.py             # /metrics endpoint [DONE]
│   └── models/
│       ├── __init__.py
│       └── responses.py           # Pydantic models [DONE]
└── tests/
    ├── conftest.py                # Pytest fixtures [DONE]
    ├── test_backend.py            # 12 testu [DONE]
    ├── test_projects.py           # 20 testu [DONE]
    ├── test_buckets.py            # 20 testu [DONE]
    ├── test_bucket_sharing.py     # 20 testu [DONE]
    ├── test_tables.py             # 34 testu [DONE]
    ├── test_table_lock.py         # 14 testu [DONE]
    ├── test_auth.py               # 13 testu [DONE]
    ├── test_api_keys.py           # 11 testu [DONE]
    ├── test_idempotency.py        # 21 testu [DONE]
    └── test_metrics.py            # 15 testu [DONE]
```

**Chybejici komponenty (potreba implementovat):**
```
├── src/
│   ├── write_queue.py             # [TODO] Async write serialization (POST /query)
│   ├── connection_pool.py         # [TODO] Per-project connection management
│   ├── routers/
│   │   ├── table_schema.py        # [TODO] AddColumn, DropColumn, etc.
│   │   ├── table_import.py        # [TODO] Import/Export pipeline
│   │   ├── aliases.py             # [TODO] Table aliases
│   │   ├── workspaces.py          # [TODO] Workspace CRUD
│   │   ├── branches.py            # [TODO] Dev branches
│   │   ├── snapshots.py           # [TODO] Snapshot CRUD + restore
│   │   ├── files.py               # [TODO] File upload/download
│   │   └── query.py               # [TODO] Query execution
│   └── services/
│       ├── import_service.py      # [TODO] 3-stage import pipeline
│       ├── snapshot_service.py    # [TODO] Parquet export/restore
│       └── file_service.py        # [TODO] File lifecycle
└── tests/
    ├── test_table_schema.py       # [TODO]
    ├── test_import_export.py      # [TODO]
    ├── test_snapshots.py          # [TODO]
    ├── test_files.py              # [TODO]
    └── test_write_queue.py        # [TODO]
```

### 2. PHP Driver v Connection (StorageDriverDuckdb Package)

> **KRITICKA KOMPONENTA** - Implementuje Protocol Buffers integraci!

```
connection/Package/StorageDriverDuckdb/
├── composer.json                        # google/protobuf, storage-driver-common
├── services.yaml
├── src/
│   ├── DuckdbDriverClient.php           # Implements ClientInterface
│   │                                    # - runCommand(Message, Message, array, Message)
│   │                                    # - Vola HandlerFactory
│   │                                    # - Vraci DriverResponse
│   │
│   ├── DuckdbApiClient.php              # HTTP client pro Python API
│   │                                    # - GET, POST, PUT, DELETE
│   │                                    # - JSON serialization
│   │                                    # - Error handling
│   │
│   ├── DuckdbCredentialsHelper.php      # Extrakce credentials z GenericBackendCredentials
│   │
│   └── Handler/
│       ├── HandlerFactory.php           # Match command -> handler
│       │                                # CreateTableCommand::class => CreateTableHandler
│       │                                # TableImportFromFileCommand::class => ImportHandler
│       │                                # ... (33+ mappings)
│       │
│       ├── BaseHttpHandler.php          # Spolecna logika pro vsechny handlery
│       │                                # - extractProjectId()
│       │                                # - callApi()
│       │                                # - buildResponse()
│       │
│       ├── Backend/
│       │   ├── InitBackendHandler.php   # InitBackendCommand → null
│       │   └── RemoveBackendHandler.php # RemoveBackendCommand → null
│       │
│       ├── Project/
│       │   ├── CreateProjectHandler.php  # CreateProjectCommand → ObjectInfoResponse
│       │   ├── UpdateProjectHandler.php  # UpdateProjectCommand → ObjectInfoResponse
│       │   └── DropProjectHandler.php    # DropProjectCommand → null
│       │
│       ├── Bucket/
│       │   ├── CreateBucketHandler.php                   # CreateBucketCommand
│       │   ├── DropBucketHandler.php                     # DropBucketCommand
│       │   ├── ShareBucketHandler.php                    # ShareBucketCommand
│       │   ├── UnshareBucketHandler.php                  # UnshareBucketCommand
│       │   ├── LinkBucketHandler.php                     # LinkBucketCommand
│       │   ├── UnlinkBucketHandler.php                   # UnlinkBucketCommand
│       │   ├── GrantBucketAccessToReadOnlyRoleHandler.php
│       │   └── RevokeBucketAccessFromReadOnlyRoleHandler.php
│       │
│       ├── Table/
│       │   ├── CreateTableHandler.php                    # CreateTableCommand → ObjectInfoResponse
│       │   ├── DropTableHandler.php                      # DropTableCommand → null
│       │   ├── AddColumnHandler.php                      # AddColumnCommand → ObjectInfoResponse
│       │   ├── DropColumnHandler.php                     # DropColumnCommand → ObjectInfoResponse
│       │   ├── AlterColumnHandler.php                    # AlterColumnCommand → ObjectInfoResponse
│       │   ├── AddPrimaryKeyHandler.php                  # AddPrimaryKeyCommand → ObjectInfoResponse
│       │   ├── DropPrimaryKeyHandler.php                 # DropPrimaryKeyCommand → ObjectInfoResponse
│       │   ├── DeleteTableRowsHandler.php                # DeleteTableRowsCommand → ObjectInfoResponse
│       │   ├── ImportTableFromFileHandler.php            # TableImportFromFileCommand → TableImportResponse
│       │   ├── ImportTableFromTableHandler.php           # TableImportFromTableCommand → TableImportResponse
│       │   ├── ExportTableToFileHandler.php              # TableExportToFileCommand → TableExportToFileResponse
│       │   ├── PreviewTableHandler.php                   # PreviewTableCommand → PreviewTableResponse
│       │   ├── ProfileTableHandler.php                   # CreateProfileTableCommand → ObjectInfoResponse
│       │   └── CreateTableFromTimeTravelHandler.php      # CreateTableFromTimeTravelCommand → ObjectInfoResponse
│       │
│       ├── Workspace/
│       │   ├── CreateWorkspaceHandler.php                # CreateWorkspaceCommand → CreateWorkspaceResponse
│       │   ├── DropWorkspaceHandler.php                  # DropWorkspaceCommand → null
│       │   ├── ClearWorkspaceHandler.php                 # ClearWorkspaceCommand → null
│       │   ├── DropWorkspaceObjectHandler.php            # DropWorkspaceObjectCommand → null
│       │   └── ResetWorkspacePasswordHandler.php         # ResetWorkspacePasswordCommand → null (N/A)
│       │
│       ├── Branch/
│       │   ├── CreateDevBranchHandler.php                # CreateDevBranchCommand → ObjectInfoResponse
│       │   └── DropDevBranchHandler.php                  # DropDevBranchCommand → null
│       │
│       ├── Info/
│       │   └── ObjectInfoHandler.php                     # ObjectInfoCommand → ObjectInfoResponse
│       │
│       └── ExecuteQuery/
│           └── ExecuteQueryHandler.php                   # ExecuteQueryCommand → ExecuteQueryResponse
│
└── tests/
    ├── Unit/
    │   ├── HandlerFactoryTest.php
    │   └── Handler/
    │       └── ...
    └── Functional/
        └── ...
```

### DuckdbDriverClient.php - Implementace

```php
<?php
namespace Keboola\StorageDriver\Duckdb;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Duckdb\Handler\HandlerFactory;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class DuckdbDriverClient implements ClientInterface
{
    private DuckdbApiClient $apiClient;
    protected LoggerInterface $internalLogger;

    public function __construct(
        string $apiUrl,
        ?LoggerInterface $internalLogger = null
    ) {
        $this->apiClient = new DuckdbApiClient($apiUrl);
        $this->internalLogger = $internalLogger ?? new NullLogger();
    }

    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);

        // Dispatch command to appropriate handler
        $handler = HandlerFactory::create(
            $command,
            $this->apiClient,
            $this->internalLogger,
        );

        // Execute handler
        $handledResponse = $handler(
            $credentials,
            $command,
            $features,
            $runtimeOptions,
        );

        // Wrap response in DriverResponse
        $response = new DriverResponse();
        if ($handledResponse !== null) {
            $any = new Any();
            $any->pack($handledResponse);
            $response->setCommandResponse($any);
        }

        // Include log messages if handler supports them
        if ($handler instanceof BaseHandler) {
            $response->setMessages($handler->getMessages());
        }

        return $response;
    }
}
```

---

## Implementacni faze

> **NOVA STRATEGIE:** Nejprve dokoncime Python API, pak PHP Driver.
> PHP Driver je az Faze 10 - ceka na kompletni Python API.

### Faze 1: Zaklad API + Backend + Observability - DONE
- [x] FastAPI app s healthcheck
- [x] DuckDB connection manager
- [x] Docker + docker-compose
- [x] Zakladni konfigurace (ENV)
- [x] Python: POST /backend/init
- [x] Python: POST /backend/remove
- [x] **Observability zaklad:**
  - [x] Structured logging (structlog) - JSON format
  - [x] Request ID middleware (X-Request-ID propagace)
  - [x] Request/response logging s timing

### Faze 2: Project operace - DONE
- [x] POST /projects (vytvorit DuckDB soubor)
- [x] PUT /projects/{id} (update metadata)
- [x] DELETE /projects/{id} (smazat soubor)
- [x] GET /projects/{id}/info
- [x] GET /projects (list s filtrovanim a paginaci)
- [x] GET /projects/{id}/stats (live statistiky)
- [x] Centralni metadata databaze (ADR-008)
- [x] Operations audit log
- [x] 32 pytest testu

### Faze 3: Bucket operace - DONE
- [x] POST /buckets (CREATE SCHEMA)
- [x] DELETE /buckets/{name}
- [x] GET /buckets (list schemas)
- [x] POST /buckets/{name}/share
- [x] DELETE /buckets/{name}/share
- [x] POST /buckets/{name}/link (ATTACH + views)
- [x] DELETE /buckets/{name}/link
- [x] POST /buckets/{name}/grant-readonly
- [x] DELETE /buckets/{name}/grant-readonly
- [x] 37 pytest testu

### Faze 4: Table CRUD + Preview - DONE
- [x] POST /tables (CREATE TABLE)
- [x] DELETE /tables/{schema}/{table}
- [x] GET /tables/{schema}/{table} (ObjectInfo)
- [x] GET /tables (list)
- [x] GET /tables/{schema}/{table}/preview (LIMIT)
- [x] Primary key support (enforced)
- [x] 29 pytest testu

### Faze 4.5: REFAKTOR NA ADR-009 - DONE

> **Proc ted?** Codex GPT-5 doporucil: "Doing it upfront is easier than retrofitting
> after a large fleet exists." Refaktor PRED Write Queue a Import/Export.

**Zmeny v architekture:**
- Projekt = adresar (`project_123/`)
- Bucket = adresar (`project_123/in_c_sales/`)
- Tabulka = soubor (`project_123/in_c_sales/orders.duckdb`)

**Komponenty k refaktoru:**

1. **database.py - ProjectDBManager**
   - [x] `get_project_path()` - vracet adresar misto souboru
   - [x] `get_bucket_path()` - vracet adresar
   - [x] `get_table_path()` - vracet cestu k .duckdb souboru
   - [x] `create_project()` - vytvorit adresar
   - [x] `delete_project()` - smazat adresar rekurzivne

2. **routers/buckets.py**
   - [x] `create_bucket()` - vytvorit adresar v projektu
   - [x] `delete_bucket()` - smazat adresar s tabulkami
   - [x] `list_buckets()` - listovat adresare

3. **routers/tables.py**
   - [x] `create_table()` - vytvorit .duckdb soubor v bucket adresari
   - [x] `delete_table()` - smazat .duckdb soubor
   - [x] `list_tables()` - listovat .duckdb soubory v bucket adresari
   - [x] `get_table_info()` - otevrit .duckdb a cist schema
   - [x] `preview_table()` - ATTACH + SELECT

4. **routers/bucket_sharing.py**
   - [x] Metadata-based routing pro linked buckety
   - [x] ATTACH z jineho projektu s READ_ONLY

5. **metadata.duckdb schema**
   - [x] Pridat `tables` tabulku (registry per-table souboru)
   - [x] Upravit `buckets` - bucket uz neni schema v DB

6. **Testy**
   - [x] Upravit fixtures pro novou strukturu
   - [x] Prejit vsechny testy na novy format
   - [x] Pridat testy pro ATTACH cross-table queries

**Vysledek:**
- 98 testu PASS
- Per-table soubory funguji

**Benefity po refaktoru:**
- Write Queue dramaticky zjednodusena (nebo zbytecna)
- Paralelni import do ruznych tabulek
- Dev branches = kopie adresare
- Snapshots = kopie souboru

### Faze 5: Write Queue + Auth - DONE

> **ADR-009 impact:** S per-table soubory je Write Queue **dramaticky zjednodusena**.
> Kazda tabulka ma vlastni soubor = vlastni writer. Fronta je potreba pouze pro
> koordinaci zapisu do STEJNE tabulky, ne celeho projektu.

**Zjednodusena architektura:**
```
BEFORE (ADR-002):                    AFTER (ADR-009):
┌─────────────────────┐              ┌─────────────────────┐
│ Import orders ──────┼──► QUEUE    │ Import orders ──────┼──► orders.duckdb
│ Import customers ───┤     │        │ Import customers ───┼──► customers.duckdb
│ Import products ────┘     ▼        │ Import products ────┼──► products.duckdb
│                      project.duckdb│                     │
│ (SERIALIZOVANO!)                   │ (PARALELNE!)        │
└─────────────────────┘              └─────────────────────┘
```

**Implementovano:**
- [x] Per-table locking (TableLockManager - simple mutex per file) - 14 testu
- [x] Auth middleware (hierarchicky API key model) - 24 testu
- [x] Idempotency middleware (X-Idempotency-Key) - 21 testu
- [x] Prometheus /metrics endpoint - 15 testu

**Jeste TODO (presunuto do pozdejsich fazi):**
- [ ] POST /projects/{id}/query endpoint (Faze 7+)
- [ ] Graceful shutdown (Faze 13)

**Co uz NENI potreba:**
- ~~Project-level write queue~~ (zbytecna)
- ~~Priority queue~~ (zbytecna pro per-table)
- ~~Complex queue management~~ (simple lock staci)

### Faze 6: Table Schema Operations - DONE
- [x] POST /tables/{table}/columns (AddColumn)
- [x] DELETE /tables/{table}/columns/{name} (DropColumn)
- [x] PUT /tables/{table}/columns/{name} (AlterColumn)
- [x] POST /tables/{table}/primary-key (AddPrimaryKey)
- [x] DELETE /tables/{table}/primary-key (DropPrimaryKey)
- [x] DELETE /tables/{table}/rows (DeleteTableRows with WHERE)
- [x] POST /tables/{table}/profile (ProfileTable - SUMMARIZE)
- [x] Pytest testy (33 novych testu, 213 total)

> **Poznamka:** DuckDB nepodporuje `ALTER TABLE ADD COLUMN` s `NOT NULL` constraint.
> Sloupce se musi pridat jako nullable a pak zmenit na NOT NULL pres `ALTER COLUMN`.

### Faze 7: Import/Export - DONE
> **Implementovano 2024-12-16**

- [x] Implementovat 3-stage import pipeline (STAGING -> TRANSFORM -> CLEANUP)
- [x] POST /tables/{table}/import/file (COPY FROM CSV/Parquet)
- [x] POST /tables/{table}/export (COPY TO CSV/Parquet)
- [x] CSV + Parquet podpora
- [x] Deduplication s primary keys (INSERT ON CONFLICT)
- [x] Incremental import (merge/upsert mode)
- [x] Column filtering, WHERE filter, LIMIT for export
- [x] Compression support (gzip for CSV, gzip/zstd/snappy for Parquet)
- [x] Pytest testy (17 novych testu)

### Faze 8: Files API (on-prem) - DONE
> **Implementovano 2024-12-16**

- [x] File metadata schema v metadata.duckdb
- [x] POST /files/prepare (staging upload session)
- [x] POST /files/upload/{key} (multipart upload)
- [x] POST /files (register uploaded file)
- [x] GET /files (list files)
- [x] GET /files/{id} (file info)
- [x] GET /files/{id}/download (download content)
- [x] DELETE /files/{id}
- [x] SHA256 checksum validation during upload
- [x] 3-stage workflow: prepare -> upload -> register
- [x] Pytest testy (20 novych testu, 250 total)

### Faze 9: Snapshots + Settings - DONE
> **Implementovano 2024-12-16**
> **Viz ADR-004 pro detailni specifikaci hierarchicke konfigurace**

- [x] Snapshot registry v metadata.duckdb (`snapshots` tabulka)
- [x] Snapshot settings registry (`snapshot_settings` tabulka)
- [x] Hierarchicka konfigurace (System -> Project -> Bucket -> Table)
- [x] POST /snapshots (Parquet export s ZSTD)
- [x] GET /snapshots (list s filtry)
- [x] GET /snapshots/{id} (vcetne schema)
- [x] DELETE /snapshots/{id}
- [x] POST /snapshots/{id}/restore
- [x] Auto-snapshot pred DROP TABLE (default enabled)
- [x] Auto-snapshot pred DROP COLUMN (konfigurovatelne)
- [x] Konfigurovatelna retention policy (per-project/bucket/table)
- [x] GET/PUT/DELETE /settings/snapshots na vsech urovnich
- [x] Pytest testy (34 novych testu, 284 total)

### Faze 10: PHP Driver Package
> **Ceka na Faze 5-9** - Python API musi byt kompletni

- [ ] Vytvorit `connection/Package/StorageDriverDuckdb/`
- [ ] `DuckdbDriverClient` (implements ClientInterface)
- [ ] `DuckdbApiClient` (HTTP client)
- [ ] `HandlerFactory` (dispatch)
- [ ] Vsechny Handlers (33+)
- [ ] Registrace v DriverClientFactory
- [ ] E2E testy

### Faze 11: Dev Branches (volitelne)
- [ ] POST /branches (CoW branch creation)
- [ ] DELETE /branches/{id}
- [ ] POST /branches/{id}/merge
- [ ] POST /branches/{id}/tables/{table}/pull
- [ ] Pytest testy

### Faze 12: Workspaces (volitelne)
- [ ] POST /workspaces (CREATE SCHEMA WORKSPACE_*)
- [ ] DELETE /workspaces/{id}
- [ ] POST /workspaces/{id}/clear
- [ ] POST /workspaces/{id}/load
- [ ] POST /workspaces/{id}/query
- [ ] Pytest testy

### Faze 13: Production Readiness
- [ ] Prometheus metrics endpoint (/metrics)
- [ ] API key autentizace
- [ ] Rate limiting
- [ ] Graceful shutdown
- [ ] Benchmark: 5000 tabulek / 500GB
- [ ] OpenAPI dokumentace

---

## DETAILNI SPECIFIKACE - POTREBA DOPLNIT

> **POZOR:** Nasledujici sekce obsahuji specifikace, ktere potrebuji rozhodnuti
> pred implementaci. Kazda sekce obsahuje "Rozhodnuti k uceneni" a "Doporuceni".

### Write Queue - Detailni specifikace

**Ucel:** DuckDB je single-writer, multi-reader. Write Queue serializuje zapisy per projekt.

**Architektura:**
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  HTTP Request   │────►│  Write Queue    │────►│  DuckDB File    │
│  (concurrent)   │     │  (per project)  │     │  (single writer)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                              │
                        asyncio.Queue
                        + PriorityQueue
```

**Rozhodnuti k uceneni:**

| Rozhodnuti | Moznosti | Doporuceni |
|------------|----------|------------|
| Queue durability | In-memory / Persistent (SQLite) | **In-memory** - jednodussi, restart = restart queue |
| Batch support | Single SQL / Multi-statement | **Single SQL** - jednodussi error handling |
| Priority levels | None / Low-Normal-High | **Normal + High** - system ops get priority |
| Timeout strategy | Fixed / Adaptive | **Fixed** s konfigurovatelnym defaultem |
| Max queue size | Fixed (1000) | **Konfigurovatelne** (default 1000) |

**API Endpoint:**

```
POST /projects/{project_id}/query

Request:
{
  "sql": "INSERT INTO bucket.table VALUES ...",
  "priority": "normal",        # normal | high
  "timeout_seconds": 300,      # max execution time
  "is_write": true             # optional, auto-detected if not provided
}

Response (success):
{
  "result": [...],             # query results (for SELECT)
  "rows_affected": 100,        # for INSERT/UPDATE/DELETE
  "execution_time_ms": 45,
  "queue_wait_time_ms": 12
}

Response (error):
{
  "error": "Queue full",
  "error_type": "QueueOverflow",
  "queue_depth": 1000
}
```

**Write vs Read Detection:**
```python
WRITE_KEYWORDS = ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'COPY']

def is_write_query(sql: str) -> bool:
    sql_upper = sql.strip().upper()
    # Handle CTEs: WITH ... AS (...) INSERT/SELECT
    if sql_upper.startswith('WITH'):
        # Find the main statement after CTEs
        # Simplified: check if INSERT/UPDATE/DELETE appears
        return any(kw in sql_upper for kw in ['INSERT', 'UPDATE', 'DELETE'])
    return any(sql_upper.startswith(kw) for kw in WRITE_KEYWORDS)
```

**Connection Pool Strategy:**
```python
# Per-project connection management
class ProjectConnectionManager:
    def __init__(self, project_id: str, db_path: Path):
        self.write_conn: duckdb.DuckDBPyConnection = None  # exclusive
        self.read_pool: list[duckdb.DuckDBPyConnection] = []  # shared
        self.max_read_connections = 10

    async def get_write_connection(self) -> duckdb.DuckDBPyConnection:
        # Single writer - return or wait
        ...

    async def get_read_connection(self) -> duckdb.DuckDBPyConnection:
        # Pool of read-only connections
        ...
```

**Graceful Shutdown:**
1. Stop accepting new requests (return 503)
2. Wait for in-flight operations (max 30s)
3. Drain queue (max 60s)
4. Close all connections

---

### Import/Export - Detailni specifikace

**Ucel:** 3-stage pipeline pro import dat z CSV/Parquet s deduplikaci.

**Pipeline:**
```
Stage 1: STAGING
  - Vytvorit staging tabulku (temp schema)
  - COPY FROM file do staging

Stage 2: TRANSFORM
  - Deduplikace podle PK (pokud existuje)
  - INSERT INTO target FROM staging

Stage 3: CLEANUP
  - DROP staging tabulka
  - Vratit statistiky
```

**Schvalena rozhodnuti:**

| Rozhodnuti | Schvaleno |
|------------|-----------|
| Staging location | Temp schema `_staging_{uuid}` |
| Dedup strategy | INSERT ON CONFLICT (DuckDB native) |
| Incremental mode | **Full MERGE** (INSERT/UPDATE/DELETE) |
| File source | File ID (z Files API) |

**Deduplication SQL (s Primary Key) - pro full load:**
```sql
-- DuckDB podporuje INSERT ... ON CONFLICT
INSERT INTO target_schema.target_table
SELECT * FROM staging_schema.staging_table
ON CONFLICT (pk_column1, pk_column2) DO UPDATE SET
  col1 = EXCLUDED.col1,
  col2 = EXCLUDED.col2,
  ...
```

**Full MERGE SQL (pro incremental load):**
```sql
-- 1. UPDATE existujicich zaznamu
UPDATE target_schema.target_table AS t
SET col1 = s.col1, col2 = s.col2, ...
FROM staging_schema.staging_table AS s
WHERE t.pk_column = s.pk_column;

-- 2. INSERT novych zaznamu
INSERT INTO target_schema.target_table
SELECT * FROM staging_schema.staging_table AS s
WHERE NOT EXISTS (
    SELECT 1 FROM target_schema.target_table AS t
    WHERE t.pk_column = s.pk_column
);

-- 3. DELETE smazanych zaznamu (pokud je _deleted flag ve zdroji)
DELETE FROM target_schema.target_table AS t
WHERE EXISTS (
    SELECT 1 FROM staging_schema.staging_table AS s
    WHERE t.pk_column = s.pk_column AND s._deleted = true
);
```

> **Poznamka:** Full MERGE je slozitejsi, ale umoznuje kompletni synchronizaci
> vcetne mazani zaznamu. Vyzaduje `_deleted` flag ve zdrojovych datech.

**API Endpoint - Import from File:**
```
POST /projects/{id}/buckets/{bucket}/tables/{table}/import/file

Request:
{
  "file_id": "12345",                    # ID z Files API
  "format": "csv",                       # csv | parquet
  "csv_options": {                       # pouze pro CSV
    "delimiter": ",",
    "quote": "\"",
    "escape": "\\",
    "header": true,
    "null_string": ""
  },
  "import_options": {
    "incremental": false,                # false = TRUNCATE + INSERT
    "dedup_mode": "update_duplicates",   # update_duplicates | insert_duplicates | fail_on_duplicates
    "columns": ["col1", "col2"]          # optional column mapping
  }
}

Response:
{
  "imported_rows": 5000,
  "table_rows_after": 12500,
  "table_size_bytes": 1048576,
  "warnings": []
}
```

**API Endpoint - Export to File:**
```
POST /projects/{id}/buckets/{bucket}/tables/{table}/export

Request:
{
  "format": "csv",                       # csv | parquet
  "compression": "gzip",                 # none | gzip | zstd (parquet only)
  "columns": ["col1", "col2"],           # optional, default all
  "where_filter": "created_at > '2024-01-01'",
  "limit": 10000                         # optional
}

Response:
{
  "file_id": "67890",                    # ID pro stahnuti z Files API
  "file_path": "/data/files/project_123/2024/12/15/export_xyz.csv",
  "rows_exported": 5000,
  "file_size_bytes": 524288
}
```

---

### Files API - Detailni specifikace

**Ucel:** On-prem nahrada S3/GCS pro staging souboru pred importem.

**Workflow:**
```
1. PREPARE: Ziskat staging path pro upload
2. UPLOAD: Nahrat soubor na staging path
3. REGISTER: Zaregistrovat soubor (move ze staging, compute checksum)
4. USE: Import do tabulky
5. CLEANUP: Smazat po pouziti (nebo po TTL)
```

**Rozhodnuti k uceneni:**

| Rozhodnuti | Moznosti | Doporuceni |
|------------|----------|------------|
| Upload mechanism | Multipart POST / Direct filesystem | **Multipart POST** (bezpecnejsi) |
| Staging TTL | 1h / 24h / 7d | **24h** |
| Checksum algorithm | MD5 / SHA256 | **SHA256** |
| Max file size | 1GB / 10GB / unlimited | **10GB** (konfigurovatelne) |
| File quotas | Per project | **Max 10000 files, 1TB total** |

**Directory Structure:**
```
/data/files/
├── project_123/
│   ├── staging/                    # Temporary uploads (TTL 24h)
│   │   └── upload_abc123.csv       # Named by upload key
│   └── 2024/12/15/                 # Permanent storage (date-organized)
│       ├── file_001_data.csv
│       └── file_002_export.parquet
```

**File Metadata Schema (v metadata.duckdb):**
```sql
CREATE TABLE files (
    id VARCHAR PRIMARY KEY,           -- UUID
    project_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,            -- original filename
    path VARCHAR NOT NULL,            -- relative path in /data/files/
    size_bytes BIGINT NOT NULL,
    checksum_sha256 VARCHAR(64),
    content_type VARCHAR(100),
    is_staging BOOLEAN DEFAULT false, -- true = in staging, not yet registered
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,           -- for staging files
    tags JSON,

    -- Foreign key
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE INDEX idx_files_project ON files(project_id);
CREATE INDEX idx_files_staging ON files(is_staging, expires_at);
```

**API Endpoints:**

```
POST /projects/{id}/files/prepare
Request: { "filename": "data.csv", "content_type": "text/csv" }
Response: {
  "upload_key": "abc123",
  "upload_url": "/projects/{id}/files/upload/abc123",
  "expires_at": "2024-12-16T10:00:00Z"
}

POST /projects/{id}/files/upload/{upload_key}
Content-Type: multipart/form-data
Body: file content
Response: { "staging_path": "/staging/upload_abc123.csv" }

POST /projects/{id}/files
Request: {
  "upload_key": "abc123",
  "name": "my_data.csv",
  "tags": {"source": "manual"}
}
Response: {
  "file_id": "file_xyz",
  "path": "/2024/12/15/file_xyz_my_data.csv",
  "size_bytes": 1048576,
  "checksum_sha256": "abc123..."
}

GET /projects/{id}/files/{file_id}
Response: {
  "id": "file_xyz",
  "name": "my_data.csv",
  "path": "...",
  "size_bytes": 1048576,
  "download_url": "/projects/{id}/files/{file_id}/download"
}

DELETE /projects/{id}/files/{file_id}
Response: { "deleted": true }
```

**Staging Cleanup Job:**
```python
async def cleanup_stale_staging_files():
    """Run every hour - delete staging files older than TTL."""
    expired = metadata_db.query("""
        SELECT id, path FROM files
        WHERE is_staging = true AND expires_at < now()
    """)
    for file in expired:
        os.unlink(f"/data/files/{file.path}")
        metadata_db.execute("DELETE FROM files WHERE id = ?", file.id)
```

---

### Snapshots - Detailni specifikace

**Ucel:** Point-in-time backup tabulky jako Parquet soubory.

**Rozhodnuti k uceneni:**

| Rozhodnuti | Moznosti | Doporuceni |
|------------|----------|------------|
| Snapshot ID format | UUID / timestamp_hash | **snap_{table}_{timestamp}** |
| Retention - manual | Forever / 30d / 90d | **90 dni** |
| Retention - auto | 7d / 30d | **7 dni** |
| Auto-snapshot trigger | DROP TABLE only / + TRUNCATE | **DROP TABLE only** |

**Snapshot Registry Schema (v metadata.duckdb):**
```sql
CREATE TABLE snapshots (
    id VARCHAR PRIMARY KEY,           -- snap_orders_20241215_143022
    project_id VARCHAR NOT NULL,
    bucket_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    snapshot_type VARCHAR NOT NULL,   -- manual | auto_predrop

    -- Snapshot data
    parquet_path VARCHAR NOT NULL,    -- relative path to parquet file
    row_count BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    schema_json JSON NOT NULL,        -- column definitions

    -- Lifecycle
    created_at TIMESTAMPTZ DEFAULT now(),
    created_by VARCHAR,
    expires_at TIMESTAMPTZ,           -- based on retention policy
    description TEXT,

    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE INDEX idx_snapshots_project ON snapshots(project_id);
CREATE INDEX idx_snapshots_table ON snapshots(project_id, bucket_name, table_name);
CREATE INDEX idx_snapshots_expires ON snapshots(expires_at);
```

**Directory Structure:**
```
/data/snapshots/
└── project_123/
    ├── snap_orders_20241215_143022/
    │   ├── metadata.json            # redundant copy for recovery
    │   └── data.parquet             # actual data
    └── snap_customers_20241214_091500/
        ├── metadata.json
        └── data.parquet
```

**API Endpoints:**

```
POST /projects/{id}/snapshots
Request: {
  "bucket": "in_c_sales",
  "table": "orders",
  "description": "Before major update"
}
Response: {
  "snapshot_id": "snap_orders_20241215_143022",
  "row_count": 50000,
  "size_bytes": 10485760,
  "expires_at": "2025-03-15T14:30:22Z"
}

GET /projects/{id}/snapshots
Query: ?bucket=in_c_sales&table=orders&limit=10
Response: {
  "snapshots": [...],
  "total": 15
}

GET /projects/{id}/snapshots/{snapshot_id}
Response: {
  "id": "snap_orders_20241215_143022",
  "bucket": "in_c_sales",
  "table": "orders",
  "row_count": 50000,
  "size_bytes": 10485760,
  "schema": [
    {"name": "id", "type": "BIGINT", "nullable": false},
    {"name": "amount", "type": "DECIMAL(10,2)", "nullable": true}
  ],
  "created_at": "2024-12-15T14:30:22Z",
  "expires_at": "2025-03-15T14:30:22Z"
}

POST /projects/{id}/snapshots/{snapshot_id}/restore
Request: {
  "target_bucket": "in_c_sales",      # optional, default = original
  "target_table": "orders_restored"   # optional, default = original
}
Response: {
  "restored_table": "in_c_sales.orders_restored",
  "row_count": 50000
}

DELETE /projects/{id}/snapshots/{snapshot_id}
Response: { "deleted": true }
```

**Auto-snapshot Implementation:**
```python
async def drop_table_with_snapshot(project_id: str, bucket: str, table: str):
    """Always create auto-snapshot before DROP TABLE."""
    # 1. Create snapshot
    snapshot_id = f"auto_predrop_{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    await create_snapshot(project_id, bucket, table, snapshot_id,
                         snapshot_type="auto_predrop")

    # 2. Drop table
    conn.execute(f"DROP TABLE {bucket}.{table}")

    # 3. Log operation
    metadata_db.log_operation(
        project_id=project_id,
        operation="drop_table",
        details={"bucket": bucket, "table": table, "snapshot_id": snapshot_id}
    )
```

**Retention Cleanup Job:**
```python
async def cleanup_expired_snapshots():
    """Run daily - delete snapshots past retention."""
    expired = metadata_db.query("""
        SELECT id, project_id, parquet_path FROM snapshots
        WHERE expires_at < now()
    """)
    for snap in expired:
        shutil.rmtree(f"/data/snapshots/{snap.project_id}/{snap.id}")
        metadata_db.execute("DELETE FROM snapshots WHERE id = ?", snap.id)
```

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
opentelemetry-api>=1.20.0          # Distributed tracing
opentelemetry-sdk>=1.20.0
opentelemetry-instrumentation-fastapi>=0.41b0
```

---

## Observability

> **Proc je observability dulezita od zacatku?**
> - Umoznuje rychle identifikovat problemy (kde a proc neco selhalo)
> - Request tracing propojuje PHP driver s Python API
> - AI asistenti (Claude Code) mohou analyzovat logy a navrhnout opravy
> - Metriky umoznuji sledovat vykon a kapacitu

### Structured Logging

```python
# src/logging_config.py
import structlog
from structlog.types import Processor

def setup_logging():
    """Configure structured logging for the application."""
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

# Pouziti v kodu:
logger = structlog.get_logger()

async def create_table(project_id: str, bucket: str, table: str):
    logger.info(
        "creating_table",
        project_id=project_id,
        bucket=bucket,
        table=table
    )
    try:
        # ... operace ...
        logger.info(
            "table_created",
            project_id=project_id,
            bucket=bucket,
            table=table,
            duration_ms=elapsed
        )
    except Exception as e:
        logger.error(
            "table_creation_failed",
            project_id=project_id,
            bucket=bucket,
            table=table,
            error=str(e),
            exc_info=True
        )
        raise
```

### Request ID Propagation

```python
# src/middleware/request_id.py
import uuid
from contextvars import ContextVar
from fastapi import Request
import structlog

request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")

class RequestIdMiddleware:
    async def __call__(self, request: Request, call_next):
        # Get or generate request ID
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request_id_ctx.set(request_id)

        # Bind to structlog context
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response
```

### Prometheus Metrics

```python
# src/metrics.py
from prometheus_client import Counter, Histogram, Gauge, generate_latest

# Request metrics
REQUEST_COUNT = Counter(
    "duckdb_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status"]
)

REQUEST_DURATION = Histogram(
    "duckdb_api_request_duration_seconds",
    "Request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]
)

# Write queue metrics
QUEUE_DEPTH = Gauge(
    "duckdb_write_queue_depth",
    "Number of operations waiting in write queue",
    ["project_id"]
)

QUEUE_WAIT_TIME = Histogram(
    "duckdb_write_queue_wait_seconds",
    "Time spent waiting in write queue",
    ["project_id"],
    buckets=[0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
)

# DuckDB metrics
DB_SIZE_BYTES = Gauge(
    "duckdb_database_size_bytes",
    "Database file size in bytes",
    ["project_id"]
)

ACTIVE_CONNECTIONS = Gauge(
    "duckdb_active_connections",
    "Number of active DuckDB connections",
    ["project_id", "mode"]  # mode: read/write
)

QUERY_DURATION = Histogram(
    "duckdb_query_duration_seconds",
    "Query execution duration",
    ["project_id", "query_type"],  # query_type: read/write
    buckets=[0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0]
)
```

### Log Format (JSON)

```json
{
  "timestamp": "2024-12-15T10:30:00.123Z",
  "level": "info",
  "event": "table_created",
  "request_id": "abc-123-def",
  "project_id": "456",
  "bucket": "in_c_sales",
  "table": "orders",
  "duration_ms": 45,
  "rows_affected": 1500
}
```

### Error Log Format

```json
{
  "timestamp": "2024-12-15T10:30:00.123Z",
  "level": "error",
  "event": "query_failed",
  "request_id": "abc-123-def",
  "project_id": "456",
  "sql": "INSERT INTO ...",
  "error": "UNIQUE constraint failed",
  "error_type": "ConstraintException",
  "traceback": "...",
  "duration_ms": 12
}
```

### PHP Driver Logging

```php
// V BaseHttpHandler.php
protected function callApi(string $method, string $endpoint, array $data = []): array
{
    $requestId = $this->generateRequestId();

    $this->logger->info('API call started', [
        'request_id' => $requestId,
        'method' => $method,
        'endpoint' => $endpoint,
        'project_id' => $data['project_id'] ?? null,
    ]);

    $startTime = microtime(true);

    try {
        $response = $this->httpClient->request($method, $endpoint, [
            'json' => $data,
            'headers' => ['X-Request-ID' => $requestId],
        ]);

        $this->logger->info('API call completed', [
            'request_id' => $requestId,
            'status' => $response->getStatusCode(),
            'duration_ms' => (microtime(true) - $startTime) * 1000,
        ]);

        return json_decode($response->getBody(), true);
    } catch (Exception $e) {
        $this->logger->error('API call failed', [
            'request_id' => $requestId,
            'error' => $e->getMessage(),
            'duration_ms' => (microtime(true) - $startTime) * 1000,
        ]);
        throw $e;
    }
}
```

### PHP Driver (StorageDriverDuckdb)
```json
{
    "name": "keboola/storage-driver-duckdb",
    "description": "Keboola DuckDB storage driver",
    "require": {
        "php": "^8.2",
        "google/protobuf": "^3.21",
        "keboola/storage-driver-common": "^7.8.0",
        "guzzlehttp/guzzle": "^7.0",
        "psr/log": "^1.1|^2.0|^3.0"
    },
    "require-dev": {
        "phpunit/phpunit": "^9.5",
        "phpstan/phpstan": "^1.8",
        "keboola/coding-standard": "^15.0"
    }
}
```

**Klicove zavislosti:**
- `google/protobuf` - Pro praci s protobuf messages
- `keboola/storage-driver-common` - Command/Response definice + ClientInterface
- `guzzlehttp/guzzle` - HTTP client pro komunikaci s Python API

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
├── bigquery-driver-research.md     # BigQuery driver analyza
├── zajca.md                        # Puvodni pozadavky
└── adr/
    ├── 001-duckdb-microservice-architecture.md
    ├── 002-duckdb-file-organization.md
    ├── 003-duckdb-branch-strategy.md
    ├── 004-duckdb-snapshots.md
    ├── 005-duckdb-write-serialization.md
    ├── 006-duckdb-on-prem-storage.md
    ├── 007-duckdb-cow-branching.md
    └── 008-central-metadata-database.md
```

---

## Budouci rozsireni (nice-to-have)

- [ ] MCP server pro AI agenty (Claude, GPT)
- [ ] Vector search extension (VSS) pro embeddings
- [ ] Delta Lake / Iceberg integrace
- [ ] Streaming import (Apache Arrow)
- [ ] Horizontal scaling (sharding per project groups)

---

## PREHLED ROZHODNUTI K UCINENI

> **Pred implementaci** je treba rozhodnout nasledujici body.
> Doporuceni jsou uvedena v detailnich specifikacich vyse.

### Kriticka rozhodnuti (blokuji implementaci)

| Oblast | Rozhodnuti | Schvaleno | Status |
|--------|------------|-----------|--------|
| **Write Queue** | Queue durability | In-memory | **APPROVED** |
| **Write Queue** | Max queue size | 1000 (konfigurovatelne) | **APPROVED** |
| **Import/Export** | Staging table location | Temp schema `_staging_{uuid}` | **APPROVED** |
| **Import/Export** | Deduplication strategy | INSERT ON CONFLICT | **APPROVED** |
| **Files API** | Upload mechanism | Multipart POST | **APPROVED** |
| **Files API** | Staging TTL | 24 hodin | **APPROVED** |
| **Snapshots** | Retention policy - manual | 90 dni | **APPROVED** |
| **Snapshots** | Retention policy - auto | 7 dni | **APPROVED** |

### Dulezita rozhodnuti (ovlivnuji design)

| Oblast | Rozhodnuti | Schvaleno | Status |
|--------|------------|-----------|--------|
| **Write Queue** | Priority levels | Normal + High | **APPROVED** |
| **Import/Export** | Incremental mode | **Full MERGE** (INSERT/UPDATE/DELETE) | **APPROVED** |
| **Files API** | Max file size | 10GB | **APPROVED** |
| **Files API** | File quotas per project | 10000 files, 1TB | **APPROVED** |
| **Snapshots** | Auto-snapshot trigger | **Per-projekt konfigurovatelne**, default pouze DROP TABLE | **APPROVED** |

### Volitelna rozhodnuti (lze odlozit)

| Oblast | Rozhodnuti | Schvaleno | Status |
|--------|------------|-----------|--------|
| **Security** | API key authentication | **Hierarchicky model** (viz nize) | **APPROVED** |
| **Security** | Rate limiting | Per project | DEFERRED |
| **Observability** | Metrics backend | Prometheus (od zacatku) | **APPROVED** |
| **Files API** | Encryption at rest | Optional AES-256 | DEFERRED |

### Autentizacni model (APPROVED)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    HIERARCHICKY API KEY MODEL                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ADMIN_API_KEY (v ENV - duckdb-api-service)                         │
│  └── Opravneni: POST /projects (vytvorit projekt)                   │
│  └── Ulozeni: Environment variable                                  │
│                                                                      │
│  PROJECT_ADMIN_API_KEY (vracen pri POST /projects)                  │
│  └── Opravneni: VSE v ramci projektu                                │
│  └── Ulozeni: Storage API (PHP) si ho ulozi                         │
│  └── Format: proj_{project_id}_admin_{random}                       │
│                                                                      │
│  PROJECT_API_KEY (vytvoreno v projektu - budouci rozsireni)         │
│  └── Opravneni: VSE v ramci projektu (zatim full access)            │
│  └── Ulozeni: metadata.duckdb tabulka api_keys                      │
│  └── Zaklad pro budouci RBAC (role-based access control)            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

Autentizace flow:
1. Request prijde s headerem: Authorization: Bearer <api_key>
2. API parsuje key format:
   - Zacina "admin_" → ADMIN_API_KEY (overit vuci ENV)
   - Zacina "proj_" → PROJECT_API_KEY (overit vuci DB)
3. Extrahuje project_id z URL a overi opravneni
4. Pokud OK → pokracuj, jinak 401/403
```

### Auto-snapshot triggers (APPROVED - UPDATED)

**Konfigurovatelna politika per-projekt:**

```python
# Default politika (konzervativni)
default_snapshot_policy = {
    "drop_table": True,      # Vzdy snapshot pred DROP TABLE
    "truncate_table": False, # Volitelne
    "delete_rows": False,    # Volitelne
    "drop_column": False,    # Volitelne
}

# Agresivni politika (maximalni ochrana)
aggressive_snapshot_policy = {
    "drop_table": True,
    "truncate_table": True,
    "delete_rows": True,
    "drop_column": True,
}
```

**Default:** Snapshot pouze pred `DROP TABLE`

**Volitelne triggery (lze zapnout per-projekt):**
- `TRUNCATE TABLE`
- `DELETE FROM table`
- `ALTER TABLE DROP COLUMN`

**Monitoring:**
- Metrika: `snapshot_storage_bytes{project_id="X"}`
- Alert: kdyz snapshot storage > 80% limitu
- Dashboard: prehled snapshot storage per project

> **Poznamka:** Snapshot se nevytvari pro: INSERT, UPDATE, SELECT, CREATE.

### Akceptovana rizika MVP

| Riziko | Popis | Mitigace | Status |
|--------|-------|----------|--------|
| **Cross-DB konzistence** | metadata.duckdb a project.duckdb nemohou byt v jedne transakci | Stats jsou jen cache, truth-of-existence je v project DB, prepocet on-demand | **ACCEPTED** |
| **Write Queue volatilita** | In-memory fronta se ztrati pri padu | Klient ceka na odpoved, retry je na strane Keboola Storage API, pridame idempotency middleware | **ACCEPTED** |
| **Single FastAPI instance** | DuckDB single-writer neumoznuje vice zapisovacu | MVP bezi na 1 instanci, HA reseni (leader election, sticky sessions) az pro enterprise | **ACCEPTED** |
| **Jednoduchy auth model** | Staticke API keys bez rotace, per-user scopes, mTLS | Pro MVP staci hierarchicky model (ADMIN + PROJECT keys), rozsireni az pro enterprise | **ACCEPTED** |
| **Bucket sharing bez DB-level ACL** | DuckDB nema per-schema pristupova prava | App-layer enforcement (API auth), filesystem permissions (700) na .duckdb soubory | **ACCEPTED** |
| **Dev branches full copy** | Kazdy branch = plna kopie project DB | Pro MVP OK, CoW optimalizace (ADR-007) pozdeji | **ACCEPTED** |
| **Bez DR/Backup v API** | Zadne backup/restore endpointy | Dokumentovat doporuceni (filesystem snapshots), implementace post-MVP | **ACCEPTED** |
| **Bez encryption at rest** | DuckDB soubory nesifrovane | Filesystem-level sifrovani (LUKS), implementace post-MVP | **ACCEPTED** |
| **Schema migrations** | metadata.duckdb schema se muze menit | Verzovani v DB (`schema_version` tabulka) + migrace pri startu FastAPI | **ACCEPTED** |

**Cross-DB konzistence - detaily:**

```
metadata.duckdb obsahuje:
├── projects          → KRITICKE (registr projektu)
├── bucket_shares     → KRITICKE (sharing relace)
├── bucket_links      → KRITICKE (linking relace)
├── files             → KRITICKE (file registry)
├── operations_log    → NEKRITRICKE (audit, ztrata = ok)
└── stats (v projects)→ NEKRITRICKE (cache, prepocitat z project DB)

Pokud zapis do project DB uspeje ale metadata selze:
- Stats budou zastarale → prepocitat pri GET /projects/{id}/stats
- Audit log bude chybet → akceptovatelne pro MVP
```

**Idempotency middleware - implementace:**

```
POST /query
Headers:
  X-Idempotency-Key: <uuid>

- Ulozit vysledek operace pod klicem (TTL 5-10 min)
- Pri retry vratit ulozeny vysledek
- Ulozeni: in-memory dict nebo metadata.duckdb
```

### Jak postupovat

1. **Schvalit doporuceni** - projdi tabulku a potvrdi/uprav doporuceni
2. **Oznacit jako APPROVED** - zmenit status na APPROVED
3. **Zacit implementaci** - implementovat podle schvalenych rozhodnuti

### Priklad schvaleni

```markdown
| **Write Queue** | Queue durability | In-memory | **APPROVED** |
```

---

## CHANGELOG

| Verze | Datum | Zmeny |
|-------|-------|-------|
| **v6.6** | **2024-12-16** | **Prometheus /metrics:** Endpoint implementovan, metriky pro requesty, DB operace, table locks. 180 testu PASS (15 novych). |
| v6.5 | 2024-12-16 | **Idempotency Middleware:** X-Idempotency-Key header s TTL 10 min, background cleanup. 165 testu PASS (21 novych). |
| v6.4 | 2024-12-16 | **Auth + Write Queue:** Hierarchicky API key model (ADMIN + PROJECT keys), TableLockManager pro per-table mutex. 144 testu PASS (46 novych). |
| v6.1 | 2024-12-16 | **ADR-009 IMPLEMENTED:** Refaktor dokoncen - projekt=adresar, bucket=adresar, tabulka=soubor. 98 testu PASS. |
| v6.0 | 2024-12-16 | ADR-009 ACCEPTED: Zmena architektury na 1 DuckDB soubor per tabulka. Validovano Codex GPT-5 (4096 ATTACH test OK). ADR-002 superseded. |
| v5.3 | 2024-12-16 | GPT-5 second opinion review - 11 bodu zpracovano, akceptovana rizika rozsirena, auto-snapshot policy zmenena na per-projekt konfigurovatelnou |
| v5.2 | 2024-12-15 | Pridana sekce "Akceptovana rizika MVP" - cross-DB konzistence, idempotency middleware |
| v5.1 | 2024-12-15 | Schvalena vsechna rozhodnuti, hierarchicky auth model, Full MERGE, auto-snapshots |
| v5 | 2024-12-15 | Aktualizace stavu, nova strategie (Python first), detailni specifikace |
| v4 | 2024-12-14 | Bucket/Table CRUD implementace |
| v3 | 2024-12-13 | Project CRUD, ADR-008 |
| v2 | 2024-12-12 | BigQuery driver research |
| v1 | 2024-12-11 | Initial plan |
