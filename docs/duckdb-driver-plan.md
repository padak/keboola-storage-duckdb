# DuckDB Storage Backend pro Keboola - Implementacni plan v4

> **Cil:** On-premise Keboola bez Snowflake a bez S3

---

## AKTUALNI STAV (2024-12-15)

### Co je hotovo

| Krok | Status | Poznamka |
|------|--------|----------|
| Lokalni Connection setup | DONE | Bezi na https://localhost:8700 |
| S3 File Storage | DONE | `padak-kbc-services-s3-files-storage-bucket` |
| GCS File Storage | DONE | `kbc-padak-files-storage` |
| Snowflake Backend | DONE | `vceecnu-bz34672.snowflakecomputing.com` |
| BigQuery Backend | DONE | GCP folder `393339196668` |
| BigQuery driver studium | **IN PROGRESS** | Viz sekce "Poznatky z BigQuery driveru" |

### Kde jsme

```
[DONE] Rozjet lokalni Connection
       ↓
[DONE] Pridat BigQuery backend (referencni implementace)
       ↓
[NOW]  Studovat BigQuery driver kod
       ↓
[NEXT] Implementovat DuckDB driver
```

### Dalsi kroky (prioritizovane)

1. **Prostudovat BigQuery driver kod** (`vendor/keboola/storage-driver-bigquery/`)
   - [ ] Jak funguje `InitBackendHandler`
   - [ ] Jak funguje `CreateProjectHandler`
   - [ ] Jak funguje `CreateTableHandler`
   - [ ] Jak funguje `ImportTableFromFileHandler`

2. **Vytvorit DuckDB API Service skeleton**
   - [ ] FastAPI app s `/health` endpoint
   - [ ] Docker + docker-compose
   - [ ] Zakladni projekt struktura

3. **Implementovat PHP Driver Package**
   - [ ] `DuckdbDriverClient` (implements `ClientInterface`)
   - [ ] `HandlerFactory` pro dispatch commands
   - [ ] Prvni handlery: `InitBackend`, `CreateProject`

---

## Poznatky z BigQuery driveru (2024-12-15)

### Architektura BigQuery driveru

BigQuery driver nam ukazuje jak Keboola drivery funguji:

```
Connection (PHP)
    │
    │ DriverClientFactory::getClientForBackend('bigquery')
    ▼
BigQueryDriverClient (implements ClientInterface)
    │
    │ runCommand(credentials, command, features, runtimeOptions)
    │              ↑ Protocol Buffers messages ↑
    ▼
HandlerFactory::create($command)
    │
    ├── InitBackendCommand → InitBackendHandler
    ├── CreateProjectCommand → CreateProjectHandler
    ├── CreateTableCommand → CreateTableHandler
    └── ... (33+ handlers)
```

### Klicove poznatky pro DuckDB

| Aspekt | BigQuery | DuckDB (plan) |
|--------|----------|---------------|
| **Kde bezi driver** | PHP knihovna v Connection | PHP knihovna + Python microservice |
| **Storage per projekt** | GCP projekt | DuckDB soubor |
| **InitBackend validace** | GCP permissions, billing | Jen health check Python API |
| **Cloud dependencies** | GCS, BigQuery API, IAM | Zadne |
| **Komplexita** | Vysoka (GCP ekosystem) | Nizka (vse lokalne) |

### BigQuery InitBackendHandler - co kontroluje

```php
// Z vendor/keboola/storage-driver-bigquery/src/Handler/Backend/Init/InitBackendHandler.php
// Driver kontroluje:
1. Folder access (folders.get, folders.list)
2. Project creation (projects.create)
3. IAM permissions (projects.getIamPolicy)
4. Required roles (roles/owner, roles/storage.objectAdmin)
5. Billing account (billing.user)
```

**Pro DuckDB:** Nas `InitBackendHandler` bude MNOHEM jednodussi - jen ping Python API.

### Proc Python microservice misto pure PHP?

BigQuery driver ukazuje, ze drivery mohou volat externi sluzby. Pro DuckDB:

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

| ADR | Rozhodnuti |
|-----|------------|
| 001 | Python microservice misto PHP FFI driver |
| 002 | 1 projekt = 1 DuckDB soubor, bucket = schema |
| 003 | Dev branches = separate DuckDB files (nahrazeno ADR-007) |
| 004 | Snapshoty = Parquet export |
| 005 | Write serialization = async fronta per projekt |
| 006 | Storage Files = lokalni filesystem + metadata v DuckDB |
| 007 | Copy-on-Write branching = lazy table-level copy |

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

### Faze 0: PHP Driver Package Setup (NOVA - KRITICKA)
> **Tato faze je nutna pro integraci do Connection!**

> **POZOR na protobuf kontrakt:** Pred implementaci handleru zkontroluj skutecne
> `storage-driver-common` definice. Implementuj pouze handlery pro commands,
> ktere existuji v `Keboola\StorageDriver\Command\*` namespace.
> Vyhni se generovani "mrtvych" handleru pro neexistujici commands.

- [ ] Vytvorit `connection/Package/StorageDriverDuckdb/` strukturu
- [ ] Vytvorit composer.json se zavislostmi:
  ```json
  {
      "require": {
          "php": "^8.2",
          "google/protobuf": "^3.21",
          "keboola/storage-driver-common": "^7.8.0",
          "guzzlehttp/guzzle": "^7.0"
      }
  }
  ```
- [ ] Implementovat `DuckdbDriverClient` (implements ClientInterface)
- [ ] Implementovat `DuckdbApiClient` (HTTP client pro Python API)
- [ ] Implementovat `HandlerFactory` (dispatch commands na handlery)
- [ ] Implementovat `BaseHttpHandler` (spolecna logika pro HTTP volani)
- [ ] Registrovat driver v `DriverClientFactory` v Connection
- [ ] Vytvorit services.yaml

### Faze 1: Zaklad API + Backend + Observability
- [ ] FastAPI app s healthcheck
- [ ] DuckDB connection manager
- [ ] Docker + docker-compose
- [ ] Zakladni konfigurace (ENV)
- [ ] Python: POST /backend/init
- [ ] Python: POST /backend/remove
- [ ] PHP: InitBackendHandler
- [ ] PHP: RemoveBackendHandler
- [ ] E2E test: Connection -> Driver -> Python API
- [ ] **Observability zaklad:**
  - [ ] Structured logging (structlog) - JSON format pro parsovani
  - [ ] Request ID middleware (X-Request-ID propagace)
  - [ ] Request/response logging s timing
  - [ ] Error logging s full traceback

### Faze 2: Project operace + Metrics + Security
- [ ] POST /projects (vytvorit DuckDB soubor)
- [ ] PUT /projects/{id} (update metadata)
- [ ] DELETE /projects/{id} (smazat soubor)
- [ ] GET /projects/{id}/info
- [ ] **Observability rozsireni:**
  - [ ] Prometheus metrics endpoint (/metrics)
  - [ ] Metriky: request_count, request_duration, queue_depth, active_connections
  - [ ] DuckDB-specific metriky: db_size_bytes, table_count, query_duration
- [ ] **Security PHP <-> Python:**
  - [ ] API key autentizace (shared secret v ENV)
  - [ ] Rate limiting per project (prevence DoS)
  - [ ] Optional: mTLS pro produkci

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

### Faze 10: Storage Files (on-prem) + Lifecycle
- [ ] File metadata schema v DuckDB
- [ ] POST /files/prepare (staging path)
- [ ] POST /files/upload
- [ ] GET /files/{id}
- [ ] DELETE /files/{id}
- [ ] Staging directory management
- [ ] Cleanup stale staging files
- [ ] **File Lifecycle Management:**
  - [ ] Staging cleanup (auto-delete after 24h)
  - [ ] Kvoty per projekt (max files, max size)
  - [ ] Checksum validace (MD5/SHA256 pri uploadu)
  - [ ] Optional encryption at rest (AES-256)
- [ ] **Backup/DR:**
  - [ ] Backup strategie pro /data/files/
  - [ ] Point-in-time recovery plan
  - [ ] Dokumentace DR procedur

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

### Faze A: Studium BigQuery driveru (AKTUALNE)

> **Proc:** BigQuery driver je referencni implementace. Pochopenim jeho kodu
> ziskame jasny vzor pro DuckDB driver.

1. [ ] Prostudovat `InitBackendHandler` - jak driver validuje backend
2. [ ] Prostudovat `CreateProjectHandler` - jak se vytvari projekt
3. [ ] Prostudovat `CreateTableHandler` - jak se vytvari tabulka
4. [ ] Prostudovat `ImportTableFromFileHandler` - jak funguje import
5. [ ] Zdokumentovat klicove patterns a helpers

**Kde hledat:**
```
vendor/keboola/storage-driver-bigquery/src/
├── BigQueryDriverClient.php          # Entry point
├── Handler/
│   ├── Backend/Init/InitBackendHandler.php
│   ├── Project/CreateProjectHandler.php
│   ├── Table/CreateTableHandler.php
│   └── Table/Import/ImportTableFromFileHandler.php
└── ...
```

### Faze B: DuckDB API Service Skeleton

1. [ ] Vytvorit `duckdb-api-service/` strukturu
2. [ ] Implementovat FastAPI app + `/health` endpoint
3. [ ] Docker + docker-compose
4. [ ] Zakladni konfigurace (ENV)

### Faze C: PHP Driver Package

1. [ ] Vytvorit `connection/Package/StorageDriverDuckdb/`
2. [ ] `DuckdbDriverClient` (implements `ClientInterface`)
3. [ ] `DuckdbApiClient` (HTTP client)
4. [ ] `HandlerFactory` pro dispatch
5. [ ] Prvni handlery: `InitBackend`, `RemoveBackend`

### Faze D: Zakladni funkcionalita

1. [ ] Pridat DuckDB connection manager
2. [ ] Implementovat write queue (ADR-005)
3. [ ] Endpointy: `/backend/init`, `/projects`
4. [ ] E2E test: Connection -> PHP Driver -> Python API -> DuckDB

---

## Budouci rozsireni (nice-to-have)

- [ ] MCP server pro AI agenty (Claude, GPT)
- [ ] Vector search extension (VSS) pro embeddings
- [ ] Delta Lake / Iceberg integrace
- [ ] Streaming import (Apache Arrow)
- [ ] Horizontal scaling (sharding per project groups)
