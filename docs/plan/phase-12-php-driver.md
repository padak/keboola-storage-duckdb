# Phase 12: PHP Driver

## Status

### Phase 12a: Python gRPC Server - DONE
- **Status:** DONE (2024-12-20)
- **Tests:** 17 (gRPC handlers, servicer, integration)
- **Implementation:** `src/grpc/` module with handlers for InitBackend, RemoveBackend, CreateProject, DropProject

### Phase 12a.1: HTTP Driver Bridge - DONE
- **Status:** DONE (2024-12-20)
- **Implementation:** `src/routers/driver.py` - HTTP endpoint for driver commands
- **Endpoint:** `POST /driver/execute` (supports admin + project API keys)
- **Why:** PHP nepotrebuje gRPC extension - vola HTTP endpoint s JSON

### Phase 12b: Connection Backend Registration - DONE
- **Status:** DONE (2024-12-20)
- **Implementation:** Registered DuckDB as storage backend and file storage provider in Connection

### Phase 12b.1: Connection Full Integration - WORKING
- **Status:** WORKING (2024-12-21)
- **Goal:** Make DuckDB fully usable from Connection UI/API
- **Result:** SUCCESS - Table creation via Connection works end-to-end!
- **Verified:** `in.c-test.orders` table created via Storage API, visible in Connection
- **See:** Detailed notes below in "Phase 12b.1 Integration Notes"

### Phase 12b.2: Secure Project API Keys - DONE
- **Status:** DONE (2024-12-21)
- **Goal:** Implement proper API key architecture for security isolation
- **Implementation:** Project-specific API keys stored in `bi_connectionsCredentials`
- **See:** "Secure API Key Architecture" section below

**What Works (2024-12-21):**
- Create DuckDB project via Manage API (`defaultBackend: duckdb`)
- Create Storage API token for DuckDB project
- Create bucket with DuckDB backend
- **CREATE TABLE via CSV upload** - Connection calls DuckDB API via HTTP bridge
- List tables shows created tables
- **Secure project isolation** - Each project has its own API key

### Phase 12c: Core gRPC Handlers - DONE
- **Status:** DONE (2024-12-21)
- **Tests:** 23 (handler tests + servicer integration + HTTP bridge)
- **Implementation:** 8 new handlers in `src/grpc/handlers/`:
  - `bucket.py`: CreateBucketHandler, DropBucketHandler
  - `table.py`: CreateTableHandler, DropTableHandler, PreviewTableHandler
  - `info.py`: ObjectInfoHandler
  - `import_export.py`: TableImportFromFileHandler, TableExportToFileHandler
- **Detailed plan:** [phase-12c-core-handlers.md](phase-12c-core-handlers.md)

### Phase 12d: Schema Handlers - DONE
- **Status:** DONE (2024-12-21)
- **Tests:** 18 (handler tests + servicer integration + HTTP bridge)
- **Implementation:** 6 new handlers in `src/grpc/handlers/schema.py`:
  - `AddColumnHandler`: Add column to table
  - `DropColumnHandler`: Drop column from table
  - `AlterColumnHandler`: Alter column (rename, type change)
  - `AddPrimaryKeyHandler`: Add primary key constraint
  - `DropPrimaryKeyHandler`: Drop primary key constraint
  - `DeleteTableRowsHandler`: Delete rows based on filters

### Phase 12e: Workspace Handlers - DONE
- **Status:** DONE (2024-12-21)
- **Tests:** 17 (handler tests + servicer integration)
- **HTTP Bridge:** Updated with 6 workspace commands (26 total)
- **Implementation:** 8 new handlers in `src/grpc/handlers/workspace.py`:
  - `CreateWorkspaceHandler`: Create workspace with credentials
  - `DropWorkspaceHandler`: Drop workspace
  - `ClearWorkspaceHandler`: Clear all objects from workspace
  - `ResetWorkspacePasswordHandler`: Reset workspace password
  - `DropWorkspaceObjectHandler`: Drop single object from workspace
  - `GrantWorkspaceAccessToProjectHandler`: Grant access (DuckDB no-op, logged)
  - `RevokeWorkspaceAccessToProjectHandler`: Revoke access (DuckDB no-op, logged)
  - `LoadTableToWorkspaceHandler`: Load table from project to workspace
- **Verified:** HTTP bridge tested with CreateWorkspace, returns credentials

### Phase 12f-g: Additional Handlers - TODO
- **Status:** Not Started
- **Phase 12f:** Sharing handlers (ShareBucket, UnshareBucket, LinkBucket, UnlinkBucket, Grant/RevokeReadOnly)
- **Phase 12g:** Advanced (DevBranch, ExecuteQuery)

---

## Secure API Key Architecture (Phase 12b.2)

### Problem

Original implementation used a single `ADMIN_API_KEY` for all operations. This was a security risk:
- Any code with admin key could access any project
- No isolation between projects
- Attack vector: malicious code could pass wrong `project_id` and access other projects' data

### Solution

Implemented hierarchical API key model matching other backends (Snowflake, BigQuery):

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. CreateProject Flow (uses ADMIN_API_KEY)                          │
│                                                                     │
│   Connection                        DuckDB API                      │
│   ─────────────────────────────────────────────────────────────    │
│   BackendAssign::assignBackendToProject()                           │
│         │                                                           │
│         │ POST /driver/execute                                      │
│         │ Auth: ADMIN_API_KEY                                       │
│         │ Body: CreateProjectCommand                                │
│         ▼                                                           │
│   DuckDB API creates project        CreateProjectResponse           │
│         │                           - projectPassword = API_KEY     │
│         │                                                           │
│   Connection stores in DB:                                          │
│   - bi_connectionsCredentials.password = encrypted(API_KEY)         │
│   - bi_projects.idDuckdbCredentials = credentials.id                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 2. Project Operations (uses PROJECT_API_KEY)                        │
│                                                                     │
│   Connection                        DuckDB API                      │
│   ─────────────────────────────────────────────────────────────    │
│   DuckDBCredentialsResolver::getProjectCredentials()                │
│         │                                                           │
│         │ Reads from bi_connectionsCredentials                      │
│         │ Decrypts password -> project API key                      │
│         ▼                                                           │
│   DuckDBDriverClient::runCommand()                                  │
│         │                                                           │
│         │ POST /driver/execute                                      │
│         │ Auth: PROJECT_API_KEY (from credentials.secret)           │
│         │ Body: {command, credentials: {project_id}}                │
│         ▼                                                           │
│   DuckDB API verifies:                                              │
│   - API key is valid for this project_id                            │
│   - Rejects if key doesn't match project_id in request              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### PHP Files Modified (Phase 12b.2)

| File | Change |
|------|--------|
| `legacy-app/sql/migrations/accounts/Version20251221150000.php` | **NEW** Migration adding `idDuckdbCredentials` to `bi_projects` |
| `legacy-app/application/modules/core/models/Projects.php` | Added `DuckdbCredentials` to `$_referenceMap` |
| `legacy-app/application/modules/core/models/Row/Project.php` | Added `@property idDuckdbCredentials`, `getDuckdbCredentials()`, `hasDuckdbActivated()`, `assignDuckdbBackend()`, `removeDuckdbBackend()` |
| `legacy-app/application/src/Storage/Service/Backend/Assign/BackendAssign.php` | Added `BACKEND_DUCKDB` case in switch for `assignBackendToProject()` |
| `legacy-app/application/src/Storage/Service/Backend/CredentialsResolver/DuckDBCredentialsResolver.php` | Rewritten to read credentials from DB, decrypt API key, return in `GenericBackendCredentials.secret` |
| `Package/StorageDriverDuckdb/src/DuckDBDriverClient.php` | Updated to use `credentials.secret` as API key when available, fallback to admin key |

### Python Files Modified (Phase 12b.2)

| File | Change |
|------|--------|
| `duckdb-api-service/src/dependencies.py` | Added `require_driver_auth()` (accepts admin + project keys), `get_project_id_from_driver_key()` |
| `duckdb-api-service/src/routers/driver.py` | Changed from `require_admin` to `require_driver_auth`, added project_id validation |

### Migration for Project Credentials

```
connection/legacy-app/sql/migrations/accounts/Version20251221150000.php
```

This migration adds:
```sql
ALTER TABLE bi_projects ADD COLUMN idDuckdbCredentials INT UNSIGNED DEFAULT NULL;
ALTER TABLE bi_projects ADD KEY idDuckdbCredentials (idDuckdbCredentials);
ALTER TABLE bi_projects ADD CONSTRAINT bi_projects_ibfk_duckdb_credentials
  FOREIGN KEY (idDuckdbCredentials) REFERENCES bi_connectionsCredentials (id);
```

Run with:
```bash
docker compose run --rm cli ./migrations.sh migrations:migrate --no-interaction
```

---

## Phase 12b Implementation Details (Connection Side)

### Storage Backend Registration

**Modified Files:**

| File | Change |
|------|--------|
| `Package/StorageBackend/src/BackendSupportsInterface.php` | Added `BACKEND_DUCKDB = 'duckdb'`, added to `SUPPORTED_BACKENDS` and `DEV_BRANCH_SUPPORTED_BACKENDS` |
| `Package/StorageBackend/src/Driver/Config/DuckDBConfig.php` | **NEW** - implements `DriverConfigInterface` |
| `Package/StorageBackend/src/Driver/DriverClientFactory.php` | Added DuckDB cases in `getClientForBackend`, `getConfigForBackend`, `supportsBackend` |
| `Package/StorageBackend/src/CommonBackendConfigurationFactory.php` | Added match for `BACKEND_DUCKDB` |
| `Package/StorageBackend/services.yaml` | Registered `DuckDBDriverClient` with env vars |
| `storage-backend/.../BackendSupportsInterface.php` | Synced with connection |

**New Package: `Package/StorageDriverDuckdb/`**

```
connection/Package/StorageDriverDuckdb/
├── composer.json
└── src/
    ├── DuckDBDriverClient.php      # HTTP bridge to DuckDB service
    └── Exception/
        └── DuckDBDriverException.php
```

### File Storage Provider Registration

**Modified Files:**

| File | Change |
|------|--------|
| `src/Manage/FileStorage/Entity/FileStorage.php` | Added `PROVIDER_DUCKDB = 'duckdb'`, updated `getProviderInstance` |
| `legacy-app/.../Row/FileStorage.php` | Added `PROVIDER_DUCKDB`, updated `toApiResponse`, `getFilesBucketForCurrentProvider`, `getProviderInstance` |

**Key Decision:** DuckDB uses S3-compatible API, so `getProviderInstance()` returns `S3Provider` for DuckDB.

### Environment Variables

Connection needs these env vars to communicate with DuckDB service:

```bash
DUCKDB_SERVICE_URL=http://duckdb-service:8000
DUCKDB_ADMIN_API_KEY=your-admin-api-key
```

**How to add in Connection:**

1. **docker-compose.yml** (for local dev):
   ```yaml
   services:
     apache:
       environment:
         DUCKDB_SERVICE_URL: http://duckdb-service:8000
         DUCKDB_ADMIN_API_KEY: ${DUCKDB_ADMIN_API_KEY}
   ```

2. **.env file** (Connection root):
   ```bash
   DUCKDB_SERVICE_URL=http://duckdb-service:8000
   DUCKDB_ADMIN_API_KEY=test-admin-key-change-in-production
   ```

3. **Symfony config** (`config/services.yaml`):
   ```yaml
   parameters:
     env(DUCKDB_SERVICE_URL): 'http://localhost:8000'
     env(DUCKDB_ADMIN_API_KEY): ''
   ```

## Phase 12a Implementation Details (Python Side)

### Created Files

**gRPC Module (`src/grpc/`):**
- `__init__.py` - Module init
- `utils.py` - `LogMessageCollector`, `get_type_name()` utilities
- `handlers/base.py` - `BaseCommandHandler` abstract class
- `handlers/backend.py` - `InitBackendHandler`, `RemoveBackendHandler`
- `handlers/project.py` - `CreateProjectHandler`, `DropProjectHandler`
- `handlers/__init__.py` - Handler exports
- `servicer.py` - `StorageDriverServicer` (routes commands to handlers)
- `server.py` - Standalone gRPC server

**Unified Server:**
- `src/unified_server.py` - REST (port 8000) + gRPC (port 50051) in single process

**Proto/Generated:**
- `proto/*.proto` - 10 proto files from storage-backend
- `generated/proto/*.py` - 22 generated Python protobuf files

**Tests:**
- `tests/test_grpc_server.py` - 17 tests

### Verified Commands

**gRPC (port 50051):**
```bash
# InitBackendCommand
grpcurl -plaintext -import-path . -proto proto/service.proto \
  -proto proto/common.proto -proto proto/backend.proto \
  -d '{"command": {"@type": "type.googleapis.com/keboola.storageDriver.command.backend.InitBackendCommand"}}' \
  localhost:50051 keboola.storageDriver.service.StorageDriverService/Execute

# CreateProjectCommand
grpcurl -plaintext -import-path . -proto proto/service.proto \
  -proto proto/common.proto -proto proto/project.proto \
  -d '{"command": {"@type": "type.googleapis.com/keboola.storageDriver.command.project.CreateProjectCommand", "projectId": "test-123"}}' \
  localhost:50051 keboola.storageDriver.service.StorageDriverService/Execute
```

**HTTP Bridge (port 8000) - pro PHP driver:**
```bash
# InitBackendCommand (requires admin key)
curl -X POST http://localhost:8000/driver/execute \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"command": {"type": "InitBackendCommand"}}'

# CreateProjectCommand (requires admin key)
curl -X POST http://localhost:8000/driver/execute \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"command": {"type": "CreateProjectCommand", "projectId": "test-123"}}'

# CreateBucketCommand (requires project key OR admin key)
curl -X POST http://localhost:8000/driver/execute \
  -H "Authorization: Bearer $PROJECT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"command": {"type": "CreateBucketCommand", "projectId": "test-123", "bucketId": "in.c-sales"}, "credentials": {"project_id": "test-123"}}'

# List supported commands
curl http://localhost:8000/driver/commands
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    KEBOOLA CONNECTION (PHP)                          │
│                                                                      │
│  Services (TableInfoService, ImportService, ...)                     │
│         │                                                            │
│         ▼                                                            │
│  DriverClientFactory::getClientForBackend('duckdb')                  │
│         │                                                            │
│         ▼                                                            │
│  DuckDBCredentialsResolver::getProjectCredentials()                  │
│         │                                                            │
│         │ Reads from bi_connectionsCredentials                       │
│         │ Returns: host=project_id, secret=decrypted_api_key         │
│         ▼                                                            │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  DuckDBDriverClient (HTTP Bridge)                             │   │
│  │         │                                                     │   │
│  │         │ HTTP POST /driver/execute                           │   │
│  │         │                                                     │   │
│  │         │ Headers:                                            │   │
│  │         │   Authorization: Bearer $PROJECT_API_KEY            │   │
│  │         │   (or $ADMIN_API_KEY for admin commands)            │   │
│  │         │                                                     │   │
│  │         │ Body:                                               │   │
│  │         │   credentials: {project_id: "..."}                  │   │
│  │         │                                                     │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                      │                                               │
│                      ▼                                               │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  DuckDB API Service (Python)                                  │   │
│  │                                                               │   │
│  │  /driver/execute endpoint:                                    │   │
│  │  1. Validates API key (admin or project key)                  │   │
│  │  2. For project keys: verifies key matches project_id         │   │
│  │  3. Routes to gRPC handler                                    │   │
│  │                                                               │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐           │   │
│  │  │ gRPC Server │  │  REST API   │  │  S3 Compat  │           │   │
│  │  │   :50051    │  │   :8000     │  │   :8000/s3  │           │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘           │   │
│  │         │                │                │                   │   │
│  │         └────────────────┼────────────────┘                   │   │
│  │                          ▼                                    │   │
│  │                 Services Layer                                │   │
│  │                          │                                    │   │
│  │                          ▼                                    │   │
│  │                      DuckDB                                   │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

**Key Decisions:**
- HTTP bridge instead of gRPC - PHP doesn't need gRPC extension
- S3-compatible API for file uploads
- DuckDB registered as both storage backend AND file storage provider
- **Project-specific API keys** for security isolation (same pattern as Snowflake/BigQuery)

## DuckDBDriverClient Implementation

```php
<?php
declare(strict_types=1);

namespace Keboola\StorageDriver\DuckDB;

use Google\Protobuf\Internal\Message;
use GuzzleHttp\Client;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

class DuckDBDriverClient implements ClientInterface
{
    private string $serviceUrl;
    private string $adminApiKey;

    public function __construct(string $serviceUrl, string $adminApiKey)
    {
        $this->serviceUrl = rtrim($serviceUrl, '/');
        $this->adminApiKey = $adminApiKey;
    }

    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);

        // Use project API key from credentials.secret if available
        // Otherwise fall back to admin key (for CreateProject, etc.)
        $apiKey = $credentials->getSecret();
        if ($apiKey === '') {
            $apiKey = $this->adminApiKey;
        }

        $httpClient = new Client([
            'base_uri' => $this->serviceUrl,
            'timeout' => 300,
            'headers' => [
                'Content-Type' => 'application/json',
                'Authorization' => 'Bearer ' . $apiKey,
            ],
        ]);

        // Build request and call POST /driver/execute
        // Include project_id in credentials for path resolution
    }
}
```

## Protocol Buffers Commands (33+)

| Category | Commands |
|----------|----------|
| Backend | InitBackend, RemoveBackend |
| Project | Create, Update, Drop, CreateDevBranch, DropDevBranch |
| Bucket | Create, Drop, Share, Unshare, Link, Unlink, Grant/Revoke ReadOnly |
| Table | Create, Drop, AddColumn, DropColumn, AlterColumn, Add/DropPrimaryKey, DeleteRows |
| Import/Export | ImportFromFile, ImportFromTable, ExportToFile |
| Info | ObjectInfo, PreviewTable, ProfileTable |
| Workspace | Create, Drop, Clear, DropObject, ResetPassword |
| Query | ExecuteQuery |

## Phase 12b.1 Integration Notes (2024-12-21)

### What We Achieved

1. **Created DuckDB project via API** (ID: 5, "DuckDB Test Project")
2. **Created DuckDB bucket** (`in.c-test` with `backend: duckdb`)
3. **Storage API token works** for the project

### Direct SQL Changes Required

Connection's MySQL database needs these changes:

```sql
-- 1. Add column to bi_maintainers (migration Version20251221100000)
ALTER TABLE bi_maintainers
ADD COLUMN idDefaultConnectionDuckdb INT UNSIGNED DEFAULT NULL;
ALTER TABLE bi_maintainers ADD KEY idDefaultConnectionDuckdb (idDefaultConnectionDuckdb);
ALTER TABLE bi_maintainers ADD CONSTRAINT bi_maintainers_ibfk_duckdb
FOREIGN KEY (idDefaultConnectionDuckdb) REFERENCES bi_connectionsMysql (id);

-- 2. Add DuckDB connection entry
INSERT INTO bi_connectionsMysql (host, backend, region, owner, technicalOwner)
VALUES ('duckdb-service', 'duckdb', 'local', 'Keboola', 'Keboola');

-- 3. Add credentials column to bi_projects (migration Version20251221150000)
ALTER TABLE bi_projects ADD COLUMN idDuckdbCredentials INT UNSIGNED DEFAULT NULL;
ALTER TABLE bi_projects ADD KEY idDuckdbCredentials (idDuckdbCredentials);
ALTER TABLE bi_projects ADD CONSTRAINT bi_projects_ibfk_duckdb_credentials
FOREIGN KEY (idDuckdbCredentials) REFERENCES bi_connectionsCredentials (id);

-- 4. Set DuckDB connection for maintainer/project
UPDATE bi_maintainers SET idDefaultConnectionDuckdb = 3 WHERE id = 3;
UPDATE bi_projects SET idDefaultConnectionDuckdb = 3 WHERE id = 5;
```

### PHP Files Modified (All Sessions)

| File | Change |
|------|--------|
| `composer.json` | Added PSR-4 autoload: `Keboola\\StorageDriver\\DuckDB\\` -> `Package/StorageDriverDuckdb/src` |
| `legacy-app/.../models/Buckets.php` | Added `BACKEND_DUCKDB` to `availableBackends()` |
| `legacy-app/.../models/Projects.php` | Added `DefaultConnectionDuckdb` and `DuckdbCredentials` reference rules |
| `legacy-app/.../models/Row/Project.php` | Added DuckDB methods: `getDuckdbCredentials()`, `hasDuckdbActivated()`, `assignDuckdbBackend()`, `removeDuckdbBackend()` |
| `src/Storage/Buckets/BucketCreate/Request/CreateBucketRequest.php` | Added DuckDB to validation choices + OpenAPI schema |
| `legacy-app/.../models/Row/Bucket.php` | Added DuckDB case in `getFullPath()` |
| `legacy-app/.../admin/controllers/MaintainersController.php` | Added DuckDB connections query + form field |
| `legacy-app/.../manage/controllers/requests/MaintainerDataInterface.php` | Added `FIELD_ID_DUCKDB` |
| `legacy-app/.../manage/controllers/requests/CreateMaintainerData.php` | Full DuckDB support |
| `legacy-app/.../admin/views/scripts/maintainers/detail.phtml` | Added `duckdbConnections` to options |
| `public/app/modules/admin/scripts/app.coffee` | Added DuckDB dropdown HTML + JS |
| `legacy-app/.../Storage/Service/Backend/Assign/BackendAssign.php` | Added DuckDB case in `assignBackendToProject()` |
| `legacy-app/.../Storage/Service/Backend/CredentialsResolver/DuckDBCredentialsResolver.php` | Rewritten for proper credential handling |
| `Package/StorageDriverDuckdb/src/DuckDBDriverClient.php` | Updated for project API keys |

### Migration Files Created

```
connection/legacy-app/sql/migrations/accounts/Version20251221100000.php  # bi_maintainers
connection/legacy-app/sql/migrations/accounts/Version20251221150000.php  # bi_projects credentials
```

Run with: `docker compose run --rm cli ./migrations.sh migrations:migrate --no-interaction`

### API Commands That Work

```bash
# List storage backends
curl -s -k "https://localhost:8700/manage/storage-backend" \
  -H "X-KBC-ManageApiToken: $TOKEN"

# Create maintainer (via API, then set DuckDB via SQL)
curl -s -k -X POST "https://localhost:8700/manage/maintainers" \
  -H "X-KBC-ManageApiToken: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "DuckDB Services"}'

# Create organization
curl -s -k -X POST "https://localhost:8700/manage/maintainers/3/organizations" \
  -H "X-KBC-ManageApiToken: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "DuckDB Org"}'

# Create project with DuckDB backend
curl -s -k -X POST "https://localhost:8700/manage/organizations/5/projects" \
  -H "X-KBC-ManageApiToken: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "DuckDB Test Project", "defaultBackend": "duckdb"}'

# Create Storage API token
curl -s -k -X POST "https://localhost:8700/manage/projects/5/tokens" \
  -H "X-KBC-ManageApiToken: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"description": "DuckDB Test Token", "canManageBuckets": true}'

# List buckets (works after PHP fixes)
curl -s -k "https://localhost:8700/v2/storage/buckets" \
  -H "X-StorageApi-Token: $STORAGE_TOKEN"
```

## Related Documents

- **[ADR-014: gRPC Driver Interface](../adr/014-grpc-driver-interface.md)** - Architektura gRPC rozhrani
- **[phase-12-implementation-plan.md](phase-12-implementation-plan.md)** - Detailed implementation plan
- BigQuery driver: `connection/Package/StorageDriverBigQuery/` (reference, ale jiny pattern - in-process)
- storage-driver-common proto: `connection/vendor/keboola/storage-driver-common/proto/`
