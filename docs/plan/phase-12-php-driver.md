# Phase 12: PHP Driver

## Status

### Phase 12a: Python gRPC Server - DONE
- **Status:** DONE (2024-12-20)
- **Tests:** 17 (gRPC handlers, servicer, integration)
- **Implementation:** `src/grpc/` module with handlers for InitBackend, RemoveBackend, CreateProject, DropProject

### Phase 12a.1: HTTP Driver Bridge - DONE
- **Status:** DONE (2024-12-20)
- **Implementation:** `src/routers/driver.py` - HTTP endpoint for driver commands
- **Endpoint:** `POST /driver/execute` (requires admin auth)
- **Why:** PHP nepotrebuje gRPC extension - vola HTTP endpoint s JSON

### Phase 12b: Connection Backend Registration - DONE
- **Status:** DONE (2024-12-20)
- **Implementation:** Registered DuckDB as storage backend and file storage provider in Connection

### Phase 12b.1: Connection Full Integration - IN PROGRESS
- **Status:** IN PROGRESS (2024-12-21)
- **Goal:** Make DuckDB fully usable from Connection UI/API
- **Result:** Partial success - project created, bucket created, but table creation needs more work
- **See:** Detailed notes below in "Phase 12b.1 Integration Notes"

### Phase 12c-e: More Commands + Testing - TODO
- **Status:** Not Started
- **Waiting for:** Connection integration testing

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
# InitBackendCommand
curl -X POST http://localhost:8000/driver/execute \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"command": {"type": "InitBackendCommand"}}'

# CreateProjectCommand
curl -X POST http://localhost:8000/driver/execute \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"command": {"type": "CreateProjectCommand", "projectId": "test-123"}}'

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
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  DuckDBDriverClient (HTTP Bridge)                             │   │
│  │         │                                                     │   │
│  │         │ HTTP POST /driver/execute                           │   │
│  │         │                                                     │   │
│  │         │ Headers:                                            │   │
│  │         │   Authorization: Bearer $DUCKDB_ADMIN_API_KEY       │   │
│  │         │                                                     │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                      │                                               │
│                      ▼                                               │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  DuckDB API Service (Python)                                  │   │
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
    private Client $httpClient;

    public function __construct(string $serviceUrl, string $adminApiKey)
    {
        $this->httpClient = new Client([
            'base_uri' => rtrim($serviceUrl, '/'),
            'timeout' => 300,
            'headers' => [
                'Content-Type' => 'application/json',
                'Authorization' => 'Bearer ' . $adminApiKey,
            ],
        ]);
    }

    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        // Build request and call POST /driver/execute
        // Parse response into DriverResponse
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
-- 1. Add column to bi_maintainers (migration created)
ALTER TABLE bi_maintainers
ADD COLUMN idDefaultConnectionDuckdb INT UNSIGNED DEFAULT NULL;
ALTER TABLE bi_maintainers ADD KEY idDefaultConnectionDuckdb (idDefaultConnectionDuckdb);
ALTER TABLE bi_maintainers ADD CONSTRAINT bi_maintainers_ibfk_duckdb
FOREIGN KEY (idDefaultConnectionDuckdb) REFERENCES bi_connectionsMysql (id);

-- 2. Add DuckDB connection entry
INSERT INTO bi_connectionsMysql (host, backend, region, owner, technicalOwner)
VALUES ('duckdb-service', 'duckdb', 'local', 'Keboola', 'Keboola');

-- 3. Add column to bi_projects (NOT in migration - done manually)
ALTER TABLE bi_projects ADD COLUMN idDefaultConnectionDuckdb INT UNSIGNED DEFAULT NULL;
ALTER TABLE bi_projects ADD KEY idDefaultConnectionDuckdb (idDefaultConnectionDuckdb);

-- 4. Set DuckDB connection for maintainer/project
UPDATE bi_maintainers SET idDefaultConnectionDuckdb = 3 WHERE id = 3;
UPDATE bi_projects SET idDefaultConnectionDuckdb = 3 WHERE id = 5;
```

### PHP Files Modified

| File | Change |
|------|--------|
| `composer.json` | Added PSR-4 autoload: `Keboola\\StorageDriver\\DuckDB\\` -> `Package/StorageDriverDuckdb/src` |
| `legacy-app/.../models/Buckets.php` | Added `BACKEND_DUCKDB` to `availableBackends()` |
| `src/Storage/Buckets/BucketCreate/Request/CreateBucketRequest.php` | Added DuckDB to validation choices + OpenAPI schema |
| `legacy-app/.../models/Row/Project.php` | Added DuckDB cases in `getBackendDatabaseName()` and `getProjectRoleName()` |
| `legacy-app/.../models/Row/Bucket.php` | Added DuckDB case in `getFullPath()` |
| `legacy-app/.../admin/controllers/MaintainersController.php` | Added DuckDB connections query + form field |
| `legacy-app/.../manage/controllers/requests/MaintainerDataInterface.php` | Added `FIELD_ID_DUCKDB` |
| `legacy-app/.../manage/controllers/requests/CreateMaintainerData.php` | Full DuckDB support |
| `legacy-app/.../admin/views/scripts/maintainers/detail.phtml` | Added `duckdbConnections` to options |
| `public/app/modules/admin/scripts/app.coffee` | Added DuckDB dropdown HTML + JS |

### Migration File Created

```
connection/legacy-app/sql/migrations/accounts/Version20251221100000.php
```

Run with: `docker compose run --rm cli ./migrations.sh migrations:migrate --no-interaction`

### What's Still Missing (Blocking Table Creation)

1. **Zend ORM Reference Rules** - `Model_Projects` needs `DefaultConnectionDuckdb` reference rule defined
   - Error: `No reference rule "DefaultConnectionDuckdb" from table Model_Projects to table Model_ConnectionsMysql`
   - Location: `legacy-app/application/modules/core/models/Projects.php` (referenceMap)

2. **Many more switch statements** - Connection has dozens of places with backend-specific logic:
   - Each `switch ($backendType)` needs a DuckDB case
   - Each `match ($this->getBackend())` needs a DuckDB case

3. **Driver client not yet called** - Current errors happen before DuckDBDriverClient is invoked

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

### Recommended Next Steps

1. **Option A: Continue PHP integration** - Fix Zend ORM reference rules, add all missing switch cases
2. **Option B: Use DuckDB API directly** - Bypass Connection, use DuckDB API Service REST endpoints
3. **Option C: Hybrid** - Use Connection for project/org management, DuckDB API for storage operations

## Related Documents

- **[ADR-014: gRPC Driver Interface](../adr/014-grpc-driver-interface.md)** - Architektura gRPC rozhrani
- **[phase-12-implementation-plan.md](phase-12-implementation-plan.md)** - Detailed implementation plan
- BigQuery driver: `connection/Package/StorageDriverBigQuery/` (reference, ale jiny pattern - in-process)
- storage-driver-common proto: `connection/vendor/keboola/storage-driver-common/proto/`
