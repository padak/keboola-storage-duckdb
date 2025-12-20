# Phase 12: PHP Driver

## Status

### Phase 12a: Python gRPC Server - DONE
- **Status:** DONE (2024-12-20)
- **Tests:** 17 (gRPC handlers, servicer, integration)
- **Implementation:** `src/grpc/` module with handlers for InitBackend, RemoveBackend, CreateProject, DropProject

### Phase 12b-e: PHP Driver + More Commands - TODO
- **Status:** Not Started
- **Waiting for:** Connection integration testing

## Phase 12a Implementation Details

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

## Related Documents

- **[ADR-014: gRPC Driver Interface](../adr/014-grpc-driver-interface.md)** - Architektura gRPC rozhrani
- **[phase-12-implementation-plan.md](phase-12-implementation-plan.md)** - Detailed implementation plan

## Goal

Implement PHP driver that communicates with DuckDB API Service via **gRPC** (not REST).

## Architecture (Updated per ADR-014)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    KEBOOLA CONNECTION (PHP)                          │
│                                                                      │
│  Services (TableInfoService, ImportService, ...)                     │
│         │                                                            │
│         ▼                                                            │
│  DriverClientFactory::getClientForBackend('duckdb')                  │
│         │                                                            │
│         ▼ PROTOBUF MESSAGES                                          │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  DuckdbDriverClient (implements ClientInterface)              │   │
│  │         │                                                     │   │
│  │         │ gRPC (port 50051)                                   │   │
│  │         │                                                     │   │
│  │         │ GenericBackendCredentials:                          │   │
│  │         │   host = project_id                                 │   │
│  │         │   principal = api_key                               │   │
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

**Key Changes from Original Plan:**
- gRPC instead of REST for driver communication
- No PHP handlers needed - gRPC client only
- S3-compatible API for file uploads (see ADR-014)

## Package Structure (Simplified with gRPC)

S gRPC pristupem nepotrebujeme PHP handlery - gRPC klient primo vola Python server.

```
connection/Package/StorageDriverDuckdb/
├── composer.json
├── services.yaml
├── src/
│   ├── DuckdbDriverClient.php       # Implements ClientInterface, gRPC calls
│   ├── DuckdbCredentialsHelper.php  # GenericBackendCredentials → host/principal
│   └── Exception/
│       └── DuckdbDriverException.php
└── tests/
    ├── Unit/
    └── Functional/
```

**Pozn:** Vsechna command logika je v Python gRPC serveru, PHP driver jen:
1. Vytvori `DriverRequest` s credentials a command
2. Zavola `grpcClient->Execute(request)`
3. Vrati `DriverResponse`

## Dependencies

```json
{
    "name": "keboola/storage-driver-duckdb",
    "require": {
        "php": "^8.2",
        "google/protobuf": "^3.21",
        "grpc/grpc": "^1.57",
        "keboola/storage-driver-common": "^7.8.0",
        "psr/log": "^1.1|^2.0|^3.0"
    }
}
```

**Pozn:** Nepotrebujeme `guzzlehttp/guzzle` - komunikace jde pres gRPC, ne HTTP.

## Key Files

### DuckdbDriverClient.php

```php
<?php
declare(strict_types=1);

namespace Keboola\StorageDriver\Duckdb;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Grpc\ChannelCredentials;
use Keboola\StorageDriver\Command\Common\DriverRequest;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Service\StorageDriverServiceClient;

class DuckdbDriverClient implements ClientInterface
{
    private StorageDriverServiceClient $grpcClient;

    public function __construct(string $serviceUrl)
    {
        // gRPC client pro DuckDB service
        $this->grpcClient = new StorageDriverServiceClient(
            $serviceUrl,
            ['credentials' => ChannelCredentials::createInsecure()]
        );
    }

    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);

        // 1. Vytvor DriverRequest
        $request = new DriverRequest();

        $credentialsAny = new Any();
        $credentialsAny->pack($credentials);
        $request->setCredentials($credentialsAny);

        $commandAny = new Any();
        $commandAny->pack($command);
        $request->setCommand($commandAny);

        $request->setFeatures($features);
        $request->setRuntimeOptions($runtimeOptions);

        // 2. Zavolej gRPC Execute
        [$response, $status] = $this->grpcClient->Execute($request)->wait();

        if ($status->code !== \Grpc\STATUS_OK) {
            throw new DuckdbDriverException(
                $status->details,
                $status->code
            );
        }

        // 3. Vrat DriverResponse
        return $response;
    }
}
```

### DuckdbCredentialsHelper.php

```php
<?php
declare(strict_types=1);

namespace Keboola\StorageDriver\Duckdb;

use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

class DuckdbCredentialsHelper
{
    /**
     * Extract project ID from credentials
     * Per ADR-014: host = project_id
     */
    public static function getProjectId(GenericBackendCredentials $credentials): string
    {
        return $credentials->getHost();
    }

    /**
     * Extract API key from credentials
     * Per ADR-014: principal = api_key
     */
    public static function getApiKey(GenericBackendCredentials $credentials): string
    {
        return $credentials->getPrincipal();
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

## Reference

- **[ADR-014: gRPC Driver Interface](../adr/014-grpc-driver-interface.md)** - Vsechna architekturni rozhodnuti
- BigQuery driver: `connection/Package/StorageDriverBigQuery/` (reference, ale jiny pattern - in-process)
- storage-driver-common proto: `connection/vendor/keboola/storage-driver-common/proto/`
- Zajcuv PR s gRPC: https://github.com/keboola/storage-backend/pull/259
