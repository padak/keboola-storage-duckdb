# Phase 12: PHP Driver - TODO

## Status
- **Status:** Not Started (LAST)
- **Waiting for:** Complete Python API

## Goal

Implement PHP driver package that integrates with Keboola Connection via Protocol Buffers.

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
│         ▼ PROTOBUF MESSAGES                                          │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  StorageDriverDuckdb Package                                  │   │
│  │                                                               │   │
│  │  DuckdbDriverClient (implements ClientInterface)              │   │
│  │         │                                                     │   │
│  │         ▼                                                     │   │
│  │  HandlerFactory::create($command)                             │   │
│  │         │                                                     │   │
│  │         ├── CreateTableCommand → CreateTableHandler           │   │
│  │         ├── ImportFromFileCommand → ImportHandler             │   │
│  │         └── ... (33+ handlers)                                │   │
│  │                │                                              │   │
│  │                │ REST/JSON                                    │   │
│  │                ▼                                              │   │
│  │  DuckdbApiClient (HTTP client)                                │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                      │                                               │
│                      ▼                                               │
│           DuckDB API Service (Python/FastAPI)                        │
└─────────────────────────────────────────────────────────────────────┘
```

## Package Structure

```
connection/Package/StorageDriverDuckdb/
├── composer.json
├── services.yaml
├── src/
│   ├── DuckdbDriverClient.php       # Implements ClientInterface
│   ├── DuckdbApiClient.php          # HTTP client for Python API
│   ├── DuckdbCredentialsHelper.php  # Credential extraction
│   └── Handler/
│       ├── HandlerFactory.php       # Command -> Handler dispatch
│       ├── BaseHttpHandler.php      # Shared handler logic
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
│       │   ├── UnshareBucketHandler.php
│       │   ├── LinkBucketHandler.php
│       │   ├── UnlinkBucketHandler.php
│       │   ├── GrantBucketAccessToReadOnlyRoleHandler.php
│       │   └── RevokeBucketAccessFromReadOnlyRoleHandler.php
│       ├── Table/
│       │   ├── CreateTableHandler.php
│       │   ├── DropTableHandler.php
│       │   ├── AddColumnHandler.php
│       │   ├── DropColumnHandler.php
│       │   ├── AlterColumnHandler.php
│       │   ├── AddPrimaryKeyHandler.php
│       │   ├── DropPrimaryKeyHandler.php
│       │   ├── DeleteTableRowsHandler.php
│       │   ├── ImportTableFromFileHandler.php
│       │   ├── ImportTableFromTableHandler.php
│       │   ├── ExportTableToFileHandler.php
│       │   ├── PreviewTableHandler.php
│       │   ├── ProfileTableHandler.php
│       │   └── CreateTableFromTimeTravelHandler.php
│       ├── Workspace/
│       │   ├── CreateWorkspaceHandler.php
│       │   ├── DropWorkspaceHandler.php
│       │   ├── ClearWorkspaceHandler.php
│       │   ├── DropWorkspaceObjectHandler.php
│       │   └── ResetWorkspacePasswordHandler.php
│       ├── Branch/
│       │   ├── CreateDevBranchHandler.php
│       │   └── DropDevBranchHandler.php
│       ├── Info/
│       │   └── ObjectInfoHandler.php
│       └── ExecuteQuery/
│           └── ExecuteQueryHandler.php
└── tests/
    ├── Unit/
    └── Functional/
```

## Dependencies

```json
{
    "name": "keboola/storage-driver-duckdb",
    "require": {
        "php": "^8.2",
        "google/protobuf": "^3.21",
        "keboola/storage-driver-common": "^7.8.0",
        "guzzlehttp/guzzle": "^7.0",
        "psr/log": "^1.1|^2.0|^3.0"
    }
}
```

## Key Files

### DuckdbDriverClient.php

```php
<?php
namespace Keboola\StorageDriver\Duckdb;

use Keboola\StorageDriver\Contract\Driver\ClientInterface;

class DuckdbDriverClient implements ClientInterface
{
    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        // 1. Dispatch to handler via HandlerFactory
        // 2. Handler calls Python API via DuckdbApiClient
        // 3. Wrap response in DriverResponse
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

- BigQuery driver: `connection/Package/StorageDriverBigQuery/`
- storage-driver-common: `keboola/storage-driver-common`
