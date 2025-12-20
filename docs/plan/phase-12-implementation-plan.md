# Phase 12: Connection Integration - Implementation Plan

**Status:** READY FOR IMPLEMENTATION (2024-12-20)
**Goal:** Enable creating Keboola projects with DuckDB backend via gRPC integration

## Executive Summary

This plan describes how to connect Keboola Connection with our DuckDB API Service using gRPC. The integration follows the existing driver pattern with a key difference: DuckDB driver runs as a remote service, not in-process.

**Good news:** Python gRPC code is already generated in `storage-backend/packages/php-storage-driver-common/generated-py/`. We just need to copy and adapt it.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ARCHITECTURE                                         │
│                                                                              │
│   Connection (PHP)                         DuckDB Service (Python)           │
│   ┌──────────────────┐                    ┌─────────────────────────────┐   │
│   │ DriverClientFactory                    │ FastAPI :8000 (existing)    │   │
│   │        │                               │ - REST API (debug/dashboard)│   │
│   │        ▼                               │ - /metrics, /health         │   │
│   │ DuckDBDriverClient ──── gRPC :50051 ──►│                             │   │
│   │   (new)          │                     │ gRPC Server :50051 (new)    │   │
│   │        │                               │ - StorageDriverServicer     │   │
│   │        ▼                               │ - Command handlers          │   │
│   │ DriverClientWrapper                    │        │                    │   │
│   └──────────────────┘                    │        ▼                    │   │
│                                            │ Existing Services           │   │
│                                            │ - database.py               │   │
│                                            │ - auth.py                   │   │
│                                            │ - routers/*                 │   │
│                                            └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Decisions (from ADR-014)

| Decision | Value | Rationale |
|----------|-------|-----------|
| Protocol | gRPC (protobuf) | Standard driver interface, type safety |
| Credentials mapping | host=project_id, principal=api_key | Fits GenericBackendCredentials |
| Deployment | Unified process | Single healthcheck, shared memory |
| File handling | S3 path → local path | Driver translates S3 paths |

---

## Phase 12a: Python gRPC Server

**Goal:** Add gRPC server to DuckDB API Service with backend/project handlers

### Source Files (ALREADY EXIST - just copy)

```bash
# Source location (zajca-grpc branch)
storage-backend/packages/php-storage-driver-common/
├── proto/                    # Proto definitions
│   ├── service.proto         # StorageDriverService (Execute RPC)
│   ├── common.proto          # DriverRequest, DriverResponse, LogMessage
│   ├── backend.proto         # InitBackendCommand, RemoveBackendCommand
│   ├── project.proto         # CreateProjectCommand, DropProjectCommand
│   ├── bucket.proto          # Bucket commands
│   ├── table.proto           # Table commands
│   ├── workspace.proto       # Workspace commands
│   ├── info.proto            # ObjectInfo commands
│   ├── credentials.proto     # GenericBackendCredentials
│   └── executeQuery.proto    # ExecuteQuery command
└── generated-py/             # ALREADY GENERATED Python code!
    ├── proto/                # pb2.py + pb2_grpc.py files
    ├── examples/             # Reference implementation
    │   ├── grpc_server.py    # StorageDriverServicer
    │   ├── utils.py          # LogMessageCollector, helpers
    │   └── handlers/
    │       └── base_handler.py
    └── requirements-grpc.txt # grpcio>=1.60.0, protobuf>=3.21.0
```

### Target Structure

```
duckdb-api-service/
├── proto/                     # Copy from storage-backend
│   └── *.proto
├── generated/                 # Copy from storage-backend/generated-py
│   ├── __init__.py
│   └── proto/
│       ├── service_pb2.py
│       ├── service_pb2_grpc.py
│       ├── common_pb2.py
│       ├── backend_pb2.py
│       ├── project_pb2.py
│       ├── credentials_pb2.py
│       └── ...
├── src/
│   ├── grpc/                  # NEW
│   │   ├── __init__.py
│   │   ├── server.py          # gRPC server startup
│   │   ├── servicer.py        # StorageDriverServicer
│   │   ├── utils.py           # LogMessageCollector (copy from examples)
│   │   └── handlers/
│   │       ├── __init__.py
│   │       ├── base.py        # BaseCommandHandler
│   │       ├── backend.py     # InitBackend, RemoveBackend
│   │       └── project.py     # CreateProject, DropProject
│   └── unified_server.py      # NEW - runs REST + gRPC
└── requirements.txt           # Add grpc dependencies
```

### Step-by-Step Implementation

#### Step 1: Copy Proto Files

```bash
cd "/Users/padak/github/Keboola Storage v3"

# Create directories
mkdir -p duckdb-api-service/proto
mkdir -p duckdb-api-service/generated

# Copy proto files
cp storage-backend/packages/php-storage-driver-common/proto/*.proto \
   duckdb-api-service/proto/

# Copy generated Python code
cp -r storage-backend/packages/php-storage-driver-common/generated-py/proto \
   duckdb-api-service/generated/

# Create __init__.py
echo '"""Generated protobuf code for gRPC."""' > duckdb-api-service/generated/__init__.py
```

#### Step 2: Update requirements.txt

Add to `duckdb-api-service/requirements.txt`:
```
# gRPC
grpcio>=1.60.0
grpcio-tools>=1.60.0
protobuf>=3.21.0,<5.0.0
```

Then install:
```bash
cd duckdb-api-service
source .venv/bin/activate
pip install grpcio grpcio-tools 'protobuf>=3.21.0,<5.0.0'
pip freeze > requirements.txt
```

#### Step 3: Create src/grpc/__init__.py

```python
"""gRPC layer for Storage Driver protocol."""
```

#### Step 4: Create src/grpc/utils.py

```python
"""
Utility functions for gRPC handlers.
Adapted from storage-backend/generated-py/examples/utils.py
"""

import logging
from typing import Optional, List
from google.protobuf.message import Message
from google.protobuf import any_pb2

# Import from our generated code
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "generated"))

from proto import common_pb2

logger = logging.getLogger(__name__)


def get_type_name(any_msg: any_pb2.Any) -> str:
    """Extract type name from Any message (e.g., 'CreateProjectCommand')."""
    return any_msg.type_url.split('/')[-1]


def create_log_message(
    level: common_pb2.LogMessage.Level,
    message: str,
    context: Optional[Message] = None
) -> common_pb2.LogMessage:
    """Create a LogMessage for DriverResponse."""
    log_msg = common_pb2.LogMessage()
    log_msg.level = level
    log_msg.message = message
    if context:
        log_msg.context.Pack(context)
    return log_msg


class LogMessageCollector:
    """Collector for LogMessage instances during command handling."""

    def __init__(self):
        self._messages: List[common_pb2.LogMessage] = []

    def info(self, message: str) -> None:
        self._messages.append(
            create_log_message(common_pb2.LogMessage.Level.Informational, message)
        )

    def warning(self, message: str) -> None:
        self._messages.append(
            create_log_message(common_pb2.LogMessage.Level.Warning, message)
        )

    def error(self, message: str) -> None:
        self._messages.append(
            create_log_message(common_pb2.LogMessage.Level.Error, message)
        )

    def debug(self, message: str) -> None:
        self._messages.append(
            create_log_message(common_pb2.LogMessage.Level.Debug, message)
        )

    def get_messages(self) -> List[common_pb2.LogMessage]:
        return self._messages

    def clear(self) -> None:
        self._messages.clear()
```

#### Step 5: Create src/grpc/handlers/base.py

```python
"""Base handler for command processing."""

import logging
from abc import ABC, abstractmethod
from typing import Optional
from google.protobuf.message import Message

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import common_pb2, credentials_pb2
from src.grpc.utils import LogMessageCollector


class BaseCommandHandler(ABC):
    """Abstract base class for command handlers."""

    def __init__(self):
        self.log_collector = LogMessageCollector()
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def handle(
        self,
        command: Message,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> Message:
        """Handle the command and return a response."""
        pass

    def get_log_messages(self) -> list:
        return self.log_collector.get_messages()

    def log_info(self, message: str) -> None:
        self.logger.info(message)
        self.log_collector.info(message)

    def log_warning(self, message: str) -> None:
        self.logger.warning(message)
        self.log_collector.warning(message)

    def log_error(self, message: str) -> None:
        self.logger.error(message)
        self.log_collector.error(message)

    @staticmethod
    def extract_credentials(credentials_any) -> dict:
        """Extract project_id and api_key from GenericBackendCredentials."""
        creds = credentials_pb2.GenericBackendCredentials()
        credentials_any.Unpack(creds)
        return {
            'project_id': creds.host,      # ADR-014: host = project_id
            'api_key': creds.principal,    # ADR-014: principal = api_key
        }
```

#### Step 6: Create src/grpc/handlers/backend.py

```python
"""Backend command handlers."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import backend_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import MetadataDB


class InitBackendHandler(BaseCommandHandler):
    """Initialize storage backend - validates connection works."""

    def __init__(self, metadata_db: MetadataDB):
        super().__init__()
        self.metadata_db = metadata_db

    def handle(self, command, credentials, runtime_options):
        # Unpack command (even if we don't use it)
        cmd = backend_pb2.InitBackendCommand()
        command.Unpack(cmd)

        # Initialize metadata DB
        self.metadata_db.init()

        self.log_info("DuckDB backend initialized successfully")

        return backend_pb2.InitBackendResponse()


class RemoveBackendHandler(BaseCommandHandler):
    """Remove storage backend."""

    def __init__(self, metadata_db: MetadataDB):
        super().__init__()
        self.metadata_db = metadata_db

    def handle(self, command, credentials, runtime_options):
        cmd = backend_pb2.RemoveBackendCommand()
        command.Unpack(cmd)

        # For DuckDB, we don't actually remove anything on backend removal
        # The data stays on disk, just Connection won't use it
        self.log_info("DuckDB backend removed from Connection")

        # No response message defined for RemoveBackend
        return None
```

#### Step 7: Create src/grpc/handlers/project.py

```python
"""Project command handlers."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import project_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import MetadataDB, ProjectDBManager


class CreateProjectHandler(BaseCommandHandler):
    """Create a new project with DuckDB storage."""

    def __init__(self, metadata_db: MetadataDB, project_manager: ProjectDBManager):
        super().__init__()
        self.metadata_db = metadata_db
        self.project_manager = project_manager

    def handle(self, command, credentials, runtime_options):
        cmd = project_pb2.CreateProjectCommand()
        command.Unpack(cmd)

        project_id = cmd.projectId

        # Create project using existing logic
        api_key = self.project_manager.create_project(project_id)

        self.log_info(f"Project {project_id} created with DuckDB backend")

        response = project_pb2.CreateProjectResponse()
        response.projectPassword = api_key  # API key returned as "password"
        response.projectDatabaseName = project_id
        response.projectUserName = f"project_{project_id}"
        return response


class DropProjectHandler(BaseCommandHandler):
    """Drop (delete) a project."""

    def __init__(self, metadata_db: MetadataDB, project_manager: ProjectDBManager):
        super().__init__()
        self.metadata_db = metadata_db
        self.project_manager = project_manager

    def handle(self, command, credentials, runtime_options):
        cmd = project_pb2.DropProjectCommand()
        command.Unpack(cmd)

        # projectDatabaseName contains the project_id
        project_id = cmd.projectDatabaseName

        # Delete project using existing logic
        self.project_manager.delete_project(project_id)

        self.log_info(f"Project {project_id} dropped")

        # No response message defined for DropProject
        return None
```

#### Step 8: Create src/grpc/handlers/__init__.py

```python
"""Command handlers for gRPC service."""

from .backend import InitBackendHandler, RemoveBackendHandler
from .project import CreateProjectHandler, DropProjectHandler

__all__ = [
    'InitBackendHandler',
    'RemoveBackendHandler',
    'CreateProjectHandler',
    'DropProjectHandler',
]
```

#### Step 9: Create src/grpc/servicer.py

```python
"""StorageDriverServicer - main gRPC service implementation."""

import logging
from typing import Optional, Tuple

import grpc

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "generated"))

from proto import service_pb2_grpc, common_pb2, backend_pb2, project_pb2
from src.grpc.utils import get_type_name
from src.grpc.handlers import (
    InitBackendHandler,
    RemoveBackendHandler,
    CreateProjectHandler,
    DropProjectHandler,
)
from src.database import MetadataDB, ProjectDBManager

logger = logging.getLogger(__name__)


class StorageDriverServicer(service_pb2_grpc.StorageDriverServiceServicer):
    """
    gRPC Servicer implementing StorageDriverService.

    Routes incoming DriverRequest to appropriate command handlers
    based on the command type packed in the Any field.
    """

    def __init__(self, metadata_db: MetadataDB, project_manager: ProjectDBManager):
        self.metadata_db = metadata_db
        self.project_manager = project_manager
        self._handlers = self._register_handlers()
        logger.info("StorageDriverServicer initialized")

    def _register_handlers(self) -> dict:
        """Register command type -> handler mappings."""
        return {
            'InitBackendCommand': (
                InitBackendHandler(self.metadata_db),
                backend_pb2.InitBackendCommand
            ),
            'RemoveBackendCommand': (
                RemoveBackendHandler(self.metadata_db),
                backend_pb2.RemoveBackendCommand
            ),
            'CreateProjectCommand': (
                CreateProjectHandler(self.metadata_db, self.project_manager),
                project_pb2.CreateProjectCommand
            ),
            'DropProjectCommand': (
                DropProjectHandler(self.metadata_db, self.project_manager),
                project_pb2.DropProjectCommand
            ),
        }

    def Execute(
        self,
        request: common_pb2.DriverRequest,
        context: grpc.ServicerContext
    ) -> common_pb2.DriverResponse:
        """Execute a storage driver command."""
        try:
            # Get command type
            command_type = get_type_name(request.command)
            logger.info(f"Received command: {command_type}")

            # Log runtime info
            if request.runtimeOptions and request.runtimeOptions.runId:
                logger.debug(f"RunID: {request.runtimeOptions.runId}")

            # Find handler
            handler_info = self._handlers.get(command_type)
            if not handler_info:
                error_msg = f"Unsupported command: {command_type}"
                logger.error(error_msg)
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error_msg)
                return self._error_response(error_msg)

            handler, command_class = handler_info

            # Extract credentials (if needed)
            credentials = None
            if request.credentials and request.credentials.ByteSize() > 0:
                from src.grpc.handlers.base import BaseCommandHandler
                credentials = BaseCommandHandler.extract_credentials(request.credentials)

            # Execute handler
            try:
                response_msg = handler.handle(
                    request.command,
                    credentials,
                    request.runtimeOptions
                )
                return self._wrap_response(response_msg, handler.get_log_messages())

            except ValueError as e:
                logger.error(f"Invalid parameters: {e}")
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(e))
                return self._error_response(str(e), handler.get_log_messages())

            except KeyError as e:
                logger.error(f"Resource not found: {e}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(str(e))
                return self._error_response(str(e), handler.get_log_messages())

            except Exception as e:
                logger.exception(f"Internal error in {command_type}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return self._error_response(str(e), handler.get_log_messages())

        except Exception as e:
            logger.exception("Error in Execute()")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return self._error_response(str(e))

    def _wrap_response(
        self,
        command_response: Optional[object],
        log_messages: list
    ) -> common_pb2.DriverResponse:
        """Wrap command response in DriverResponse."""
        driver_response = common_pb2.DriverResponse()

        if command_response is not None:
            driver_response.commandResponse.Pack(command_response)

        driver_response.messages.extend(log_messages)
        return driver_response

    def _error_response(
        self,
        error_message: str,
        log_messages: Optional[list] = None
    ) -> common_pb2.DriverResponse:
        """Create error response."""
        driver_response = common_pb2.DriverResponse()

        if log_messages:
            driver_response.messages.extend(log_messages)

        error_log = common_pb2.LogMessage()
        error_log.level = common_pb2.LogMessage.Level.Error
        error_log.message = error_message
        driver_response.messages.append(error_log)

        return driver_response
```

#### Step 10: Create src/grpc/server.py

```python
"""gRPC server for StorageDriverService."""

import logging
import signal
import sys
from concurrent import futures

import grpc

# Add generated proto to path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "generated"))

from proto import service_pb2_grpc
from src.grpc.servicer import StorageDriverServicer
from src.database import MetadataDB, ProjectDBManager

logger = logging.getLogger(__name__)


def create_server(
    metadata_db: MetadataDB,
    project_manager: ProjectDBManager,
    host: str = "0.0.0.0",
    port: int = 50051,
    max_workers: int = 10
) -> grpc.Server:
    """Create and configure gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

    servicer = StorageDriverServicer(metadata_db, project_manager)
    service_pb2_grpc.add_StorageDriverServiceServicer_to_server(servicer, server)

    address = f"{host}:{port}"
    server.add_insecure_port(address)

    logger.info(f"gRPC server configured on {address}")
    return server


def serve(
    metadata_db: MetadataDB,
    project_manager: ProjectDBManager,
    host: str = "0.0.0.0",
    port: int = 50051,
    max_workers: int = 10
) -> grpc.Server:
    """Start gRPC server (blocking)."""
    server = create_server(metadata_db, project_manager, host, port, max_workers)
    server.start()

    logger.info(f"gRPC server started on {host}:{port}")

    # Graceful shutdown
    def shutdown(signum, frame):
        logger.info("Shutting down gRPC server...")
        server.stop(grace=5)
        logger.info("gRPC server stopped")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    server.wait_for_termination()
    return server
```

#### Step 11: Create src/unified_server.py

```python
"""
Unified server running both REST (FastAPI) and gRPC.

Usage:
    python -m src.unified_server
"""

import asyncio
import logging
import signal
import sys
import threading
from concurrent import futures

import grpc
import uvicorn

# Add generated proto to path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "generated"))

from proto import service_pb2_grpc
from src.main import app
from src.grpc.servicer import StorageDriverServicer
from src.database import MetadataDB, ProjectDBManager
from src.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_grpc_server(
    metadata_db: MetadataDB,
    project_manager: ProjectDBManager,
    port: int = 50051
) -> grpc.Server:
    """Run gRPC server in background thread."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    servicer = StorageDriverServicer(metadata_db, project_manager)
    service_pb2_grpc.add_StorageDriverServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()

    logger.info(f"gRPC server started on port {port}")
    return server


def main():
    """Start unified server with both REST and gRPC."""
    # Shared instances
    metadata_db = MetadataDB()
    project_manager = ProjectDBManager()

    # Initialize metadata DB
    metadata_db.init()

    # Start gRPC in background thread
    grpc_server = run_grpc_server(metadata_db, project_manager, port=50051)

    # Graceful shutdown
    def shutdown():
        logger.info("Shutting down servers...")
        grpc_server.stop(grace=5)

    # Run FastAPI in main thread
    logger.info("Starting FastAPI on port 8000...")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
    except KeyboardInterrupt:
        pass
    finally:
        shutdown()


if __name__ == "__main__":
    main()
```

#### Step 12: Update Dockerfile

Add to `duckdb-api-service/Dockerfile`:
```dockerfile
# Expose both ports
EXPOSE 8000 50051

# Use unified server
CMD ["python", "-m", "src.unified_server"]
```

#### Step 13: Update docker-compose.yml

```yaml
services:
  duckdb-service:
    build: .
    command: python -m src.unified_server
    ports:
      - "8000:8000"    # REST API
      - "50051:50051"  # gRPC
    volumes:
      - ./data:/data
    environment:
      - DATA_DIR=/data
      - ADMIN_API_KEY=${ADMIN_API_KEY}
```

---

## Phase 12b: Testing gRPC Server

### Test with grpcurl

```bash
# Install grpcurl
brew install grpcurl

# List services
grpcurl -plaintext localhost:50051 list

# Describe service
grpcurl -plaintext localhost:50051 describe keboola.storageDriver.service.StorageDriverService

# Call InitBackend
grpcurl -plaintext -d '{
  "command": {
    "@type": "type.googleapis.com/keboola.storageDriver.command.backend.InitBackendCommand"
  }
}' localhost:50051 keboola.storageDriver.service.StorageDriverService/Execute

# Call CreateProject
grpcurl -plaintext -d '{
  "command": {
    "@type": "type.googleapis.com/keboola.storageDriver.command.project.CreateProjectCommand",
    "projectId": "test-123"
  }
}' localhost:50051 keboola.storageDriver.service.StorageDriverService/Execute
```

### Python Test Client

```python
# tests/test_grpc_server.py
import pytest
import grpc
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "generated"))

from proto import service_pb2_grpc, common_pb2, backend_pb2, project_pb2
from google.protobuf import any_pb2


@pytest.fixture
def grpc_channel():
    """Create gRPC channel for testing."""
    channel = grpc.insecure_channel('localhost:50051')
    yield channel
    channel.close()


@pytest.fixture
def stub(grpc_channel):
    """Create service stub."""
    return service_pb2_grpc.StorageDriverServiceStub(grpc_channel)


def test_init_backend(stub):
    """Test InitBackendCommand."""
    command = backend_pb2.InitBackendCommand()

    request = common_pb2.DriverRequest()
    request.command.Pack(command)

    response = stub.Execute(request)

    assert response is not None
    assert any(msg.level == common_pb2.LogMessage.Level.Informational
               for msg in response.messages)


def test_create_project(stub):
    """Test CreateProjectCommand."""
    command = project_pb2.CreateProjectCommand()
    command.projectId = "test-grpc-123"

    request = common_pb2.DriverRequest()
    request.command.Pack(command)

    response = stub.Execute(request)

    assert response is not None
    assert response.commandResponse.ByteSize() > 0

    # Unpack response
    project_response = project_pb2.CreateProjectResponse()
    response.commandResponse.Unpack(project_response)

    assert project_response.projectDatabaseName == "test-grpc-123"
    assert project_response.projectPassword  # API key returned
```

---

## Phase 12c: PHP Driver (Connection Side)

**Goal:** Register DuckDB backend in Connection

### Files to Modify

```
connection/Package/StorageBackend/src/
├── BackendSupportsInterface.php    # Add BACKEND_DUCKDB constant
└── Driver/
    ├── DriverClientFactory.php     # Add DuckDB case
    └── Config/
        └── DuckDBDriverConfig.php  # NEW
```

### BackendSupportsInterface.php Changes

```php
// Add constant
public const BACKEND_DUCKDB = 'duckdb';

// Add to arrays
public const SUPPORTED_BACKENDS = [
    // ... existing ...
    self::BACKEND_DUCKDB,
];

public const DEV_BRANCH_SUPPORTED_BACKENDS = [
    self::BACKEND_SNOWFLAKE,
    self::BACKEND_BIGQUERY,
    self::BACKEND_DUCKDB,  // DuckDB supports dev branches!
];
```

### DuckDBDriverConfig.php

```php
<?php
declare(strict_types=1);

namespace Keboola\Package\StorageBackend\Driver\Config;

class DuckDBDriverConfig implements DriverConfigInterface
{
    public function isCaseSensitive(): bool
    {
        return true;
    }

    public function getTypesUnsupportedInFilters(): array
    {
        return ['BLOB', 'JSON'];
    }

    public function getCreateTableMetaObject(): ?string
    {
        return null;
    }

    public function extendTableDefinitionResponse($table, array $response): array
    {
        return $response;
    }
}
```

### DriverClientFactory.php Changes

```php
use Keboola\StorageDriver\DuckDB\DuckDBDriverClient;

// In getClientForBackend()
case BackendSupportsInterface::BACKEND_DUCKDB:
    $client = $this->clients->get(DuckDBDriverClient::class);
    assert($client instanceof DuckDBDriverClient);
    return new DriverClientWrapper($client);

// In getConfigForBackend()
case BackendSupportsInterface::BACKEND_DUCKDB:
    return $this->configs->get(DuckDBDriverConfig::class);
```

---

## Command Implementation Priority

### Phase 12a (Minimum Viable) - 3 commands
| Command | Handler | Notes |
|---------|---------|-------|
| InitBackendCommand | InitBackendHandler | Init metadata DB |
| CreateProjectCommand | CreateProjectHandler | Returns API key |
| DropProjectCommand | DropProjectHandler | Delete project |

### Phase 12d (Basic Operations) - +5 commands
| Command | Handler | Notes |
|---------|---------|-------|
| CreateBucketCommand | CreateBucketHandler | Create bucket dir |
| DropBucketCommand | DropBucketHandler | Delete bucket |
| CreateTableCommand | CreateTableHandler | Create table file |
| DropTableCommand | DropTableHandler | Delete table file |
| ObjectInfoCommand | ObjectInfoHandler | Get object metadata |

### Phase 12e (Full Driver) - +12 commands
| Command | Handler |
|---------|---------|
| AddColumnCommand | AddColumnHandler |
| DropColumnCommand | DropColumnHandler |
| AlterColumnCommand | AlterColumnHandler |
| AddPrimaryKeyCommand | PKHandler |
| DropPrimaryKeyCommand | PKHandler |
| TableImportFromFileCommand | ImportHandler |
| TableExportToFileCommand | ExportHandler |
| PreviewTableCommand | PreviewHandler |
| CreateWorkspaceCommand | WorkspaceHandler |
| DropWorkspaceCommand | WorkspaceHandler |
| CreateDevBranchCommand | BranchHandler |
| DropDevBranchCommand | BranchHandler |

---

## Protobuf Message Reference

### DriverRequest
```protobuf
message DriverRequest {
  google.protobuf.Any credentials = 1;  // GenericBackendCredentials
  google.protobuf.Any command = 2;      // Specific command
  repeated string features = 3;         // Feature flags
  RuntimeOptions runtimeOptions = 4;    // runId, queryTags
}
```

### DriverResponse
```protobuf
message DriverResponse {
  google.protobuf.Any commandResponse = 1;  // Specific response
  repeated LogMessage messages = 2;          // Log messages
}
```

### GenericBackendCredentials (ADR-014 mapping)
```protobuf
message GenericBackendCredentials {
  string host = 1;       // = project_id
  string principal = 2;  // = api_key
  string secret = 3;     // unused
  int32 port = 4;        // unused
}
```

### CreateProjectCommand
```protobuf
message CreateProjectCommand {
  string stackPrefix = 1;
  string projectId = 2;
  int32 dataRetentionTime = 6;
  FileStorageType fileStorage = 7;
  google.protobuf.Any meta = 5;
}
```

### CreateProjectResponse
```protobuf
message CreateProjectResponse {
  string projectUserName = 1;
  string projectRoleName = 2;
  string projectPassword = 3;      // = api_key
  string projectReadOnlyRoleName = 4;
  string projectDatabaseName = 8;  // = project_id
}
```

---

## Verification Checklist

After implementation:

1. [ ] Proto files copied to `duckdb-api-service/proto/`
2. [ ] Generated Python code in `duckdb-api-service/generated/proto/`
3. [ ] gRPC dependencies installed
4. [ ] `python -m src.unified_server` starts both REST and gRPC
5. [ ] `grpcurl -plaintext localhost:50051 list` shows service
6. [ ] InitBackendCommand works via grpcurl
7. [ ] CreateProjectCommand creates project and returns API key
8. [ ] DropProjectCommand deletes project
9. [ ] REST API still works on port 8000
10. [ ] Tests pass

---

## References

- [ADR-014: gRPC Driver Interface](../adr/014-grpc-driver-interface.md)
- [storage-backend zajca-grpc branch](../../storage-backend/)
- [generated-py examples](../../storage-backend/packages/php-storage-driver-common/generated-py/examples/)
