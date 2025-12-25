# Phase 17: CLI & Python SDK

**Status:** CLI DONE (2024-12-25), SDK TODO
**Priority:** Post-MVP

## Overview

Vytvořit **CLI nástroj** a **Python SDK** pro externí vývojáře k práci s DuckDB Storage API.

**Strategie:** CLI first, postavené nad Python SDK. REST API pro externí použití, gRPC pro interní.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    External Developers                       │
├─────────────────────────────────────────────────────────────┤
│  keboola-duckdb CLI        │  keboola-duckdb Python SDK     │
│  (typer)                   │  (async httpx + sync wrapper)  │
├─────────────────────────────────────────────────────────────┤
│                    REST API (FastAPI)                        │
│                    Port 8000                                 │
├─────────────────────────────────────────────────────────────┤
│                    DuckDB API Service                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    Internal Services                         │
├─────────────────────────────────────────────────────────────┤
│  Generated gRPC Clients    │  PHP Driver (Connection)       │
│  (Python, Go, PHP)         │  (existing)                    │
├─────────────────────────────────────────────────────────────┤
│                    gRPC API                                  │
│                    Port 50051                                │
└─────────────────────────────────────────────────────────────┘
```

---

## Deliverables

### Phase 17a: Python SDK Core (MVP)
**Package:** `keboola-duckdb-sdk`

```python
# Installation
pip install keboola-duckdb-sdk

# Usage
from keboola_duckdb import DuckDBStorageClient

client = DuckDBStorageClient(
    base_url="http://localhost:8000",
    api_key="proj_123_admin_xxx"
)

# Sync API
projects = client.projects.list()
table = client.tables.get("my-project", "default", "in.c-sales", "orders")

# Async API
async with DuckDBStorageClient(...) as client:
    projects = await client.projects.list_async()
```

**SDK Structure:**
```
sdk/
├── pyproject.toml
├── src/
│   └── keboola_duckdb/
│       ├── __init__.py          # Public exports
│       ├── client.py            # Main client class
│       ├── config.py            # Configuration
│       ├── exceptions.py        # Custom exceptions
│       ├── _http.py             # HTTP client wrapper
│       ├── _retry.py            # Retry logic
│       ├── _pagination.py       # Pagination utilities
│       ├── models/              # Pydantic response models
│       │   ├── __init__.py
│       │   ├── project.py
│       │   ├── bucket.py
│       │   ├── table.py
│       │   ├── file.py
│       │   ├── snapshot.py
│       │   ├── branch.py
│       │   └── workspace.py
│       └── resources/           # API resource classes
│           ├── __init__.py
│           ├── base.py          # BaseResource
│           ├── projects.py      # ProjectsResource
│           ├── buckets.py       # BucketsResource
│           ├── tables.py        # TablesResource
│           ├── files.py         # FilesResource
│           ├── snapshots.py     # SnapshotsResource
│           ├── branches.py      # BranchesResource
│           └── workspaces.py    # WorkspacesResource
└── tests/
    ├── __init__.py
    ├── conftest.py              # Fixtures, mocks
    ├── test_client.py
    ├── test_exceptions.py
    ├── test_pagination.py
    ├── test_retry.py
    └── resources/
        ├── test_projects.py
        ├── test_buckets.py
        └── ...
```

**Core Features:**
- Async-first design (httpx)
- Sync wrapper for convenience
- Type hints everywhere
- Pydantic models for responses
- Automatic pagination with iterator pattern
- Retry logic for transient failures
- Request ID tracing
- Progress callbacks for file uploads
- Streaming support for large data
- Configurable timeouts
- Environment variable support

---

## SDK Detailed Specifications

### Exception Hierarchy

```python
# exceptions.py

class DuckDBStorageError(Exception):
    """Base exception for all SDK errors."""

    def __init__(self, message: str, request_id: str | None = None):
        self.message = message
        self.request_id = request_id
        super().__init__(message)


class ConfigurationError(DuckDBStorageError):
    """Invalid SDK configuration (missing URL, invalid timeout, etc.)."""
    pass


class NetworkError(DuckDBStorageError):
    """Network-level errors (connection refused, DNS failure, timeout)."""

    def __init__(self, message: str, original_error: Exception | None = None, **kwargs):
        self.original_error = original_error
        super().__init__(message, **kwargs)


class TimeoutError(NetworkError):
    """Request timed out."""
    pass


class APIError(DuckDBStorageError):
    """Base class for API response errors (non-2xx status codes)."""

    def __init__(
        self,
        message: str,
        status_code: int,
        error_code: str | None = None,
        details: dict | None = None,
        **kwargs
    ):
        self.status_code = status_code
        self.error_code = error_code  # API-specific error code
        self.details = details or {}
        super().__init__(message, **kwargs)


class AuthenticationError(APIError):
    """401 Unauthorized - Invalid or missing API key."""
    pass


class PermissionDeniedError(APIError):
    """403 Forbidden - Valid key but insufficient permissions."""
    pass


class NotFoundError(APIError):
    """404 Not Found - Resource doesn't exist."""

    def __init__(self, resource_type: str, resource_id: str, **kwargs):
        self.resource_type = resource_type
        self.resource_id = resource_id
        message = f"{resource_type} '{resource_id}' not found"
        super().__init__(message, status_code=404, **kwargs)


class ConflictError(APIError):
    """409 Conflict - Resource already exists or state conflict."""
    pass


class ValidationError(APIError):
    """422 Unprocessable Entity - Invalid request data."""

    def __init__(self, message: str, field_errors: dict | None = None, **kwargs):
        self.field_errors = field_errors or {}
        super().__init__(message, status_code=422, **kwargs)


class RateLimitError(APIError):
    """429 Too Many Requests - Rate limit exceeded."""

    def __init__(self, message: str, retry_after: int | None = None, **kwargs):
        self.retry_after = retry_after  # seconds
        super().__init__(message, status_code=429, **kwargs)


class ServerError(APIError):
    """5xx Server Error - Server-side failure."""

    @property
    def is_retryable(self) -> bool:
        """5xx errors are generally retryable."""
        return True


# Mapping HTTP status codes to exceptions
HTTP_STATUS_EXCEPTIONS = {
    401: AuthenticationError,
    403: PermissionDeniedError,
    404: NotFoundError,
    409: ConflictError,
    422: ValidationError,
    429: RateLimitError,
}
```

**Usage Examples:**

```python
from keboola_duckdb import DuckDBStorageClient
from keboola_duckdb.exceptions import (
    NotFoundError,
    ValidationError,
    RateLimitError,
    ServerError,
)

client = DuckDBStorageClient(...)

try:
    table = client.tables.get("proj", "default", "bucket", "nonexistent")
except NotFoundError as e:
    print(f"Table not found: {e.resource_id}")
    print(f"Request ID: {e.request_id}")  # For support tickets
except ValidationError as e:
    print(f"Invalid request: {e.message}")
    for field, error in e.field_errors.items():
        print(f"  - {field}: {error}")
except RateLimitError as e:
    print(f"Rate limited. Retry after {e.retry_after} seconds")
except ServerError as e:
    if e.is_retryable:
        print("Server error, will retry...")
```

---

### Configuration & Initialization

```python
# config.py

from dataclasses import dataclass, field
from pathlib import Path
import os

@dataclass
class Timeout:
    """HTTP timeout configuration."""
    connect: float = 5.0    # Connection timeout (seconds)
    read: float = 30.0      # Read timeout (seconds)
    write: float = 30.0     # Write timeout (seconds)
    pool: float = 10.0      # Connection pool timeout (seconds)


@dataclass
class RetryConfig:
    """Retry configuration for transient failures."""
    max_retries: int = 3
    backoff_factor: float = 0.5       # Exponential backoff multiplier
    retry_statuses: tuple = (429, 500, 502, 503, 504)
    retry_methods: tuple = ("GET", "HEAD", "OPTIONS", "PUT", "DELETE")


@dataclass
class ClientConfig:
    """SDK client configuration."""
    base_url: str
    api_key: str
    timeout: Timeout = field(default_factory=Timeout)
    retry: RetryConfig = field(default_factory=RetryConfig)

    # Logging
    debug: bool = False
    logger_name: str = "keboola_duckdb"

    # Request tracing
    include_request_id: bool = True

    # User agent
    user_agent: str | None = None  # Auto-generated if None

    @classmethod
    def from_env(cls) -> "ClientConfig":
        """Load configuration from environment variables.

        Environment variables:
            KEBOOLA_DUCKDB_URL: API base URL (required)
            KEBOOLA_DUCKDB_API_KEY: API key (required)
            KEBOOLA_DUCKDB_TIMEOUT_CONNECT: Connection timeout
            KEBOOLA_DUCKDB_TIMEOUT_READ: Read timeout
            KEBOOLA_DUCKDB_TIMEOUT_WRITE: Write timeout
            KEBOOLA_DUCKDB_MAX_RETRIES: Max retry attempts
            KEBOOLA_DUCKDB_DEBUG: Enable debug logging
        """
        base_url = os.environ.get("KEBOOLA_DUCKDB_URL")
        api_key = os.environ.get("KEBOOLA_DUCKDB_API_KEY")

        if not base_url:
            raise ConfigurationError("KEBOOLA_DUCKDB_URL environment variable not set")
        if not api_key:
            raise ConfigurationError("KEBOOLA_DUCKDB_API_KEY environment variable not set")

        timeout = Timeout(
            connect=float(os.environ.get("KEBOOLA_DUCKDB_TIMEOUT_CONNECT", 5.0)),
            read=float(os.environ.get("KEBOOLA_DUCKDB_TIMEOUT_READ", 30.0)),
            write=float(os.environ.get("KEBOOLA_DUCKDB_TIMEOUT_WRITE", 30.0)),
        )

        retry = RetryConfig(
            max_retries=int(os.environ.get("KEBOOLA_DUCKDB_MAX_RETRIES", 3)),
        )

        return cls(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            retry=retry,
            debug=os.environ.get("KEBOOLA_DUCKDB_DEBUG", "").lower() in ("1", "true"),
        )

    @classmethod
    def from_file(cls, path: str | Path = "~/.keboola-duckdb/config.yaml") -> "ClientConfig":
        """Load configuration from YAML file."""
        import yaml

        path = Path(path).expanduser()
        if not path.exists():
            raise ConfigurationError(f"Config file not found: {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        return cls(
            base_url=data["url"],
            api_key=data["api_key"],
            timeout=Timeout(**data.get("timeout", {})),
            retry=RetryConfig(**data.get("retry", {})),
            debug=data.get("debug", False),
        )
```

**Client Initialization:**

```python
# client.py

class DuckDBStorageClient:
    """DuckDB Storage API client."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        *,
        timeout: Timeout | None = None,
        retry: RetryConfig | None = None,
        debug: bool = False,
        config: ClientConfig | None = None,
    ):
        """Initialize the client.

        Args:
            base_url: API base URL (or use KEBOOLA_DUCKDB_URL env var)
            api_key: API key (or use KEBOOLA_DUCKDB_API_KEY env var)
            timeout: Timeout configuration
            retry: Retry configuration
            debug: Enable debug logging
            config: Full configuration object (overrides other args)

        Examples:
            # Explicit configuration
            client = DuckDBStorageClient(
                base_url="http://localhost:8000",
                api_key="proj_123_admin_xxx"
            )

            # From environment variables
            client = DuckDBStorageClient.from_env()

            # From config file
            client = DuckDBStorageClient.from_config()

            # With custom timeouts
            client = DuckDBStorageClient(
                base_url="...",
                api_key="...",
                timeout=Timeout(connect=10.0, read=60.0)
            )
        """
        ...

    @classmethod
    def from_env(cls) -> "DuckDBStorageClient":
        """Create client from environment variables."""
        return cls(config=ClientConfig.from_env())

    @classmethod
    def from_config(cls, path: str = "~/.keboola-duckdb/config.yaml") -> "DuckDBStorageClient":
        """Create client from config file."""
        return cls(config=ClientConfig.from_file(path))
```

---

### Pagination Strategy

```python
# _pagination.py

from dataclasses import dataclass
from typing import Generic, TypeVar, Iterator, AsyncIterator, Callable, Awaitable

T = TypeVar("T")


@dataclass
class Page(Generic[T]):
    """A single page of results."""
    items: list[T]
    total: int | None = None
    limit: int = 100
    offset: int = 0

    @property
    def has_more(self) -> bool:
        """Check if there are more pages."""
        if self.total is not None:
            return self.offset + len(self.items) < self.total
        return len(self.items) == self.limit


class PaginatedList(Generic[T]):
    """Lazy pagination wrapper - iterates through all pages automatically."""

    def __init__(
        self,
        fetch_page: Callable[[int, int], Page[T]],
        fetch_page_async: Callable[[int, int], Awaitable[Page[T]]] | None = None,
        limit: int = 100,
    ):
        self._fetch_page = fetch_page
        self._fetch_page_async = fetch_page_async
        self._limit = limit
        self._first_page: Page[T] | None = None

    def __iter__(self) -> Iterator[T]:
        """Iterate through all items across all pages (sync)."""
        offset = 0
        while True:
            page = self._fetch_page(self._limit, offset)
            yield from page.items

            if not page.has_more:
                break
            offset += self._limit

    async def __aiter__(self) -> AsyncIterator[T]:
        """Iterate through all items across all pages (async)."""
        if self._fetch_page_async is None:
            raise NotImplementedError("Async iteration not supported")

        offset = 0
        while True:
            page = await self._fetch_page_async(self._limit, offset)
            for item in page.items:
                yield item

            if not page.has_more:
                break
            offset += self._limit

    def first_page(self) -> Page[T]:
        """Get just the first page (for quick checks)."""
        if self._first_page is None:
            self._first_page = self._fetch_page(self._limit, 0)
        return self._first_page

    def all(self) -> list[T]:
        """Fetch all items into a list (use with caution for large datasets)."""
        return list(self)

    async def all_async(self) -> list[T]:
        """Fetch all items into a list asynchronously."""
        return [item async for item in self]
```

**Usage Examples:**

```python
# Get first page only (fast)
page = client.projects.list(limit=10)
for project in page.items:
    print(project.name)

# Iterate through ALL projects lazily (memory efficient)
for project in client.projects.list_all():
    print(project.name)

# Async iteration
async for project in client.projects.list_all_async():
    print(project.name)

# Get all into list (use with caution)
all_projects = client.projects.list_all().all()
```

---

### Streaming for Large Data

```python
# resources/tables.py

from typing import Iterator, AsyncIterator

class TablesResource(BaseResource):
    """Tables API resource."""

    def preview(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
        *,
        limit: int = 100,
        columns: list[str] | None = None,
    ) -> TablePreview:
        """Get table preview (in-memory, for small results)."""
        ...

    def preview_stream(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
        *,
        batch_size: int = 1000,
        columns: list[str] | None = None,
    ) -> Iterator[dict]:
        """Stream table rows (memory efficient, for large tables).

        Yields one row at a time as dictionaries.
        """
        offset = 0
        while True:
            preview = self.preview(
                project_id, branch_id, bucket_name, table_name,
                limit=batch_size,
                offset=offset,
                columns=columns,
            )
            yield from preview.rows

            if len(preview.rows) < batch_size:
                break
            offset += batch_size

    def export_stream(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
        *,
        chunk_size: int = 8192,
    ) -> Iterator[bytes]:
        """Stream table export as raw bytes.

        Use this for downloading large exports without loading into memory.
        """
        response = self._client._http.stream(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/export",
        )
        for chunk in response.iter_bytes(chunk_size):
            yield chunk


# resources/files.py

from typing import Callable

ProgressCallback = Callable[[int, int], None]  # (bytes_transferred, total_bytes)

class FilesResource(BaseResource):
    """Files API resource."""

    def upload(
        self,
        project_id: str,
        file_path: str | Path,
        *,
        name: str | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> FileInfo:
        """Upload a file with optional progress tracking.

        Args:
            project_id: Project ID
            file_path: Path to file to upload
            name: Override filename (default: use file name)
            progress_callback: Called with (bytes_uploaded, total_bytes)

        Example:
            def on_progress(uploaded, total):
                pct = (uploaded / total) * 100
                print(f"\\rUploading: {pct:.1f}%", end="")

            client.files.upload("proj", "large.csv", progress_callback=on_progress)
        """
        ...

    def download(
        self,
        project_id: str,
        file_id: int,
        *,
        output_path: str | Path | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> Path:
        """Download a file with optional progress tracking."""
        ...

    def download_stream(
        self,
        project_id: str,
        file_id: int,
        *,
        chunk_size: int = 8192,
    ) -> Iterator[bytes]:
        """Stream file download as raw bytes."""
        ...
```

---

### Logging & Debugging

```python
# _logging.py

import logging
from typing import Any

def setup_logging(
    level: int = logging.INFO,
    logger_name: str = "keboola_duckdb",
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
) -> logging.Logger:
    """Configure SDK logging.

    Usage:
        import logging
        from keboola_duckdb import setup_logging

        # Basic setup
        setup_logging(logging.DEBUG)

        # Or configure the logger directly
        logging.getLogger("keboola_duckdb").setLevel(logging.DEBUG)
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(format))
        logger.addHandler(handler)

    return logger


class RequestLogger:
    """Log HTTP requests and responses for debugging."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log_request(
        self,
        method: str,
        url: str,
        headers: dict,
        body: Any = None,
    ):
        self.logger.debug(f"Request: {method} {url}")
        self.logger.debug(f"Headers: {self._sanitize_headers(headers)}")
        if body:
            self.logger.debug(f"Body: {self._truncate(body)}")

    def log_response(
        self,
        status_code: int,
        headers: dict,
        body: Any = None,
        elapsed_ms: float = 0,
    ):
        self.logger.debug(f"Response: {status_code} ({elapsed_ms:.2f}ms)")
        if body:
            self.logger.debug(f"Body: {self._truncate(body)}")

    def _sanitize_headers(self, headers: dict) -> dict:
        """Remove sensitive values from headers."""
        sanitized = dict(headers)
        for key in ("Authorization", "X-Api-Key"):
            if key in sanitized:
                sanitized[key] = "***REDACTED***"
        return sanitized

    def _truncate(self, data: Any, max_length: int = 1000) -> str:
        """Truncate long data for logging."""
        s = str(data)
        if len(s) > max_length:
            return s[:max_length] + "... (truncated)"
        return s
```

**Debug Mode:**

```python
# Enable via environment variable
export KEBOOLA_DUCKDB_DEBUG=1

# Or programmatically
client = DuckDBStorageClient(
    base_url="...",
    api_key="...",
    debug=True,  # Enables DEBUG level logging
)

# Or configure logger directly
import logging
logging.getLogger("keboola_duckdb").setLevel(logging.DEBUG)
```

---

### Phase 17b: CLI Tool (MVP)
**Package:** `keboola-duckdb-cli`

```bash
# Installation
pip install keboola-duckdb-cli
# or
pipx install keboola-duckdb-cli

# Configuration
export KEBOOLA_DUCKDB_URL=http://localhost:8000
export KEBOOLA_DUCKDB_API_KEY=proj_123_admin_xxx
# or
keboola-duckdb config set url http://localhost:8000
keboola-duckdb config set api-key proj_123_admin_xxx

# Usage
keboola-duckdb projects list
keboola-duckdb tables list my-project in.c-sales
keboola-duckdb tables preview my-project in.c-sales orders --limit 10
keboola-duckdb files upload my-project data.csv
keboola-duckdb tables import my-project in.c-sales orders --file-id 123
```

**CLI Commands:**
```
keboola-duckdb
├── config
│   ├── set <key> <value>
│   ├── get <key>
│   ├── list
│   ├── use-profile <name>       # Switch active profile
│   ├── list-profiles            # List all profiles
│   └── delete-profile <name>    # Delete a profile
├── projects
│   ├── list [--limit] [--offset]
│   ├── create <name>
│   ├── get <id>
│   ├── stats <id>
│   └── delete <id> [--force] [--yes]
├── branches
│   ├── list <project>
│   ├── create <project> <name>
│   ├── get <project> <branch>
│   └── delete <project> <branch> [--yes]
├── buckets
│   ├── list <project> [--branch default]
│   ├── create <project> <name> [--branch default]
│   ├── get <project> <bucket> [--branch default]
│   └── delete <project> <bucket> [--branch default] [--cascade] [--yes]
├── tables
│   ├── list <project> <bucket> [--branch default]
│   ├── create <project> <bucket> <name> --columns <json> [--branch default]
│   ├── get <project> <bucket> <table> [--branch default]
│   ├── preview <project> <bucket> <table> [--limit 100] [--branch default]
│   ├── profile <project> <bucket> <table> [-m mode] [-q] [-d] [-r] [-c cols] [--branch default]
│   ├── delete <project> <bucket> <table> [--branch default] [--yes]
│   └── delete-many <project> <bucket> --tables <t1,t2,...> [--yes]  # Bulk delete
├── schema
│   ├── add-column <project> <bucket> <table> <name> <type> [--branch default]
│   ├── drop-column <project> <bucket> <table> <column> [--branch default] [--yes]
│   ├── set-pk <project> <bucket> <table> <columns...> [--branch default]
│   └── drop-pk <project> <bucket> <table> [--branch default] [--yes]
├── data
│   ├── import <project> <bucket> <table> --file-id <id> [--incremental] [--branch default]
│   └── export <project> <bucket> <table> --output <file> [--branch default]
├── files
│   ├── list <project> [--limit] [--offset]
│   ├── upload <project> <file> [--name]        # Single file
│   ├── upload <project> <glob-pattern>          # Multiple files: *.csv
│   ├── download <project> <file-id> [--output]
│   └── delete <project> <file-id> [--yes]
├── snapshots
│   ├── list <project> <bucket> <table> [--branch default]
│   ├── create <project> <bucket> <table> [--name] [--branch default]
│   ├── get <project> <bucket> <table> <snapshot-id> [--branch default]
│   ├── restore <project> <bucket> <table> <snapshot-id> [--branch default] [--yes]
│   └── delete <project> <bucket> <table> <snapshot-id> [--branch default] [--yes]
└── workspaces
    ├── list <project>
    ├── create <project> [--name]
    ├── get <project> <workspace-id>
    ├── load <project> <workspace-id> --tables <bucket.table,...>
    ├── connect <project> <workspace-id>  # prints psql command
    └── delete <project> <workspace-id> [--yes]
```

**CLI Structure:**
```
cli/
├── pyproject.toml
├── src/
│   └── keboola_duckdb_cli/
│       ├── __init__.py
│       ├── main.py              # CLI entry point
│       ├── config.py            # Config management + profiles
│       ├── credentials.py       # Secure credential storage
│       ├── output.py            # Table/JSON/CSV formatting
│       ├── progress.py          # Progress bars
│       └── commands/
│           ├── __init__.py
│           ├── config.py
│           ├── projects.py
│           ├── buckets.py
│           ├── tables.py
│           ├── schema.py
│           ├── data.py
│           ├── files.py
│           ├── snapshots.py
│           ├── branches.py
│           └── workspaces.py
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_config.py
    ├── test_output.py
    └── commands/
        ├── test_projects.py
        └── ...
```

---

## CLI Detailed Specifications

### Global Flags

All commands support these global flags:

| Flag | Short | Description |
|------|-------|-------------|
| `--help` | `-h` | Show help message |
| `--version` | `-V` | Show CLI version |
| `--json` | `-j` | Output as JSON (machine readable) |
| `--format` | `-f` | Output format: `table`, `json`, `csv`, `yaml` |
| `--quiet` | `-q` | Suppress non-essential output |
| `--verbose` | `-v` | Increase verbosity (-v, -vv, -vvv) |
| `--no-color` | | Disable colored output (for CI/CD) |
| `--dry-run` | | Show what would be done without executing |
| `--yes` | `-y` | Skip confirmation prompts |
| `--profile` | `-p` | Use specific config profile |

```bash
# Examples
keboola-duckdb projects list --json
keboola-duckdb projects list --format csv > projects.csv
keboola-duckdb buckets delete my-proj bucket --dry-run
keboola-duckdb buckets delete my-proj bucket --yes  # No confirmation
keboola-duckdb --profile production projects list
```

### Configuration & Profiles

```yaml
# ~/.keboola-duckdb/config.yaml

# Active profile
active_profile: development

# Profiles
profiles:
  development:
    url: http://localhost:8000
    # api_key stored in system keychain, not plaintext

  production:
    url: https://duckdb.keboola.com
    # api_key stored in system keychain

# Global settings (apply to all profiles)
settings:
  output_format: table       # Default output format
  color: auto               # auto, always, never
  confirm_destructive: true # Prompt before delete operations
  page_size: 100            # Default pagination size
```

**Secure Credential Storage:**

```python
# credentials.py

import keyring
from typing import Optional

KEYRING_SERVICE = "keboola-duckdb-cli"

def store_api_key(profile: str, api_key: str) -> None:
    """Store API key in system keychain."""
    keyring.set_password(KEYRING_SERVICE, profile, api_key)

def get_api_key(profile: str) -> Optional[str]:
    """Retrieve API key from system keychain."""
    return keyring.get_password(KEYRING_SERVICE, profile)

def delete_api_key(profile: str) -> None:
    """Remove API key from system keychain."""
    keyring.delete_password(KEYRING_SERVICE, profile)
```

**Profile Commands:**

```bash
# Create/update profile
keboola-duckdb config set url http://localhost:8000 --profile dev
keboola-duckdb config set api-key proj_xxx --profile dev  # Stored in keychain

# Switch profile
keboola-duckdb config use-profile production

# List profiles
keboola-duckdb config list-profiles
# Output:
#   development (active)
#   production
#   staging

# Delete profile
keboola-duckdb config delete-profile staging
```

### Dangerous Operations & Confirmations

Operations that require confirmation (unless `--yes` is provided):

| Command | Confirmation Message |
|---------|---------------------|
| `projects delete` | "Delete project 'X' and ALL its data? This cannot be undone." |
| `buckets delete` | "Delete bucket 'X' and all its tables?" |
| `buckets delete --cascade` | "Delete bucket 'X', all tables, and all snapshots?" |
| `tables delete` | "Delete table 'X'?" |
| `tables delete-many` | "Delete N tables? (list tables)" |
| `snapshots restore` | "Restore table 'X' to snapshot from YYYY-MM-DD?" |
| `schema drop-column` | "Drop column 'X' from table 'Y'? Data will be lost." |
| `schema drop-pk` | "Drop primary key from table 'X'?" |
| `branches delete` | "Delete branch 'X' and all its data?" |
| `workspaces delete` | "Delete workspace 'X'?" |

```bash
# Interactive confirmation
$ keboola-duckdb projects delete my-project
Delete project 'my-project' and ALL its data? This cannot be undone. [y/N]: y
Project deleted.

# Skip confirmation
$ keboola-duckdb projects delete my-project --yes
Project deleted.

# Dry run (no confirmation needed)
$ keboola-duckdb projects delete my-project --dry-run
[DRY RUN] Would delete project 'my-project'
```

### Output Formatting

```python
# output.py

from enum import Enum
from typing import Any
import json
import csv
import io
from rich.console import Console
from rich.table import Table

class OutputFormat(Enum):
    TABLE = "table"
    JSON = "json"
    CSV = "csv"
    YAML = "yaml"

def format_output(
    data: Any,
    format: OutputFormat,
    columns: list[str] | None = None,
    title: str | None = None,
) -> str:
    """Format data for output."""

    if format == OutputFormat.JSON:
        return json.dumps(data, indent=2, default=str)

    elif format == OutputFormat.CSV:
        output = io.StringIO()
        if isinstance(data, list) and data:
            writer = csv.DictWriter(output, fieldnames=columns or data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        return output.getvalue()

    elif format == OutputFormat.YAML:
        import yaml
        return yaml.dump(data, default_flow_style=False)

    else:  # TABLE
        console = Console()
        table = Table(title=title)

        if isinstance(data, list) and data:
            cols = columns or list(data[0].keys())
            for col in cols:
                table.add_column(col)
            for row in data:
                table.add_row(*[str(row.get(c, "")) for c in cols])

        with console.capture() as capture:
            console.print(table)
        return capture.get()
```

### Pipe Support (stdin/stdout)

```bash
# Read from stdin
cat data.csv | keboola-duckdb files upload my-project --name data.csv --stdin

# Pipe to other commands
keboola-duckdb tables preview my-project bucket table --format csv | head -10
keboola-duckdb projects list --json | jq '.[] | .name'

# Chain commands
keboola-duckdb files upload my-project data.csv --json | \
  jq -r '.id' | \
  xargs -I {} keboola-duckdb data import my-project bucket table --file-id {}
```

### Environment Variable Support

| Variable | Description |
|----------|-------------|
| `KEBOOLA_DUCKDB_URL` | API base URL |
| `KEBOOLA_DUCKDB_API_KEY` | API key |
| `KEBOOLA_DUCKDB_PROFILE` | Active profile name |
| `KEBOOLA_DUCKDB_FORMAT` | Default output format |
| `KEBOOLA_DUCKDB_NO_COLOR` | Disable colors (set to 1) |
| `KEBOOLA_DUCKDB_DEBUG` | Enable debug output (set to 1) |

```bash
# CI/CD example
export KEBOOLA_DUCKDB_URL=$DUCKDB_API_URL
export KEBOOLA_DUCKDB_API_KEY=$DUCKDB_API_KEY
export KEBOOLA_DUCKDB_NO_COLOR=1
export KEBOOLA_DUCKDB_FORMAT=json

keboola-duckdb projects list
```

### Bulk Operations

```bash
# Upload multiple files
keboola-duckdb files upload my-project data/*.csv
# Uploading 5 files...
#   data/orders.csv    [====================] 100%
#   data/customers.csv [====================] 100%
#   data/products.csv  [====================] 100%
#   ...

# Delete multiple tables
keboola-duckdb tables delete-many my-project in.c-sales \
  --tables orders,customers,products
# Delete 3 tables from bucket 'in.c-sales'?
#   - orders
#   - customers
#   - products
# [y/N]: y

# Import multiple files (from file list)
cat file_ids.txt | xargs -I {} keboola-duckdb data import my-project bucket table --file-id {}
```

---

### Phase 17c: gRPC Generated Clients (Internal)

Generate clients from existing `.proto` files:

```bash
# Python
python -m grpc_tools.protoc -I./proto --python_out=./generated --grpc_python_out=./generated proto/*.proto

# Go
protoc --go_out=./generated --go-grpc_out=./generated proto/*.proto

# PHP
protoc --php_out=./generated --grpc_out=./generated --plugin=protoc-gen-grpc=grpc_php_plugin proto/*.proto
```

**Note:** gRPC clients are for internal Keboola services only. External developers use REST SDK.

---

## Testing Strategy

### SDK Testing

```
sdk/tests/
├── conftest.py              # Shared fixtures
├── unit/                    # Unit tests (mocked HTTP)
│   ├── test_client.py
│   ├── test_config.py
│   ├── test_exceptions.py
│   ├── test_pagination.py
│   ├── test_retry.py
│   └── resources/
│       ├── test_projects.py
│       ├── test_buckets.py
│       └── ...
├── integration/             # Integration tests (real API)
│   ├── conftest.py          # Test server setup
│   ├── test_projects.py
│   ├── test_buckets.py
│   └── ...
└── contract/                # Contract tests (OpenAPI validation)
    └── test_api_contract.py
```

**Unit Tests (Mocked):**

```python
# tests/unit/test_client.py

import pytest
from unittest.mock import Mock, patch
from keboola_duckdb import DuckDBStorageClient
from keboola_duckdb.exceptions import NotFoundError

@pytest.fixture
def mock_http():
    """Mock HTTP client."""
    with patch("keboola_duckdb.client.HTTPClient") as mock:
        yield mock.return_value

def test_projects_list(mock_http):
    mock_http.get.return_value = {
        "items": [{"id": "proj-1", "name": "Test"}],
        "total": 1
    }

    client = DuckDBStorageClient(base_url="http://test", api_key="test")
    result = client.projects.list()

    assert len(result.items) == 1
    assert result.items[0].id == "proj-1"
    mock_http.get.assert_called_once_with("/projects", params={"limit": 100, "offset": 0})

def test_table_not_found(mock_http):
    mock_http.get.side_effect = NotFoundError("table", "orders")

    client = DuckDBStorageClient(base_url="http://test", api_key="test")

    with pytest.raises(NotFoundError) as exc:
        client.tables.get("proj", "default", "bucket", "orders")

    assert exc.value.resource_type == "table"
    assert exc.value.resource_id == "orders"
```

**Integration Tests:**

```python
# tests/integration/conftest.py

import pytest
import os

@pytest.fixture(scope="session")
def api_client():
    """Real API client for integration tests."""
    from keboola_duckdb import DuckDBStorageClient

    url = os.environ.get("TEST_DUCKDB_URL", "http://localhost:8000")
    key = os.environ.get("TEST_DUCKDB_API_KEY")

    if not key:
        pytest.skip("TEST_DUCKDB_API_KEY not set")

    return DuckDBStorageClient(base_url=url, api_key=key)

@pytest.fixture
def test_project(api_client):
    """Create a test project, cleanup after test."""
    project = api_client.projects.create(name=f"test-{uuid.uuid4()}")
    yield project
    api_client.projects.delete(project.id)
```

**Contract Tests (OpenAPI Validation):**

```python
# tests/contract/test_api_contract.py

import pytest
from openapi_core import OpenAPI
from openapi_core.testing.mock import MockRequest, MockResponse

@pytest.fixture
def openapi_spec():
    """Load OpenAPI spec."""
    return OpenAPI.from_path("openapi.yaml")

def test_projects_list_response_matches_schema(openapi_spec, recorded_response):
    """Verify API response matches OpenAPI schema."""
    request = MockRequest("http://test", "GET", "/projects")
    response = MockResponse(recorded_response.body, recorded_response.status)

    result = openapi_spec.validate_response(request, response)
    assert not result.errors
```

### CLI Testing

```
cli/tests/
├── conftest.py              # CLI test fixtures
├── test_config.py           # Config file handling
├── test_output.py           # Output formatting
├── test_credentials.py      # Keychain integration
├── snapshots/               # Output snapshots
│   ├── test_projects_list.txt
│   └── ...
└── commands/
    ├── test_projects.py
    ├── test_buckets.py
    └── ...
```

**CLI Unit Tests:**

```python
# tests/commands/test_projects.py

import pytest
from typer.testing import CliRunner
from keboola_duckdb_cli.main import app

runner = CliRunner()

def test_projects_list_table_format(mocker):
    """Test projects list with table output."""
    mock_client = mocker.patch("keboola_duckdb_cli.commands.projects.get_client")
    mock_client.return_value.projects.list.return_value = Page(
        items=[
            Project(id="proj-1", name="Test 1"),
            Project(id="proj-2", name="Test 2"),
        ]
    )

    result = runner.invoke(app, ["projects", "list"])

    assert result.exit_code == 0
    assert "proj-1" in result.output
    assert "Test 1" in result.output

def test_projects_list_json_format(mocker):
    """Test projects list with JSON output."""
    # ... setup mock ...

    result = runner.invoke(app, ["projects", "list", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data) == 2

def test_projects_delete_requires_confirmation(mocker):
    """Test that delete prompts for confirmation."""
    result = runner.invoke(app, ["projects", "delete", "my-project"], input="n\n")

    assert result.exit_code == 1
    assert "Aborted" in result.output

def test_projects_delete_with_yes_flag(mocker):
    """Test that --yes skips confirmation."""
    mock_client = mocker.patch("keboola_duckdb_cli.commands.projects.get_client")

    result = runner.invoke(app, ["projects", "delete", "my-project", "--yes"])

    assert result.exit_code == 0
    mock_client.return_value.projects.delete.assert_called_once_with("my-project")
```

**Snapshot Tests:**

```python
# tests/test_output_snapshots.py

import pytest
from syrupy.assertion import SnapshotAssertion

def test_projects_list_output(snapshot: SnapshotAssertion, mocker):
    """Verify CLI output hasn't changed unexpectedly."""
    # ... setup ...
    result = runner.invoke(app, ["projects", "list"])
    assert result.output == snapshot
```

---

## Versioning & Compatibility

### SDK Versioning

```python
# __init__.py

__version__ = "0.1.0"

# Version tuple for programmatic access
VERSION = (0, 1, 0)
```

### API Version Negotiation

```python
# client.py

class DuckDBStorageClient:
    SUPPORTED_API_VERSIONS = ["v1"]

    def __init__(self, ..., api_version: str = "v1"):
        if api_version not in self.SUPPORTED_API_VERSIONS:
            raise ConfigurationError(f"Unsupported API version: {api_version}")
        self.api_version = api_version
```

### Deprecation Warnings

```python
# _deprecation.py

import warnings
from functools import wraps

def deprecated(message: str, removal_version: str):
    """Mark a function as deprecated."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"{func.__name__} is deprecated and will be removed in {removal_version}. {message}",
                DeprecationWarning,
                stacklevel=2,
            )
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Usage
class TablesResource:
    @deprecated("Use import_file() instead", removal_version="1.0.0")
    def legacy_import(self, ...):
        return self.import_file(...)
```

### Compatibility Matrix

| SDK Version | API Version | Python | Status |
|-------------|-------------|--------|--------|
| 0.1.x | v1 | 3.9+ | Current |
| 0.2.x | v1 | 3.10+ | Planned |

---

## Implementation Plan

### Step 1: SDK Foundation (Priority 1)
- [ ] Create `sdk/` package structure with pyproject.toml
- [ ] Implement exception hierarchy (`exceptions.py`)
- [ ] Implement configuration (`config.py`)
- [ ] Implement HTTP client wrapper with retry logic (`_http.py`, `_retry.py`)
- [ ] Add timeout configuration
- [ ] Add environment variable support
- [ ] Setup logging infrastructure

### Step 2: SDK Models
- [ ] Define Pydantic models matching API responses
- [ ] Add model validation tests
- [ ] Implement model serialization/deserialization

### Step 3: SDK Pagination & Streaming
- [ ] Implement `Page` and `PaginatedList` classes
- [ ] Add iterator pattern for lazy pagination
- [ ] Implement streaming for large data exports
- [ ] Add progress callbacks for file operations

### Step 4: SDK Resources
- [ ] BaseResource with common functionality
- [ ] ProjectsResource (CRUD + stats)
- [ ] BucketsResource (CRUD + sharing)
- [ ] TablesResource (CRUD + preview + schema ops + streaming)
- [ ] FilesResource (upload workflow with progress)
- [ ] SnapshotsResource (CRUD + restore)
- [ ] BranchesResource (CRUD)
- [ ] WorkspacesResource (CRUD + load)

### Step 5: SDK Testing
- [ ] Unit tests with mocked HTTP
- [ ] Integration tests with real API
- [ ] Contract tests (OpenAPI validation)
- [ ] Test fixtures and helpers

### Step 6: CLI Foundation
- [ ] Create `cli/` package structure with pyproject.toml
- [ ] Setup typer with command groups
- [ ] Implement config management with profiles
- [ ] Implement secure credential storage (keyring)
- [ ] Add output formatting (table, JSON, CSV, YAML)
- [ ] Implement global flags (--json, --quiet, --dry-run, etc.)

### Step 7: CLI Commands
- [ ] Implement all command groups
- [ ] Add progress bars for file operations
- [ ] Add shell completion (bash, zsh, fish)
- [ ] Add interactive confirmations for dangerous operations
- [ ] Implement pipe support (stdin/stdout)

### Step 8: CLI Testing
- [ ] Command unit tests with mocked SDK
- [ ] Output snapshot tests
- [ ] Integration tests
- [ ] Config and credentials tests

### Step 9: Documentation & Release
- [ ] SDK README with examples
- [ ] CLI README with examples
- [ ] API reference documentation (generated from docstrings)
- [ ] Changelog
- [ ] PyPI package setup

---

## Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| HTTP client | httpx | Async support, modern API, streaming |
| CLI framework | typer | Type hints, auto-completion, rich integration |
| Output formatting | rich | Tables, progress bars, colors, markdown |
| Config storage | YAML + keyring | Human readable config, secure credentials |
| Package manager | uv/pip | Standard Python tooling |
| Packaging | pyproject.toml | Modern Python packaging (PEP 517/518) |
| Package naming | `keboola-duckdb-sdk`, `keboola-duckdb-cli` | Keboola brand + DuckDB specific |
| Location | `sdk/` and `cli/` directories | In this repository |
| Testing | pytest + syrupy | Standard + snapshot testing |
| Logging | stdlib logging | No dependencies, configurable |
| Credentials | keyring | Cross-platform secure storage |

---

## Final Directory Structure

```
Keboola Storage v3/
├── duckdb-api-service/      # Existing API service
├── sdk/                     # NEW: Python SDK
│   ├── pyproject.toml
│   ├── README.md
│   ├── CHANGELOG.md
│   └── src/keboola_duckdb/
│       ├── __init__.py
│       ├── client.py
│       ├── config.py
│       ├── exceptions.py
│       ├── _http.py
│       ├── _retry.py
│       ├── _pagination.py
│       ├── _logging.py
│       ├── _deprecation.py
│       ├── models/
│       │   ├── __init__.py
│       │   ├── project.py
│       │   ├── bucket.py
│       │   ├── table.py
│       │   ├── file.py
│       │   ├── snapshot.py
│       │   ├── branch.py
│       │   └── workspace.py
│       └── resources/
│           ├── __init__.py
│           ├── base.py
│           ├── projects.py
│           ├── buckets.py
│           ├── tables.py
│           ├── files.py
│           ├── snapshots.py
│           ├── branches.py
│           └── workspaces.py
└── cli/                     # NEW: CLI tool
    ├── pyproject.toml
    ├── README.md
    ├── CHANGELOG.md
    └── src/keboola_duckdb_cli/
        ├── __init__.py
        ├── main.py
        ├── config.py
        ├── credentials.py
        ├── output.py
        ├── progress.py
        └── commands/
            ├── __init__.py
            ├── config.py
            ├── projects.py
            ├── buckets.py
            ├── tables.py
            ├── schema.py
            ├── data.py
            ├── files.py
            ├── snapshots.py
            ├── branches.py
            └── workspaces.py
```

---

## Example Usage

### SDK Examples

```python
from keboola_duckdb import DuckDBStorageClient
from keboola_duckdb.exceptions import NotFoundError, ValidationError

# Initialize from environment
client = DuckDBStorageClient.from_env()

# Or explicit configuration
client = DuckDBStorageClient(
    base_url="http://localhost:8000",
    api_key="proj_123_admin_xxx",
    timeout=Timeout(connect=10.0, read=60.0),
    debug=True,
)

# List projects (first page)
page = client.projects.list(limit=10)
print(f"Total projects: {page.total}")
for project in page.items:
    print(f"{project.id}: {project.name}")

# Iterate through ALL projects (lazy pagination)
for project in client.projects.list_all():
    print(project.name)

# Error handling
try:
    table = client.tables.get("proj", "default", "bucket", "nonexistent")
except NotFoundError as e:
    print(f"Table not found: {e.resource_id}")
    print(f"Request ID for support: {e.request_id}")
except ValidationError as e:
    print(f"Validation error: {e.message}")
    for field, error in e.field_errors.items():
        print(f"  {field}: {error}")

# Create table
table = client.tables.create(
    project_id="my-project",
    branch_id="default",
    bucket_name="in.c-sales",
    name="orders",
    columns=[
        {"name": "id", "type": "INTEGER"},
        {"name": "customer", "type": "VARCHAR"},
        {"name": "amount", "type": "DECIMAL(10,2)"}
    ],
    primary_key=["id"]
)

# Upload file with progress
def on_progress(uploaded, total):
    pct = (uploaded / total) * 100
    print(f"\rUploading: {pct:.1f}%", end="", flush=True)

file_info = client.files.upload(
    "my-project",
    "orders.csv",
    progress_callback=on_progress
)
print(f"\nUploaded: {file_info.id}")

# Import data
client.tables.import_file(
    project_id="my-project",
    branch_id="default",
    bucket_name="in.c-sales",
    table_name="orders",
    file_id=file_info.id,
    incremental=False
)

# Stream large table preview (memory efficient)
for row in client.tables.preview_stream("my-project", "default", "in.c-sales", "orders"):
    print(row)

# Export to file with streaming
with open("export.csv", "wb") as f:
    for chunk in client.tables.export_stream("my-project", "default", "in.c-sales", "orders"):
        f.write(chunk)
```

### CLI Examples

```bash
# Setup profile
keboola-duckdb config set url http://localhost:8000 --profile dev
keboola-duckdb config set api-key proj_123_admin_xxx --profile dev
keboola-duckdb config use-profile dev

# Or use environment variables
export KEBOOLA_DUCKDB_URL=http://localhost:8000
export KEBOOLA_DUCKDB_API_KEY=proj_123_admin_xxx

# List projects (table format)
keboola-duckdb projects list

# List projects (JSON for scripting)
keboola-duckdb projects list --json | jq '.[] | .name'

# List projects (CSV for export)
keboola-duckdb projects list --format csv > projects.csv

# Create table from JSON schema
keboola-duckdb tables create my-project in.c-sales orders \
  --columns '[{"name":"id","type":"INTEGER"},{"name":"customer","type":"VARCHAR"}]' \
  --pk id

# Upload file with progress bar
keboola-duckdb files upload my-project orders.csv
# Uploading: orders.csv [====================] 100% 2.3MB/2.3MB

# Upload multiple files
keboola-duckdb files upload my-project data/*.csv

# Import data
keboola-duckdb data import my-project in.c-sales orders --file-id 123

# Preview data as table
keboola-duckdb tables preview my-project in.c-sales orders --limit 5
# +----+----------+--------+
# | id | customer | amount |
# +----+----------+--------+
# |  1 | Acme     | 100.00 |
# |  2 | Beta     | 250.50 |
# +----+----------+--------+

# Preview as JSON (pipe to jq)
keboola-duckdb tables preview my-project in.c-sales orders --json | jq '.[0]'

# Dry run (see what would happen)
keboola-duckdb projects delete my-project --dry-run
# [DRY RUN] Would delete project 'my-project'

# Skip confirmation
keboola-duckdb projects delete my-project --yes

# Quiet mode (minimal output, good for scripts)
keboola-duckdb files upload my-project data.csv --quiet

# Debug mode (verbose logging)
keboola-duckdb projects list -vvv

# CI/CD mode (no color, JSON output)
KEBOOLA_DUCKDB_NO_COLOR=1 keboola-duckdb projects list --json

# Create snapshot
keboola-duckdb snapshots create my-project in.c-sales orders --name "before-migration"

# Connect to workspace
keboola-duckdb workspaces create my-project --name "analytics"
keboola-duckdb workspaces load my-project ws-123 --tables in.c-sales.orders,in.c-sales.customers
keboola-duckdb workspaces connect my-project ws-123
# Run: psql -h localhost -p 5433 -U ws_user_xxx -d workspace
```

---

## API Coverage

The SDK covers **91 REST endpoints** across these areas:

| Area | Endpoints | Description |
|------|-----------|-------------|
| Backend | 3 | Health, init, remove |
| Projects | 6 | CRUD + stats |
| Buckets | 4 | CRUD (Branch-First) |
| Tables | 5 | CRUD + preview (Branch-First) |
| Table Schema | 7 | Columns, PK, rows, profile |
| Import/Export | 2 | File import/export |
| Files | 7 | 3-stage upload workflow |
| Snapshots | 5 | CRUD + restore |
| Snapshot Settings | 9 | Hierarchical config |
| Dev Branches | 5 | CRUD + pull |
| Bucket Sharing | 6 | Share, link, readonly |
| Workspaces | 11 | CRUD + load + connection info |
| API Keys | 5 | CRUD + rotate |
| S3-Compatible | 5 | GET/PUT/DELETE/HEAD/List |
| PG Wire Auth | 7 | Session management |
| Metrics | 1 | Prometheus |
| gRPC Bridge | 2 | Driver execute |

---

## Dependencies

### SDK Dependencies

```toml
# sdk/pyproject.toml
[project]
name = "keboola-duckdb-sdk"
version = "0.1.0"
requires-python = ">=3.9"
dependencies = [
    "httpx>=0.25.0",
    "pydantic>=2.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "pytest-mock>=3.10.0",
    "respx>=0.20.0",  # httpx mocking
    "syrupy>=4.0.0",  # snapshot testing
]
```

### CLI Dependencies

```toml
# cli/pyproject.toml
[project]
name = "keboola-duckdb-cli"
version = "0.1.0"
requires-python = ">=3.9"
dependencies = [
    "keboola-duckdb-sdk>=0.1.0",
    "typer>=0.9.0",
    "rich>=13.0.0",
    "keyring>=24.0.0",
    "pyyaml>=6.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-mock>=3.10.0",
    "syrupy>=4.0.0",
]

[project.scripts]
keboola-duckdb = "keboola_duckdb_cli.main:app"
```
