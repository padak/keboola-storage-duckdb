"""DuckDB database management - MetadataDB and ProjectDB connections.

ADR-009: Per-table file architecture
====================================
- Project = directory (e.g., /data/duckdb/project_123/)
- Bucket = directory (e.g., /data/duckdb/project_123/in_c_sales/)
- Table = file (e.g., /data/duckdb/project_123/in_c_sales/orders.duckdb)

Each table's data is stored in `main.data` table within its own .duckdb file.
"""

import shutil
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import duckdb
import structlog

from src.config import settings

logger = structlog.get_logger()

# Standard table name within each per-table DuckDB file
TABLE_DATA_NAME = "data"


# ============================================
# Table Lock Manager (Write Queue simplified)
# ============================================


class TableLockManager:
    """
    Per-table write lock manager for DuckDB single-writer constraint.

    ADR-009 + Write Queue simplification:
    - Each table has its own .duckdb file
    - Each file can have only one writer at a time
    - This manager provides per-table mutex locks

    Note: Keboola Storage API serializes writes on their side,
    so this is a safety net for standalone usage or future extensions.
    """

    def __init__(self):
        self._locks: dict[str, threading.Lock] = {}
        self._manager_lock = threading.Lock()  # Protects _locks dict

    def _get_table_key(
        self, project_id: str, bucket_name: str, table_name: str
    ) -> str:
        """Generate unique key for a table."""
        return f"{project_id}/{bucket_name}/{table_name}"

    def get_lock(
        self, project_id: str, bucket_name: str, table_name: str
    ) -> threading.Lock:
        """
        Get or create a lock for a specific table.

        Thread-safe: uses internal lock to protect the locks dictionary.
        """
        key = self._get_table_key(project_id, bucket_name, table_name)

        with self._manager_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
                logger.debug("table_lock_created", table_key=key)
            return self._locks[key]

    @contextmanager
    def acquire(
        self, project_id: str, bucket_name: str, table_name: str
    ) -> Generator[None, None, None]:
        """
        Context manager to acquire a table lock.

        Usage:
            with table_lock_manager.acquire("proj", "bucket", "table"):
                # exclusive access to table
                pass
        """
        # Import here to avoid circular import at module load time
        from src.metrics import (
            TABLE_LOCK_ACQUISITIONS,
            TABLE_LOCK_WAIT_TIME,
            TABLE_LOCKS_ACTIVE,
        )

        lock = self.get_lock(project_id, bucket_name, table_name)
        key = self._get_table_key(project_id, bucket_name, table_name)

        logger.debug("table_lock_acquiring", table_key=key)

        # Measure wait time
        wait_start = time.perf_counter()
        lock.acquire()
        wait_duration = time.perf_counter() - wait_start

        # Record metrics
        TABLE_LOCK_WAIT_TIME.observe(wait_duration)
        TABLE_LOCK_ACQUISITIONS.labels(
            project_id=project_id,
            bucket=bucket_name,
            table=table_name
        ).inc()
        TABLE_LOCKS_ACTIVE.inc()

        logger.debug("table_lock_acquired", table_key=key, wait_ms=wait_duration * 1000)

        try:
            yield
        finally:
            lock.release()
            TABLE_LOCKS_ACTIVE.dec()
            logger.debug("table_lock_released", table_key=key)

    def remove_lock(
        self, project_id: str, bucket_name: str, table_name: str
    ) -> None:
        """
        Remove a lock for a deleted table.

        Called when a table is deleted to clean up resources.
        """
        key = self._get_table_key(project_id, bucket_name, table_name)

        with self._manager_lock:
            if key in self._locks:
                del self._locks[key]
                logger.debug("table_lock_removed", table_key=key)

    def clear_project_locks(self, project_id: str) -> None:
        """
        Remove all locks for a project.

        Called when a project is deleted.
        """
        prefix = f"{project_id}/"

        with self._manager_lock:
            keys_to_remove = [k for k in self._locks if k.startswith(prefix)]
            for key in keys_to_remove:
                del self._locks[key]

            if keys_to_remove:
                logger.debug(
                    "project_locks_cleared",
                    project_id=project_id,
                    count=len(keys_to_remove),
                )

    @property
    def active_locks_count(self) -> int:
        """Return count of tracked locks (for monitoring/debugging)."""
        return len(self._locks)


# Global singleton instance
table_lock_manager = TableLockManager()


# ============================================
# Schema definitions
# ============================================

METADATA_SCHEMA = """
-- Projects registry
CREATE TABLE IF NOT EXISTS projects (
    id VARCHAR PRIMARY KEY,
    name VARCHAR,
    db_path VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ,
    size_bytes BIGINT DEFAULT 0,
    table_count INTEGER DEFAULT 0,
    bucket_count INTEGER DEFAULT 0,
    status VARCHAR DEFAULT 'active',
    settings JSON
);

CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
CREATE INDEX IF NOT EXISTS idx_projects_created ON projects(created_at);

-- Files metadata (S3 replacement for on-prem)
CREATE TABLE IF NOT EXISTS files (
    id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    size_bytes BIGINT NOT NULL,
    content_type VARCHAR,
    checksum_md5 VARCHAR,
    checksum_sha256 VARCHAR,
    is_staged BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,
    tags JSON
);

CREATE INDEX IF NOT EXISTS idx_files_project ON files(project_id);
CREATE INDEX IF NOT EXISTS idx_files_staged ON files(is_staged, expires_at);

-- Operations audit log
CREATE SEQUENCE IF NOT EXISTS operations_log_seq;

CREATE TABLE IF NOT EXISTS operations_log (
    id BIGINT DEFAULT nextval('operations_log_seq') PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT now(),
    request_id VARCHAR,
    project_id VARCHAR,
    operation VARCHAR NOT NULL,
    resource_type VARCHAR,
    resource_id VARCHAR,
    details JSON,
    duration_ms INTEGER,
    status VARCHAR NOT NULL,
    error_message VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_ops_project ON operations_log(project_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_ops_timestamp ON operations_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_ops_request ON operations_log(request_id);

-- Aggregated stats cache
CREATE TABLE IF NOT EXISTS stats (
    key VARCHAR PRIMARY KEY,
    value JSON NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Bucket sharing tracking
-- TODO: Expand this with more detailed sharing permissions when needed
CREATE TABLE IF NOT EXISTS bucket_shares (
    id VARCHAR PRIMARY KEY,
    source_project_id VARCHAR NOT NULL,
    source_bucket_name VARCHAR NOT NULL,
    target_project_id VARCHAR NOT NULL,
    share_type VARCHAR DEFAULT 'readonly',
    created_at TIMESTAMPTZ DEFAULT now(),
    created_by VARCHAR,
    UNIQUE(source_project_id, source_bucket_name, target_project_id)
);

CREATE INDEX IF NOT EXISTS idx_shares_source ON bucket_shares(source_project_id, source_bucket_name);
CREATE INDEX IF NOT EXISTS idx_shares_target ON bucket_shares(target_project_id);

-- Bucket links tracking (for ATTACH operations)
CREATE TABLE IF NOT EXISTS bucket_links (
    id VARCHAR PRIMARY KEY,
    target_project_id VARCHAR NOT NULL,
    target_bucket_name VARCHAR NOT NULL,
    source_project_id VARCHAR NOT NULL,
    source_bucket_name VARCHAR NOT NULL,
    attached_db_alias VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(target_project_id, target_bucket_name)
);

CREATE INDEX IF NOT EXISTS idx_links_target ON bucket_links(target_project_id);
CREATE INDEX IF NOT EXISTS idx_links_source ON bucket_links(source_project_id, source_bucket_name);

-- API keys for authentication
CREATE TABLE IF NOT EXISTS api_keys (
    id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    key_hash VARCHAR(64) NOT NULL,
    key_prefix VARCHAR(30) NOT NULL,
    description VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys(key_prefix);
CREATE INDEX IF NOT EXISTS idx_api_keys_project ON api_keys(project_id);

-- Idempotency keys for HTTP request deduplication
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key VARCHAR PRIMARY KEY,
    method VARCHAR(10) NOT NULL,
    endpoint VARCHAR(500) NOT NULL,
    request_hash VARCHAR(64),
    response_status INTEGER NOT NULL,
    response_body TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys(expires_at);
"""


class MetadataDB:
    """
    Singleton class for managing the central metadata database.

    Thread-safe connection management for the metadata.duckdb file.
    Note: db_path is read from settings on each access to support testing.
    """

    _instance: "MetadataDB | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "MetadataDB":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return

        self._conn: duckdb.DuckDBPyConnection | None = None
        self._conn_lock = threading.Lock()
        self._initialized = True

    @property
    def _db_path(self) -> Path:
        """Get db path from settings (allows runtime override in tests)."""
        return settings.metadata_db_path

    def initialize(self) -> None:
        """Initialize the metadata database and create schema."""
        db_path = self._db_path
        with self._conn_lock:
            # Ensure parent directory exists
            db_path.parent.mkdir(parents=True, exist_ok=True)

            # Connect and create schema
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(METADATA_SCHEMA)
                conn.commit()
                logger.info("metadata_db_schema_created", path=str(db_path))
            finally:
                conn.close()

    @contextmanager
    def connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a connection to the metadata database.

        Usage:
            with metadata_db.connection() as conn:
                conn.execute("SELECT * FROM projects")
        """
        conn = duckdb.connect(str(self._db_path))
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, query: str, params: list | None = None) -> list[tuple]:
        """Execute a query and return results."""
        with self.connection() as conn:
            if params:
                result = conn.execute(query, params).fetchall()
            else:
                result = conn.execute(query).fetchall()
            return result

    def execute_one(self, query: str, params: list | None = None) -> tuple | None:
        """Execute a query and return single result."""
        results = self.execute(query, params)
        return results[0] if results else None

    def execute_write(self, query: str, params: list | None = None) -> None:
        """Execute a write query (INSERT, UPDATE, DELETE)."""
        with self.connection() as conn:
            if params:
                conn.execute(query, params)
            else:
                conn.execute(query)
            conn.commit()

    # ========================================
    # Project operations
    # ========================================

    def create_project(
        self,
        project_id: str,
        name: str | None = None,
        settings_json: dict | None = None,
    ) -> dict[str, Any]:
        """
        Register a new project in metadata database.

        Returns the created project record.

        Note (ADR-009): db_path now stores directory path, not file path.
        """
        # ADR-009: Project is now a directory, not a single file
        db_path = f"project_{project_id}"
        now = datetime.now(timezone.utc)

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO projects (id, name, db_path, created_at, updated_at, settings)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [project_id, name, db_path, now, now, settings_json],
            )
            conn.commit()

            result = conn.execute(
                "SELECT * FROM projects WHERE id = ?", [project_id]
            ).fetchone()

        logger.info("project_registered", project_id=project_id, db_path=db_path)
        return self._row_to_project_dict(result)

    def get_project(self, project_id: str) -> dict[str, Any] | None:
        """Get project by ID."""
        result = self.execute_one(
            "SELECT * FROM projects WHERE id = ?", [project_id]
        )
        return self._row_to_project_dict(result) if result else None

    def list_projects(
        self,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List projects with optional filtering."""
        if status:
            results = self.execute(
                """
                SELECT * FROM projects
                WHERE status = ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                [status, limit, offset],
            )
        else:
            results = self.execute(
                """
                SELECT * FROM projects
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                [limit, offset],
            )

        return [self._row_to_project_dict(row) for row in results]

    def update_project(
        self,
        project_id: str,
        name: str | None = None,
        status: str | None = None,
        size_bytes: int | None = None,
        table_count: int | None = None,
        bucket_count: int | None = None,
    ) -> dict[str, Any] | None:
        """Update project metadata."""
        updates = []
        params = []

        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if size_bytes is not None:
            updates.append("size_bytes = ?")
            params.append(size_bytes)
        if table_count is not None:
            updates.append("table_count = ?")
            params.append(table_count)
        if bucket_count is not None:
            updates.append("bucket_count = ?")
            params.append(bucket_count)

        if not updates:
            return self.get_project(project_id)

        updates.append("updated_at = ?")
        params.append(datetime.now(timezone.utc))
        params.append(project_id)

        query = f"UPDATE projects SET {', '.join(updates)} WHERE id = ?"
        self.execute_write(query, params)

        logger.info("project_updated", project_id=project_id)
        return self.get_project(project_id)

    def delete_project(self, project_id: str) -> bool:
        """Mark project as deleted (soft delete)."""
        self.execute_write(
            "UPDATE projects SET status = 'deleted', updated_at = ? WHERE id = ?",
            [datetime.now(timezone.utc), project_id],
        )
        logger.info("project_deleted", project_id=project_id)
        return True

    def hard_delete_project(self, project_id: str) -> bool:
        """Permanently remove project from metadata (use with caution)."""
        self.execute_write("DELETE FROM projects WHERE id = ?", [project_id])
        logger.info("project_hard_deleted", project_id=project_id)
        return True

    def _row_to_project_dict(self, row: tuple | None) -> dict[str, Any] | None:
        """Convert database row to project dictionary."""
        import json

        if row is None:
            return None

        # Parse settings JSON if it's a string
        settings = row[9]
        if isinstance(settings, str):
            try:
                settings = json.loads(settings)
            except (json.JSONDecodeError, TypeError):
                settings = None

        return {
            "id": row[0],
            "name": row[1],
            "db_path": row[2],
            "created_at": row[3].isoformat() if row[3] else None,
            "updated_at": row[4].isoformat() if row[4] else None,
            "size_bytes": row[5],
            "table_count": row[6],
            "bucket_count": row[7],
            "status": row[8],
            "settings": settings,
        }

    # ========================================
    # Count methods (for metrics)
    # ========================================

    def count_projects(self) -> int:
        """Count total number of projects (excluding deleted)."""
        result = self.execute_one(
            "SELECT COUNT(*) FROM projects WHERE status != 'deleted'"
        )
        return result[0] if result else 0

    def count_buckets(self) -> int:
        """
        Count total number of buckets across all projects.

        ADR-009: Counts directories in all project directories.
        """
        from src.config import settings

        total = 0
        duckdb_dir = settings.duckdb_dir
        if duckdb_dir.exists():
            for project_dir in duckdb_dir.iterdir():
                if project_dir.is_dir() and project_dir.name.startswith("project_"):
                    for item in project_dir.iterdir():
                        if item.is_dir() and not item.name.startswith("_"):
                            total += 1
        return total

    def count_tables(self) -> int:
        """
        Count total number of tables across all projects.

        ADR-009: Counts .duckdb files in all bucket directories.
        """
        from src.config import settings

        total = 0
        duckdb_dir = settings.duckdb_dir
        if duckdb_dir.exists():
            for project_dir in duckdb_dir.iterdir():
                if project_dir.is_dir() and project_dir.name.startswith("project_"):
                    for bucket_dir in project_dir.iterdir():
                        if bucket_dir.is_dir() and not bucket_dir.name.startswith("_"):
                            total += len(list(bucket_dir.glob("*.duckdb")))
        return total

    def count_idempotency_keys(self) -> int:
        """Count current (non-expired) idempotency keys."""
        result = self.execute_one(
            "SELECT COUNT(*) FROM idempotency_keys WHERE expires_at > now()"
        )
        return result[0] if result else 0

    # ========================================
    # Operations logging
    # ========================================

    def log_operation(
        self,
        operation: str,
        status: str,
        project_id: str | None = None,
        request_id: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        details: dict | None = None,
        duration_ms: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """Log an operation to the audit trail."""
        self.execute_write(
            """
            INSERT INTO operations_log
            (request_id, project_id, operation, resource_type, resource_id,
             details, duration_ms, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                request_id,
                project_id,
                operation,
                resource_type,
                resource_id,
                details,
                duration_ms,
                status,
                error_message,
            ],
        )

    # ========================================
    # Bucket sharing operations
    # ========================================

    def create_bucket_share(
        self,
        source_project_id: str,
        source_bucket_name: str,
        target_project_id: str,
        share_type: str = "readonly",
        created_by: str | None = None,
    ) -> str:
        """Create a bucket share record."""
        import uuid

        share_id = str(uuid.uuid4())
        self.execute_write(
            """
            INSERT INTO bucket_shares
            (id, source_project_id, source_bucket_name, target_project_id, share_type, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [share_id, source_project_id, source_bucket_name, target_project_id, share_type, created_by],
        )
        logger.info(
            "bucket_share_created",
            share_id=share_id,
            source_project=source_project_id,
            source_bucket=source_bucket_name,
            target_project=target_project_id,
        )
        return share_id

    def delete_bucket_share(
        self,
        source_project_id: str,
        source_bucket_name: str,
        target_project_id: str,
    ) -> bool:
        """Delete a bucket share record."""
        self.execute_write(
            """
            DELETE FROM bucket_shares
            WHERE source_project_id = ? AND source_bucket_name = ? AND target_project_id = ?
            """,
            [source_project_id, source_bucket_name, target_project_id],
        )
        logger.info(
            "bucket_share_deleted",
            source_project=source_project_id,
            source_bucket=source_bucket_name,
            target_project=target_project_id,
        )
        return True

    def get_bucket_shares(
        self,
        source_project_id: str,
        source_bucket_name: str,
    ) -> list[str]:
        """Get list of project IDs this bucket is shared with."""
        results = self.execute(
            """
            SELECT target_project_id FROM bucket_shares
            WHERE source_project_id = ? AND source_bucket_name = ?
            """,
            [source_project_id, source_bucket_name],
        )
        return [row[0] for row in results]

    def create_bucket_link(
        self,
        target_project_id: str,
        target_bucket_name: str,
        source_project_id: str,
        source_bucket_name: str,
        attached_db_alias: str,
    ) -> str:
        """Create a bucket link record."""
        import uuid

        link_id = str(uuid.uuid4())
        self.execute_write(
            """
            INSERT INTO bucket_links
            (id, target_project_id, target_bucket_name, source_project_id, source_bucket_name, attached_db_alias)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [link_id, target_project_id, target_bucket_name, source_project_id, source_bucket_name, attached_db_alias],
        )
        logger.info(
            "bucket_link_created",
            link_id=link_id,
            target_project=target_project_id,
            target_bucket=target_bucket_name,
            source_project=source_project_id,
            source_bucket=source_bucket_name,
        )
        return link_id

    def get_bucket_link(
        self,
        target_project_id: str,
        target_bucket_name: str,
    ) -> dict[str, Any] | None:
        """Get bucket link information."""
        result = self.execute_one(
            """
            SELECT source_project_id, source_bucket_name, attached_db_alias
            FROM bucket_links
            WHERE target_project_id = ? AND target_bucket_name = ?
            """,
            [target_project_id, target_bucket_name],
        )
        if result:
            return {
                "source_project_id": result[0],
                "source_bucket_name": result[1],
                "attached_db_alias": result[2],
            }
        return None

    def delete_bucket_link(
        self,
        target_project_id: str,
        target_bucket_name: str,
    ) -> bool:
        """Delete a bucket link record."""
        self.execute_write(
            """
            DELETE FROM bucket_links
            WHERE target_project_id = ? AND target_bucket_name = ?
            """,
            [target_project_id, target_bucket_name],
        )
        logger.info(
            "bucket_link_deleted",
            target_project=target_project_id,
            target_bucket=target_bucket_name,
        )
        return True

    # ========================================
    # API key operations
    # ========================================

    def create_api_key(
        self,
        key_id: str,
        project_id: str,
        key_hash: str,
        key_prefix: str,
        description: str | None = None,
    ) -> dict[str, Any]:
        """
        Store a new API key (hashed).

        Args:
            key_id: Unique identifier for the API key
            project_id: The project this key belongs to
            key_hash: SHA-256 hash of the full API key
            key_prefix: First ~30 chars of the API key for lookup
            description: Optional description

        Returns:
            Dict with the created API key record (without the hash)
        """
        now = datetime.now(timezone.utc)

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO api_keys (id, project_id, key_hash, key_prefix, description, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [key_id, project_id, key_hash, key_prefix, description, now],
            )
            conn.commit()

            result = conn.execute(
                "SELECT id, project_id, key_prefix, description, created_at, last_used_at FROM api_keys WHERE id = ?",
                [key_id],
            ).fetchone()

        logger.info(
            "api_key_created",
            key_id=key_id,
            project_id=project_id,
            key_prefix=key_prefix[:10] + "...",
        )

        if result:
            return {
                "id": result[0],
                "project_id": result[1],
                "key_prefix": result[2],
                "description": result[3],
                "created_at": result[4].isoformat() if result[4] else None,
                "last_used_at": result[5].isoformat() if result[5] else None,
            }

        return {}

    def get_api_key_by_prefix(self, key_prefix: str) -> dict[str, Any] | None:
        """
        Find API key by prefix for validation.

        Args:
            key_prefix: The key prefix to search for

        Returns:
            Dict with id, project_id, key_hash, key_prefix, etc. or None if not found
        """
        result = self.execute_one(
            """
            SELECT id, project_id, key_hash, key_prefix, description, created_at, last_used_at
            FROM api_keys
            WHERE key_prefix = ?
            """,
            [key_prefix],
        )

        if result:
            return {
                "id": result[0],
                "project_id": result[1],
                "key_hash": result[2],
                "key_prefix": result[3],
                "description": result[4],
                "created_at": result[5].isoformat() if result[5] else None,
                "last_used_at": result[6].isoformat() if result[6] else None,
            }

        return None

    def get_api_keys_for_project(self, project_id: str) -> list[dict[str, Any]]:
        """
        List all API keys for a project.

        Args:
            project_id: The project ID

        Returns:
            List of API key dicts (without key_hash for security)
        """
        results = self.execute(
            """
            SELECT id, project_id, key_prefix, description, created_at, last_used_at
            FROM api_keys
            WHERE project_id = ?
            ORDER BY created_at DESC
            """,
            [project_id],
        )

        return [
            {
                "id": row[0],
                "project_id": row[1],
                "key_prefix": row[2],
                "description": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
                "last_used_at": row[5].isoformat() if row[5] else None,
            }
            for row in results
        ]

    def update_api_key_last_used(self, key_id: str) -> None:
        """
        Update last_used_at timestamp for an API key.

        Args:
            key_id: The API key ID
        """
        now = datetime.now(timezone.utc)
        self.execute_write(
            "UPDATE api_keys SET last_used_at = ? WHERE id = ?",
            [now, key_id],
        )

        logger.debug("api_key_last_used_updated", key_id=key_id)

    def delete_api_key(self, key_id: str) -> bool:
        """
        Delete an API key.

        Args:
            key_id: The API key ID

        Returns:
            True if key was deleted
        """
        self.execute_write("DELETE FROM api_keys WHERE id = ?", [key_id])

        logger.info("api_key_deleted", key_id=key_id)
        return True

    def delete_project_api_keys(self, project_id: str) -> int:
        """
        Delete all API keys for a project.

        Args:
            project_id: The project ID

        Returns:
            Count of deleted API keys
        """
        # Get count before deletion
        count_result = self.execute_one(
            "SELECT COUNT(*) FROM api_keys WHERE project_id = ?",
            [project_id],
        )
        count = count_result[0] if count_result else 0

        # Delete all keys
        self.execute_write(
            "DELETE FROM api_keys WHERE project_id = ?",
            [project_id],
        )

        logger.info(
            "project_api_keys_deleted",
            project_id=project_id,
            count=count,
        )

        return count

    # ========================================
    # Idempotency key operations
    # ========================================

    def get_idempotency_key(self, key: str) -> dict[str, Any] | None:
        """
        Get an idempotency key record if it exists and hasn't expired.

        Args:
            key: The idempotency key

        Returns:
            Dict with key info including cached response, or None if not found/expired
        """
        result = self.execute_one(
            """
            SELECT key, method, endpoint, request_hash, response_status, response_body,
                   created_at, expires_at
            FROM idempotency_keys
            WHERE key = ? AND expires_at > now()
            """,
            [key],
        )

        if result:
            return {
                "key": result[0],
                "method": result[1],
                "endpoint": result[2],
                "request_hash": result[3],
                "response_status": result[4],
                "response_body": result[5],
                "created_at": result[6].isoformat() if result[6] else None,
                "expires_at": result[7].isoformat() if result[7] else None,
            }

        return None

    def store_idempotency_key(
        self,
        key: str,
        method: str,
        endpoint: str,
        request_hash: str | None,
        response_status: int,
        response_body: str,
        ttl_seconds: int = 600,
    ) -> None:
        """
        Store an idempotency key with its response.

        Args:
            key: The idempotency key
            method: HTTP method (POST, PUT, DELETE)
            endpoint: The request endpoint path
            request_hash: SHA-256 hash of request body (for validation)
            response_status: HTTP status code of the response
            response_body: JSON string of the response body
            ttl_seconds: Time to live in seconds (default 10 minutes)
        """
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl_seconds)

        self.execute_write(
            """
            INSERT INTO idempotency_keys
            (key, method, endpoint, request_hash, response_status, response_body, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (key) DO UPDATE SET
                response_status = EXCLUDED.response_status,
                response_body = EXCLUDED.response_body,
                expires_at = EXCLUDED.expires_at
            """,
            [key, method, endpoint, request_hash, response_status, response_body, now, expires_at],
        )

        logger.debug(
            "idempotency_key_stored",
            key=key[:20] + "...",
            method=method,
            endpoint=endpoint,
            ttl_seconds=ttl_seconds,
        )

    def cleanup_expired_idempotency_keys(self) -> int:
        """
        Delete expired idempotency keys.

        Returns:
            Number of deleted keys
        """
        # Get count before deletion
        count_result = self.execute_one(
            "SELECT COUNT(*) FROM idempotency_keys WHERE expires_at <= now()"
        )
        count = count_result[0] if count_result else 0

        if count > 0:
            self.execute_write(
                "DELETE FROM idempotency_keys WHERE expires_at <= now()"
            )
            logger.info("idempotency_keys_cleaned", count=count)

        return count

    # ========================================
    # Files operations (on-prem S3 replacement)
    # ========================================

    def create_file_record(
        self,
        file_id: str,
        project_id: str,
        name: str,
        path: str,
        size_bytes: int,
        content_type: str | None = None,
        checksum_sha256: str | None = None,
        is_staged: bool = True,
        expires_at: datetime | None = None,
        tags: dict | None = None,
    ) -> dict[str, Any]:
        """
        Create a file record in metadata database.

        Args:
            file_id: Unique file identifier
            project_id: Project the file belongs to
            name: Original filename
            path: Relative path in storage
            size_bytes: File size in bytes
            content_type: MIME type
            checksum_sha256: SHA256 hash of file content
            is_staged: True if file is in staging (not yet registered)
            expires_at: When staging file expires
            tags: Optional tags/metadata

        Returns:
            Created file record dict
        """
        import json

        now = datetime.now(timezone.utc)

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO files (
                    id, project_id, name, path, size_bytes, content_type,
                    checksum_sha256, is_staged, created_at, expires_at, tags
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    file_id, project_id, name, path, size_bytes, content_type,
                    checksum_sha256, is_staged, now, expires_at,
                    json.dumps(tags) if tags else None
                ],
            )
            conn.commit()

            result = conn.execute(
                "SELECT * FROM files WHERE id = ?", [file_id]
            ).fetchone()

        logger.info(
            "file_record_created",
            file_id=file_id,
            project_id=project_id,
            name=name,
            is_staged=is_staged,
        )

        return self._row_to_file_dict(result)

    def get_file(self, file_id: str) -> dict[str, Any] | None:
        """Get file by ID."""
        result = self.execute_one(
            "SELECT * FROM files WHERE id = ?", [file_id]
        )
        return self._row_to_file_dict(result) if result else None

    def get_file_by_project(
        self, project_id: str, file_id: str
    ) -> dict[str, Any] | None:
        """Get file by ID, ensuring it belongs to project."""
        result = self.execute_one(
            "SELECT * FROM files WHERE id = ? AND project_id = ?",
            [file_id, project_id]
        )
        return self._row_to_file_dict(result) if result else None

    def list_files(
        self,
        project_id: str,
        include_staged: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        List files for a project.

        Args:
            project_id: Project ID
            include_staged: If True, include staging files
            limit: Maximum number of files to return
            offset: Offset for pagination

        Returns:
            List of file record dicts
        """
        if include_staged:
            results = self.execute(
                """
                SELECT * FROM files
                WHERE project_id = ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                [project_id, limit, offset],
            )
        else:
            results = self.execute(
                """
                SELECT * FROM files
                WHERE project_id = ? AND is_staged = false
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                [project_id, limit, offset],
            )

        return [self._row_to_file_dict(row) for row in results]

    def count_files(self, project_id: str, include_staged: bool = False) -> int:
        """Count files for a project."""
        if include_staged:
            result = self.execute_one(
                "SELECT COUNT(*) FROM files WHERE project_id = ?",
                [project_id]
            )
        else:
            result = self.execute_one(
                "SELECT COUNT(*) FROM files WHERE project_id = ? AND is_staged = false",
                [project_id]
            )
        return result[0] if result else 0

    def update_file(
        self,
        file_id: str,
        name: str | None = None,
        path: str | None = None,
        size_bytes: int | None = None,
        checksum_sha256: str | None = None,
        is_staged: bool | None = None,
        expires_at: datetime | None = None,
        tags: dict | None = None,
    ) -> dict[str, Any] | None:
        """Update a file record."""
        import json

        updates = []
        params = []

        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if path is not None:
            updates.append("path = ?")
            params.append(path)
        if size_bytes is not None:
            updates.append("size_bytes = ?")
            params.append(size_bytes)
        if checksum_sha256 is not None:
            updates.append("checksum_sha256 = ?")
            params.append(checksum_sha256)
        if is_staged is not None:
            updates.append("is_staged = ?")
            params.append(is_staged)
        if expires_at is not None:
            updates.append("expires_at = ?")
            params.append(expires_at)
        if tags is not None:
            updates.append("tags = ?")
            params.append(json.dumps(tags))

        if not updates:
            return self.get_file(file_id)

        params.append(file_id)

        query = f"UPDATE files SET {', '.join(updates)} WHERE id = ?"
        self.execute_write(query, params)

        logger.info("file_updated", file_id=file_id)
        return self.get_file(file_id)

    def delete_file(self, file_id: str) -> bool:
        """Delete a file record."""
        self.execute_write("DELETE FROM files WHERE id = ?", [file_id])
        logger.info("file_deleted", file_id=file_id)
        return True

    def delete_project_files(self, project_id: str) -> int:
        """Delete all file records for a project."""
        count_result = self.execute_one(
            "SELECT COUNT(*) FROM files WHERE project_id = ?",
            [project_id]
        )
        count = count_result[0] if count_result else 0

        self.execute_write(
            "DELETE FROM files WHERE project_id = ?",
            [project_id]
        )

        logger.info("project_files_deleted", project_id=project_id, count=count)
        return count

    def cleanup_expired_files(self) -> list[dict[str, Any]]:
        """
        Find and return expired staging files for cleanup.

        Returns:
            List of expired file records (caller should delete actual files)
        """
        results = self.execute(
            """
            SELECT * FROM files
            WHERE is_staged = true AND expires_at <= now()
            """
        )

        expired_files = [self._row_to_file_dict(row) for row in results]

        if expired_files:
            # Delete records
            self.execute_write(
                "DELETE FROM files WHERE is_staged = true AND expires_at <= now()"
            )
            logger.info("expired_files_cleaned", count=len(expired_files))

        return expired_files

    def _row_to_file_dict(self, row: tuple | None) -> dict[str, Any] | None:
        """
        Convert database row to file dictionary.

        Schema: id(0), project_id(1), name(2), path(3), size_bytes(4),
                content_type(5), checksum_md5(6), checksum_sha256(7),
                is_staged(8), created_at(9), expires_at(10), tags(11)
        """
        import json

        if row is None:
            return None

        # Parse tags JSON if it's a string
        tags = row[11] if len(row) > 11 else None
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except (json.JSONDecodeError, TypeError):
                tags = None

        return {
            "id": row[0],
            "project_id": row[1],
            "name": row[2],
            "path": row[3],
            "size_bytes": row[4],
            "content_type": row[5],
            "checksum_md5": row[6],
            "checksum_sha256": row[7],
            "is_staged": row[8],
            "created_at": row[9].isoformat() if row[9] else None,
            "expires_at": row[10].isoformat() if row[10] else None,
            "tags": tags,
        }


class ProjectDBManager:
    """
    Manager for project-specific DuckDB databases.

    ADR-009: Per-table file architecture
    ====================================
    - Project = directory (e.g., /data/duckdb/project_123/)
    - Bucket = subdirectory (e.g., /data/duckdb/project_123/in_c_sales/)
    - Table = file (e.g., /data/duckdb/project_123/in_c_sales/orders.duckdb)

    Each table has its own .duckdb file with data in `main.data` table.
    """

    @property
    def _duckdb_dir(self) -> Path:
        """Get duckdb dir from settings (allows runtime override in tests)."""
        return settings.duckdb_dir

    # ========================================
    # Path helpers (ADR-009)
    # ========================================

    def get_project_dir(self, project_id: str) -> Path:
        """Get the directory path for a project."""
        return self._duckdb_dir / f"project_{project_id}"

    def get_bucket_dir(self, project_id: str, bucket_name: str) -> Path:
        """Get the directory path for a bucket within a project."""
        return self.get_project_dir(project_id) / bucket_name

    def get_table_path(
        self, project_id: str, bucket_name: str, table_name: str
    ) -> Path:
        """Get the file path for a table's DuckDB file."""
        return self.get_bucket_dir(project_id, bucket_name) / f"{table_name}.duckdb"

    # Legacy method - returns project directory for backward compatibility
    def get_db_path(self, project_id: str) -> Path:
        """
        Get the path to a project's storage location.

        Note (ADR-009): Now returns directory path, not file path.
        Kept for backward compatibility.
        """
        return self.get_project_dir(project_id)

    # ========================================
    # Project operations (ADR-009)
    # ========================================

    def create_project_db(self, project_id: str) -> Path:
        """
        Create the directory structure for a project.

        ADR-009: Projects are directories, not single files.
        Tables are created individually as needed.

        Returns the path to the created project directory.
        """
        project_dir = self.get_project_dir(project_id)

        # Create project directory
        project_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "project_dir_created", project_id=project_id, path=str(project_dir)
        )

        return project_dir

    def delete_project_db(self, project_id: str) -> bool:
        """
        Delete a project's directory and all its contents.

        ADR-009: Recursively deletes project directory with all buckets and tables.
        Also cleans up all table locks for the project.
        """
        project_dir = self.get_project_dir(project_id)

        if project_dir.exists():
            # Clean up all locks for this project
            table_lock_manager.clear_project_locks(project_id)

            shutil.rmtree(project_dir)
            logger.info(
                "project_dir_deleted", project_id=project_id, path=str(project_dir)
            )
            return True

        logger.warning(
            "project_dir_not_found", project_id=project_id, path=str(project_dir)
        )
        return False

    def project_exists(self, project_id: str) -> bool:
        """Check if a project directory exists."""
        return self.get_project_dir(project_id).is_dir()

    def get_db_size(self, project_id: str) -> int:
        """
        Get the total size of all table files in a project (in bytes).

        ADR-009: Sums sizes of all .duckdb files in project directory.
        """
        project_dir = self.get_project_dir(project_id)
        if not project_dir.exists():
            return 0

        total_size = 0
        for duckdb_file in project_dir.rglob("*.duckdb"):
            total_size += duckdb_file.stat().st_size

        return total_size

    @contextmanager
    def table_connection(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        read_only: bool = False,
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a connection to a specific table's DuckDB file.

        ADR-009: Each table has its own .duckdb file.

        Write operations (read_only=False) acquire a table lock to ensure
        single-writer access. Read operations can run concurrently.

        Usage:
            with project_db.table_connection("123", "bucket", "table") as conn:
                conn.execute("SELECT * FROM main.data")
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        # For write operations, acquire table lock
        if not read_only:
            with table_lock_manager.acquire(project_id, bucket_name, table_name):
                conn = duckdb.connect(str(table_path), read_only=False)
                try:
                    yield conn
                finally:
                    conn.close()
        else:
            # Read operations don't need lock
            conn = duckdb.connect(str(table_path), read_only=True)
            try:
                yield conn
            finally:
                conn.close()

    @contextmanager
    def connection(
        self, project_id: str, read_only: bool = False
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get an in-memory connection for project-level operations.

        ADR-009: Since projects are now directories (not single files),
        this returns an in-memory connection that can ATTACH individual
        table files as needed.

        For table-specific operations, use table_connection() instead.
        """
        project_dir = self.get_project_dir(project_id)

        if not project_dir.exists():
            raise FileNotFoundError(f"Project not found: {project_id}")

        # Create in-memory connection for workspace operations
        conn = duckdb.connect(":memory:")
        try:
            yield conn
        finally:
            conn.close()

    def get_project_stats(self, project_id: str) -> dict[str, Any]:
        """
        Get statistics about a project by scanning the filesystem.

        ADR-009: Counts directories (buckets) and .duckdb files (tables).
        """
        project_dir = self.get_project_dir(project_id)

        if not project_dir.exists():
            return {"bucket_count": 0, "table_count": 0, "size_bytes": 0}

        bucket_count = 0
        table_count = 0
        size_bytes = 0

        # Iterate through project directory
        for item in project_dir.iterdir():
            # Skip hidden/special directories
            if item.name.startswith("_") or item.name.startswith("."):
                continue

            if item.is_dir():
                bucket_count += 1
                # Count .duckdb files in bucket
                for table_file in item.glob("*.duckdb"):
                    table_count += 1
                    size_bytes += table_file.stat().st_size

        return {
            "bucket_count": bucket_count,
            "table_count": table_count,
            "size_bytes": size_bytes,
        }

    # ========================================
    # Bucket operations (ADR-009: directories)
    # ========================================

    def create_bucket(
        self, project_id: str, bucket_name: str, description: str | None = None
    ) -> dict[str, Any]:
        """
        Create a bucket directory in a project.

        ADR-009: Buckets are directories, not DuckDB schemas.

        Returns bucket info dict with name and created status.
        """
        bucket_dir = self.get_bucket_dir(project_id, bucket_name)

        # Create bucket directory
        bucket_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "bucket_created",
            project_id=project_id,
            bucket_name=bucket_name,
            path=str(bucket_dir),
        )

        return {"name": bucket_name, "table_count": 0, "description": description}

    def delete_bucket(
        self, project_id: str, bucket_name: str, cascade: bool = True
    ) -> bool:
        """
        Delete a bucket directory from a project.

        ADR-009: Buckets are directories containing .duckdb table files.

        Args:
            project_id: The project ID
            bucket_name: The bucket name (directory name)
            cascade: If True, delete all tables in the bucket

        Returns True if successful.
        """
        bucket_dir = self.get_bucket_dir(project_id, bucket_name)

        if not bucket_dir.exists():
            logger.warning(
                "bucket_not_found",
                project_id=project_id,
                bucket_name=bucket_name,
            )
            return True  # Idempotent - already deleted

        # Get list of tables before deletion (for lock cleanup)
        table_files = list(bucket_dir.glob("*.duckdb"))

        # Check if bucket has tables and cascade is False
        if not cascade and table_files:
            raise ValueError(
                f"Bucket {bucket_name} is not empty and cascade=False"
            )

        # Clean up locks for all tables in bucket
        for table_file in table_files:
            table_name = table_file.stem  # Remove .duckdb extension
            table_lock_manager.remove_lock(project_id, bucket_name, table_name)

        # Delete bucket directory and all contents
        shutil.rmtree(bucket_dir)

        logger.info(
            "bucket_deleted",
            project_id=project_id,
            bucket_name=bucket_name,
            cascade=cascade,
            tables_removed=len(table_files),
        )
        return True

    def list_buckets(self, project_id: str) -> list[dict[str, Any]]:
        """
        List all buckets in a project by scanning directories.

        ADR-009: Buckets are directories within project directory.

        Returns list of dicts with bucket info.
        """
        project_dir = self.get_project_dir(project_id)

        if not project_dir.exists():
            return []

        buckets = []
        for item in sorted(project_dir.iterdir()):
            # Skip hidden/special directories
            if item.name.startswith("_") or item.name.startswith("."):
                continue

            if item.is_dir():
                # Count .duckdb files in bucket
                table_count = len(list(item.glob("*.duckdb")))
                buckets.append(
                    {
                        "name": item.name,
                        "table_count": table_count,
                        "description": None,
                    }
                )

        return buckets

    def get_bucket(self, project_id: str, bucket_name: str) -> dict[str, Any] | None:
        """
        Get information about a specific bucket.

        ADR-009: Checks if bucket directory exists and counts tables.

        Returns bucket info dict or None if bucket doesn't exist.
        """
        bucket_dir = self.get_bucket_dir(project_id, bucket_name)

        if not bucket_dir.is_dir():
            return None

        # Count .duckdb files in bucket
        table_count = len(list(bucket_dir.glob("*.duckdb")))

        return {"name": bucket_name, "table_count": table_count, "description": None}

    def bucket_exists(self, project_id: str, bucket_name: str) -> bool:
        """Check if a bucket directory exists."""
        return self.get_bucket_dir(project_id, bucket_name).is_dir()

    # ========================================
    # Bucket sharing - ATTACH/DETACH operations (ADR-009)
    # ========================================
    #
    # NOTE: With ADR-009 (per-table files), bucket sharing works differently:
    # - Instead of ATTACHing a single project file, we ATTACH individual table files
    # - Views are created in a target bucket directory as .duckdb files
    # - For linked buckets, we use metadata to route queries to source tables
    #
    # Current implementation is simplified for MVP - stores link info in metadata
    # and resolves at query time.

    def attach_database(
        self,
        target_project_id: str,
        source_project_id: str,
        alias: str,
        read_only: bool = True,
    ) -> None:
        """
        DEPRECATED in ADR-009: Projects are directories, not single files.

        This method is kept for backward compatibility but doesn't do anything
        meaningful with ADR-009 architecture. Use link_bucket_with_views instead.
        """
        logger.warning(
            "attach_database_deprecated",
            message="attach_database is deprecated with ADR-009 per-table architecture",
            target_project=target_project_id,
            source_project=source_project_id,
        )

    def detach_database(self, target_project_id: str, alias: str) -> None:
        """
        DEPRECATED in ADR-009: Projects are directories, not single files.

        This method is kept for backward compatibility but doesn't do anything
        meaningful with ADR-009 architecture.
        """
        logger.warning(
            "detach_database_deprecated",
            message="detach_database is deprecated with ADR-009 per-table architecture",
            target_project=target_project_id,
            alias=alias,
        )

    def link_bucket_with_views(
        self,
        target_project_id: str,
        target_bucket_name: str,
        source_project_id: str,
        source_bucket_name: str,
        source_db_alias: str,
    ) -> list[str]:
        """
        Link a bucket by creating references to source tables.

        ADR-009: With per-table files, linking works by:
        1. Creating the target bucket directory
        2. Storing link metadata (source -> target mapping)
        3. Query routing resolves links at runtime

        Note: This implementation creates symlinks or copies view definitions.
        Full implementation would use a workspace session with ATTACH.

        Args:
            target_project_id: The project where linked bucket will be created
            target_bucket_name: The name for the linked bucket
            source_project_id: The source project ID
            source_bucket_name: The source bucket name
            source_db_alias: Ignored in ADR-009 (kept for API compatibility)

        Returns:
            List of table names that were linked
        """
        # Get list of tables from source bucket (filesystem scan)
        source_bucket_dir = self.get_bucket_dir(source_project_id, source_bucket_name)

        if not source_bucket_dir.exists():
            raise FileNotFoundError(
                f"Source bucket not found: {source_project_id}/{source_bucket_name}"
            )

        # List source tables
        source_tables = [f.stem for f in source_bucket_dir.glob("*.duckdb")]

        if not source_tables:
            logger.warning(
                "link_bucket_empty_source",
                source_project=source_project_id,
                source_bucket=source_bucket_name,
            )
            return []

        # Create target bucket directory
        target_bucket_dir = self.get_bucket_dir(target_project_id, target_bucket_name)
        target_bucket_dir.mkdir(parents=True, exist_ok=True)

        # Note: With ADR-009, linked buckets work through metadata routing
        # The actual query resolution happens at runtime using ATTACH
        # Here we just record which tables are available

        logger.info(
            "bucket_linked_with_views",
            target_project=target_project_id,
            target_bucket=target_bucket_name,
            source_project=source_project_id,
            source_bucket=source_bucket_name,
            table_count=len(source_tables),
        )

        return source_tables

    def create_views_for_bucket(
        self,
        target_project_id: str,
        target_bucket_name: str,
        source_db_alias: str,
        source_bucket_name: str,
    ) -> list[str]:
        """
        DEPRECATED: Use link_bucket_with_views instead.

        With ADR-009, this method is no longer applicable since
        buckets are directories, not schemas in a shared database.
        """
        logger.warning(
            "create_views_for_bucket_deprecated",
            message="create_views_for_bucket is deprecated with ADR-009",
            target_project=target_project_id,
            target_bucket=target_bucket_name,
        )
        return []

    def drop_bucket_views(
        self,
        target_project_id: str,
        target_bucket_name: str,
    ) -> None:
        """
        Drop a linked bucket (cleanup views/references).

        ADR-009: With per-table files, this just removes the linked bucket directory
        (which should be empty if it was only used for links).

        Args:
            target_project_id: The project containing the linked bucket
            target_bucket_name: The linked bucket name
        """
        target_bucket_dir = self.get_bucket_dir(target_project_id, target_bucket_name)

        # For linked buckets, the directory should be empty
        # If it has real tables, don't delete them
        if target_bucket_dir.exists():
            table_files = list(target_bucket_dir.glob("*.duckdb"))
            if table_files:
                logger.warning(
                    "drop_bucket_views_has_tables",
                    message="Linked bucket has table files, not deleting",
                    target_project=target_project_id,
                    target_bucket=target_bucket_name,
                    table_count=len(table_files),
                )
                return

            # Remove empty directory
            try:
                target_bucket_dir.rmdir()
            except OSError:
                pass  # Directory not empty or doesn't exist

        logger.info(
            "bucket_views_dropped",
            target_project=target_project_id,
            target_bucket=target_bucket_name,
        )

    # ========================================
    # Table operations (ADR-009: per-table files)
    # ========================================

    def create_table(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        columns: list[dict[str, Any]],
        primary_key: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Create a table as an individual DuckDB file.

        ADR-009: Each table is stored in its own .duckdb file.
        The table data is stored as `main.{TABLE_DATA_NAME}` within the file.

        Args:
            project_id: The project ID
            bucket_name: The bucket name (directory)
            table_name: The table name (becomes filename.duckdb)
            columns: List of column definitions with keys: name, type, nullable, default
            primary_key: Optional list of column names for primary key

        Returns:
            Table info dict with name, bucket, columns, row_count
        """
        # Ensure bucket directory exists
        bucket_dir = self.get_bucket_dir(project_id, bucket_name)
        bucket_dir.mkdir(parents=True, exist_ok=True)

        # Get table file path
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        # Build column definitions
        col_defs = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            if not col.get("nullable", True):
                col_def += " NOT NULL"
            if col.get("default") is not None:
                col_def += f" DEFAULT {col['default']}"
            col_defs.append(col_def)

        # Add primary key constraint if specified
        if primary_key:
            pk_cols = ", ".join(primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ", ".join(col_defs)
        # ADR-009: Table is stored as main.{TABLE_DATA_NAME} in its own file
        create_sql = f"CREATE TABLE main.{TABLE_DATA_NAME} ({columns_sql})"

        # Create new DuckDB file for this table
        conn = duckdb.connect(str(table_path))
        try:
            conn.execute(f"SET threads = {settings.duckdb_threads}")
            conn.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
            conn.execute(create_sql)
            conn.commit()
        finally:
            conn.close()

        logger.info(
            "table_created",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            path=str(table_path),
            column_count=len(columns),
            primary_key=primary_key,
        )

        # Return table info
        return self.get_table(project_id, bucket_name, table_name)

    def delete_table(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
    ) -> bool:
        """
        Delete a table by removing its DuckDB file.

        ADR-009: Tables are individual files, so deletion is file removal.

        Args:
            project_id: The project ID
            bucket_name: The bucket name (directory)
            table_name: The table name

        Returns:
            True if successful
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if table_path.exists():
            table_path.unlink()
            # Clean up lock for deleted table
            table_lock_manager.remove_lock(project_id, bucket_name, table_name)
            logger.info(
                "table_deleted",
                project_id=project_id,
                bucket_name=bucket_name,
                table_name=table_name,
                path=str(table_path),
            )
        else:
            logger.warning(
                "table_not_found",
                project_id=project_id,
                bucket_name=bucket_name,
                table_name=table_name,
            )

        return True

    def get_table(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
    ) -> dict[str, Any] | None:
        """
        Get information about a specific table (ObjectInfo).

        ADR-009: Opens the table's individual DuckDB file to query metadata.

        Args:
            project_id: The project ID
            bucket_name: The bucket name (directory)
            table_name: The table name

        Returns:
            Table info dict or None if not found
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            return None

        conn = duckdb.connect(str(table_path), read_only=True)
        try:
            # Check if data table exists (ADR-009: table is main.{TABLE_DATA_NAME})
            table_result = conn.execute(
                f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = '{TABLE_DATA_NAME}'
                  AND table_type = 'BASE TABLE'
                """
            ).fetchone()

            if not table_result:
                return None

            # Get column information
            columns_result = conn.execute(
                f"""
                SELECT column_name, data_type, is_nullable, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{TABLE_DATA_NAME}'
                ORDER BY ordinal_position
                """
            ).fetchall()

            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "ordinal_position": row[3],
                }
                for row in columns_result
            ]

            # Get row count
            row_count_result = conn.execute(
                f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
            ).fetchone()
            row_count = row_count_result[0] if row_count_result else 0

            # Try to get primary key info from duckdb_constraints
            primary_key = []
            try:
                pk_result = conn.execute(
                    f"""
                    SELECT constraint_column_names
                    FROM duckdb_constraints()
                    WHERE schema_name = 'main' AND table_name = '{TABLE_DATA_NAME}'
                      AND constraint_type = 'PRIMARY KEY'
                    """
                ).fetchone()
                if pk_result and pk_result[0]:
                    primary_key = list(pk_result[0])
            except Exception:
                pass

            # ADR-009: Table size is the file size
            size_bytes = table_path.stat().st_size

            return {
                "name": table_name,
                "bucket": bucket_name,
                "columns": columns,
                "row_count": row_count,
                "size_bytes": size_bytes,
                "primary_key": primary_key,
                "created_at": None,
            }
        finally:
            conn.close()

    def list_tables(
        self,
        project_id: str,
        bucket_name: str,
    ) -> list[dict[str, Any]]:
        """
        List all tables in a bucket by scanning .duckdb files.

        ADR-009: Tables are .duckdb files in the bucket directory.

        Args:
            project_id: The project ID
            bucket_name: The bucket name (directory)

        Returns:
            List of table info dicts
        """
        bucket_dir = self.get_bucket_dir(project_id, bucket_name)

        if not bucket_dir.exists():
            return []

        tables = []
        for table_file in sorted(bucket_dir.glob("*.duckdb")):
            table_name = table_file.stem  # filename without .duckdb
            table_info = self.get_table(project_id, bucket_name, table_name)
            if table_info:
                tables.append(table_info)

        return tables

    def table_exists(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
    ) -> bool:
        """
        Check if a table exists by checking if its file exists.

        ADR-009: Table existence = file existence.

        Args:
            project_id: The project ID
            bucket_name: The bucket name (directory)
            table_name: The table name

        Returns:
            True if table file exists
        """
        return self.get_table_path(project_id, bucket_name, table_name).exists()

    def get_table_preview(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """
        Get a preview of table data.

        ADR-009: Opens the table's individual DuckDB file to query data.

        Args:
            project_id: The project ID
            bucket_name: The bucket name (directory)
            table_name: The table name
            limit: Maximum number of rows to return (default 1000)

        Returns:
            Dict with columns, rows, total_row_count, preview_row_count
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        conn = duckdb.connect(str(table_path), read_only=True)
        try:
            # Get column information
            columns_result = conn.execute(
                f"""
                SELECT column_name, data_type, is_nullable, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{TABLE_DATA_NAME}'
                ORDER BY ordinal_position
                """
            ).fetchall()

            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "ordinal_position": row[3],
                }
                for row in columns_result
            ]

            # Get total row count
            total_count_result = conn.execute(
                f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
            ).fetchone()
            total_row_count = total_count_result[0] if total_count_result else 0

            # Get preview rows
            preview_result = conn.execute(
                f"SELECT * FROM main.{TABLE_DATA_NAME} LIMIT {limit}"
            ).fetchall()

            # Get column names for row dicts
            col_names = [col["name"] for col in columns]

            # Convert rows to list of dicts
            rows = []
            for row in preview_result:
                row_dict = {}
                for i, val in enumerate(row):
                    # Handle special types for JSON serialization
                    if isinstance(val, datetime):
                        row_dict[col_names[i]] = val.isoformat()
                    elif hasattr(val, "__str__") and not isinstance(
                        val, (str, int, float, bool, type(None), list, dict)
                    ):
                        row_dict[col_names[i]] = str(val)
                    else:
                        row_dict[col_names[i]] = val
                rows.append(row_dict)

            return {
                "columns": columns,
                "rows": rows,
                "total_row_count": total_row_count,
                "preview_row_count": len(rows),
            }
        finally:
            conn.close()


    # ========================================
    # Table schema operations (ADR-009: per-table files)
    # ========================================

    def add_column(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        column_name: str,
        column_type: str,
        nullable: bool = True,
        default: str | None = None,
    ) -> dict[str, Any]:
        """
        Add a column to a table.

        Args:
            project_id: The project ID
            bucket_name: The bucket name
            table_name: The table name
            column_name: Name for the new column
            column_type: DuckDB data type
            nullable: Whether column allows NULL
            default: Default value expression

        Returns:
            Updated table info dict
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        # Build ALTER TABLE statement
        col_def = f"{column_name} {column_type}"
        if not nullable:
            col_def += " NOT NULL"
        if default is not None:
            col_def += f" DEFAULT {default}"

        alter_sql = f"ALTER TABLE main.{TABLE_DATA_NAME} ADD COLUMN {col_def}"

        # Execute with table lock
        with table_lock_manager.acquire(project_id, bucket_name, table_name):
            conn = duckdb.connect(str(table_path))
            try:
                conn.execute(alter_sql)
                conn.commit()
            finally:
                conn.close()

        logger.info(
            "column_added",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            column_type=column_type,
        )

        return self.get_table(project_id, bucket_name, table_name)

    def drop_column(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        column_name: str,
    ) -> dict[str, Any]:
        """
        Drop a column from a table.

        Args:
            project_id: The project ID
            bucket_name: The bucket name
            table_name: The table name
            column_name: Name of column to drop

        Returns:
            Updated table info dict
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        alter_sql = f"ALTER TABLE main.{TABLE_DATA_NAME} DROP COLUMN {column_name}"

        # Execute with table lock
        with table_lock_manager.acquire(project_id, bucket_name, table_name):
            conn = duckdb.connect(str(table_path))
            try:
                conn.execute(alter_sql)
                conn.commit()
            finally:
                conn.close()

        logger.info(
            "column_dropped",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
        )

        return self.get_table(project_id, bucket_name, table_name)

    def alter_column(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        column_name: str,
        new_name: str | None = None,
        new_type: str | None = None,
        set_not_null: bool | None = None,
        set_default: str | None = None,
    ) -> dict[str, Any]:
        """
        Alter a column in a table.

        Supports: rename, type change, NOT NULL constraint, default value.

        Args:
            project_id: The project ID
            bucket_name: The bucket name
            table_name: The table name
            column_name: Current column name
            new_name: New column name (for rename)
            new_type: New data type
            set_not_null: True to add NOT NULL, False to drop it
            set_default: New default value (empty string to drop)

        Returns:
            Updated table info dict
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        # Build ALTER statements
        alter_statements = []

        if new_name is not None:
            alter_statements.append(
                f"ALTER TABLE main.{TABLE_DATA_NAME} RENAME COLUMN {column_name} TO {new_name}"
            )
            # Update column_name for subsequent operations
            column_name = new_name

        if new_type is not None:
            alter_statements.append(
                f"ALTER TABLE main.{TABLE_DATA_NAME} ALTER COLUMN {column_name} SET DATA TYPE {new_type}"
            )

        if set_not_null is True:
            alter_statements.append(
                f"ALTER TABLE main.{TABLE_DATA_NAME} ALTER COLUMN {column_name} SET NOT NULL"
            )
        elif set_not_null is False:
            alter_statements.append(
                f"ALTER TABLE main.{TABLE_DATA_NAME} ALTER COLUMN {column_name} DROP NOT NULL"
            )

        if set_default is not None:
            if set_default == "":
                alter_statements.append(
                    f"ALTER TABLE main.{TABLE_DATA_NAME} ALTER COLUMN {column_name} DROP DEFAULT"
                )
            else:
                alter_statements.append(
                    f"ALTER TABLE main.{TABLE_DATA_NAME} ALTER COLUMN {column_name} SET DEFAULT {set_default}"
                )

        if not alter_statements:
            # Nothing to do
            return self.get_table(project_id, bucket_name, table_name)

        # Execute with table lock
        with table_lock_manager.acquire(project_id, bucket_name, table_name):
            conn = duckdb.connect(str(table_path))
            try:
                for sql in alter_statements:
                    conn.execute(sql)
                conn.commit()
            finally:
                conn.close()

        logger.info(
            "column_altered",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            changes=len(alter_statements),
        )

        return self.get_table(project_id, bucket_name, table_name)

    def add_primary_key(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        columns: list[str],
    ) -> dict[str, Any]:
        """
        Add a primary key constraint to a table.

        Note: DuckDB doesn't support adding PK to existing table with data.
        This recreates the table with the PK constraint.

        Args:
            project_id: The project ID
            bucket_name: The bucket name
            table_name: The table name
            columns: Column names for the primary key

        Returns:
            Updated table info dict
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        pk_cols = ", ".join(columns)

        # DuckDB requires recreating the table to add PK
        # We'll use a temp table approach
        with table_lock_manager.acquire(project_id, bucket_name, table_name):
            conn = duckdb.connect(str(table_path))
            try:
                # Check if PK already exists
                pk_result = conn.execute(
                    f"""
                    SELECT constraint_column_names
                    FROM duckdb_constraints()
                    WHERE schema_name = 'main' AND table_name = '{TABLE_DATA_NAME}'
                      AND constraint_type = 'PRIMARY KEY'
                    """
                ).fetchone()

                if pk_result:
                    raise ValueError(
                        f"Table already has a primary key: {list(pk_result[0])}"
                    )

                # Get current table schema
                columns_info = conn.execute(
                    f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = '{TABLE_DATA_NAME}'
                    ORDER BY ordinal_position
                    """
                ).fetchall()

                # Build new table definition with PK
                col_defs = []
                for col in columns_info:
                    col_def = f"{col[0]} {col[1]}"
                    if col[2] == "NO":
                        col_def += " NOT NULL"
                    col_defs.append(col_def)
                col_defs.append(f"PRIMARY KEY ({pk_cols})")

                # Recreate table with PK
                conn.execute(f"ALTER TABLE main.{TABLE_DATA_NAME} RENAME TO _temp_data")
                conn.execute(
                    f"CREATE TABLE main.{TABLE_DATA_NAME} ({', '.join(col_defs)})"
                )
                conn.execute(
                    f"INSERT INTO main.{TABLE_DATA_NAME} SELECT * FROM main._temp_data"
                )
                conn.execute("DROP TABLE main._temp_data")
                conn.commit()

            finally:
                conn.close()

        logger.info(
            "primary_key_added",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            columns=columns,
        )

        return self.get_table(project_id, bucket_name, table_name)

    def drop_primary_key(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
    ) -> dict[str, Any]:
        """
        Drop the primary key constraint from a table.

        Note: DuckDB doesn't support DROP CONSTRAINT for PK.
        This recreates the table without the PK constraint.

        Args:
            project_id: The project ID
            bucket_name: The bucket name
            table_name: The table name

        Returns:
            Updated table info dict
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        with table_lock_manager.acquire(project_id, bucket_name, table_name):
            conn = duckdb.connect(str(table_path))
            try:
                # Check if PK exists
                pk_result = conn.execute(
                    f"""
                    SELECT constraint_column_names
                    FROM duckdb_constraints()
                    WHERE schema_name = 'main' AND table_name = '{TABLE_DATA_NAME}'
                      AND constraint_type = 'PRIMARY KEY'
                    """
                ).fetchone()

                if not pk_result:
                    raise ValueError("Table does not have a primary key")

                # Get current table schema
                columns_info = conn.execute(
                    f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = '{TABLE_DATA_NAME}'
                    ORDER BY ordinal_position
                    """
                ).fetchall()

                # Build new table definition without PK
                col_defs = []
                for col in columns_info:
                    col_def = f"{col[0]} {col[1]}"
                    # Keep NOT NULL for non-nullable columns, but remove from PK columns
                    if col[2] == "NO":
                        col_def += " NOT NULL"
                    col_defs.append(col_def)

                # Recreate table without PK
                conn.execute(f"ALTER TABLE main.{TABLE_DATA_NAME} RENAME TO _temp_data")
                conn.execute(
                    f"CREATE TABLE main.{TABLE_DATA_NAME} ({', '.join(col_defs)})"
                )
                conn.execute(
                    f"INSERT INTO main.{TABLE_DATA_NAME} SELECT * FROM main._temp_data"
                )
                conn.execute("DROP TABLE main._temp_data")
                conn.commit()

            finally:
                conn.close()

        logger.info(
            "primary_key_dropped",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )

        return self.get_table(project_id, bucket_name, table_name)

    def delete_table_rows(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        where_clause: str,
    ) -> dict[str, Any]:
        """
        Delete rows from a table matching a WHERE condition.

        Args:
            project_id: The project ID
            bucket_name: The bucket name
            table_name: The table name
            where_clause: SQL WHERE condition (without 'WHERE' keyword)

        Returns:
            Dict with deleted_rows count and table_rows_after
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        # Basic SQL injection prevention - check for dangerous patterns
        dangerous_patterns = [";", "--", "/*", "*/", "drop ", "truncate ", "alter "]
        where_lower = where_clause.lower()
        for pattern in dangerous_patterns:
            if pattern in where_lower:
                raise ValueError(f"Invalid WHERE clause: contains '{pattern}'")

        delete_sql = f"DELETE FROM main.{TABLE_DATA_NAME} WHERE {where_clause}"

        with table_lock_manager.acquire(project_id, bucket_name, table_name):
            conn = duckdb.connect(str(table_path))
            try:
                # Get count before
                count_before = conn.execute(
                    f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
                ).fetchone()[0]

                # Execute delete
                conn.execute(delete_sql)
                conn.commit()

                # Get count after
                count_after = conn.execute(
                    f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
                ).fetchone()[0]

                deleted_rows = count_before - count_after

            finally:
                conn.close()

        logger.info(
            "table_rows_deleted",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            deleted_rows=deleted_rows,
            rows_remaining=count_after,
        )

        return {
            "deleted_rows": deleted_rows,
            "table_rows_after": count_after,
        }

    def get_table_profile(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
    ) -> dict[str, Any]:
        """
        Get statistical profile of a table using DuckDB's SUMMARIZE.

        Args:
            project_id: The project ID
            bucket_name: The bucket name
            table_name: The table name

        Returns:
            Dict with table info and per-column statistics
        """
        table_path = self.get_table_path(project_id, bucket_name, table_name)

        if not table_path.exists():
            raise FileNotFoundError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        conn = duckdb.connect(str(table_path), read_only=True)
        try:
            # Get row count
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
            ).fetchone()[0]

            # Get column count
            column_count_result = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{TABLE_DATA_NAME}'
                """
            ).fetchone()
            column_count = column_count_result[0] if column_count_result else 0

            # Run SUMMARIZE to get statistics
            # SUMMARIZE returns columns: column_name, column_type, min, max, approx_unique, avg, std, q25, q50, q75, count, null_percentage
            summarize_result = conn.execute(
                f"SUMMARIZE main.{TABLE_DATA_NAME}"
            ).fetchall()

            statistics = []
            for row in summarize_result:
                stat = {
                    "column_name": row[0],
                    "column_type": row[1],
                    "min": self._serialize_value(row[2]),
                    "max": self._serialize_value(row[3]),
                    "approx_unique": row[4],
                    "avg": row[5],
                    "std": row[6],
                    "q25": self._serialize_value(row[7]),
                    "q50": self._serialize_value(row[8]),
                    "q75": self._serialize_value(row[9]),
                    "count": row[10],
                    "null_percentage": row[11],
                }
                statistics.append(stat)

            return {
                "table_name": table_name,
                "bucket_name": bucket_name,
                "row_count": row_count,
                "column_count": column_count,
                "statistics": statistics,
            }

        finally:
            conn.close()

    def _serialize_value(self, val: Any) -> Any:
        """Serialize a value for JSON response."""
        if val is None:
            return None
        if isinstance(val, datetime):
            return val.isoformat()
        if hasattr(val, "__str__") and not isinstance(
            val, (str, int, float, bool, type(None), list, dict)
        ):
            return str(val)
        return val


# Global instances
metadata_db = MetadataDB()
project_db_manager = ProjectDBManager()
