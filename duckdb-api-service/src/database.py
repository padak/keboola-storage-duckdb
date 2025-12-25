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
from src import metrics

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

-- Snapshot settings (hierarchical configuration: project -> bucket -> table)
-- ADR-004 extension: Per-entity snapshot configuration with inheritance
CREATE TABLE IF NOT EXISTS snapshot_settings (
    id VARCHAR PRIMARY KEY,
    entity_type VARCHAR NOT NULL,        -- 'project' | 'bucket' | 'table'
    entity_id VARCHAR NOT NULL,          -- project_id | project_id/bucket | project_id/bucket/table
    project_id VARCHAR NOT NULL,         -- Always filled (for FK and queries)
    config JSON NOT NULL,                -- Partial config (only explicit values)
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_settings_entity ON snapshot_settings(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_settings_project ON snapshot_settings(project_id);

-- Snapshots registry (point-in-time backups as Parquet files)
-- ADR-004: Snapshots stored as Parquet with ZSTD compression
CREATE TABLE IF NOT EXISTS snapshots (
    id VARCHAR PRIMARY KEY,              -- snap_{table}_{timestamp}
    project_id VARCHAR NOT NULL,
    bucket_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    snapshot_type VARCHAR NOT NULL,      -- 'manual' | 'auto_predrop' | 'auto_pretruncate' | ...
    parquet_path VARCHAR NOT NULL,       -- Relative path to snapshot directory
    row_count BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    schema_json JSON NOT NULL,           -- Column definitions for restore
    created_at TIMESTAMPTZ DEFAULT now(),
    created_by VARCHAR,
    expires_at TIMESTAMPTZ,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_snapshots_project ON snapshots(project_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_table ON snapshots(project_id, bucket_name, table_name);
CREATE INDEX IF NOT EXISTS idx_snapshots_expires ON snapshots(expires_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_type ON snapshots(snapshot_type);

-- Dev branches (ADR-007: CoW branching)
CREATE TABLE IF NOT EXISTS branches (
    id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    created_by VARCHAR,
    description TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE INDEX IF NOT EXISTS idx_branches_project ON branches(project_id);

-- API keys for authentication
-- Note: Placed after branches table to satisfy FOREIGN KEY constraint
CREATE TABLE IF NOT EXISTS api_keys (
    id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    branch_id VARCHAR,                    -- NULL for project-wide keys
    key_hash VARCHAR(64) NOT NULL,
    key_prefix VARCHAR(50) NOT NULL,      -- Extended for branch key prefixes
    scope VARCHAR(20) NOT NULL DEFAULT 'project_admin',  -- 'project_admin', 'branch_admin', 'branch_read'
    description VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,               -- Optional expiration
    revoked_at TIMESTAMPTZ,               -- Soft delete (revoked keys)
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (branch_id) REFERENCES branches(id)
);

CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys(key_prefix);
CREATE INDEX IF NOT EXISTS idx_api_keys_project ON api_keys(project_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_branch ON api_keys(branch_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_revoked ON api_keys(revoked_at);

-- Track which tables have been copied to branch (Copy-on-Write tracking)
CREATE TABLE IF NOT EXISTS branch_tables (
    branch_id VARCHAR NOT NULL,
    bucket_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    copied_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (branch_id, bucket_name, table_name),
    FOREIGN KEY (branch_id) REFERENCES branches(id)
);

CREATE INDEX IF NOT EXISTS idx_branch_tables_branch ON branch_tables(branch_id);

-- Workspaces for data transformation
CREATE TABLE IF NOT EXISTS workspaces (
    id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    branch_id VARCHAR,                  -- NULL for main branch workspaces
    name VARCHAR NOT NULL,
    db_path VARCHAR NOT NULL,           -- Path to workspace .duckdb file
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,             -- TTL expiration
    size_limit_bytes BIGINT DEFAULT 10737418240,  -- 10GB default
    status VARCHAR DEFAULT 'active',    -- active, expired, error
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (branch_id) REFERENCES branches(id)
);

CREATE INDEX IF NOT EXISTS idx_workspaces_project ON workspaces(project_id);
CREATE INDEX IF NOT EXISTS idx_workspaces_branch ON workspaces(branch_id);
CREATE INDEX IF NOT EXISTS idx_workspaces_expires ON workspaces(expires_at);
CREATE INDEX IF NOT EXISTS idx_workspaces_status ON workspaces(status);

-- Workspace credentials for PG Wire authentication
-- Note: DuckDB doesn't support CASCADE on FK, so deletion is handled manually
CREATE TABLE IF NOT EXISTS workspace_credentials (
    workspace_id VARCHAR PRIMARY KEY,
    username VARCHAR NOT NULL UNIQUE,   -- ws_{workspace_id}_{random}
    password_hash VARCHAR NOT NULL,     -- SHA256 hash
    created_at TIMESTAMPTZ DEFAULT now(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_creds_username ON workspace_credentials(username);

-- PG Wire sessions tracking (Phase 11b)
-- Tracks active PostgreSQL Wire Protocol connections to workspaces
CREATE TABLE IF NOT EXISTS pgwire_sessions (
    session_id VARCHAR PRIMARY KEY,       -- Unique session identifier
    workspace_id VARCHAR NOT NULL,         -- Link to workspace
    client_ip VARCHAR,                     -- Client IP for auditing
    connected_at TIMESTAMPTZ DEFAULT now(), -- Connection start time
    last_activity_at TIMESTAMPTZ DEFAULT now(), -- Last query time
    query_count INTEGER DEFAULT 0,         -- Number of queries executed
    status VARCHAR DEFAULT 'active',       -- active, disconnected, timeout
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
);

CREATE INDEX IF NOT EXISTS idx_pgwire_sessions_workspace ON pgwire_sessions(workspace_id);
CREATE INDEX IF NOT EXISTS idx_pgwire_sessions_status ON pgwire_sessions(status);
CREATE INDEX IF NOT EXISTS idx_pgwire_sessions_activity ON pgwire_sessions(last_activity_at);
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
                # Run migrations BEFORE creating schema
                # This allows us to migrate existing tables before CREATE TABLE IF NOT EXISTS
                self._migrate_api_keys_schema(conn)

                # Create/update schema
                conn.execute(METADATA_SCHEMA)
                conn.commit()
                logger.info("metadata_db_schema_created", path=str(db_path))
            finally:
                conn.close()

    def _migrate_api_keys_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Migrate api_keys table to new schema if needed.

        Adds: branch_id, scope, expires_at, revoked_at columns if they don't exist.
        Extends key_prefix from VARCHAR(30) to VARCHAR(50).
        """
        # Check if we need to migrate by checking if branch_id column exists
        try:
            result = conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'api_keys'"
            ).fetchall()
            columns = {row[0] for row in result}

            if not columns:
                # Table doesn't exist yet, will be created by METADATA_SCHEMA
                return

            needs_migration = False

            # Check if new columns exist
            if 'branch_id' not in columns:
                needs_migration = True
                logger.info("api_keys_migration_needed", reason="missing_branch_id_column")

            if not needs_migration:
                logger.debug("api_keys_schema_up_to_date")
                return

            # Perform migration: recreate table with new schema
            logger.info("api_keys_migration_started")

            # 1. Drop old indexes first (required before renaming table)
            conn.execute("DROP INDEX IF EXISTS idx_api_keys_prefix")
            conn.execute("DROP INDEX IF EXISTS idx_api_keys_project")

            # 2. Rename old table
            conn.execute("ALTER TABLE api_keys RENAME TO api_keys_old")

            # 3. Create new table with updated schema
            # Re-run just the api_keys portion of the schema
            conn.execute("""
                CREATE TABLE api_keys (
                    id VARCHAR PRIMARY KEY,
                    project_id VARCHAR NOT NULL,
                    branch_id VARCHAR,
                    key_hash VARCHAR(64) NOT NULL,
                    key_prefix VARCHAR(50) NOT NULL,
                    scope VARCHAR(20) NOT NULL DEFAULT 'project_admin',
                    description VARCHAR(255),
                    created_at TIMESTAMPTZ DEFAULT now(),
                    last_used_at TIMESTAMPTZ,
                    expires_at TIMESTAMPTZ,
                    revoked_at TIMESTAMPTZ,
                    FOREIGN KEY (project_id) REFERENCES projects(id),
                    FOREIGN KEY (branch_id) REFERENCES branches(id)
                );

                CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);
                CREATE INDEX idx_api_keys_project ON api_keys(project_id);
                CREATE INDEX idx_api_keys_branch ON api_keys(branch_id);
                CREATE INDEX idx_api_keys_revoked ON api_keys(revoked_at);
            """)

            # 4. Migrate data from old table
            conn.execute("""
                INSERT INTO api_keys (id, project_id, branch_id, key_hash, key_prefix,
                                      scope, description, created_at, last_used_at)
                SELECT id, project_id, NULL, key_hash, key_prefix,
                       'project_admin', description, created_at, last_used_at
                FROM api_keys_old
            """)

            # 5. Drop old table
            conn.execute("DROP TABLE api_keys_old")

            conn.commit()
            logger.info("api_keys_migration_completed")

        except Exception as e:
            logger.error("api_keys_migration_failed", error=str(e))
            # Rollback if possible
            try:
                conn.execute("DROP TABLE IF EXISTS api_keys")
                conn.execute("ALTER TABLE api_keys_old RENAME TO api_keys")
                conn.commit()
                logger.info("api_keys_migration_rolled_back")
            except Exception:
                pass
            raise

    @contextmanager
    def connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a connection to the metadata database.

        Usage:
            with metadata_db.connection() as conn:
                conn.execute("SELECT * FROM projects")
        """
        metrics.METADATA_CONNECTIONS_ACTIVE.inc()
        conn = duckdb.connect(str(self._db_path))
        try:
            yield conn
        finally:
            conn.close()
            metrics.METADATA_CONNECTIONS_ACTIVE.dec()

    def execute(self, query: str, params: list | None = None) -> list[tuple]:
        """Execute a read query and return results."""
        start_time = time.time()
        try:
            with self.connection() as conn:
                if params:
                    result = conn.execute(query, params).fetchall()
                else:
                    result = conn.execute(query).fetchall()
                return result
        finally:
            duration = time.time() - start_time
            metrics.METADATA_QUERIES_TOTAL.labels(operation="read").inc()
            metrics.METADATA_QUERY_DURATION.labels(operation="read").observe(duration)

    def execute_one(self, query: str, params: list | None = None) -> tuple | None:
        """Execute a query and return single result."""
        results = self.execute(query, params)
        return results[0] if results else None

    def execute_write(self, query: str, params: list | None = None) -> None:
        """Execute a write query (INSERT, UPDATE, DELETE)."""
        start_time = time.time()
        try:
            with self.connection() as conn:
                if params:
                    conn.execute(query, params)
                else:
                    conn.execute(query)
                conn.commit()
        finally:
            duration = time.time() - start_time
            metrics.METADATA_QUERIES_TOTAL.labels(operation="write").inc()
            metrics.METADATA_QUERY_DURATION.labels(operation="write").observe(duration)

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

    def cascade_delete_project_metadata(self, project_id: str) -> dict[str, int]:
        """
        Delete all metadata records related to a project (cascading delete).

        Order respects foreign key constraints:
        1. pgwire_sessions (FK -> workspaces)
        2. workspace_credentials (FK -> workspaces)
        3. workspaces (FK -> projects, branches)
        4. branch_tables (FK -> branches)
        5. snapshot_settings (FK -> projects, branches)
        6. snapshots (FK -> projects, branches)
        7. files (FK -> projects, branches)
        8. branches (FK -> projects)
        9. api_keys (FK -> projects)

        Returns:
            Dict with counts of deleted records per table
        """
        counts: dict[str, int] = {}

        with self.connection() as conn:
            # 1. Delete pgwire_sessions for workspaces in this project
            result = conn.execute(
                """
                DELETE FROM pgwire_sessions
                WHERE workspace_id IN (
                    SELECT id FROM workspaces WHERE project_id = ?
                )
                """,
                [project_id],
            )
            counts["pgwire_sessions"] = result.rowcount

            # 2. Delete workspace_credentials for workspaces in this project
            result = conn.execute(
                """
                DELETE FROM workspace_credentials
                WHERE workspace_id IN (
                    SELECT id FROM workspaces WHERE project_id = ?
                )
                """,
                [project_id],
            )
            counts["workspace_credentials"] = result.rowcount

            # 3. Delete workspaces
            result = conn.execute(
                "DELETE FROM workspaces WHERE project_id = ?",
                [project_id],
            )
            counts["workspaces"] = result.rowcount

            # 4. Delete branch_tables for branches in this project
            result = conn.execute(
                """
                DELETE FROM branch_tables
                WHERE branch_id IN (
                    SELECT id FROM branches WHERE project_id = ?
                )
                """,
                [project_id],
            )
            counts["branch_tables"] = result.rowcount

            # 5. Delete snapshot_settings
            result = conn.execute(
                "DELETE FROM snapshot_settings WHERE project_id = ?",
                [project_id],
            )
            counts["snapshot_settings"] = result.rowcount

            # 6. Delete snapshots
            result = conn.execute(
                "DELETE FROM snapshots WHERE project_id = ?",
                [project_id],
            )
            counts["snapshots"] = result.rowcount

            # 7. Delete files
            result = conn.execute(
                "DELETE FROM files WHERE project_id = ?",
                [project_id],
            )
            counts["files"] = result.rowcount

            # 8. Delete branches
            result = conn.execute(
                "DELETE FROM branches WHERE project_id = ?",
                [project_id],
            )
            counts["branches"] = result.rowcount

            # 9. Delete api_keys
            result = conn.execute(
                "DELETE FROM api_keys WHERE project_id = ?",
                [project_id],
            )
            counts["api_keys"] = result.rowcount

        logger.info(
            "cascade_delete_project_metadata",
            project_id=project_id,
            deleted_counts=counts,
        )
        return counts

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

    def list_bucket_links(
        self,
        target_project_id: str,
    ) -> list[dict[str, Any]]:
        """List all bucket links for a project."""
        results = self.execute(
            """
            SELECT target_bucket_name, source_project_id, source_bucket_name, attached_db_alias
            FROM bucket_links
            WHERE target_project_id = ?
            ORDER BY target_bucket_name
            """,
            [target_project_id],
        )
        return [
            {
                "target_bucket_name": row[0],
                "source_project_id": row[1],
                "source_bucket_name": row[2],
                "attached_db_alias": row[3],
            }
            for row in results
        ]

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
        branch_id: str | None = None,
        scope: str = "project_admin",
        expires_at: datetime | None = None,
    ) -> dict[str, Any]:
        """
        Store a new API key (hashed).

        Args:
            key_id: Unique identifier for the API key
            project_id: The project this key belongs to
            key_hash: SHA-256 hash of the full API key
            key_prefix: First ~50 chars of the API key for lookup
            description: Optional description
            branch_id: Branch ID for branch-specific keys (NULL for project-wide)
            scope: Key scope ('project_admin', 'branch_admin', 'branch_read')
            expires_at: Optional expiration timestamp

        Returns:
            Dict with the created API key record (without the hash)
        """
        now = datetime.now(timezone.utc)

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO api_keys (id, project_id, branch_id, key_hash, key_prefix,
                                      scope, description, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [key_id, project_id, branch_id, key_hash, key_prefix, scope, description, now, expires_at],
            )
            conn.commit()

            result = conn.execute(
                """
                SELECT id, project_id, branch_id, key_prefix, scope, description,
                       created_at, last_used_at, expires_at, revoked_at
                FROM api_keys WHERE id = ?
                """,
                [key_id],
            ).fetchone()

        logger.info(
            "api_key_created",
            key_id=key_id,
            project_id=project_id,
            branch_id=branch_id,
            scope=scope,
            key_prefix=key_prefix[:10] + "...",
        )

        if result:
            return {
                "id": result[0],
                "project_id": result[1],
                "branch_id": result[2],
                "key_prefix": result[3],
                "scope": result[4],
                "description": result[5],
                "created_at": result[6].isoformat() if result[6] else None,
                "last_used_at": result[7].isoformat() if result[7] else None,
                "expires_at": result[8].isoformat() if result[8] else None,
                "revoked_at": result[9].isoformat() if result[9] else None,
            }

        return {}

    def get_api_key_by_prefix(self, key_prefix: str) -> dict[str, Any] | None:
        """
        Find API key by prefix for validation.

        Args:
            key_prefix: The key prefix to search for

        Returns:
            Dict with id, project_id, branch_id, scope, key_hash, etc. or None if not found
            Only returns non-revoked, non-expired keys
        """
        now = datetime.now(timezone.utc)
        result = self.execute_one(
            """
            SELECT id, project_id, branch_id, key_hash, key_prefix, scope,
                   description, created_at, last_used_at, expires_at, revoked_at
            FROM api_keys
            WHERE key_prefix = ?
              AND revoked_at IS NULL
              AND (expires_at IS NULL OR expires_at > ?)
            """,
            [key_prefix, now],
        )

        if result:
            return {
                "id": result[0],
                "project_id": result[1],
                "branch_id": result[2],
                "key_hash": result[3],
                "key_prefix": result[4],
                "scope": result[5],
                "description": result[6],
                "created_at": result[7].isoformat() if result[7] else None,
                "last_used_at": result[8].isoformat() if result[8] else None,
                "expires_at": result[9].isoformat() if result[9] else None,
                "revoked_at": result[10].isoformat() if result[10] else None,
            }

        return None

    def get_api_keys_for_project(self, project_id: str, include_revoked: bool = False) -> list[dict[str, Any]]:
        """
        List all API keys for a project.

        Args:
            project_id: The project ID
            include_revoked: Whether to include revoked keys (default: False)

        Returns:
            List of API key dicts (without key_hash for security)
        """
        if include_revoked:
            query = """
                SELECT id, project_id, branch_id, key_prefix, scope, description,
                       created_at, last_used_at, expires_at, revoked_at
                FROM api_keys
                WHERE project_id = ?
                ORDER BY created_at DESC
            """
        else:
            query = """
                SELECT id, project_id, branch_id, key_prefix, scope, description,
                       created_at, last_used_at, expires_at, revoked_at
                FROM api_keys
                WHERE project_id = ? AND revoked_at IS NULL
                ORDER BY created_at DESC
            """

        results = self.execute(query, [project_id])

        return [
            {
                "id": row[0],
                "project_id": row[1],
                "branch_id": row[2],
                "key_prefix": row[3],
                "scope": row[4],
                "description": row[5],
                "created_at": row[6].isoformat() if row[6] else None,
                "last_used_at": row[7].isoformat() if row[7] else None,
                "expires_at": row[8].isoformat() if row[8] else None,
                "revoked_at": row[9].isoformat() if row[9] else None,
            }
            for row in results
        ]

    def get_api_key_by_id(self, key_id: str) -> dict[str, Any] | None:
        """
        Get a single API key by its ID.

        Args:
            key_id: The API key ID

        Returns:
            Dict with API key details (without key_hash for security) or None if not found
        """
        result = self.execute_one(
            """
            SELECT id, project_id, branch_id, key_prefix, scope, description,
                   created_at, last_used_at, expires_at, revoked_at
            FROM api_keys
            WHERE id = ?
            """,
            [key_id],
        )

        if result:
            return {
                "id": result[0],
                "project_id": result[1],
                "branch_id": result[2],
                "key_prefix": result[3],
                "scope": result[4],
                "description": result[5],
                "created_at": result[6].isoformat() if result[6] else None,
                "last_used_at": result[7].isoformat() if result[7] else None,
                "expires_at": result[8].isoformat() if result[8] else None,
                "revoked_at": result[9].isoformat() if result[9] else None,
            }

        return None

    def revoke_api_key(self, key_id: str) -> bool:
        """
        Revoke an API key (soft delete by setting revoked_at timestamp).

        Args:
            key_id: The API key ID

        Returns:
            True if key was revoked, False if key not found
        """
        now = datetime.now(timezone.utc)

        # Check if key exists
        key = self.get_api_key_by_id(key_id)
        if not key:
            return False

        self.execute_write(
            "UPDATE api_keys SET revoked_at = ? WHERE id = ?",
            [now, key_id],
        )

        logger.info(
            "api_key_revoked",
            key_id=key_id,
            project_id=key.get("project_id"),
            branch_id=key.get("branch_id"),
        )
        return True

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

    def count_active_project_admin_keys(self, project_id: str) -> int:
        """
        Count active (non-revoked, non-expired) project_admin keys.

        Args:
            project_id: The project ID

        Returns:
            Count of active project_admin keys
        """
        now = datetime.now(timezone.utc)
        result = self.execute_one(
            """
            SELECT COUNT(*)
            FROM api_keys
            WHERE project_id = ?
              AND scope = 'project_admin'
              AND revoked_at IS NULL
              AND (expires_at IS NULL OR expires_at > ?)
            """,
            [project_id, now],
        )
        return result[0] if result else 0

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

    # ========================================
    # Snapshot settings operations (ADR-004)
    # ========================================

    def get_snapshot_settings(
        self,
        entity_type: str,
        entity_id: str,
    ) -> dict[str, Any] | None:
        """
        Get snapshot settings for an entity.

        Args:
            entity_type: 'project', 'bucket', or 'table'
            entity_id: Entity identifier

        Returns:
            Config dict or None if not found
        """
        import json

        result = self.execute_one(
            """
            SELECT id, entity_type, entity_id, project_id, config, created_at, updated_at
            FROM snapshot_settings
            WHERE entity_type = ? AND entity_id = ?
            """,
            [entity_type, entity_id],
        )

        if result:
            config = result[4]
            if isinstance(config, str):
                try:
                    config = json.loads(config)
                except (json.JSONDecodeError, TypeError):
                    config = {}

            return {
                "id": result[0],
                "entity_type": result[1],
                "entity_id": result[2],
                "project_id": result[3],
                "config": config,
                "created_at": result[5].isoformat() if result[5] else None,
                "updated_at": result[6].isoformat() if result[6] else None,
            }

        return None

    def set_snapshot_settings(
        self,
        entity_type: str,
        entity_id: str,
        project_id: str,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Set snapshot settings for an entity (upsert).

        Args:
            entity_type: 'project', 'bucket', or 'table'
            entity_id: Entity identifier
            project_id: Project ID (always required)
            config: Partial config dict

        Returns:
            Updated settings dict
        """
        import json
        import uuid

        now = datetime.now(timezone.utc)
        settings_id = str(uuid.uuid4())

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO snapshot_settings (id, entity_type, entity_id, project_id, config, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                    config = EXCLUDED.config,
                    updated_at = EXCLUDED.updated_at
                """,
                [settings_id, entity_type, entity_id, project_id, json.dumps(config), now, now],
            )
            conn.commit()

        logger.info(
            "snapshot_settings_updated",
            entity_type=entity_type,
            entity_id=entity_id,
            project_id=project_id,
        )

        return self.get_snapshot_settings(entity_type, entity_id)

    def delete_snapshot_settings(
        self,
        entity_type: str,
        entity_id: str,
    ) -> bool:
        """
        Delete snapshot settings for an entity.

        Args:
            entity_type: 'project', 'bucket', or 'table'
            entity_id: Entity identifier

        Returns:
            True if deleted
        """
        self.execute_write(
            """
            DELETE FROM snapshot_settings
            WHERE entity_type = ? AND entity_id = ?
            """,
            [entity_type, entity_id],
        )

        logger.info(
            "snapshot_settings_deleted",
            entity_type=entity_type,
            entity_id=entity_id,
        )
        return True

    def delete_project_snapshot_settings(self, project_id: str) -> int:
        """
        Delete all snapshot settings for a project (cascading delete).

        Args:
            project_id: Project ID

        Returns:
            Count of deleted settings
        """
        count_result = self.execute_one(
            "SELECT COUNT(*) FROM snapshot_settings WHERE project_id = ?",
            [project_id]
        )
        count = count_result[0] if count_result else 0

        self.execute_write(
            "DELETE FROM snapshot_settings WHERE project_id = ?",
            [project_id]
        )

        logger.info(
            "project_snapshot_settings_deleted",
            project_id=project_id,
            count=count,
        )
        return count

    # ========================================
    # Snapshots operations (ADR-004)
    # ========================================

    def create_snapshot(
        self,
        snapshot_id: str,
        project_id: str,
        bucket_name: str,
        table_name: str,
        snapshot_type: str,
        parquet_path: str,
        row_count: int,
        size_bytes: int,
        schema_json: dict[str, Any],
        expires_at: datetime | None = None,
        created_by: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a snapshot record in metadata database.

        Args:
            snapshot_id: Unique snapshot identifier
            project_id: Project the snapshot belongs to
            bucket_name: Source bucket name
            table_name: Source table name
            snapshot_type: 'manual', 'auto_predrop', etc.
            parquet_path: Relative path to snapshot directory
            row_count: Number of rows in snapshot
            size_bytes: Parquet file size
            schema_json: Column definitions for restore
            expires_at: When snapshot expires
            created_by: Who created the snapshot
            description: Optional description

        Returns:
            Created snapshot record dict
        """
        import json

        now = datetime.now(timezone.utc)

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO snapshots (
                    id, project_id, bucket_name, table_name, snapshot_type,
                    parquet_path, row_count, size_bytes, schema_json,
                    created_at, created_by, expires_at, description
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    snapshot_id, project_id, bucket_name, table_name, snapshot_type,
                    parquet_path, row_count, size_bytes, json.dumps(schema_json),
                    now, created_by, expires_at, description
                ],
            )
            conn.commit()

            result = conn.execute(
                "SELECT * FROM snapshots WHERE id = ?", [snapshot_id]
            ).fetchone()

        logger.info(
            "snapshot_created",
            snapshot_id=snapshot_id,
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            snapshot_type=snapshot_type,
            row_count=row_count,
        )

        return self._row_to_snapshot_dict(result)

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        """Get snapshot by ID."""
        result = self.execute_one(
            "SELECT * FROM snapshots WHERE id = ?", [snapshot_id]
        )
        return self._row_to_snapshot_dict(result) if result else None

    def get_snapshot_by_project(
        self, project_id: str, snapshot_id: str
    ) -> dict[str, Any] | None:
        """Get snapshot by ID, ensuring it belongs to project."""
        result = self.execute_one(
            "SELECT * FROM snapshots WHERE id = ? AND project_id = ?",
            [snapshot_id, project_id]
        )
        return self._row_to_snapshot_dict(result) if result else None

    def list_snapshots(
        self,
        project_id: str,
        bucket_name: str | None = None,
        table_name: str | None = None,
        snapshot_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        List snapshots for a project with optional filtering.

        Args:
            project_id: Project ID
            bucket_name: Filter by bucket
            table_name: Filter by table (requires bucket_name)
            snapshot_type: Filter by type ('manual', 'auto_predrop', etc.)
            limit: Maximum results
            offset: Offset for pagination

        Returns:
            List of snapshot record dicts
        """
        conditions = ["project_id = ?"]
        params = [project_id]

        if bucket_name:
            conditions.append("bucket_name = ?")
            params.append(bucket_name)

        if table_name:
            conditions.append("table_name = ?")
            params.append(table_name)

        if snapshot_type:
            conditions.append("snapshot_type = ?")
            params.append(snapshot_type)

        where_clause = " AND ".join(conditions)
        params.extend([limit, offset])

        results = self.execute(
            f"""
            SELECT * FROM snapshots
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            params,
        )

        return [self._row_to_snapshot_dict(row) for row in results]

    def count_snapshots(
        self,
        project_id: str,
        bucket_name: str | None = None,
        table_name: str | None = None,
        snapshot_type: str | None = None,
    ) -> int:
        """Count snapshots matching filters."""
        conditions = ["project_id = ?"]
        params = [project_id]

        if bucket_name:
            conditions.append("bucket_name = ?")
            params.append(bucket_name)

        if table_name:
            conditions.append("table_name = ?")
            params.append(table_name)

        if snapshot_type:
            conditions.append("snapshot_type = ?")
            params.append(snapshot_type)

        where_clause = " AND ".join(conditions)

        result = self.execute_one(
            f"SELECT COUNT(*) FROM snapshots WHERE {where_clause}",
            params
        )
        return result[0] if result else 0

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot record (caller should delete files)."""
        self.execute_write("DELETE FROM snapshots WHERE id = ?", [snapshot_id])
        logger.info("snapshot_deleted", snapshot_id=snapshot_id)
        return True

    def delete_project_snapshots(self, project_id: str) -> int:
        """Delete all snapshot records for a project."""
        count_result = self.execute_one(
            "SELECT COUNT(*) FROM snapshots WHERE project_id = ?",
            [project_id]
        )
        count = count_result[0] if count_result else 0

        self.execute_write(
            "DELETE FROM snapshots WHERE project_id = ?",
            [project_id]
        )

        logger.info("project_snapshots_deleted", project_id=project_id, count=count)
        return count

    def cleanup_expired_snapshots(self) -> list[dict[str, Any]]:
        """
        Find and return expired snapshots for cleanup.

        Returns:
            List of expired snapshot records (caller should delete actual files)
        """
        results = self.execute(
            """
            SELECT * FROM snapshots
            WHERE expires_at IS NOT NULL AND expires_at <= now()
            """
        )

        expired_snapshots = [self._row_to_snapshot_dict(row) for row in results]

        if expired_snapshots:
            # Delete records
            self.execute_write(
                "DELETE FROM snapshots WHERE expires_at IS NOT NULL AND expires_at <= now()"
            )
            logger.info("expired_snapshots_cleaned", count=len(expired_snapshots))

        return expired_snapshots

    def _row_to_snapshot_dict(self, row: tuple | None) -> dict[str, Any] | None:
        """
        Convert database row to snapshot dictionary.

        Schema: id(0), project_id(1), bucket_name(2), table_name(3), snapshot_type(4),
                parquet_path(5), row_count(6), size_bytes(7), schema_json(8),
                created_at(9), created_by(10), expires_at(11), description(12)
        """
        import json

        if row is None:
            return None

        # Parse schema JSON if it's a string
        schema_json = row[8]
        if isinstance(schema_json, str):
            try:
                schema_json = json.loads(schema_json)
            except (json.JSONDecodeError, TypeError):
                schema_json = {}

        return {
            "id": row[0],
            "project_id": row[1],
            "bucket_name": row[2],
            "table_name": row[3],
            "snapshot_type": row[4],
            "parquet_path": row[5],
            "row_count": row[6],
            "size_bytes": row[7],
            "schema_json": schema_json,
            "created_at": row[9].isoformat() if row[9] else None,
            "created_by": row[10],
            "expires_at": row[11].isoformat() if row[11] else None,
            "description": row[12],
        }

    # ========================================
    # Branch operations (ADR-007: CoW branching)
    # ========================================

    def create_branch(
        self,
        branch_id: str,
        project_id: str,
        name: str,
        created_by: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a new dev branch record."""
        now = datetime.now(timezone.utc)

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO branches (id, project_id, name, created_at, created_by, description)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [branch_id, project_id, name, now, created_by, description],
            )
            conn.commit()

            result = conn.execute(
                "SELECT * FROM branches WHERE id = ?", [branch_id]
            ).fetchone()

        logger.info("branch_created", branch_id=branch_id, project_id=project_id, name=name)
        return self._row_to_branch_dict(result)

    def get_branch(self, branch_id: str) -> dict[str, Any] | None:
        """Get branch by ID."""
        result = self.execute_one(
            "SELECT * FROM branches WHERE id = ?", [branch_id]
        )
        return self._row_to_branch_dict(result) if result else None

    def get_branch_by_project(
        self, project_id: str, branch_id: str
    ) -> dict[str, Any] | None:
        """Get branch by ID, ensuring it belongs to project."""
        result = self.execute_one(
            "SELECT * FROM branches WHERE id = ? AND project_id = ?",
            [branch_id, project_id]
        )
        return self._row_to_branch_dict(result) if result else None

    def list_branches(
        self,
        project_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List all branches for a project."""
        results = self.execute(
            """
            SELECT * FROM branches
            WHERE project_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            [project_id, limit, offset],
        )
        return [self._row_to_branch_dict(row) for row in results]

    def delete_branch(self, branch_id: str) -> bool:
        """Delete a branch and its table records."""
        # First delete branch_tables records
        self.execute_write(
            "DELETE FROM branch_tables WHERE branch_id = ?",
            [branch_id]
        )
        # Then delete branch
        self.execute_write(
            "DELETE FROM branches WHERE id = ?",
            [branch_id]
        )
        logger.info("branch_deleted", branch_id=branch_id)
        return True

    def mark_table_copied_to_branch(
        self,
        branch_id: str,
        bucket_name: str,
        table_name: str,
    ) -> None:
        """Record that a table has been copied to a branch (CoW triggered)."""
        now = datetime.now(timezone.utc)
        self.execute_write(
            """
            INSERT INTO branch_tables (branch_id, bucket_name, table_name, copied_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (branch_id, bucket_name, table_name) DO NOTHING
            """,
            [branch_id, bucket_name, table_name, now],
        )
        logger.info(
            "branch_table_copied",
            branch_id=branch_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )

    def remove_table_from_branch(
        self,
        branch_id: str,
        bucket_name: str,
        table_name: str,
    ) -> bool:
        """Remove a table from branch tracking (for pull operation)."""
        self.execute_write(
            """
            DELETE FROM branch_tables
            WHERE branch_id = ? AND bucket_name = ? AND table_name = ?
            """,
            [branch_id, bucket_name, table_name],
        )
        logger.info(
            "branch_table_removed",
            branch_id=branch_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )
        return True

    def get_branch_tables(self, branch_id: str) -> list[dict[str, str]]:
        """Get list of tables that have been copied to a branch."""
        results = self.execute(
            """
            SELECT bucket_name, table_name, copied_at
            FROM branch_tables
            WHERE branch_id = ?
            ORDER BY copied_at
            """,
            [branch_id],
        )
        return [
            {
                "bucket_name": row[0],
                "table_name": row[1],
                "copied_at": row[2].isoformat() if row[2] else None,
            }
            for row in results
        ]

    def is_table_in_branch(
        self,
        branch_id: str,
        bucket_name: str,
        table_name: str,
    ) -> bool:
        """Check if a table has been copied to a branch."""
        result = self.execute_one(
            """
            SELECT 1 FROM branch_tables
            WHERE branch_id = ? AND bucket_name = ? AND table_name = ?
            """,
            [branch_id, bucket_name, table_name],
        )
        return result is not None

    def count_branches(self, project_id: str | None = None) -> int:
        """Count branches, optionally for a specific project."""
        if project_id:
            result = self.execute_one(
                "SELECT COUNT(*) FROM branches WHERE project_id = ?",
                [project_id]
            )
        else:
            result = self.execute_one("SELECT COUNT(*) FROM branches")
        return result[0] if result else 0

    def _row_to_branch_dict(self, row: tuple | None) -> dict[str, Any] | None:
        """Convert database row to branch dictionary."""
        if row is None:
            return None

        return {
            "id": row[0],
            "project_id": row[1],
            "name": row[2],
            "created_at": row[3].isoformat() if row[3] else None,
            "created_by": row[4],
            "description": row[5],
        }

    # ==================== Workspace Methods ====================

    def _row_to_workspace_dict(self, row: tuple) -> dict[str, Any]:
        """Convert workspace row to dictionary."""
        return {
            "id": row[0],
            "project_id": row[1],
            "branch_id": row[2],
            "name": row[3],
            "db_path": row[4],
            "created_at": row[5].isoformat() if row[5] else None,
            "expires_at": row[6].isoformat() if row[6] else None,
            "size_limit_bytes": row[7],
            "status": row[8],
        }

    def create_workspace(
        self,
        workspace_id: str,
        project_id: str,
        name: str,
        db_path: str,
        branch_id: str | None = None,
        expires_at: str | None = None,
        size_limit_bytes: int = 10737418240,
    ) -> dict[str, Any]:
        """Create a new workspace."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO workspaces (id, project_id, branch_id, name, db_path, expires_at, size_limit_bytes, status)
                VALUES (?, ?, ?, ?, ?, ?::TIMESTAMPTZ, ?, 'active')
                """,
                [workspace_id, project_id, branch_id, name, db_path, expires_at, size_limit_bytes],
            )
            result = conn.execute(
                "SELECT * FROM workspaces WHERE id = ?", [workspace_id]
            ).fetchone()
            return self._row_to_workspace_dict(result)

    def get_workspace(self, workspace_id: str) -> dict[str, Any] | None:
        """Get workspace by ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM workspaces WHERE id = ?", [workspace_id]
            ).fetchone()
            return self._row_to_workspace_dict(result) if result else None

    def get_workspace_by_project(
        self, project_id: str, workspace_id: str
    ) -> dict[str, Any] | None:
        """Get workspace by project and workspace ID."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM workspaces WHERE id = ? AND project_id = ?",
                [workspace_id, project_id],
            ).fetchone()
            return self._row_to_workspace_dict(result) if result else None

    def list_workspaces(
        self,
        project_id: str,
        branch_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List workspaces for a project, optionally filtered by branch and status."""
        with self.connection() as conn:
            query = "SELECT * FROM workspaces WHERE project_id = ?"
            params: list[Any] = [project_id]

            if branch_id is not None:
                query += " AND branch_id = ?"
                params.append(branch_id)
            elif branch_id is None:
                # For main branch, branch_id is NULL
                pass  # Don't filter, show all

            if status:
                query += " AND status = ?"
                params.append(status)

            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            results = conn.execute(query, params).fetchall()
            return [self._row_to_workspace_dict(row) for row in results]

    def update_workspace_status(self, workspace_id: str, status: str) -> bool:
        """Update workspace status."""
        with self.connection() as conn:
            conn.execute(
                "UPDATE workspaces SET status = ? WHERE id = ?",
                [status, workspace_id],
            )
            result = conn.execute(
                "SELECT COUNT(*) FROM workspaces WHERE id = ?", [workspace_id]
            ).fetchone()
            return result[0] > 0

    def delete_workspace(self, workspace_id: str) -> bool:
        """Delete workspace and its credentials and sessions."""
        with self.connection() as conn:
            # Delete pgwire sessions first (foreign key)
            conn.execute(
                "DELETE FROM pgwire_sessions WHERE workspace_id = ?",
                [workspace_id],
            )
            # Delete credentials (foreign key)
            conn.execute(
                "DELETE FROM workspace_credentials WHERE workspace_id = ?",
                [workspace_id],
            )
            # Delete workspace
            conn.execute("DELETE FROM workspaces WHERE id = ?", [workspace_id])
            return True

    def count_workspaces(self, project_id: str | None = None) -> int:
        """Count workspaces, optionally filtered by project."""
        with self.connection() as conn:
            if project_id:
                result = conn.execute(
                    "SELECT COUNT(*) FROM workspaces WHERE project_id = ?",
                    [project_id],
                ).fetchone()
            else:
                result = conn.execute("SELECT COUNT(*) FROM workspaces").fetchone()
            return result[0]

    def get_expired_workspaces(self) -> list[dict[str, Any]]:
        """Get all expired workspaces that need cleanup."""
        with self.connection() as conn:
            results = conn.execute(
                """
                SELECT * FROM workspaces
                WHERE expires_at IS NOT NULL
                AND expires_at < now()
                AND status = 'active'
                """
            ).fetchall()
            return [self._row_to_workspace_dict(row) for row in results]

    # ==================== Workspace Credentials Methods ====================

    def create_workspace_credentials(
        self, workspace_id: str, username: str, password_hash: str
    ) -> dict[str, Any]:
        """Create credentials for workspace."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO workspace_credentials (workspace_id, username, password_hash)
                VALUES (?, ?, ?)
                """,
                [workspace_id, username, password_hash],
            )
            return {
                "workspace_id": workspace_id,
                "username": username,
            }

    def get_workspace_credentials(self, workspace_id: str) -> dict[str, Any] | None:
        """Get credentials for workspace."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT workspace_id, username, password_hash, created_at FROM workspace_credentials WHERE workspace_id = ?",
                [workspace_id],
            ).fetchone()
            if not result:
                return None
            return {
                "workspace_id": result[0],
                "username": result[1],
                "password_hash": result[2],
                "created_at": result[3].isoformat() if result[3] else None,
            }

    def get_workspace_by_username(self, username: str) -> dict[str, Any] | None:
        """Get workspace by credentials username (for auth)."""
        with self.connection() as conn:
            result = conn.execute(
                """
                SELECT w.*, wc.username, wc.password_hash
                FROM workspaces w
                JOIN workspace_credentials wc ON w.id = wc.workspace_id
                WHERE wc.username = ?
                """,
                [username],
            ).fetchone()
            if not result:
                return None
            ws = self._row_to_workspace_dict(result[:9])
            ws["username"] = result[9]
            ws["password_hash"] = result[10]
            return ws

    def update_workspace_credentials(
        self, workspace_id: str, password_hash: str
    ) -> bool:
        """Update workspace password."""
        with self.connection() as conn:
            conn.execute(
                "UPDATE workspace_credentials SET password_hash = ? WHERE workspace_id = ?",
                [password_hash, workspace_id],
            )
            return True

    # ==================== PG Wire Session Methods (Phase 11b) ====================

    def create_pgwire_session(
        self,
        session_id: str,
        workspace_id: str,
        client_ip: str | None = None,
    ) -> dict[str, Any]:
        """Create a new PG Wire session."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO pgwire_sessions (session_id, workspace_id, client_ip, status)
                VALUES (?, ?, ?, 'active')
                """,
                [session_id, workspace_id, client_ip],
            )
            return {
                "session_id": session_id,
                "workspace_id": workspace_id,
                "client_ip": client_ip,
                "status": "active",
            }

    def get_pgwire_session(self, session_id: str) -> dict[str, Any] | None:
        """Get PG Wire session by ID."""
        with self.connection() as conn:
            result = conn.execute(
                """
                SELECT session_id, workspace_id, client_ip, connected_at,
                       last_activity_at, query_count, status
                FROM pgwire_sessions WHERE session_id = ?
                """,
                [session_id],
            ).fetchone()
            if not result:
                return None
            return {
                "session_id": result[0],
                "workspace_id": result[1],
                "client_ip": result[2],
                "connected_at": result[3].isoformat() if result[3] else None,
                "last_activity_at": result[4].isoformat() if result[4] else None,
                "query_count": result[5],
                "status": result[6],
            }

    def update_pgwire_session_activity(
        self, session_id: str, increment_queries: bool = True
    ) -> bool:
        """Update last activity time and optionally increment query count."""
        with self.connection() as conn:
            if increment_queries:
                conn.execute(
                    """
                    UPDATE pgwire_sessions
                    SET last_activity_at = now(), query_count = query_count + 1
                    WHERE session_id = ?
                    """,
                    [session_id],
                )
            else:
                conn.execute(
                    "UPDATE pgwire_sessions SET last_activity_at = now() WHERE session_id = ?",
                    [session_id],
                )
            return True

    def close_pgwire_session(
        self, session_id: str, status: str = "disconnected"
    ) -> bool:
        """Close a PG Wire session."""
        with self.connection() as conn:
            conn.execute(
                "UPDATE pgwire_sessions SET status = ? WHERE session_id = ?",
                [status, session_id],
            )
            return True

    def count_active_pgwire_sessions(self, workspace_id: str) -> int:
        """Count active sessions for a workspace."""
        with self.connection() as conn:
            result = conn.execute(
                """
                SELECT COUNT(*) FROM pgwire_sessions
                WHERE workspace_id = ? AND status = 'active'
                """,
                [workspace_id],
            ).fetchone()
            return result[0]

    def list_pgwire_sessions(
        self, workspace_id: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        """List PG Wire sessions, optionally filtered."""
        with self.connection() as conn:
            query = "SELECT session_id, workspace_id, client_ip, connected_at, last_activity_at, query_count, status FROM pgwire_sessions WHERE 1=1"
            params: list[Any] = []

            if workspace_id:
                query += " AND workspace_id = ?"
                params.append(workspace_id)
            if status:
                query += " AND status = ?"
                params.append(status)

            query += " ORDER BY connected_at DESC"
            results = conn.execute(query, params).fetchall()

            return [
                {
                    "session_id": r[0],
                    "workspace_id": r[1],
                    "client_ip": r[2],
                    "connected_at": r[3].isoformat() if r[3] else None,
                    "last_activity_at": r[4].isoformat() if r[4] else None,
                    "query_count": r[5],
                    "status": r[6],
                }
                for r in results
            ]

    def cleanup_stale_pgwire_sessions(self, idle_timeout_seconds: int = 3600) -> int:
        """Mark sessions as timeout if idle for too long. Returns count."""
        with self.connection() as conn:
            # DuckDB doesn't support parameterized INTERVAL, format directly
            # idle_timeout_seconds is an int so this is safe
            result = conn.execute(
                f"""
                UPDATE pgwire_sessions
                SET status = 'timeout'
                WHERE status = 'active'
                AND last_activity_at < now() - INTERVAL '{idle_timeout_seconds} seconds'
                RETURNING session_id
                """
            ).fetchall()
            return len(result)

    def delete_pgwire_sessions_for_workspace(self, workspace_id: str) -> int:
        """Delete all sessions for a workspace (called on workspace delete)."""
        with self.connection() as conn:
            result = conn.execute(
                "DELETE FROM pgwire_sessions WHERE workspace_id = ? RETURNING session_id",
                [workspace_id],
            ).fetchall()
            return len(result)


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
    # Branch path helpers (ADR-007: CoW branching)
    # ========================================

    def get_branch_dir(self, project_id: str, branch_id: str) -> Path:
        """Get the directory path for a dev branch."""
        return self._duckdb_dir / f"project_{project_id}_branch_{branch_id}"

    def get_branch_bucket_dir(
        self, project_id: str, branch_id: str, bucket_name: str
    ) -> Path:
        """Get the directory path for a bucket within a branch."""
        return self.get_branch_dir(project_id, branch_id) / bucket_name

    def get_branch_table_path(
        self, project_id: str, branch_id: str, bucket_name: str, table_name: str
    ) -> Path:
        """Get the file path for a table within a branch."""
        return self.get_branch_bucket_dir(project_id, branch_id, bucket_name) / f"{table_name}.duckdb"

    # ========================================
    # Branch operations (ADR-007: CoW branching)
    # ========================================

    def create_branch_db(self, project_id: str, branch_id: str) -> Path:
        """
        Create the directory structure for a dev branch.

        ADR-007: Branches start empty - tables are copied on first write (CoW).

        Returns the path to the created branch directory.
        """
        branch_dir = self.get_branch_dir(project_id, branch_id)
        branch_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "branch_dir_created",
            project_id=project_id,
            branch_id=branch_id,
            path=str(branch_dir),
        )

        return branch_dir

    def delete_branch_db(self, project_id: str, branch_id: str) -> bool:
        """
        Delete a branch's directory and all its contents.

        ADR-007: All branch tables are deleted (data is lost).
        """
        branch_dir = self.get_branch_dir(project_id, branch_id)

        if branch_dir.exists():
            # Clean up locks for all tables in branch
            for bucket_dir in branch_dir.iterdir():
                if bucket_dir.is_dir():
                    for table_file in bucket_dir.glob("*.duckdb"):
                        table_name = table_file.stem
                        # Use branch-specific lock key
                        lock_key = f"{project_id}_branch_{branch_id}/{bucket_dir.name}/{table_name}"
                        table_lock_manager._locks.pop(lock_key, None)

            shutil.rmtree(branch_dir)
            logger.info(
                "branch_dir_deleted",
                project_id=project_id,
                branch_id=branch_id,
                path=str(branch_dir),
            )
            return True

        logger.warning(
            "branch_dir_not_found",
            project_id=project_id,
            branch_id=branch_id,
        )
        return False

    def branch_exists(self, project_id: str, branch_id: str) -> bool:
        """Check if a branch directory exists."""
        return self.get_branch_dir(project_id, branch_id).is_dir()

    def copy_table_to_branch(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
    ) -> Path:
        """
        Copy a table from main to branch (Copy-on-Write operation).

        ADR-007: Called before first write to a table in a branch.
        Copies the entire .duckdb file from main project to branch.

        Returns the path to the copied table in branch.
        """
        # Source: main project table
        source_path = self.get_table_path(project_id, bucket_name, table_name)

        if not source_path.exists():
            raise FileNotFoundError(
                f"Source table not found: {project_id}/{bucket_name}/{table_name}"
            )

        # Target: branch table
        branch_bucket_dir = self.get_branch_bucket_dir(project_id, branch_id, bucket_name)
        branch_bucket_dir.mkdir(parents=True, exist_ok=True)

        target_path = self.get_branch_table_path(project_id, branch_id, bucket_name, table_name)

        # Copy the file
        shutil.copy2(source_path, target_path)

        logger.info(
            "table_copied_to_branch",
            project_id=project_id,
            branch_id=branch_id,
            bucket_name=bucket_name,
            table_name=table_name,
            source_path=str(source_path),
            target_path=str(target_path),
            size_bytes=target_path.stat().st_size,
        )

        return target_path

    def delete_table_from_branch(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
    ) -> bool:
        """
        Delete a table from branch (for pull operation - restore live view).

        ADR-007: After deletion, reads will go back to main (live view).
        """
        table_path = self.get_branch_table_path(project_id, branch_id, bucket_name, table_name)

        if table_path.exists():
            table_path.unlink()
            logger.info(
                "table_deleted_from_branch",
                project_id=project_id,
                branch_id=branch_id,
                bucket_name=bucket_name,
                table_name=table_name,
            )
            return True

        return False

    def branch_table_exists(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
    ) -> bool:
        """Check if a table exists in branch (has been copied via CoW)."""
        return self.get_branch_table_path(
            project_id, branch_id, bucket_name, table_name
        ).exists()

    def get_branch_stats(self, project_id: str, branch_id: str) -> dict[str, Any]:
        """
        Get statistics about a branch by scanning the filesystem.

        Returns counts and sizes of tables that have been copied to the branch.
        """
        branch_dir = self.get_branch_dir(project_id, branch_id)

        if not branch_dir.exists():
            return {"bucket_count": 0, "table_count": 0, "size_bytes": 0}

        bucket_count = 0
        table_count = 0
        size_bytes = 0

        for bucket_dir in branch_dir.iterdir():
            if bucket_dir.is_dir() and not bucket_dir.name.startswith("_"):
                bucket_count += 1
                for table_file in bucket_dir.glob("*.duckdb"):
                    table_count += 1
                    size_bytes += table_file.stat().st_size

        return {
            "bucket_count": bucket_count,
            "table_count": table_count,
            "size_bytes": size_bytes,
        }

    # ========================================
    # Branch-First API helpers (ADR-012)
    # ========================================

    def resolve_branch_path(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str | None = None,
        table_name: str | None = None,
    ) -> Path:
        """
        Resolve the correct path based on branch_id.

        ADR-012: Branch-First API
        - branch_id = "default" -> main project path
        - Other branch_id -> branch path

        Args:
            project_id: The project ID
            branch_id: "default" for main, or branch ID for dev branch
            bucket_name: Optional bucket name
            table_name: Optional table name

        Returns:
            Path to project directory, bucket directory, or table file
        """
        if branch_id == "default":
            # Main project paths
            if table_name and bucket_name:
                return self.get_table_path(project_id, bucket_name, table_name)
            elif bucket_name:
                return self.get_bucket_dir(project_id, bucket_name)
            else:
                return self.get_project_dir(project_id)
        else:
            # Branch paths
            if table_name and bucket_name:
                return self.get_branch_table_path(project_id, branch_id, bucket_name, table_name)
            elif bucket_name:
                return self.get_branch_bucket_dir(project_id, branch_id, bucket_name)
            else:
                return self.get_branch_dir(project_id, branch_id)

    def get_table_source(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
    ) -> str:
        """
        Determine where a table exists in relation to a branch.

        ADR-012: Branch-First API
        - For default branch: always returns "main"
        - For dev branches:
          - "main" if table only exists in main project
          - "branch" if table was copied to branch (CoW)
          - "branch_only" if table only exists in branch

        Args:
            project_id: The project ID
            branch_id: "default" for main, or branch ID for dev branch
            bucket_name: The bucket name
            table_name: The table name

        Returns:
            "main", "branch", or "branch_only"
        """
        if branch_id == "default":
            # Default branch always returns "main"
            return "main"

        # Check if table exists in branch
        branch_table_path = self.get_branch_table_path(
            project_id, branch_id, bucket_name, table_name
        )
        branch_exists = branch_table_path.exists()

        # Check if table exists in main
        main_table_path = self.get_table_path(project_id, bucket_name, table_name)
        main_exists = main_table_path.exists()

        if branch_exists and main_exists:
            return "branch"  # CoW copy exists
        elif branch_exists and not main_exists:
            return "branch_only"  # Created in branch
        elif main_exists and not branch_exists:
            return "main"  # Only in main (live view)
        else:
            # Table doesn't exist anywhere - this shouldn't happen in normal usage
            # but we'll return "main" as the default
            return "main"

    def list_tables_with_source(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
    ) -> list[dict[str, Any]]:
        """
        List all tables visible in a branch with source information.

        ADR-012: Branch-First API
        - For default branch: list tables from main
        - For dev branches: merge main tables + branch tables

        Each table entry includes a "source" field:
        - "main": table from main project (live view)
        - "branch": table copied to branch (CoW)
        - "branch_only": table created only in branch

        Args:
            project_id: The project ID
            branch_id: "default" for main, or branch ID for dev branch
            bucket_name: The bucket name

        Returns:
            List of table info dicts with added "source" field
        """
        if branch_id == "default":
            # Default branch: just list tables from main
            tables = self.list_tables(project_id, bucket_name)
            # Add source field
            for table in tables:
                table["source"] = "main"
            return tables

        # Dev branch: merge main + branch tables
        tables_dict: dict[str, dict[str, Any]] = {}

        # First, add all tables from main (live view)
        main_bucket_dir = self.get_bucket_dir(project_id, bucket_name)
        if main_bucket_dir.exists():
            for table_file in sorted(main_bucket_dir.glob("*.duckdb")):
                table_name = table_file.stem
                table_info = self.get_table(project_id, bucket_name, table_name)
                if table_info:
                    table_info["source"] = "main"
                    tables_dict[table_name] = table_info

        # Then, override with branch tables (CoW copies and branch-only)
        branch_bucket_dir = self.get_branch_bucket_dir(project_id, branch_id, bucket_name)
        if branch_bucket_dir.exists():
            for table_file in sorted(branch_bucket_dir.glob("*.duckdb")):
                table_name = table_file.stem

                # Determine source
                if table_name in tables_dict:
                    # Table exists in main, so this is a CoW copy
                    source = "branch"
                else:
                    # Table only exists in branch
                    source = "branch_only"

                # Get table info from branch
                table_info = self._get_table_info_from_path(table_file, bucket_name)
                if table_info:
                    table_info["source"] = source
                    tables_dict[table_name] = table_info

        return list(tables_dict.values())

    def list_buckets_for_branch(
        self,
        project_id: str,
        branch_id: str,
    ) -> list[dict[str, Any]]:
        """
        List all buckets visible in a branch.

        ADR-012: Branch-First API
        - For default branch: list buckets from main
        - For dev branches: merge main buckets + branch-only buckets

        Args:
            project_id: The project ID
            branch_id: "default" for main, or branch ID for dev branch

        Returns:
            List of bucket info dicts
        """
        if branch_id == "default":
            # Default branch: just list buckets from main
            return self.list_buckets(project_id)

        # Dev branch: merge main + branch buckets
        buckets_dict: dict[str, dict[str, Any]] = {}

        # First, add all buckets from main
        main_project_dir = self.get_project_dir(project_id)
        if main_project_dir.exists():
            for item in sorted(main_project_dir.iterdir()):
                # Skip hidden/special directories
                if item.name.startswith("_") or item.name.startswith("."):
                    continue

                if item.is_dir():
                    # Count .duckdb files in bucket
                    table_count = len(list(item.glob("*.duckdb")))
                    buckets_dict[item.name] = {
                        "name": item.name,
                        "table_count": table_count,
                        "description": None,
                    }

        # Then, add branch-only buckets
        branch_dir = self.get_branch_dir(project_id, branch_id)
        if branch_dir.exists():
            for item in sorted(branch_dir.iterdir()):
                # Skip hidden/special directories
                if item.name.startswith("_") or item.name.startswith("."):
                    continue

                if item.is_dir():
                    # Count .duckdb files in branch bucket
                    table_count = len(list(item.glob("*.duckdb")))

                    # If bucket already exists from main, update table count
                    # (branch may have CoW copies or branch-only tables)
                    if item.name in buckets_dict:
                        # For simplicity, we'll use the branch count
                        # In a more sophisticated implementation, we'd merge the counts
                        buckets_dict[item.name]["table_count"] = table_count
                    else:
                        # Branch-only bucket
                        buckets_dict[item.name] = {
                            "name": item.name,
                            "table_count": table_count,
                            "description": None,
                        }

        return list(buckets_dict.values())

    def create_table_in_branch(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
        columns: list[dict[str, Any]],
        primary_key: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Create a table in a specific branch.

        ADR-012: Branch-First API
        - For default branch: create in main project
        - For dev branch: create only in branch directory (branch_only table)

        Args:
            project_id: The project ID
            branch_id: "default" for main, or branch ID for dev branch
            bucket_name: The bucket name
            table_name: The table name
            columns: List of column definitions
            primary_key: Optional list of column names for primary key

        Returns:
            Table info dict with name, bucket, columns, row_count
        """
        if branch_id == "default":
            # Default branch: create in main
            return self.create_table(
                project_id, bucket_name, table_name, columns, primary_key
            )

        # Dev branch: create in branch directory
        # Ensure branch bucket directory exists
        branch_bucket_dir = self.get_branch_bucket_dir(project_id, branch_id, bucket_name)
        branch_bucket_dir.mkdir(parents=True, exist_ok=True)

        # Get table file path in branch
        table_path = self.get_branch_table_path(
            project_id, branch_id, bucket_name, table_name
        )

        if table_path.exists():
            raise FileExistsError(
                f"Table already exists in branch: {project_id}/{branch_id}/{bucket_name}/{table_name}"
            )

        # Build CREATE TABLE statement
        col_defs = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            if not col.get("nullable", True):
                col_def += " NOT NULL"
            if col.get("default") is not None:
                col_def += f" DEFAULT {col['default']}"
            col_defs.append(col_def)

        if primary_key:
            col_defs.append(f"PRIMARY KEY ({', '.join(primary_key)})")

        create_sql = f"CREATE TABLE {TABLE_DATA_NAME} ({', '.join(col_defs)})"

        # Create the table file
        with duckdb.connect(str(table_path)) as conn:
            conn.execute(create_sql)

        logger.info(
            "table_created_in_branch",
            project_id=project_id,
            branch_id=branch_id,
            bucket_name=bucket_name,
            table_name=table_name,
            columns=len(columns),
            primary_key=primary_key,
        )

        # Return table info
        return {
            "name": table_name,
            "bucket": bucket_name,
            "columns": columns,
            "primary_key": primary_key or [],
            "row_count": 0,
        }

    def ensure_cow_for_write(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
    ) -> bool:
        """
        Ensure Copy-on-Write for a table before writing.

        ADR-012: Branch-First API
        - If branch_id is "default": no CoW needed, return False
        - If table already in branch: no CoW needed, return False
        - If table only in main: copy to branch (CoW), return True

        This should be called before any write operation to a table in a dev branch.

        Args:
            project_id: The project ID
            branch_id: "default" for main, or branch ID for dev branch
            bucket_name: The bucket name
            table_name: The table name

        Returns:
            True if CoW was performed, False otherwise
        """
        if branch_id == "default":
            # Default branch: no CoW needed
            return False

        # Check if table already exists in branch
        branch_table_path = self.get_branch_table_path(
            project_id, branch_id, bucket_name, table_name
        )
        if branch_table_path.exists():
            # Table already in branch, no CoW needed
            return False

        # Check if table exists in main
        main_table_path = self.get_table_path(project_id, bucket_name, table_name)
        if not main_table_path.exists():
            # Table doesn't exist in main either
            # This might be a branch_only table that doesn't exist yet
            return False

        # Table exists in main but not in branch: perform CoW
        self.copy_table_to_branch(project_id, branch_id, bucket_name, table_name)

        logger.info(
            "cow_performed_for_write",
            project_id=project_id,
            branch_id=branch_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )

        return True

    def _get_table_info_from_path(
        self,
        table_path: Path,
        bucket_name: str,
    ) -> dict[str, Any] | None:
        """
        Helper method to get table info from a specific path.

        Used by list_tables_with_source to read table info from branch files.

        Args:
            table_path: Path to the .duckdb file
            bucket_name: The bucket name

        Returns:
            Table info dict or None if error
        """
        try:
            table_name = table_path.stem

            with duckdb.connect(str(table_path), read_only=True) as conn:
                # Get columns
                columns_result = conn.execute(
                    f"DESCRIBE {TABLE_DATA_NAME}"
                ).fetchall()
                columns = [
                    {
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2] == "YES",
                        "default": row[4],
                    }
                    for row in columns_result
                ]

                # Get primary key
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

                # Get row count
                row_count_result = conn.execute(
                    f"SELECT COUNT(*) FROM {TABLE_DATA_NAME}"
                ).fetchone()
                row_count = row_count_result[0] if row_count_result else 0

            return {
                "name": table_name,
                "bucket": bucket_name,
                "columns": columns,
                "primary_key": primary_key,
                "row_count": row_count,
            }

        except Exception as e:
            logger.error(
                "failed_to_get_table_info",
                table_path=str(table_path),
                bucket_name=bucket_name,
                error=str(e),
            )
            return None

    @contextmanager
    def branch_table_connection(
        self,
        project_id: str,
        branch_id: str,
        bucket_name: str,
        table_name: str,
        read_only: bool = False,
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a connection to a table, routing to branch or main based on CoW status.

        ADR-007 Live View + CoW:
        - READ: If table in branch -> read from branch, else read from main (live!)
        - WRITE: If table not in branch -> copy from main first (CoW), then write

        Args:
            project_id: The project ID
            branch_id: The branch ID
            bucket_name: The bucket name
            table_name: The table name
            read_only: If True, read-only access (no CoW trigger)

        Yields:
            DuckDB connection to the appropriate table (branch copy or main)
        """
        branch_table_path = self.get_branch_table_path(
            project_id, branch_id, bucket_name, table_name
        )
        main_table_path = self.get_table_path(project_id, bucket_name, table_name)

        # Determine which table to use
        table_in_branch = branch_table_path.exists()

        if read_only:
            # READ: Use branch if exists, otherwise main (live view)
            if table_in_branch:
                target_path = branch_table_path
            else:
                # Live view - read from main
                if not main_table_path.exists():
                    raise FileNotFoundError(
                        f"Table not found: {project_id}/{bucket_name}/{table_name}"
                    )
                target_path = main_table_path

            conn = duckdb.connect(str(target_path), read_only=True)
            try:
                yield conn
            finally:
                conn.close()
        else:
            # WRITE: Need CoW if table not in branch
            # Note: Caller should handle CoW and metadata update before calling this
            # This method just provides connection to the branch table
            if not table_in_branch:
                raise ValueError(
                    f"Table not in branch - CoW required first: {branch_id}/{bucket_name}/{table_name}"
                )

            # Use branch-specific lock key
            lock_key = f"{project_id}_branch_{branch_id}/{bucket_name}/{table_name}"

            # Manually handle the lock since we're using a custom key
            lock = table_lock_manager.get_lock(
                f"{project_id}_branch_{branch_id}", bucket_name, table_name
            )

            from src.metrics import (
                TABLE_LOCK_ACQUISITIONS,
                TABLE_LOCK_WAIT_TIME,
                TABLE_LOCKS_ACTIVE,
            )

            wait_start = time.perf_counter()
            lock.acquire()
            wait_duration = time.perf_counter() - wait_start

            TABLE_LOCK_WAIT_TIME.observe(wait_duration)
            TABLE_LOCK_ACQUISITIONS.labels(
                project_id=f"{project_id}_branch_{branch_id}",
                bucket=bucket_name,
                table=table_name
            ).inc()
            TABLE_LOCKS_ACTIVE.inc()

            try:
                conn = duckdb.connect(str(branch_table_path), read_only=False)
                try:
                    yield conn
                finally:
                    conn.close()
            finally:
                lock.release()
                TABLE_LOCKS_ACTIVE.dec()

    # ========================================
    # Workspace Methods
    # ========================================

    def get_workspaces_dir(self, project_id: str, branch_id: str | None = None) -> Path:
        """Get the workspaces directory for a project or branch."""
        if branch_id:
            base_dir = self.get_branch_dir(project_id, branch_id)
        else:
            base_dir = self.get_project_dir(project_id)
        return base_dir / "_workspaces"

    def get_workspace_path(
        self, project_id: str, workspace_id: str, branch_id: str | None = None
    ) -> Path:
        """Get the path to a workspace .duckdb file."""
        workspaces_dir = self.get_workspaces_dir(project_id, branch_id)
        return workspaces_dir / f"{workspace_id}.duckdb"

    def create_workspace_db(
        self, project_id: str, workspace_id: str, branch_id: str | None = None
    ) -> Path:
        """Create an empty workspace database file."""
        workspaces_dir = self.get_workspaces_dir(project_id, branch_id)
        workspaces_dir.mkdir(parents=True, exist_ok=True)

        workspace_path = workspaces_dir / f"{workspace_id}.duckdb"

        # Create empty DuckDB file with a marker table
        conn = duckdb.connect(str(workspace_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _workspace_info (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO _workspace_info (key, value) VALUES
            ('workspace_id', ?),
            ('created_at', now()::VARCHAR)
        """, [workspace_id])
        conn.close()

        return workspace_path

    def delete_workspace_db(
        self, project_id: str, workspace_id: str, branch_id: str | None = None
    ) -> bool:
        """Delete a workspace database file."""
        workspace_path = self.get_workspace_path(project_id, workspace_id, branch_id)
        if workspace_path.exists():
            workspace_path.unlink()
            # Also remove WAL file if exists
            wal_path = workspace_path.with_suffix(".duckdb.wal")
            if wal_path.exists():
                wal_path.unlink()
            return True
        return False

    def workspace_exists(
        self, project_id: str, workspace_id: str, branch_id: str | None = None
    ) -> bool:
        """Check if a workspace database file exists."""
        workspace_path = self.get_workspace_path(project_id, workspace_id, branch_id)
        return workspace_path.exists()

    def get_workspace_size(
        self, project_id: str, workspace_id: str, branch_id: str | None = None
    ) -> int:
        """Get workspace file size in bytes."""
        workspace_path = self.get_workspace_path(project_id, workspace_id, branch_id)
        if workspace_path.exists():
            return workspace_path.stat().st_size
        return 0

    def list_workspace_objects(
        self, project_id: str, workspace_id: str, branch_id: str | None = None
    ) -> list[dict[str, Any]]:
        """List tables/views in a workspace."""
        workspace_path = self.get_workspace_path(project_id, workspace_id, branch_id)
        if not workspace_path.exists():
            return []

        # Note: Use SHOW TABLES instead of information_schema.tables or duckdb_tables()
        # because those may return empty after ATTACH/DETACH operations (DuckDB quirk)
        conn = duckdb.connect(str(workspace_path))
        try:
            # Get all objects using SHOW TABLES (includes both tables and views)
            results = conn.execute("SHOW TABLES").fetchall()

            objects = []
            for (name,) in results:
                # Skip internal objects
                if name.startswith("_"):
                    continue

                # Check if it's a view using sqlite_master
                is_view = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'view' AND name = ?",
                    [name],
                ).fetchone()

                obj_type = "view" if is_view else "table"

                # Get row count
                row_count = 0
                try:
                    count_result = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()
                    row_count = count_result[0] if count_result else 0
                except Exception:
                    pass

                objects.append({
                    "name": name,
                    "type": obj_type,
                    "rows": row_count,
                })

            return objects
        finally:
            conn.close()

    def clear_workspace(
        self, project_id: str, workspace_id: str, branch_id: str | None = None
    ) -> bool:
        """Clear all user objects from workspace (keep system tables)."""
        workspace_path = self.get_workspace_path(project_id, workspace_id, branch_id)
        if not workspace_path.exists():
            return False

        conn = duckdb.connect(str(workspace_path))
        try:
            # Get all objects using SHOW TABLES (includes tables and views)
            results = conn.execute("SHOW TABLES").fetchall()

            for (name,) in results:
                # Skip internal objects
                if name.startswith("_"):
                    continue

                # Check if it's a view using sqlite_master
                is_view = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'view' AND name = ?",
                    [name],
                ).fetchone()

                if is_view:
                    conn.execute(f'DROP VIEW IF EXISTS "{name}"')
                else:
                    conn.execute(f'DROP TABLE IF EXISTS "{name}"')

            return True
        finally:
            conn.close()

    def drop_workspace_object(
        self,
        project_id: str,
        workspace_id: str,
        object_name: str,
        branch_id: str | None = None,
    ) -> bool:
        """Drop a specific object from workspace."""
        workspace_path = self.get_workspace_path(project_id, workspace_id, branch_id)
        if not workspace_path.exists():
            return False

        conn = duckdb.connect(str(workspace_path))
        try:
            # Check if object exists using SHOW TABLES
            results = conn.execute("SHOW TABLES").fetchall()
            object_exists = any(name == object_name for (name,) in results)

            if not object_exists:
                return False

            # Check if it's a view using sqlite_master
            is_view = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'view' AND name = ?",
                [object_name],
            ).fetchone()

            if is_view:
                conn.execute(f'DROP VIEW "{object_name}"')
            else:
                conn.execute(f'DROP TABLE "{object_name}"')

            return True
        finally:
            conn.close()

    def load_table_to_workspace(
        self,
        project_id: str,
        workspace_id: str,
        source_bucket: str,
        source_table: str,
        dest_table: str,
        columns: list[str] | None = None,
        where_clause: str | None = None,
        branch_id: str | None = None,
    ) -> dict[str, Any]:
        """Load data from a project table into the workspace."""
        workspace_path = self.get_workspace_path(project_id, workspace_id, branch_id)

        # Get source table path
        if branch_id:
            source_path = self.get_branch_table_path(project_id, branch_id, source_bucket, source_table)
            if not source_path.exists():
                # Fall back to main branch table
                source_path = self.get_table_path(project_id, source_bucket, source_table)
        else:
            source_path = self.get_table_path(project_id, source_bucket, source_table)

        if not source_path.exists():
            raise FileNotFoundError(f"Source table not found: {source_bucket}.{source_table}")

        conn = duckdb.connect(str(workspace_path))
        try:
            # Attach source table
            conn.execute(f"ATTACH '{source_path}' AS source_db (READ_ONLY)")

            # Build column list
            col_list = ", ".join(f'"{c}"' for c in columns) if columns else "*"

            # Build WHERE clause
            where = f"WHERE {where_clause}" if where_clause else ""

            # Copy data
            conn.execute(f"""
                CREATE OR REPLACE TABLE "{dest_table}" AS
                SELECT {col_list} FROM source_db.main.data {where}
            """)

            # Get stats
            row_count = conn.execute(f'SELECT COUNT(*) FROM "{dest_table}"').fetchone()[0]

            conn.execute("DETACH source_db")
            conn.close()

            # Get file size
            size_bytes = workspace_path.stat().st_size

            return {
                "rows": row_count,
                "size_bytes": size_bytes,
            }
        except Exception as e:
            conn.close()
            raise e

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
        mode: str = "basic",
    ) -> dict[str, Any]:
        """
        Get statistical profile of a table with advanced analytics.

        Args:
            project_id: The project ID
            bucket_name: The bucket name
            table_name: The table name
            mode: Profile mode - "basic", "full", "distribution", "quality"

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

            # Get column info from information_schema
            columns_info = conn.execute(
                f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{TABLE_DATA_NAME}'
                ORDER BY ordinal_position
                """
            ).fetchall()
            column_count = len(columns_info)

            # Numeric types for which advanced stats make sense
            numeric_types = {
                "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT",
                "UINTEGER", "USMALLINT", "UTINYINT", "UHUGEINT",
                "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC",
            }

            # String types for pattern analysis
            string_types = {"VARCHAR", "TEXT", "STRING", "CHAR"}

            statistics = []
            quality_issues = []
            quality_score = 100.0

            for col_name, col_type in columns_info:
                col_type_upper = col_type.upper() if col_type else ""
                is_numeric = any(col_type_upper.startswith(nt) for nt in numeric_types)
                is_string = any(col_type_upper.startswith(st) for st in string_types)

                # Build advanced stats query for this column
                stat = self._get_column_stats(
                    conn, col_name, col_type, is_numeric, is_string, row_count, mode
                )
                statistics.append(stat)

                # Collect quality issues
                if stat.get("null_percentage", 0) > 50:
                    quality_issues.append({
                        "column": col_name,
                        "type": "high_nulls",
                        "severity": "warning",
                        "message": f"Column has {stat['null_percentage']:.1f}% null values",
                    })
                    quality_score -= 5

                if stat.get("cardinality_class") == "constant":
                    quality_issues.append({
                        "column": col_name,
                        "type": "constant",
                        "severity": "info",
                        "message": "Column has constant value (all rows identical)",
                    })

                if is_numeric and stat.get("outlier_count", 0) > 0:
                    outlier_pct = (stat["outlier_count"] / row_count * 100) if row_count > 0 else 0
                    if outlier_pct > 5:
                        quality_issues.append({
                            "column": col_name,
                            "type": "outliers",
                            "severity": "warning",
                            "message": f"{stat['outlier_count']} outliers detected ({outlier_pct:.1f}%)",
                        })
                        quality_score -= 2

                if is_numeric and abs(stat.get("skewness") or 0) > 2:
                    skew_dir = "right" if stat["skewness"] > 0 else "left"
                    quality_issues.append({
                        "column": col_name,
                        "type": "skewed",
                        "severity": "info",
                        "message": f"Highly {skew_dir}-skewed distribution (skewness={stat['skewness']:.2f})",
                    })

                # Suggest primary key candidates
                if stat.get("cardinality_class") == "unique" and stat.get("null_percentage", 0) == 0:
                    quality_issues.append({
                        "column": col_name,
                        "type": "pk_candidate",
                        "severity": "info",
                        "message": "100% unique, non-null - potential primary key",
                    })

                # Suggest ENUM for low cardinality strings
                if is_string and stat.get("cardinality_class") in ("very_low", "low"):
                    quality_issues.append({
                        "column": col_name,
                        "type": "enum_candidate",
                        "severity": "info",
                        "message": f"Only {stat['approx_unique']} distinct values - consider ENUM type",
                    })

            quality_score = max(0, min(100, quality_score))

            result = {
                "table_name": table_name,
                "bucket_name": bucket_name,
                "row_count": row_count,
                "column_count": column_count,
                "statistics": statistics,
                "quality_score": round(quality_score, 1),
                "quality_issues": quality_issues,
            }

            # Add correlations for full/quality mode
            if mode in ("full", "quality") and row_count > 0:
                numeric_cols = [
                    s["column_name"] for s in statistics
                    if any(s["column_type"].upper().startswith(nt) for nt in numeric_types)
                ]
                if len(numeric_cols) >= 2:
                    result["correlations"] = self._get_correlations(conn, numeric_cols[:10])

            return result

        finally:
            conn.close()

    def _get_column_stats(
        self,
        conn: Any,
        col_name: str,
        col_type: str,
        is_numeric: bool,
        is_string: bool,
        row_count: int,
        mode: str,
    ) -> dict[str, Any]:
        """Get comprehensive statistics for a single column."""
        quoted_col = f'"{col_name}"'

        # Base stats query
        base_query = f"""
        SELECT
            COUNT(*) as total_count,
            COUNT({quoted_col}) as non_null_count,
            COUNT(DISTINCT {quoted_col}) as unique_count,
            MIN({quoted_col}) as min_val,
            MAX({quoted_col}) as max_val
        FROM main.{TABLE_DATA_NAME}
        """
        base_result = conn.execute(base_query).fetchone()

        total_count = base_result[0]
        non_null_count = base_result[1]
        unique_count = base_result[2]
        min_val = base_result[3]
        max_val = base_result[4]

        null_percentage = ((total_count - non_null_count) / total_count * 100) if total_count > 0 else 0
        cardinality_ratio = (unique_count / non_null_count) if non_null_count > 0 else 0

        # Classify cardinality
        if unique_count == non_null_count and non_null_count > 0:
            cardinality_class = "unique"
        elif unique_count == 1:
            cardinality_class = "constant"
        elif cardinality_ratio > 0.9:
            cardinality_class = "high"
        elif cardinality_ratio > 0.5:
            cardinality_class = "medium"
        elif cardinality_ratio > 0.01:
            cardinality_class = "low"
        else:
            cardinality_class = "very_low"

        stat = {
            "column_name": col_name,
            "column_type": col_type,
            "min": self._serialize_value(min_val),
            "max": self._serialize_value(max_val),
            "approx_unique": unique_count,
            "cardinality_ratio": round(cardinality_ratio, 4),
            "cardinality_class": cardinality_class,
            "count": non_null_count,
            "null_percentage": round(null_percentage, 2),
        }

        # Numeric-specific stats
        if is_numeric and non_null_count > 0:
            numeric_query = f"""
            SELECT
                AVG({quoted_col}) as avg_val,
                STDDEV({quoted_col}) as std_val,
                SKEWNESS({quoted_col}) as skewness,
                KURTOSIS({quoted_col}) as kurtosis,
                QUANTILE_CONT({quoted_col}, [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]) as percentiles
            FROM main.{TABLE_DATA_NAME}
            WHERE {quoted_col} IS NOT NULL
            """
            try:
                num_result = conn.execute(numeric_query).fetchone()
                stat["avg"] = round(num_result[0], 4) if num_result[0] is not None else None
                stat["std"] = round(num_result[1], 4) if num_result[1] is not None else None
                stat["skewness"] = round(num_result[2], 4) if num_result[2] is not None else None
                stat["kurtosis"] = round(num_result[3], 4) if num_result[3] is not None else None

                percentiles = num_result[4] if num_result[4] else []
                if len(percentiles) == 7:
                    stat["q01"] = self._serialize_value(percentiles[0])
                    stat["q05"] = self._serialize_value(percentiles[1])
                    stat["q25"] = self._serialize_value(percentiles[2])
                    stat["q50"] = self._serialize_value(percentiles[3])
                    stat["q75"] = self._serialize_value(percentiles[4])
                    stat["q95"] = self._serialize_value(percentiles[5])
                    stat["q99"] = self._serialize_value(percentiles[6])

                    # Calculate IQR outliers
                    q25 = percentiles[2]
                    q75 = percentiles[4]
                    if q25 is not None and q75 is not None:
                        iqr = q75 - q25
                        lower_bound = q25 - 1.5 * iqr
                        upper_bound = q75 + 1.5 * iqr
                        outlier_query = f"""
                        SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}
                        WHERE {quoted_col} < {lower_bound} OR {quoted_col} > {upper_bound}
                        """
                        outlier_count = conn.execute(outlier_query).fetchone()[0]
                        stat["outlier_count"] = outlier_count
                        stat["outlier_lower_bound"] = round(lower_bound, 4)
                        stat["outlier_upper_bound"] = round(upper_bound, 4)
            except Exception:
                # Skip advanced numeric stats if query fails
                pass

            # Histogram for distribution mode
            if mode in ("full", "distribution") and non_null_count > 0:
                try:
                    hist_query = f"""
                    SELECT HISTOGRAM({quoted_col})
                    FROM main.{TABLE_DATA_NAME}
                    WHERE {quoted_col} IS NOT NULL
                    """
                    hist_result = conn.execute(hist_query).fetchone()
                    if hist_result and hist_result[0]:
                        stat["histogram"] = hist_result[0]
                except Exception:
                    pass
        else:
            # Non-numeric columns don't have these stats
            stat["avg"] = None
            stat["std"] = None
            stat["skewness"] = None
            stat["kurtosis"] = None

        # String-specific stats
        if is_string and non_null_count > 0:
            string_query = f"""
            SELECT
                AVG(LENGTH({quoted_col})) as avg_length,
                MIN(LENGTH({quoted_col})) as min_length,
                MAX(LENGTH({quoted_col})) as max_length,
                COUNT(*) FILTER (WHERE {quoted_col} = '') as empty_count,
                COUNT(*) FILTER (WHERE TRIM({quoted_col}) = '' AND {quoted_col} != '') as whitespace_only_count
            FROM main.{TABLE_DATA_NAME}
            WHERE {quoted_col} IS NOT NULL
            """
            try:
                str_result = conn.execute(string_query).fetchone()
                stat["avg_length"] = round(str_result[0], 1) if str_result[0] else None
                stat["min_length"] = str_result[1]
                stat["max_length"] = str_result[2]
                stat["empty_count"] = str_result[3]
                stat["whitespace_only_count"] = str_result[4]
            except Exception:
                pass

            # Pattern detection for full mode
            if mode in ("full", "quality") and non_null_count > 0:
                stat["detected_patterns"] = self._detect_patterns(conn, col_name, non_null_count)

        return stat

    def _detect_patterns(self, conn: Any, col_name: str, non_null_count: int) -> list[dict]:
        """Detect common patterns in string column."""
        quoted_col = f'"{col_name}"'
        patterns = []

        pattern_checks = [
            ("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
            ("uuid", r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"),
            ("url", r"^https?://"),
            ("phone", r"^\+?[0-9\s\-\(\)]{10,20}$"),
            ("ipv4", r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"),
            ("date_iso", r"^\d{4}-\d{2}-\d{2}$"),
            ("datetime_iso", r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"),
        ]

        for pattern_name, regex in pattern_checks:
            try:
                query = f"""
                SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}
                WHERE {quoted_col} IS NOT NULL
                AND regexp_full_match({quoted_col}, '{regex}')
                """
                match_count = conn.execute(query).fetchone()[0]
                if match_count > 0:
                    match_pct = (match_count / non_null_count * 100) if non_null_count > 0 else 0
                    patterns.append({
                        "pattern": pattern_name,
                        "match_count": match_count,
                        "match_percentage": round(match_pct, 1),
                    })
            except Exception:
                pass

        return patterns

    def _get_correlations(self, conn: Any, numeric_cols: list[str]) -> list[dict]:
        """Calculate correlations between numeric columns."""
        correlations = []

        for i, col1 in enumerate(numeric_cols):
            for col2 in numeric_cols[i + 1:]:
                try:
                    query = f"""
                    SELECT CORR("{col1}", "{col2}")
                    FROM main.{TABLE_DATA_NAME}
                    WHERE "{col1}" IS NOT NULL AND "{col2}" IS NOT NULL
                    """
                    corr_val = conn.execute(query).fetchone()[0]
                    if corr_val is not None and abs(corr_val) > 0.3:
                        correlations.append({
                            "column1": col1,
                            "column2": col2,
                            "correlation": round(corr_val, 4),
                            "strength": "strong" if abs(corr_val) > 0.7 else "moderate",
                        })
                except Exception:
                    pass

        # Sort by absolute correlation value
        correlations.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        return correlations[:20]  # Top 20 correlations

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
