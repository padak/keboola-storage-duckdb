"""DuckDB database management - MetadataDB and ProjectDB connections."""

import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import duckdb
import structlog

from src.config import settings

logger = structlog.get_logger()


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
        """
        db_path = f"project_{project_id}.duckdb"
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


class ProjectDBManager:
    """
    Manager for project-specific DuckDB databases.

    Each Keboola project has its own .duckdb file containing:
    - Buckets (as schemas)
    - Tables (in bucket schemas)
    - Workspaces (as schemas with WORKSPACE_ prefix)
    """

    @property
    def _duckdb_dir(self) -> Path:
        """Get duckdb dir from settings (allows runtime override in tests)."""
        return settings.duckdb_dir

    def get_db_path(self, project_id: str) -> Path:
        """Get the path to a project's DuckDB file."""
        return self._duckdb_dir / f"project_{project_id}.duckdb"

    def create_project_db(self, project_id: str) -> Path:
        """
        Create a new DuckDB database file for a project.

        Returns the path to the created database.
        """
        db_path = self.get_db_path(project_id)

        # Ensure directory exists
        self._duckdb_dir.mkdir(parents=True, exist_ok=True)

        # Create empty database
        conn = duckdb.connect(str(db_path))
        try:
            # Set some default configuration
            conn.execute(f"SET threads = {settings.duckdb_threads}")
            conn.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
            conn.commit()
            logger.info("project_db_created", project_id=project_id, path=str(db_path))
        finally:
            conn.close()

        return db_path

    def delete_project_db(self, project_id: str) -> bool:
        """Delete a project's DuckDB database file."""
        db_path = self.get_db_path(project_id)

        if db_path.exists():
            db_path.unlink()
            logger.info("project_db_deleted", project_id=project_id, path=str(db_path))
            return True

        logger.warning("project_db_not_found", project_id=project_id, path=str(db_path))
        return False

    def project_exists(self, project_id: str) -> bool:
        """Check if a project database file exists."""
        return self.get_db_path(project_id).exists()

    def get_db_size(self, project_id: str) -> int:
        """Get the size of a project's database file in bytes."""
        db_path = self.get_db_path(project_id)
        return db_path.stat().st_size if db_path.exists() else 0

    @contextmanager
    def connection(
        self, project_id: str, read_only: bool = False
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a connection to a project's database.

        Usage:
            with project_db.connection("123") as conn:
                conn.execute("SELECT * FROM bucket.table")
        """
        db_path = self.get_db_path(project_id)

        if not db_path.exists():
            raise FileNotFoundError(f"Project database not found: {project_id}")

        conn = duckdb.connect(str(db_path), read_only=read_only)
        try:
            yield conn
        finally:
            conn.close()

    def get_project_stats(self, project_id: str) -> dict[str, Any]:
        """Get statistics about a project's database."""
        with self.connection(project_id, read_only=True) as conn:
            # Count schemas (buckets)
            schemas = conn.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')
                """
            ).fetchall()

            # Count tables
            tables = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                """
            ).fetchone()

            return {
                "bucket_count": len(schemas),
                "table_count": tables[0] if tables else 0,
                "size_bytes": self.get_db_size(project_id),
            }


# Global instances
metadata_db = MetadataDB()
project_db_manager = ProjectDBManager()
