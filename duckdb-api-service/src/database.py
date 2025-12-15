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

    # ========================================
    # Bucket operations
    # ========================================

    def create_bucket(
        self, project_id: str, bucket_name: str, description: str | None = None
    ) -> dict[str, Any]:
        """
        Create a bucket (schema) in a project's database.

        Returns bucket info dict with name and created status.
        """
        with self.connection(project_id) as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {bucket_name}")
            conn.commit()

        logger.info(
            "bucket_created",
            project_id=project_id,
            bucket_name=bucket_name,
            description=description,
        )

        # Return bucket info (description not stored in DuckDB, just passed through)
        return {"name": bucket_name, "table_count": 0, "description": description}

    def delete_bucket(
        self, project_id: str, bucket_name: str, cascade: bool = True
    ) -> bool:
        """
        Delete a bucket (schema) from a project's database.

        Args:
            project_id: The project ID
            bucket_name: The bucket/schema name
            cascade: If True, drop all tables in the bucket

        Returns True if successful.
        """
        cascade_clause = "CASCADE" if cascade else "RESTRICT"

        with self.connection(project_id) as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {bucket_name} {cascade_clause}")
            conn.commit()

        logger.info(
            "bucket_deleted",
            project_id=project_id,
            bucket_name=bucket_name,
            cascade=cascade,
        )
        return True

    def list_buckets(self, project_id: str) -> list[dict[str, Any]]:
        """
        List all buckets (schemas) in a project's database.

        Excludes system schemas: information_schema, pg_catalog, main.

        Returns list of dicts with bucket info.
        """
        with self.connection(project_id, read_only=True) as conn:
            # Get all non-system schemas
            schemas = conn.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')
                ORDER BY schema_name
                """
            ).fetchall()

            buckets = []
            for (schema_name,) in schemas:
                # Count tables in this schema
                table_count_result = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = ?
                    """,
                    [schema_name],
                ).fetchone()

                table_count = table_count_result[0] if table_count_result else 0

                buckets.append(
                    {
                        "name": schema_name,
                        "table_count": table_count,
                        "description": None,
                    }
                )

        return buckets

    def get_bucket(self, project_id: str, bucket_name: str) -> dict[str, Any] | None:
        """
        Get information about a specific bucket (schema).

        Returns bucket info dict or None if bucket doesn't exist.
        """
        with self.connection(project_id, read_only=True) as conn:
            # Check if schema exists
            schema_result = conn.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = ?
                """,
                [bucket_name],
            ).fetchone()

            if not schema_result:
                return None

            # Count tables in this schema
            table_count_result = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = ?
                """,
                [bucket_name],
            ).fetchone()

            table_count = table_count_result[0] if table_count_result else 0

            return {"name": bucket_name, "table_count": table_count, "description": None}

    def bucket_exists(self, project_id: str, bucket_name: str) -> bool:
        """Check if a bucket (schema) exists in a project's database."""
        with self.connection(project_id, read_only=True) as conn:
            result = conn.execute(
                """
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = ?
                """,
                [bucket_name],
            ).fetchone()

            return result is not None

    # ========================================
    # Bucket sharing - ATTACH/DETACH operations
    # ========================================

    def attach_database(
        self,
        target_project_id: str,
        source_project_id: str,
        alias: str,
        read_only: bool = True,
    ) -> None:
        """
        Attach a source project's database to a target project.

        This allows the target project to access tables from the source project.
        Used for bucket linking/sharing.

        Args:
            target_project_id: The project that will access the source
            source_project_id: The project being attached
            alias: The database alias to use (e.g., 'source_proj_123')
            read_only: Whether to attach in READ_ONLY mode (default True)
        """
        source_db_path = self.get_db_path(source_project_id)

        if not source_db_path.exists():
            raise FileNotFoundError(
                f"Source project database not found: {source_project_id}"
            )

        with self.connection(target_project_id) as conn:
            # ATTACH the source database
            read_only_clause = "READ_ONLY" if read_only else ""
            conn.execute(
                f"ATTACH DATABASE '{source_db_path}' AS {alias} ({read_only_clause})"
            )
            conn.commit()

        logger.info(
            "database_attached",
            target_project=target_project_id,
            source_project=source_project_id,
            alias=alias,
            read_only=read_only,
        )

    def detach_database(self, target_project_id: str, alias: str) -> None:
        """
        Detach a previously attached database.

        Args:
            target_project_id: The project with the attached database
            alias: The database alias to detach
        """
        with self.connection(target_project_id) as conn:
            conn.execute(f"DETACH DATABASE {alias}")
            conn.commit()

        logger.info(
            "database_detached",
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
        Link a bucket by ATTACHing source DB and creating views - all in one connection.

        DuckDB ATTACH is session-specific, so we must do everything in one connection.

        Args:
            target_project_id: The project where views will be created
            target_bucket_name: The schema name for the views
            source_project_id: The source project ID
            source_bucket_name: The schema name in the source database
            source_db_alias: The alias for the attached database

        Returns:
            List of view names created
        """
        source_db_path = self.get_db_path(source_project_id)

        if not source_db_path.exists():
            raise FileNotFoundError(
                f"Source project database not found: {source_project_id}"
            )

        # First, get list of tables from source database directly
        with self.connection(source_project_id, read_only=True) as source_conn:
            tables = source_conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ? AND table_type = 'BASE TABLE'
                """,
                [source_bucket_name],
            ).fetchall()

        # Now work with target database
        with self.connection(target_project_id) as conn:
            # 1. ATTACH source database in READ_ONLY mode
            conn.execute(
                f"ATTACH DATABASE '{source_db_path}' AS {source_db_alias} (READ_ONLY)"
            )

            # 2. Create target schema if needed
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_bucket_name}")

            # 3. Create views for each table
            created_views = []
            for (table_name,) in tables:
                view_sql = f"""
                CREATE OR REPLACE VIEW {target_bucket_name}.{table_name} AS
                SELECT * FROM {source_db_alias}.{source_bucket_name}.{table_name}
                """
                conn.execute(view_sql)
                created_views.append(table_name)

            # 4. Detach the database (views will remain pointing to files)
            # Note: We detach because ATTACH is session-specific anyway
            # The views store the actual file path, not the alias
            conn.execute(f"DETACH DATABASE {source_db_alias}")

            conn.commit()

        logger.info(
            "bucket_linked_with_views",
            target_project=target_project_id,
            target_bucket=target_bucket_name,
            source_project=source_project_id,
            source_bucket=source_bucket_name,
            view_count=len(created_views),
        )

        return created_views

    def create_views_for_bucket(
        self,
        target_project_id: str,
        target_bucket_name: str,
        source_db_alias: str,
        source_bucket_name: str,
    ) -> list[str]:
        """
        Create views in target bucket pointing to tables in source bucket.

        DEPRECATED: Use link_bucket_with_views instead which handles ATTACH in same connection.

        This is used when linking a bucket - views are created that reference
        tables in the attached database.

        Args:
            target_project_id: The project where views will be created
            target_bucket_name: The schema name for the views
            source_db_alias: The alias of the attached database
            source_bucket_name: The schema name in the source database

        Returns:
            List of view names created
        """
        with self.connection(target_project_id) as conn:
            # First create the schema if it doesn't exist
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_bucket_name}")

            # Get list of tables in source bucket
            tables = conn.execute(
                f"""
                SELECT table_name
                FROM {source_db_alias}.information_schema.tables
                WHERE table_schema = ?
                """,
                [source_bucket_name],
            ).fetchall()

            created_views = []
            for (table_name,) in tables:
                # Create view: target_bucket.table -> source_db.source_bucket.table
                view_sql = f"""
                CREATE OR REPLACE VIEW {target_bucket_name}.{table_name} AS
                SELECT * FROM {source_db_alias}.{source_bucket_name}.{table_name}
                """
                conn.execute(view_sql)
                created_views.append(table_name)

            conn.commit()

        logger.info(
            "bucket_views_created",
            target_project=target_project_id,
            target_bucket=target_bucket_name,
            source_alias=source_db_alias,
            source_bucket=source_bucket_name,
            view_count=len(created_views),
        )

        return created_views

    def drop_bucket_views(
        self,
        target_project_id: str,
        target_bucket_name: str,
    ) -> None:
        """
        Drop all views in a bucket (used when unlinking).

        Args:
            target_project_id: The project containing the views
            target_bucket_name: The bucket/schema name
        """
        with self.connection(target_project_id) as conn:
            # Get all views in the schema
            views = conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ? AND table_type = 'VIEW'
                """,
                [target_bucket_name],
            ).fetchall()

            for (view_name,) in views:
                conn.execute(f"DROP VIEW IF EXISTS {target_bucket_name}.{view_name}")

            conn.commit()

        logger.info(
            "bucket_views_dropped",
            target_project=target_project_id,
            target_bucket=target_bucket_name,
            view_count=len(views),
        )

    # ========================================
    # Table operations
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
        Create a table in a bucket (schema).

        Args:
            project_id: The project ID
            bucket_name: The bucket/schema name
            table_name: The table name
            columns: List of column definitions with keys: name, type, nullable, default
            primary_key: Optional list of column names for primary key

        Returns:
            Table info dict with name, bucket, columns, row_count
        """
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
        create_sql = f"CREATE TABLE {bucket_name}.{table_name} ({columns_sql})"

        with self.connection(project_id) as conn:
            conn.execute(create_sql)
            conn.commit()

        logger.info(
            "table_created",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
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
        Delete a table from a bucket.

        Args:
            project_id: The project ID
            bucket_name: The bucket/schema name
            table_name: The table name

        Returns:
            True if successful
        """
        with self.connection(project_id) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {bucket_name}.{table_name}")
            conn.commit()

        logger.info(
            "table_deleted",
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

        Args:
            project_id: The project ID
            bucket_name: The bucket/schema name
            table_name: The table name

        Returns:
            Table info dict or None if not found
        """
        with self.connection(project_id, read_only=True) as conn:
            # Check if table exists
            table_result = conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'
                """,
                [bucket_name, table_name],
            ).fetchone()

            if not table_result:
                return None

            # Get column information
            columns_result = conn.execute(
                """
                SELECT column_name, data_type, is_nullable, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
                """,
                [bucket_name, table_name],
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
                f"SELECT COUNT(*) FROM {bucket_name}.{table_name}"
            ).fetchone()
            row_count = row_count_result[0] if row_count_result else 0

            # Try to get primary key info from duckdb_constraints
            primary_key = []
            try:
                pk_result = conn.execute(
                    """
                    SELECT constraint_column_names
                    FROM duckdb_constraints()
                    WHERE schema_name = ? AND table_name = ? AND constraint_type = 'PRIMARY KEY'
                    """,
                    [bucket_name, table_name],
                ).fetchone()
                if pk_result and pk_result[0]:
                    # constraint_column_names is a list of column names
                    primary_key = list(pk_result[0])
            except Exception:
                # Constraint query might fail in some cases, ignore
                pass

            # Estimate table size (DuckDB doesn't have direct table size, use file size as proxy)
            # For more accurate size, we'd need to use pragma_storage_info
            size_bytes = 0
            try:
                storage_info = conn.execute(
                    f"SELECT SUM(compressed_size) FROM pragma_storage_info('{bucket_name}.{table_name}')"
                ).fetchone()
                size_bytes = storage_info[0] if storage_info and storage_info[0] else 0
            except Exception:
                pass

            return {
                "name": table_name,
                "bucket": bucket_name,
                "columns": columns,
                "row_count": row_count,
                "size_bytes": size_bytes,
                "primary_key": primary_key,
                "created_at": None,  # DuckDB doesn't track creation time
            }

    def list_tables(
        self,
        project_id: str,
        bucket_name: str,
    ) -> list[dict[str, Any]]:
        """
        List all tables in a bucket.

        Args:
            project_id: The project ID
            bucket_name: The bucket/schema name

        Returns:
            List of table info dicts
        """
        with self.connection(project_id, read_only=True) as conn:
            tables_result = conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ? AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                [bucket_name],
            ).fetchall()

        tables = []
        for (table_name,) in tables_result:
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
        Check if a table exists in a bucket.

        Args:
            project_id: The project ID
            bucket_name: The bucket/schema name
            table_name: The table name

        Returns:
            True if table exists
        """
        with self.connection(project_id, read_only=True) as conn:
            result = conn.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'
                """,
                [bucket_name, table_name],
            ).fetchone()

            return result is not None

    def get_table_preview(
        self,
        project_id: str,
        bucket_name: str,
        table_name: str,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """
        Get a preview of table data.

        Args:
            project_id: The project ID
            bucket_name: The bucket/schema name
            table_name: The table name
            limit: Maximum number of rows to return (default 1000)

        Returns:
            Dict with columns, rows, total_row_count, preview_row_count
        """
        with self.connection(project_id, read_only=True) as conn:
            # Get column information
            columns_result = conn.execute(
                """
                SELECT column_name, data_type, is_nullable, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
                """,
                [bucket_name, table_name],
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
                f"SELECT COUNT(*) FROM {bucket_name}.{table_name}"
            ).fetchone()
            total_row_count = total_count_result[0] if total_count_result else 0

            # Get preview rows
            preview_result = conn.execute(
                f"SELECT * FROM {bucket_name}.{table_name} LIMIT {limit}"
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


# Global instances
metadata_db = MetadataDB()
project_db_manager = ProjectDBManager()
