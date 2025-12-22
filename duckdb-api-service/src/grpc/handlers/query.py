"""Execute query command handler for gRPC service.

Phase 12g: ExecuteQueryCommand handler
Executes SQL queries on a project database.
"""

import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import executeQuery_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import MetadataDB, ProjectDBManager


class ExecuteQueryHandler(BaseCommandHandler):
    """
    Execute a SQL query on a project database.

    This handler:
    1. Validates the query (basic sanity check)
    2. Executes query on the project's tables
    3. Returns results for SELECT queries
    4. Returns status for non-SELECT queries

    Note: For security, this should only be used with trusted SQL
    from Connection, not user-provided queries.
    """

    def __init__(self, metadata_db: MetadataDB, project_manager: ProjectDBManager):
        super().__init__()
        self.metadata_db = metadata_db
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> executeQuery_pb2.ExecuteQueryResponse:
        cmd = executeQuery_pb2.ExecuteQueryCommand()
        command.Unpack(cmd)

        query = cmd.query
        timeout = cmd.timeout or 300  # Default 5 minutes
        path_restriction = list(cmd.pathRestriction)

        if not query:
            raise ValueError("query is required")

        # Extract project_id from credentials or path
        project_id = None
        if credentials and "project_id" in credentials:
            project_id = credentials["project_id"]
        elif path_restriction:
            project_id = path_restriction[0]

        if not project_id:
            raise ValueError("project_id must be provided via credentials or pathRestriction")

        # Verify project exists
        project = self.metadata_db.get_project(project_id)
        if not project:
            raise KeyError(f"Project {project_id} not found")

        self.log_info(f"Executing query on project {project_id}")

        start_time = time.time()

        try:
            # Execute query on project database
            # For DuckDB with per-table files, we need to execute on a workspace
            # or on a specific table. For now, we'll create a temporary connection
            # and attach the needed tables.

            result = self._execute_project_query(project_id, query, path_restriction)

            duration_ms = int((time.time() - start_time) * 1000)
            self.log_info(f"Query executed in {duration_ms}ms")

            return result

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            self.log_error(f"Query failed after {duration_ms}ms: {e}")

            response = executeQuery_pb2.ExecuteQueryResponse()
            response.status = executeQuery_pb2.ExecuteQueryResponse.Status.Error
            response.message = str(e)
            return response

    def _execute_project_query(
        self,
        project_id: str,
        query: str,
        path_restriction: list
    ) -> executeQuery_pb2.ExecuteQueryResponse:
        """Execute SQL query on project database."""
        import duckdb
        from src.config import settings

        # For simple queries, we can use an in-memory connection
        # and ATTACH the relevant tables
        conn = duckdb.connect(":memory:")

        try:
            # Determine which tables need to be attached based on query/path
            # For now, we'll try to execute directly and let it fail if
            # tables are not available.

            # If path restriction includes bucket/table, attach those
            if len(path_restriction) >= 2:
                bucket_name = path_restriction[1] if len(path_restriction) > 1 else None
                if bucket_name:
                    # Attach all tables in the bucket
                    bucket_path = settings.duckdb_path / project_id / bucket_name
                    if bucket_path.exists():
                        for table_file in bucket_path.glob("*.duckdb"):
                            table_name = table_file.stem
                            alias = f"{bucket_name}_{table_name}"
                            conn.execute(
                                f"ATTACH '{table_file}' AS {alias} (READ_ONLY)"
                            )

            # Execute the query
            result = conn.execute(query)

            # Check if this is a SELECT query (returns results)
            description = result.description
            if description:
                # It's a SELECT query
                columns = [col[0] for col in description]
                rows = result.fetchall()

                response = executeQuery_pb2.ExecuteQueryResponse()
                response.status = executeQuery_pb2.ExecuteQueryResponse.Status.Success

                # Build data response
                response.data.columns.extend(columns)
                for row in rows:
                    row_msg = executeQuery_pb2.ExecuteQueryResponse.Data.Row()
                    for i, col in enumerate(columns):
                        # Convert value to string for proto
                        value = row[i]
                        row_msg.fields[col] = str(value) if value is not None else ""
                    response.data.rows.append(row_msg)

                response.message = f"Query returned {len(rows)} rows"
                return response
            else:
                # Non-SELECT query (CREATE, INSERT, etc.)
                response = executeQuery_pb2.ExecuteQueryResponse()
                response.status = executeQuery_pb2.ExecuteQueryResponse.Status.Success
                response.message = "Query executed successfully"
                return response

        finally:
            conn.close()
