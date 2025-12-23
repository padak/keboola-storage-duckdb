"""Object info command handlers for gRPC service."""

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import info_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import ProjectDBManager


class ObjectInfoHandler(BaseCommandHandler):
    """
    Get information about a storage object (table, schema/bucket, database/project).

    This handler:
    1. Determines object type from path and expectedObjectType
    2. Returns appropriate info based on object type:
       - TABLE: Column info, row count, size, primary keys
       - SCHEMA (bucket): List of tables in bucket
       - DATABASE (project): List of buckets in project

    Path format varies by object type:
    - DATABASE: [project_id]
    - SCHEMA: [project_id, bucket_name]
    - TABLE: [project_id, bucket_name, table_name]
    """

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> info_pb2.ObjectInfoResponse:
        cmd = info_pb2.ObjectInfoCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        expected_type = cmd.expectedObjectType

        if not path:
            raise ValueError("Path is required")

        # Build response
        response = info_pb2.ObjectInfoResponse()
        response.path.extend(path)

        # Handle based on expected object type
        if expected_type == info_pb2.TABLE:
            self._handle_table_info(path, response)
        elif expected_type == info_pb2.SCHEMA:
            self._handle_schema_info(path, response)
        elif expected_type == info_pb2.DATABASE:
            self._handle_database_info(path, response)
        elif expected_type == info_pb2.VIEW:
            # Views are not supported in DuckDB backend
            raise ValueError("VIEW object type not supported")
        else:
            # Default to TABLE if not specified
            self._handle_table_info(path, response)

        return response

    def _handle_table_info(
        self, path: list, response: info_pb2.ObjectInfoResponse
    ) -> None:
        """Handle TABLE object info request."""
        if len(path) < 3:
            raise ValueError(
                "Path for TABLE must contain [project_id, bucket_name, table_name]"
            )

        project_id = path[0]
        bucket_name = path[-2]  # Second to last
        table_name = path[-1]   # Last element

        # Handle branch in path if present
        branch_id = "default"
        if len(path) > 3:
            branch_id = path[1]
            bucket_name = path[-2]
            table_name = path[-1]

        # Adjust project_id for branch operations
        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Get table info
        table_data = self.project_manager.get_table(
            effective_project_id, bucket_name, table_name
        )

        if not table_data:
            raise KeyError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        response.objectType = info_pb2.TABLE

        # Fill TableInfo
        table_info = response.tableInfo
        table_info.path.extend(path[:-1])  # Path without table name
        table_info.tableName = table_name
        table_info.rowsCount = table_data.get("row_count", 0)
        table_info.sizeBytes = table_data.get("size_bytes", 0)
        table_info.tableType = info_pb2.NORMAL

        # Add columns
        for col in table_data.get("columns", []):
            tc = table_info.columns.add()
            tc.name = col["name"]
            tc.type = col["type"]
            tc.nullable = col.get("nullable", True)
            if col.get("default"):
                tc.default = str(col["default"])

        # Add primary keys
        table_info.primaryKeysNames.extend(table_data.get("primary_key", []))

        self.log_info(
            f"ObjectInfo for TABLE {table_name}: "
            f"{table_data.get('row_count', 0)} rows, "
            f"{len(table_data.get('columns', []))} columns"
        )

    def _handle_schema_info(
        self, path: list, response: info_pb2.ObjectInfoResponse
    ) -> None:
        """Handle SCHEMA (bucket) object info request."""
        if len(path) < 2:
            raise ValueError(
                "Path for SCHEMA must contain [project_id, bucket_name]"
            )

        project_id = path[0]
        bucket_name = path[-1]

        # Handle branch in path if present
        branch_id = "default"
        if len(path) > 2:
            branch_id = path[1]
            bucket_name = path[-1]

        # Adjust project_id for branch operations
        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Check if bucket exists
        if not self.project_manager.bucket_exists(effective_project_id, bucket_name):
            raise KeyError(f"Bucket not found: {project_id}/{bucket_name}")

        # Get list of tables in bucket
        tables = self.project_manager.list_tables(effective_project_id, bucket_name)

        response.objectType = info_pb2.SCHEMA

        # Fill SchemaInfo with list of objects
        schema_info = response.schemaInfo
        for table in tables:
            obj = schema_info.objects.add()
            obj.objectName = table["name"]
            obj.objectType = info_pb2.TABLE

        self.log_info(
            f"ObjectInfo for SCHEMA {bucket_name}: {len(tables)} tables"
        )

    def _handle_database_info(
        self, path: list, response: info_pb2.ObjectInfoResponse
    ) -> None:
        """Handle DATABASE (project) object info request."""
        if not path:
            raise ValueError("Path for DATABASE must contain [project_id]")

        project_id = path[0]

        # Handle branch in path if present
        branch_id = "default"
        if len(path) > 1:
            branch_id = path[1]

        # Adjust project_id for branch operations
        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Get list of buckets in project
        buckets = self.project_manager.list_buckets(effective_project_id)

        response.objectType = info_pb2.DATABASE

        # Fill DatabaseInfo with list of objects (buckets as schemas)
        database_info = response.databaseInfo
        for bucket in buckets:
            obj = database_info.objects.add()
            obj.objectName = bucket["name"]
            obj.objectType = info_pb2.SCHEMA

        self.log_info(
            f"ObjectInfo for DATABASE {project_id}: {len(buckets)} buckets"
        )
