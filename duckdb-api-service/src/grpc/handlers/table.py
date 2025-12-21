"""Table command handlers for gRPC service."""

import sys
from pathlib import Path
from typing import Optional

from google.protobuf import struct_pb2

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import table_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import ProjectDBManager


class CreateTableHandler(BaseCommandHandler):
    """
    Create a new table in a bucket.

    This handler:
    1. Parses path to extract project_id and bucket_name
    2. Converts column definitions from protobuf format
    3. Creates the table using ProjectDBManager
    4. Creates .duckdb file with specified columns and primary key

    Path format: [project_id, bucket_name] or [project_id, branch_id, bucket_name]
    """

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = table_pb2.CreateTableCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName

        if not table_name:
            raise ValueError("tableName is required")
        if len(path) < 1:
            raise ValueError("Path must contain at least bucket_name")

        # Parse path flexibly:
        # - [bucket_name] - project_id comes from credentials
        # - [project_id, bucket_name]
        # - [project_id, branch_id, bucket_name]
        if len(path) == 1:
            # Path contains only bucket_name, get project_id from credentials
            if not credentials or 'project_id' not in credentials:
                raise ValueError("Path must contain [project_id, bucket_name] or credentials must have project_id")
            project_id = credentials['project_id']
            bucket_name = path[0]
            branch_id = "default"
        elif len(path) == 2:
            project_id = path[0]
            bucket_name = path[1]
            branch_id = "default"
        else:
            project_id = path[0]
            bucket_name = path[-1]
            branch_id = path[1] if len(path) > 2 else "default"

        # Adjust project_id for branch operations
        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Convert columns from protobuf to dict format
        columns = []
        for col in cmd.columns:
            col_def = {
                "name": col.name,
                "type": col.type if col.type else "VARCHAR",
                "nullable": col.nullable,
            }
            if col.default:
                col_def["default"] = col.default
            columns.append(col_def)

        # Get primary key names
        primary_key = list(cmd.primaryKeysNames) if cmd.primaryKeysNames else None

        # Create table using existing logic
        self.project_manager.create_table(
            project_id=effective_project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            columns=columns,
            primary_key=primary_key,
        )

        self.log_info(
            f"Table {table_name} created in {bucket_name} "
            f"(project: {project_id}, columns: {len(columns)})"
        )

        # CreateTableCommand has no response message defined in proto
        return None


class DropTableHandler(BaseCommandHandler):
    """
    Drop (delete) a table from a bucket.

    This handler:
    1. Parses path to extract project_id and bucket_name
    2. Deletes the table .duckdb file
    3. Cleans up any table locks

    Path format: [project_id, bucket_name] or [project_id, branch_id, bucket_name]
    """

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = table_pb2.DropTableCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName

        if len(path) < 2:
            raise ValueError("Path must contain at least [project_id, bucket_name]")
        if not table_name:
            raise ValueError("tableName is required")

        # Parse path
        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        # Adjust project_id for branch operations
        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Check if table exists
        if not self.project_manager.table_exists(
            effective_project_id, bucket_name, table_name
        ):
            self.log_warning(
                f"Table {table_name} not found in {bucket_name} "
                f"(project: {project_id})"
            )
            # Idempotent - return success even if not found
            return None

        # Delete table
        self.project_manager.delete_table(
            project_id=effective_project_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )

        self.log_info(
            f"Table {table_name} dropped from {bucket_name} (project: {project_id})"
        )

        # DropTableCommand has no response message defined
        return None


class PreviewTableHandler(BaseCommandHandler):
    """
    Get a preview of table data.

    This handler:
    1. Parses path to extract project_id and bucket_name
    2. Queries table for preview data with optional filters
    3. Returns rows with column values

    Path format: [project_id, bucket_name] or [project_id, branch_id, bucket_name]
    """

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> table_pb2.PreviewTableResponse:
        cmd = table_pb2.PreviewTableCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName

        if len(path) < 2:
            raise ValueError("Path must contain at least [project_id, bucket_name]")
        if not table_name:
            raise ValueError("tableName is required")

        # Parse path
        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        # Adjust project_id for branch operations
        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Get limit from filters (default to 100)
        limit = 100
        if cmd.HasField("filters") and cmd.filters.limit > 0:
            limit = cmd.filters.limit

        # Get preview data
        preview_data = self.project_manager.get_table_preview(
            project_id=effective_project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            limit=limit,
        )

        # Build response
        response = table_pb2.PreviewTableResponse()

        # Column names (filter if specific columns requested)
        columns_filter = list(cmd.columns) if cmd.columns else None
        if columns_filter:
            response.columns.extend(columns_filter)
        else:
            response.columns.extend(
                [col["name"] for col in preview_data.get("columns", [])]
            )

        # Convert rows to protobuf format
        for row_data in preview_data.get("rows", []):
            row = response.rows.add()
            for col_name in response.columns:
                col = row.columns.add()
                col.columnName = col_name
                value = row_data.get(col_name)

                # Convert value to protobuf Value
                if value is None:
                    col.value.null_value = struct_pb2.NullValue.NULL_VALUE
                elif isinstance(value, bool):
                    col.value.bool_value = value
                elif isinstance(value, (int, float)):
                    col.value.number_value = float(value)
                elif isinstance(value, list):
                    # Convert list to ListValue
                    for item in value:
                        if isinstance(item, (int, float)):
                            col.value.list_value.values.add().number_value = float(item)
                        else:
                            col.value.list_value.values.add().string_value = str(item)
                else:
                    col.value.string_value = str(value)

                col.isTruncated = False

        self.log_info(
            f"Preview of {table_name} in {bucket_name}: "
            f"{len(preview_data.get('rows', []))} rows"
        )

        return response
