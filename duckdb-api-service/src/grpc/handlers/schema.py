"""Schema command handlers for gRPC service.

Phase 12d: Schema handlers
==========================
- AddColumnHandler
- DropColumnHandler
- AlterColumnHandler
- AddPrimaryKeyHandler
- DropPrimaryKeyHandler
- DeleteTableRowsHandler
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import table_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import ProjectDBManager


class AddColumnHandler(BaseCommandHandler):
    """Add a new column to an existing table."""

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(self, command, credentials, runtime_options):
        """
        Handle AddColumnCommand.

        Proto:
            message AddColumnCommand {
              repeated string path = 1;  // path where table is located
              string tableName = 2;      // table name
              TableColumnShared columnDefinition = 3;  // table column definition
            }
        """
        cmd = table_pb2.AddColumnCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName
        column_def = cmd.columnDefinition

        if len(path) < 2:
            raise ValueError("Invalid path: must include at least [project_id, bucket_name]")

        project_id = path[0]
        bucket_name = path[-1]  # Last element is bucket
        branch_id = path[1] if len(path) > 2 else "default"

        # Validate project exists
        if not self.project_manager.project_exists(project_id):
            raise KeyError(f"Project {project_id} not found")

        # Validate bucket exists
        if not self.project_manager.bucket_exists(project_id, bucket_name):
            raise KeyError(f"Bucket {bucket_name} not found in project {project_id}")

        # Validate table exists
        if not self.project_manager.table_exists(project_id, bucket_name, table_name):
            raise KeyError(f"Table {table_name} not found in bucket {bucket_name}")

        # Extract column definition
        column_name = column_def.name
        column_type = column_def.type if column_def.type else "VARCHAR"
        nullable = column_def.nullable
        default = column_def.default if column_def.default else None

        # Add column using existing logic
        self.project_manager.add_column(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            column_type=column_type,
            nullable=nullable,
            default=default,
        )

        self.log_info(f"Column {column_name} added to table {table_name}")

        # AddColumnCommand has no response in proto
        return None


class DropColumnHandler(BaseCommandHandler):
    """Drop a column from an existing table."""

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(self, command, credentials, runtime_options):
        """
        Handle DropColumnCommand.

        Proto:
            message DropColumnCommand {
              repeated string path = 1;  // path where table is located
              string tableName = 2;      // table name
              string columnName = 3;     // column to drop
            }
        """
        cmd = table_pb2.DropColumnCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName
        column_name = cmd.columnName

        if len(path) < 2:
            raise ValueError("Invalid path: must include at least [project_id, bucket_name]")

        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        # Validate project exists
        if not self.project_manager.project_exists(project_id):
            raise KeyError(f"Project {project_id} not found")

        # Validate bucket exists
        if not self.project_manager.bucket_exists(project_id, bucket_name):
            raise KeyError(f"Bucket {bucket_name} not found in project {project_id}")

        # Validate table exists
        if not self.project_manager.table_exists(project_id, bucket_name, table_name):
            raise KeyError(f"Table {table_name} not found in bucket {bucket_name}")

        # Drop column using existing logic
        self.project_manager.drop_column(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
        )

        self.log_info(f"Column {column_name} dropped from table {table_name}")

        # DropColumnCommand has no response in proto
        return None


class AlterColumnHandler(BaseCommandHandler):
    """Alter a column in an existing table."""

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(self, command, credentials, runtime_options):
        """
        Handle AlterColumnCommand.

        Proto:
            message AlterColumnCommand {
              repeated string path = 1;
              string tableName = 2;
              TableColumnShared desiredDefiniton = 3;  // desired definition
              repeated string attributesToUpdate = 4;  // attributes to update
            }
        """
        cmd = table_pb2.AlterColumnCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName
        desired_def = cmd.desiredDefiniton
        attributes_to_update = list(cmd.attributesToUpdate)

        if len(path) < 2:
            raise ValueError("Invalid path: must include at least [project_id, bucket_name]")

        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        # Validate project exists
        if not self.project_manager.project_exists(project_id):
            raise KeyError(f"Project {project_id} not found")

        # Validate bucket exists
        if not self.project_manager.bucket_exists(project_id, bucket_name):
            raise KeyError(f"Bucket {bucket_name} not found in project {project_id}")

        # Validate table exists
        if not self.project_manager.table_exists(project_id, bucket_name, table_name):
            raise KeyError(f"Table {table_name} not found in bucket {bucket_name}")

        column_name = desired_def.name

        # Determine what to update based on attributesToUpdate
        new_name = None
        new_type = None
        set_not_null = None
        set_default = None

        if "name" in attributes_to_update:
            new_name = desired_def.name
        if "type" in attributes_to_update:
            new_type = desired_def.type
        if "nullable" in attributes_to_update:
            set_not_null = not desired_def.nullable
        if "default" in attributes_to_update:
            set_default = desired_def.default

        # Alter column using existing logic
        self.project_manager.alter_column(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            new_name=new_name,
            new_type=new_type,
            set_not_null=set_not_null,
            set_default=set_default,
        )

        self.log_info(f"Column {column_name} altered in table {table_name}")

        # AlterColumnCommand has no response in proto
        return None


class AddPrimaryKeyHandler(BaseCommandHandler):
    """Add a primary key constraint to a table."""

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(self, command, credentials, runtime_options):
        """
        Handle AddPrimaryKeyCommand.

        Proto:
            message AddPrimaryKeyCommand {
              repeated string path = 1;           // path where table is located
              string tableName = 2;               // table name
              repeated string primaryKeysNames = 4;  // primary key columns
            }
        """
        cmd = table_pb2.AddPrimaryKeyCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName
        pk_columns = list(cmd.primaryKeysNames)

        if len(path) < 2:
            raise ValueError("Invalid path: must include at least [project_id, bucket_name]")

        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        # Validate project exists
        if not self.project_manager.project_exists(project_id):
            raise KeyError(f"Project {project_id} not found")

        # Validate bucket exists
        if not self.project_manager.bucket_exists(project_id, bucket_name):
            raise KeyError(f"Bucket {bucket_name} not found in project {project_id}")

        # Validate table exists
        if not self.project_manager.table_exists(project_id, bucket_name, table_name):
            raise KeyError(f"Table {table_name} not found in bucket {bucket_name}")

        # Add primary key using existing logic
        self.project_manager.add_primary_key(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            columns=pk_columns,
        )

        self.log_info(f"Primary key {pk_columns} added to table {table_name}")

        # AddPrimaryKeyCommand has no response in proto
        return None


class DropPrimaryKeyHandler(BaseCommandHandler):
    """Remove the primary key constraint from a table."""

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(self, command, credentials, runtime_options):
        """
        Handle DropPrimaryKeyCommand.

        Proto:
            message DropPrimaryKeyCommand {
              repeated string path = 1;   // path where table is located
              string tableName = 2;       // table name
            }
        """
        cmd = table_pb2.DropPrimaryKeyCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName

        if len(path) < 2:
            raise ValueError("Invalid path: must include at least [project_id, bucket_name]")

        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        # Validate project exists
        if not self.project_manager.project_exists(project_id):
            raise KeyError(f"Project {project_id} not found")

        # Validate bucket exists
        if not self.project_manager.bucket_exists(project_id, bucket_name):
            raise KeyError(f"Bucket {bucket_name} not found in project {project_id}")

        # Validate table exists
        if not self.project_manager.table_exists(project_id, bucket_name, table_name):
            raise KeyError(f"Table {table_name} not found in bucket {bucket_name}")

        # Drop primary key using existing logic
        self.project_manager.drop_primary_key(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )

        self.log_info(f"Primary key dropped from table {table_name}")

        # DropPrimaryKeyCommand has no response in proto
        return None


class DeleteTableRowsHandler(BaseCommandHandler):
    """Delete rows from a table based on filters."""

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(self, command, credentials, runtime_options):
        """
        Handle DeleteTableRowsCommand.

        Proto:
            message DeleteTableRowsCommand {
              repeated string path = 1;
              string tableName = 2;
              string changeSince = 3;
              string changeUntil = 4;
              repeated TableWhereFilter whereFilters = 5;
              repeated WhereRefTableFilter whereRefTableFilters = 6;
            }

            message DeleteTableRowsResponse {
              int64 deletedRowsCount = 1;
              int64 tableRowsCount = 2;
              int64 tableSizeBytes = 3;
            }
        """
        cmd = table_pb2.DeleteTableRowsCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName
        change_since = cmd.changeSince if cmd.changeSince else None
        change_until = cmd.changeUntil if cmd.changeUntil else None
        where_filters = list(cmd.whereFilters)

        if len(path) < 2:
            raise ValueError("Invalid path: must include at least [project_id, bucket_name]")

        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        # Validate project exists
        if not self.project_manager.project_exists(project_id):
            raise KeyError(f"Project {project_id} not found")

        # Validate bucket exists
        if not self.project_manager.bucket_exists(project_id, bucket_name):
            raise KeyError(f"Bucket {bucket_name} not found in project {project_id}")

        # Validate table exists
        if not self.project_manager.table_exists(project_id, bucket_name, table_name):
            raise KeyError(f"Table {table_name} not found in bucket {bucket_name}")

        # Build WHERE clause from filters
        where_clauses = []

        # Handle changeSince/changeUntil (timestamp-based filters)
        if change_since:
            where_clauses.append(f"_timestamp >= '{change_since}'")
        if change_until:
            where_clauses.append(f"_timestamp <= '{change_until}'")

        # Handle whereFilters
        operator_map = {
            0: "=",  # eq
            1: "!=",  # ne
            2: ">",  # gt
            3: ">=",  # ge
            4: "<",  # lt
            5: "<=",  # le
        }

        for wf in where_filters:
            column_name = wf.columnsName
            operator = operator_map.get(wf.operator, "=")
            values = list(wf.values)

            if len(values) == 1:
                where_clauses.append(f'"{column_name}" {operator} \'{values[0]}\'')
            elif len(values) > 1:
                values_str = ", ".join([f"'{v}'" for v in values])
                where_clauses.append(f'"{column_name}" IN ({values_str})')

        # Combine WHERE clauses
        if not where_clauses:
            # If no filters, delete all rows (equivalent to TRUNCATE but with stats)
            where_clause = "1=1"
        else:
            where_clause = " AND ".join(where_clauses)

        # Delete rows using existing logic
        result = self.project_manager.delete_table_rows(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            where_clause=where_clause,
        )

        self.log_info(f"Deleted {result['deleted_rows']} rows from table {table_name}")

        # Create response
        response = table_pb2.DeleteTableRowsResponse()
        response.deletedRowsCount = result["deleted_rows"]
        response.tableRowsCount = result["table_rows_after"]
        response.tableSizeBytes = result.get("size_bytes", 0)

        return response
