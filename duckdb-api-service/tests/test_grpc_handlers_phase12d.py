"""Tests for Phase 12d gRPC handlers.

Tests cover:
- AddColumnHandler
- DropColumnHandler
- AlterColumnHandler
- AddPrimaryKeyHandler
- DropPrimaryKeyHandler
- DeleteTableRowsHandler

Note: Uses fixtures from conftest.py (metadata_db, project_db_manager, temp_data_dir)
"""

import pytest
import duckdb
import sys
from pathlib import Path

# Add generated proto to path
sys.path.insert(0, str(Path(__file__).parent.parent / "generated"))

from proto import common_pb2, table_pb2

from src.database import TABLE_DATA_NAME


# ============================================
# Fixtures
# ============================================


@pytest.fixture
def phase12d_project(metadata_db, project_db_manager) -> str:
    """Create a test project for Phase 12d and return its ID."""
    project_id = "test-project-12d"
    metadata_db.create_project(project_id, "Test Project 12d", None)
    project_db_manager.create_project_db(project_id)
    return project_id


@pytest.fixture
def phase12d_bucket(phase12d_project, project_db_manager) -> tuple[str, str]:
    """Create a test bucket and return (project_id, bucket_name)."""
    project_id = phase12d_project
    bucket_name = "phase12d_bucket"
    project_db_manager.create_bucket(project_id, bucket_name)
    return project_id, bucket_name


@pytest.fixture
def phase12d_table(phase12d_bucket, project_db_manager) -> tuple[str, str, str]:
    """Create a test table without primary key and return (project_id, bucket_name, table_name)."""
    project_id, bucket_name = phase12d_bucket
    table_name = "phase12d_table"

    project_db_manager.create_table(
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        columns=[
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": True},
        ],
        primary_key=[],  # No PK initially
    )

    return project_id, bucket_name, table_name


@pytest.fixture
def phase12d_table_with_pk(phase12d_bucket, project_db_manager) -> tuple[str, str, str]:
    """Create a test table WITH primary key."""
    project_id, bucket_name = phase12d_bucket
    table_name = "phase12d_table_pk"

    project_db_manager.create_table(
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        columns=[
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": True},
        ],
        primary_key=["id"],
    )

    return project_id, bucket_name, table_name


@pytest.fixture
def phase12d_table_with_data(phase12d_table, project_db_manager) -> tuple[str, str, str]:
    """Create a test table with sample data."""
    project_id, bucket_name, table_name = phase12d_table

    table_path = project_db_manager.get_table_path(project_id, bucket_name, table_name)
    conn = duckdb.connect(str(table_path))
    try:
        conn.execute(f"""
            INSERT INTO main.{TABLE_DATA_NAME} (id, name) VALUES
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')
        """)
        conn.commit()
    finally:
        conn.close()

    return project_id, bucket_name, table_name


# ============================================
# AddColumnHandler Tests
# ============================================


class TestAddColumnHandler:
    """Tests for AddColumnHandler."""

    def test_add_column_success(self, phase12d_table, project_db_manager):
        """Add column successfully."""
        from src.grpc.handlers.schema import AddColumnHandler

        project_id, bucket_name, table_name = phase12d_table
        handler = AddColumnHandler(project_db_manager)

        # Create command
        cmd = table_pb2.AddColumnCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name
        cmd.columnDefinition.name = "email"
        cmd.columnDefinition.type = "VARCHAR"
        cmd.columnDefinition.nullable = True

        # Pack into Any
        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # AddColumnCommand returns None
        assert response is None

        # Verify column was added
        table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
        column_names = [col["name"] for col in table_info["columns"]]
        assert "email" in column_names

    def test_add_column_project_not_found(self, project_db_manager):
        """Add column fails for non-existent project."""
        from src.grpc.handlers.schema import AddColumnHandler

        handler = AddColumnHandler(project_db_manager)

        cmd = table_pb2.AddColumnCommand()
        cmd.path.extend(["nonexistent-project", "bucket"])
        cmd.tableName = "table"
        cmd.columnDefinition.name = "email"
        cmd.columnDefinition.type = "VARCHAR"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# DropColumnHandler Tests
# ============================================


class TestDropColumnHandler:
    """Tests for DropColumnHandler."""

    def test_drop_column_success(self, phase12d_table, project_db_manager):
        """Drop column successfully."""
        from src.grpc.handlers.schema import DropColumnHandler

        project_id, bucket_name, table_name = phase12d_table
        handler = DropColumnHandler(project_db_manager)

        # Create command
        cmd = table_pb2.DropColumnCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name
        cmd.columnName = "name"  # Drop the 'name' column

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # DropColumnCommand returns None
        assert response is None

        # Verify column was dropped
        table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
        column_names = [col["name"] for col in table_info["columns"]]
        assert "name" not in column_names
        assert "id" in column_names

    def test_drop_column_table_not_found(self, phase12d_bucket, project_db_manager):
        """Drop column fails for non-existent table."""
        from src.grpc.handlers.schema import DropColumnHandler

        project_id, bucket_name = phase12d_bucket
        handler = DropColumnHandler(project_db_manager)

        cmd = table_pb2.DropColumnCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = "nonexistent_table"
        cmd.columnName = "name"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# AlterColumnHandler Tests
# ============================================


class TestAlterColumnHandler:
    """Tests for AlterColumnHandler."""

    def test_alter_column_change_type(self, phase12d_table, project_db_manager):
        """Alter column type successfully."""
        from src.grpc.handlers.schema import AlterColumnHandler

        project_id, bucket_name, table_name = phase12d_table
        handler = AlterColumnHandler(project_db_manager)

        # Create command
        cmd = table_pb2.AlterColumnCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name
        cmd.desiredDefiniton.name = "name"
        cmd.desiredDefiniton.type = "TEXT"  # Change from VARCHAR to TEXT
        cmd.attributesToUpdate.append("type")

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # AlterColumnCommand returns None
        assert response is None

        # Verify column type was changed
        table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
        name_col = next((c for c in table_info["columns"] if c["name"] == "name"), None)
        assert name_col is not None
        # DuckDB may normalize TEXT to VARCHAR, so just verify it's a string type
        assert "CHAR" in name_col["type"].upper() or "TEXT" in name_col["type"].upper()

    def test_alter_column_table_not_found(self, phase12d_bucket, project_db_manager):
        """Alter column fails for non-existent table."""
        from src.grpc.handlers.schema import AlterColumnHandler

        project_id, bucket_name = phase12d_bucket
        handler = AlterColumnHandler(project_db_manager)

        cmd = table_pb2.AlterColumnCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = "nonexistent_table"
        cmd.desiredDefiniton.name = "col"
        cmd.desiredDefiniton.type = "TEXT"
        cmd.attributesToUpdate.append("type")

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# AddPrimaryKeyHandler Tests
# ============================================


class TestAddPrimaryKeyHandler:
    """Tests for AddPrimaryKeyHandler."""

    def test_add_primary_key_success(self, phase12d_table, project_db_manager):
        """Add primary key successfully."""
        from src.grpc.handlers.schema import AddPrimaryKeyHandler

        project_id, bucket_name, table_name = phase12d_table
        handler = AddPrimaryKeyHandler(project_db_manager)

        # Create command
        cmd = table_pb2.AddPrimaryKeyCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name
        cmd.primaryKeysNames.append("id")

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # AddPrimaryKeyCommand returns None
        assert response is None

        # Verify PK was added
        table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
        assert "id" in table_info.get("primary_key", [])

    def test_add_primary_key_table_not_found(self, phase12d_bucket, project_db_manager):
        """Add PK fails for non-existent table."""
        from src.grpc.handlers.schema import AddPrimaryKeyHandler

        project_id, bucket_name = phase12d_bucket
        handler = AddPrimaryKeyHandler(project_db_manager)

        cmd = table_pb2.AddPrimaryKeyCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = "nonexistent_table"
        cmd.primaryKeysNames.append("id")

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# DropPrimaryKeyHandler Tests
# ============================================


class TestDropPrimaryKeyHandler:
    """Tests for DropPrimaryKeyHandler."""

    def test_drop_primary_key_success(self, phase12d_table_with_pk, project_db_manager):
        """Drop primary key successfully."""
        from src.grpc.handlers.schema import DropPrimaryKeyHandler

        project_id, bucket_name, table_name = phase12d_table_with_pk
        handler = DropPrimaryKeyHandler(project_db_manager)

        # Verify PK exists before
        table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
        assert len(table_info.get("primary_key", [])) > 0

        # Create command
        cmd = table_pb2.DropPrimaryKeyCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # DropPrimaryKeyCommand returns None
        assert response is None

        # Verify PK was dropped
        table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
        assert len(table_info.get("primary_key", [])) == 0

    def test_drop_primary_key_table_not_found(self, phase12d_bucket, project_db_manager):
        """Drop PK fails for non-existent table."""
        from src.grpc.handlers.schema import DropPrimaryKeyHandler

        project_id, bucket_name = phase12d_bucket
        handler = DropPrimaryKeyHandler(project_db_manager)

        cmd = table_pb2.DropPrimaryKeyCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = "nonexistent_table"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# DeleteTableRowsHandler Tests
# ============================================


class TestDeleteTableRowsHandler:
    """Tests for DeleteTableRowsHandler."""

    def test_delete_rows_success(self, phase12d_table_with_data, project_db_manager):
        """Delete rows successfully."""
        from src.grpc.handlers.schema import DeleteTableRowsHandler

        project_id, bucket_name, table_name = phase12d_table_with_data
        handler = DeleteTableRowsHandler(project_db_manager)

        # Create command - delete where name = 'Bob'
        cmd = table_pb2.DeleteTableRowsCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name

        # Add where filter
        wf = cmd.whereFilters.add()
        wf.columnsName = "name"
        wf.operator = 0  # eq (=)
        wf.values.append("Bob")

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # DeleteTableRowsCommand returns DeleteTableRowsResponse
        assert response is not None
        assert response.deletedRowsCount == 1
        assert response.tableRowsCount == 2  # 3 - 1 = 2

    def test_delete_rows_multiple_matches(self, phase12d_table_with_data, project_db_manager):
        """Delete multiple rows matching filter."""
        from src.grpc.handlers.schema import DeleteTableRowsHandler

        project_id, bucket_name, table_name = phase12d_table_with_data
        handler = DeleteTableRowsHandler(project_db_manager)

        # Delete where id >= 2 (Bob and Charlie)
        cmd = table_pb2.DeleteTableRowsCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name

        wf = cmd.whereFilters.add()
        wf.columnsName = "id"
        wf.operator = 3  # ge (>=)
        wf.values.append("2")

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        assert response.deletedRowsCount == 2
        assert response.tableRowsCount == 1  # Only Alice remains

    def test_delete_rows_table_not_found(self, phase12d_bucket, project_db_manager):
        """Delete rows fails for non-existent table."""
        from src.grpc.handlers.schema import DeleteTableRowsHandler

        project_id, bucket_name = phase12d_bucket
        handler = DeleteTableRowsHandler(project_db_manager)

        cmd = table_pb2.DeleteTableRowsCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = "nonexistent_table"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# Servicer Integration Tests
# ============================================


class TestServicerIntegration:
    """Integration tests using StorageDriverServicer."""

    def test_add_column_via_servicer(self, metadata_db, phase12d_table, project_db_manager):
        """Test AddColumnCommand through servicer."""
        from src.grpc.servicer import StorageDriverServicer

        project_id, bucket_name, table_name = phase12d_table
        servicer = StorageDriverServicer(metadata_db, project_db_manager)

        # Create command - note: DuckDB has limitations on ADD COLUMN with constraints
        # So we add a simple nullable column without constraints
        cmd = table_pb2.AddColumnCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name
        cmd.columnDefinition.name = "status"
        cmd.columnDefinition.type = "VARCHAR"
        cmd.columnDefinition.nullable = True  # Must be nullable for DuckDB ADD COLUMN

        # Create request
        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        # Mock context
        class MockContext:
            def set_code(self, code):
                pass

            def set_details(self, details):
                pass

        response = servicer.Execute(request, MockContext())

        # Verify response has log messages (at least one info message)
        assert len(response.messages) > 0

        # Verify column was added
        table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
        column_names = [col["name"] for col in table_info["columns"]]
        assert "status" in column_names

    def test_delete_rows_via_servicer(self, metadata_db, phase12d_table_with_data, project_db_manager):
        """Test DeleteTableRowsCommand through servicer."""
        from src.grpc.servicer import StorageDriverServicer

        project_id, bucket_name, table_name = phase12d_table_with_data
        servicer = StorageDriverServicer(metadata_db, project_db_manager)

        # Create command - delete where id = 1
        cmd = table_pb2.DeleteTableRowsCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name

        wf = cmd.whereFilters.add()
        wf.columnsName = "id"
        wf.operator = 0  # eq
        wf.values.append("1")

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        class MockContext:
            def set_code(self, code):
                pass

            def set_details(self, details):
                pass

        response = servicer.Execute(request, MockContext())

        # Verify response has commandResponse
        assert response.commandResponse.ByteSize() > 0

        # Unpack response
        delete_response = table_pb2.DeleteTableRowsResponse()
        response.commandResponse.Unpack(delete_response)

        assert delete_response.deletedRowsCount == 1
        assert delete_response.tableRowsCount == 2


# ============================================
# HTTP Bridge Integration Tests
# ============================================


class TestHTTPBridgeIntegration:
    """Test HTTP bridge includes Phase 12d commands."""

    def test_list_commands_includes_phase12d(self, client):
        """Verify /driver/commands includes Phase 12d commands."""
        response = client.get("/driver/commands")
        assert response.status_code == 200

        data = response.json()
        command_types = [cmd["type"] for cmd in data["supported_commands"]]

        # Verify Phase 12d commands are listed
        assert "AddColumnCommand" in command_types
        assert "DropColumnCommand" in command_types
        assert "AlterColumnCommand" in command_types
        assert "AddPrimaryKeyCommand" in command_types
        assert "DropPrimaryKeyCommand" in command_types
        assert "DeleteTableRowsCommand" in command_types

        # Verify total count (26 commands as of Phase 12e)
        assert data["total_commands"] == 26

    def test_add_column_via_http(self, client, phase12d_table, admin_headers, project_db_manager):
        """Test AddColumnCommand via HTTP bridge."""
        project_id, bucket_name, table_name = phase12d_table

        response = client.post(
            "/driver/execute",
            headers=admin_headers,
            json={
                "command": {
                    "type": "AddColumnCommand",
                    "path": [project_id, bucket_name],
                    "tableName": table_name,
                    "columnDefinition": {
                        "name": "http_test_col",
                        "type": "INTEGER",
                        "nullable": True,
                    },
                }
            },
        )

        assert response.status_code == 200

        # Verify column was added
        table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
        column_names = [col["name"] for col in table_info["columns"]]
        assert "http_test_col" in column_names

    def test_delete_rows_via_http(
        self, client, phase12d_table_with_data, admin_headers, project_db_manager
    ):
        """Test DeleteTableRowsCommand via HTTP bridge."""
        project_id, bucket_name, table_name = phase12d_table_with_data

        response = client.post(
            "/driver/execute",
            headers=admin_headers,
            json={
                "command": {
                    "type": "DeleteTableRowsCommand",
                    "path": [project_id, bucket_name],
                    "tableName": table_name,
                    "whereFilters": [
                        {"columnsName": "name", "operator": 0, "values": ["Alice"]}
                    ],
                }
            },
        )

        assert response.status_code == 200

        data = response.json()
        assert data["commandResponse"] is not None
        # Note: protobuf int64 fields are serialized as strings in JSON
        assert int(data["commandResponse"]["deletedRowsCount"]) == 1
        assert int(data["commandResponse"]["tableRowsCount"]) == 2
