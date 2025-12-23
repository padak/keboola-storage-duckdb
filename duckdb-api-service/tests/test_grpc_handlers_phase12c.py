"""Tests for Phase 12c gRPC handlers.

Tests cover:
- CreateBucketHandler, DropBucketHandler
- CreateTableHandler, DropTableHandler, PreviewTableHandler
- ObjectInfoHandler
- TableImportFromFileHandler, TableExportToFileHandler (basic tests)

Note: Uses fixtures from conftest.py (metadata_db, project_db_manager, temp_data_dir)
"""

import pytest
import grpc
import sys
from pathlib import Path

# Add generated proto to path
sys.path.insert(0, str(Path(__file__).parent.parent / "generated"))

from proto import common_pb2, bucket_pb2, table_pb2, info_pb2


# ============================================
# Fixtures (extend conftest.py fixtures)
# ============================================


@pytest.fixture
def phase12c_project(metadata_db, project_db_manager) -> str:
    """Create a test project for Phase 12c and return its ID."""
    project_id = "test-project-12c"

    # Register in metadata
    metadata_db.create_project(project_id, "Test Project", None)

    # Create project directory structure
    project_db_manager.create_project_db(project_id)

    return project_id


@pytest.fixture
def phase12c_bucket(phase12c_project, project_db_manager) -> tuple[str, str]:
    """Create a test bucket and return (project_id, bucket_name)."""
    project_id = phase12c_project
    bucket_name = "phase12c_bucket"

    project_db_manager.create_bucket(project_id, bucket_name)

    return project_id, bucket_name


@pytest.fixture
def phase12c_table(phase12c_bucket, project_db_manager) -> tuple[str, str, str]:
    """Create a test table and return (project_id, bucket_name, table_name)."""
    project_id, bucket_name = phase12c_bucket
    table_name = "phase12c_table"

    project_db_manager.create_table(
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        columns=[
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": True},
            {"name": "value", "type": "DOUBLE", "nullable": True},
        ],
        primary_key=["id"],
    )

    return project_id, bucket_name, table_name


@pytest.fixture
def phase12c_table_with_data(phase12c_table, project_db_manager) -> tuple[str, str, str]:
    """Create a test table with sample data."""
    import duckdb
    from src.database import TABLE_DATA_NAME

    project_id, bucket_name, table_name = phase12c_table

    # Insert some test data
    table_path = project_db_manager.get_table_path(project_id, bucket_name, table_name)
    conn = duckdb.connect(str(table_path))
    try:
        conn.execute(f"""
            INSERT INTO main.{TABLE_DATA_NAME} (id, name, value) VALUES
            (1, 'Alice', 100.5),
            (2, 'Bob', 200.0),
            (3, 'Charlie', 300.25)
        """)
        conn.commit()
    finally:
        conn.close()

    return project_id, bucket_name, table_name


# ============================================
# Bucket Handler Tests
# ============================================

class TestCreateBucketHandler:
    """Tests for CreateBucketHandler."""

    def test_create_bucket_success(self, phase12c_project, project_db_manager):
        """Create bucket successfully."""
        from src.grpc.handlers.bucket import CreateBucketHandler

        handler = CreateBucketHandler(project_db_manager)

        # Create command
        cmd = bucket_pb2.CreateBucketCommand()
        cmd.projectId = phase12c_project
        cmd.bucketId = "in.c-new-bucket"

        # Pack into Any
        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # Verify
        assert response is not None
        assert response.createBucketObjectName == "in_c_new_bucket"
        assert phase12c_project in response.path

        # Verify bucket exists
        assert project_db_manager.bucket_exists(phase12c_project, "in_c_new_bucket")

    def test_create_bucket_missing_project_id(self, project_db_manager):
        """Create bucket fails without project ID."""
        from src.grpc.handlers.bucket import CreateBucketHandler

        handler = CreateBucketHandler(project_db_manager)

        cmd = bucket_pb2.CreateBucketCommand()
        cmd.bucketId = "test-bucket"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(ValueError, match="projectId is required"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


class TestDropBucketHandler:
    """Tests for DropBucketHandler."""

    def test_drop_bucket_success(self, phase12c_bucket, project_db_manager):
        """Drop bucket successfully."""
        from src.grpc.handlers.bucket import DropBucketHandler

        project_id, bucket_name = phase12c_bucket
        handler = DropBucketHandler(project_db_manager)

        # Create command
        cmd = bucket_pb2.DropBucketCommand()
        cmd.bucketObjectName = bucket_name
        cmd.isCascade = True

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute with credentials
        credentials = {"project_id": project_id, "api_key": "test-key"}
        response = handler.handle(any_cmd, credentials, common_pb2.RuntimeOptions())

        # Verify
        assert response is None  # No response for drop
        assert not project_db_manager.bucket_exists(project_id, bucket_name)

    def test_drop_nonexistent_bucket(self, phase12c_project, project_db_manager):
        """Drop nonexistent bucket is idempotent."""
        from src.grpc.handlers.bucket import DropBucketHandler

        handler = DropBucketHandler(project_db_manager)

        cmd = bucket_pb2.DropBucketCommand()
        cmd.bucketObjectName = "nonexistent-bucket"
        cmd.isCascade = True

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        credentials = {"project_id": phase12c_project, "api_key": "test-key"}

        # Should not raise
        response = handler.handle(any_cmd, credentials, common_pb2.RuntimeOptions())
        assert response is None


# ============================================
# Table Handler Tests
# ============================================

class TestCreateTableHandler:
    """Tests for CreateTableHandler."""

    def test_create_table_success(self, phase12c_bucket, project_db_manager):
        """Create table successfully."""
        from src.grpc.handlers.table import CreateTableHandler

        project_id, bucket_name = phase12c_bucket
        handler = CreateTableHandler(project_db_manager)

        # Create command
        cmd = table_pb2.CreateTableCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = "new_table"

        # Add columns
        col1 = cmd.columns.add()
        col1.name = "id"
        col1.type = "INTEGER"
        col1.nullable = False

        col2 = cmd.columns.add()
        col2.name = "description"
        col2.type = "VARCHAR"
        col2.nullable = True

        cmd.primaryKeysNames.append("id")

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # Verify
        assert response is None  # No response for create table
        assert project_db_manager.table_exists(project_id, bucket_name, "new_table")

    def test_create_table_missing_path(self, project_db_manager):
        """Create table fails with invalid path."""
        from src.grpc.handlers.table import CreateTableHandler

        handler = CreateTableHandler(project_db_manager)

        cmd = table_pb2.CreateTableCommand()
        cmd.path.append("only-one-element")
        cmd.tableName = "test"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(ValueError, match="Path must contain"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


class TestDropTableHandler:
    """Tests for DropTableHandler."""

    def test_drop_table_success(self, phase12c_table, project_db_manager):
        """Drop table successfully."""
        from src.grpc.handlers.table import DropTableHandler

        project_id, bucket_name, table_name = phase12c_table
        handler = DropTableHandler(project_db_manager)

        # Create command
        cmd = table_pb2.DropTableCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # Verify
        assert response is None
        assert not project_db_manager.table_exists(project_id, bucket_name, table_name)

    def test_drop_nonexistent_table(self, phase12c_bucket, project_db_manager):
        """Drop nonexistent table is idempotent."""
        from src.grpc.handlers.table import DropTableHandler

        project_id, bucket_name = phase12c_bucket
        handler = DropTableHandler(project_db_manager)

        cmd = table_pb2.DropTableCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = "nonexistent"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Should not raise
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None


class TestPreviewTableHandler:
    """Tests for PreviewTableHandler."""

    def test_preview_table_success(self, phase12c_table_with_data, project_db_manager):
        """Preview table returns rows."""
        from src.grpc.handlers.table import PreviewTableHandler

        project_id, bucket_name, table_name = phase12c_table_with_data
        handler = PreviewTableHandler(project_db_manager)

        # Create command
        cmd = table_pb2.PreviewTableCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # Verify
        assert response is not None
        assert len(response.columns) == 3  # id, name, value
        assert len(response.rows) == 3  # 3 rows

    def test_preview_table_with_limit(self, phase12c_table_with_data, project_db_manager):
        """Preview table respects limit."""
        from src.grpc.handlers.table import PreviewTableHandler

        project_id, bucket_name, table_name = phase12c_table_with_data
        handler = PreviewTableHandler(project_db_manager)

        cmd = table_pb2.PreviewTableCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = table_name
        cmd.filters.limit = 1

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        assert len(response.rows) == 1


# ============================================
# Object Info Handler Tests
# ============================================

class TestObjectInfoHandler:
    """Tests for ObjectInfoHandler."""

    def test_table_info(self, phase12c_table_with_data, project_db_manager):
        """Get table info."""
        from src.grpc.handlers.info import ObjectInfoHandler

        project_id, bucket_name, table_name = phase12c_table_with_data
        handler = ObjectInfoHandler(project_db_manager)

        # Create command
        cmd = info_pb2.ObjectInfoCommand()
        cmd.path.extend([project_id, bucket_name, table_name])
        cmd.expectedObjectType = info_pb2.TABLE

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # Verify
        assert response.objectType == info_pb2.TABLE
        assert response.tableInfo.tableName == table_name
        assert response.tableInfo.rowsCount == 3
        assert len(response.tableInfo.columns) == 3
        assert "id" in response.tableInfo.primaryKeysNames

    def test_schema_info(self, phase12c_table, project_db_manager):
        """Get bucket (schema) info."""
        from src.grpc.handlers.info import ObjectInfoHandler

        project_id, bucket_name, table_name = phase12c_table
        handler = ObjectInfoHandler(project_db_manager)

        cmd = info_pb2.ObjectInfoCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.expectedObjectType = info_pb2.SCHEMA

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        assert response.objectType == info_pb2.SCHEMA
        assert len(response.schemaInfo.objects) == 1
        assert response.schemaInfo.objects[0].objectName == table_name
        assert response.schemaInfo.objects[0].objectType == info_pb2.TABLE

    def test_database_info(self, phase12c_bucket, project_db_manager):
        """Get project (database) info."""
        from src.grpc.handlers.info import ObjectInfoHandler

        project_id, bucket_name = phase12c_bucket
        handler = ObjectInfoHandler(project_db_manager)

        cmd = info_pb2.ObjectInfoCommand()
        cmd.path.append(project_id)
        cmd.expectedObjectType = info_pb2.DATABASE

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        assert response.objectType == info_pb2.DATABASE
        assert len(response.databaseInfo.objects) == 1
        assert response.databaseInfo.objects[0].objectName == bucket_name
        assert response.databaseInfo.objects[0].objectType == info_pb2.SCHEMA

    def test_table_not_found(self, phase12c_bucket, project_db_manager):
        """Get info for nonexistent table raises KeyError."""
        from src.grpc.handlers.info import ObjectInfoHandler

        project_id, bucket_name = phase12c_bucket
        handler = ObjectInfoHandler(project_db_manager)

        cmd = info_pb2.ObjectInfoCommand()
        cmd.path.extend([project_id, bucket_name, "nonexistent"])
        cmd.expectedObjectType = info_pb2.TABLE

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="Table not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# Import/Export Handler Tests (Basic)
# ============================================

class TestTableImportFromFileHandler:
    """Basic tests for TableImportFromFileHandler.

    Note: Full S3 integration tests require actual S3 setup.
    These tests verify handler initialization and basic validation.
    """

    def test_handler_initialization(self, project_db_manager):
        """Handler can be initialized."""
        from src.grpc.handlers.import_export import TableImportFromFileHandler

        handler = TableImportFromFileHandler(project_db_manager)
        assert handler is not None

    def test_import_missing_table_raises(self, phase12c_bucket, project_db_manager):
        """Import to nonexistent table raises KeyError."""
        from src.grpc.handlers.import_export import TableImportFromFileHandler

        project_id, bucket_name = phase12c_bucket
        handler = TableImportFromFileHandler(project_db_manager)

        # Create command for nonexistent table
        cmd = table_pb2.TableImportFromFileCommand()
        cmd.destination.path.extend([project_id, bucket_name])
        cmd.destination.tableName = "nonexistent"
        cmd.fileProvider = table_pb2.ImportExportShared.FileProvider.S3
        cmd.filePath.root = "bucket"
        cmd.filePath.fileName = "data.csv"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="Table not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


class TestTableExportToFileHandler:
    """Basic tests for TableExportToFileHandler."""

    def test_handler_initialization(self, project_db_manager):
        """Handler can be initialized."""
        from src.grpc.handlers.import_export import TableExportToFileHandler

        handler = TableExportToFileHandler(project_db_manager)
        assert handler is not None

    def test_export_missing_table_raises(self, phase12c_bucket, project_db_manager):
        """Export from nonexistent table raises KeyError."""
        from src.grpc.handlers.import_export import TableExportToFileHandler

        project_id, bucket_name = phase12c_bucket
        handler = TableExportToFileHandler(project_db_manager)

        # Create command for nonexistent table
        cmd = table_pb2.TableExportToFileCommand()
        cmd.source.path.extend([project_id, bucket_name])
        cmd.source.tableName = "nonexistent"
        cmd.fileProvider = table_pb2.ImportExportShared.FileProvider.S3
        cmd.filePath.root = "bucket"
        cmd.filePath.fileName = "export.csv"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="Table not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# Integration Tests via Servicer
# ============================================

class TestServicerIntegration:
    """Integration tests via StorageDriverServicer."""

    @pytest.fixture
    def servicer(self, metadata_db, project_db_manager):
        """Create servicer."""
        from src.grpc.servicer import StorageDriverServicer
        return StorageDriverServicer(metadata_db, project_db_manager)

    def test_create_bucket_via_servicer(self, servicer, phase12c_project):
        """Create bucket via servicer Execute."""
        # Create command
        cmd = bucket_pb2.CreateBucketCommand()
        cmd.projectId = phase12c_project
        cmd.bucketId = "out.c-integration-test"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        class MockContext:
            code = None
            details = None

            def set_code(self, code):
                self.code = code

            def set_details(self, details):
                self.details = details

        context = MockContext()
        response = servicer.Execute(request, context)

        # Verify success
        assert context.code is None
        assert response.commandResponse.ByteSize() > 0

        # Unpack response
        bucket_response = bucket_pb2.CreateBucketResponse()
        response.commandResponse.Unpack(bucket_response)
        assert bucket_response.createBucketObjectName == "out_c_integration_test"

    def test_create_table_via_servicer(self, servicer, phase12c_bucket):
        """Create table via servicer Execute."""
        project_id, bucket_name = phase12c_bucket

        cmd = table_pb2.CreateTableCommand()
        cmd.path.extend([project_id, bucket_name])
        cmd.tableName = "servicer_table"

        col = cmd.columns.add()
        col.name = "id"
        col.type = "INTEGER"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        class MockContext:
            code = None
            details = None
            def set_code(self, code): self.code = code
            def set_details(self, details): self.details = details

        context = MockContext()
        response = servicer.Execute(request, context)

        # CreateTableCommand returns no response, but should succeed
        assert context.code is None

    def test_object_info_via_servicer(self, servicer, phase12c_table_with_data):
        """Get object info via servicer Execute."""
        project_id, bucket_name, table_name = phase12c_table_with_data

        cmd = info_pb2.ObjectInfoCommand()
        cmd.path.extend([project_id, bucket_name, table_name])
        cmd.expectedObjectType = info_pb2.TABLE

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        class MockContext:
            code = None
            details = None
            def set_code(self, code): self.code = code
            def set_details(self, details): self.details = details

        context = MockContext()
        response = servicer.Execute(request, context)

        # Verify success
        assert context.code is None

        # Unpack response
        info_response = info_pb2.ObjectInfoResponse()
        response.commandResponse.Unpack(info_response)
        assert info_response.tableInfo.tableName == table_name
        assert info_response.tableInfo.rowsCount == 3


class TestHTTPBridgeIntegration:
    """Test HTTP bridge for Phase 12c commands.

    Uses client and admin_headers fixtures from conftest.py.
    """

    def test_list_commands_includes_phase12c(self, client):
        """Commands endpoint lists Phase 12c commands."""
        response = client.get("/driver/commands")
        assert response.status_code == 200

        data = response.json()
        command_types = [cmd["type"] for cmd in data["supported_commands"]]

        # Verify Phase 12c commands are listed
        assert "CreateBucketCommand" in command_types
        assert "DropBucketCommand" in command_types
        assert "CreateTableCommand" in command_types
        assert "DropTableCommand" in command_types
        assert "PreviewTableCommand" in command_types
        assert "ObjectInfoCommand" in command_types
        assert "TableImportFromFileCommand" in command_types
        assert "TableExportToFileCommand" in command_types

    def test_create_bucket_via_http(
        self, client, metadata_db, project_db_manager, admin_headers
    ):
        """Create bucket via HTTP bridge."""
        # First create a project
        project_id = "http-test-project"
        metadata_db.create_project(project_id, "HTTP Test", None)
        project_db_manager.create_project_db(project_id)

        # Reset the driver servicer cache to pick up new DB state
        from src.routers import driver
        driver._servicer = None

        # Create bucket via HTTP
        response = client.post(
            "/driver/execute",
            json={
                "command": {
                    "type": "CreateBucketCommand",
                    "projectId": project_id,
                    "bucketId": "in.c-http-bucket",
                }
            },
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["commandResponse"]["createBucketObjectName"] == "in_c_http_bucket"
