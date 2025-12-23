"""Tests for Phase 12e gRPC workspace handlers.

Tests cover:
- CreateWorkspaceHandler
- DropWorkspaceHandler
- ClearWorkspaceHandler
- ResetWorkspacePasswordHandler
- DropWorkspaceObjectHandler
- GrantWorkspaceAccessToProjectHandler
- RevokeWorkspaceAccessToProjectHandler
- LoadTableToWorkspaceHandler

Note: Uses fixtures from conftest.py (metadata_db, project_db_manager, temp_data_dir)
"""

import pytest
import sys
from pathlib import Path

# Add generated proto to path
sys.path.insert(0, str(Path(__file__).parent.parent / "generated"))

from proto import common_pb2, workspace_pb2


# ============================================
# Fixtures
# ============================================


@pytest.fixture
def phase12e_project(metadata_db, project_db_manager) -> str:
    """Create a test project for Phase 12e and return its ID."""
    project_id = "test-project-12e"

    # Register in metadata
    metadata_db.create_project(project_id, "Test Project 12e", None)

    # Create project directory structure
    project_db_manager.create_project_db(project_id)

    return project_id


@pytest.fixture
def phase12e_bucket(phase12e_project, project_db_manager) -> tuple[str, str]:
    """Create a test bucket and return (project_id, bucket_name)."""
    project_id = phase12e_project
    bucket_name = "phase12e_bucket"

    project_db_manager.create_bucket(project_id, bucket_name)

    return project_id, bucket_name


@pytest.fixture
def phase12e_table_with_data(phase12e_bucket, project_db_manager) -> tuple[str, str, str]:
    """Create a test table with sample data."""
    import duckdb
    from src.database import TABLE_DATA_NAME

    project_id, bucket_name = phase12e_bucket
    table_name = "phase12e_table"

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


@pytest.fixture
def phase12e_workspace(phase12e_project, metadata_db, project_db_manager) -> tuple[str, str, str]:
    """Create a test workspace and return (project_id, workspace_id, username)."""
    import hashlib
    import secrets

    project_id = phase12e_project
    workspace_id = "ws_test_12e"
    username = f"ws_{workspace_id}_{secrets.token_hex(4)}"
    password = secrets.token_urlsafe(24)[:32]
    password_hash = hashlib.sha256(password.encode()).hexdigest()

    # Create workspace database file
    db_path = project_db_manager.create_workspace_db(project_id, workspace_id)

    # Create workspace metadata
    metadata_db.create_workspace(
        workspace_id=workspace_id,
        project_id=project_id,
        name=f"workspace_{workspace_id}",
        db_path=str(db_path),
        branch_id=None,
        expires_at=None,
        size_limit_bytes=10 * 1024 * 1024 * 1024,
    )

    # Create credentials
    metadata_db.create_workspace_credentials(workspace_id, username, password_hash)

    return project_id, workspace_id, username


# ============================================
# CreateWorkspaceHandler Tests
# ============================================


class TestCreateWorkspaceHandler:
    """Tests for CreateWorkspaceHandler."""

    def test_create_workspace_success(self, phase12e_project, metadata_db, project_db_manager):
        """Create workspace successfully."""
        from src.grpc.handlers.workspace import CreateWorkspaceHandler

        handler = CreateWorkspaceHandler(project_db_manager, metadata_db)

        # Create command
        cmd = workspace_pb2.CreateWorkspaceCommand()
        cmd.projectId = phase12e_project
        cmd.workspaceId = "ws_new_test"
        cmd.isBranchDefault = True

        # Pack into Any
        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # Verify response
        assert response is not None
        assert response.workspaceObjectName == "ws_new_test"
        assert response.workspacePassword  # Password is returned
        assert response.workspaceUserName.startswith("ws_")
        assert response.workspaceRoleName  # Role name is set

        # Verify workspace was created
        workspace = metadata_db.get_workspace("ws_new_test")
        assert workspace is not None
        assert workspace["project_id"] == phase12e_project

        # Verify workspace file exists
        assert project_db_manager.workspace_exists(phase12e_project, "ws_new_test")

    def test_create_workspace_missing_project_id(self, metadata_db, project_db_manager):
        """Create workspace fails without project ID."""
        from src.grpc.handlers.workspace import CreateWorkspaceHandler

        handler = CreateWorkspaceHandler(project_db_manager, metadata_db)

        cmd = workspace_pb2.CreateWorkspaceCommand()
        cmd.workspaceId = "ws_test"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(ValueError, match="projectId is required"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

    def test_create_workspace_missing_workspace_id(self, phase12e_project, metadata_db, project_db_manager):
        """Create workspace fails without workspace ID."""
        from src.grpc.handlers.workspace import CreateWorkspaceHandler

        handler = CreateWorkspaceHandler(project_db_manager, metadata_db)

        cmd = workspace_pb2.CreateWorkspaceCommand()
        cmd.projectId = phase12e_project

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(ValueError, match="workspaceId is required"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# DropWorkspaceHandler Tests
# ============================================


class TestDropWorkspaceHandler:
    """Tests for DropWorkspaceHandler."""

    def test_drop_workspace_success(self, phase12e_workspace, metadata_db, project_db_manager):
        """Drop workspace successfully."""
        from src.grpc.handlers.workspace import DropWorkspaceHandler

        project_id, workspace_id, _ = phase12e_workspace
        handler = DropWorkspaceHandler(project_db_manager, metadata_db)

        # Verify workspace exists first
        assert metadata_db.get_workspace(workspace_id) is not None

        # Create command
        cmd = workspace_pb2.DropWorkspaceCommand()
        cmd.workspaceObjectName = workspace_id

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # Verify - no response for drop
        assert response is None

        # Verify workspace is deleted
        assert metadata_db.get_workspace(workspace_id) is None

    def test_drop_workspace_not_found_is_idempotent(self, metadata_db, project_db_manager):
        """Drop non-existent workspace is idempotent (no error)."""
        from src.grpc.handlers.workspace import DropWorkspaceHandler

        handler = DropWorkspaceHandler(project_db_manager, metadata_db)

        cmd = workspace_pb2.DropWorkspaceCommand()
        cmd.workspaceObjectName = "non_existent_workspace"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Should not raise
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None


# ============================================
# ClearWorkspaceHandler Tests
# ============================================


class TestClearWorkspaceHandler:
    """Tests for ClearWorkspaceHandler."""

    def test_clear_workspace_success(self, phase12e_workspace, metadata_db, project_db_manager):
        """Clear workspace successfully."""
        import duckdb
        from src.grpc.handlers.workspace import ClearWorkspaceHandler

        project_id, workspace_id, _ = phase12e_workspace
        handler = ClearWorkspaceHandler(project_db_manager, metadata_db)

        # Create some objects in workspace first
        workspace_path = project_db_manager.get_workspace_path(project_id, workspace_id)
        conn = duckdb.connect(str(workspace_path))
        try:
            conn.execute("CREATE TABLE test_table (id INTEGER)")
            conn.execute("INSERT INTO test_table VALUES (1), (2), (3)")
            conn.commit()
        finally:
            conn.close()

        # Verify table exists
        objects = project_db_manager.list_workspace_objects(project_id, workspace_id)
        assert any(obj["name"] == "test_table" for obj in objects)

        # Create command
        cmd = workspace_pb2.ClearWorkspaceCommand()
        cmd.workspaceObjectName = workspace_id

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None

        # Verify workspace is cleared
        objects = project_db_manager.list_workspace_objects(project_id, workspace_id)
        assert len(objects) == 0

    def test_clear_workspace_not_found(self, metadata_db, project_db_manager):
        """Clear non-existent workspace raises error unless ignoreErrors is set."""
        from src.grpc.handlers.workspace import ClearWorkspaceHandler

        handler = ClearWorkspaceHandler(project_db_manager, metadata_db)

        cmd = workspace_pb2.ClearWorkspaceCommand()
        cmd.workspaceObjectName = "non_existent"
        cmd.ignoreErrors = False

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

    def test_clear_workspace_not_found_ignore_errors(self, metadata_db, project_db_manager):
        """Clear non-existent workspace with ignoreErrors does not raise."""
        from src.grpc.handlers.workspace import ClearWorkspaceHandler

        handler = ClearWorkspaceHandler(project_db_manager, metadata_db)

        cmd = workspace_pb2.ClearWorkspaceCommand()
        cmd.workspaceObjectName = "non_existent"
        cmd.ignoreErrors = True

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Should not raise
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None


# ============================================
# ResetWorkspacePasswordHandler Tests
# ============================================


class TestResetWorkspacePasswordHandler:
    """Tests for ResetWorkspacePasswordHandler."""

    def test_reset_password_success(self, phase12e_workspace, metadata_db):
        """Reset workspace password successfully."""
        from src.grpc.handlers.workspace import ResetWorkspacePasswordHandler

        _, workspace_id, username = phase12e_workspace
        handler = ResetWorkspacePasswordHandler(metadata_db)

        # Get old password hash
        old_creds = metadata_db.get_workspace_credentials(workspace_id)
        old_hash = old_creds["password_hash"]

        # Create command
        cmd = workspace_pb2.ResetWorkspacePasswordCommand()
        cmd.workspaceUserName = username

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # Verify response
        assert response is not None
        assert response.workspaceUserName == username
        assert response.workspacePassword  # New password returned

        # Verify password was changed
        new_creds = metadata_db.get_workspace_credentials(workspace_id)
        assert new_creds["password_hash"] != old_hash

    def test_reset_password_user_not_found(self, metadata_db):
        """Reset password fails for non-existent user."""
        from src.grpc.handlers.workspace import ResetWorkspacePasswordHandler

        handler = ResetWorkspacePasswordHandler(metadata_db)

        cmd = workspace_pb2.ResetWorkspacePasswordCommand()
        cmd.workspaceUserName = "non_existent_user"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


# ============================================
# DropWorkspaceObjectHandler Tests
# ============================================


class TestDropWorkspaceObjectHandler:
    """Tests for DropWorkspaceObjectHandler."""

    def test_drop_workspace_object_success(self, phase12e_workspace, metadata_db, project_db_manager):
        """Drop workspace object successfully."""
        import duckdb
        from src.grpc.handlers.workspace import DropWorkspaceObjectHandler

        project_id, workspace_id, _ = phase12e_workspace
        handler = DropWorkspaceObjectHandler(project_db_manager, metadata_db)

        # Create an object in workspace
        workspace_path = project_db_manager.get_workspace_path(project_id, workspace_id)
        conn = duckdb.connect(str(workspace_path))
        try:
            conn.execute("CREATE TABLE drop_me (id INTEGER)")
            conn.commit()
        finally:
            conn.close()

        # Verify object exists
        objects = project_db_manager.list_workspace_objects(project_id, workspace_id)
        assert any(obj["name"] == "drop_me" for obj in objects)

        # Create command
        cmd = workspace_pb2.DropWorkspaceObjectCommand()
        cmd.workspaceObjectName = workspace_id
        cmd.objectNameToDrop = "drop_me"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None

        # Verify object is dropped
        objects = project_db_manager.list_workspace_objects(project_id, workspace_id)
        assert not any(obj["name"] == "drop_me" for obj in objects)

    def test_drop_workspace_object_ignore_not_exists(self, phase12e_workspace, metadata_db, project_db_manager):
        """Drop non-existent object with ignoreIfNotExists does not raise."""
        from src.grpc.handlers.workspace import DropWorkspaceObjectHandler

        _, workspace_id, _ = phase12e_workspace
        handler = DropWorkspaceObjectHandler(project_db_manager, metadata_db)

        cmd = workspace_pb2.DropWorkspaceObjectCommand()
        cmd.workspaceObjectName = workspace_id
        cmd.objectNameToDrop = "non_existent_object"
        cmd.ignoreIfNotExists = True

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Should not raise
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None


# ============================================
# GrantWorkspaceAccessToProjectHandler Tests
# ============================================


class TestGrantWorkspaceAccessToProjectHandler:
    """Tests for GrantWorkspaceAccessToProjectHandler."""

    def test_grant_access_success(self, phase12e_workspace, metadata_db):
        """Grant workspace access (logs operation for DuckDB)."""
        from src.grpc.handlers.workspace import GrantWorkspaceAccessToProjectHandler

        _, workspace_id, _ = phase12e_workspace
        handler = GrantWorkspaceAccessToProjectHandler(metadata_db)

        cmd = workspace_pb2.GrantWorkspaceAccessToProjectCommand()
        cmd.workspaceObjectName = workspace_id
        cmd.projectUserName = "other_project_user"
        cmd.projectRoleName = "other_project_role"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute - this is a no-op for DuckDB but should not raise
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None

        # Verify log message was created
        logs = handler.get_log_messages()
        assert any("Grant workspace access recorded" in msg.message for msg in logs)


# ============================================
# RevokeWorkspaceAccessToProjectHandler Tests
# ============================================


class TestRevokeWorkspaceAccessToProjectHandler:
    """Tests for RevokeWorkspaceAccessToProjectHandler."""

    def test_revoke_access_success(self, phase12e_workspace, metadata_db):
        """Revoke workspace access (logs operation for DuckDB)."""
        from src.grpc.handlers.workspace import RevokeWorkspaceAccessToProjectHandler

        _, workspace_id, _ = phase12e_workspace
        handler = RevokeWorkspaceAccessToProjectHandler(metadata_db)

        cmd = workspace_pb2.RevokeWorkspaceAccessToProjectCommand()
        cmd.workspaceObjectName = workspace_id
        cmd.projectUserName = "other_project_user"
        cmd.projectRoleName = "other_project_role"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute - this is a no-op for DuckDB but should not raise
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None

        # Verify log message was created
        logs = handler.get_log_messages()
        assert any("Revoke workspace access recorded" in msg.message for msg in logs)


# ============================================
# LoadTableToWorkspaceHandler Tests
# ============================================


class TestLoadTableToWorkspaceHandler:
    """Tests for LoadTableToWorkspaceHandler."""

    def test_load_table_success(
        self, phase12e_table_with_data, phase12e_workspace, metadata_db, project_db_manager
    ):
        """Load table to workspace successfully."""
        import duckdb
        from src.grpc.handlers.workspace import LoadTableToWorkspaceHandler

        project_id, bucket_name, table_name = phase12e_table_with_data
        _, workspace_id, _ = phase12e_workspace

        handler = LoadTableToWorkspaceHandler(project_db_manager, metadata_db)

        # Create command
        cmd = workspace_pb2.LoadTableToWorkspaceCommand()

        # Set source
        cmd.source.path.extend([project_id, bucket_name])
        cmd.source.tableName = table_name

        # Set destination
        cmd.destination.path.extend([workspace_id])
        cmd.destination.tableName = "loaded_table"

        any_cmd = common_pb2.DriverRequest().command
        any_cmd.Pack(cmd)

        # Execute
        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())
        assert response is None

        # Verify table was loaded to workspace
        workspace_path = project_db_manager.get_workspace_path(project_id, workspace_id)
        conn = duckdb.connect(str(workspace_path))
        try:
            count = conn.execute("SELECT COUNT(*) FROM loaded_table").fetchone()[0]
            assert count == 3  # Our test data has 3 rows
        finally:
            conn.close()

        # Verify log message
        logs = handler.get_log_messages()
        assert any("Loaded" in msg.message and "loaded_table" in msg.message for msg in logs)


# ============================================
# Servicer Integration Tests
# ============================================


class TestServicerWorkspaceCommands:
    """Test workspace commands through the servicer."""

    def test_servicer_has_workspace_handlers(self, metadata_db, project_db_manager):
        """Verify servicer has all workspace handlers registered."""
        from src.grpc.servicer import StorageDriverServicer

        servicer = StorageDriverServicer(metadata_db, project_db_manager)

        # Check all workspace handlers are registered
        expected_commands = [
            'CreateWorkspaceCommand',
            'DropWorkspaceCommand',
            'ClearWorkspaceCommand',
            'ResetWorkspacePasswordCommand',
            'DropWorkspaceObjectCommand',
            'GrantWorkspaceAccessToProjectCommand',
            'RevokeWorkspaceAccessToProjectCommand',
            'LoadTableToWorkspaceCommand',
        ]

        for cmd in expected_commands:
            assert cmd in servicer._handlers, f"Handler for {cmd} not registered"

    def test_servicer_create_workspace_via_execute(self, phase12e_project, metadata_db, project_db_manager):
        """Test CreateWorkspaceCommand through servicer.Execute()."""
        from unittest.mock import MagicMock
        from src.grpc.servicer import StorageDriverServicer

        servicer = StorageDriverServicer(metadata_db, project_db_manager)

        # Create request
        request = common_pb2.DriverRequest()
        cmd = workspace_pb2.CreateWorkspaceCommand()
        cmd.projectId = phase12e_project
        cmd.workspaceId = "ws_via_servicer"
        cmd.isBranchDefault = True
        request.command.Pack(cmd)

        # Mock context
        context = MagicMock()

        # Execute
        response = servicer.Execute(request, context)

        # Verify response
        assert response is not None
        assert response.commandResponse.ByteSize() > 0

        # Unpack and verify
        ws_response = workspace_pb2.CreateWorkspaceResponse()
        response.commandResponse.Unpack(ws_response)
        assert ws_response.workspaceObjectName == "ws_via_servicer"
        assert ws_response.workspacePassword  # Password returned
