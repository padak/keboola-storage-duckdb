"""Tests for Phase 12f-g gRPC handlers.

Phase 12f: Bucket sharing handlers
- ShareBucketHandler
- UnshareBucketHandler
- LinkBucketHandler
- UnlinkBucketHandler
- GrantBucketAccessToReadOnlyRoleHandler
- RevokeBucketAccessFromReadOnlyRoleHandler

Phase 12g: Branch and query handlers
- CreateDevBranchHandler
- DropDevBranchHandler
- ExecuteQueryHandler
"""

import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "generated"))

from proto import common_pb2, bucket_pb2, project_pb2, executeQuery_pb2


# Use fixtures from conftest.py: metadata_db, project_db_manager, temp_data_dir


@pytest.fixture
def servicer(metadata_db, project_db_manager):
    """Create StorageDriverServicer for testing."""
    from src.grpc.servicer import StorageDriverServicer
    return StorageDriverServicer(metadata_db, project_db_manager)


class MockContext:
    """Mock gRPC context for testing."""

    def __init__(self):
        self.code = None
        self.details = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details


class TestShareBucketHandler:
    """Tests for ShareBucketHandler."""

    def test_share_bucket_success(self, servicer, metadata_db, project_db_manager):
        """Share a bucket successfully."""
        # Create project and bucket first
        metadata_db.create_project("source-proj", "Source Project")
        project_db_manager.create_bucket("source-proj", "in_c_sales")

        # Share bucket
        cmd = bucket_pb2.ShareBucketCommand()
        cmd.sourceProjectId = "source-proj"
        cmd.sourceBucketId = "in_c_sales"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        response = servicer.Execute(request, context)

        assert context.code is None
        assert response.commandResponse.ByteSize() > 0

        # Unpack response
        share_response = bucket_pb2.ShareBucketResponse()
        response.commandResponse.Unpack(share_response)
        assert "share_source-proj_in_c_sales" in share_response.bucketShareRoleName

    def test_share_bucket_not_found(self, servicer, metadata_db):
        """Share non-existent bucket fails."""
        cmd = bucket_pb2.ShareBucketCommand()
        cmd.sourceProjectId = "nonexistent-proj"
        cmd.sourceBucketId = "nonexistent_bucket"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        servicer.Execute(request, context)

        import grpc
        assert context.code == grpc.StatusCode.NOT_FOUND


class TestUnshareBucketHandler:
    """Tests for UnshareBucketHandler."""

    def test_unshare_bucket_success(self, servicer, metadata_db, project_db_manager):
        """Unshare a bucket successfully."""
        # Create project and bucket
        metadata_db.create_project("source-proj", "Source Project")
        project_db_manager.create_bucket("source-proj", "in_c_sales")

        # Prepare unshare command with credentials
        cmd = bucket_pb2.UnshareBucketCommand()
        cmd.bucketObjectName = "in_c_sales"
        cmd.bucketShareRoleName = "share_source-proj_in_c_sales"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        # Add credentials
        from proto import credentials_pb2
        creds = credentials_pb2.GenericBackendCredentials()
        creds.host = "source-proj"
        request.credentials.Pack(creds)

        context = MockContext()
        response = servicer.Execute(request, context)

        # Should succeed (no response message for unshare)
        assert context.code is None


class TestLinkBucketHandler:
    """Tests for LinkBucketHandler."""

    def test_link_bucket_success(self, servicer, metadata_db, project_db_manager):
        """Link a bucket successfully."""
        # Create source and target projects
        metadata_db.create_project("source-proj", "Source Project")
        metadata_db.create_project("target-proj", "Target Project")

        # Create source bucket with a table
        project_db_manager.create_bucket("source-proj", "in_c_sales")
        project_db_manager.create_table(
            "source-proj", "in_c_sales", "orders",
            [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "VARCHAR"}]
        )

        # Ensure target project directory exists
        project_db_manager.get_project_dir("target-proj").mkdir(parents=True, exist_ok=True)

        # Link bucket
        cmd = bucket_pb2.LinkBucketCommand()
        cmd.targetProjectId = "target-proj"
        cmd.targetBucketId = "linked_sales"
        cmd.sourceShareRoleName = "share_source-proj_in_c_sales"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        response = servicer.Execute(request, context)

        assert context.code is None
        assert response.commandResponse.ByteSize() > 0

        # Unpack response
        link_response = bucket_pb2.LinkedBucketResponse()
        response.commandResponse.Unpack(link_response)
        assert link_response.linkedBucketObjectName == "linked_sales"

    def test_link_bucket_invalid_share_role(self, servicer, metadata_db):
        """Link with invalid share role name fails."""
        metadata_db.create_project("target-proj", "Target Project")

        cmd = bucket_pb2.LinkBucketCommand()
        cmd.targetProjectId = "target-proj"
        cmd.targetBucketId = "linked_sales"
        cmd.sourceShareRoleName = "invalid_format"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        servicer.Execute(request, context)

        import grpc
        assert context.code == grpc.StatusCode.INVALID_ARGUMENT


class TestUnlinkBucketHandler:
    """Tests for UnlinkBucketHandler."""

    def test_unlink_bucket_not_linked(self, servicer, metadata_db, project_db_manager):
        """Unlink a bucket that's not linked (idempotent)."""
        metadata_db.create_project("proj", "Project")

        cmd = bucket_pb2.UnlinkBucketCommand()
        cmd.bucketObjectName = "nonexistent_bucket"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        # Add credentials
        from proto import credentials_pb2
        creds = credentials_pb2.GenericBackendCredentials()
        creds.host = "proj"
        request.credentials.Pack(creds)

        context = MockContext()
        response = servicer.Execute(request, context)

        # Should succeed (idempotent)
        assert context.code is None


class TestGrantBucketAccessHandler:
    """Tests for GrantBucketAccessToReadOnlyRoleHandler."""

    def test_grant_readonly_success(self, servicer, metadata_db, project_db_manager):
        """Grant readonly access successfully."""
        metadata_db.create_project("proj", "Project")
        project_db_manager.create_bucket("proj", "in_c_sales")

        cmd = bucket_pb2.GrantBucketAccessToReadOnlyRoleCommand()
        cmd.path.append("in_c_sales")
        cmd.projectReadOnlyRoleName = "readonly_role"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        response = servicer.Execute(request, context)

        assert context.code is None
        assert response.commandResponse.ByteSize() > 0

        # Unpack response
        grant_response = bucket_pb2.GrantBucketAccessToReadOnlyRoleResponse()
        response.commandResponse.Unpack(grant_response)
        assert "in_c_sales" in grant_response.createBucketObjectName


class TestRevokeBucketAccessHandler:
    """Tests for RevokeBucketAccessFromReadOnlyRoleHandler."""

    def test_revoke_readonly_success(self, servicer, metadata_db):
        """Revoke readonly access successfully (no-op for DuckDB)."""
        cmd = bucket_pb2.RevokeBucketAccessFromReadOnlyRoleCommand()
        cmd.bucketObjectName = "in_c_sales"
        cmd.projectReadOnlyRoleName = "readonly_role"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        response = servicer.Execute(request, context)

        # Should succeed (no-op for DuckDB)
        assert context.code is None


class TestCreateDevBranchHandler:
    """Tests for CreateDevBranchHandler."""

    def test_create_dev_branch_success(self, servicer, metadata_db, project_db_manager):
        """Create a dev branch successfully."""
        metadata_db.create_project("proj", "Project")
        project_db_manager.get_project_dir("proj").mkdir(parents=True, exist_ok=True)

        cmd = project_pb2.CreateDevBranchCommand()
        cmd.projectId = "proj"
        cmd.branchId = "feature-123"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        response = servicer.Execute(request, context)

        assert context.code is None
        assert response.commandResponse.ByteSize() > 0

        # Unpack response
        branch_response = project_pb2.CreateDevBranchResponse()
        response.commandResponse.Unpack(branch_response)
        assert "feature-123" in branch_response.devBranchReadOnlyRoleName

    def test_create_dev_branch_project_not_found(self, servicer, metadata_db):
        """Create branch for non-existent project fails."""
        cmd = project_pb2.CreateDevBranchCommand()
        cmd.projectId = "nonexistent-proj"
        cmd.branchId = "feature-123"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        servicer.Execute(request, context)

        import grpc
        assert context.code == grpc.StatusCode.NOT_FOUND


class TestDropDevBranchHandler:
    """Tests for DropDevBranchHandler."""

    def test_drop_dev_branch_success(self, servicer, metadata_db, project_db_manager):
        """Drop a dev branch successfully."""
        # Create project and branch
        metadata_db.create_project("proj", "Project")
        project_db_manager.get_project_dir("proj").mkdir(parents=True, exist_ok=True)
        project_db_manager.create_branch_db("proj", "feature-123")
        metadata_db.create_branch("feature-123", "proj", "Feature Branch")

        cmd = project_pb2.DropDevBranchCommand()
        cmd.devBranchReadOnlyRoleName = "branch_proj_feature-123_readonly"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        response = servicer.Execute(request, context)

        assert context.code is None

    def test_drop_dev_branch_not_found(self, servicer, metadata_db, project_db_manager):
        """Drop non-existent branch (idempotent)."""
        metadata_db.create_project("proj", "Project")

        cmd = project_pb2.DropDevBranchCommand()
        cmd.devBranchReadOnlyRoleName = "branch_proj_nonexistent_readonly"

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        context = MockContext()
        response = servicer.Execute(request, context)

        # Should succeed (idempotent)
        assert context.code is None


class TestExecuteQueryHandler:
    """Tests for ExecuteQueryHandler."""

    def test_execute_query_success(self, servicer, metadata_db, project_db_manager):
        """Execute a simple query successfully."""
        metadata_db.create_project("proj", "Project")

        cmd = executeQuery_pb2.ExecuteQueryCommand()
        cmd.query = "SELECT 1 AS value"
        cmd.pathRestriction.append("proj")

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        # Add credentials
        from proto import credentials_pb2
        creds = credentials_pb2.GenericBackendCredentials()
        creds.host = "proj"
        request.credentials.Pack(creds)

        context = MockContext()
        response = servicer.Execute(request, context)

        assert context.code is None
        assert response.commandResponse.ByteSize() > 0

        # Unpack response
        query_response = executeQuery_pb2.ExecuteQueryResponse()
        response.commandResponse.Unpack(query_response)
        assert query_response.status == executeQuery_pb2.ExecuteQueryResponse.Status.Success
        assert len(query_response.data.rows) == 1

    def test_execute_query_missing_query(self, servicer, metadata_db):
        """Execute without query fails."""
        metadata_db.create_project("proj", "Project")

        cmd = executeQuery_pb2.ExecuteQueryCommand()
        # No query set
        cmd.pathRestriction.append("proj")

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        from proto import credentials_pb2
        creds = credentials_pb2.GenericBackendCredentials()
        creds.host = "proj"
        request.credentials.Pack(creds)

        context = MockContext()
        servicer.Execute(request, context)

        import grpc
        assert context.code == grpc.StatusCode.INVALID_ARGUMENT

    def test_execute_query_error(self, servicer, metadata_db):
        """Execute invalid SQL returns error response."""
        metadata_db.create_project("proj", "Project")

        cmd = executeQuery_pb2.ExecuteQueryCommand()
        cmd.query = "SELECT * FROM nonexistent_table"
        cmd.pathRestriction.append("proj")

        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        from proto import credentials_pb2
        creds = credentials_pb2.GenericBackendCredentials()
        creds.host = "proj"
        request.credentials.Pack(creds)

        context = MockContext()
        response = servicer.Execute(request, context)

        # Query handler returns error in response, not via gRPC status
        assert context.code is None

        query_response = executeQuery_pb2.ExecuteQueryResponse()
        response.commandResponse.Unpack(query_response)
        assert query_response.status == executeQuery_pb2.ExecuteQueryResponse.Status.Error
