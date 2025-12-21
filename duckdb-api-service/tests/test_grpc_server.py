"""Tests for gRPC server and handlers."""

import pytest
import grpc
import sys
from pathlib import Path
from concurrent import futures

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "generated"))
sys.path.insert(0, str(Path(__file__).parent.parent))

from proto import service_pb2_grpc, common_pb2, backend_pb2, project_pb2
from src.grpc.servicer import StorageDriverServicer
from src.grpc.handlers import InitBackendHandler, RemoveBackendHandler
from src.grpc.handlers import CreateProjectHandler, DropProjectHandler
from src.grpc.utils import LogMessageCollector, get_type_name
from src.database import MetadataDB, ProjectDBManager


class TestLogMessageCollector:
    """Tests for LogMessageCollector utility."""

    def test_empty_collector(self):
        """Empty collector has no messages."""
        collector = LogMessageCollector()
        assert len(collector) == 0
        assert collector.get_messages() == []

    def test_info_message(self):
        """Info messages are collected."""
        collector = LogMessageCollector()
        collector.info("Test message")

        messages = collector.get_messages()
        assert len(messages) == 1
        assert messages[0].message == "Test message"
        assert messages[0].level == common_pb2.LogMessage.Level.Informational

    def test_multiple_messages(self):
        """Multiple messages of different levels."""
        collector = LogMessageCollector()
        collector.info("Info message")
        collector.warning("Warning message")
        collector.error("Error message")
        collector.debug("Debug message")

        messages = collector.get_messages()
        assert len(messages) == 4
        assert messages[0].level == common_pb2.LogMessage.Level.Informational
        assert messages[1].level == common_pb2.LogMessage.Level.Warning
        assert messages[2].level == common_pb2.LogMessage.Level.Error
        assert messages[3].level == common_pb2.LogMessage.Level.Debug

    def test_clear_messages(self):
        """Clear removes all messages."""
        collector = LogMessageCollector()
        collector.info("Test")
        assert len(collector) == 1

        collector.clear()
        assert len(collector) == 0


class TestGetTypeName:
    """Tests for get_type_name utility."""

    def test_extract_type_name(self):
        """Extracts type name from Any message."""
        from google.protobuf import any_pb2

        any_msg = any_pb2.Any()
        cmd = backend_pb2.InitBackendCommand()
        any_msg.Pack(cmd)

        type_name = get_type_name(any_msg)
        assert type_name == "InitBackendCommand"

    def test_project_command_type(self):
        """Extracts CreateProjectCommand type."""
        from google.protobuf import any_pb2

        any_msg = any_pb2.Any()
        cmd = project_pb2.CreateProjectCommand()
        cmd.projectId = "123"
        any_msg.Pack(cmd)

        type_name = get_type_name(any_msg)
        assert type_name == "CreateProjectCommand"


class TestInitBackendHandler:
    """Tests for InitBackendHandler."""

    def test_init_backend_success(self, metadata_db, project_db_manager):
        """InitBackendCommand initializes metadata DB."""
        from google.protobuf import any_pb2

        handler = InitBackendHandler(metadata_db)

        cmd = backend_pb2.InitBackendCommand()
        any_cmd = any_pb2.Any()
        any_cmd.Pack(cmd)

        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        assert isinstance(response, backend_pb2.InitBackendResponse)
        messages = handler.get_log_messages()
        assert len(messages) >= 1
        assert "initialized" in messages[0].message.lower()


class TestRemoveBackendHandler:
    """Tests for RemoveBackendHandler."""

    def test_remove_backend(self, metadata_db, project_db_manager):
        """RemoveBackendCommand completes without error."""
        from google.protobuf import any_pb2

        handler = RemoveBackendHandler(metadata_db)

        cmd = backend_pb2.RemoveBackendCommand()
        any_cmd = any_pb2.Any()
        any_cmd.Pack(cmd)

        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        # RemoveBackend has no response
        assert response is None
        messages = handler.get_log_messages()
        assert len(messages) >= 1


class TestCreateProjectHandler:
    """Tests for CreateProjectHandler."""

    def test_create_project_success(self, metadata_db, project_db_manager):
        """CreateProjectCommand creates project and returns API key."""
        from google.protobuf import any_pb2

        handler = CreateProjectHandler(metadata_db, project_db_manager)

        cmd = project_pb2.CreateProjectCommand()
        cmd.projectId = "test-grpc-123"
        any_cmd = any_pb2.Any()
        any_cmd.Pack(cmd)

        response = handler.handle(any_cmd, None, common_pb2.RuntimeOptions())

        assert isinstance(response, project_pb2.CreateProjectResponse)
        assert response.projectDatabaseName == "test-grpc-123"
        assert response.projectPassword.startswith("proj_test-grpc-123_admin_")
        assert response.projectUserName == "project_test-grpc-123"

    def test_create_project_duplicate(self, metadata_db, project_db_manager):
        """CreateProjectCommand fails for duplicate project."""
        from google.protobuf import any_pb2

        handler = CreateProjectHandler(metadata_db, project_db_manager)

        # Create first project
        cmd1 = project_pb2.CreateProjectCommand()
        cmd1.projectId = "test-dup-456"
        any_cmd1 = any_pb2.Any()
        any_cmd1.Pack(cmd1)
        handler.handle(any_cmd1, None, common_pb2.RuntimeOptions())

        # Try to create duplicate
        cmd2 = project_pb2.CreateProjectCommand()
        cmd2.projectId = "test-dup-456"
        any_cmd2 = any_pb2.Any()
        any_cmd2.Pack(cmd2)

        # Create new handler to clear log messages
        handler2 = CreateProjectHandler(metadata_db, project_db_manager)

        with pytest.raises(ValueError, match="already exists"):
            handler2.handle(any_cmd2, None, common_pb2.RuntimeOptions())


class TestDropProjectHandler:
    """Tests for DropProjectHandler."""

    def test_drop_project_success(self, metadata_db, project_db_manager):
        """DropProjectCommand deletes project."""
        from google.protobuf import any_pb2

        # First create a project
        create_handler = CreateProjectHandler(metadata_db, project_db_manager)
        create_cmd = project_pb2.CreateProjectCommand()
        create_cmd.projectId = "test-drop-789"
        any_create = any_pb2.Any()
        any_create.Pack(create_cmd)
        create_handler.handle(any_create, None, common_pb2.RuntimeOptions())

        # Now drop it
        drop_handler = DropProjectHandler(metadata_db, project_db_manager)
        drop_cmd = project_pb2.DropProjectCommand()
        drop_cmd.projectDatabaseName = "test-drop-789"
        any_drop = any_pb2.Any()
        any_drop.Pack(drop_cmd)

        response = drop_handler.handle(any_drop, None, common_pb2.RuntimeOptions())

        # DropProject has no response
        assert response is None

        # Verify project is deleted
        project = metadata_db.get_project("test-drop-789")
        assert project is None or project["status"] == "deleted"

    def test_drop_nonexistent_project(self, metadata_db, project_db_manager):
        """DropProjectCommand fails for nonexistent project."""
        from google.protobuf import any_pb2

        handler = DropProjectHandler(metadata_db, project_db_manager)
        cmd = project_pb2.DropProjectCommand()
        cmd.projectDatabaseName = "nonexistent-999"
        any_cmd = any_pb2.Any()
        any_cmd.Pack(cmd)

        with pytest.raises(KeyError, match="not found"):
            handler.handle(any_cmd, None, common_pb2.RuntimeOptions())


class TestStorageDriverServicer:
    """Tests for StorageDriverServicer gRPC service."""

    @pytest.fixture
    def servicer(self, metadata_db, project_db_manager):
        """Create servicer instance."""
        return StorageDriverServicer(metadata_db, project_db_manager)

    def test_execute_init_backend(self, servicer):
        """Execute InitBackendCommand via servicer."""
        from google.protobuf import any_pb2

        cmd = backend_pb2.InitBackendCommand()
        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        # Create mock context
        class MockContext:
            def set_code(self, code): pass
            def set_details(self, details): pass

        response = servicer.Execute(request, MockContext())

        assert isinstance(response, common_pb2.DriverResponse)
        assert len(response.messages) >= 1

    def test_execute_create_project(self, servicer):
        """Execute CreateProjectCommand via servicer."""
        from google.protobuf import any_pb2

        cmd = project_pb2.CreateProjectCommand()
        cmd.projectId = "servicer-test-123"
        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        class MockContext:
            def set_code(self, code): pass
            def set_details(self, details): pass

        response = servicer.Execute(request, MockContext())

        assert isinstance(response, common_pb2.DriverResponse)
        assert response.commandResponse.ByteSize() > 0

        # Unpack and verify response
        project_response = project_pb2.CreateProjectResponse()
        response.commandResponse.Unpack(project_response)

        assert project_response.projectDatabaseName == "servicer-test-123"
        assert project_response.projectPassword.startswith("proj_")

    def test_execute_unsupported_command(self, servicer):
        """Unsupported command returns UNIMPLEMENTED."""
        from google.protobuf import any_pb2
        from proto import bucket_pb2

        # Use a bucket command that's not implemented yet (LinkBucketCommand)
        cmd = bucket_pb2.LinkBucketCommand()
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

        assert context.code == grpc.StatusCode.UNIMPLEMENTED
        assert "Unsupported command" in context.details


class TestGRPCServerIntegration:
    """Integration tests for gRPC server."""

    @pytest.fixture
    def grpc_server(self, metadata_db, project_db_manager):
        """Start gRPC server for testing."""
        from src.grpc.server import create_server

        server = create_server(
            metadata_db, project_db_manager,
            host="localhost", port=50052, max_workers=2
        )
        server.start()
        yield server
        server.stop(grace=0)

    @pytest.fixture
    def stub(self, grpc_server):
        """Create service stub."""
        channel = grpc.insecure_channel('localhost:50052')
        yield service_pb2_grpc.StorageDriverServiceStub(channel)
        channel.close()

    def test_init_backend_via_grpc(self, stub):
        """Test InitBackend via actual gRPC call."""
        cmd = backend_pb2.InitBackendCommand()
        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        response = stub.Execute(request)

        assert response is not None
        assert any(
            msg.level == common_pb2.LogMessage.Level.Informational
            for msg in response.messages
        )

    def test_create_project_via_grpc(self, stub):
        """Test CreateProject via actual gRPC call."""
        cmd = project_pb2.CreateProjectCommand()
        cmd.projectId = "grpc-int-test-123"
        request = common_pb2.DriverRequest()
        request.command.Pack(cmd)

        response = stub.Execute(request)

        assert response is not None
        assert response.commandResponse.ByteSize() > 0

        # Unpack response
        project_response = project_pb2.CreateProjectResponse()
        response.commandResponse.Unpack(project_response)

        assert project_response.projectDatabaseName == "grpc-int-test-123"
        assert project_response.projectPassword  # API key returned
