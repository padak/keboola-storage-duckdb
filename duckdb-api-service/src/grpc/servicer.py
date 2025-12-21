"""StorageDriverServicer - main gRPC service implementation."""

import logging
import time
from typing import Optional

import grpc

from src import metrics

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "generated"))

from proto import service_pb2_grpc, common_pb2, backend_pb2, project_pb2, bucket_pb2, table_pb2, info_pb2, workspace_pb2
from src.grpc.utils import get_type_name
from src.grpc.handlers import (
    # Backend handlers
    InitBackendHandler,
    RemoveBackendHandler,
    # Project handlers
    CreateProjectHandler,
    DropProjectHandler,
    # Bucket handlers (Phase 12c)
    CreateBucketHandler,
    DropBucketHandler,
    # Table handlers (Phase 12c)
    CreateTableHandler,
    DropTableHandler,
    PreviewTableHandler,
    # Info handlers (Phase 12c)
    ObjectInfoHandler,
    # Import/Export handlers (Phase 12c)
    TableImportFromFileHandler,
    TableExportToFileHandler,
    # Schema handlers (Phase 12d)
    AddColumnHandler,
    DropColumnHandler,
    AlterColumnHandler,
    AddPrimaryKeyHandler,
    DropPrimaryKeyHandler,
    DeleteTableRowsHandler,
    # Workspace handlers (Phase 12e)
    CreateWorkspaceHandler,
    DropWorkspaceHandler,
    ClearWorkspaceHandler,
    ResetWorkspacePasswordHandler,
    DropWorkspaceObjectHandler,
    GrantWorkspaceAccessToProjectHandler,
    RevokeWorkspaceAccessToProjectHandler,
    LoadTableToWorkspaceHandler,
)
from src.database import MetadataDB, ProjectDBManager

logger = logging.getLogger(__name__)


class StorageDriverServicer(service_pb2_grpc.StorageDriverServiceServicer):
    """
    gRPC Servicer implementing StorageDriverService.

    Routes incoming DriverRequest to appropriate command handlers
    based on the command type packed in the Any field.

    This is the Python equivalent of the PHP DriverClientWrapper.
    """

    def __init__(self, metadata_db: MetadataDB, project_manager: ProjectDBManager):
        self.metadata_db = metadata_db
        self.project_manager = project_manager
        self._handlers = self._register_handlers()
        logger.info("StorageDriverServicer initialized with %d handlers", len(self._handlers))

    def _register_handlers(self) -> dict:
        """
        Register command type -> handler mappings.

        Returns dict mapping command type name to (handler_instance, command_class) tuple.
        """
        return {
            # Backend handlers
            'InitBackendCommand': (
                InitBackendHandler(self.metadata_db),
                backend_pb2.InitBackendCommand
            ),
            'RemoveBackendCommand': (
                RemoveBackendHandler(self.metadata_db),
                backend_pb2.RemoveBackendCommand
            ),
            # Project handlers
            'CreateProjectCommand': (
                CreateProjectHandler(self.metadata_db, self.project_manager),
                project_pb2.CreateProjectCommand
            ),
            'DropProjectCommand': (
                DropProjectHandler(self.metadata_db, self.project_manager),
                project_pb2.DropProjectCommand
            ),
            # Bucket handlers (Phase 12c)
            'CreateBucketCommand': (
                CreateBucketHandler(self.project_manager),
                bucket_pb2.CreateBucketCommand
            ),
            'DropBucketCommand': (
                DropBucketHandler(self.project_manager),
                bucket_pb2.DropBucketCommand
            ),
            # Table handlers (Phase 12c)
            'CreateTableCommand': (
                CreateTableHandler(self.project_manager),
                table_pb2.CreateTableCommand
            ),
            'DropTableCommand': (
                DropTableHandler(self.project_manager),
                table_pb2.DropTableCommand
            ),
            'PreviewTableCommand': (
                PreviewTableHandler(self.project_manager),
                table_pb2.PreviewTableCommand
            ),
            # Info handlers (Phase 12c)
            'ObjectInfoCommand': (
                ObjectInfoHandler(self.project_manager),
                info_pb2.ObjectInfoCommand
            ),
            # Import/Export handlers (Phase 12c)
            'TableImportFromFileCommand': (
                TableImportFromFileHandler(self.project_manager),
                table_pb2.TableImportFromFileCommand
            ),
            'TableExportToFileCommand': (
                TableExportToFileHandler(self.project_manager),
                table_pb2.TableExportToFileCommand
            ),
            # Schema handlers (Phase 12d)
            'AddColumnCommand': (
                AddColumnHandler(self.project_manager),
                table_pb2.AddColumnCommand
            ),
            'DropColumnCommand': (
                DropColumnHandler(self.project_manager),
                table_pb2.DropColumnCommand
            ),
            'AlterColumnCommand': (
                AlterColumnHandler(self.project_manager),
                table_pb2.AlterColumnCommand
            ),
            'AddPrimaryKeyCommand': (
                AddPrimaryKeyHandler(self.project_manager),
                table_pb2.AddPrimaryKeyCommand
            ),
            'DropPrimaryKeyCommand': (
                DropPrimaryKeyHandler(self.project_manager),
                table_pb2.DropPrimaryKeyCommand
            ),
            'DeleteTableRowsCommand': (
                DeleteTableRowsHandler(self.project_manager),
                table_pb2.DeleteTableRowsCommand
            ),
            # Workspace handlers (Phase 12e)
            'CreateWorkspaceCommand': (
                CreateWorkspaceHandler(self.project_manager, self.metadata_db),
                workspace_pb2.CreateWorkspaceCommand
            ),
            'DropWorkspaceCommand': (
                DropWorkspaceHandler(self.project_manager, self.metadata_db),
                workspace_pb2.DropWorkspaceCommand
            ),
            'ClearWorkspaceCommand': (
                ClearWorkspaceHandler(self.project_manager, self.metadata_db),
                workspace_pb2.ClearWorkspaceCommand
            ),
            'ResetWorkspacePasswordCommand': (
                ResetWorkspacePasswordHandler(self.metadata_db),
                workspace_pb2.ResetWorkspacePasswordCommand
            ),
            'DropWorkspaceObjectCommand': (
                DropWorkspaceObjectHandler(self.project_manager, self.metadata_db),
                workspace_pb2.DropWorkspaceObjectCommand
            ),
            'GrantWorkspaceAccessToProjectCommand': (
                GrantWorkspaceAccessToProjectHandler(self.metadata_db),
                workspace_pb2.GrantWorkspaceAccessToProjectCommand
            ),
            'RevokeWorkspaceAccessToProjectCommand': (
                RevokeWorkspaceAccessToProjectHandler(self.metadata_db),
                workspace_pb2.RevokeWorkspaceAccessToProjectCommand
            ),
            'LoadTableToWorkspaceCommand': (
                LoadTableToWorkspaceHandler(self.project_manager, self.metadata_db),
                workspace_pb2.LoadTableToWorkspaceCommand
            ),
        }

    def Execute(
        self,
        request: common_pb2.DriverRequest,
        context: grpc.ServicerContext
    ) -> common_pb2.DriverResponse:
        """
        Execute a storage driver command.

        This is the main RPC method that handles all commands.
        It uses the google.protobuf.Any wrapper pattern to support
        different command types through a single interface.
        """
        start_time = time.time()
        command_type = "unknown"
        status = "success"
        error_type = None

        try:
            # Get command type from Any field
            command_type = get_type_name(request.command)
            logger.info("Received command: %s", command_type)

            # Log runtime info if present
            if request.runtimeOptions and request.runtimeOptions.runId:
                logger.debug("RunID: %s", request.runtimeOptions.runId)

            # Find handler for this command type
            handler_info = self._handlers.get(command_type)
            if not handler_info:
                error_msg = f"Unsupported command: {command_type}"
                logger.error(error_msg)
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error_msg)
                status = "error"
                error_type = "unimplemented"
                return self._error_response(error_msg)

            handler, command_class = handler_info

            # Extract credentials if present
            credentials = None
            if request.credentials and request.credentials.ByteSize() > 0:
                from src.grpc.handlers.base import BaseCommandHandler
                credentials = BaseCommandHandler.extract_credentials(request.credentials)

            # Execute handler
            try:
                response_msg = handler.handle(
                    request.command,
                    credentials,
                    request.runtimeOptions
                )
                return self._wrap_response(response_msg, handler.get_log_messages())

            except ValueError as e:
                # Invalid parameters
                logger.error("Invalid parameters: %s", e)
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(e))
                status = "error"
                error_type = "invalid_argument"
                return self._error_response(str(e), handler.get_log_messages())

            except KeyError as e:
                # Resource not found
                logger.error("Resource not found: %s", e)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(str(e))
                status = "error"
                error_type = "not_found"
                return self._error_response(str(e), handler.get_log_messages())

            except Exception as e:
                # Internal error
                logger.exception("Internal error in %s", command_type)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                status = "error"
                error_type = "internal"
                return self._error_response(str(e), handler.get_log_messages())

        except Exception as e:
            # Error during routing or unpacking
            logger.exception("Error in Execute()")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            status = "error"
            error_type = "internal"
            return self._error_response(str(e))

        finally:
            # Record metrics
            duration = time.time() - start_time
            metrics.GRPC_REQUESTS_TOTAL.labels(command=command_type, status=status).inc()
            metrics.GRPC_REQUEST_DURATION.labels(command=command_type).observe(duration)
            if error_type:
                metrics.GRPC_ERRORS_TOTAL.labels(command=command_type, error_type=error_type).inc()

    def _wrap_response(
        self,
        command_response: Optional[object],
        log_messages: list
    ) -> common_pb2.DriverResponse:
        """Wrap command response in DriverResponse."""
        driver_response = common_pb2.DriverResponse()

        if command_response is not None:
            driver_response.commandResponse.Pack(command_response)

        driver_response.messages.extend(log_messages)
        return driver_response

    def _error_response(
        self,
        error_message: str,
        log_messages: Optional[list] = None
    ) -> common_pb2.DriverResponse:
        """Create error response."""
        driver_response = common_pb2.DriverResponse()

        if log_messages:
            driver_response.messages.extend(log_messages)

        # Add error log message
        error_log = common_pb2.LogMessage()
        error_log.level = common_pb2.LogMessage.Level.Error
        error_log.message = error_message
        driver_response.messages.append(error_log)

        return driver_response
