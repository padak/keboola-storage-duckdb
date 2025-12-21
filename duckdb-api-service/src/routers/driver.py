"""Driver Execute Router - HTTP bridge for gRPC driver commands.

This router provides an HTTP endpoint that accepts protobuf commands
in JSON format and delegates to gRPC handlers. This allows PHP drivers
to communicate without needing gRPC PHP extension.

The endpoint mirrors the gRPC StorageDriverService.Execute() RPC method.
"""

import logging
from typing import Any, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from src.dependencies import require_driver_auth, get_project_id_from_driver_key, verify_admin_key

from google.protobuf import json_format
from google.protobuf.any_pb2 import Any as AnyProto

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "generated"))

from proto import common_pb2, backend_pb2, project_pb2, bucket_pb2, table_pb2, info_pb2, credentials_pb2, workspace_pb2
from src.database import metadata_db, project_db_manager
from src.grpc.servicer import StorageDriverServicer


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/driver", tags=["driver"])


class DriverExecuteRequest(BaseModel):
    """Request model for driver execute endpoint.

    Mirrors the gRPC DriverRequest structure:
    - command: The command to execute (as JSON with @type field)
    - credentials: Optional credentials (as JSON)
    - features: Optional list of enabled features
    - runtimeOptions: Optional runtime options
    """
    command: dict
    credentials: Optional[dict] = None
    features: Optional[list[str]] = None
    runtimeOptions: Optional[dict] = None


class LogMessageResponse(BaseModel):
    """Log message from driver execution."""
    level: str  # "Error", "Warning", "Info"
    message: str


class DriverExecuteResponse(BaseModel):
    """Response model for driver execute endpoint.

    Mirrors the gRPC DriverResponse structure:
    - commandResponse: The command-specific response (as JSON with @type field)
    - messages: Log messages from the driver
    """
    commandResponse: Optional[dict] = None
    messages: list[LogMessageResponse] = []


# Lazy-initialized servicer (shares database with REST API)
_servicer: Optional[StorageDriverServicer] = None


def get_servicer() -> StorageDriverServicer:
    """Get or create the StorageDriverServicer instance."""
    global _servicer
    if _servicer is None:
        _servicer = StorageDriverServicer(metadata_db, project_db_manager)
    return _servicer


class MockGrpcContext:
    """Mock gRPC context for HTTP requests."""

    def __init__(self):
        self._code = None
        self._details = None

    def set_code(self, code):
        self._code = code

    def set_details(self, details):
        self._details = details

    def get_code(self):
        return self._code

    def get_details(self):
        return self._details


def json_to_driver_request(request: DriverExecuteRequest) -> common_pb2.DriverRequest:
    """Convert JSON request to protobuf DriverRequest.

    The command JSON must contain 'type' field indicating the command type,
    e.g., "InitBackendCommand" or "CreateProjectCommand"
    """
    driver_request = common_pb2.DriverRequest()

    # Get command type - support both simple name and full type URL
    command_type = request.command.get("type", request.command.get("@type", ""))
    if not command_type:
        raise ValueError("Command must contain 'type' field (e.g., 'InitBackendCommand')")

    # Extract just the command name if full type URL was provided
    # e.g., "type.googleapis.com/keboola.storageDriver.command.backend.InitBackendCommand"
    # -> "InitBackendCommand"
    type_name = command_type.split(".")[-1]

    # Create the appropriate command message
    command_msg = _create_command_message(type_name, request.command)

    # Pack into Any
    driver_request.command.Pack(command_msg)

    # Pack credentials if present
    if request.credentials:
        creds = credentials_pb2.GenericBackendCredentials()
        # Support both 'host' and 'project_id' for host field
        # PHP sends 'project_id', protobuf uses 'host'
        if "host" in request.credentials:
            creds.host = request.credentials["host"]
        elif "project_id" in request.credentials:
            creds.host = request.credentials["project_id"]
        if "principal" in request.credentials:
            creds.principal = request.credentials["principal"]
        # Add more credential fields as needed
        driver_request.credentials.Pack(creds)

    # Set features
    if request.features:
        driver_request.features.extend(request.features)

    # Set runtime options
    if request.runtimeOptions:
        if "runId" in request.runtimeOptions:
            driver_request.runtimeOptions.runId = request.runtimeOptions["runId"]

    return driver_request


def _snake_to_camel(name: str) -> str:
    """Convert snake_case to camelCase."""
    components = name.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def _convert_keys_to_camel_case(data: dict) -> dict:
    """Recursively convert all dict keys from snake_case to camelCase."""
    if not isinstance(data, dict):
        return data

    result = {}
    for key, value in data.items():
        new_key = _snake_to_camel(key)
        if isinstance(value, dict):
            result[new_key] = _convert_keys_to_camel_case(value)
        elif isinstance(value, list):
            result[new_key] = [
                _convert_keys_to_camel_case(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            result[new_key] = value
    return result


def _create_command_message(type_name: str, command_json: dict):
    """Create a protobuf command message from JSON.

    Supports all command types from the driver protocol.
    The PHP driver sends snake_case field names, but protobuf expects camelCase.
    """
    # Remove type/@type fields before parsing (these are just identifiers, not data)
    command_data = {k: v for k, v in command_json.items() if k not in ("type", "@type")}

    # Convert snake_case keys to camelCase (PHP driver sends snake_case)
    command_data = _convert_keys_to_camel_case(command_data)

    # Map type name to message class
    message_classes = {
        # Backend commands
        "InitBackendCommand": backend_pb2.InitBackendCommand,
        "RemoveBackendCommand": backend_pb2.RemoveBackendCommand,
        # Project commands
        "CreateProjectCommand": project_pb2.CreateProjectCommand,
        "DropProjectCommand": project_pb2.DropProjectCommand,
        # Bucket commands (Phase 12c)
        "CreateBucketCommand": bucket_pb2.CreateBucketCommand,
        "DropBucketCommand": bucket_pb2.DropBucketCommand,
        # Table commands (Phase 12c)
        "CreateTableCommand": table_pb2.CreateTableCommand,
        "DropTableCommand": table_pb2.DropTableCommand,
        "PreviewTableCommand": table_pb2.PreviewTableCommand,
        # Info commands (Phase 12c)
        "ObjectInfoCommand": info_pb2.ObjectInfoCommand,
        # Import/Export commands (Phase 12c)
        "TableImportFromFileCommand": table_pb2.TableImportFromFileCommand,
        "TableExportToFileCommand": table_pb2.TableExportToFileCommand,
        # Schema commands (Phase 12d)
        "AddColumnCommand": table_pb2.AddColumnCommand,
        "DropColumnCommand": table_pb2.DropColumnCommand,
        "AlterColumnCommand": table_pb2.AlterColumnCommand,
        "AddPrimaryKeyCommand": table_pb2.AddPrimaryKeyCommand,
        "DropPrimaryKeyCommand": table_pb2.DropPrimaryKeyCommand,
        "DeleteTableRowsCommand": table_pb2.DeleteTableRowsCommand,
        # Workspace commands (Phase 12e)
        "CreateWorkspaceCommand": workspace_pb2.CreateWorkspaceCommand,
        "DropWorkspaceCommand": workspace_pb2.DropWorkspaceCommand,
        "ClearWorkspaceCommand": workspace_pb2.ClearWorkspaceCommand,
        "ResetWorkspacePasswordCommand": workspace_pb2.ResetWorkspacePasswordCommand,
        "DropWorkspaceObjectCommand": workspace_pb2.DropWorkspaceObjectCommand,
        "GrantWorkspaceAccessToProjectCommand": workspace_pb2.GrantWorkspaceAccessToProjectCommand,
        "RevokeWorkspaceAccessToProjectCommand": workspace_pb2.RevokeWorkspaceAccessToProjectCommand,
        "LoadTableToWorkspaceCommand": workspace_pb2.LoadTableToWorkspaceCommand,
    }

    message_class = message_classes.get(type_name)
    if not message_class:
        raise ValueError(f"Unsupported command type: {type_name}")

    # Parse JSON into message
    message = message_class()
    json_format.ParseDict(command_data, message)
    return message


def driver_response_to_json(response: common_pb2.DriverResponse) -> DriverExecuteResponse:
    """Convert protobuf DriverResponse to JSON response."""
    result = DriverExecuteResponse()

    # Convert command response if present
    if response.commandResponse.ByteSize() > 0:
        # Get the packed message type and unpack
        type_url = response.commandResponse.type_url
        type_name = type_url.split(".")[-1]

        # Unpack based on type
        unpacked = _unpack_response(type_name, response.commandResponse)
        if unpacked:
            result.commandResponse = json_format.MessageToDict(
                unpacked,
                preserving_proto_field_name=True
            )
            # Add @type field
            result.commandResponse["@type"] = type_url

    # Convert log messages
    level_names = {
        common_pb2.LogMessage.Level.Emergency: "Error",
        common_pb2.LogMessage.Level.Alert: "Error",
        common_pb2.LogMessage.Level.Critical: "Error",
        common_pb2.LogMessage.Level.Error: "Error",
        common_pb2.LogMessage.Level.Warning: "Warning",
        common_pb2.LogMessage.Level.Notice: "Info",
        common_pb2.LogMessage.Level.Informational: "Info",
        common_pb2.LogMessage.Level.Debug: "Info",
    }

    for msg in response.messages:
        result.messages.append(LogMessageResponse(
            level=level_names.get(msg.level, "Info"),
            message=msg.message
        ))

    return result


def _unpack_response(type_name: str, any_proto: AnyProto):
    """Unpack a response message from Any field."""
    response_classes = {
        # Backend responses
        "InitBackendResponse": backend_pb2.InitBackendResponse,
        # RemoveBackendCommand returns None (no response message)
        # Project responses
        "CreateProjectResponse": project_pb2.CreateProjectResponse,
        # DropProjectCommand returns None (no response message)
        # Bucket responses (Phase 12c)
        "CreateBucketResponse": bucket_pb2.CreateBucketResponse,
        # DropBucketCommand returns None (no response message)
        # Table responses (Phase 12c)
        # CreateTableCommand returns None (no response message)
        # DropTableCommand returns None (no response message)
        "PreviewTableResponse": table_pb2.PreviewTableResponse,
        # Info responses (Phase 12c)
        "ObjectInfoResponse": info_pb2.ObjectInfoResponse,
        # Import/Export responses (Phase 12c)
        "TableImportResponse": table_pb2.TableImportResponse,
        "TableExportToFileResponse": table_pb2.TableExportToFileResponse,
        # Schema responses (Phase 12d)
        # AddColumnCommand, DropColumnCommand, AlterColumnCommand,
        # AddPrimaryKeyCommand, DropPrimaryKeyCommand return None
        "DeleteTableRowsResponse": table_pb2.DeleteTableRowsResponse,
        # Workspace responses (Phase 12e)
        "CreateWorkspaceResponse": workspace_pb2.CreateWorkspaceResponse,
        "ResetWorkspacePasswordResponse": workspace_pb2.ResetWorkspacePasswordResponse,
        # DropWorkspaceCommand, ClearWorkspaceCommand, DropWorkspaceObjectCommand,
        # GrantWorkspaceAccessToProjectCommand, RevokeWorkspaceAccessToProjectCommand,
        # LoadTableToWorkspaceCommand return None
    }

    response_class = response_classes.get(type_name)
    if not response_class:
        return None

    message = response_class()
    any_proto.Unpack(message)
    return message


@router.post("/execute", response_model=DriverExecuteResponse)
async def execute_driver_command(
    request: DriverExecuteRequest,
    api_key: str = Depends(require_driver_auth),
) -> DriverExecuteResponse:
    """Execute a storage driver command.

    This endpoint is the HTTP bridge for driver protocol commands.
    It accepts commands in JSON format.

    **Command format:**
    ```json
    {
        "command": {
            "type": "InitBackendCommand"
        }
    }
    ```

    **With credentials:**
    ```json
    {
        "command": {
            "type": "CreateProjectCommand",
            "projectId": "my-project"
        },
        "credentials": {
            "host": "project_id",
            "principal": "api_key"
        }
    }
    ```

    **Supported commands:**
    - InitBackendCommand
    - RemoveBackendCommand
    - CreateProjectCommand
    - DropProjectCommand

    More commands will be added as handlers are implemented.
    """
    try:
        # Get command type for authorization check
        command_type = request.command.get("type", request.command.get("@type", ""))
        type_name = command_type.split(".")[-1]

        # Commands that require admin key (not project key)
        admin_only_commands = {
            "InitBackendCommand",
            "RemoveBackendCommand",
            "CreateProjectCommand",
            "DropProjectCommand",
        }

        # Check authorization based on command type
        if type_name in admin_only_commands:
            # Admin-only commands require admin key
            if not verify_admin_key(api_key):
                raise HTTPException(
                    status_code=403,
                    detail=f"Command {type_name} requires admin API key"
                )
        else:
            # Project commands - verify project_id matches the API key
            request_project_id = request.credentials.get("project_id") if request.credentials else None
            key_project_id = get_project_id_from_driver_key(api_key)

            if key_project_id is not None:
                # Using project key - must match the project_id in credentials
                if request_project_id != key_project_id:
                    logger.warning(
                        "auth_project_mismatch_in_driver",
                        key_project=key_project_id,
                        request_project=request_project_id,
                    )
                    raise HTTPException(
                        status_code=403,
                        detail=f"API key is for project {key_project_id}, but request is for project {request_project_id}"
                    )
            # else: using admin key - allowed for any project

        # Convert JSON to protobuf
        driver_request = json_to_driver_request(request)

        # Get servicer and execute
        servicer = get_servicer()
        context = MockGrpcContext()

        response = servicer.Execute(driver_request, context)

        # Check for gRPC errors
        if context.get_code() is not None:
            import grpc
            if context.get_code() == grpc.StatusCode.UNIMPLEMENTED:
                raise HTTPException(status_code=501, detail=context.get_details())
            elif context.get_code() == grpc.StatusCode.INVALID_ARGUMENT:
                raise HTTPException(status_code=400, detail=context.get_details())
            elif context.get_code() == grpc.StatusCode.NOT_FOUND:
                raise HTTPException(status_code=404, detail=context.get_details())
            else:
                raise HTTPException(status_code=500, detail=context.get_details())

        # Convert response to JSON
        return driver_response_to_json(response)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error executing driver command")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/commands")
async def list_supported_commands() -> dict:
    """List all supported driver commands.

    Returns a list of command types that can be sent to the /execute endpoint.
    """
    return {
        "supported_commands": [
            # Backend commands
            {
                "type": "InitBackendCommand",
                "description": "Initialize the backend (verify configuration)",
                "example": {"type": "InitBackendCommand"}
            },
            {
                "type": "RemoveBackendCommand",
                "description": "Remove all backend data",
                "example": {"type": "RemoveBackendCommand"}
            },
            # Project commands
            {
                "type": "CreateProjectCommand",
                "description": "Create a new project",
                "example": {"type": "CreateProjectCommand", "projectId": "my-project"}
            },
            {
                "type": "DropProjectCommand",
                "description": "Drop a project and all its data",
                "example": {"type": "DropProjectCommand", "projectDatabaseName": "my-project"}
            },
            # Bucket commands (Phase 12c)
            {
                "type": "CreateBucketCommand",
                "description": "Create a bucket in a project",
                "example": {
                    "type": "CreateBucketCommand",
                    "projectId": "my-project",
                    "bucketId": "in.c-sales"
                }
            },
            {
                "type": "DropBucketCommand",
                "description": "Drop a bucket from a project",
                "example": {
                    "type": "DropBucketCommand",
                    "bucketObjectName": "in_c_sales",
                    "isCascade": True
                }
            },
            # Table commands (Phase 12c)
            {
                "type": "CreateTableCommand",
                "description": "Create a table in a bucket",
                "example": {
                    "type": "CreateTableCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders",
                    "columns": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "VARCHAR"}
                    ]
                }
            },
            {
                "type": "DropTableCommand",
                "description": "Drop a table from a bucket",
                "example": {
                    "type": "DropTableCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders"
                }
            },
            {
                "type": "PreviewTableCommand",
                "description": "Preview table data",
                "example": {
                    "type": "PreviewTableCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders"
                }
            },
            # Info commands (Phase 12c)
            {
                "type": "ObjectInfoCommand",
                "description": "Get object information (table, bucket, project)",
                "example": {
                    "type": "ObjectInfoCommand",
                    "path": ["my-project", "in_c_sales", "orders"],
                    "expectedObjectType": 2  # 2 = TABLE
                }
            },
            # Import/Export commands (Phase 12c)
            {
                "type": "TableImportFromFileCommand",
                "description": "Import data from a file into a table",
                "example": {
                    "type": "TableImportFromFileCommand",
                    "destination": {
                        "path": ["my-project", "in_c_sales"],
                        "tableName": "orders"
                    },
                    "fileProvider": 0,  # 0 = S3
                    "filePath": {
                        "root": "bucket-name",
                        "path": "path/to",
                        "fileName": "data.csv"
                    }
                }
            },
            {
                "type": "TableExportToFileCommand",
                "description": "Export table data to a file",
                "example": {
                    "type": "TableExportToFileCommand",
                    "source": {
                        "path": ["my-project", "in_c_sales"],
                        "tableName": "orders"
                    },
                    "fileProvider": 0,  # 0 = S3
                    "filePath": {
                        "root": "bucket-name",
                        "path": "exports",
                        "fileName": "export.csv"
                    }
                }
            },
            # Schema commands (Phase 12d)
            {
                "type": "AddColumnCommand",
                "description": "Add a new column to a table",
                "example": {
                    "type": "AddColumnCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders",
                    "columnDefinition": {"name": "email", "type": "VARCHAR", "nullable": True}
                }
            },
            {
                "type": "DropColumnCommand",
                "description": "Drop a column from a table",
                "example": {
                    "type": "DropColumnCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders",
                    "columnName": "email"
                }
            },
            {
                "type": "AlterColumnCommand",
                "description": "Alter a column in a table",
                "example": {
                    "type": "AlterColumnCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders",
                    "desiredDefiniton": {"name": "email_new", "type": "TEXT"},
                    "attributesToUpdate": ["name", "type"]
                }
            },
            {
                "type": "AddPrimaryKeyCommand",
                "description": "Add a primary key to a table",
                "example": {
                    "type": "AddPrimaryKeyCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders",
                    "primaryKeysNames": ["id"]
                }
            },
            {
                "type": "DropPrimaryKeyCommand",
                "description": "Drop the primary key from a table",
                "example": {
                    "type": "DropPrimaryKeyCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders"
                }
            },
            {
                "type": "DeleteTableRowsCommand",
                "description": "Delete rows from a table based on filters",
                "example": {
                    "type": "DeleteTableRowsCommand",
                    "path": ["my-project", "in_c_sales"],
                    "tableName": "orders",
                    "whereFilters": [
                        {"columnsName": "status", "operator": 0, "values": ["deleted"]}
                    ]
                }
            },
            # Workspace commands (Phase 12e)
            {
                "type": "CreateWorkspaceCommand",
                "description": "Create a new workspace with credentials",
                "example": {
                    "type": "CreateWorkspaceCommand",
                    "projectId": "my-project",
                    "workspaceId": "ws-123",
                    "isBranchDefault": True
                }
            },
            {
                "type": "DropWorkspaceCommand",
                "description": "Drop a workspace",
                "example": {
                    "type": "DropWorkspaceCommand",
                    "workspaceObjectName": "ws-123"
                }
            },
            {
                "type": "ClearWorkspaceCommand",
                "description": "Clear all objects from a workspace",
                "example": {
                    "type": "ClearWorkspaceCommand",
                    "workspaceObjectName": "ws-123",
                    "ignoreErrors": False
                }
            },
            {
                "type": "ResetWorkspacePasswordCommand",
                "description": "Reset workspace password",
                "example": {
                    "type": "ResetWorkspacePasswordCommand",
                    "workspaceUserName": "ws_ws-123_abc123"
                }
            },
            {
                "type": "DropWorkspaceObjectCommand",
                "description": "Drop a single object from workspace",
                "example": {
                    "type": "DropWorkspaceObjectCommand",
                    "workspaceObjectName": "ws-123",
                    "objectNameToDrop": "my_table",
                    "ignoreIfNotExists": True
                }
            },
            {
                "type": "LoadTableToWorkspaceCommand",
                "description": "Load table from project storage to workspace",
                "example": {
                    "type": "LoadTableToWorkspaceCommand",
                    "source": {
                        "path": ["my-project", "in_c_sales"],
                        "tableName": "orders"
                    },
                    "destination": {
                        "path": ["ws-123"],
                        "tableName": "orders_copy"
                    }
                }
            },
        ],
        "total_commands": 26
    }
