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

from src.dependencies import require_admin

from google.protobuf import json_format
from google.protobuf.any_pb2 import Any as AnyProto

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "generated"))

from proto import common_pb2, backend_pb2, project_pb2, credentials_pb2
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
        if "host" in request.credentials:
            creds.host = request.credentials["host"]
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


def _create_command_message(type_name: str, command_json: dict):
    """Create a protobuf command message from JSON.

    Supports all command types from the driver protocol.
    """
    # Remove type/@type fields before parsing (these are just identifiers, not data)
    command_data = {k: v for k, v in command_json.items() if k not in ("type", "@type")}

    # Map type name to message class
    message_classes = {
        "InitBackendCommand": backend_pb2.InitBackendCommand,
        "RemoveBackendCommand": backend_pb2.RemoveBackendCommand,
        "CreateProjectCommand": project_pb2.CreateProjectCommand,
        "DropProjectCommand": project_pb2.DropProjectCommand,
        # Add more command types as handlers are implemented
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
        "InitBackendResponse": backend_pb2.InitBackendResponse,
        # RemoveBackendCommand returns None (no response message)
        "CreateProjectResponse": project_pb2.CreateProjectResponse,
        # DropProjectCommand returns None (no response message)
        # Add more response types as handlers are implemented
    }

    response_class = response_classes.get(type_name)
    if not response_class:
        return None

    message = response_class()
    any_proto.Unpack(message)
    return message


@router.post("/execute", response_model=DriverExecuteResponse, dependencies=[Depends(require_admin)])
async def execute_driver_command(request: DriverExecuteRequest) -> DriverExecuteResponse:
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
            {
                "type": "CreateProjectCommand",
                "description": "Create a new project",
                "example": {"type": "CreateProjectCommand", "projectId": "my-project"}
            },
            {
                "type": "DropProjectCommand",
                "description": "Drop a project and all its data",
                "example": {"type": "DropProjectCommand", "projectId": "my-project"}
            },
        ],
        "note": "More commands will be added as handlers are implemented"
    }
