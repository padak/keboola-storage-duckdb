"""Workspaces management endpoints.

Workspaces provide isolated SQL sandboxes for data exploration and transformations:
- Isolated DuckDB instances with TTL expiration
- Credential-based access (username/password)
- Read-only ATTACH of project tables
- Full SQL query capabilities
- Automatic cleanup on expiration
"""

import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.config import settings
from src.database import metadata_db, project_db_manager
from src.dependencies import require_project_access
from src.metrics import WORKSPACES_TOTAL
from src.models.responses import (
    ErrorResponse,
    WorkspaceConnectionInfo,
    WorkspaceCreateRequest,
    WorkspaceDetailResponse,
    WorkspaceListResponse,
    WorkspaceLoadRequest,
    WorkspaceLoadResponse,
    WorkspaceLoadTableResult,
    WorkspaceObjectInfo,
    WorkspaceResponse,
    WorkspaceTableInfo,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["workspaces"])


def _validate_project_exists(project_id: str) -> None:
    """Validate that project exists."""
    project = metadata_db.get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )


def _validate_workspace_exists(project_id: str, workspace_id: str) -> dict:
    """Validate that workspace exists and belongs to project. Returns workspace dict."""
    _validate_project_exists(project_id)
    workspace = metadata_db.get_workspace_by_project(project_id, workspace_id)
    if not workspace:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "workspace_not_found",
                "message": f"Workspace {workspace_id} not found in project {project_id}",
                "details": {"project_id": project_id, "workspace_id": workspace_id},
            },
        )
    return workspace


def _check_workspace_expired(workspace: dict) -> None:
    """Check if workspace is expired and raise error if so."""
    if workspace.get("expires_at"):
        expires_at = datetime.fromisoformat(workspace["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= datetime.now(timezone.utc):
            raise HTTPException(
                status_code=status.HTTP_410_GONE,
                detail={
                    "error": "workspace_expired",
                    "message": f"Workspace {workspace['id']} has expired",
                    "details": {
                        "workspace_id": workspace["id"],
                        "expired_at": workspace["expires_at"],
                    },
                },
            )


def _generate_password() -> str:
    """Generate a secure random password (32 characters)."""
    return secrets.token_urlsafe(24)[:32]


def _hash_password(password: str) -> str:
    """Hash password using SHA256."""
    return hashlib.sha256(password.encode()).hexdigest()


def _generate_username(workspace_id: str) -> str:
    """Generate workspace username."""
    random_suffix = secrets.token_hex(4)
    return f"ws_{workspace_id}_{random_suffix}"


def _workspace_to_response(
    workspace: dict, credentials: dict | None = None, include_password: bool = False
) -> WorkspaceResponse:
    """Convert workspace dict to response model."""
    size_bytes = project_db_manager.get_workspace_size(
        workspace["project_id"], workspace["id"], workspace.get("branch_id")
    )

    # Check if expired
    status_val = workspace.get("status", "active")
    if workspace.get("expires_at"):
        expires_at = datetime.fromisoformat(workspace["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= datetime.now(timezone.utc):
            status_val = "expired"

    # Build connection info
    connection = None
    if credentials:
        connection = WorkspaceConnectionInfo(
            host=settings.workspace_host if hasattr(settings, "workspace_host") else "localhost",
            port=settings.workspace_port if hasattr(settings, "workspace_port") else 5432,
            database=f"workspace_{workspace['id']}",
            username=credentials.get("username", ""),
            password=credentials.get("password") if include_password else None,
            ssl_mode="prefer",
            connection_string=None,
        )

    return WorkspaceResponse(
        id=workspace["id"],
        name=workspace["name"],
        project_id=workspace["project_id"],
        branch_id=workspace.get("branch_id"),
        created_at=workspace.get("created_at"),
        expires_at=workspace.get("expires_at"),
        size_bytes=size_bytes,
        size_limit_gb=workspace.get("size_limit_bytes", 10737418240) // (1024 * 1024 * 1024),
        status=status_val,
        connection=connection,
    )


@router.post(
    "/projects/{project_id}/workspaces",
    response_model=WorkspaceResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Workspace created successfully"},
        404: {"model": ErrorResponse, "description": "Project not found"},
    },
    summary="Create a workspace",
    description="""
    Create a new temporary SQL workspace for data exploration.

    The workspace provides:
    - Isolated DuckDB instance
    - Temporary credentials (username/password)
    - TTL-based automatic expiration
    - Table loading from project buckets

    **IMPORTANT**: The password is returned ONLY on creation. Save it securely.
    """,
)
async def create_workspace(
    project_id: str,
    request: WorkspaceCreateRequest,
    _auth: None = Depends(require_project_access),
) -> WorkspaceResponse:
    """Create a new workspace."""
    _validate_project_exists(project_id)

    # Generate workspace ID
    workspace_id = f"ws_{str(uuid.uuid4())[:8]}"

    # Generate credentials
    username = _generate_username(workspace_id)
    password = _generate_password()
    password_hash = _hash_password(password)

    # Calculate expiration
    expires_at = None
    if request.ttl_hours:
        expires_at = datetime.now(timezone.utc) + timedelta(hours=request.ttl_hours)

    # Create workspace database file
    db_path = project_db_manager.create_workspace_db(project_id, workspace_id)

    # Calculate size limit in bytes
    size_limit_bytes = request.size_limit_gb * 1024 * 1024 * 1024

    # Create workspace metadata record
    workspace = metadata_db.create_workspace(
        workspace_id=workspace_id,
        project_id=project_id,
        name=request.name,
        db_path=str(db_path),
        branch_id=None,
        expires_at=expires_at.isoformat() if expires_at else None,
        size_limit_bytes=size_limit_bytes,
    )

    # Create credentials
    metadata_db.create_workspace_credentials(workspace_id, username, password_hash)

    # Update metrics
    WORKSPACES_TOTAL.set(metadata_db.count_workspaces())

    # Log operation
    metadata_db.log_operation(
        operation="create_workspace",
        status="success",
        project_id=project_id,
        resource_type="workspace",
        resource_id=workspace_id,
        details={
            "name": request.name,
            "ttl_hours": request.ttl_hours,
            "expires_at": expires_at.isoformat() if expires_at else None,
        },
    )

    logger.info(
        "workspace_created",
        project_id=project_id,
        workspace_id=workspace_id,
        name=request.name,
        ttl_hours=request.ttl_hours,
    )

    # Return response with password (only time it's returned)
    credentials = {"username": username, "password": password}
    return _workspace_to_response(workspace, credentials, include_password=True)


@router.get(
    "/projects/{project_id}/workspaces",
    response_model=WorkspaceListResponse,
    responses={
        200: {"description": "List of workspaces"},
        404: {"model": ErrorResponse, "description": "Project not found"},
    },
    summary="List workspaces",
    description="List all workspaces for a project, including expired ones.",
)
async def list_workspaces(
    project_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    _auth: None = Depends(require_project_access),
) -> WorkspaceListResponse:
    """List all workspaces for a project."""
    _validate_project_exists(project_id)

    workspaces = metadata_db.list_workspaces(project_id, limit=limit, offset=offset)

    # Get credentials for each workspace
    responses = []
    for ws in workspaces:
        creds = metadata_db.get_workspace_credentials(ws["id"])
        responses.append(_workspace_to_response(ws, creds, include_password=False))

    return WorkspaceListResponse(
        workspaces=responses,
        count=len(responses),
    )


@router.get(
    "/projects/{project_id}/workspaces/{workspace_id}",
    response_model=WorkspaceDetailResponse,
    responses={
        200: {"description": "Workspace details"},
        404: {"model": ErrorResponse, "description": "Workspace not found"},
        410: {"model": ErrorResponse, "description": "Workspace expired"},
    },
    summary="Get workspace details",
    description="Get detailed information about a workspace including objects.",
)
async def get_workspace(
    project_id: str,
    workspace_id: str,
    _auth: None = Depends(require_project_access),
) -> WorkspaceDetailResponse:
    """Get workspace details."""
    workspace = _validate_workspace_exists(project_id, workspace_id)
    _check_workspace_expired(workspace)

    # Get credentials
    credentials = metadata_db.get_workspace_credentials(workspace_id)

    # Get workspace objects (tables, views)
    objects_raw = project_db_manager.list_workspace_objects(
        project_id, workspace_id, workspace.get("branch_id")
    )
    objects = [
        WorkspaceObjectInfo(name=obj["name"], type=obj["type"], rows=obj["rows"])
        for obj in objects_raw
    ]

    # Get size
    size_bytes = project_db_manager.get_workspace_size(
        project_id, workspace_id, workspace.get("branch_id")
    )

    # Check if expired
    status_val = workspace.get("status", "active")
    if workspace.get("expires_at"):
        expires_at = datetime.fromisoformat(workspace["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= datetime.now(timezone.utc):
            status_val = "expired"

    # Build connection info
    connection = None
    if credentials:
        connection = WorkspaceConnectionInfo(
            host=settings.workspace_host if hasattr(settings, "workspace_host") else "localhost",
            port=settings.workspace_port if hasattr(settings, "workspace_port") else 5432,
            database=f"workspace_{workspace['id']}",
            username=credentials.get("username", ""),
            password=None,  # Never return password on GET
            ssl_mode="prefer",
            connection_string=None,
        )

    # Get attached project tables (all tables in project that can be ATTACHed)
    attached_tables = []
    buckets = project_db_manager.list_buckets(project_id)
    for bucket in buckets:
        bucket_name = bucket["name"]
        tables = project_db_manager.list_tables(project_id, bucket_name)
        for t in tables:
            attached_tables.append(
                WorkspaceTableInfo(schema=bucket_name, table=t["name"], rows=t.get("row_count", 0))
            )

    return WorkspaceDetailResponse(
        id=workspace["id"],
        name=workspace["name"],
        project_id=workspace["project_id"],
        branch_id=workspace.get("branch_id"),
        created_at=workspace.get("created_at"),
        expires_at=workspace.get("expires_at"),
        size_bytes=size_bytes,
        size_limit_gb=workspace.get("size_limit_bytes", 10737418240) // (1024 * 1024 * 1024),
        status=status_val,
        connection=connection,
        active_sessions=0,  # TODO: Track active sessions
        attached_tables=attached_tables,
        workspace_objects=objects,
    )


@router.delete(
    "/projects/{project_id}/workspaces/{workspace_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Workspace deleted"},
        404: {"model": ErrorResponse, "description": "Workspace not found"},
    },
    summary="Delete a workspace",
    description="""
    Delete a workspace and all its data.

    WARNING: All data in this workspace will be permanently deleted.
    This operation cannot be undone.
    """,
)
async def delete_workspace(
    project_id: str,
    workspace_id: str,
    _auth: None = Depends(require_project_access),
) -> None:
    """Delete a workspace."""
    workspace = _validate_workspace_exists(project_id, workspace_id)

    # Get workspace size for logging
    size_bytes = project_db_manager.get_workspace_size(
        project_id, workspace_id, workspace.get("branch_id")
    )

    # Delete workspace database file
    project_db_manager.delete_workspace_db(
        project_id, workspace_id, workspace.get("branch_id")
    )

    # Delete workspace metadata (credentials cascade)
    metadata_db.delete_workspace(workspace_id)

    # Update metrics
    WORKSPACES_TOTAL.set(metadata_db.count_workspaces())

    # Log operation
    metadata_db.log_operation(
        operation="delete_workspace",
        status="success",
        project_id=project_id,
        resource_type="workspace",
        resource_id=workspace_id,
        details={
            "name": workspace["name"],
            "size_bytes": size_bytes,
        },
    )

    logger.info(
        "workspace_deleted",
        project_id=project_id,
        workspace_id=workspace_id,
        name=workspace["name"],
    )


@router.post(
    "/projects/{project_id}/workspaces/{workspace_id}/clear",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Workspace cleared"},
        404: {"model": ErrorResponse, "description": "Workspace not found"},
        410: {"model": ErrorResponse, "description": "Workspace expired"},
    },
    summary="Clear workspace",
    description="Clear all data from workspace (drop all tables, views, etc.).",
)
async def clear_workspace(
    project_id: str,
    workspace_id: str,
    _auth: None = Depends(require_project_access),
) -> None:
    """Clear all data from workspace."""
    workspace = _validate_workspace_exists(project_id, workspace_id)
    _check_workspace_expired(workspace)

    # Clear workspace (drop all objects)
    project_db_manager.clear_workspace(
        project_id, workspace_id, workspace.get("branch_id")
    )

    # Log operation
    metadata_db.log_operation(
        operation="clear_workspace",
        status="success",
        project_id=project_id,
        resource_type="workspace",
        resource_id=workspace_id,
        details={},
    )

    logger.info(
        "workspace_cleared",
        project_id=project_id,
        workspace_id=workspace_id,
    )


@router.delete(
    "/projects/{project_id}/workspaces/{workspace_id}/objects/{object_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Object dropped"},
        404: {"model": ErrorResponse, "description": "Workspace or object not found"},
        410: {"model": ErrorResponse, "description": "Workspace expired"},
    },
    summary="Drop workspace object",
    description="Drop a specific object (table, view) from workspace.",
)
async def drop_workspace_object(
    project_id: str,
    workspace_id: str,
    object_name: str,
    _auth: None = Depends(require_project_access),
) -> None:
    """Drop a workspace object."""
    workspace = _validate_workspace_exists(project_id, workspace_id)
    _check_workspace_expired(workspace)

    # Check if object exists
    objects = project_db_manager.list_workspace_objects(
        project_id, workspace_id, workspace.get("branch_id")
    )
    object_exists = any(obj["name"] == object_name for obj in objects)

    if not object_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "object_not_found",
                "message": f"Object '{object_name}' not found in workspace {workspace_id}",
                "details": {
                    "project_id": project_id,
                    "workspace_id": workspace_id,
                    "object_name": object_name,
                },
            },
        )

    # Drop the object
    project_db_manager.drop_workspace_object(
        project_id, workspace_id, object_name, workspace.get("branch_id")
    )

    # Log operation
    metadata_db.log_operation(
        operation="drop_workspace_object",
        status="success",
        project_id=project_id,
        resource_type="workspace_object",
        resource_id=f"{workspace_id}/{object_name}",
        details={"object_name": object_name},
    )

    logger.info(
        "workspace_object_dropped",
        project_id=project_id,
        workspace_id=workspace_id,
        object_name=object_name,
    )


@router.post(
    "/projects/{project_id}/workspaces/{workspace_id}/load",
    response_model=WorkspaceLoadResponse,
    responses={
        200: {"description": "Tables loaded successfully"},
        404: {"model": ErrorResponse, "description": "Workspace or table not found"},
        410: {"model": ErrorResponse, "description": "Workspace expired"},
    },
    summary="Load data into workspace",
    description="""
    Load tables from project buckets into workspace.

    This operation copies table data into the workspace DuckDB instance.
    Changes in workspace don't affect source tables.
    """,
)
async def load_data(
    project_id: str,
    workspace_id: str,
    request: WorkspaceLoadRequest,
    _auth: None = Depends(require_project_access),
) -> WorkspaceLoadResponse:
    """Load tables into workspace."""
    workspace = _validate_workspace_exists(project_id, workspace_id)
    _check_workspace_expired(workspace)

    results = []
    total_size = 0

    for table_spec in request.tables:
        source = table_spec.get("source", "")
        destination = table_spec.get("destination")
        columns = table_spec.get("columns")
        where_clause = table_spec.get("where")

        # Parse source (format: bucket.table)
        parts = source.split(".")
        if len(parts) != 2:
            results.append(
                WorkspaceLoadTableResult(
                    source=source,
                    destination=destination or source,
                    rows=0,
                    size_bytes=0,
                )
            )
            continue

        bucket_name, table_name = parts

        # Check if table exists
        if not project_db_manager.table_exists(project_id, bucket_name, table_name):
            results.append(
                WorkspaceLoadTableResult(
                    source=source,
                    destination=destination or table_name,
                    rows=0,
                    size_bytes=0,
                )
            )
            continue

        try:
            # Load table into workspace
            dest_table = destination or table_name
            load_result = project_db_manager.load_table_to_workspace(
                project_id=project_id,
                workspace_id=workspace_id,
                source_bucket=bucket_name,
                source_table=table_name,
                dest_table=dest_table,
                columns=columns,
                where_clause=where_clause,
                branch_id=workspace.get("branch_id"),
            )

            results.append(
                WorkspaceLoadTableResult(
                    source=source,
                    destination=dest_table,
                    rows=load_result["rows"],
                    size_bytes=load_result["size_bytes"],
                )
            )
            total_size = load_result["size_bytes"]

            logger.info(
                "workspace_table_loaded",
                project_id=project_id,
                workspace_id=workspace_id,
                source=source,
                destination=dest_table,
                rows=load_result["rows"],
            )

        except Exception as e:
            logger.error(
                "workspace_table_load_failed",
                project_id=project_id,
                workspace_id=workspace_id,
                source=source,
                error=str(e),
            )
            results.append(
                WorkspaceLoadTableResult(
                    source=source,
                    destination=destination or table_name,
                    rows=0,
                    size_bytes=0,
                )
            )

    # Log operation
    metadata_db.log_operation(
        operation="load_workspace_tables",
        status="success",
        project_id=project_id,
        resource_type="workspace",
        resource_id=workspace_id,
        details={
            "tables_requested": len(request.tables),
            "tables_loaded": len([r for r in results if r.rows > 0]),
        },
    )

    return WorkspaceLoadResponse(
        loaded=results,
        workspace_size_bytes=total_size,
    )


@router.post(
    "/projects/{project_id}/workspaces/{workspace_id}/credentials/reset",
    response_model=WorkspaceConnectionInfo,
    responses={
        200: {"description": "Credentials reset successfully"},
        404: {"model": ErrorResponse, "description": "Workspace not found"},
        410: {"model": ErrorResponse, "description": "Workspace expired"},
    },
    summary="Reset workspace credentials",
    description="""
    Reset workspace credentials (generate new password).

    **IMPORTANT**: The new password is returned ONLY once. Save it securely.
    Old credentials will be immediately invalidated.
    """,
)
async def reset_credentials(
    project_id: str,
    workspace_id: str,
    _auth: None = Depends(require_project_access),
) -> WorkspaceConnectionInfo:
    """Reset workspace credentials."""
    workspace = _validate_workspace_exists(project_id, workspace_id)
    _check_workspace_expired(workspace)

    # Get current credentials to keep username
    current_creds = metadata_db.get_workspace_credentials(workspace_id)
    if not current_creds:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "credentials_not_found",
                "message": f"Credentials not found for workspace {workspace_id}",
            },
        )

    # Generate new password
    password = _generate_password()
    password_hash = _hash_password(password)

    # Update credentials (keep same username)
    metadata_db.update_workspace_credentials(workspace_id, password_hash)

    # Log operation
    metadata_db.log_operation(
        operation="reset_workspace_credentials",
        status="success",
        project_id=project_id,
        resource_type="workspace",
        resource_id=workspace_id,
        details={},
    )

    logger.info(
        "workspace_credentials_reset",
        project_id=project_id,
        workspace_id=workspace_id,
    )

    # Return connection info with new password
    return WorkspaceConnectionInfo(
        host=settings.workspace_host if hasattr(settings, "workspace_host") else "localhost",
        port=settings.workspace_port if hasattr(settings, "workspace_port") else 5432,
        database=f"workspace_{workspace_id}",
        username=current_creds["username"],
        password=password,
        ssl_mode="prefer",
        connection_string=None,
    )


# ====================================================================================
# Branch Workspace Endpoints
# ====================================================================================


def _validate_branch_exists(project_id: str, branch_id: str) -> dict:
    """Validate that branch exists. Returns branch dict."""
    _validate_project_exists(project_id)
    branch = metadata_db.get_branch(branch_id)
    if not branch or branch.get("project_id") != project_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "branch_not_found",
                "message": f"Branch {branch_id} not found in project {project_id}",
                "details": {"project_id": project_id, "branch_id": branch_id},
            },
        )
    return branch


def _validate_branch_workspace_exists(
    project_id: str, branch_id: str, workspace_id: str
) -> dict:
    """Validate that workspace exists and belongs to the branch. Returns workspace dict."""
    _validate_branch_exists(project_id, branch_id)
    workspace = metadata_db.get_workspace_by_project(project_id, workspace_id)
    if not workspace or workspace.get("branch_id") != branch_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "workspace_not_found",
                "message": f"Workspace {workspace_id} not found in branch {branch_id}",
                "details": {
                    "project_id": project_id,
                    "branch_id": branch_id,
                    "workspace_id": workspace_id,
                },
            },
        )
    return workspace


@router.post(
    "/projects/{project_id}/branches/{branch_id}/workspaces",
    response_model=WorkspaceResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Branch workspace created successfully"},
        404: {"model": ErrorResponse, "description": "Project or branch not found"},
    },
    summary="Create a branch workspace",
    description="""
    Create a new workspace scoped to a development branch.

    Branch workspaces can access:
    - Branch-specific table data (modified from main)
    - Main branch tables (read-only fallback)

    **IMPORTANT**: The password is returned ONLY on creation. Save it securely.
    """,
)
async def create_branch_workspace(
    project_id: str,
    branch_id: str,
    request: WorkspaceCreateRequest,
    _auth: None = Depends(require_project_access),
) -> WorkspaceResponse:
    """Create a new workspace for a development branch."""
    _validate_branch_exists(project_id, branch_id)

    # Generate workspace ID
    workspace_id = f"ws_{str(uuid.uuid4())[:8]}"

    # Generate credentials
    username = _generate_username(workspace_id)
    password = _generate_password()
    password_hash = _hash_password(password)

    # Calculate expiration
    expires_at = None
    if request.ttl_hours:
        expires_at = datetime.now(timezone.utc) + timedelta(hours=request.ttl_hours)

    # Create workspace database file in branch workspace directory
    db_path = project_db_manager.create_workspace_db(
        project_id, workspace_id, branch_id=branch_id
    )

    # Calculate size limit in bytes
    size_limit_bytes = request.size_limit_gb * 1024 * 1024 * 1024

    # Create workspace metadata record
    workspace = metadata_db.create_workspace(
        workspace_id=workspace_id,
        project_id=project_id,
        name=request.name,
        db_path=str(db_path),
        branch_id=branch_id,
        expires_at=expires_at.isoformat() if expires_at else None,
        size_limit_bytes=size_limit_bytes,
    )

    # Create credentials
    metadata_db.create_workspace_credentials(workspace_id, username, password_hash)

    # Update metrics
    WORKSPACES_TOTAL.set(metadata_db.count_workspaces())

    # Log operation
    metadata_db.log_operation(
        operation="create_branch_workspace",
        status="success",
        project_id=project_id,
        resource_type="workspace",
        resource_id=workspace_id,
        details={
            "name": request.name,
            "branch_id": branch_id,
            "ttl_hours": request.ttl_hours,
            "expires_at": expires_at.isoformat() if expires_at else None,
        },
    )

    logger.info(
        "branch_workspace_created",
        project_id=project_id,
        branch_id=branch_id,
        workspace_id=workspace_id,
        name=request.name,
        ttl_hours=request.ttl_hours,
    )

    # Return response with password (only time it's returned)
    credentials = {"username": username, "password": password}
    return _workspace_to_response(workspace, credentials, include_password=True)


@router.get(
    "/projects/{project_id}/branches/{branch_id}/workspaces",
    response_model=WorkspaceListResponse,
    responses={
        200: {"description": "List of branch workspaces"},
        404: {"model": ErrorResponse, "description": "Project or branch not found"},
    },
    summary="List branch workspaces",
    description="List all workspaces for a development branch.",
)
async def list_branch_workspaces(
    project_id: str,
    branch_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    _auth: None = Depends(require_project_access),
) -> WorkspaceListResponse:
    """List all workspaces for a branch."""
    _validate_branch_exists(project_id, branch_id)

    workspaces = metadata_db.list_workspaces(
        project_id, branch_id=branch_id, limit=limit, offset=offset
    )

    # Get credentials for each workspace
    responses = []
    for ws in workspaces:
        creds = metadata_db.get_workspace_credentials(ws["id"])
        responses.append(_workspace_to_response(ws, creds, include_password=False))

    return WorkspaceListResponse(
        workspaces=responses,
        count=len(responses),
    )


@router.get(
    "/projects/{project_id}/branches/{branch_id}/workspaces/{workspace_id}",
    response_model=WorkspaceDetailResponse,
    responses={
        200: {"description": "Branch workspace details"},
        404: {"model": ErrorResponse, "description": "Workspace not found"},
        410: {"model": ErrorResponse, "description": "Workspace expired"},
    },
    summary="Get branch workspace details",
    description="Get detailed information about a branch workspace.",
)
async def get_branch_workspace(
    project_id: str,
    branch_id: str,
    workspace_id: str,
    _auth: None = Depends(require_project_access),
) -> WorkspaceDetailResponse:
    """Get branch workspace details."""
    workspace = _validate_branch_workspace_exists(project_id, branch_id, workspace_id)
    _check_workspace_expired(workspace)

    # Get credentials
    credentials = metadata_db.get_workspace_credentials(workspace_id)

    # Get workspace objects (tables, views)
    objects_raw = project_db_manager.list_workspace_objects(
        project_id, workspace_id, branch_id
    )
    objects = [
        WorkspaceObjectInfo(name=obj["name"], type=obj["type"], rows=obj["rows"])
        for obj in objects_raw
    ]

    # Get size
    size_bytes = project_db_manager.get_workspace_size(project_id, workspace_id, branch_id)

    # Check if expired
    status_val = workspace.get("status", "active")
    if workspace.get("expires_at"):
        expires_at = datetime.fromisoformat(workspace["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= datetime.now(timezone.utc):
            status_val = "expired"

    # Build connection info
    connection = None
    if credentials:
        connection = WorkspaceConnectionInfo(
            host=settings.workspace_host if hasattr(settings, "workspace_host") else "localhost",
            port=settings.workspace_port if hasattr(settings, "workspace_port") else 5432,
            database=f"workspace_{workspace['id']}",
            username=credentials.get("username", ""),
            password=None,  # Never return password on GET
            ssl_mode="prefer",
            connection_string=None,
        )

    # Get attached branch tables
    attached_tables = []
    # First get branch-specific tables
    branch_dir = project_db_manager.get_branch_dir(project_id, branch_id)
    if branch_dir.exists():
        for bucket_path in branch_dir.iterdir():
            if bucket_path.is_dir() and not bucket_path.name.startswith("_"):
                for table_file in bucket_path.glob("*.duckdb"):
                    attached_tables.append(
                        WorkspaceTableInfo(
                            schema=bucket_path.name,
                            table=table_file.stem,
                            rows=0,  # Would need to query for actual count
                        )
                    )

    # Also include main project tables
    buckets = project_db_manager.list_buckets(project_id)
    for bucket in buckets:
        bucket_name = bucket["name"]
        tables = project_db_manager.list_tables(project_id, bucket_name)
        for t in tables:
            # Only add if not already in branch tables
            if not any(
                at.schema == bucket_name and at.table == t["name"]
                for at in attached_tables
            ):
                attached_tables.append(
                    WorkspaceTableInfo(
                        schema=bucket_name, table=t["name"], rows=t.get("row_count", 0)
                    )
                )

    return WorkspaceDetailResponse(
        id=workspace["id"],
        name=workspace["name"],
        project_id=workspace["project_id"],
        branch_id=branch_id,
        created_at=workspace.get("created_at"),
        expires_at=workspace.get("expires_at"),
        size_bytes=size_bytes,
        size_limit_gb=workspace.get("size_limit_bytes", 10737418240) // (1024 * 1024 * 1024),
        status=status_val,
        active_sessions=0,
        connection=connection,
        attached_tables=attached_tables,
        workspace_objects=objects,
    )


@router.delete(
    "/projects/{project_id}/branches/{branch_id}/workspaces/{workspace_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Branch workspace deleted"},
        404: {"model": ErrorResponse, "description": "Workspace not found"},
    },
    summary="Delete branch workspace",
    description="Delete a branch workspace and its associated files and credentials.",
)
async def delete_branch_workspace(
    project_id: str,
    branch_id: str,
    workspace_id: str,
    _auth: None = Depends(require_project_access),
) -> None:
    """Delete a branch workspace."""
    _validate_branch_workspace_exists(project_id, branch_id, workspace_id)

    # Delete workspace database file
    project_db_manager.delete_workspace_db(project_id, workspace_id, branch_id)

    # Delete from metadata (also deletes credentials)
    metadata_db.delete_workspace(workspace_id)

    # Update metrics
    WORKSPACES_TOTAL.set(metadata_db.count_workspaces())

    # Log operation
    metadata_db.log_operation(
        operation="delete_branch_workspace",
        status="success",
        project_id=project_id,
        resource_type="workspace",
        resource_id=workspace_id,
        details={"branch_id": branch_id},
    )

    logger.info(
        "branch_workspace_deleted",
        project_id=project_id,
        branch_id=branch_id,
        workspace_id=workspace_id,
    )
