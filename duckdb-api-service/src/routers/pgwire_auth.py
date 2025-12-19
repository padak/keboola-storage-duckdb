"""PG Wire authentication bridge endpoints (Phase 11b).

Internal endpoints called by duckgres/pgwire server to:
- Authenticate workspace credentials
- Initialize sessions with workspace info
- Track and manage active sessions
- Cleanup stale sessions

These endpoints are NOT protected by standard API key auth - they use
a shared secret or are expected to be called only from internal services.
"""

import hashlib
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, HTTPException, Query, status

from src.config import settings
from src.database import metadata_db, project_db_manager
from src.models.responses import (
    ErrorResponse,
    PGWireAuthRequest,
    PGWireAuthResponse,
    PGWireSessionCreateRequest,
    PGWireSessionInfo,
    PGWireSessionUpdateRequest,
    PGWireTableInfo,
)

logger = structlog.get_logger()
router = APIRouter(prefix="/internal/pgwire", tags=["pgwire-internal"])


def _hash_password(password: str) -> str:
    """Hash password using SHA256 (same as workspace credentials)."""
    return hashlib.sha256(password.encode()).hexdigest()


def _is_workspace_expired(workspace: dict) -> bool:
    """Check if workspace is expired."""
    if not workspace.get("expires_at"):
        return False
    expires_at = datetime.fromisoformat(workspace["expires_at"])
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at <= datetime.now(timezone.utc)


def _list_project_tables(project_id: str, branch_id: str | None = None) -> list[PGWireTableInfo]:
    """List all tables in project for ATTACHing to workspace session."""
    tables = []

    # Get buckets from project_db_manager (not metadata_db)
    buckets = project_db_manager.list_buckets(project_id)

    for bucket in buckets:
        bucket_name = bucket["name"]
        # Get tables from project_db_manager
        bucket_tables = project_db_manager.list_tables(project_id, bucket_name)

        for table in bucket_tables:
            table_name = table["name"]
            # Get path to DuckDB file
            if branch_id:
                # For branch workspace, check if branch has its own copy
                branch_table_path = project_db_manager.get_branch_table_path(
                    project_id, branch_id, bucket_name, table_name
                )
                if branch_table_path.exists():
                    path = str(branch_table_path)
                else:
                    # Fall back to main branch table
                    path = str(project_db_manager.get_table_path(project_id, bucket_name, table_name))
            else:
                path = str(project_db_manager.get_table_path(project_id, bucket_name, table_name))

            tables.append(
                PGWireTableInfo(
                    bucket=bucket_name,
                    name=table_name,
                    path=path,
                    rows=table.get("row_count", 0),
                )
            )

    return tables


@router.post(
    "/auth",
    response_model=PGWireAuthResponse,
    responses={
        401: {"model": ErrorResponse, "description": "Invalid credentials"},
        410: {"model": ErrorResponse, "description": "Workspace expired"},
        429: {"model": ErrorResponse, "description": "Connection limit reached"},
    },
    summary="Authenticate PG Wire session",
    description="""
    Called by duckgres to validate workspace credentials.
    Returns workspace info for session initialization including:
    - Path to workspace DuckDB file
    - List of project tables to ATTACH (read-only)
    - Resource limits for the session
    """,
)
async def authenticate_pgwire_session(request: PGWireAuthRequest) -> PGWireAuthResponse:
    """Authenticate workspace credentials and return session info."""
    logger.info(
        "pgwire_auth_attempt",
        username=request.username,
        client_ip=request.client_ip,
    )

    # Look up workspace by username
    workspace = metadata_db.get_workspace_by_username(request.username)

    if not workspace:
        logger.warning(
            "pgwire_auth_failed",
            reason="user_not_found",
            username=request.username,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": "invalid_credentials",
                "message": "Invalid username or password",
            },
        )

    # Verify password
    password_hash = _hash_password(request.password)
    if password_hash != workspace.get("password_hash"):
        logger.warning(
            "pgwire_auth_failed",
            reason="invalid_password",
            username=request.username,
            workspace_id=workspace["id"],
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": "invalid_credentials",
                "message": "Invalid username or password",
            },
        )

    # Check if workspace is expired
    if _is_workspace_expired(workspace):
        logger.warning(
            "pgwire_auth_failed",
            reason="workspace_expired",
            workspace_id=workspace["id"],
            expired_at=workspace.get("expires_at"),
        )
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail={
                "error": "workspace_expired",
                "message": f"Workspace {workspace['id']} has expired",
                "details": {
                    "workspace_id": workspace["id"],
                    "expired_at": workspace.get("expires_at"),
                },
            },
        )

    # Check workspace status
    if workspace.get("status") != "active":
        logger.warning(
            "pgwire_auth_failed",
            reason="workspace_not_active",
            workspace_id=workspace["id"],
            status=workspace.get("status"),
        )
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail={
                "error": "workspace_not_active",
                "message": f"Workspace {workspace['id']} is not active",
                "details": {
                    "workspace_id": workspace["id"],
                    "status": workspace.get("status"),
                },
            },
        )

    # Check connection limit per workspace
    active_sessions = metadata_db.count_active_pgwire_sessions(workspace["id"])
    if active_sessions >= settings.pgwire_max_connections_per_workspace:
        logger.warning(
            "pgwire_auth_failed",
            reason="connection_limit",
            workspace_id=workspace["id"],
            active_sessions=active_sessions,
            limit=settings.pgwire_max_connections_per_workspace,
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "connection_limit_reached",
                "message": f"Maximum connections ({settings.pgwire_max_connections_per_workspace}) reached for workspace",
                "details": {
                    "workspace_id": workspace["id"],
                    "active_sessions": active_sessions,
                    "limit": settings.pgwire_max_connections_per_workspace,
                },
            },
        )

    # Get list of project tables to ATTACH
    tables = _list_project_tables(workspace["project_id"], workspace.get("branch_id"))

    logger.info(
        "pgwire_auth_success",
        workspace_id=workspace["id"],
        project_id=workspace["project_id"],
        branch_id=workspace.get("branch_id"),
        tables_count=len(tables),
    )

    return PGWireAuthResponse(
        workspace_id=workspace["id"],
        project_id=workspace["project_id"],
        branch_id=workspace.get("branch_id"),
        db_path=workspace["db_path"],
        tables=tables,
        memory_limit=settings.pgwire_session_memory_limit,
        query_timeout_seconds=settings.pgwire_query_timeout_seconds,
    )


@router.post(
    "/sessions",
    response_model=PGWireSessionInfo,
    status_code=status.HTTP_201_CREATED,
    summary="Register new session",
    description="Called by duckgres after successful auth to register session.",
)
async def create_session(request: PGWireSessionCreateRequest) -> PGWireSessionInfo:
    """Register a new PG Wire session."""
    # Verify workspace exists
    workspace = metadata_db.get_workspace(request.workspace_id)
    if not workspace:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "workspace_not_found",
                "message": f"Workspace {request.workspace_id} not found",
            },
        )

    # Create session
    session = metadata_db.create_pgwire_session(
        session_id=request.session_id,
        workspace_id=request.workspace_id,
        client_ip=request.client_ip,
    )

    logger.info(
        "pgwire_session_created",
        session_id=request.session_id,
        workspace_id=request.workspace_id,
        client_ip=request.client_ip,
    )

    return PGWireSessionInfo(**session)


@router.get(
    "/sessions/{session_id}",
    response_model=PGWireSessionInfo,
    responses={404: {"model": ErrorResponse}},
    summary="Get session info",
)
async def get_session(session_id: str) -> PGWireSessionInfo:
    """Get information about a session."""
    session = metadata_db.get_pgwire_session(session_id)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "session_not_found",
                "message": f"Session {session_id} not found",
            },
        )
    return PGWireSessionInfo(**session)


@router.patch(
    "/sessions/{session_id}/activity",
    response_model=PGWireSessionInfo,
    responses={404: {"model": ErrorResponse}},
    summary="Update session activity",
    description="Called by duckgres to update last activity time and query count.",
)
async def update_session_activity(
    session_id: str, request: PGWireSessionUpdateRequest
) -> PGWireSessionInfo:
    """Update session activity timestamp and optionally increment query count."""
    session = metadata_db.get_pgwire_session(session_id)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "session_not_found",
                "message": f"Session {session_id} not found",
            },
        )

    metadata_db.update_pgwire_session_activity(
        session_id, increment_queries=request.increment_queries
    )

    # Return updated session
    updated_session = metadata_db.get_pgwire_session(session_id)
    return PGWireSessionInfo(**updated_session)


@router.delete(
    "/sessions/{session_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Close session",
    description="Called by duckgres when connection is closed.",
)
async def close_session(session_id: str, reason: str = Query(default="disconnected")):
    """Close a PG Wire session."""
    session = metadata_db.get_pgwire_session(session_id)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "session_not_found",
                "message": f"Session {session_id} not found",
            },
        )

    metadata_db.close_pgwire_session(session_id, status=reason)

    logger.info(
        "pgwire_session_closed",
        session_id=session_id,
        workspace_id=session["workspace_id"],
        reason=reason,
        query_count=session.get("query_count", 0),
    )


@router.get(
    "/sessions",
    response_model=list[PGWireSessionInfo],
    summary="List sessions",
    description="List all sessions, optionally filtered by workspace or status.",
)
async def list_sessions(
    workspace_id: str | None = Query(default=None),
    session_status: str | None = Query(default=None, alias="status"),
) -> list[PGWireSessionInfo]:
    """List PG Wire sessions."""
    sessions = metadata_db.list_pgwire_sessions(
        workspace_id=workspace_id, status=session_status
    )
    return [PGWireSessionInfo(**s) for s in sessions]


@router.post(
    "/sessions/cleanup",
    summary="Cleanup stale sessions",
    description="Mark sessions as timeout if idle for too long.",
)
async def cleanup_stale_sessions(
    idle_timeout_seconds: int = Query(default=None),
) -> dict:
    """Cleanup stale sessions based on idle timeout."""
    timeout = idle_timeout_seconds or settings.pgwire_idle_timeout_seconds
    count = metadata_db.cleanup_stale_pgwire_sessions(timeout)

    logger.info(
        "pgwire_sessions_cleanup",
        timeout_seconds=timeout,
        sessions_marked=count,
    )

    return {
        "cleaned_up": count,
        "idle_timeout_seconds": timeout,
    }
