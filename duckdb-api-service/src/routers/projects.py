"""Project management endpoints: CRUD operations for Keboola projects."""

import time
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query, status

from src.database import metadata_db, project_db_manager
from src.models.responses import (
    ErrorResponse,
    ProjectCreate,
    ProjectListResponse,
    ProjectResponse,
    ProjectStatsResponse,
    ProjectUpdate,
)

logger = structlog.get_logger()
router = APIRouter(prefix="/projects", tags=["projects"])


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        import structlog
        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


@router.post(
    "",
    response_model=ProjectResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Create project",
    description="Create a new Keboola project with its DuckDB database file.",
)
async def create_project(project: ProjectCreate) -> ProjectResponse:
    """
    Create a new project.

    This endpoint:
    1. Registers project in metadata database
    2. Creates the DuckDB database file
    3. Returns project information

    Equivalent to BigQuery's CreateProjectHandler but simpler:
    - No GCP project creation
    - No service account setup
    - No billing account linkage
    - Just create a .duckdb file
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "create_project_start",
        project_id=project.id,
        name=project.name,
        request_id=request_id,
    )

    # Check if project already exists
    existing = metadata_db.get_project(project.id)
    if existing:
        logger.warning("create_project_conflict", project_id=project.id)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "project_exists",
                "message": f"Project {project.id} already exists",
                "details": {"project_id": project.id, "status": existing["status"]},
            },
        )

    try:
        # 1. Register in metadata DB
        project_data = metadata_db.create_project(
            project_id=project.id,
            name=project.name,
            settings_json=project.settings,
        )

        # 2. Create DuckDB file
        db_path = project_db_manager.create_project_db(project.id)

        # 3. Update metadata with file info
        size_bytes = project_db_manager.get_db_size(project.id)
        project_data = metadata_db.update_project(
            project_id=project.id,
            size_bytes=size_bytes,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        # Log operation
        metadata_db.log_operation(
            operation="create_project",
            status="success",
            project_id=project.id,
            request_id=request_id,
            resource_type="project",
            resource_id=project.id,
            details={"name": project.name, "db_path": str(db_path)},
            duration_ms=duration_ms,
        )

        logger.info(
            "create_project_success",
            project_id=project.id,
            db_path=str(db_path),
            duration_ms=duration_ms,
        )

        return ProjectResponse(**project_data)

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        # Log failed operation
        metadata_db.log_operation(
            operation="create_project",
            status="failed",
            project_id=project.id,
            request_id=request_id,
            resource_type="project",
            resource_id=project.id,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "create_project_failed",
            project_id=project.id,
            error=str(e),
            duration_ms=duration_ms,
            exc_info=True,
        )

        # Cleanup: try to remove metadata if DB creation failed
        try:
            metadata_db.hard_delete_project(project.id)
        except Exception:
            pass

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "create_failed",
                "message": f"Failed to create project: {e}",
                "details": {"project_id": project.id},
            },
        )


@router.get(
    "",
    response_model=ProjectListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="List projects",
    description="List all projects with optional filtering by status.",
)
async def list_projects(
    status_filter: str | None = Query(
        default=None,
        alias="status",
        description="Filter by status: active, deleted",
    ),
    limit: int = Query(default=100, ge=1, le=1000, description="Max results"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination"),
) -> ProjectListResponse:
    """List projects with optional filtering."""
    logger.info(
        "list_projects",
        status=status_filter,
        limit=limit,
        offset=offset,
    )

    projects = metadata_db.list_projects(
        status=status_filter,
        limit=limit,
        offset=offset,
    )

    # Get total count (without limit/offset)
    if status_filter:
        total_result = metadata_db.execute_one(
            "SELECT COUNT(*) FROM projects WHERE status = ?", [status_filter]
        )
    else:
        total_result = metadata_db.execute_one("SELECT COUNT(*) FROM projects")

    total = total_result[0] if total_result else 0

    return ProjectListResponse(
        projects=[ProjectResponse(**p) for p in projects],
        total=total,
    )


@router.get(
    "/{project_id}",
    response_model=ProjectResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get project",
    description="Get information about a specific project.",
)
async def get_project(project_id: str) -> ProjectResponse:
    """Get project by ID."""
    logger.info("get_project", project_id=project_id)

    project = metadata_db.get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"Project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    return ProjectResponse(**project)


@router.get(
    "/{project_id}/stats",
    response_model=ProjectStatsResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get project statistics",
    description="Get live statistics from the project's DuckDB database.",
)
async def get_project_stats(project_id: str) -> ProjectStatsResponse:
    """
    Get live statistics about a project.

    This queries the actual DuckDB file for current stats
    and updates the metadata database.
    """
    logger.info("get_project_stats", project_id=project_id)

    # Check project exists in metadata
    project = metadata_db.get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"Project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    # Check DB file exists
    if not project_db_manager.project_exists(project_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "db_not_found",
                "message": f"Database file for project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    # Get live stats from DuckDB
    stats = project_db_manager.get_project_stats(project_id)

    # Update metadata with fresh stats
    metadata_db.update_project(
        project_id=project_id,
        size_bytes=stats["size_bytes"],
        table_count=stats["table_count"],
        bucket_count=stats["bucket_count"],
    )

    return ProjectStatsResponse(
        id=project_id,
        size_bytes=stats["size_bytes"],
        table_count=stats["table_count"],
        bucket_count=stats["bucket_count"],
    )


@router.put(
    "/{project_id}",
    response_model=ProjectResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Update project",
    description="Update project metadata (name, settings).",
)
async def update_project(project_id: str, update: ProjectUpdate) -> ProjectResponse:
    """Update project metadata."""
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "update_project",
        project_id=project_id,
        name=update.name,
    )

    # Check project exists
    project = metadata_db.get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"Project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    # Update
    updated = metadata_db.update_project(
        project_id=project_id,
        name=update.name,
    )

    duration_ms = int((time.time() - start_time) * 1000)

    metadata_db.log_operation(
        operation="update_project",
        status="success",
        project_id=project_id,
        request_id=request_id,
        resource_type="project",
        resource_id=project_id,
        details={"name": update.name},
        duration_ms=duration_ms,
    )

    return ProjectResponse(**updated)


@router.delete(
    "/{project_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Delete project",
    description="Delete a project and its DuckDB database file.",
)
async def delete_project(project_id: str) -> None:
    """
    Delete a project.

    This:
    1. Marks project as 'deleted' in metadata
    2. Deletes the DuckDB database file
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info("delete_project", project_id=project_id)

    # Check project exists
    project = metadata_db.get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"Project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    try:
        # 1. Delete DB file
        project_db_manager.delete_project_db(project_id)

        # 2. Mark as deleted in metadata (soft delete)
        metadata_db.delete_project(project_id)

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="delete_project",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="project",
            resource_id=project_id,
            duration_ms=duration_ms,
        )

        logger.info(
            "delete_project_success",
            project_id=project_id,
            duration_ms=duration_ms,
        )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="delete_project",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="project",
            resource_id=project_id,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "delete_project_failed",
            project_id=project_id,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "delete_failed",
                "message": f"Failed to delete project: {e}",
                "details": {"project_id": project_id},
            },
        )
