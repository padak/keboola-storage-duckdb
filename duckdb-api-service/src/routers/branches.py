"""Dev branches management endpoints (ADR-007: CoW branching).

Dev branches provide isolated development environments with Copy-on-Write semantics:
- Live View: Branch sees current main data until table is modified
- Copy-on-Write: First write to table copies it to branch
- No table merge: Merge = only configurations, branch tables are deleted
"""

import time
import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.config import settings
from src.database import metadata_db, project_db_manager
from src.dependencies import require_project_access
from src.metrics import (
    BRANCHES_TOTAL,
    BRANCH_COW_OPERATIONS,
    BRANCH_COW_DURATION,
    BRANCH_COW_SIZE_BYTES,
    BRANCH_TABLES_TOTAL,
)
from src.models.responses import (
    BranchCreateRequest,
    BranchDetailResponse,
    BranchListResponse,
    BranchResponse,
    BranchTableInfo,
    ErrorResponse,
    PullTableResponse,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["branches"])


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


def _validate_branch_exists(project_id: str, branch_id: str) -> dict:
    """Validate that branch exists and belongs to project. Returns branch dict."""
    _validate_project_exists(project_id)
    branch = metadata_db.get_branch_by_project(project_id, branch_id)
    if not branch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "branch_not_found",
                "message": f"Branch {branch_id} not found in project {project_id}",
                "details": {"project_id": project_id, "branch_id": branch_id},
            },
        )
    return branch


def _branch_to_response(branch: dict, include_stats: bool = True) -> BranchResponse:
    """Convert branch dict to response model."""
    stats = {"table_count": 0, "size_bytes": 0}
    if include_stats:
        stats = project_db_manager.get_branch_stats(
            branch["project_id"], branch["id"]
        )

    return BranchResponse(
        id=branch["id"],
        project_id=branch["project_id"],
        name=branch["name"],
        created_at=branch["created_at"],
        created_by=branch.get("created_by"),
        description=branch.get("description"),
        table_count=stats.get("table_count", 0),
        size_bytes=stats.get("size_bytes", 0),
    )


@router.post(
    "/projects/{project_id}/branches",
    response_model=BranchResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Branch created successfully"},
        404: {"model": ErrorResponse, "description": "Project not found"},
        409: {"model": ErrorResponse, "description": "Branch name already exists"},
    },
    summary="Create a dev branch",
    description="""
    Create a new dev branch for isolated development.

    ADR-007 behavior:
    - Branch starts empty (no data copied)
    - Reads from branch return main data (live view)
    - First write to a table triggers Copy-on-Write
    - Branch tables are isolated after CoW
    """,
)
async def create_branch(
    project_id: str,
    request: BranchCreateRequest,
    _auth: None = Depends(require_project_access),
) -> BranchResponse:
    """Create a new dev branch."""
    _validate_project_exists(project_id)

    # Check if branch name already exists
    existing_branches = metadata_db.list_branches(project_id)
    for branch in existing_branches:
        if branch["name"] == request.name:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "branch_name_exists",
                    "message": f"Branch with name '{request.name}' already exists",
                    "details": {"project_id": project_id, "name": request.name},
                },
            )

    # Generate branch ID
    branch_id = str(uuid.uuid4())[:8]  # Short ID for readability

    # Create branch directory
    project_db_manager.create_branch_db(project_id, branch_id)

    # Create branch metadata record
    branch = metadata_db.create_branch(
        branch_id=branch_id,
        project_id=project_id,
        name=request.name,
        description=request.description,
    )

    # Update metrics
    BRANCHES_TOTAL.set(metadata_db.count_branches())

    # Log operation
    metadata_db.log_operation(
        operation="create_branch",
        status="success",
        project_id=project_id,
        resource_type="branch",
        resource_id=branch_id,
        details={"name": request.name},
    )

    logger.info(
        "branch_created",
        project_id=project_id,
        branch_id=branch_id,
        name=request.name,
    )

    return _branch_to_response(branch)


@router.get(
    "/projects/{project_id}/branches",
    response_model=BranchListResponse,
    responses={
        200: {"description": "List of branches"},
        404: {"model": ErrorResponse, "description": "Project not found"},
    },
    summary="List dev branches",
    description="List all dev branches for a project.",
)
async def list_branches(
    project_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    _auth: None = Depends(require_project_access),
) -> BranchListResponse:
    """List all branches for a project."""
    _validate_project_exists(project_id)

    branches = metadata_db.list_branches(project_id, limit=limit, offset=offset)

    return BranchListResponse(
        branches=[_branch_to_response(b) for b in branches],
        count=len(branches),
    )


@router.get(
    "/projects/{project_id}/branches/{branch_id}",
    response_model=BranchDetailResponse,
    responses={
        200: {"description": "Branch details"},
        404: {"model": ErrorResponse, "description": "Branch not found"},
    },
    summary="Get branch details",
    description="Get detailed information about a dev branch including copied tables list.",
)
async def get_branch(
    project_id: str,
    branch_id: str,
    _auth: None = Depends(require_project_access),
) -> BranchDetailResponse:
    """Get branch details."""
    branch = _validate_branch_exists(project_id, branch_id)

    # Get list of copied tables
    copied_tables = metadata_db.get_branch_tables(branch_id)

    # Get stats
    stats = project_db_manager.get_branch_stats(project_id, branch_id)

    return BranchDetailResponse(
        id=branch["id"],
        project_id=branch["project_id"],
        name=branch["name"],
        created_at=branch["created_at"],
        created_by=branch.get("created_by"),
        description=branch.get("description"),
        table_count=stats.get("table_count", 0),
        size_bytes=stats.get("size_bytes", 0),
        copied_tables=copied_tables,
    )


@router.delete(
    "/projects/{project_id}/branches/{branch_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Branch deleted"},
        404: {"model": ErrorResponse, "description": "Branch not found"},
    },
    summary="Delete a dev branch",
    description="""
    Delete a dev branch and all its data.

    WARNING: All tables copied to this branch will be permanently deleted.
    This operation cannot be undone.
    """,
)
async def delete_branch(
    project_id: str,
    branch_id: str,
    _auth: None = Depends(require_project_access),
) -> None:
    """Delete a branch."""
    branch = _validate_branch_exists(project_id, branch_id)

    # Get list of tables that will be deleted (for logging)
    copied_tables = metadata_db.get_branch_tables(branch_id)

    # Delete branch directory and all contents
    project_db_manager.delete_branch_db(project_id, branch_id)

    # Delete branch metadata (cascades to branch_tables)
    metadata_db.delete_branch(branch_id)

    # Update metrics
    BRANCHES_TOTAL.set(metadata_db.count_branches())

    # Log operation
    metadata_db.log_operation(
        operation="delete_branch",
        status="success",
        project_id=project_id,
        resource_type="branch",
        resource_id=branch_id,
        details={
            "name": branch["name"],
            "tables_deleted": len(copied_tables),
        },
    )

    logger.info(
        "branch_deleted",
        project_id=project_id,
        branch_id=branch_id,
        name=branch["name"],
        tables_deleted=len(copied_tables),
    )


@router.post(
    "/projects/{project_id}/branches/{branch_id}/tables/{bucket_name}/{table_name}/pull",
    response_model=PullTableResponse,
    responses={
        200: {"description": "Table pulled successfully"},
        404: {"model": ErrorResponse, "description": "Branch or table not found"},
    },
    summary="Pull table from main",
    description="""
    Pull (refresh) a table from main, restoring live view.

    This operation:
    1. Deletes the branch copy of the table (if exists)
    2. Future reads will return current main data (live view)
    3. Future writes will trigger a new Copy-on-Write

    Use this to discard branch changes and sync with main.
    """,
)
async def pull_table(
    project_id: str,
    branch_id: str,
    bucket_name: str,
    table_name: str,
    _auth: None = Depends(require_project_access),
) -> PullTableResponse:
    """Pull table from main, restoring live view."""
    _validate_branch_exists(project_id, branch_id)

    # Check if table exists in main
    if not project_db_manager.table_exists(project_id, bucket_name, table_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "table_not_found",
                "message": f"Table {bucket_name}.{table_name} not found in main project",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )

    # Check if table was in branch
    was_local = metadata_db.is_table_in_branch(branch_id, bucket_name, table_name)

    if was_local:
        # Delete table from branch (filesystem)
        project_db_manager.delete_table_from_branch(
            project_id, branch_id, bucket_name, table_name
        )

        # Remove from branch_tables tracking
        metadata_db.remove_table_from_branch(branch_id, bucket_name, table_name)

        # Update metrics
        BRANCH_TABLES_TOTAL.set(_count_total_branch_tables())

    # Log operation
    metadata_db.log_operation(
        operation="pull_table",
        status="success",
        project_id=project_id,
        resource_type="branch_table",
        resource_id=f"{branch_id}/{bucket_name}/{table_name}",
        details={"was_local": was_local},
    )

    logger.info(
        "branch_table_pulled",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table_name,
        was_local=was_local,
    )

    return PullTableResponse(
        bucket_name=bucket_name,
        table_name=table_name,
        message="Table now reads from main (live view)" if was_local else "Table was already reading from main",
        was_local=was_local,
    )


def _count_total_branch_tables() -> int:
    """Count total number of tables copied to all branches."""
    total = 0
    # Get all projects
    projects = metadata_db.list_projects(limit=10000)
    for project in projects:
        branches = metadata_db.list_branches(project["id"])
        for branch in branches:
            tables = metadata_db.get_branch_tables(branch["id"])
            total += len(tables)
    return total


# =============================================================================
# Helper function for CoW (called from table operations)
# =============================================================================


def ensure_table_in_branch(
    project_id: str,
    branch_id: str,
    bucket_name: str,
    table_name: str,
) -> bool:
    """
    Ensure table exists in branch, copying from main if needed (CoW).

    Returns True if CoW was performed, False if table was already in branch.

    This function should be called before any write operation to a branch table.
    """
    # Check if already in branch
    if metadata_db.is_table_in_branch(branch_id, bucket_name, table_name):
        return False

    # Check if table exists in main
    if not project_db_manager.table_exists(project_id, bucket_name, table_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "table_not_found",
                "message": f"Table {bucket_name}.{table_name} not found in main project",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )

    # Perform CoW
    start_time = time.perf_counter()

    target_path = project_db_manager.copy_table_to_branch(
        project_id, branch_id, bucket_name, table_name
    )

    cow_duration = time.perf_counter() - start_time
    size_bytes = target_path.stat().st_size

    # Record in metadata
    metadata_db.mark_table_copied_to_branch(branch_id, bucket_name, table_name)

    # Update metrics
    BRANCH_COW_OPERATIONS.labels(project_id=project_id, branch_id=branch_id).inc()
    BRANCH_COW_DURATION.observe(cow_duration)
    BRANCH_COW_SIZE_BYTES.labels(project_id=project_id, branch_id=branch_id).inc(size_bytes)
    BRANCH_TABLES_TOTAL.set(_count_total_branch_tables())

    logger.info(
        "cow_performed",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table_name,
        duration_seconds=cow_duration,
        size_bytes=size_bytes,
    )

    return True
