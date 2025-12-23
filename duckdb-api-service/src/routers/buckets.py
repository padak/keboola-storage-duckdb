"""Bucket management endpoints: CRUD operations for buckets (schemas) within projects.

ADR-012: Branch-First API
==========================
All bucket operations are scoped to a branch via /projects/{id}/branches/{branch_id}/buckets.

Branch ID:
- "default" = main (production) project
- {uuid} = dev branch

Note: Buckets are always defined in main project. Dev branches share buckets with main.
Creating/deleting buckets always operates on main, regardless of branch_id.
"""

import time
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.branch_utils import (
    resolve_branch,
    require_default_branch,
    validate_project_db_exists,
    validate_bucket_exists,
)
from src.database import metadata_db, project_db_manager
from src.dependencies import require_project_access
from src.models.responses import (
    BucketCreate,
    BucketListResponse,
    BucketResponse,
    ErrorResponse,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["buckets"])


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        import structlog

        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


@router.post(
    "/projects/{project_id}/branches/{branch_id}/buckets",
    response_model=BucketResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Create bucket",
    description="""
    Create a new bucket (schema) in a project's DuckDB database.

    Note: Buckets are always created in main project (shared across branches).
    For dev branches, this creates the bucket in main.
    """,
    dependencies=[Depends(require_project_access)],
)
async def create_bucket(
    project_id: str, branch_id: str, bucket: BucketCreate
) -> BucketResponse:
    """
    Create a new bucket in a project.

    This endpoint:
    1. Verifies the project and branch exist
    2. Creates a schema in the project's main DuckDB (buckets are shared)
    3. Updates project metadata
    4. Logs the operation
    """
    start_time = time.time()
    request_id = _get_request_id()

    # Resolve branch (validates project and branch exist)
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    logger.info(
        "create_bucket_start",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket.name,
        request_id=request_id,
    )

    # Check if project DB exists
    validate_project_db_exists(resolved_project_id)

    # Always create in main project (buckets are shared)
    # Check if bucket already exists
    if project_db_manager.bucket_exists(resolved_project_id, bucket.name):
        logger.warning(
            "create_bucket_conflict",
            project_id=resolved_project_id,
            bucket_name=bucket.name,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bucket_exists",
                "message": f"Bucket {bucket.name} already exists in project {project_id}",
                "details": {"project_id": project_id, "bucket_name": bucket.name},
            },
        )

    try:
        # Create bucket (schema) in project's main DuckDB
        bucket_data = project_db_manager.create_bucket(
            project_id=resolved_project_id,
            bucket_name=bucket.name,
            description=bucket.description,
        )

        # Update project metadata with new bucket count
        stats = project_db_manager.get_project_stats(resolved_project_id)
        metadata_db.update_project(
            project_id=resolved_project_id,
            bucket_count=stats["bucket_count"],
            table_count=stats["table_count"],
            size_bytes=stats["size_bytes"],
        )

        duration_ms = int((time.time() - start_time) * 1000)

        # Log operation
        metadata_db.log_operation(
            operation="create_bucket",
            status="success",
            project_id=resolved_project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket.name,
            details={
                "bucket_name": bucket.name,
                "description": bucket.description,
                "branch_id": branch_id,
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "create_bucket_success",
            project_id=resolved_project_id,
            bucket_name=bucket.name,
            duration_ms=duration_ms,
        )

        return BucketResponse(**bucket_data)

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        # Log failed operation
        metadata_db.log_operation(
            operation="create_bucket",
            status="failed",
            project_id=resolved_project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket.name,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "create_bucket_failed",
            project_id=resolved_project_id,
            bucket_name=bucket.name,
            error=str(e),
            duration_ms=duration_ms,
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "create_failed",
                "message": f"Failed to create bucket: {e}",
                "details": {"project_id": project_id, "bucket_name": bucket.name},
            },
        )


@router.get(
    "/projects/{project_id}/branches/{branch_id}/buckets",
    response_model=BucketListResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="List buckets",
    description="""
    List all buckets visible in branch context.

    Note: Buckets are shared across branches, so this always returns
    buckets from main project regardless of branch_id.
    Includes both owned buckets and linked buckets from other projects.
    """,
    dependencies=[Depends(require_project_access)],
)
async def list_buckets(project_id: str, branch_id: str) -> BucketListResponse:
    """List all buckets in a project (always from main, buckets are shared)."""
    # Resolve branch (validates project and branch exist)
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    logger.info(
        "list_buckets",
        project_id=project_id,
        branch_id=branch_id,
    )

    # Check if project DB exists
    validate_project_db_exists(resolved_project_id)

    try:
        # List owned buckets from main (buckets are shared)
        buckets = project_db_manager.list_buckets(resolved_project_id)
        bucket_responses = [BucketResponse(**b) for b in buckets]

        # List linked buckets
        linked_buckets = metadata_db.list_bucket_links(resolved_project_id)
        for link in linked_buckets:
            # Get source bucket info for table count
            source_bucket = project_db_manager.get_bucket(
                link["source_project_id"], link["source_bucket_name"]
            )
            table_count = source_bucket.get("table_count", 0) if source_bucket else 0

            bucket_responses.append(
                BucketResponse(
                    name=link["target_bucket_name"],
                    table_count=table_count,
                    description=f"Linked from {link['source_project_id']}.{link['source_bucket_name']}",
                    is_linked=True,
                    source_project_id=link["source_project_id"],
                    source_bucket_name=link["source_bucket_name"],
                )
            )

        return BucketListResponse(
            buckets=bucket_responses,
            total=len(bucket_responses),
        )

    except Exception as e:
        logger.error(
            "list_buckets_failed",
            project_id=resolved_project_id,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "list_failed",
                "message": f"Failed to list buckets: {e}",
                "details": {"project_id": project_id},
            },
        )


@router.get(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}",
    response_model=BucketResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get bucket",
    description="Get information about a specific bucket.",
    dependencies=[Depends(require_project_access)],
)
async def get_bucket(
    project_id: str, branch_id: str, bucket_name: str
) -> BucketResponse:
    """Get bucket information (always from main, buckets are shared)."""
    # Resolve branch (validates project and branch exist)
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    logger.info(
        "get_bucket",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
    )

    # Check if project DB exists
    validate_project_db_exists(resolved_project_id)

    # Get bucket from main
    bucket = project_db_manager.get_bucket(resolved_project_id, bucket_name)
    if bucket:
        return BucketResponse(**bucket)

    # Check if this is a linked bucket
    link = metadata_db.get_bucket_link(resolved_project_id, bucket_name)
    if link:
        # Get source bucket info
        source_bucket = project_db_manager.get_bucket(
            link["source_project_id"], link["source_bucket_name"]
        )
        if source_bucket:
            # Return source bucket info with linked metadata
            return BucketResponse(
                name=bucket_name,
                table_count=source_bucket.get("table_count", 0),
                description=f"Linked from {link['source_project_id']}.{link['source_bucket_name']}",
                is_linked=True,
                source_project_id=link["source_project_id"],
                source_bucket_name=link["source_bucket_name"],
            )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={
            "error": "bucket_not_found",
            "message": f"Bucket {bucket_name} not found in project {project_id}",
            "details": {"project_id": project_id, "bucket_name": bucket_name},
        },
    )


@router.delete(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Delete bucket",
    description="""
    Delete a bucket and optionally all its tables.

    Note: Buckets can only be deleted from the default branch (main).
    Dev branches share buckets with main, so deleting from a branch is not allowed.
    """,
    dependencies=[Depends(require_project_access)],
)
async def delete_bucket(
    project_id: str,
    branch_id: str,
    bucket_name: str,
    cascade: bool = Query(default=True, description="Drop all tables in the bucket"),
) -> None:
    """
    Delete a bucket.

    This:
    1. Verifies the project exists and operation is on default branch
    2. Deletes the schema (and optionally all tables if cascade=True)
    3. Updates project metadata
    4. Logs the operation
    """
    start_time = time.time()
    request_id = _get_request_id()

    # Resolve branch (validates project and branch exist)
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    # Only allow bucket deletion on default/main branch
    require_default_branch(resolved_branch_id, "delete buckets")

    logger.info(
        "delete_bucket",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        cascade=cascade,
    )

    # Check if project DB exists and bucket exists
    validate_project_db_exists(resolved_project_id)
    validate_bucket_exists(resolved_project_id, bucket_name)

    try:
        # Delete bucket
        project_db_manager.delete_bucket(
            project_id=resolved_project_id,
            bucket_name=bucket_name,
            cascade=cascade,
        )

        # Update project metadata with new bucket count
        stats = project_db_manager.get_project_stats(resolved_project_id)
        metadata_db.update_project(
            project_id=resolved_project_id,
            bucket_count=stats["bucket_count"],
            table_count=stats["table_count"],
            size_bytes=stats["size_bytes"],
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="delete_bucket",
            status="success",
            project_id=resolved_project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            details={
                "bucket_name": bucket_name,
                "cascade": cascade,
                "branch_id": branch_id,
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "delete_bucket_success",
            project_id=resolved_project_id,
            bucket_name=bucket_name,
            duration_ms=duration_ms,
        )

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="delete_bucket",
            status="failed",
            project_id=resolved_project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "delete_bucket_failed",
            project_id=resolved_project_id,
            bucket_name=bucket_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "delete_failed",
                "message": f"Failed to delete bucket: {e}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )
