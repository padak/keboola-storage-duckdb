"""Bucket management endpoints: CRUD operations for buckets (schemas) within projects."""

import time
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query, status

from src.database import metadata_db, project_db_manager
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
    "/projects/{project_id}/buckets",
    response_model=BucketResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Create bucket",
    description="Create a new bucket (schema) in a project's DuckDB database.",
)
async def create_bucket(project_id: str, bucket: BucketCreate) -> BucketResponse:
    """
    Create a new bucket in a project.

    This endpoint:
    1. Verifies the project exists
    2. Creates a schema in the project's DuckDB file
    3. Updates project metadata
    4. Logs the operation

    Equivalent to BigQuery's CreateDatasetHandler but simpler:
    - No GCP dataset creation
    - No IAM permissions setup
    - Just create a DuckDB schema
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "create_bucket_start",
        project_id=project_id,
        bucket_name=bucket.name,
        request_id=request_id,
    )

    # Check if project exists
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

    # Check if project DB exists
    if not project_db_manager.project_exists(project_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_db_not_found",
                "message": f"Database file for project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    # Check if bucket already exists
    if project_db_manager.bucket_exists(project_id, bucket.name):
        logger.warning("create_bucket_conflict", project_id=project_id, bucket_name=bucket.name)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bucket_exists",
                "message": f"Bucket {bucket.name} already exists in project {project_id}",
                "details": {"project_id": project_id, "bucket_name": bucket.name},
            },
        )

    try:
        # Create bucket (schema) in project's DuckDB
        bucket_data = project_db_manager.create_bucket(
            project_id=project_id,
            bucket_name=bucket.name,
            description=bucket.description,
        )

        # Update project metadata with new bucket count
        stats = project_db_manager.get_project_stats(project_id)
        metadata_db.update_project(
            project_id=project_id,
            bucket_count=stats["bucket_count"],
            table_count=stats["table_count"],
            size_bytes=stats["size_bytes"],
        )

        duration_ms = int((time.time() - start_time) * 1000)

        # Log operation
        metadata_db.log_operation(
            operation="create_bucket",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket.name,
            details={"bucket_name": bucket.name, "description": bucket.description},
            duration_ms=duration_ms,
        )

        logger.info(
            "create_bucket_success",
            project_id=project_id,
            bucket_name=bucket.name,
            duration_ms=duration_ms,
        )

        return BucketResponse(**bucket_data)

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        # Log failed operation
        metadata_db.log_operation(
            operation="create_bucket",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket.name,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "create_bucket_failed",
            project_id=project_id,
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
    "/projects/{project_id}/buckets",
    response_model=BucketListResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="List buckets",
    description="List all buckets in a project.",
)
async def list_buckets(project_id: str) -> BucketListResponse:
    """List all buckets in a project."""
    logger.info("list_buckets", project_id=project_id)

    # Check if project exists
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

    # Check if project DB exists
    if not project_db_manager.project_exists(project_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_db_not_found",
                "message": f"Database file for project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    try:
        buckets = project_db_manager.list_buckets(project_id)

        return BucketListResponse(
            buckets=[BucketResponse(**b) for b in buckets],
            total=len(buckets),
        )

    except Exception as e:
        logger.error(
            "list_buckets_failed",
            project_id=project_id,
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
    "/projects/{project_id}/buckets/{bucket_name}",
    response_model=BucketResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get bucket",
    description="Get information about a specific bucket.",
)
async def get_bucket(project_id: str, bucket_name: str) -> BucketResponse:
    """Get bucket information."""
    logger.info("get_bucket", project_id=project_id, bucket_name=bucket_name)

    # Check if project exists
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

    # Check if project DB exists
    if not project_db_manager.project_exists(project_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_db_not_found",
                "message": f"Database file for project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    # Get bucket
    bucket = project_db_manager.get_bucket(project_id, bucket_name)
    if not bucket:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "bucket_not_found",
                "message": f"Bucket {bucket_name} not found in project {project_id}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )

    return BucketResponse(**bucket)


@router.delete(
    "/projects/{project_id}/buckets/{bucket_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Delete bucket",
    description="Delete a bucket and optionally all its tables.",
)
async def delete_bucket(
    project_id: str,
    bucket_name: str,
    cascade: bool = Query(default=True, description="Drop all tables in the bucket"),
) -> None:
    """
    Delete a bucket.

    This:
    1. Verifies the project exists
    2. Deletes the schema (and optionally all tables if cascade=True)
    3. Updates project metadata
    4. Logs the operation
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "delete_bucket",
        project_id=project_id,
        bucket_name=bucket_name,
        cascade=cascade,
    )

    # Check if project exists
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

    # Check if project DB exists
    if not project_db_manager.project_exists(project_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_db_not_found",
                "message": f"Database file for project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    # Check if bucket exists
    if not project_db_manager.bucket_exists(project_id, bucket_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "bucket_not_found",
                "message": f"Bucket {bucket_name} not found in project {project_id}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )

    try:
        # Delete bucket
        project_db_manager.delete_bucket(
            project_id=project_id,
            bucket_name=bucket_name,
            cascade=cascade,
        )

        # Update project metadata with new bucket count
        stats = project_db_manager.get_project_stats(project_id)
        metadata_db.update_project(
            project_id=project_id,
            bucket_count=stats["bucket_count"],
            table_count=stats["table_count"],
            size_bytes=stats["size_bytes"],
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="delete_bucket",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            details={"bucket_name": bucket_name, "cascade": cascade},
            duration_ms=duration_ms,
        )

        logger.info(
            "delete_bucket_success",
            project_id=project_id,
            bucket_name=bucket_name,
            duration_ms=duration_ms,
        )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="delete_bucket",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "delete_bucket_failed",
            project_id=project_id,
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
