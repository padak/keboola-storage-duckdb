"""Bucket sharing and linking endpoints: Share buckets between projects."""

import time

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.database import metadata_db, project_db_manager
from src.dependencies import require_project_access
from src.models.responses import (
    BucketLinkRequest,
    BucketResponse,
    BucketShareInfo,
    BucketShareRequest,
    ErrorResponse,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["bucket-sharing"])


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        import structlog

        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/share",
    response_model=BucketShareInfo,
    status_code=status.HTTP_200_OK,
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Share bucket with another project",
    description="Share a bucket with another project (records the share, actual linking done on target side).",
    dependencies=[Depends(require_project_access)],
)
async def share_bucket(
    project_id: str,
    bucket_name: str,
    request: BucketShareRequest,
) -> BucketShareInfo:
    """
    Share a bucket with another project.

    This records the share in metadata but doesn't actually link the bucket.
    The target project must call the link endpoint to create the actual views.

    Similar to BigQuery Analytics Hub sharing model.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "share_bucket_start",
        project_id=project_id,
        bucket_name=bucket_name,
        target_project=request.target_project_id,
        request_id=request_id,
    )

    # Verify source project exists
    source_project = metadata_db.get_project(project_id)
    if not source_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Source project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    # Verify target project exists
    target_project = metadata_db.get_project(request.target_project_id)
    if not target_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Target project {request.target_project_id} not found",
                "details": {"project_id": request.target_project_id},
            },
        )

    # Verify bucket exists in source project
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
        # Check if already shared
        existing_shares = metadata_db.get_bucket_shares(project_id, bucket_name)
        if request.target_project_id in existing_shares:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "already_shared",
                    "message": f"Bucket {bucket_name} is already shared with project {request.target_project_id}",
                    "details": {
                        "project_id": project_id,
                        "bucket_name": bucket_name,
                        "target_project_id": request.target_project_id,
                    },
                },
            )

        # Create share record
        share_id = metadata_db.create_bucket_share(
            source_project_id=project_id,
            source_bucket_name=bucket_name,
            target_project_id=request.target_project_id,
            share_type="readonly",
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="share_bucket",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            details={
                "target_project_id": request.target_project_id,
                "share_id": share_id,
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "share_bucket_success",
            project_id=project_id,
            bucket_name=bucket_name,
            target_project=request.target_project_id,
            share_id=share_id,
            duration_ms=duration_ms,
        )

        # Get updated share list
        all_shares = metadata_db.get_bucket_shares(project_id, bucket_name)

        return BucketShareInfo(
            shared_with=all_shares,
            is_linked=False,
        )

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="share_bucket",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "share_bucket_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "share_failed",
                "message": f"Failed to share bucket: {e}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )


@router.delete(
    "/projects/{project_id}/buckets/{bucket_name}/share",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Unshare bucket",
    description="Remove bucket share with a specific project.",
    dependencies=[Depends(require_project_access)],
)
async def unshare_bucket(
    project_id: str,
    bucket_name: str,
    target_project_id: str = Query(..., description="Target project ID to unshare with"),
) -> None:
    """
    Unshare a bucket with a specific project.

    This removes the share record but doesn't automatically unlink on the target side.
    The target project should call the unlink endpoint to remove views.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "unshare_bucket_start",
        project_id=project_id,
        bucket_name=bucket_name,
        target_project=target_project_id,
        request_id=request_id,
    )

    # Verify source project exists
    source_project = metadata_db.get_project(project_id)
    if not source_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Source project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    try:
        # Delete share record
        metadata_db.delete_bucket_share(
            source_project_id=project_id,
            source_bucket_name=bucket_name,
            target_project_id=target_project_id,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="unshare_bucket",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            details={"target_project_id": target_project_id},
            duration_ms=duration_ms,
        )

        logger.info(
            "unshare_bucket_success",
            project_id=project_id,
            bucket_name=bucket_name,
            target_project=target_project_id,
            duration_ms=duration_ms,
        )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="unshare_bucket",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "unshare_bucket_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "unshare_failed",
                "message": f"Failed to unshare bucket: {e}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/link",
    response_model=BucketResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Link bucket from another project",
    description="Link (attach) a bucket from another project using DuckDB ATTACH and views.",
    dependencies=[Depends(require_project_access)],
)
async def link_bucket(
    project_id: str,
    bucket_name: str,
    request: BucketLinkRequest,
) -> BucketResponse:
    """
    Link a bucket from another project.

    This:
    1. ATTACHes the source project's .duckdb file in READ_ONLY mode
    2. Creates a schema (bucket) in the target project
    3. Creates VIEWs for each table in the source bucket

    The views point to the attached database, allowing readonly access.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "link_bucket_start",
        target_project=project_id,
        target_bucket=bucket_name,
        source_project=request.source_project_id,
        source_bucket=request.source_bucket_name,
        request_id=request_id,
    )

    # Verify target project exists
    target_project = metadata_db.get_project(project_id)
    if not target_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Target project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    # Verify source project exists
    source_project = metadata_db.get_project(request.source_project_id)
    if not source_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Source project {request.source_project_id} not found",
                "details": {"project_id": request.source_project_id},
            },
        )

    # Verify source bucket exists
    if not project_db_manager.bucket_exists(
        request.source_project_id, request.source_bucket_name
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "bucket_not_found",
                "message": f"Source bucket {request.source_bucket_name} not found in project {request.source_project_id}",
                "details": {
                    "project_id": request.source_project_id,
                    "bucket_name": request.source_bucket_name,
                },
            },
        )

    # Check if target bucket already exists or is linked
    if project_db_manager.bucket_exists(project_id, bucket_name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bucket_exists",
                "message": f"Bucket {bucket_name} already exists in project {project_id}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )

    existing_link = metadata_db.get_bucket_link(project_id, bucket_name)
    if existing_link:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bucket_already_linked",
                "message": f"Bucket {bucket_name} is already linked in project {project_id}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "existing_link": existing_link,
                },
            },
        )

    try:
        # Generate alias for attached database
        db_alias = f"source_proj_{request.source_project_id}"

        # Link bucket using combined method (ATTACH + views in one connection)
        # This is necessary because DuckDB ATTACH is session-specific
        created_views = project_db_manager.link_bucket_with_views(
            target_project_id=project_id,
            target_bucket_name=bucket_name,
            source_project_id=request.source_project_id,
            source_bucket_name=request.source_bucket_name,
            source_db_alias=db_alias,
        )

        # 3. Record link in metadata
        link_id = metadata_db.create_bucket_link(
            target_project_id=project_id,
            target_bucket_name=bucket_name,
            source_project_id=request.source_project_id,
            source_bucket_name=request.source_bucket_name,
            attached_db_alias=db_alias,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="link_bucket",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            details={
                "source_project_id": request.source_project_id,
                "source_bucket_name": request.source_bucket_name,
                "link_id": link_id,
                "view_count": len(created_views),
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "link_bucket_success",
            target_project=project_id,
            target_bucket=bucket_name,
            source_project=request.source_project_id,
            source_bucket=request.source_bucket_name,
            link_id=link_id,
            view_count=len(created_views),
            duration_ms=duration_ms,
        )

        return BucketResponse(
            name=bucket_name,
            table_count=len(created_views),
            description=f"Linked from {request.source_project_id}.{request.source_bucket_name}",
        )

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="link_bucket",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "link_bucket_failed",
            target_project=project_id,
            target_bucket=bucket_name,
            error=str(e),
            exc_info=True,
        )

        # Cleanup: try to detach database if it was attached
        try:
            db_alias = f"source_proj_{request.source_project_id}"
            project_db_manager.detach_database(project_id, db_alias)
        except Exception:
            pass

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "link_failed",
                "message": f"Failed to link bucket: {e}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )


@router.delete(
    "/projects/{project_id}/buckets/{bucket_name}/link",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Unlink bucket",
    description="Unlink a previously linked bucket (drop views and detach database).",
    dependencies=[Depends(require_project_access)],
)
async def unlink_bucket(
    project_id: str,
    bucket_name: str,
) -> None:
    """
    Unlink a previously linked bucket.

    This:
    1. Drops all views in the bucket
    2. Drops the schema (bucket)
    3. DETACHes the source database
    4. Removes link record from metadata
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "unlink_bucket_start",
        project_id=project_id,
        bucket_name=bucket_name,
        request_id=request_id,
    )

    # Verify project exists
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

    # Get link info
    link = metadata_db.get_bucket_link(project_id, bucket_name)
    if not link:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "link_not_found",
                "message": f"No link found for bucket {bucket_name} in project {project_id}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )

    try:
        # 1. Drop views
        project_db_manager.drop_bucket_views(
            target_project_id=project_id,
            target_bucket_name=bucket_name,
        )

        # 2. Drop the schema
        project_db_manager.delete_bucket(
            project_id=project_id,
            bucket_name=bucket_name,
            cascade=True,
        )

        # 3. Detach database
        project_db_manager.detach_database(
            target_project_id=project_id,
            alias=link["attached_db_alias"],
        )

        # 4. Remove link record
        metadata_db.delete_bucket_link(
            target_project_id=project_id,
            target_bucket_name=bucket_name,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="unlink_bucket",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            details={
                "source_project_id": link["source_project_id"],
                "source_bucket_name": link["source_bucket_name"],
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "unlink_bucket_success",
            project_id=project_id,
            bucket_name=bucket_name,
            duration_ms=duration_ms,
        )

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="unlink_bucket",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="bucket",
            resource_id=bucket_name,
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "unlink_bucket_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "unlink_failed",
                "message": f"Failed to unlink bucket: {e}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/grant-readonly",
    status_code=status.HTTP_200_OK,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Grant readonly access",
    description="Grant readonly access to a bucket (metadata operation for DuckDB).",
    dependencies=[Depends(require_project_access)],
)
async def grant_readonly_access(
    project_id: str,
    bucket_name: str,
) -> dict:
    """
    Grant readonly access to a bucket.

    For DuckDB, this is mostly a metadata operation since DuckDB doesn't have
    user-level permissions. The actual readonly enforcement happens via ATTACH READ_ONLY.

    This endpoint is here for API compatibility but doesn't perform actual permission grants.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "grant_readonly_start",
        project_id=project_id,
        bucket_name=bucket_name,
        request_id=request_id,
    )

    # Verify project exists
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

    # Verify bucket exists
    if not project_db_manager.bucket_exists(project_id, bucket_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "bucket_not_found",
                "message": f"Bucket {bucket_name} not found in project {project_id}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )

    duration_ms = int((time.time() - start_time) * 1000)

    metadata_db.log_operation(
        operation="grant_readonly",
        status="success",
        project_id=project_id,
        request_id=request_id,
        resource_type="bucket",
        resource_id=bucket_name,
        details={"note": "DuckDB has no user permissions - readonly via ATTACH"},
        duration_ms=duration_ms,
    )

    logger.info(
        "grant_readonly_success",
        project_id=project_id,
        bucket_name=bucket_name,
        duration_ms=duration_ms,
    )

    return {
        "status": "success",
        "message": "Readonly access is enforced via ATTACH READ_ONLY when linking buckets",
        "bucket_name": bucket_name,
    }


@router.delete(
    "/projects/{project_id}/buckets/{bucket_name}/grant-readonly",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Revoke readonly access",
    description="Revoke readonly access to a bucket (metadata operation for DuckDB).",
    dependencies=[Depends(require_project_access)],
)
async def revoke_readonly_access(
    project_id: str,
    bucket_name: str,
) -> None:
    """
    Revoke readonly access to a bucket.

    For DuckDB, this is a no-op since there are no user-level permissions.
    This endpoint exists for API compatibility.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "revoke_readonly_start",
        project_id=project_id,
        bucket_name=bucket_name,
        request_id=request_id,
    )

    # Verify project exists
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

    duration_ms = int((time.time() - start_time) * 1000)

    metadata_db.log_operation(
        operation="revoke_readonly",
        status="success",
        project_id=project_id,
        request_id=request_id,
        resource_type="bucket",
        resource_id=bucket_name,
        details={"note": "DuckDB has no user permissions - no-op"},
        duration_ms=duration_ms,
    )

    logger.info(
        "revoke_readonly_success",
        project_id=project_id,
        bucket_name=bucket_name,
        duration_ms=duration_ms,
    )
