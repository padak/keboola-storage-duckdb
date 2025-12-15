"""Table management endpoints: CRUD operations for tables within buckets."""

import time
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query, status

from src.database import metadata_db, project_db_manager
from src.models.responses import (
    ColumnInfo,
    ErrorResponse,
    TableCreate,
    TableListResponse,
    TablePreviewResponse,
    TableResponse,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["tables"])


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        import structlog

        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


def _validate_project_and_bucket(
    project_id: str, bucket_name: str
) -> tuple[dict[str, Any], None]:
    """
    Validate that project and bucket exist.

    Returns:
        Tuple of (project_dict, None) if valid

    Raises:
        HTTPException if project or bucket not found
    """
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

    return project, None


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/tables",
    response_model=TableResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Create table",
    description="Create a new table in a bucket.",
)
async def create_table(
    project_id: str, bucket_name: str, table: TableCreate
) -> TableResponse:
    """
    Create a new table in a bucket.

    This endpoint:
    1. Verifies the project and bucket exist
    2. Creates a table with the specified columns
    3. Optionally adds a primary key constraint
    4. Updates project metadata
    5. Logs the operation

    Note: Unlike BigQuery, DuckDB enforces primary keys as real constraints.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "create_table_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table.name,
        column_count=len(table.columns),
        request_id=request_id,
    )

    # Validate project and bucket
    _validate_project_and_bucket(project_id, bucket_name)

    # Check if table already exists
    if project_db_manager.table_exists(project_id, bucket_name, table.name):
        logger.warning(
            "create_table_conflict",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table.name,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "table_exists",
                "message": f"Table {table.name} already exists in bucket {bucket_name}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table.name,
                },
            },
        )

    # Validate primary key columns exist
    if table.primary_key:
        column_names = {col.name for col in table.columns}
        for pk_col in table.primary_key:
            if pk_col not in column_names:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "invalid_primary_key",
                        "message": f"Primary key column '{pk_col}' not found in columns",
                        "details": {
                            "primary_key": table.primary_key,
                            "columns": list(column_names),
                        },
                    },
                )

    try:
        # Convert Pydantic models to dicts for database layer
        columns_data = [col.model_dump() for col in table.columns]

        # Create table
        table_data = project_db_manager.create_table(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table.name,
            columns=columns_data,
            primary_key=table.primary_key,
        )

        # Update project metadata
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
            operation="create_table",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table.name}",
            details={
                "bucket_name": bucket_name,
                "table_name": table.name,
                "column_count": len(table.columns),
                "primary_key": table.primary_key,
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "create_table_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table.name,
            duration_ms=duration_ms,
        )

        # Convert to response model
        return TableResponse(
            name=table_data["name"],
            bucket=table_data["bucket"],
            columns=[ColumnInfo(**col) for col in table_data["columns"]],
            row_count=table_data["row_count"],
            size_bytes=table_data["size_bytes"],
            primary_key=table_data["primary_key"],
            created_at=table_data["created_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        # Log failed operation
        metadata_db.log_operation(
            operation="create_table",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table.name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "create_table_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table.name,
            error=str(e),
            duration_ms=duration_ms,
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "create_failed",
                "message": f"Failed to create table: {e}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table.name,
                },
            },
        )


@router.get(
    "/projects/{project_id}/buckets/{bucket_name}/tables",
    response_model=TableListResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="List tables",
    description="List all tables in a bucket.",
)
async def list_tables(project_id: str, bucket_name: str) -> TableListResponse:
    """List all tables in a bucket."""
    logger.info(
        "list_tables",
        project_id=project_id,
        bucket_name=bucket_name,
    )

    # Validate project and bucket
    _validate_project_and_bucket(project_id, bucket_name)

    try:
        tables_data = project_db_manager.list_tables(project_id, bucket_name)

        tables = [
            TableResponse(
                name=t["name"],
                bucket=t["bucket"],
                columns=[ColumnInfo(**col) for col in t["columns"]],
                row_count=t["row_count"],
                size_bytes=t["size_bytes"],
                primary_key=t["primary_key"],
                created_at=t["created_at"],
            )
            for t in tables_data
        ]

        return TableListResponse(
            tables=tables,
            total=len(tables),
        )

    except Exception as e:
        logger.error(
            "list_tables_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "list_failed",
                "message": f"Failed to list tables: {e}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )


@router.get(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}",
    response_model=TableResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get table (ObjectInfo)",
    description="Get detailed information about a table.",
)
async def get_table(
    project_id: str, bucket_name: str, table_name: str
) -> TableResponse:
    """
    Get table information (ObjectInfo).

    Returns detailed metadata including:
    - Column definitions (name, type, nullable)
    - Row count
    - Size in bytes (estimated)
    - Primary key columns
    """
    logger.info(
        "get_table",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
    )

    # Validate project and bucket
    _validate_project_and_bucket(project_id, bucket_name)

    # Get table
    table_data = project_db_manager.get_table(project_id, bucket_name, table_name)
    if not table_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "table_not_found",
                "message": f"Table {table_name} not found in bucket {bucket_name}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )

    return TableResponse(
        name=table_data["name"],
        bucket=table_data["bucket"],
        columns=[ColumnInfo(**col) for col in table_data["columns"]],
        row_count=table_data["row_count"],
        size_bytes=table_data["size_bytes"],
        primary_key=table_data["primary_key"],
        created_at=table_data["created_at"],
    )


@router.delete(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Delete table",
    description="Delete a table from a bucket.",
)
async def delete_table(
    project_id: str,
    bucket_name: str,
    table_name: str,
) -> None:
    """
    Delete a table.

    This:
    1. Verifies the project, bucket, and table exist
    2. Drops the table
    3. Updates project metadata
    4. Logs the operation
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "delete_table_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        request_id=request_id,
    )

    # Validate project and bucket
    _validate_project_and_bucket(project_id, bucket_name)

    # Check if table exists
    if not project_db_manager.table_exists(project_id, bucket_name, table_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "table_not_found",
                "message": f"Table {table_name} not found in bucket {bucket_name}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )

    try:
        # Delete table
        project_db_manager.delete_table(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )

        # Update project metadata
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
            operation="delete_table",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            details={"bucket_name": bucket_name, "table_name": table_name},
            duration_ms=duration_ms,
        )

        logger.info(
            "delete_table_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            duration_ms=duration_ms,
        )

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        # Log failed operation
        metadata_db.log_operation(
            operation="delete_table",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "delete_table_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            error=str(e),
            duration_ms=duration_ms,
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "delete_failed",
                "message": f"Failed to delete table: {e}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )


@router.get(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/preview",
    response_model=TablePreviewResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Preview table",
    description="Get a preview of table data (first N rows).",
)
async def preview_table(
    project_id: str,
    bucket_name: str,
    table_name: str,
    limit: int = Query(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum number of rows to return (1-10000)",
    ),
) -> TablePreviewResponse:
    """
    Get a preview of table data.

    Returns:
    - Column information (name, type)
    - Row data (up to `limit` rows)
    - Total row count in table
    - Number of rows in preview

    Equivalent to `SELECT * FROM table LIMIT N`.
    """
    logger.info(
        "preview_table",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        limit=limit,
    )

    # Validate project and bucket
    _validate_project_and_bucket(project_id, bucket_name)

    # Check if table exists
    if not project_db_manager.table_exists(project_id, bucket_name, table_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "table_not_found",
                "message": f"Table {table_name} not found in bucket {bucket_name}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )

    try:
        preview_data = project_db_manager.get_table_preview(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            limit=limit,
        )

        return TablePreviewResponse(
            columns=[ColumnInfo(**col) for col in preview_data["columns"]],
            rows=preview_data["rows"],
            total_row_count=preview_data["total_row_count"],
            preview_row_count=preview_data["preview_row_count"],
        )

    except Exception as e:
        logger.error(
            "preview_table_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "preview_failed",
                "message": f"Failed to preview table: {e}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )
