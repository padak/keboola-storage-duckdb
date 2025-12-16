"""Table schema operations: column management, primary keys, row deletion, profiling."""

import time
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, status

from src.database import metadata_db, project_db_manager
from src.dependencies import require_project_access
from src.models.responses import (
    AddColumnRequest,
    AlterColumnRequest,
    ColumnInfo,
    ColumnStatistics,
    DeleteRowsRequest,
    DeleteRowsResponse,
    ErrorResponse,
    SetPrimaryKeyRequest,
    TableProfileResponse,
    TableResponse,
)
from src.snapshot_config import should_create_snapshot

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["table-schema"])


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        import structlog

        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


def _validate_table_exists(
    project_id: str, bucket_name: str, table_name: str
) -> None:
    """
    Validate that project, bucket, and table exist.

    Raises:
        HTTPException if any resource is not found
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


# ============================================
# Column operations
# ============================================


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/columns",
    response_model=TableResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Add column",
    description="Add a new column to an existing table.",
    dependencies=[Depends(require_project_access)],
)
async def add_column(
    project_id: str,
    bucket_name: str,
    table_name: str,
    column: AddColumnRequest,
) -> TableResponse:
    """
    Add a new column to an existing table.

    The column will be added at the end of the existing columns.
    If the table has existing rows, the new column will have NULL values
    (or the specified default value).
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "add_column_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        column_name=column.name,
        column_type=column.type,
        request_id=request_id,
    )

    _validate_table_exists(project_id, bucket_name, table_name)

    # Check if column already exists
    table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
    existing_columns = {col["name"] for col in table_info["columns"]}
    if column.name in existing_columns:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "column_exists",
                "message": f"Column {column.name} already exists in table {table_name}",
                "details": {
                    "column_name": column.name,
                    "existing_columns": list(existing_columns),
                },
            },
        )

    try:
        table_data = project_db_manager.add_column(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column.name,
            column_type=column.type,
            nullable=column.nullable,
            default=column.default,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="add_column",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="column",
            resource_id=f"{bucket_name}.{table_name}.{column.name}",
            details={
                "column_name": column.name,
                "column_type": column.type,
                "nullable": column.nullable,
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "add_column_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column.name,
            duration_ms=duration_ms,
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

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="add_column",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="column",
            resource_id=f"{bucket_name}.{table_name}.{column.name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "add_column_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column.name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "add_column_failed",
                "message": f"Failed to add column: {e}",
                "details": {"column_name": column.name},
            },
        )


@router.delete(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/columns/{column_name}",
    response_model=TableResponse,
    responses={
        404: {"model": ErrorResponse},
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Drop column",
    description="Remove a column from an existing table.",
    dependencies=[Depends(require_project_access)],
)
async def drop_column(
    project_id: str,
    bucket_name: str,
    table_name: str,
    column_name: str,
) -> TableResponse:
    """
    Remove a column from an existing table.

    Warning: This operation is destructive and cannot be undone.
    All data in the column will be lost.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "drop_column_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        column_name=column_name,
        request_id=request_id,
    )

    _validate_table_exists(project_id, bucket_name, table_name)

    # Check if column exists
    table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
    existing_columns = {col["name"] for col in table_info["columns"]}
    if column_name not in existing_columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "column_not_found",
                "message": f"Column {column_name} not found in table {table_name}",
                "details": {
                    "column_name": column_name,
                    "existing_columns": list(existing_columns),
                },
            },
        )

    # Don't allow dropping the last column
    if len(existing_columns) <= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "cannot_drop_last_column",
                "message": "Cannot drop the last column from a table",
                "details": {"column_count": len(existing_columns)},
            },
        )

    # Check if column is part of primary key
    if column_name in table_info.get("primary_key", []):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "column_in_primary_key",
                "message": f"Cannot drop column {column_name}: it is part of the primary key",
                "details": {"primary_key": table_info["primary_key"]},
            },
        )

    # Check if auto-snapshot should be created before dropping column
    if should_create_snapshot(project_id, bucket_name, table_name, "drop_column"):
        from src.routers.snapshots import create_snapshot_internal

        try:
            await create_snapshot_internal(
                project_id=project_id,
                bucket_name=bucket_name,
                table_name=table_name,
                snapshot_type="auto_predrop_column",
                description=f"Auto-backup before DROP COLUMN {bucket_name}.{table_name}.{column_name}",
            )
            logger.info(
                "auto_snapshot_created_before_drop_column",
                project_id=project_id,
                bucket_name=bucket_name,
                table_name=table_name,
                column_name=column_name,
            )
        except Exception as e:
            # Log but don't fail the drop if snapshot fails
            logger.warning(
                "auto_snapshot_failed_before_drop_column",
                project_id=project_id,
                bucket_name=bucket_name,
                table_name=table_name,
                column_name=column_name,
                error=str(e),
            )

    try:
        table_data = project_db_manager.drop_column(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="drop_column",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="column",
            resource_id=f"{bucket_name}.{table_name}.{column_name}",
            details={"column_name": column_name},
            duration_ms=duration_ms,
        )

        logger.info(
            "drop_column_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            duration_ms=duration_ms,
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

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="drop_column",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="column",
            resource_id=f"{bucket_name}.{table_name}.{column_name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "drop_column_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "drop_column_failed",
                "message": f"Failed to drop column: {e}",
                "details": {"column_name": column_name},
            },
        )


@router.put(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/columns/{column_name}",
    response_model=TableResponse,
    responses={
        404: {"model": ErrorResponse},
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Alter column",
    description="Modify a column (rename, change type, set/drop NOT NULL, set/drop default).",
    dependencies=[Depends(require_project_access)],
)
async def alter_column(
    project_id: str,
    bucket_name: str,
    table_name: str,
    column_name: str,
    changes: AlterColumnRequest,
) -> TableResponse:
    """
    Modify an existing column.

    Supports:
    - Rename: Provide `new_name`
    - Type change: Provide `new_type` (data must be compatible)
    - NOT NULL: Set `set_not_null` to True/False
    - Default: Set `set_default` (empty string to drop default)

    Multiple changes can be made in a single request.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "alter_column_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        column_name=column_name,
        request_id=request_id,
    )

    _validate_table_exists(project_id, bucket_name, table_name)

    # Check if column exists
    table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
    existing_columns = {col["name"] for col in table_info["columns"]}
    if column_name not in existing_columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "column_not_found",
                "message": f"Column {column_name} not found in table {table_name}",
                "details": {
                    "column_name": column_name,
                    "existing_columns": list(existing_columns),
                },
            },
        )

    # Validate request has at least one change
    if (
        changes.new_name is None
        and changes.new_type is None
        and changes.set_not_null is None
        and changes.set_default is None
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "no_changes_specified",
                "message": "At least one change must be specified",
                "details": {},
            },
        )

    # If renaming, check new name doesn't conflict
    if changes.new_name and changes.new_name != column_name:
        if changes.new_name in existing_columns:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "column_exists",
                    "message": f"Column {changes.new_name} already exists",
                    "details": {"new_name": changes.new_name},
                },
            )

    try:
        table_data = project_db_manager.alter_column(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            new_name=changes.new_name,
            new_type=changes.new_type,
            set_not_null=changes.set_not_null,
            set_default=changes.set_default,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="alter_column",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="column",
            resource_id=f"{bucket_name}.{table_name}.{column_name}",
            details={
                "column_name": column_name,
                "new_name": changes.new_name,
                "new_type": changes.new_type,
                "set_not_null": changes.set_not_null,
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "alter_column_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            duration_ms=duration_ms,
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

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="alter_column",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="column",
            resource_id=f"{bucket_name}.{table_name}.{column_name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "alter_column_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            column_name=column_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "alter_column_failed",
                "message": f"Failed to alter column: {e}",
                "details": {"column_name": column_name},
            },
        )


# ============================================
# Primary key operations
# ============================================


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/primary-key",
    response_model=TableResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"model": ErrorResponse},
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Add primary key",
    description="Add a primary key constraint to a table.",
    dependencies=[Depends(require_project_access)],
)
async def add_primary_key(
    project_id: str,
    bucket_name: str,
    table_name: str,
    pk_request: SetPrimaryKeyRequest,
) -> TableResponse:
    """
    Add a primary key constraint to a table.

    Note: DuckDB enforces primary keys (unlike BigQuery where they're metadata-only).
    If the table has duplicate values in the specified columns, this will fail.

    The operation recreates the table internally to add the constraint.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "add_primary_key_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        columns=pk_request.columns,
        request_id=request_id,
    )

    _validate_table_exists(project_id, bucket_name, table_name)

    # Check if columns exist
    table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
    existing_columns = {col["name"] for col in table_info["columns"]}
    for col in pk_request.columns:
        if col not in existing_columns:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "column_not_found",
                    "message": f"Column {col} not found in table {table_name}",
                    "details": {
                        "column": col,
                        "existing_columns": list(existing_columns),
                    },
                },
            )

    # Check if table already has a primary key
    if table_info.get("primary_key"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "primary_key_exists",
                "message": f"Table {table_name} already has a primary key",
                "details": {"existing_primary_key": table_info["primary_key"]},
            },
        )

    try:
        table_data = project_db_manager.add_primary_key(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            columns=pk_request.columns,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="add_primary_key",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            details={"columns": pk_request.columns},
            duration_ms=duration_ms,
        )

        logger.info(
            "add_primary_key_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            columns=pk_request.columns,
            duration_ms=duration_ms,
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

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "primary_key_exists",
                "message": str(e),
                "details": {},
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="add_primary_key",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "add_primary_key_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "add_primary_key_failed",
                "message": f"Failed to add primary key: {e}",
                "details": {},
            },
        )


@router.delete(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/primary-key",
    response_model=TableResponse,
    responses={
        404: {"model": ErrorResponse},
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Drop primary key",
    description="Remove the primary key constraint from a table.",
    dependencies=[Depends(require_project_access)],
)
async def drop_primary_key(
    project_id: str,
    bucket_name: str,
    table_name: str,
) -> TableResponse:
    """
    Remove the primary key constraint from a table.

    The data is preserved; only the constraint is removed.
    The operation recreates the table internally to remove the constraint.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "drop_primary_key_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        request_id=request_id,
    )

    _validate_table_exists(project_id, bucket_name, table_name)

    # Check if table has a primary key
    table_info = project_db_manager.get_table(project_id, bucket_name, table_name)
    if not table_info.get("primary_key"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "no_primary_key",
                "message": f"Table {table_name} does not have a primary key",
                "details": {},
            },
        )

    try:
        table_data = project_db_manager.drop_primary_key(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="drop_primary_key",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            details={"previous_primary_key": table_info["primary_key"]},
            duration_ms=duration_ms,
        )

        logger.info(
            "drop_primary_key_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            duration_ms=duration_ms,
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

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "no_primary_key",
                "message": str(e),
                "details": {},
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="drop_primary_key",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "drop_primary_key_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "drop_primary_key_failed",
                "message": f"Failed to drop primary key: {e}",
                "details": {},
            },
        )


# ============================================
# Row operations
# ============================================


@router.delete(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/rows",
    response_model=DeleteRowsResponse,
    responses={
        404: {"model": ErrorResponse},
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Delete rows",
    description="Delete rows from a table matching a WHERE condition.",
    dependencies=[Depends(require_project_access)],
)
async def delete_rows(
    project_id: str,
    bucket_name: str,
    table_name: str,
    delete_request: DeleteRowsRequest,
) -> DeleteRowsResponse:
    """
    Delete rows from a table matching a WHERE condition.

    The `where_clause` should be a valid SQL condition without the WHERE keyword.
    Examples:
    - `status = 'deleted'`
    - `created_at < '2024-01-01'`
    - `id IN (1, 2, 3)`

    Warning: This operation is destructive and cannot be undone.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "delete_rows_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        where_clause=delete_request.where_clause[:100],  # Truncate for logging
        request_id=request_id,
    )

    _validate_table_exists(project_id, bucket_name, table_name)

    try:
        result = project_db_manager.delete_table_rows(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            where_clause=delete_request.where_clause,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="delete_rows",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            details={
                "where_clause": delete_request.where_clause[:200],
                "deleted_rows": result["deleted_rows"],
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "delete_rows_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            deleted_rows=result["deleted_rows"],
            rows_remaining=result["table_rows_after"],
            duration_ms=duration_ms,
        )

        return DeleteRowsResponse(
            deleted_rows=result["deleted_rows"],
            table_rows_after=result["table_rows_after"],
        )

    except ValueError as e:
        # SQL injection prevention or invalid WHERE clause
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_where_clause",
                "message": str(e),
                "details": {"where_clause": delete_request.where_clause[:200]},
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="delete_rows",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "delete_rows_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "delete_rows_failed",
                "message": f"Failed to delete rows: {e}",
                "details": {},
            },
        )


# ============================================
# Table profiling
# ============================================


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/profile",
    response_model=TableProfileResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Profile table",
    description="Get statistical profile of a table (min, max, avg, percentiles, etc.).",
    dependencies=[Depends(require_project_access)],
)
async def profile_table(
    project_id: str,
    bucket_name: str,
    table_name: str,
) -> TableProfileResponse:
    """
    Get statistical profile of a table.

    Uses DuckDB's SUMMARIZE command to calculate:
    - Min/max values
    - Approximate unique count
    - Average and standard deviation (numeric columns)
    - Percentiles (q25, q50, q75)
    - Null percentage

    This is a read-only operation and does not modify data.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "profile_table_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        request_id=request_id,
    )

    _validate_table_exists(project_id, bucket_name, table_name)

    try:
        profile_data = project_db_manager.get_table_profile(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="profile_table",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            details={
                "row_count": profile_data["row_count"],
                "column_count": profile_data["column_count"],
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "profile_table_success",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            row_count=profile_data["row_count"],
            column_count=profile_data["column_count"],
            duration_ms=duration_ms,
        )

        return TableProfileResponse(
            table_name=profile_data["table_name"],
            bucket_name=profile_data["bucket_name"],
            row_count=profile_data["row_count"],
            column_count=profile_data["column_count"],
            statistics=[
                ColumnStatistics(**stat) for stat in profile_data["statistics"]
            ],
        )

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        metadata_db.log_operation(
            operation="profile_table",
            status="failed",
            project_id=project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "profile_table_failed",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            error=str(e),
            exc_info=True,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "profile_table_failed",
                "message": f"Failed to profile table: {e}",
                "details": {},
            },
        )
