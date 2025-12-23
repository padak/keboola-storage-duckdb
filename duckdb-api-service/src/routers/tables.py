"""Table management endpoints: CRUD operations for tables within buckets.

ADR-012: Branch-First API
==========================
All table operations are scoped to a branch via:
/projects/{id}/branches/{branch_id}/buckets/{bucket}/tables

Branch ID:
- "default" = main (production) project
- {uuid} = dev branch with CoW semantics

Branch Behavior:
- READ: For dev branches, reads from branch if CoW'd, otherwise from main (Live View)
- WRITE: For dev branches, triggers CoW before first write
- CREATE: Creates table in branch context (isolated from main if dev branch)
- DELETE: On default deletes from main; on dev branch deletes only branch copy
"""

import time
from typing import Any, Literal

import structlog
import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.branch_utils import (
    get_table_source,
    resolve_branch,
    resolve_linked_bucket,
    validate_project_and_bucket,
    validate_project_db_exists,
)
from src.database import metadata_db, project_db_manager, TABLE_DATA_NAME
from src.dependencies import require_project_access
from src.models.responses import (
    ColumnInfo,
    ErrorResponse,
    TableCreate,
    TableListResponse,
    TablePreviewResponse,
    TableResponse,
)
from src.snapshot_config import should_create_snapshot

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["tables"])


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        import structlog

        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


def _table_exists_in_context(
    project_id: str, branch_id: str | None, bucket_name: str, table_name: str
) -> bool:
    """Check if table exists in the given branch context (including linked buckets)."""
    # Resolve linked bucket to source project if linked
    effective_project_id, effective_bucket_name, _ = resolve_linked_bucket(
        project_id, bucket_name
    )

    if branch_id is None:
        # Default branch - check main (or linked source)
        return project_db_manager.table_exists(
            effective_project_id, effective_bucket_name, table_name
        )
    else:
        # Dev branch - check if exists in main OR branch
        exists_in_main = project_db_manager.table_exists(
            effective_project_id, effective_bucket_name, table_name
        )
        exists_in_branch = metadata_db.is_table_in_branch(
            branch_id, bucket_name, table_name
        )
        return exists_in_main or exists_in_branch


@router.post(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables",
    response_model=TableResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Create table",
    description="""
    Create a new table in a bucket.

    - For default branch: creates table in main project
    - For dev branches: creates table in branch (isolated from main)
    """,
    dependencies=[Depends(require_project_access)],
)
async def create_table(
    project_id: str, branch_id: str, bucket_name: str, table: TableCreate
) -> TableResponse:
    """
    Create a new table in a bucket.

    This endpoint:
    1. Verifies the project, branch, and bucket exist
    2. Creates a table with the specified columns
    3. Optionally adds a primary key constraint
    4. Updates project metadata
    5. Logs the operation
    """
    start_time = time.time()
    request_id = _get_request_id()

    # Resolve branch (validates project and branch exist)
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    logger.info(
        "create_table_start",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table.name,
        column_count=len(table.columns),
        request_id=request_id,
    )

    # Validate bucket exists
    validate_project_and_bucket(resolved_project_id, resolved_branch_id, bucket_name)

    # Check if table already exists in context
    if _table_exists_in_context(
        resolved_project_id, resolved_branch_id, bucket_name, table.name
    ):
        logger.warning(
            "create_table_conflict",
            project_id=resolved_project_id,
            branch_id=resolved_branch_id,
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

        # Determine target and source
        if resolved_branch_id is None:
            # Default branch - create in main
            table_data = project_db_manager.create_table(
                project_id=resolved_project_id,
                bucket_name=bucket_name,
                table_name=table.name,
                columns=columns_data,
                primary_key=table.primary_key,
            )
            source: Literal["main", "branch"] = "main"

            # Update project metadata
            stats = project_db_manager.get_project_stats(resolved_project_id)
            metadata_db.update_project(
                project_id=resolved_project_id,
                bucket_count=stats["bucket_count"],
                table_count=stats["table_count"],
                size_bytes=stats["size_bytes"],
            )
        else:
            # Dev branch - create in branch directory
            branch_bucket_dir = project_db_manager.get_branch_bucket_dir(
                resolved_project_id, resolved_branch_id, bucket_name
            )
            branch_bucket_dir.mkdir(parents=True, exist_ok=True)

            table_path = project_db_manager.get_branch_table_path(
                resolved_project_id, resolved_branch_id, bucket_name, table.name
            )

            # Create the table file
            conn = duckdb.connect(str(table_path))
            try:
                # Build CREATE TABLE statement
                col_defs = []
                for col in columns_data:
                    col_def = f'"{col["name"]}" {col["type"]}'
                    if not col.get("nullable", True):
                        col_def += " NOT NULL"
                    col_defs.append(col_def)

                # Add primary key constraint
                if table.primary_key:
                    pk_cols = ", ".join(f'"{c}"' for c in table.primary_key)
                    col_defs.append(f"PRIMARY KEY ({pk_cols})")

                create_sql = (
                    f"CREATE TABLE {TABLE_DATA_NAME} ({', '.join(col_defs)})"
                )
                conn.execute(create_sql)

                # Get table info for response
                table_data = {
                    "name": table.name,
                    "bucket": bucket_name,
                    "columns": columns_data,
                    "row_count": 0,
                    "size_bytes": table_path.stat().st_size,
                    "primary_key": table.primary_key or [],
                    "created_at": None,
                }
            finally:
                conn.close()

            # Record in branch_tables metadata
            metadata_db.mark_table_copied_to_branch(
                resolved_branch_id, bucket_name, table.name
            )
            source = "branch"

        duration_ms = int((time.time() - start_time) * 1000)

        # Log operation
        metadata_db.log_operation(
            operation="create_table",
            status="success",
            project_id=resolved_project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table.name}",
            details={
                "bucket_name": bucket_name,
                "table_name": table.name,
                "column_count": len(table.columns),
                "primary_key": table.primary_key,
                "branch_id": branch_id,
                "source": source,
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "create_table_success",
            project_id=resolved_project_id,
            branch_id=resolved_branch_id,
            bucket_name=bucket_name,
            table_name=table.name,
            source=source,
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
            source=source,
        )

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)

        # Log failed operation
        metadata_db.log_operation(
            operation="create_table",
            status="failed",
            project_id=resolved_project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table.name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "create_table_failed",
            project_id=resolved_project_id,
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
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables",
    response_model=TableListResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="List tables",
    description="""
    List all tables visible in branch context.

    - For default branch: lists tables from main
    - For dev branches: merges main + branch tables with source indicators
    """,
    dependencies=[Depends(require_project_access)],
)
async def list_tables(
    project_id: str, branch_id: str, bucket_name: str
) -> TableListResponse:
    """List all tables in a bucket (branch-aware)."""
    # Resolve branch
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    logger.info(
        "list_tables",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
    )

    # Validate bucket exists
    validate_project_and_bucket(resolved_project_id, resolved_branch_id, bucket_name)

    try:
        if resolved_branch_id is None:
            # Default branch - list from main
            tables_data = project_db_manager.list_tables(
                resolved_project_id, bucket_name
            )
            tables = [
                TableResponse(
                    name=t["name"],
                    bucket=t["bucket"],
                    columns=[ColumnInfo(**col) for col in t["columns"]],
                    row_count=t["row_count"],
                    size_bytes=t["size_bytes"],
                    primary_key=t["primary_key"],
                    created_at=t["created_at"],
                    source="main",
                )
                for t in tables_data
            ]
        else:
            # Dev branch - merge main + branch tables
            main_tables = project_db_manager.list_tables(
                resolved_project_id, bucket_name
            )
            branch_tables_meta = metadata_db.get_branch_tables(resolved_branch_id)

            # Build set of branch table names in this bucket
            branch_table_names = {
                t["table_name"]
                for t in branch_tables_meta
                if t["bucket_name"] == bucket_name
            }

            tables = []
            for t in main_tables:
                source = "branch" if t["name"] in branch_table_names else "main"
                tables.append(
                    TableResponse(
                        name=t["name"],
                        bucket=t["bucket"],
                        columns=[ColumnInfo(**col) for col in t["columns"]],
                        row_count=t["row_count"],
                        size_bytes=t["size_bytes"],
                        primary_key=t["primary_key"],
                        created_at=t["created_at"],
                        source=source,
                    )
                )

        return TableListResponse(
            tables=tables,
            total=len(tables),
        )

    except Exception as e:
        logger.error(
            "list_tables_failed",
            project_id=resolved_project_id,
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
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}",
    response_model=TableResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get table (ObjectInfo)",
    description="""
    Get detailed information about a table.

    - For default branch: gets from main
    - For dev branches: gets from branch if CoW'd, otherwise from main (Live View)
    """,
    dependencies=[Depends(require_project_access)],
)
async def get_table(
    project_id: str, branch_id: str, bucket_name: str, table_name: str
) -> TableResponse:
    """Get table information (ObjectInfo) - branch-aware."""
    # Resolve branch
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    logger.info(
        "get_table",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table_name,
    )

    # Validate bucket exists
    validate_project_and_bucket(resolved_project_id, resolved_branch_id, bucket_name)

    # Resolve linked bucket to source project if linked
    effective_project_id, effective_bucket_name, is_linked = resolve_linked_bucket(
        resolved_project_id, bucket_name
    )

    # Determine source
    source: Literal["main", "branch", "linked"] = "linked" if is_linked else get_table_source(
        resolved_project_id, resolved_branch_id, bucket_name, table_name
    )

    # Get table data from effective location (follows links)
    table_data = project_db_manager.get_table(
        effective_project_id, effective_bucket_name, table_name
    )
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
        source=source,
    )


@router.delete(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Delete table",
    description="""
    Delete a table from a bucket.

    - For default branch: deletes from main
    - For dev branches: deletes branch copy only (cannot delete main table from branch)
    """,
    dependencies=[Depends(require_project_access)],
)
async def delete_table(
    project_id: str,
    branch_id: str,
    bucket_name: str,
    table_name: str,
) -> None:
    """Delete a table - branch-aware."""
    start_time = time.time()
    request_id = _get_request_id()

    # Resolve branch
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    logger.info(
        "delete_table_start",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table_name,
        request_id=request_id,
    )

    # Validate bucket exists
    validate_project_and_bucket(resolved_project_id, resolved_branch_id, bucket_name)

    try:
        if resolved_branch_id is None:
            # Default branch - delete from main
            if not project_db_manager.table_exists(
                resolved_project_id, bucket_name, table_name
            ):
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

            # Check if auto-snapshot should be created before deletion
            if should_create_snapshot(
                resolved_project_id, bucket_name, table_name, "drop_table"
            ):
                from src.routers.snapshots import create_snapshot_internal

                try:
                    await create_snapshot_internal(
                        project_id=resolved_project_id,
                        bucket_name=bucket_name,
                        table_name=table_name,
                        snapshot_type="auto_predrop",
                        description=f"Auto-backup before DROP TABLE {bucket_name}.{table_name}",
                    )
                    logger.info(
                        "auto_snapshot_created_before_drop",
                        project_id=resolved_project_id,
                        bucket_name=bucket_name,
                        table_name=table_name,
                    )
                except Exception as e:
                    logger.warning(
                        "auto_snapshot_failed_before_drop",
                        project_id=resolved_project_id,
                        bucket_name=bucket_name,
                        table_name=table_name,
                        error=str(e),
                    )

            # Delete table from main
            project_db_manager.delete_table(
                project_id=resolved_project_id,
                bucket_name=bucket_name,
                table_name=table_name,
            )

            # Update project metadata
            stats = project_db_manager.get_project_stats(resolved_project_id)
            metadata_db.update_project(
                project_id=resolved_project_id,
                bucket_count=stats["bucket_count"],
                table_count=stats["table_count"],
                size_bytes=stats["size_bytes"],
            )
        else:
            # Dev branch - only delete if in branch
            is_in_branch = metadata_db.is_table_in_branch(
                resolved_branch_id, bucket_name, table_name
            )

            if not is_in_branch:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "operation_not_allowed",
                        "message": "Cannot delete table from branch - it's only in main. Use pull endpoint to copy to branch first.",
                        "details": {
                            "branch_id": branch_id,
                            "bucket_name": bucket_name,
                            "table_name": table_name,
                        },
                    },
                )

            # Delete from branch
            project_db_manager.delete_table_from_branch(
                resolved_project_id, resolved_branch_id, bucket_name, table_name
            )

            # Remove from metadata
            metadata_db.remove_table_from_branch(
                resolved_branch_id, bucket_name, table_name
            )

        duration_ms = int((time.time() - start_time) * 1000)

        # Log operation
        metadata_db.log_operation(
            operation="delete_table",
            status="success",
            project_id=resolved_project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            details={
                "bucket_name": bucket_name,
                "table_name": table_name,
                "branch_id": branch_id,
            },
            duration_ms=duration_ms,
        )

        logger.info(
            "delete_table_success",
            project_id=resolved_project_id,
            branch_id=resolved_branch_id,
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
            project_id=resolved_project_id,
            request_id=request_id,
            resource_type="table",
            resource_id=f"{bucket_name}.{table_name}",
            error_message=str(e),
            duration_ms=duration_ms,
        )

        logger.error(
            "delete_table_failed",
            project_id=resolved_project_id,
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
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/preview",
    response_model=TablePreviewResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Preview table",
    description="""
    Get a preview of table data (first N rows).

    - For default branch: previews from main
    - For dev branches: previews from branch if CoW'd, otherwise from main
    """,
    dependencies=[Depends(require_project_access)],
)
async def preview_table(
    project_id: str,
    branch_id: str,
    bucket_name: str,
    table_name: str,
    limit: int = Query(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum number of rows to return (1-10000)",
    ),
) -> TablePreviewResponse:
    """Get a preview of table data - branch-aware."""
    # Resolve branch
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)

    logger.info(
        "preview_table",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table_name,
        limit=limit,
    )

    # Validate bucket exists
    validate_project_and_bucket(resolved_project_id, resolved_branch_id, bucket_name)

    # Resolve linked bucket to source project if linked
    effective_project_id, effective_bucket_name, _ = resolve_linked_bucket(
        resolved_project_id, bucket_name
    )

    # Check if table exists
    if not _table_exists_in_context(
        resolved_project_id, resolved_branch_id, bucket_name, table_name
    ):
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
        # Get preview data from effective location (follows links)
        preview_data = project_db_manager.get_table_preview(
            project_id=effective_project_id,
            bucket_name=effective_bucket_name,
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
            project_id=resolved_project_id,
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
