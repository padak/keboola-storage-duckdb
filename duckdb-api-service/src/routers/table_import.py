"""Table Import/Export endpoints: data loading and extraction.

Import Pipeline (3-stage):
1. STAGING: Load file into staging table using COPY FROM
2. TRANSFORM: Deduplicate and merge into target table
3. CLEANUP: Drop staging table and return statistics

Export:
- Export table data to CSV or Parquet file
- Support filtering, column selection, and compression
"""

import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import structlog
from fastapi import APIRouter, Depends, HTTPException, status

from src.config import settings
from src.database import (
    TABLE_DATA_NAME,
    metadata_db,
    project_db_manager,
    table_lock_manager,
)
from src.dependencies import require_project_access
from src.models.responses import (
    ErrorResponse,
    ExportRequest,
    ExportResponse,
    ImportFromFileRequest,
    ImportResponse,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["import-export"])


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        import structlog
        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


def _validate_project_bucket_table(
    project_id: str, bucket_name: str, table_name: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Validate that project, bucket, and table exist.

    Returns:
        Tuple of (project_dict, table_dict) if valid

    Raises:
        HTTPException if any resource not found
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
    table = project_db_manager.get_table(project_id, bucket_name, table_name)
    if not table:
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

    return project, table


def _get_file_path(project_id: str, file_id: str) -> Path:
    """
    Get the physical path for a file.

    Validates file exists and belongs to project.

    Raises:
        HTTPException if file not found
    """
    file_record = metadata_db.get_file_by_project(project_id, file_id)
    if not file_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "file_not_found",
                "message": f"File {file_id} not found in project {project_id}",
                "details": {"project_id": project_id, "file_id": file_id},
            },
        )

    file_path = settings.files_dir / file_record["path"]
    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "file_content_not_found",
                "message": "File content not found on disk",
                "details": {"file_id": file_id},
            },
        )

    return file_path


def _build_copy_from_sql(
    file_path: Path,
    format: str,
    csv_options: dict | None = None,
) -> str:
    """Build DuckDB COPY FROM SQL statement."""
    options = []

    if format == "csv":
        options.append("FORMAT CSV")
        if csv_options:
            if csv_options.get("delimiter"):
                options.append(f"DELIMITER '{csv_options['delimiter']}'")
            if csv_options.get("quote"):
                options.append(f"QUOTE '{csv_options['quote']}'")
            if csv_options.get("escape"):
                options.append(f"ESCAPE '{csv_options['escape']}'")
            if csv_options.get("header") is not None:
                options.append(f"HEADER {'true' if csv_options['header'] else 'false'}")
            if csv_options.get("null_string"):
                options.append(f"NULLSTR '{csv_options['null_string']}'")
        else:
            # Default CSV options
            options.append("HEADER true")
    elif format == "parquet":
        options.append("FORMAT PARQUET")
    else:
        raise ValueError(f"Unsupported format: {format}")

    options_str = ", ".join(options)
    return f"COPY staging FROM '{file_path}' ({options_str})"


def _build_dedup_sql(
    target_columns: list[str],
    primary_key: list[str] | None,
    dedup_mode: str,
) -> list[str]:
    """
    Build SQL statements for deduplication and merge.

    Args:
        target_columns: List of column names in target table
        primary_key: Primary key columns (None if no PK)
        dedup_mode: How to handle duplicates

    Returns:
        List of SQL statements to execute
    """
    statements = []

    if not primary_key:
        # No primary key - simple INSERT (no dedup possible)
        statements.append(
            f"INSERT INTO main.{TABLE_DATA_NAME} SELECT * FROM staging"
        )
        return statements

    # Build column lists
    pk_cols = ", ".join(primary_key)
    all_cols = ", ".join(target_columns)
    update_cols = [c for c in target_columns if c not in primary_key]

    if dedup_mode == "fail_on_duplicates":
        # Try insert, will fail if duplicates exist
        statements.append(
            f"INSERT INTO main.{TABLE_DATA_NAME} SELECT * FROM staging"
        )
    elif dedup_mode == "insert_duplicates":
        # Insert all rows, including duplicates
        statements.append(
            f"INSERT INTO main.{TABLE_DATA_NAME} SELECT * FROM staging"
        )
    else:  # update_duplicates (default)
        # Use INSERT ON CONFLICT for upsert
        if update_cols:
            update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            statements.append(
                f"""INSERT INTO main.{TABLE_DATA_NAME} ({all_cols})
                SELECT {all_cols} FROM staging
                ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set}"""
            )
        else:
            # Only PK columns, nothing to update
            statements.append(
                f"""INSERT INTO main.{TABLE_DATA_NAME} ({all_cols})
                SELECT {all_cols} FROM staging
                ON CONFLICT ({pk_cols}) DO NOTHING"""
            )

    return statements


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/import/file",
    response_model=ImportResponse,
    status_code=status.HTTP_200_OK,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Import from file",
    description="Import data from a file into a table using 3-stage pipeline.",
    dependencies=[Depends(require_project_access)],
)
async def import_from_file(
    project_id: str,
    bucket_name: str,
    table_name: str,
    request: ImportFromFileRequest,
) -> ImportResponse:
    """
    Import data from a file into a table.

    This implements the 3-stage import pipeline:
    1. STAGING: Create staging table and load file data
    2. TRANSFORM: Deduplicate and merge into target
    3. CLEANUP: Drop staging table

    Supports:
    - CSV and Parquet formats
    - Full load (truncate + insert) or incremental (merge/upsert)
    - Deduplication based on primary key
    """
    start_time = time.time()
    request_id = _get_request_id()
    warnings: list[str] = []

    logger.info(
        "import_from_file_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        file_id=request.file_id,
        format=request.format,
        incremental=request.import_options.incremental,
        dedup_mode=request.import_options.dedup_mode,
        request_id=request_id,
    )

    # Validate resources
    project, table_info = _validate_project_bucket_table(
        project_id, bucket_name, table_name
    )

    # Get file path
    file_path = _get_file_path(project_id, request.file_id)

    # Validate format
    if request.format not in ("csv", "parquet"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_format",
                "message": f"Unsupported format: {request.format}. Use 'csv' or 'parquet'.",
                "details": {"format": request.format},
            },
        )

    # Get table path
    table_path = project_db_manager.get_table_path(project_id, bucket_name, table_name)

    # Get column info from target table
    target_columns = [col["name"] for col in table_info["columns"]]
    primary_key = table_info.get("primary_key", [])

    # Execute import with table lock
    with table_lock_manager.acquire(project_id, bucket_name, table_name):
        conn = duckdb.connect(str(table_path))
        try:
            # Get row count before
            rows_before = conn.execute(
                f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
            ).fetchone()[0]

            # Stage 1: Create staging table and load data
            logger.debug("import_stage_1_staging", request_id=request_id)

            # Create staging table with same structure as target
            column_defs = ", ".join([
                f"{col['name']} {col['type']}"
                for col in table_info["columns"]
            ])
            conn.execute(f"CREATE TEMPORARY TABLE staging ({column_defs})")

            # Build and execute COPY FROM
            csv_opts = request.csv_options.model_dump() if request.csv_options else None
            copy_sql = _build_copy_from_sql(file_path, request.format, csv_opts)

            try:
                conn.execute(copy_sql)
            except Exception as e:
                error_msg = str(e)
                logger.error(
                    "import_copy_failed",
                    error=error_msg,
                    request_id=request_id,
                )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "import_failed",
                        "message": f"Failed to load file: {error_msg}",
                        "details": {"file_id": request.file_id, "format": request.format},
                    },
                )

            # Count rows in staging
            staging_rows = conn.execute("SELECT COUNT(*) FROM staging").fetchone()[0]
            logger.debug(
                "import_staging_complete",
                staging_rows=staging_rows,
                request_id=request_id,
            )

            # Stage 2: Transform - truncate (if not incremental) and merge
            logger.debug("import_stage_2_transform", request_id=request_id)

            if not request.import_options.incremental:
                # Full load - truncate table first
                conn.execute(f"DELETE FROM main.{TABLE_DATA_NAME}")
                logger.debug("import_table_truncated", request_id=request_id)

            # Build and execute dedup/merge SQL
            dedup_statements = _build_dedup_sql(
                target_columns,
                primary_key if primary_key else None,
                request.import_options.dedup_mode,
            )

            try:
                for sql in dedup_statements:
                    conn.execute(sql)
            except duckdb.ConstraintException as e:
                if request.import_options.dedup_mode == "fail_on_duplicates":
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "duplicate_key",
                            "message": f"Duplicate key violation: {str(e)}",
                            "details": {"dedup_mode": request.import_options.dedup_mode},
                        },
                    )
                raise

            # Stage 3: Cleanup and get stats
            logger.debug("import_stage_3_cleanup", request_id=request_id)

            conn.execute("DROP TABLE IF EXISTS staging")
            conn.commit()

            # Get final stats
            rows_after = conn.execute(
                f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
            ).fetchone()[0]

        finally:
            conn.close()

    # Calculate imported rows
    if request.import_options.incremental:
        imported_rows = rows_after - rows_before
    else:
        imported_rows = staging_rows  # Full load = staging count

    # Get table size after import
    table_size = table_path.stat().st_size

    duration_ms = int((time.time() - start_time) * 1000)

    logger.info(
        "import_from_file_complete",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        file_id=request.file_id,
        staging_rows=staging_rows,
        imported_rows=imported_rows,
        rows_before=rows_before,
        rows_after=rows_after,
        duration_ms=duration_ms,
        request_id=request_id,
    )

    # Log operation
    metadata_db.log_operation(
        operation="import_from_file",
        status="success",
        project_id=project_id,
        request_id=request_id,
        resource_type="table",
        resource_id=f"{bucket_name}.{table_name}",
        details={
            "file_id": request.file_id,
            "format": request.format,
            "incremental": request.import_options.incremental,
            "imported_rows": imported_rows,
        },
        duration_ms=duration_ms,
    )

    return ImportResponse(
        imported_rows=imported_rows,
        table_rows_after=rows_after,
        table_size_bytes=table_size,
        warnings=warnings,
    )


@router.post(
    "/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/export",
    response_model=ExportResponse,
    status_code=status.HTTP_200_OK,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Export to file",
    description="Export table data to a file (CSV or Parquet).",
    dependencies=[Depends(require_project_access)],
)
async def export_to_file(
    project_id: str,
    bucket_name: str,
    table_name: str,
    request: ExportRequest,
) -> ExportResponse:
    """
    Export table data to a file.

    Supports:
    - CSV and Parquet formats
    - Column selection
    - Row filtering with WHERE clause
    - Row limit
    - Compression (gzip for CSV, gzip/zstd/snappy for Parquet)
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "export_to_file_start",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        format=request.format,
        compression=request.compression,
        request_id=request_id,
    )

    # Validate resources
    project, table_info = _validate_project_bucket_table(
        project_id, bucket_name, table_name
    )

    # Validate format
    if request.format not in ("csv", "parquet"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_format",
                "message": f"Unsupported format: {request.format}. Use 'csv' or 'parquet'.",
                "details": {"format": request.format},
            },
        )

    # Validate compression
    valid_compression = {
        "csv": [None, "gzip"],
        "parquet": [None, "gzip", "zstd", "snappy"],
    }
    if request.compression not in valid_compression[request.format]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_compression",
                "message": f"Invalid compression for {request.format}: {request.compression}",
                "details": {
                    "format": request.format,
                    "valid_compression": valid_compression[request.format],
                },
            },
        )

    # Validate WHERE clause if provided (basic SQL injection prevention)
    if request.where_filter:
        dangerous_patterns = [";", "--", "/*", "*/", "drop ", "truncate ", "alter ", "delete ", "insert ", "update "]
        where_lower = request.where_filter.lower()
        for pattern in dangerous_patterns:
            if pattern in where_lower:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "invalid_where_clause",
                        "message": f"Invalid WHERE clause: contains '{pattern}'",
                        "details": {"where_filter": request.where_filter},
                    },
                )

    # Generate export file ID and path
    file_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    # Determine file extension
    ext = request.format
    if request.compression == "gzip":
        ext += ".gz"

    export_filename = f"export_{table_name}_{now.strftime('%Y%m%d_%H%M%S')}.{ext}"

    # Create export directory (date-organized)
    export_dir = settings.files_dir / f"project_{project_id}" / now.strftime("%Y/%m/%d")
    export_dir.mkdir(parents=True, exist_ok=True)

    export_path = export_dir / f"{file_id}_{export_filename}"

    # Build SELECT query
    columns = request.columns if request.columns else ["*"]
    columns_sql = ", ".join(columns)

    # Get table path
    table_path = project_db_manager.get_table_path(project_id, bucket_name, table_name)

    # Execute export (read-only, no lock needed)
    conn = duckdb.connect(str(table_path), read_only=True)
    try:
        # Build query
        query = f"SELECT {columns_sql} FROM main.{TABLE_DATA_NAME}"
        if request.where_filter:
            query += f" WHERE {request.where_filter}"
        if request.limit:
            query += f" LIMIT {request.limit}"

        # Count rows for export
        count_query = f"SELECT COUNT(*) FROM ({query}) AS export_data"
        rows_exported = conn.execute(count_query).fetchone()[0]

        # Build COPY TO options
        options = []
        if request.format == "csv":
            options.append("FORMAT CSV")
            options.append("HEADER true")
            if request.compression == "gzip":
                options.append("COMPRESSION GZIP")
        else:  # parquet
            options.append("FORMAT PARQUET")
            if request.compression:
                options.append(f"COMPRESSION {request.compression.upper()}")

        options_str = ", ".join(options)

        # Execute COPY TO
        copy_sql = f"COPY ({query}) TO '{export_path}' ({options_str})"
        conn.execute(copy_sql)

    finally:
        conn.close()

    # Get file size
    file_size = export_path.stat().st_size

    # Compute relative path for storage
    relative_path = str(export_path.relative_to(settings.files_dir))

    # Create file record in metadata
    file_record = metadata_db.create_file_record(
        file_id=file_id,
        project_id=project_id,
        name=export_filename,
        path=relative_path,
        size_bytes=file_size,
        content_type="text/csv" if request.format == "csv" else "application/x-parquet",
        checksum_sha256=None,  # Skip checksum for exports
        is_staged=False,
        expires_at=None,
        tags={"type": "export", "table": f"{bucket_name}.{table_name}"},
    )

    duration_ms = int((time.time() - start_time) * 1000)

    logger.info(
        "export_to_file_complete",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        file_id=file_id,
        rows_exported=rows_exported,
        file_size_bytes=file_size,
        duration_ms=duration_ms,
        request_id=request_id,
    )

    # Log operation
    metadata_db.log_operation(
        operation="export_to_file",
        status="success",
        project_id=project_id,
        request_id=request_id,
        resource_type="table",
        resource_id=f"{bucket_name}.{table_name}",
        details={
            "file_id": file_id,
            "format": request.format,
            "rows_exported": rows_exported,
        },
        duration_ms=duration_ms,
    )

    return ExportResponse(
        file_id=file_id,
        file_path=relative_path,
        rows_exported=rows_exported,
        file_size_bytes=file_size,
    )
