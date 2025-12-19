"""Snapshot management endpoints: create, list, get, delete, restore.

Uses branch-first URL pattern per ADR-012, but snapshots only work on default branch for MVP.
"""

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.branch_utils import require_default_branch, resolve_branch, validate_project_and_bucket
from src.config import settings
from src.database import metadata_db, project_db_manager
from src.dependencies import require_project_access
from src.models.responses import (
    ErrorResponse,
    SnapshotCreateRequest,
    SnapshotDetailResponse,
    SnapshotListResponse,
    SnapshotResponse,
    SnapshotRestoreRequest,
    SnapshotRestoreResponse,
    SnapshotSchemaColumn,
)
from src.snapshot_config import get_retention_days, resolve_snapshot_config

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["snapshots"])


def _validate_table_exists(
    project_id: str, branch_id: str | None, bucket_name: str, table_name: str
) -> None:
    """Validate that table exists in branch context."""
    validate_project_and_bucket(project_id, branch_id, bucket_name)

    # For snapshots (default branch only), always check main project
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


def _get_table_schema(project_id: str, bucket_name: str, table_name: str) -> dict:
    """Get table schema (columns and primary key)."""
    table_path = project_db_manager.get_table_path(project_id, bucket_name, table_name)

    conn = duckdb.connect(str(table_path), read_only=True)
    try:
        # Get columns
        columns_result = conn.execute("""
            SELECT column_name, data_type,
                   CASE WHEN is_nullable = 'YES' THEN true ELSE false END as nullable
            FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = 'data'
            ORDER BY ordinal_position
        """).fetchall()

        columns = [
            {"name": row[0], "type": row[1], "nullable": row[2]}
            for row in columns_result
        ]

        # Get primary key columns using constraint_column_names
        primary_key = []
        try:
            pk_result = conn.execute("""
                SELECT constraint_column_names
                FROM duckdb_constraints()
                WHERE constraint_type = 'PRIMARY KEY'
                  AND table_name = 'data'
                  AND schema_name = 'main'
            """).fetchone()
            if pk_result and pk_result[0]:
                primary_key = list(pk_result[0])
        except Exception:
            pass

        return {"columns": columns, "primary_key": primary_key}
    finally:
        conn.close()


def _snapshot_record_to_response(record: dict) -> SnapshotResponse:
    """Convert snapshot database record to API response."""
    return SnapshotResponse(
        id=record["id"],
        project_id=record["project_id"],
        bucket_name=record["bucket_name"],
        table_name=record["table_name"],
        snapshot_type=record["snapshot_type"],
        row_count=record["row_count"],
        size_bytes=record["size_bytes"],
        created_at=record["created_at"],
        created_by=record.get("created_by"),
        expires_at=record.get("expires_at"),
        description=record.get("description"),
    )


def _snapshot_record_to_detail_response(record: dict) -> SnapshotDetailResponse:
    """Convert snapshot database record to detailed API response."""
    schema_json = record.get("schema_json", {})
    columns = schema_json.get("columns", [])
    primary_key = schema_json.get("primary_key", [])

    schema_columns = [
        SnapshotSchemaColumn(
            name=col["name"],
            type=col["type"],
            nullable=col.get("nullable", True),
        )
        for col in columns
    ]

    return SnapshotDetailResponse(
        id=record["id"],
        project_id=record["project_id"],
        bucket_name=record["bucket_name"],
        table_name=record["table_name"],
        snapshot_type=record["snapshot_type"],
        row_count=record["row_count"],
        size_bytes=record["size_bytes"],
        created_at=record["created_at"],
        created_by=record.get("created_by"),
        expires_at=record.get("expires_at"),
        description=record.get("description"),
        schema_columns=schema_columns,
        primary_key=primary_key,
    )


async def create_snapshot_internal(
    project_id: str,
    bucket_name: str,
    table_name: str,
    snapshot_type: str = "manual",
    description: str | None = None,
    created_by: str | None = None,
) -> dict:
    """
    Internal function to create a snapshot.

    Used by both the API endpoint and auto-snapshot triggers.

    Returns:
        Snapshot record dict
    """
    # Check if snapshots are enabled
    config, _ = resolve_snapshot_config(project_id, bucket_name, table_name)
    if not config.get("enabled", True):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "snapshots_disabled",
                "message": f"Snapshots are disabled for table {bucket_name}.{table_name}",
                "details": {
                    "project_id": project_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )

    # Generate snapshot ID with milliseconds for uniqueness
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S") + f"_{now.microsecond // 1000:03d}"
    snapshot_id = f"snap_{table_name}_{timestamp}"

    # Get table schema
    schema_json = _get_table_schema(project_id, bucket_name, table_name)

    # Create snapshot directory
    snapshot_dir = settings.snapshots_dir / project_id / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = snapshot_dir / "data.parquet"

    # Get table path
    table_path = project_db_manager.get_table_path(project_id, bucket_name, table_name)

    # Export to Parquet
    conn = duckdb.connect(str(table_path), read_only=True)
    try:
        conn.execute(f"""
            COPY main.data TO '{parquet_path}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Get row count
        row_count = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()[0]
    finally:
        conn.close()

    # Get file size
    size_bytes = parquet_path.stat().st_size

    # Calculate expiration based on retention config
    retention_days = get_retention_days(
        project_id, bucket_name, table_name, snapshot_type
    )
    expires_at = datetime.now(timezone.utc) + timedelta(days=retention_days)

    # Save metadata JSON (redundant copy for recovery)
    metadata = {
        "snapshot_id": snapshot_id,
        "project_id": project_id,
        "bucket_name": bucket_name,
        "table_name": table_name,
        "snapshot_type": snapshot_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": created_by,
        "expires_at": expires_at.isoformat(),
        "description": description,
        "row_count": row_count,
        "size_bytes": size_bytes,
        "schema": schema_json,
    }

    with open(snapshot_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    # Register in metadata database
    snapshot_record = metadata_db.create_snapshot(
        snapshot_id=snapshot_id,
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        snapshot_type=snapshot_type,
        parquet_path=f"{project_id}/{snapshot_id}",
        row_count=row_count,
        size_bytes=size_bytes,
        schema_json=schema_json,
        expires_at=expires_at,
        created_by=created_by,
        description=description,
    )

    logger.info(
        "snapshot_created",
        snapshot_id=snapshot_id,
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        snapshot_type=snapshot_type,
        row_count=row_count,
        size_bytes=size_bytes,
    )

    return snapshot_record


@router.post(
    "/projects/{project_id}/branches/{branch_id}/snapshots",
    response_model=SnapshotResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
    summary="Create snapshot",
    description="Create a manual snapshot of a table. Only available on default branch for MVP.",
    dependencies=[Depends(require_project_access)],
)
async def create_snapshot(
    project_id: str, branch_id: str, request: SnapshotCreateRequest
) -> SnapshotResponse:
    """Create a manual snapshot of a table (default branch only)."""
    # Resolve branch and require default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "create snapshots")

    _validate_table_exists(resolved_project_id, resolved_branch_id, request.bucket, request.table)

    snapshot_record = await create_snapshot_internal(
        project_id=resolved_project_id,
        bucket_name=request.bucket,
        table_name=request.table,
        snapshot_type="manual",
        description=request.description,
    )

    logger.info(
        "snapshot_create_requested",
        project_id=resolved_project_id,
        branch_id=branch_id,
        bucket_name=request.bucket,
        table_name=request.table,
    )

    return _snapshot_record_to_response(snapshot_record)


@router.get(
    "/projects/{project_id}/branches/{branch_id}/snapshots",
    response_model=SnapshotListResponse,
    responses={404: {"model": ErrorResponse}},
    summary="List snapshots",
    description="List snapshots for a project with optional filtering. Only available on default branch for MVP.",
    dependencies=[Depends(require_project_access)],
)
async def list_snapshots(
    project_id: str,
    branch_id: str,
    bucket: str | None = Query(default=None, description="Filter by bucket name"),
    table: str | None = Query(default=None, description="Filter by table name"),
    type: str | None = Query(
        default=None, description="Filter by snapshot type (manual, auto_predrop, etc.)"
    ),
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination"),
) -> SnapshotListResponse:
    """List snapshots for a project (default branch only)."""
    # Resolve branch and require default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "list snapshots")

    snapshots = metadata_db.list_snapshots(
        project_id=resolved_project_id,
        bucket_name=bucket,
        table_name=table,
        snapshot_type=type,
        limit=limit,
        offset=offset,
    )

    total = metadata_db.count_snapshots(
        project_id=resolved_project_id,
        bucket_name=bucket,
        table_name=table,
        snapshot_type=type,
    )

    logger.info(
        "snapshots_listed",
        project_id=resolved_project_id,
        branch_id=branch_id,
        total=total,
    )

    return SnapshotListResponse(
        snapshots=[_snapshot_record_to_response(s) for s in snapshots],
        total=total,
    )


@router.get(
    "/projects/{project_id}/branches/{branch_id}/snapshots/{snapshot_id}",
    response_model=SnapshotDetailResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get snapshot details",
    description="Get detailed information about a snapshot including schema. Only available on default branch for MVP.",
    dependencies=[Depends(require_project_access)],
)
async def get_snapshot(project_id: str, branch_id: str, snapshot_id: str) -> SnapshotDetailResponse:
    """Get detailed snapshot information (default branch only)."""
    # Resolve branch and require default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "get snapshots")

    snapshot = metadata_db.get_snapshot_by_project(resolved_project_id, snapshot_id)
    if not snapshot:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "snapshot_not_found",
                "message": f"Snapshot {snapshot_id} not found",
                "details": {"project_id": resolved_project_id, "snapshot_id": snapshot_id},
            },
        )

    logger.info(
        "snapshot_retrieved",
        project_id=resolved_project_id,
        branch_id=branch_id,
        snapshot_id=snapshot_id,
    )

    return _snapshot_record_to_detail_response(snapshot)


@router.delete(
    "/projects/{project_id}/branches/{branch_id}/snapshots/{snapshot_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Delete snapshot",
    description="Delete a snapshot and its files. Only available on default branch for MVP.",
    dependencies=[Depends(require_project_access)],
)
async def delete_snapshot(project_id: str, branch_id: str, snapshot_id: str) -> None:
    """Delete a snapshot (default branch only)."""
    # Resolve branch and require default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "delete snapshots")

    snapshot = metadata_db.get_snapshot_by_project(resolved_project_id, snapshot_id)
    if not snapshot:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "snapshot_not_found",
                "message": f"Snapshot {snapshot_id} not found",
                "details": {"project_id": resolved_project_id, "snapshot_id": snapshot_id},
            },
        )

    # Delete files
    snapshot_dir = settings.snapshots_dir / resolved_project_id / snapshot_id
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)

    # Delete record
    metadata_db.delete_snapshot(snapshot_id)

    logger.info(
        "snapshot_deleted",
        snapshot_id=snapshot_id,
        project_id=resolved_project_id,
        branch_id=branch_id,
    )


@router.post(
    "/projects/{project_id}/branches/{branch_id}/snapshots/{snapshot_id}/restore",
    response_model=SnapshotRestoreResponse,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
    summary="Restore from snapshot",
    description="Restore a table from a snapshot. Only available on default branch for MVP.",
    dependencies=[Depends(require_project_access)],
)
async def restore_snapshot(
    project_id: str, branch_id: str, snapshot_id: str, request: SnapshotRestoreRequest
) -> SnapshotRestoreResponse:
    """Restore a table from a snapshot (default branch only)."""
    # Resolve branch and require default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "restore snapshots")

    snapshot = metadata_db.get_snapshot_by_project(resolved_project_id, snapshot_id)
    if not snapshot:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "snapshot_not_found",
                "message": f"Snapshot {snapshot_id} not found",
                "details": {"project_id": project_id, "snapshot_id": snapshot_id},
            },
        )

    # Determine target location
    target_bucket = request.target_bucket or snapshot["bucket_name"]
    target_table = request.target_table or snapshot["table_name"]

    # Validate target bucket exists
    if not project_db_manager.bucket_exists(resolved_project_id, target_bucket):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "bucket_not_found",
                "message": f"Target bucket {target_bucket} not found",
                "details": {"project_id": resolved_project_id, "bucket_name": target_bucket},
            },
        )

    # Check if target table already exists (unless restoring to same location)
    if project_db_manager.table_exists(resolved_project_id, target_bucket, target_table):
        # If restoring to original location, we'll replace the table
        if target_bucket != snapshot["bucket_name"] or target_table != snapshot["table_name"]:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "table_exists",
                    "message": f"Table {target_table} already exists in bucket {target_bucket}",
                    "details": {
                        "project_id": resolved_project_id,
                        "bucket_name": target_bucket,
                        "table_name": target_table,
                    },
                },
            )

    # Get parquet file path
    parquet_path = settings.snapshots_dir / snapshot["parquet_path"] / "data.parquet"
    if not parquet_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "snapshot_file_not_found",
                "message": f"Snapshot file not found at {parquet_path}",
                "details": {"snapshot_id": snapshot_id},
            },
        )

    # Get or create table path
    table_path = project_db_manager.get_table_path(resolved_project_id, target_bucket, target_table)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    # Restore from Parquet
    conn = duckdb.connect(str(table_path))
    try:
        # Create table from Parquet
        conn.execute(f"""
            CREATE OR REPLACE TABLE main.data AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)

        # Add primary key if exists in schema
        schema_json = snapshot.get("schema_json", {})
        primary_key = schema_json.get("primary_key", [])
        if primary_key:
            pk_cols = ", ".join(primary_key)
            try:
                conn.execute(f"ALTER TABLE main.data ADD PRIMARY KEY ({pk_cols})")
            except Exception as e:
                logger.warning(
                    "restore_pk_failed",
                    snapshot_id=snapshot_id,
                    primary_key=primary_key,
                    error=str(e),
                )

        # Get restored row count
        row_count = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()[0]
    finally:
        conn.close()

    # Note: Table is automatically "registered" by creating the DuckDB file
    # No separate metadata registry needed per ADR-009

    logger.info(
        "snapshot_restored",
        snapshot_id=snapshot_id,
        project_id=resolved_project_id,
        branch_id=branch_id,
        source_bucket=snapshot["bucket_name"],
        source_table=snapshot["table_name"],
        target_bucket=target_bucket,
        target_table=target_table,
        row_count=row_count,
    )

    return SnapshotRestoreResponse(
        restored_to={"bucket": target_bucket, "table": target_table},
        row_count=row_count,
    )
