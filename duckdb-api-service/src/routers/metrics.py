"""Prometheus metrics endpoint router.

Exposes /metrics endpoint for Prometheus scraping.
Also provides storage metrics collection.
"""

import os
from pathlib import Path

import duckdb
import structlog
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.config import settings
from src.database import metadata_db
from src.metrics import (
    PROJECTS_TOTAL,
    BUCKETS_TOTAL,
    TABLES_TOTAL,
    STORAGE_SIZE_BYTES,
    IDEMPOTENCY_CACHE_SIZE,
    TABLE_LOCKS_ACTIVE,
    set_service_info,
)

logger = structlog.get_logger()

router = APIRouter(tags=["metrics"])


def get_directory_size(path: Path) -> int:
    """Calculate total size of all files in a directory recursively."""
    total = 0
    if path.exists():
        for item in path.rglob("*"):
            if item.is_file():
                try:
                    total += item.stat().st_size
                except OSError:
                    pass
    return total


def collect_storage_metrics() -> None:
    """Collect current storage metrics from database and filesystem."""
    try:
        # Count projects
        project_count = metadata_db.count_projects()
        PROJECTS_TOTAL.set(project_count)

        # Count buckets
        bucket_count = metadata_db.count_buckets()
        BUCKETS_TOTAL.set(bucket_count)

        # Count tables
        table_count = metadata_db.count_tables()
        TABLES_TOTAL.set(table_count)

        # Metadata DB size
        metadata_path = settings.metadata_db_path
        if metadata_path.exists():
            STORAGE_SIZE_BYTES.labels(type="metadata").set(metadata_path.stat().st_size)

        # Tables storage size
        duckdb_path = settings.duckdb_dir
        STORAGE_SIZE_BYTES.labels(type="tables").set(get_directory_size(duckdb_path))

        # Staging storage size
        staging_path = settings.duckdb_dir / "_staging"
        STORAGE_SIZE_BYTES.labels(type="staging").set(get_directory_size(staging_path))

        # Files storage size (if exists)
        files_path = settings.files_dir
        if files_path.exists():
            STORAGE_SIZE_BYTES.labels(type="files").set(get_directory_size(files_path))

        # Idempotency cache size
        cache_size = metadata_db.count_idempotency_keys()
        IDEMPOTENCY_CACHE_SIZE.set(cache_size)

        # Table locks (from TableLockManager)
        # This is imported here to avoid circular imports
        try:
            from src.database import table_lock_manager
            TABLE_LOCKS_ACTIVE.set(table_lock_manager.active_locks_count)
        except Exception:
            TABLE_LOCKS_ACTIVE.set(0)

    except Exception as e:
        logger.error("metrics_collection_failed", error=str(e))


@router.get(
    "/metrics",
    response_class=PlainTextResponse,
    summary="Prometheus metrics endpoint",
    description="Returns metrics in Prometheus text format for scraping.",
)
async def get_metrics():
    """
    Expose Prometheus metrics.

    This endpoint is intentionally not authenticated to allow
    Prometheus scraping without credentials.

    Returns metrics in text/plain format using Prometheus exposition format.
    """
    # Set service info on first call
    try:
        set_service_info(
            version=settings.api_version,
            duckdb_version=duckdb.__version__
        )
    except Exception:
        pass

    # Collect storage metrics before returning
    collect_storage_metrics()

    # Generate Prometheus format
    metrics_output = generate_latest()

    return PlainTextResponse(
        content=metrics_output,
        media_type=CONTENT_TYPE_LATEST
    )
