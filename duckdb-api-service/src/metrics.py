"""Prometheus metrics definitions for DuckDB API Service.

This module defines all Prometheus metrics used for observability:
- HTTP request metrics (count, duration, in-flight)
- DuckDB operation metrics (queries, duration)
- Storage metrics (database sizes, table counts)
- Idempotency cache metrics
- Process metrics (CPU, memory, file descriptors)
"""

import platform
import time
from prometheus_client import Counter, Histogram, Gauge, Info
from prometheus_client import REGISTRY, ProcessCollector

# Register ProcessCollector for process_* metrics
# Note: ProcessCollector only works on Linux (uses /proc filesystem)
# On macOS/Windows, these metrics will not be available
if platform.system() == "Linux":
    try:
        # ProcessCollector provides:
        # - process_cpu_seconds_total
        # - process_resident_memory_bytes
        # - process_virtual_memory_bytes
        # - process_open_fds
        # - process_max_fds
        # - process_start_time_seconds
        ProcessCollector()
    except Exception:
        pass  # Silently fail if already registered or not supported

# =============================================================================
# Service Health Metrics
# =============================================================================

SERVICE_UP = Gauge(
    "duckdb_api_up",
    "Whether the DuckDB API service is up (1) or down (0)"
)

SERVICE_START_TIME = Gauge(
    "duckdb_api_start_time_seconds",
    "Unix timestamp when the service started"
)

# Initialize start time when module loads
_start_time = time.time()
SERVICE_START_TIME.set(_start_time)
SERVICE_UP.set(1)

# =============================================================================
# HTTP Request Metrics
# =============================================================================

REQUEST_COUNT = Counter(
    "duckdb_api_requests_total",
    "Total number of HTTP requests",
    ["method", "endpoint", "status_code"]
)

REQUEST_DURATION = Histogram(
    "duckdb_api_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

REQUEST_IN_FLIGHT = Gauge(
    "duckdb_api_requests_in_flight",
    "Number of HTTP requests currently being processed",
    ["method"]
)

# =============================================================================
# Error Metrics
# =============================================================================

ERROR_COUNT = Counter(
    "duckdb_api_errors_total",
    "Total number of errors by type",
    ["type", "endpoint"]
)

# =============================================================================
# DuckDB Operation Metrics
# =============================================================================

OPERATION_COUNT = Counter(
    "duckdb_operations_total",
    "Total number of DuckDB operations",
    ["operation", "status"]
)

OPERATION_DURATION = Histogram(
    "duckdb_operation_duration_seconds",
    "DuckDB operation duration in seconds",
    ["operation"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0]
)

# =============================================================================
# Storage Metrics (collected on-demand)
# =============================================================================

PROJECTS_TOTAL = Gauge(
    "duckdb_projects_total",
    "Total number of projects"
)

BUCKETS_TOTAL = Gauge(
    "duckdb_buckets_total",
    "Total number of buckets across all projects"
)

TABLES_TOTAL = Gauge(
    "duckdb_tables_total",
    "Total number of tables across all projects"
)

WORKSPACES_TOTAL = Gauge(
    "duckdb_workspaces_total",
    "Total number of workspaces across all projects"
)

STORAGE_SIZE_BYTES = Gauge(
    "duckdb_storage_size_bytes",
    "Total storage size in bytes",
    ["type"]  # metadata, tables, staging
)

# =============================================================================
# Idempotency Cache Metrics
# =============================================================================

IDEMPOTENCY_CACHE_HITS = Counter(
    "duckdb_idempotency_cache_hits_total",
    "Total number of idempotency cache hits"
)

IDEMPOTENCY_CACHE_MISSES = Counter(
    "duckdb_idempotency_cache_misses_total",
    "Total number of idempotency cache misses"
)

IDEMPOTENCY_CACHE_SIZE = Gauge(
    "duckdb_idempotency_cache_size",
    "Current number of entries in idempotency cache"
)

IDEMPOTENCY_CACHE_CONFLICTS = Counter(
    "duckdb_idempotency_conflicts_total",
    "Total number of idempotency key conflicts"
)

# =============================================================================
# Write Queue Metrics (prepared for future use)
# =============================================================================

WRITE_QUEUE_DEPTH = Gauge(
    "duckdb_write_queue_depth",
    "Number of operations waiting in write queue",
    ["project_id"]
)

WRITE_QUEUE_WAIT_TIME = Histogram(
    "duckdb_write_queue_wait_seconds",
    "Time spent waiting in write queue",
    ["project_id"],
    buckets=[0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
)

# =============================================================================
# Table Lock Metrics
# =============================================================================

TABLE_LOCK_ACQUISITIONS = Counter(
    "duckdb_table_lock_acquisitions_total",
    "Total number of table lock acquisitions",
    ["project_id", "bucket", "table"]
)

TABLE_LOCK_WAIT_TIME = Histogram(
    "duckdb_table_lock_wait_seconds",
    "Time spent waiting for table lock",
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
)

TABLE_LOCKS_ACTIVE = Gauge(
    "duckdb_table_locks_active",
    "Number of currently held table locks"
)

# =============================================================================
# PG Wire Server Metrics (Phase 11c)
# =============================================================================

PGWIRE_CONNECTIONS_TOTAL = Counter(
    "pgwire_connections_total",
    "Total PG Wire connection attempts",
    ["status"]  # success, auth_failed, expired, limit_reached
)

PGWIRE_CONNECTIONS_ACTIVE = Gauge(
    "pgwire_connections_active",
    "Active PG Wire connections",
    ["workspace_id"]
)

PGWIRE_QUERIES_TOTAL = Counter(
    "pgwire_queries_total",
    "Total PG Wire queries executed",
    ["workspace_id", "status"]  # success, error, timeout
)

PGWIRE_QUERY_DURATION = Histogram(
    "pgwire_query_duration_seconds",
    "PG Wire query duration in seconds",
    ["workspace_id"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
)

PGWIRE_SESSIONS_TOTAL = Gauge(
    "pgwire_sessions_total",
    "Total active PG Wire sessions"
)

PGWIRE_AUTH_DURATION = Histogram(
    "pgwire_auth_duration_seconds",
    "PG Wire authentication duration",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
)

# =============================================================================
# Branch Metrics (ADR-007: CoW branching)
# =============================================================================

BRANCHES_TOTAL = Gauge(
    "duckdb_branches_total",
    "Total number of dev branches"
)

BRANCH_COW_OPERATIONS = Counter(
    "duckdb_branch_cow_operations_total",
    "Total number of Copy-on-Write operations",
    ["project_id", "branch_id"]
)

BRANCH_COW_DURATION = Histogram(
    "duckdb_branch_cow_duration_seconds",
    "Duration of Copy-on-Write operations",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]
)

BRANCH_COW_SIZE_BYTES = Counter(
    "duckdb_branch_cow_bytes_total",
    "Total bytes copied in CoW operations",
    ["project_id", "branch_id"]
)

BRANCH_TABLES_TOTAL = Gauge(
    "duckdb_branch_tables_total",
    "Total number of tables copied to branches"
)

# =============================================================================
# Metadata DB Metrics (Phase 13a)
# =============================================================================

METADATA_QUERIES_TOTAL = Counter(
    "duckdb_metadata_queries_total",
    "Total metadata database queries",
    ["operation"]  # read, write
)

METADATA_QUERY_DURATION = Histogram(
    "duckdb_metadata_query_duration_seconds",
    "Metadata query duration in seconds",
    ["operation"],  # read, write
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5]
)

METADATA_CONNECTIONS_ACTIVE = Gauge(
    "duckdb_metadata_connections_active",
    "Active connections to metadata.duckdb"
)

# =============================================================================
# gRPC Metrics (Phase 13b)
# =============================================================================

GRPC_REQUESTS_TOTAL = Counter(
    "duckdb_grpc_requests_total",
    "Total gRPC requests",
    ["command", "status"]  # command: CreateBucket, CreateTable, etc.; status: success, error
)

GRPC_REQUEST_DURATION = Histogram(
    "duckdb_grpc_request_duration_seconds",
    "gRPC request duration in seconds",
    ["command"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 30.0]
)

GRPC_CONNECTIONS_ACTIVE = Gauge(
    "duckdb_grpc_connections_active",
    "Active gRPC connections"
)

GRPC_ERRORS_TOTAL = Counter(
    "duckdb_grpc_errors_total",
    "gRPC errors by type",
    ["command", "error_type"]  # error_type: invalid_argument, not_found, internal, unimplemented
)

# =============================================================================
# Import/Export Metrics (Phase 13c)
# =============================================================================

IMPORT_OPERATIONS_TOTAL = Counter(
    "duckdb_import_operations_total",
    "Total import operations",
    ["format", "mode", "status"]  # format: csv/parquet, mode: full/incremental, status: success/error
)

IMPORT_DURATION = Histogram(
    "duckdb_import_duration_seconds",
    "Import operation duration in seconds",
    ["format"],
    buckets=[0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0, 600.0]
)

IMPORT_ROWS_TOTAL = Counter(
    "duckdb_import_rows_total",
    "Total rows imported"
)

IMPORT_BYTES_TOTAL = Counter(
    "duckdb_import_bytes_total",
    "Total bytes imported",
    ["format"]
)

EXPORT_OPERATIONS_TOTAL = Counter(
    "duckdb_export_operations_total",
    "Total export operations",
    ["format", "status"]
)

EXPORT_DURATION = Histogram(
    "duckdb_export_duration_seconds",
    "Export operation duration in seconds",
    ["format"],
    buckets=[0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0]
)

EXPORT_ROWS_TOTAL = Counter(
    "duckdb_export_rows_total",
    "Total rows exported"
)

# =============================================================================
# S3 Compat Metrics (Phase 13d)
# =============================================================================

S3_OPERATIONS_TOTAL = Counter(
    "duckdb_s3_operations_total",
    "Total S3-compatible API operations",
    ["operation", "status"]  # operation: GetObject, PutObject, DeleteObject, ListObjects, HeadObject
)

S3_OPERATION_DURATION = Histogram(
    "duckdb_s3_operation_duration_seconds",
    "S3 operation duration in seconds",
    ["operation"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]
)

S3_BYTES_IN_TOTAL = Counter(
    "duckdb_s3_bytes_in_total",
    "Total bytes received via S3 API"
)

S3_BYTES_OUT_TOTAL = Counter(
    "duckdb_s3_bytes_out_total",
    "Total bytes sent via S3 API"
)

S3_MULTIPART_UPLOADS_ACTIVE = Gauge(
    "duckdb_s3_multipart_uploads_active",
    "Active multipart uploads"
)

S3_PRESIGN_REQUESTS_TOTAL = Counter(
    "duckdb_s3_presign_requests_total",
    "Pre-signed URL generation requests",
    ["method"]  # GET, PUT
)

# =============================================================================
# Snapshots Metrics (Phase 13e)
# =============================================================================

SNAPSHOTS_TOTAL = Gauge(
    "duckdb_snapshots_total",
    "Total snapshots",
    ["type"]  # manual, auto
)

SNAPSHOTS_CREATED_TOTAL = Counter(
    "duckdb_snapshots_created_total",
    "Total snapshots created",
    ["type", "trigger"]  # type: manual/auto, trigger: manual, drop_table, truncate
)

SNAPSHOTS_RESTORED_TOTAL = Counter(
    "duckdb_snapshots_restored_total",
    "Total snapshots restored"
)

SNAPSHOT_CREATE_DURATION = Histogram(
    "duckdb_snapshot_create_duration_seconds",
    "Snapshot creation duration in seconds",
    buckets=[0.01, 0.1, 0.5, 1.0, 5.0, 30.0]
)

SNAPSHOT_RESTORE_DURATION = Histogram(
    "duckdb_snapshot_restore_duration_seconds",
    "Snapshot restore duration in seconds",
    buckets=[0.1, 0.5, 1.0, 5.0, 30.0, 60.0]
)

SNAPSHOTS_SIZE_BYTES = Gauge(
    "duckdb_snapshots_size_bytes",
    "Total snapshot storage size in bytes"
)

SNAPSHOTS_EXPIRED_TOTAL = Counter(
    "duckdb_snapshots_expired_total",
    "Total snapshots expired by retention"
)

# =============================================================================
# Files API Metrics (Phase 13f)
# =============================================================================

FILES_UPLOADS_TOTAL = Counter(
    "duckdb_files_uploads_total",
    "Total file uploads",
    ["status"]  # success, error
)

FILES_DOWNLOADS_TOTAL = Counter(
    "duckdb_files_downloads_total",
    "Total file downloads",
    ["status"]
)

FILES_UPLOAD_BYTES_TOTAL = Counter(
    "duckdb_files_upload_bytes_total",
    "Total bytes uploaded via Files API"
)

FILES_DOWNLOAD_BYTES_TOTAL = Counter(
    "duckdb_files_download_bytes_total",
    "Total bytes downloaded via Files API"
)

FILES_UPLOAD_DURATION = Histogram(
    "duckdb_files_upload_duration_seconds",
    "File upload duration in seconds",
    buckets=[0.1, 0.5, 1.0, 5.0, 30.0, 60.0]
)

FILES_STAGING_COUNT = Gauge(
    "duckdb_files_staging_count",
    "Files in staging (pending upload)"
)

FILES_TOTAL = Gauge(
    "duckdb_files_total",
    "Total registered files"
)

# =============================================================================
# Schema Operations Metrics (Phase 13g)
# =============================================================================

SCHEMA_OPERATIONS_TOTAL = Counter(
    "duckdb_schema_operations_total",
    "Total schema operations",
    ["operation", "status"]  # operation: add_column, drop_column, alter_column, add_pk, drop_pk
)

SCHEMA_OPERATION_DURATION = Histogram(
    "duckdb_schema_operation_duration_seconds",
    "Schema operation duration in seconds",
    ["operation"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]
)

# =============================================================================
# Bucket Sharing Metrics (Phase 13h)
# =============================================================================

BUCKET_SHARES_TOTAL = Gauge(
    "duckdb_bucket_shares_total",
    "Total bucket shares"
)

BUCKET_LINKS_TOTAL = Gauge(
    "duckdb_bucket_links_total",
    "Total bucket links"
)

BUCKET_SHARING_OPERATIONS = Counter(
    "duckdb_bucket_sharing_operations_total",
    "Bucket sharing operations",
    ["operation", "status"]  # operation: share, unshare, link, unlink, grant_readonly
)

# =============================================================================
# Service Info
# =============================================================================

SERVICE_INFO = Info(
    "duckdb_api_service",
    "DuckDB API Service information"
)


def set_service_info(version: str, duckdb_version: str) -> None:
    """Set service info labels."""
    SERVICE_INFO.info({
        "version": version,
        "duckdb_version": duckdb_version
    })
