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
