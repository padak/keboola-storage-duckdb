# Phase 13: Complete Observability

**Status:** DONE
**Goal:** Full Prometheus metrics coverage for all DuckDB API operations
**Prerequisites:** Phase 12h.1 (S3-Compatible API) - DONE

---

## Executive Summary

Current metrics cover core operations (HTTP requests, table locks, PG Wire, branches). This phase adds metrics for all remaining features to enable complete observability and bottleneck detection.

**Key Insight:** Metadata DB (`metadata.duckdb`) is a shared resource - single-writer constraint means all metadata writes are serialized. Metrics are critical for detecting this bottleneck under load.

---

## Current State

| Category | Status | Metrics |
|----------|--------|---------|
| Service Health | DONE | up, start_time, info |
| HTTP Requests | DONE | count, duration, in_flight |
| Errors | DONE | errors_total |
| Storage Counts | DONE | projects, buckets, tables, workspaces |
| Storage Size | DONE | metadata, tables, staging, files |
| Table Locks | DONE | acquisitions, wait_time, active |
| Idempotency | DONE | hits, misses, size, conflicts |
| Write Queue | DONE | depth, wait_time |
| PG Wire | DONE | connections, queries, sessions, auth |
| Branches (CoW) | DONE | count, cow_ops, cow_duration, cow_bytes |
| **Metadata DB** | **DONE** | queries, duration, connections |
| **Import/Export** | **DONE** | operations, duration, rows, bytes |
| **Files API** | **DONE** | uploads, downloads, bytes |
| **Snapshots** | **DONE** | created, restored, expired, size |
| **Schema Ops** | **DONE** | add/drop column, add/drop PK |
| **gRPC** | **DONE** | requests, duration, connections |
| **S3 Compat** | **DONE** | operations, bytes in/out |
| **Bucket Sharing** | **DONE** | shares, links, operations |

---

## Implementation Plan

### Phase 13a: Metadata DB Metrics (P0 - Critical)

**Priority:** P0 - Potential bottleneck, critical for production

**Why:** Single `metadata.duckdb` file shared by all projects. All metadata writes are serialized by DuckDB's internal locking. Under high load, this becomes a bottleneck.

**New Metrics:**

```python
# src/metrics.py

METADATA_QUERIES_TOTAL = Counter(
    "duckdb_metadata_queries_total",
    "Total metadata database queries",
    ["operation"]  # read, write
)

METADATA_QUERY_DURATION = Histogram(
    "duckdb_metadata_query_duration_seconds",
    "Metadata query duration",
    ["operation"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5]
)

METADATA_CONNECTIONS_ACTIVE = Gauge(
    "duckdb_metadata_connections_active",
    "Active connections to metadata.duckdb"
)

METADATA_WRITE_QUEUE_DEPTH = Gauge(
    "duckdb_metadata_write_queue_depth",
    "Pending write operations to metadata DB"
)
```

**Instrumentation:** `src/database.py` - wrap `connection()` context manager

**Files to modify:**
- `src/metrics.py` - add metric definitions
- `src/database.py` - instrument MetadataDB class
- `docs/prometheus.md` - document new metrics
- `dashboard.html` - add Metadata DB section

**Tests:** 5-10 tests verifying metric increments

---

### Phase 13b: gRPC Metrics (P0 - Driver Communication)

**Priority:** P0 - gRPC is primary driver interface

**New Metrics:**

```python
GRPC_REQUESTS_TOTAL = Counter(
    "duckdb_grpc_requests_total",
    "Total gRPC requests",
    ["command", "status"]  # command: CreateBucket, CreateTable, etc.
)

GRPC_REQUEST_DURATION = Histogram(
    "duckdb_grpc_request_duration_seconds",
    "gRPC request duration",
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
    ["command", "error_type"]
)
```

**Instrumentation:** `src/grpc/servicer.py` - wrap Execute method

**Files to modify:**
- `src/metrics.py` - add metric definitions
- `src/grpc/servicer.py` - instrument Execute method
- `docs/prometheus.md` - document new metrics
- `dashboard.html` - add gRPC section

---

### Phase 13c: Import/Export Metrics (P1)

**Priority:** P1 - Critical data operations

**New Metrics:**

```python
IMPORT_OPERATIONS_TOTAL = Counter(
    "duckdb_import_operations_total",
    "Total import operations",
    ["format", "mode", "status"]  # format: csv/parquet, mode: full/incremental
)

IMPORT_DURATION = Histogram(
    "duckdb_import_duration_seconds",
    "Import operation duration",
    ["format"],
    buckets=[0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0, 600.0]
)

IMPORT_ROWS_TOTAL = Counter(
    "duckdb_import_rows_total",
    "Total rows imported",
    ["project_id"]
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
    "Export operation duration",
    ["format"],
    buckets=[0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0]
)

EXPORT_ROWS_TOTAL = Counter(
    "duckdb_export_rows_total",
    "Total rows exported"
)
```

**Files to modify:**
- `src/metrics.py`
- `src/routers/table_import.py`
- `src/grpc/handlers/import_export.py`
- `docs/prometheus.md`
- `dashboard.html`

---

### Phase 13d: S3 Compat Metrics (P1)

**Priority:** P1 - File storage monitoring

**New Metrics:**

```python
S3_OPERATIONS_TOTAL = Counter(
    "duckdb_s3_operations_total",
    "Total S3-compatible API operations",
    ["operation", "status"]  # GetObject, PutObject, DeleteObject, ListObjects
)

S3_OPERATION_DURATION = Histogram(
    "duckdb_s3_operation_duration_seconds",
    "S3 operation duration",
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
```

**Files to modify:**
- `src/metrics.py`
- `src/routers/s3_compat.py`
- `docs/prometheus.md`
- `dashboard.html`

---

### Phase 13e: Snapshots Metrics (P2)

**Priority:** P2 - Backup/recovery monitoring

**New Metrics:**

```python
SNAPSHOTS_TOTAL = Gauge(
    "duckdb_snapshots_total",
    "Total snapshots",
    ["type"]  # manual, auto
)

SNAPSHOTS_CREATED_TOTAL = Counter(
    "duckdb_snapshots_created_total",
    "Total snapshots created",
    ["type", "trigger"]  # trigger: manual, drop_table, truncate
)

SNAPSHOTS_RESTORED_TOTAL = Counter(
    "duckdb_snapshots_restored_total",
    "Total snapshots restored"
)

SNAPSHOT_CREATE_DURATION = Histogram(
    "duckdb_snapshot_create_duration_seconds",
    "Snapshot creation duration",
    buckets=[0.01, 0.1, 0.5, 1.0, 5.0, 30.0]
)

SNAPSHOT_RESTORE_DURATION = Histogram(
    "duckdb_snapshot_restore_duration_seconds",
    "Snapshot restore duration",
    buckets=[0.1, 0.5, 1.0, 5.0, 30.0, 60.0]
)

SNAPSHOTS_SIZE_BYTES = Gauge(
    "duckdb_snapshots_size_bytes",
    "Total snapshot storage size"
)

SNAPSHOTS_EXPIRED_TOTAL = Counter(
    "duckdb_snapshots_expired_total",
    "Total snapshots expired by retention"
)
```

**Files to modify:**
- `src/metrics.py`
- `src/routers/snapshots.py`
- `docs/prometheus.md`
- `dashboard.html`

---

### Phase 13f: Files API Metrics (P2)

**Priority:** P2 - Storage monitoring

**New Metrics:**

```python
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
    "Total bytes uploaded"
)

FILES_DOWNLOAD_BYTES_TOTAL = Counter(
    "duckdb_files_download_bytes_total",
    "Total bytes downloaded"
)

FILES_UPLOAD_DURATION = Histogram(
    "duckdb_files_upload_duration_seconds",
    "File upload duration",
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
```

**Files to modify:**
- `src/metrics.py`
- `src/routers/files.py`
- `docs/prometheus.md`
- `dashboard.html`

---

### Phase 13g: Schema Operations Metrics (P3)

**Priority:** P3 - Less frequent operations

**New Metrics:**

```python
SCHEMA_OPERATIONS_TOTAL = Counter(
    "duckdb_schema_operations_total",
    "Total schema operations",
    ["operation", "status"]  # add_column, drop_column, alter_column, add_pk, drop_pk
)

SCHEMA_OPERATION_DURATION = Histogram(
    "duckdb_schema_operation_duration_seconds",
    "Schema operation duration",
    ["operation"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]
)
```

**Files to modify:**
- `src/metrics.py`
- `src/routers/table_schema.py`
- `src/grpc/handlers/schema.py`
- `docs/prometheus.md`
- `dashboard.html`

---

### Phase 13h: Bucket Sharing Metrics (P3)

**Priority:** P3 - Administrative operations

**New Metrics:**

```python
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
    ["operation", "status"]  # share, unshare, link, unlink, grant_readonly
)
```

**Files to modify:**
- `src/metrics.py`
- `src/routers/bucket_sharing.py`
- `docs/prometheus.md`
- `dashboard.html`

---

## Dashboard Updates - DONE

All new metric sections have been added to `dashboard.html` (2024-12-21).

### New Sections in dashboard.html

1. **Metadata Database** (after Storage)
   - [x] Read/Write queries count
   - [x] Query P95 latency
   - [x] Active connections
   - [x] Average query duration per operation type

2. **Import / Export** (after Metadata DB)
   - [x] Import/Export operations count
   - [x] Rows imported/exported
   - [x] Import bytes total
   - [x] Import/Export P95 durations

3. **Files & S3 Storage** (after Import/Export)
   - [x] Total files count
   - [x] Staging files count
   - [x] S3 operations count
   - [x] Bytes in/out
   - [x] Active multipart uploads
   - [x] Upload/Download bytes

4. **Snapshots** (after Files)
   - [x] Restored/Expired counts
   - [x] Total snapshot size
   - [x] Create/Restore P95 durations

5. **gRPC Driver Interface** (after PG Wire)
   - [x] Total requests
   - [x] Active connections
   - [x] Request P95 latency
   - [x] Error rate with count
   - [x] Unique commands count

6. **Schema Operations & Bucket Sharing** (new section)
   - [x] Schema operations count with P95
   - [x] Schema breakdown by operation type
   - [x] Bucket shares count
   - [x] Sharing operations count

---

## Implementation Checklist

### Phase 13a: Metadata DB (P0) - DONE
- [x] Add metric definitions to `metrics.py`
- [x] Instrument `MetadataDB.connection()` context manager
- [x] Instrument execute/execute_write methods
- [x] Update `prometheus.md`
- [x] Update `dashboard.html`
- [x] Write tests

### Phase 13b: gRPC (P0) - DONE
- [x] Add metric definitions to `metrics.py`
- [x] Instrument `StorageDriverServicer.Execute()`
- [x] Track active connections via MetricsInterceptor
- [x] Update `prometheus.md`
- [x] Update `dashboard.html`
- [x] Write tests

### Phase 13c: Import/Export (P1) - DONE
- [x] Add metric definitions to `metrics.py`
- [x] Instrument `table_import.py` endpoints
- [x] Track rows and bytes
- [x] Update `prometheus.md`
- [x] Update `dashboard.html`
- [x] Write tests

### Phase 13d: S3 Compat (P1) - DONE
- [x] Add metric definitions to `metrics.py`
- [x] Instrument `s3_compat.py` endpoints
- [x] Track bytes in/out
- [x] Update `prometheus.md`
- [x] Update `dashboard.html`
- [x] Write tests

### Phase 13e: Snapshots (P2) - DONE
- [x] Add metric definitions to `metrics.py`
- [x] Instrument `snapshots.py` endpoints
- [x] Track create/restore duration
- [x] Update `prometheus.md`
- [x] Update `dashboard.html`
- [x] Write tests

### Phase 13f: Files API (P2) - DONE
- [x] Add metric definitions to `metrics.py`
- [x] Instrument `files.py` endpoints
- [x] Track upload/download bytes
- [x] Update `prometheus.md`
- [x] Update `dashboard.html`
- [x] Write tests

### Phase 13g: Schema Ops (P3) - DONE
- [x] Add metric definitions to `metrics.py`
- [x] Instrument `table_schema.py` endpoints
- [x] Update `prometheus.md`
- [x] Update `dashboard.html`
- [x] Write tests

### Phase 13h: Bucket Sharing (P3) - DONE
- [x] Add metric definitions to `metrics.py`
- [x] Instrument `bucket_sharing.py`
- [x] Update `prometheus.md`
- [x] Update `dashboard.html`
- [x] Write tests

---

## Estimated Test Count

| Sub-phase | New Tests |
|-----------|-----------|
| 13a Metadata DB | 8 |
| 13b gRPC | 10 |
| 13c Import/Export | 12 |
| 13d S3 Compat | 8 |
| 13e Snapshots | 8 |
| 13f Files API | 6 |
| 13g Schema Ops | 6 |
| 13h Bucket Sharing | 4 |
| **Total** | **62** |

---

## Alerts to Add

```yaml
groups:
  - name: duckdb-api-extended
    rules:
      # Metadata DB bottleneck
      - alert: MetadataDBHighLatency
        expr: |
          histogram_quantile(0.95,
            sum(rate(duckdb_metadata_query_duration_seconds_bucket[5m])) by (le, operation)
          ) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Metadata DB P95 latency above 100ms"

      # gRPC errors
      - alert: gRPCHighErrorRate
        expr: |
          sum(rate(duckdb_grpc_requests_total{status!="success"}[5m]))
          / sum(rate(duckdb_grpc_requests_total[5m])) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "gRPC error rate above 5%"

      # Import failures
      - alert: ImportHighFailureRate
        expr: |
          sum(rate(duckdb_import_operations_total{status="error"}[5m]))
          / sum(rate(duckdb_import_operations_total[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Import failure rate above 10%"

      # Snapshot storage growth
      - alert: SnapshotStorageHigh
        expr: duckdb_snapshots_size_bytes > 50e9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Snapshot storage exceeds 50GB"
```

---

## Related Documents

- [Phase 1: Backend + Observability](phase-01-backend.md)
- [Prometheus Metrics Documentation](../../duckdb-api-service/docs/prometheus.md)
- [Dashboard](../../duckdb-api-service/dashboard.html)
- [ADR-008: Central Metadata Database](../adr/008-central-metadata-database.md)
