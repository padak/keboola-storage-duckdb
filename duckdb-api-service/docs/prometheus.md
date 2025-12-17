# Prometheus Metrics Documentation

DuckDB API Service exposes metrics at `/metrics` endpoint in Prometheus text format.

## Quick Start

```bash
# Fetch metrics
curl http://localhost:8000/metrics

# Prometheus scrape config
scrape_configs:
  - job_name: 'duckdb-api'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 15s
```

---

## Service Health Metrics

### `duckdb_api_up`
**Type:** Gauge

Whether the service is up and running.

| Value | Meaning |
|-------|---------|
| 1 | Service is healthy |
| 0 | Service is down (you won't see this - no scrape possible) |

**Example:**
```
duckdb_api_up 1.0
```

**Alert example:**
```yaml
- alert: DuckDBAPIDown
  expr: duckdb_api_up == 0
  for: 1m
  labels:
    severity: critical
```

---

### `duckdb_api_start_time_seconds`
**Type:** Gauge

Unix timestamp when the service started. Use for uptime calculation.

**Example:**
```
duckdb_api_start_time_seconds 1.765907808e+09
```

**PromQL - Calculate uptime:**
```promql
time() - duckdb_api_start_time_seconds
```

---

### `duckdb_api_service_info`
**Type:** Info (Gauge with labels)

Service version information.

**Labels:**
- `version` - API service version
- `duckdb_version` - DuckDB library version

**Example:**
```
duckdb_api_service_info{duckdb_version="1.4.3",version="0.1.0"} 1.0
```

---

## HTTP Request Metrics

### `duckdb_api_requests_total`
**Type:** Counter

Total number of HTTP requests processed.

**Labels:**
- `method` - HTTP method (GET, POST, PUT, DELETE)
- `endpoint` - Normalized endpoint path
- `status_code` - HTTP status code

**Example:**
```
duckdb_api_requests_total{endpoint="/health",method="GET",status_code="200"} 150.0
duckdb_api_requests_total{endpoint="/projects",method="POST",status_code="201"} 10.0
duckdb_api_requests_total{endpoint="/projects/{project_id}",method="GET",status_code="200"} 45.0
duckdb_api_requests_total{endpoint="/projects/{project_id}",method="GET",status_code="404"} 3.0
```

**PromQL - Request rate:**
```promql
rate(duckdb_api_requests_total[5m])
```

**PromQL - Error rate (4xx + 5xx):**
```promql
sum(rate(duckdb_api_requests_total{status_code=~"4..|5.."}[5m]))
/
sum(rate(duckdb_api_requests_total[5m]))
```

**PromQL - Requests by endpoint:**
```promql
topk(10, sum by (endpoint) (rate(duckdb_api_requests_total[5m])))
```

---

### `duckdb_api_request_duration_seconds`
**Type:** Histogram

HTTP request latency distribution.

**Labels:**
- `method` - HTTP method
- `endpoint` - Normalized endpoint path

**Buckets:** 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s

**Example:**
```
duckdb_api_request_duration_seconds_bucket{endpoint="/projects",le="0.1",method="POST"} 8.0
duckdb_api_request_duration_seconds_bucket{endpoint="/projects",le="0.25",method="POST"} 10.0
duckdb_api_request_duration_seconds_bucket{endpoint="/projects",le="+Inf",method="POST"} 10.0
duckdb_api_request_duration_seconds_sum{endpoint="/projects",method="POST"} 0.856
duckdb_api_request_duration_seconds_count{endpoint="/projects",method="POST"} 10.0
```

**Interpretation:**
- 8 out of 10 POST /projects requests completed in under 100ms
- Average latency: sum/count = 0.856/10 = 85.6ms

**PromQL - P95 latency:**
```promql
histogram_quantile(0.95,
  sum by (endpoint, le) (rate(duckdb_api_request_duration_seconds_bucket[5m]))
)
```

**PromQL - Average latency by endpoint:**
```promql
sum by (endpoint) (rate(duckdb_api_request_duration_seconds_sum[5m]))
/
sum by (endpoint) (rate(duckdb_api_request_duration_seconds_count[5m]))
```

---

### `duckdb_api_requests_in_flight`
**Type:** Gauge

Number of requests currently being processed.

**Labels:**
- `method` - HTTP method

**Example:**
```
duckdb_api_requests_in_flight{method="GET"} 2.0
duckdb_api_requests_in_flight{method="POST"} 1.0
```

**Interpretation:**
- High values indicate request queuing or slow processing
- Sustained high values may indicate resource exhaustion

**Alert example:**
```yaml
- alert: HighRequestConcurrency
  expr: sum(duckdb_api_requests_in_flight) > 50
  for: 5m
  labels:
    severity: warning
```

---

## Error Metrics

### `duckdb_api_errors_total`
**Type:** Counter

Total number of unhandled exceptions by type.

**Labels:**
- `type` - Python exception class name
- `endpoint` - Normalized endpoint path

**Example:**
```
duckdb_api_errors_total{endpoint="/projects/{project_id}",type="ValueError"} 2.0
duckdb_api_errors_total{endpoint="/projects/{project_id}/buckets",type="IOError"} 1.0
```

**PromQL - Error rate by type:**
```promql
sum by (type) (rate(duckdb_api_errors_total[5m]))
```

---

## DuckDB Operations Metrics

### `duckdb_operations_total`
**Type:** Counter

Total number of DuckDB database operations (queries, inserts, etc.).

**Labels:**
- `operation` - Operation type (query, insert, update, delete, etc.)
- `project_id` - Project identifier

**Example:**
```
duckdb_operations_total{operation="query",project_id="proj_123"} 500.0
duckdb_operations_total{operation="insert",project_id="proj_123"} 150.0
```

**PromQL - Operations per second:**
```promql
sum(rate(duckdb_operations_total[5m])) by (operation)
```

---

### `duckdb_operation_duration_seconds`
**Type:** Histogram

Duration of DuckDB operations in seconds.

**Labels:**
- `operation` - Operation type
- `project_id` - Project identifier

**Example:**
```
duckdb_operation_duration_seconds_bucket{operation="query",project_id="proj_123",le="0.1"} 480.0
duckdb_operation_duration_seconds_bucket{operation="query",project_id="proj_123",le="+Inf"} 500.0
duckdb_operation_duration_seconds_sum{operation="query",project_id="proj_123"} 25.5
duckdb_operation_duration_seconds_count{operation="query",project_id="proj_123"} 500.0
```

**PromQL - P95 operation latency:**
```promql
histogram_quantile(0.95,
  sum by (operation, le) (rate(duckdb_operation_duration_seconds_bucket[5m]))
)
```

---

## Storage Metrics

### `duckdb_projects_total`
**Type:** Gauge

Total number of active projects (excluding deleted).

**Example:**
```
duckdb_projects_total 15.0
```

---

### `duckdb_buckets_total`
**Type:** Gauge

Total number of buckets across all projects.

**Example:**
```
duckdb_buckets_total 45.0
```

---

### `duckdb_tables_total`
**Type:** Gauge

Total number of tables across all projects.

**Example:**
```
duckdb_tables_total 230.0
```

---

### `duckdb_storage_size_bytes`
**Type:** Gauge

Storage size in bytes by type.

**Labels:**
- `type` - Storage type

| Type | Description |
|------|-------------|
| `metadata` | metadata.duckdb file size |
| `tables` | Total size of all table .duckdb files |
| `staging` | Temporary staging files |
| `files` | Files API storage (if used) |

**Example:**
```
duckdb_storage_size_bytes{type="metadata"} 1.25952e+07
duckdb_storage_size_bytes{type="tables"} 5.61152e+05
duckdb_storage_size_bytes{type="staging"} 0.0
duckdb_storage_size_bytes{type="files"} 0.0
```

**Interpretation:**
- metadata: 12.6 MB
- tables: 561 KB
- staging: 0 (no pending operations)

**PromQL - Total storage:**
```promql
sum(duckdb_storage_size_bytes)
```

**Alert example:**
```yaml
- alert: HighStorageUsage
  expr: sum(duckdb_storage_size_bytes) > 100e9  # 100GB
  for: 5m
  labels:
    severity: warning
```

---

## Idempotency Cache Metrics

### `duckdb_idempotency_cache_hits_total`
**Type:** Counter

Number of requests served from idempotency cache (duplicate requests).

**Example:**
```
duckdb_idempotency_cache_hits_total 25.0
```

**Interpretation:**
- High hit rate indicates many retried requests (network issues? slow clients?)

---

### `duckdb_idempotency_cache_misses_total`
**Type:** Counter

Number of new requests processed (not in cache).

**Example:**
```
duckdb_idempotency_cache_misses_total 1000.0
```

**PromQL - Cache hit ratio:**
```promql
rate(duckdb_idempotency_cache_hits_total[5m])
/
(rate(duckdb_idempotency_cache_hits_total[5m]) + rate(duckdb_idempotency_cache_misses_total[5m]))
```

---

### `duckdb_idempotency_cache_size`
**Type:** Gauge

Current number of entries in the idempotency cache.

**Example:**
```
duckdb_idempotency_cache_size 42.0
```

**Note:** Entries expire after 10 minutes (TTL).

---

### `duckdb_idempotency_conflicts_total`
**Type:** Counter

Number of idempotency key conflicts (same key, different request).

**Example:**
```
duckdb_idempotency_conflicts_total 3.0
```

**Interpretation:**
- Should be very low or zero
- High values indicate client bugs (reusing idempotency keys incorrectly)

---

## Write Queue Metrics

### `duckdb_write_queue_depth`
**Type:** Gauge

Number of operations currently waiting in the write queue.

**Example:**
```
duckdb_write_queue_depth 3.0
```

**Interpretation:**
- Value of 0 means no queued operations (healthy)
- High values indicate backpressure or slow writes

**Alert example:**
```yaml
- alert: WriteQueueBackpressure
  expr: duckdb_write_queue_depth > 10
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Write queue depth exceeds 10"
```

---

### `duckdb_write_queue_wait_seconds`
**Type:** Histogram

Time spent waiting in the write queue before processing.

**Example:**
```
duckdb_write_queue_wait_seconds_bucket{le="0.01"} 95.0
duckdb_write_queue_wait_seconds_bucket{le="0.1"} 99.0
duckdb_write_queue_wait_seconds_bucket{le="+Inf"} 100.0
duckdb_write_queue_wait_seconds_sum 0.85
duckdb_write_queue_wait_seconds_count 100.0
```

**PromQL - P95 queue wait time:**
```promql
histogram_quantile(0.95, sum(rate(duckdb_write_queue_wait_seconds_bucket[5m])) by (le))
```

---

## Table Lock Metrics

### `duckdb_table_lock_acquisitions_total`
**Type:** Counter

Total number of table lock acquisitions.

**Labels:**
- `project_id` - Project identifier
- `bucket` - Bucket name
- `table` - Table name

**Example:**
```
duckdb_table_lock_acquisitions_total{bucket="in_c_sales",project_id="proj_123",table="orders"} 150.0
duckdb_table_lock_acquisitions_total{bucket="in_c_sales",project_id="proj_123",table="customers"} 80.0
```

**PromQL - Most active tables:**
```promql
topk(10, sum by (project_id, bucket, table) (rate(duckdb_table_lock_acquisitions_total[5m])))
```

---

### `duckdb_table_lock_wait_seconds`
**Type:** Histogram

Time spent waiting to acquire a table lock.

**Buckets:** 1ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s

**Example:**
```
duckdb_table_lock_wait_seconds_bucket{le="0.001"} 145.0
duckdb_table_lock_wait_seconds_bucket{le="0.01"} 148.0
duckdb_table_lock_wait_seconds_bucket{le="0.05"} 150.0
duckdb_table_lock_wait_seconds_bucket{le="+Inf"} 150.0
duckdb_table_lock_wait_seconds_sum 0.523
duckdb_table_lock_wait_seconds_count 150.0
```

**Interpretation:**
- 145 out of 150 lock acquisitions waited less than 1ms (no contention)
- 3 waited 1-10ms
- 2 waited 10-50ms
- Average wait: 0.523/150 = 3.5ms

**PromQL - P99 lock wait time:**
```promql
histogram_quantile(0.99, sum(rate(duckdb_table_lock_wait_seconds_bucket[5m])) by (le))
```

**Alert example:**
```yaml
- alert: HighLockContention
  expr: histogram_quantile(0.95, sum(rate(duckdb_table_lock_wait_seconds_bucket[5m])) by (le)) > 1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "P95 lock wait time exceeds 1 second"
```

---

### `duckdb_table_locks_active`
**Type:** Gauge

Number of table locks currently held.

**Example:**
```
duckdb_table_locks_active 2.0
```

**Interpretation:**
- Should typically be low (0-5)
- High sustained values indicate slow operations or deadlocks

---

## Branch Metrics (ADR-007: CoW branching)

### `duckdb_branches_total`
**Type:** Gauge

Total number of dev branches across all projects.

**Example:**
```
duckdb_branches_total 5.0
```

---

### `duckdb_branch_cow_operations_total`
**Type:** Counter

Total number of Copy-on-Write operations (table copies to branches).

**Labels:**
- `project_id` - Project identifier
- `branch_id` - Branch identifier

**Example:**
```
duckdb_branch_cow_operations_total{project_id="proj_123",branch_id="abc12345"} 15.0
```

**PromQL - CoW rate by project:**
```promql
sum by (project_id) (rate(duckdb_branch_cow_operations_total[5m]))
```

---

### `duckdb_branch_cow_duration_seconds`
**Type:** Histogram

Duration of Copy-on-Write operations (time to copy table file).

**Buckets:** 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s, 30s

**Example:**
```
duckdb_branch_cow_duration_seconds_bucket{le="0.1"} 45.0
duckdb_branch_cow_duration_seconds_bucket{le="1.0"} 48.0
duckdb_branch_cow_duration_seconds_bucket{le="+Inf"} 50.0
duckdb_branch_cow_duration_seconds_sum 25.5
duckdb_branch_cow_duration_seconds_count 50.0
```

**PromQL - P95 CoW duration:**
```promql
histogram_quantile(0.95, sum(rate(duckdb_branch_cow_duration_seconds_bucket[5m])) by (le))
```

---

### `duckdb_branch_cow_bytes_total`
**Type:** Counter

Total bytes copied in Copy-on-Write operations.

**Labels:**
- `project_id` - Project identifier
- `branch_id` - Branch identifier

**Example:**
```
duckdb_branch_cow_bytes_total{project_id="proj_123",branch_id="abc12345"} 1.5e+09
```

**PromQL - CoW throughput (bytes/sec):**
```promql
sum(rate(duckdb_branch_cow_bytes_total[5m]))
```

---

### `duckdb_branch_tables_total`
**Type:** Gauge

Total number of tables copied to branches (across all branches).

**Example:**
```
duckdb_branch_tables_total 42.0
```

**Interpretation:**
- Higher values indicate more branch-local modifications
- Low values indicate most branches use live view from main

---

## Process Metrics (Linux only)

These metrics are only available when running on Linux (e.g., in Docker).

### `process_cpu_seconds_total`
**Type:** Counter

Total CPU time used by the process.

**PromQL - CPU usage:**
```promql
rate(process_cpu_seconds_total[5m])
```

---

### `process_resident_memory_bytes`
**Type:** Gauge

Resident memory size (RSS) in bytes.

**Alert example:**
```yaml
- alert: HighMemoryUsage
  expr: process_resident_memory_bytes > 2e9  # 2GB
  for: 5m
  labels:
    severity: warning
```

---

### `process_open_fds`
**Type:** Gauge

Number of open file descriptors.

**Note:** Important for DuckDB as each table is a separate file.

---

### `process_max_fds`
**Type:** Gauge

Maximum allowed file descriptors.

**Alert example:**
```yaml
- alert: FileDescriptorExhaustion
  expr: process_open_fds / process_max_fds > 0.8
  for: 5m
  labels:
    severity: warning
```

---

## Python Runtime Metrics

### `python_info`
**Type:** Info

Python version information.

**Example:**
```
python_info{implementation="CPython",major="3",minor="14",patchlevel="2",version="3.14.2"} 1.0
```

---

### `python_gc_collections_total`
**Type:** Counter

Number of garbage collection runs by generation.

**Labels:**
- `generation` - GC generation (0, 1, 2)

**Example:**
```
python_gc_collections_total{generation="0"} 0.0
python_gc_collections_total{generation="1"} 15.0
python_gc_collections_total{generation="2"} 0.0
```

---

### `python_gc_objects_collected_total`
**Type:** Counter

Total number of objects collected during garbage collection by generation.

**Labels:**
- `generation` - GC generation (0, 1, 2)

**Example:**
```
python_gc_objects_collected_total{generation="0"} 0.0
python_gc_objects_collected_total{generation="1"} 378.0
python_gc_objects_collected_total{generation="2"} 0.0
```

---

### `python_gc_objects_uncollectable_total`
**Type:** Counter

Total number of uncollectable objects found during garbage collection (potential memory leaks).

**Labels:**
- `generation` - GC generation (0, 1, 2)

**Example:**
```
python_gc_objects_uncollectable_total{generation="0"} 0.0
python_gc_objects_uncollectable_total{generation="1"} 0.0
python_gc_objects_uncollectable_total{generation="2"} 0.0
```

**Interpretation:**
- Should always be 0 in healthy applications
- Non-zero values indicate circular references that cannot be collected (memory leak)

**Alert example:**
```yaml
- alert: PythonMemoryLeak
  expr: sum(python_gc_objects_uncollectable_total) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Uncollectable objects detected - potential memory leak"
```

---

## Endpoint Path Normalization

Dynamic path segments are normalized to prevent high cardinality:

| Actual Path | Normalized |
|------------|------------|
| `/projects/abc123` | `/projects/{project_id}` |
| `/projects/abc123/buckets/sales` | `/projects/{project_id}/buckets/{bucket_name}` |
| `/projects/abc123/buckets/sales/tables/orders` | `/projects/{project_id}/buckets/{bucket_name}/tables/{table_name}` |

**Skipped endpoints** (not tracked):
- `/metrics` - Avoid recursion
- `/docs` - Debug only
- `/redoc` - Debug only
- `/openapi.json` - Debug only

---

## Grafana Dashboard Queries

### Request Rate Panel
```promql
sum(rate(duckdb_api_requests_total[5m])) by (endpoint)
```

### Error Rate Panel
```promql
sum(rate(duckdb_api_requests_total{status_code=~"5.."}[5m]))
/
sum(rate(duckdb_api_requests_total[5m])) * 100
```

### P95 Latency Panel
```promql
histogram_quantile(0.95,
  sum(rate(duckdb_api_request_duration_seconds_bucket[5m])) by (le, endpoint)
)
```

### Storage Growth Panel
```promql
sum(duckdb_storage_size_bytes) by (type)
```

### Active Projects/Tables Panel
```promql
duckdb_projects_total
duckdb_tables_total
```

---

## Recommended Alerts

```yaml
groups:
  - name: duckdb-api
    rules:
      - alert: DuckDBAPIHighErrorRate
        expr: |
          sum(rate(duckdb_api_requests_total{status_code=~"5.."}[5m]))
          / sum(rate(duckdb_api_requests_total[5m])) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Error rate above 5%"

      - alert: DuckDBAPIHighLatency
        expr: |
          histogram_quantile(0.95,
            sum(rate(duckdb_api_request_duration_seconds_bucket[5m])) by (le)
          ) > 2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P95 latency above 2 seconds"

      - alert: DuckDBAPIHighLockContention
        expr: |
          histogram_quantile(0.95,
            sum(rate(duckdb_table_lock_wait_seconds_bucket[5m])) by (le)
          ) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P95 lock wait time above 1 second"

      - alert: DuckDBAPIStorageHigh
        expr: sum(duckdb_storage_size_bytes) > 500e9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Total storage exceeds 500GB"

      - alert: DuckDBAPIHighCoWDuration
        expr: |
          histogram_quantile(0.95,
            sum(rate(duckdb_branch_cow_duration_seconds_bucket[5m])) by (le)
          ) > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P95 Copy-on-Write duration above 10 seconds"
```
