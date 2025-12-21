# ADR-015: Observability and Operation Tracking

**Status:** Proposed
**Date:** 2025-12-21
**Authors:** Claude Code
**Deciders:** TBD

## Context

DuckDB API Service currently has basic observability:
- **Prometheus metrics** at `/metrics` - request counts, latencies (histograms), storage sizes
- **Structured logs** via structlog - request start/end, command names, durations
- **gRPC response timers** - `timers` field in responses (e.g., TableImportResponse)

However, we lack:
1. **Operation history** - no way to query "what operations ran on this table in last hour?"
2. **Per-operation timing** - metrics are aggregated, can't see individual operation durations
3. **Correlation with Connection jobs** - no link between DuckDB operations and Connection job IDs
4. **Slow query analysis** - no easy way to identify slow operations

### Current Pain Points

1. **Debugging production issues**: When a Connection job fails, we have to grep logs manually
2. **Performance analysis**: Can't easily answer "how long do imports typically take for table X?"
3. **Capacity planning**: No historical data on operation patterns
4. **SLA monitoring**: Can't track if operations meet latency requirements

## Decision Drivers

- **Minimal overhead**: Observability shouldn't slow down operations
- **Query-able history**: Ability to analyze past operations via API
- **Connection integration**: Link DuckDB operations to Connection job IDs
- **On-premise friendly**: No external dependencies (no cloud monitoring services)

## Considered Options

### Option A: Operation Log in Metadata DB

Store operation history in `metadata.duckdb`:

```sql
CREATE TABLE operation_log (
    id VARCHAR PRIMARY KEY,           -- UUID
    timestamp TIMESTAMPTZ NOT NULL,
    project_id VARCHAR NOT NULL,
    bucket_name VARCHAR,
    table_name VARCHAR,
    command VARCHAR NOT NULL,         -- 'TableImportFromFile', 'CreateTable', etc.
    status VARCHAR NOT NULL,          -- 'success', 'error'
    duration_ms DOUBLE NOT NULL,
    rows_affected BIGINT,
    bytes_processed BIGINT,
    error_message VARCHAR,
    connection_job_id VARCHAR,        -- Link to Connection job
    connection_run_id VARCHAR,        -- Link to Connection run
    request_id VARCHAR,               -- For log correlation
    metadata JSON                     -- Additional context
);

CREATE INDEX idx_operation_log_project ON operation_log(project_id, timestamp);
CREATE INDEX idx_operation_log_table ON operation_log(project_id, bucket_name, table_name, timestamp);
CREATE INDEX idx_operation_log_command ON operation_log(command, timestamp);
```

**Pros:**
- Query-able via SQL
- No external dependencies
- Can join with other metadata tables
- Supports complex queries (e.g., "slowest imports by table")

**Cons:**
- Adds write overhead to every operation
- Log table can grow large
- Need retention/cleanup policy

### Option B: Enhanced Prometheus Metrics

Add more detailed metrics:

```python
# Per-table operation histogram
operation_duration = Histogram(
    'duckdb_operation_duration_seconds',
    'Operation duration by table',
    ['project_id', 'bucket', 'table', 'command', 'status'],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 30.0, 120.0]
)

# Operation counter with more labels
operation_total = Counter(
    'duckdb_operations_total',
    'Total operations',
    ['project_id', 'command', 'status']
)

# Rows processed
rows_processed = Counter(
    'duckdb_rows_processed_total',
    'Total rows processed',
    ['project_id', 'command']  # import, export, delete
)
```

**Pros:**
- Standard Prometheus approach
- Easy to alert on
- Works with existing Grafana dashboards
- No additional storage

**Cons:**
- High cardinality risk (per-table labels)
- No individual operation details
- Can't query historical operations
- Loses data on restart (unless using remote write)

### Option C: Hybrid Approach (Recommended)

Combine both:
1. **Prometheus metrics** for real-time monitoring and alerting
2. **Operation log table** for historical analysis and debugging
3. **API endpoints** for querying operation history

```
┌─────────────────────────────────────────────────────────────────┐
│                     DuckDB API Service                          │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │  Prometheus │    │  Operation  │    │    Structured       │ │
│  │   Metrics   │    │     Log     │    │       Logs          │ │
│  │  /metrics   │    │  metadata   │    │    (structlog)      │ │
│  └──────┬──────┘    └──────┬──────┘    └──────────┬──────────┘ │
│         │                  │                      │             │
│         ▼                  ▼                      ▼             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │  Grafana /  │    │   REST API  │    │   Log Aggregator    │ │
│  │  Alerting   │    │  /operations│    │   (optional)        │ │
│  └─────────────┘    └─────────────┘    └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Decision

**Option C: Hybrid Approach**

Implement in phases:
1. **Phase 1**: Operation log table + basic API (high value, low effort)
2. **Phase 2**: Enhanced Prometheus metrics (if needed for alerting)
3. **Phase 3**: Advanced analytics (slow query reports, capacity planning)

## Implementation Plan

### Phase 1: Operation Log (Priority: HIGH)

#### 1.1 Schema

```sql
-- In metadata.duckdb
CREATE TABLE IF NOT EXISTS operation_log (
    id VARCHAR PRIMARY KEY DEFAULT uuid(),
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Scope
    project_id VARCHAR NOT NULL,
    branch_id VARCHAR DEFAULT 'default',
    bucket_name VARCHAR,
    table_name VARCHAR,

    -- Operation
    command VARCHAR NOT NULL,
    status VARCHAR NOT NULL,  -- 'success', 'error', 'timeout'

    -- Timing
    duration_ms DOUBLE NOT NULL,
    queue_wait_ms DOUBLE,     -- Time waiting for table lock

    -- Data metrics
    rows_affected BIGINT,
    bytes_in BIGINT,
    bytes_out BIGINT,

    -- Error handling
    error_code VARCHAR,
    error_message VARCHAR,

    -- Correlation
    request_id VARCHAR,
    connection_job_id VARCHAR,
    connection_run_id VARCHAR,

    -- Context (JSON for flexibility)
    metadata JSON
);

-- Indexes for common queries
CREATE INDEX idx_oplog_project_time ON operation_log(project_id, timestamp DESC);
CREATE INDEX idx_oplog_table_time ON operation_log(project_id, bucket_name, table_name, timestamp DESC);
CREATE INDEX idx_oplog_command ON operation_log(command, timestamp DESC);
CREATE INDEX idx_oplog_errors ON operation_log(status, timestamp DESC) WHERE status = 'error';
```

#### 1.2 API Endpoints

```
GET /projects/{project_id}/operations
    ?limit=100
    &offset=0
    &command=TableImportFromFile
    &status=error
    &table=bucket/table
    &from=2025-12-21T00:00:00Z
    &to=2025-12-22T00:00:00Z

GET /projects/{project_id}/operations/{operation_id}

GET /projects/{project_id}/operations/stats
    ?period=1h  # 1h, 24h, 7d, 30d

    Returns:
    {
        "period": "1h",
        "total_operations": 1234,
        "success_rate": 0.997,
        "avg_duration_ms": 45.2,
        "p50_duration_ms": 23.1,
        "p95_duration_ms": 156.3,
        "p99_duration_ms": 892.1,
        "by_command": {
            "TableImportFromFile": { "count": 500, "avg_ms": 34.2 },
            "PreviewTable": { "count": 600, "avg_ms": 12.1 }
        },
        "errors": [
            { "code": "DUPLICATE_KEY", "count": 3 }
        ]
    }
```

#### 1.3 Retention Policy

```python
# Default: 7 days for operations, 30 days for errors
OPERATION_LOG_RETENTION_DAYS = 7
ERROR_LOG_RETENTION_DAYS = 30

# Cleanup job (runs daily)
async def cleanup_operation_log():
    conn.execute(f"""
        DELETE FROM operation_log
        WHERE timestamp < NOW() - INTERVAL '{OPERATION_LOG_RETENTION_DAYS} days'
        AND status = 'success'
    """)
    conn.execute(f"""
        DELETE FROM operation_log
        WHERE timestamp < NOW() - INTERVAL '{ERROR_LOG_RETENTION_DAYS} days'
    """)
```

### Phase 2: Enhanced Metrics (Priority: MEDIUM)

Add per-project metrics (careful with cardinality):

```python
# Only track top-level metrics to avoid cardinality explosion
project_operations = Counter(
    'duckdb_project_operations_total',
    'Operations per project',
    ['project_id', 'command', 'status']
)

# Histogram without table-level labels
import_duration = Histogram(
    'duckdb_import_duration_seconds',
    'Import operation duration',
    ['incremental'],  # true/false
    buckets=[0.1, 0.5, 1, 5, 30, 120, 300]
)
```

### Phase 3: Analytics (Priority: LOW)

- Slow query report endpoint
- Capacity planning queries
- Usage patterns analysis

## Consequences

### Positive

- **Debugging**: Easy to trace operations via API
- **Performance analysis**: Can identify slow operations and patterns
- **Connection integration**: Link DuckDB operations to Connection jobs
- **On-premise**: No external dependencies

### Negative

- **Storage growth**: Operation log adds ~500 bytes per operation
- **Write overhead**: Small latency increase (~1ms) per operation
- **Maintenance**: Need to run retention cleanup

### Neutral

- Existing Prometheus metrics remain unchanged
- Structured logs continue to work as before

## Metrics

Success criteria:
- [ ] 100% of operations logged
- [ ] < 1ms overhead per operation
- [ ] API response < 100ms for typical queries
- [ ] Log size < 1GB per month (at 100k ops/day)

## References

- Phase 13: Observability (docs/plan/phase-13-observability.md)
- Current metrics implementation (src/metrics.py)
- Prometheus best practices for cardinality

## Appendix: Query Examples

```sql
-- Slowest imports in last hour
SELECT project_id, bucket_name, table_name, duration_ms, rows_affected
FROM operation_log
WHERE command = 'TableImportFromFile'
  AND timestamp > NOW() - INTERVAL '1 hour'
ORDER BY duration_ms DESC
LIMIT 10;

-- Error rate by command
SELECT command,
       COUNT(*) as total,
       SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors,
       ROUND(100.0 * SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_rate
FROM operation_log
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY command
ORDER BY error_rate DESC;

-- Operations linked to Connection job
SELECT * FROM operation_log
WHERE connection_job_id = '1000008'
ORDER BY timestamp;

-- P95 latency by table (last 7 days)
SELECT project_id, bucket_name, table_name,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_ms,
       COUNT(*) as operation_count
FROM operation_log
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY project_id, bucket_name, table_name
HAVING COUNT(*) > 10
ORDER BY p95_ms DESC;
```
