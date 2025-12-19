# Phase 11: Workspaces

## Status
- **Status:** DONE
- **Specification:** 100%
- **ADR:** [ADR-010: SQL Interface](../adr/010-duckdb-sql-interface.md)
- **Tests:** 76 passed (41 REST + 26 pgwire_auth + 9 e2e)

**Completed:**
- Phase 11a: Workspace Management REST API (12 endpoints) - 41 tests
- Phase 11b: PG Wire Server (buenavista) - 35 tests
- Branch workspace support
- SSL/TLS support
- Docker Compose deployment

**See also:**
- [Phase 11b: PG Wire Server](phase-11b-pgwire.md) - implementation details
- [Phase 11c: Polish & Production](phase-11c-workspace-polish.md) - next steps

## Goal

Implement isolated workspaces for data transformation with **PostgreSQL-compatible SQL interface**.

Users connect to workspaces using standard PostgreSQL clients (DBeaver, psql, Python psycopg2, etc.) and can:
- **Read** all project tables (via ATTACH READ_ONLY)
- **Write** to workspace (CREATE TABLE, INSERT, etc.)

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         USER TOOLS                                    │
│  DBeaver, DataGrip, psql, Python psycopg2, R, Tableau, ...          │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                │ PostgreSQL Wire Protocol (port 5432)
                                │
┌───────────────────────────────▼──────────────────────────────────────┐
│                      PG WIRE SERVER                                   │
│                   (duckgres / pgwire)                                │
│                                                                       │
│  - Authentication (workspace credentials)                            │
│  - Session management                                                │
│  - Query routing                                                     │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                │ DuckDB Python API
                                │
┌───────────────────────────────▼──────────────────────────────────────┐
│                     WORKSPACE SESSION                                 │
│                                                                       │
│  workspace_123.duckdb (RW)     ← User's working space                │
│      │                                                                │
│      ├── ATTACH 'in_c_sales/orders.duckdb' AS orders (READ_ONLY)     │
│      ├── ATTACH 'in_c_sales/customers.duckdb' AS customers (RO)      │
│      └── ... (all project tables attached read-only)                 │
│                                                                       │
│  User capabilities:                                                   │
│  - SELECT FROM any attached table (project data, read-only)          │
│  - CREATE TABLE, INSERT, UPDATE in workspace (read-write)            │
│  - JOINs across all tables, CTEs, window functions                   │
└───────────────────────────────────────────────────────────────────────┘
```

## File Structure

```
/data/duckdb/
├── project_123/
│   ├── in_c_sales/
│   │   ├── orders.duckdb
│   │   └── customers.duckdb
│   └── _workspaces/                    # Workspace directory
│       ├── ws_abc123.duckdb            # Workspace = isolated DB
│       └── ws_def456.duckdb
├── project_123_branch_789/
│   └── _workspaces/                    # Branch workspaces
│       └── ws_xyz789.duckdb
```

## Endpoints to Implement

### Workspace Management (REST API)

```
POST   /projects/{id}/workspaces                        # CreateWorkspace
GET    /projects/{id}/workspaces                        # ListWorkspaces
GET    /projects/{id}/workspaces/{ws_id}                # WorkspaceDetail
DELETE /projects/{id}/workspaces/{ws_id}                # DropWorkspace
POST   /projects/{id}/workspaces/{ws_id}/clear          # ClearWorkspace
DELETE /projects/{id}/workspaces/{ws_id}/objects/{name} # DropWorkspaceObject
POST   /projects/{id}/workspaces/{ws_id}/load           # LoadDataToWorkspace
POST   /projects/{id}/workspaces/{ws_id}/credentials/reset  # ResetPassword

# Dev branch workspaces
POST   /projects/{id}/branches/{branch_id}/workspaces              # CreateBranchWorkspace
GET    /projects/{id}/branches/{branch_id}/workspaces              # ListBranchWorkspaces
GET    /projects/{id}/branches/{branch_id}/workspaces/{ws_id}      # BranchWorkspaceDetail
DELETE /projects/{id}/branches/{branch_id}/workspaces/{ws_id}      # DropBranchWorkspace
```

### SQL Interface (PostgreSQL Wire Protocol)

```
Host: duckdb.keboola.local (or localhost for on-prem)
Port: 5432
Database: workspace_{ws_id}
Username: ws_{ws_id}_{random}
Password: <generated, returned on CreateWorkspace>
SSL: required (production)
```

## API Specifications

### POST /projects/{id}/workspaces - CreateWorkspace

**Request:**
```json
{
    "name": "My Analysis Workspace",
    "ttl_hours": 24,                    // Optional, default 24h
    "size_limit_gb": 10,                // Optional, default 10GB
    "preload_tables": [                 // Optional - tables to COPY into workspace
        "in.c-sales.orders",
        "in.c-sales.customers"
    ]
}
```

**Response:**
```json
{
    "id": "ws_abc123def456",
    "name": "My Analysis Workspace",
    "project_id": "123",
    "created_at": "2024-12-18T10:00:00Z",
    "expires_at": "2024-12-19T10:00:00Z",
    "size_limit_gb": 10,
    "connection": {
        "host": "duckdb.keboola.local",
        "port": 5432,
        "database": "workspace_ws_abc123def456",
        "username": "ws_abc123def456_user",
        "password": "generated_password_shown_only_once",
        "ssl_mode": "require",
        "connection_string": "postgresql://ws_abc123def456_user:xxx@duckdb.keboola.local:5432/workspace_ws_abc123def456?sslmode=require"
    },
    "attached_tables": [
        {"schema": "in_c_sales", "table": "orders", "rows": 150000},
        {"schema": "in_c_sales", "table": "customers", "rows": 5000},
        {"schema": "out_c_reports", "table": "summary", "rows": 100}
    ]
}
```

### GET /projects/{id}/workspaces/{ws_id} - WorkspaceDetail

**Response:**
```json
{
    "id": "ws_abc123def456",
    "name": "My Analysis Workspace",
    "project_id": "123",
    "created_at": "2024-12-18T10:00:00Z",
    "expires_at": "2024-12-19T10:00:00Z",
    "size_bytes": 52428800,
    "size_limit_gb": 10,
    "status": "active",                 // active, expired, error
    "active_sessions": 1,
    "connection": {
        "host": "duckdb.keboola.local",
        "port": 5432,
        "database": "workspace_ws_abc123def456",
        "username": "ws_abc123def456_user"
        // password NOT returned - shown only on create
    },
    "workspace_objects": [
        {"name": "my_analysis", "type": "table", "rows": 5000},
        {"name": "temp_results", "type": "table", "rows": 100}
    ]
}
```

### POST /projects/{id}/workspaces/{ws_id}/load - LoadDataToWorkspace

Pre-copy tables into workspace for faster repeated access.

**Request:**
```json
{
    "tables": [
        {
            "source": "in.c-sales.orders",
            "destination": "orders_copy",   // Optional, defaults to table name
            "columns": ["id", "customer_id", "amount"],  // Optional, all if empty
            "where": "created_at > '2024-01-01'"         // Optional filter
        }
    ]
}
```

**Response:**
```json
{
    "loaded": [
        {
            "source": "in.c-sales.orders",
            "destination": "orders_copy",
            "rows": 50000,
            "size_bytes": 2097152
        }
    ],
    "workspace_size_bytes": 54525952
}
```

## Metadata Schema

```sql
-- In metadata.duckdb

CREATE TABLE workspaces (
    id VARCHAR PRIMARY KEY,             -- ws_{uuid}
    project_id VARCHAR NOT NULL,
    branch_id VARCHAR,                  -- NULL for main branch
    name VARCHAR NOT NULL,
    db_path VARCHAR NOT NULL,           -- Path to workspace .duckdb file
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,
    size_limit_bytes BIGINT DEFAULT 10737418240,  -- 10GB
    status VARCHAR DEFAULT 'active',    -- active, expired, error

    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (branch_id) REFERENCES branches(id)
);

CREATE TABLE workspace_credentials (
    workspace_id VARCHAR PRIMARY KEY,
    username VARCHAR NOT NULL UNIQUE,   -- ws_{workspace_id}_{random}
    password_hash VARCHAR NOT NULL,     -- SHA256
    created_at TIMESTAMPTZ DEFAULT now(),

    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

-- Index for auth lookup
CREATE INDEX idx_workspace_creds_username ON workspace_credentials(username);
```

## Key Decisions (APPROVED)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **SQL Interface** | **PostgreSQL Wire Protocol** | Maximum tool compatibility (ADR-010) |
| **Implementation** | **duckgres / pgwire** | Production-proven (PostHog uses duckgres) |
| **Workspace isolation** | **File per workspace** | Consistent with ADR-009 |
| **Project data access** | **ATTACH READ_ONLY** | Safe, no data duplication |
| **TTL default** | **24 hours** | Balance usability vs resource cleanup |
| **Size limit default** | **10GB** | Reasonable for analysis workloads |
| **Credentials** | **One-time password display** | Security best practice |

## Implementation Plan

### Phase 11a: Workspace Management (REST API)
1. Metadata schema for workspaces
2. CreateWorkspace / ListWorkspaces / GetWorkspace / DropWorkspace
3. ClearWorkspace / DropWorkspaceObject
4. LoadDataToWorkspace
5. TTL expiration cleanup (background task)

### Phase 11b: PG Wire Server Integration
1. Evaluate and choose: duckgres vs duckdb-pgwire
2. Authentication integration with workspace_credentials
3. Session initialization (ATTACH all project tables)
4. Connection pooling and resource limits
5. Idle session timeout

### Phase 11c: Branch Workspaces
1. CreateBranchWorkspace (ATTACHes branch tables instead of main)
2. Branch workspace isolation

## PG Wire Server Comparison

| Feature | duckgres (PostHog) | duckdb-pgwire |
|---------|-------------------|---------------|
| Language | Rust | C++ |
| Maturity | Production (PostHog) | Experimental |
| Maintenance | Active | Less active |
| Auth | Custom | Custom |
| SSL | Yes | Yes |
| Prepared Statements | Partial | Partial |

**Recommendation:** Start with **duckgres** - production-proven at PostHog.

## Resource Limits

```python
@dataclass
class WorkspaceConfig:
    max_workspaces_per_project: int = 10
    max_attached_tables: int = 1000     # Max ATTACHed databases
    max_memory_per_session: str = "4GB" # DuckDB memory limit
    max_temp_storage: str = "10GB"      # Temp files for large queries
    session_idle_timeout: int = 3600    # 1 hour
    query_timeout: int = 300            # 5 min per query
    default_ttl_hours: int = 24
    max_ttl_hours: int = 168            # 7 days
```

## Testing Strategy

### Unit Tests
- Workspace CRUD operations
- Credential generation and verification
- TTL expiration logic

### Integration Tests
- PG Wire connection with psycopg2
- ATTACH READ_ONLY verification (cannot write to project tables)
- Workspace write operations
- Session timeout handling

### E2E Tests
- Full workflow: create workspace -> connect -> query -> drop
- Branch workspace with branch data
- Concurrent sessions

## Security Considerations

1. **Credentials**: Password shown only once, stored as SHA256 hash
2. **Isolation**: Each workspace is separate .duckdb file
3. **Read-only project data**: ATTACH READ_ONLY prevents accidental writes
4. **TTL**: Automatic cleanup of expired workspaces
5. **Resource limits**: Prevent resource exhaustion
6. **SSL**: Required in production

## DuckDB Quirks Discovered

### 1. information_schema.tables Returns Empty

After ATTACH/DETACH operations, `information_schema.tables` and `duckdb_tables()` may return empty results even with regular (non-read_only) connections.

**Solution:** Use `SHOW TABLES` which works reliably.

### 2. Distinguishing Tables from Views

`SHOW TABLES` returns both tables and views. To distinguish:
```sql
SELECT 1 FROM sqlite_master WHERE type = 'view' AND name = ?
```

### 3. FK Constraint on UPDATE

DuckDB checks FK constraints even for UPDATE of non-key columns. This caused issues when updating `expires_at` on a workspace that had credentials.

**Note:** DuckDB doesn't support CASCADE on foreign keys.

**Workaround:** Delete referencing rows before updating, or handle in application code.

## References

- [ADR-010: SQL Interface](../adr/010-duckdb-sql-interface.md)
- [ADR-009: File per Table](../adr/009-duckdb-file-per-table.md)
- [duckgres (PostHog)](https://github.com/PostHog/duckgres)
- [duckdb-pgwire](https://github.com/euiko/duckdb-pgwire)
- [DuckDB ATTACH](https://duckdb.org/docs/sql/statements/attach.html)
