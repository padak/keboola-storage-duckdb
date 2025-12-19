# Phase 11b: PostgreSQL Wire Protocol Server

## Status
- **Status:** DONE
- **Depends on:** Phase 11a (Workspace REST API) - DONE
- **ADR:** [ADR-010: SQL Interface](../adr/010-duckdb-sql-interface.md)
- **Tests:** 35 passed (26 pgwire_auth + 9 e2e)

### Implementation
Used **buenavista** Python library (instead of duckgres) for easier integration.

### Completed
- Custom PG Wire server (`src/pgwire_server.py`) based on buenavista
- Cleartext password authentication over TLS
- Dynamic workspace credential lookup from metadata_db
- Per-workspace DuckDB session with ATTACH of project tables
- Session tracking in pgwire_sessions table
- Connection limits per workspace
- SSL/TLS support
- Docker Compose deployment (REST API + PG Wire server)
- Certificate generation script (`scripts/generate_certs.sh`)
- 35 comprehensive tests

### Usage

**Start PG Wire server:**
```bash
python -m src.pgwire_server --host 0.0.0.0 --port 5432

# With SSL:
./scripts/generate_certs.sh
python -m src.pgwire_server --ssl-cert certs/server.crt --ssl-key certs/server.key
```

**Connect with psql:**
```bash
psql "host=localhost port=5432 user=ws_xxx_yyy password=your_password"
```

**Docker Compose:**
```bash
docker compose up -d
# REST API on port 8000, PG Wire on port 5432
```

## Goal

Implement PostgreSQL Wire Protocol server that allows users to connect to DuckDB workspaces using standard PostgreSQL clients (DBeaver, psql, psycopg2, etc.).

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
│  Components:                                                          │
│  ├── Connection Handler (accept, auth, session init)                 │
│  ├── Query Router (parse, plan, execute)                             │
│  ├── Session Manager (tracking, cleanup, timeouts)                   │
│  └── Auth Provider (workspace_credentials lookup)                    │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                │ DuckDB Python/C API
                                │
┌───────────────────────────────▼──────────────────────────────────────┐
│                     WORKSPACE SESSION                                 │
│                                                                       │
│  workspace_123.duckdb (RW)                                           │
│      ├── ATTACH 'orders.duckdb' AS in_c_sales_orders (READ_ONLY)     │
│      ├── ATTACH 'customers.duckdb' AS in_c_sales_customers (RO)      │
│      └── User tables (CREATE TABLE, INSERT, UPDATE - RW)             │
└───────────────────────────────────────────────────────────────────────┘
```

## Implementation Options

### Option A: duckgres (Recommended)

**Repository:** https://github.com/PostHog/duckgres

**Pros:**
- Production-proven at PostHog (handles millions of queries/day)
- Written in Rust (performance, safety)
- Active maintenance
- Good PostgreSQL protocol coverage

**Cons:**
- Rust dependency (separate process, not embedded in Python)
- Need to build/maintain Rust codebase

**Integration approach:**
1. Run duckgres as separate process alongside FastAPI
2. Configure duckgres to authenticate against our workspace_credentials
3. On session init, duckgres opens workspace DuckDB file and ATTACHes project tables

### Option B: duckdb-pgwire

**Repository:** https://github.com/euiko/duckdb-pgwire

**Pros:**
- DuckDB extension (simpler deployment)
- C++ (native DuckDB integration)

**Cons:**
- Less mature (experimental)
- Less active maintenance
- Limited PostgreSQL protocol coverage

### Option C: Custom Python Implementation

**Library:** https://github.com/pgjones/pgwire (Python)

**Pros:**
- Full control
- Same language as FastAPI (shared codebase)
- Easy integration with existing workspace_credentials

**Cons:**
- More development effort
- Need to implement protocol from scratch
- Performance concerns for high-load scenarios

## Recommended Approach: duckgres + Custom Auth Bridge

```
┌─────────────────────────────────────────────────────────────────┐
│                        DEPLOYMENT                                │
│                                                                  │
│  ┌─────────────────────┐     ┌─────────────────────────────────┐ │
│  │   FastAPI Service   │     │      duckgres Server            │ │
│  │   (REST API)        │     │      (PG Wire)                  │ │
│  │   Port: 8000        │     │      Port: 5432                 │ │
│  │                     │     │                                 │ │
│  │  /workspaces/*      │────▶│  Auth callback ──────────────▶  │ │
│  │  /projects/*        │     │  (HTTP to FastAPI)              │ │
│  │  /buckets/*         │     │                                 │ │
│  └─────────────────────┘     └─────────────────────────────────┘ │
│            │                           │                          │
│            └───────────┬───────────────┘                          │
│                        ▼                                          │
│              ┌─────────────────────┐                              │
│              │   metadata.duckdb   │                              │
│              │   workspace_creds   │                              │
│              └─────────────────────┘                              │
│                        │                                          │
│                        ▼                                          │
│              ┌─────────────────────┐                              │
│              │  /data/duckdb/      │                              │
│              │  project_*/         │                              │
│              │  _workspaces/       │                              │
│              └─────────────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Steps

### Step 1: duckgres Evaluation & Setup

1. **Clone and build duckgres**
   ```bash
   git clone https://github.com/PostHog/duckgres
   cd duckgres
   cargo build --release
   ```

2. **Understand duckgres architecture**
   - How it handles connections
   - Where to inject custom auth
   - Session initialization hooks

3. **Create proof-of-concept**
   - Connect to a workspace DuckDB file
   - ATTACH a project table
   - Run basic queries

### Step 2: Authentication Bridge

Create FastAPI endpoint for duckgres to validate credentials:

```python
# src/routers/pgwire_auth.py

@router.post("/internal/pgwire/auth")
async def authenticate_pgwire_session(
    username: str,
    password: str,
) -> dict:
    """
    Called by duckgres to validate workspace credentials.
    Returns workspace info if valid, 401 if not.
    """
    # Parse username format: ws_{workspace_id}_{random}
    workspace_id = extract_workspace_id(username)

    # Get credentials from metadata
    creds = metadata_db.get_workspace_by_username(username)
    if not creds:
        raise HTTPException(401, "Invalid credentials")

    # Verify password hash
    if not verify_password(password, creds["password_hash"]):
        raise HTTPException(401, "Invalid credentials")

    # Check workspace not expired
    workspace = metadata_db.get_workspace(workspace_id)
    if is_expired(workspace):
        raise HTTPException(410, "Workspace expired")

    return {
        "workspace_id": workspace_id,
        "project_id": workspace["project_id"],
        "branch_id": workspace.get("branch_id"),
        "db_path": workspace["db_path"],
        "tables": list_project_tables(workspace["project_id"]),
    }
```

### Step 3: Session Initialization

Modify duckgres (or create wrapper) to initialize workspace sessions:

```rust
// Pseudo-code for duckgres modification

async fn on_client_authenticated(auth_response: AuthResponse) -> DuckDBConnection {
    // 1. Open workspace database
    let conn = duckdb::Connection::open(&auth_response.db_path)?;

    // 2. Set resource limits
    conn.execute("SET memory_limit='4GB'")?;
    conn.execute("SET temp_directory='/tmp/duckdb'")?;

    // 3. ATTACH all project tables as READ_ONLY
    for table in auth_response.tables {
        let alias = format!("{}_{}", table.bucket, table.name);
        conn.execute(&format!(
            "ATTACH '{}' AS {} (READ_ONLY)",
            table.path, alias
        ))?;
    }

    // 4. Create schema views for user convenience
    for table in auth_response.tables {
        let alias = format!("{}_{}", table.bucket, table.name);
        // Create schema if not exists
        conn.execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            table.bucket
        ))?;
        // Create view
        conn.execute(&format!(
            "CREATE OR REPLACE VIEW {}.{} AS SELECT * FROM {}.main.data",
            table.bucket, table.name, alias
        ))?;
    }

    Ok(conn)
}
```

### Step 4: Session Management

Track active sessions for:
- Idle timeout enforcement
- Connection limits per workspace
- Graceful shutdown

```python
# Session tracking in metadata
CREATE TABLE pgwire_sessions (
    session_id VARCHAR PRIMARY KEY,
    workspace_id VARCHAR NOT NULL,
    client_ip VARCHAR,
    connected_at TIMESTAMPTZ DEFAULT now(),
    last_activity_at TIMESTAMPTZ DEFAULT now(),
    query_count INTEGER DEFAULT 0,

    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
);
```

### Step 5: Connection Pooling & Limits

```python
@dataclass
class PGWireConfig:
    # Connection limits
    max_connections_total: int = 100
    max_connections_per_workspace: int = 5

    # Timeouts
    idle_timeout_seconds: int = 3600      # 1 hour
    query_timeout_seconds: int = 300       # 5 minutes
    connection_timeout_seconds: int = 30

    # Resource limits per session
    memory_limit: str = "4GB"
    temp_storage_limit: str = "10GB"
    max_attached_databases: int = 1000
```

## Deployment Options

### Option 1: Docker Compose (Development)

```yaml
# docker-compose.yml
services:
  api:
    build: ./duckdb-api-service
    ports:
      - "8000:8000"
    volumes:
      - ./data:/data
    environment:
      - DUCKDB_DATA_DIR=/data/duckdb
      - PGWIRE_AUTH_URL=http://localhost:8000/internal/pgwire/auth

  pgwire:
    build: ./duckgres
    ports:
      - "5432:5432"
    volumes:
      - ./data:/data
    environment:
      - AUTH_CALLBACK_URL=http://api:8000/internal/pgwire/auth
      - DATA_DIR=/data/duckdb
    depends_on:
      - api
```

### Option 2: Kubernetes (Production)

```yaml
# Sidecar pattern - pgwire container alongside API
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: api
    image: duckdb-api-service:latest
    ports:
    - containerPort: 8000
  - name: pgwire
    image: duckgres:latest
    ports:
    - containerPort: 5432
    env:
    - name: AUTH_CALLBACK_URL
      value: "http://localhost:8000/internal/pgwire/auth"
```

## Testing Strategy

### Unit Tests
- Auth bridge endpoint
- Password verification
- Session tracking

### Integration Tests
```python
def test_pgwire_connection():
    """Test connecting via psycopg2."""
    import psycopg2

    # Create workspace via REST API
    workspace = create_workspace(project_id="test", name="pg-test")

    # Connect via PG Wire
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database=f"workspace_{workspace['id']}",
        user=workspace['connection']['username'],
        password=workspace['connection']['password'],
    )

    # Query project table (read-only)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM in_c_sales.orders LIMIT 10")
        rows = cur.fetchall()
        assert len(rows) > 0

    # Create table in workspace (read-write)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE my_analysis AS SELECT * FROM in_c_sales.orders")
        conn.commit()

    conn.close()
```

### E2E Tests
- Full workflow: create workspace -> connect PG -> query -> drop
- Concurrent sessions
- Session timeout
- Connection limit enforcement

## Security Considerations

1. **TLS/SSL Required**
   - All PG Wire connections must use SSL in production
   - Self-signed certs OK for development

2. **Network Isolation**
   - PG Wire server only accessible within VPC
   - No direct internet exposure

3. **Audit Logging**
   - Log all connections (workspace_id, client_ip, timestamp)
   - Log query statistics (count, duration)

4. **Rate Limiting**
   - Max connections per IP
   - Max queries per minute

## Milestones

| Milestone | Description | Estimate |
|-----------|-------------|----------|
| M1 | duckgres POC (basic connection) | 2-3 days |
| M2 | Auth bridge implementation | 1-2 days |
| M3 | Session initialization (ATTACH) | 2-3 days |
| M4 | Session management & limits | 2 days |
| M5 | Docker deployment | 1 day |
| M6 | Integration tests | 2 days |
| M7 | Documentation | 1 day |

## Open Questions

1. **duckgres fork vs wrapper?**
   - Fork and modify duckgres directly?
   - Or create Python wrapper that spawns duckgres?

2. **Schema presentation?**
   - How to present `bucket.table` to users?
   - Views in bucket-named schemas?
   - Or flat namespace with `bucket_table` naming?

3. **Prepared statement support?**
   - Which level of support is needed?
   - psycopg2 uses prepared statements by default

4. **HA/Failover?**
   - Multiple pgwire instances?
   - Session affinity or stateless?

## References

- [duckgres Repository](https://github.com/PostHog/duckgres)
- [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [DuckDB ATTACH Statement](https://duckdb.org/docs/sql/statements/attach.html)
- [ADR-010: SQL Interface](../adr/010-duckdb-sql-interface.md)
- [Phase 11a: Workspace REST API](phase-11-workspaces.md)
