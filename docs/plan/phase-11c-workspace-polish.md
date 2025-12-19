# Phase 11c: Workspace Polish & Production Readiness

## Status
- **Status:** IN PROGRESS
- **Depends on:** Phase 11a (REST API) - DONE, Phase 11b (PG Wire) - DONE
- **Tests:** 62 E2E tests (target: 100+)
- **Last Updated:** 2024-12-19

## Goal

Polish the workspace implementation for production use:
- Comprehensive e2e testing
- Performance optimization
- Monitoring & observability
- Documentation

---

## Task Overview

| # | Task | Priority | Effort | Status |
|---|------|----------|--------|--------|
| 1 | Admin force-disconnect sessions | HIGH | 4h | TODO |
| 2 | Query audit logging | HIGH | 8h | TODO |
| 3 | Query performance tracing | HIGH | 8h | TODO |
| 4 | Error rate alerting | MEDIUM | 4h | TODO |
| 5 | Real PG Wire E2E tests | HIGH | 16h | TODO |
| 6 | Resource limits enforcement | MEDIUM | 8h | TODO |
| 7 | User documentation | HIGH | 8h | TODO |
| 8 | Load & performance tests | MEDIUM | 8h | TODO |

**Total estimated effort:** 64h

---

## Task 1: Admin Force-Disconnect Sessions

**Priority:** HIGH | **Effort:** 4h | **Status:** TODO

### Popis
Endpoint pro administratora k okamzitemu ukonceni PG Wire sessions - jednotlive nebo vsech na workspace.

### Endpoints

```
DELETE /projects/{id}/workspaces/{ws_id}/sessions/{session_id}
POST   /projects/{id}/workspaces/{ws_id}/disconnect-all
```

### Request/Response

**DELETE session:**
```bash
curl -X DELETE \
  -H "Authorization: Bearer $PROJECT_KEY" \
  /projects/123/workspaces/ws_abc/sessions/sess_xyz

# Response 200:
{"message": "Session disconnected", "session_id": "sess_xyz"}
```

**Disconnect all:**
```bash
curl -X POST \
  -H "Authorization: Bearer $PROJECT_KEY" \
  /projects/123/workspaces/ws_abc/disconnect-all

# Response 200:
{"message": "All sessions disconnected", "count": 3}
```

### Implementace

1. **Router:** `src/routers/workspaces.py`
   - Pridat dva endpointy
   - Autorizace: project admin key

2. **PG Wire Server:** `src/pgwire_server.py`
   - Pridat metodu `disconnect_session(session_id)`
   - Pridat metodu `disconnect_all_sessions(workspace_id)`
   - Graceful close - poslat PostgreSQL error message pred odpojenim

3. **Komunikace REST <-> PG Wire:**
   - Varianta A: Sdileny objekt v pameti (WorkspaceSessionManager)
   - Varianta B: Signal pres soubor/socket
   - **Doporuceni:** Varianta A - jednodussi pro single-instance

### Testy

```python
def test_admin_disconnect_single_session():
    """Disconnect specific session, verify connection closed."""

def test_admin_disconnect_all_sessions():
    """Disconnect all sessions on workspace."""

def test_disconnect_nonexistent_session():
    """Returns 404 for unknown session."""

def test_disconnect_requires_auth():
    """Requires project admin key."""
```

### Acceptance Criteria

- [ ] Admin muze odpojit konkretni session podle ID
- [ ] Admin muze odpojit vsechny sessions na workspace
- [ ] Odpojeny klient dostane PostgreSQL error (ne jen TCP reset)
- [ ] Metriky `pgwire_connections_active` se aktualizuji
- [ ] Log entry pro kazde odpojeni

---

## Task 2: Query Audit Logging

**Priority:** HIGH | **Effort:** 8h | **Status:** TODO

### Popis
Logovani KAZDEHO SQL dotazu - kdo, kdy, co, jak dlouho, vysledek. Pro compliance, debugging a analytics.

### Log Format (structlog JSON)

```json
{
  "event": "query_executed",
  "timestamp": "2024-12-19T10:30:45.123Z",
  "workspace_id": "ws_abc123",
  "project_id": "123",
  "session_id": "sess_xyz",
  "username": "ws_abc123_user",
  "client_ip": "192.168.1.100",
  "query": "SELECT * FROM in_c_sales.orders WHERE date > '2024-01-01'",
  "query_hash": "sha256:abc123...",
  "duration_ms": 234,
  "rows_returned": 1500,
  "bytes_returned": 45000,
  "status": "success",
  "error": null
}
```

### Implementace

1. **PG Wire Server:** `src/pgwire_server.py`
   - V metode `execute()` logovat pred a po provedeni
   - Zachytit: query text, duration, row count, error

2. **Query sanitization:**
   - Nelogovat hodnoty v INSERT/UPDATE (mohou obsahovat PII)
   - Varianta: logovat jen strukturu (`INSERT INTO x VALUES (?, ?, ?)`)
   - Konfigurovatelne pres `settings.query_log_mode`: `full` | `structure` | `hash_only`

3. **Storage:**
   - Varianta A: Jen structlog -> stdout/file (default)
   - Varianta B: Do DuckDB tabulky `query_log` (pro analytics)
   - **Doporuceni:** Zacit s A, B jako rozsireni

4. **Config:**
   ```python
   # config.py
   query_audit_enabled: bool = True
   query_audit_mode: str = "full"  # full | structure | hash_only
   query_audit_max_length: int = 10000  # truncate long queries
   ```

### Testy

```python
def test_query_logged_on_success():
    """Successful query is logged with all fields."""

def test_query_logged_on_error():
    """Failed query is logged with error details."""

def test_query_sanitization_structure_mode():
    """In structure mode, VALUES are replaced with placeholders."""

def test_query_truncation():
    """Long queries are truncated to max_length."""

def test_audit_disabled():
    """When disabled, queries are not logged."""
```

### Acceptance Criteria

- [ ] Kazdy SQL dotaz je zalogovan
- [ ] Log obsahuje: workspace_id, session_id, query, duration, rows, status
- [ ] Podpora pro sanitizaci (structure mode)
- [ ] Konfigurovatelne zapnuti/vypnuti
- [ ] Dlouhe dotazy jsou truncated
- [ ] Performance impact < 5ms per query

---

## Task 3: Query Performance Tracing

**Priority:** HIGH | **Effort:** 8h | **Status:** TODO

### Popis
Detailni trace dotazu - EXPLAIN ANALYZE, memory, I/O. Pro identifikaci pomalych dotazu a optimalizaci.

### Implementace

1. **Automatic EXPLAIN pro pomale dotazy:**
   ```python
   # Pokud dotaz trva > threshold, automaticky spustit EXPLAIN ANALYZE
   if duration_ms > settings.slow_query_threshold_ms:
       explain_result = conn.execute(f"EXPLAIN ANALYZE {query}")
       logger.warning("slow_query_detected",
           query=query,
           duration_ms=duration_ms,
           explain=explain_result
       )
   ```

2. **Prometheus metriky:**
   ```python
   # Histogram s buckety pro latenci
   QUERY_DURATION_HISTOGRAM = Histogram(
       "pgwire_query_duration_seconds",
       buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]
   )

   # Counter pro pomale dotazy
   SLOW_QUERIES_TOTAL = Counter("pgwire_slow_queries_total")
   ```

3. **Config:**
   ```python
   # config.py
   slow_query_threshold_ms: int = 1000  # 1 sekunda
   slow_query_explain: bool = True
   slow_query_log_full: bool = True  # log cely dotaz, ne jen hash
   ```

4. **Dashboard update:**
   - Pridat panel "Slow Queries" do dashboard.html
   - Graf slow_queries_total over time
   - Tabulka recent slow queries (z logu)

### Testy

```python
def test_slow_query_detected():
    """Query over threshold triggers slow query log."""

def test_slow_query_explain_captured():
    """EXPLAIN ANALYZE is captured for slow queries."""

def test_fast_query_no_explain():
    """Fast queries don't trigger EXPLAIN overhead."""

def test_slow_query_metric_incremented():
    """Prometheus counter incremented for slow queries."""
```

### Acceptance Criteria

- [ ] Dotazy nad threshold jsou oznaceny jako "slow"
- [ ] Pro slow queries se loguje EXPLAIN ANALYZE
- [ ] Prometheus metrika `pgwire_slow_queries_total`
- [ ] Konfigurovatelny threshold
- [ ] Dashboard zobrazuje slow queries

---

## Task 4: Error Rate Alerting

**Priority:** MEDIUM | **Effort:** 4h | **Status:** TODO

### Popis
Konfigurace alertu v Prometheus/Grafana pro automaticke notifikace pri problemech.

### Alert Rules (Prometheus)

```yaml
# alerts.yml
groups:
  - name: pgwire_alerts
    rules:
      # Vysoka error rate
      - alert: PGWireHighErrorRate
        expr: |
          rate(pgwire_queries_total{status="error"}[5m])
          / rate(pgwire_queries_total[5m]) > 0.05
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "PG Wire error rate > 5%"
          description: "Error rate is {{ $value | humanizePercentage }}"

      # Failed logins (brute force detection)
      - alert: PGWireAuthFailures
        expr: rate(pgwire_connections_total{status="auth_failed"}[5m]) > 0.1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Multiple PG Wire auth failures"
          description: "Possible brute force attack"

      # Pomale dotazy
      - alert: PGWireSlowQueries
        expr: rate(pgwire_slow_queries_total[5m]) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High rate of slow queries"

      # Server down
      - alert: PGWireServerDown
        expr: up{job="pgwire"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "PG Wire server is down"
```

### Alert Delivery: Generic Webhook

Alerty se posilaji pres **konfigurovatelny webhook** - flexibilni reseni pro Slack, Discord, Teams, custom endpointy.

```python
# config.py
alert_webhook_url: str | None = None  # None = alerts disabled
alert_webhook_headers: dict = {}       # Optional custom headers
alert_min_interval_seconds: int = 60   # Rate limiting (1 alert/min per type)
```

### Webhook Payload Format

```json
{
  "alert": "PGWireHighErrorRate",
  "severity": "warning",
  "timestamp": "2024-12-19T10:30:45Z",
  "summary": "PG Wire error rate > 5%",
  "description": "Error rate is 7.3% over last 5 minutes",
  "context": {
    "project_id": "123",
    "project_name": "My Project",
    "workspace_id": "ws_abc123",
    "branch_id": "default",
    "session_id": "sess_xyz",
    "client_ip": "192.168.1.100"
  },
  "labels": {
    "service": "pgwire",
    "instance": "duckdb-api-1"
  },
  "value": 0.073
}
```

**Typy alertu a jejich kontext:**

| Alert | Kontext |
|-------|---------|
| `PGWireHighErrorRate` | project_id, workspace_id (agregace per workspace) |
| `PGWireAuthFailures` | project_id, client_ip, username (attempted) |
| `PGWireSlowQuery` | project_id, workspace_id, session_id, query_hash |
| `PGWireServerDown` | instance only (global) |
| `WorkspaceSizeExceeded` | project_id, workspace_id, current_size, limit |

### Implementace

**Varianta A: Vlastni alerting v Pythonu (doporuceno pro jednoduchost)**

```python
# src/alerting.py
import httpx
from datetime import datetime, timedelta

class AlertManager:
    def __init__(self, webhook_url: str | None, min_interval: int = 60):
        self.webhook_url = webhook_url
        self.min_interval = min_interval
        self._last_sent: dict[str, datetime] = {}

    async def send_alert(self, alert_name: str, severity: str, summary: str, **kwargs):
        if not self.webhook_url:
            return  # Alerts disabled

        # Rate limiting
        now = datetime.utcnow()
        if alert_name in self._last_sent:
            if now - self._last_sent[alert_name] < timedelta(seconds=self.min_interval):
                return  # Too soon, skip

        payload = {
            "alert": alert_name,
            "severity": severity,
            "timestamp": now.isoformat() + "Z",
            "summary": summary,
            **kwargs
        }

        async with httpx.AsyncClient() as client:
            await client.post(self.webhook_url, json=payload)

        self._last_sent[alert_name] = now
```

**Pouziti v kodu:**

```python
# V pgwire_server.py
if error_rate > 0.05:
    await alert_manager.send_alert(
        "PGWireHighErrorRate",
        severity="warning",
        summary=f"PG Wire error rate > 5%",
        description=f"Error rate is {error_rate:.1%}",
        value=error_rate
    )
```

**Varianta B: Prometheus Alertmanager (pro slozitejsi setup)**

Pouzit standardni Alertmanager s webhook receiverem - vhodne pokud uz Alertmanager bezi.

### Deliverables

- [ ] `src/alerting.py` - AlertManager trida
- [ ] Integrace do `pgwire_server.py` (error rate, auth failures)
- [ ] Integrace do `main.py` (server health)
- [ ] Config v `config.py` (webhook_url, headers, rate limit)
- [ ] Dokumentace nastaveni v README

---

## Task 5: Real PG Wire E2E Tests

**Priority:** HIGH | **Effort:** 16h | **Status:** TODO

### Popis
Automatizovane testy ktere skutecne startnou PG Wire server a pripoji se pres psycopg2.

### Test File: `tests/test_pgwire_e2e.py`

```python
import pytest
import psycopg2
import subprocess
import time

@pytest.fixture(scope="module")
def pgwire_server():
    """Start PG Wire server for tests."""
    proc = subprocess.Popen([
        "python", "-m", "src.pgwire_server",
        "--host", "127.0.0.1",
        "--port", "15432"  # non-standard port for tests
    ])
    time.sleep(2)  # wait for startup
    yield proc
    proc.terminate()
    proc.wait()

@pytest.fixture
def workspace_connection(test_client, pgwire_server):
    """Create workspace and return connection params."""
    # Create workspace via REST API
    resp = test_client.post("/projects/test/workspaces", json={"name": "e2e-test"})
    ws = resp.json()

    yield {
        "host": "127.0.0.1",
        "port": 15432,
        "database": f"workspace_{ws['id']}",
        "user": ws["connection"]["username"],
        "password": ws["connection"]["password"]
    }

    # Cleanup
    test_client.delete(f"/projects/test/workspaces/{ws['id']}")


class TestPGWireConnection:
    """Test basic PG Wire connectivity."""

    def test_connect_with_valid_credentials(self, workspace_connection):
        """Can connect with valid credentials."""
        conn = psycopg2.connect(**workspace_connection)
        assert conn.status == psycopg2.extensions.STATUS_READY
        conn.close()

    def test_connect_with_invalid_password(self, workspace_connection):
        """Reject invalid password."""
        params = {**workspace_connection, "password": "wrong"}
        with pytest.raises(psycopg2.OperationalError):
            psycopg2.connect(**params)

    def test_connect_to_expired_workspace(self, ...):
        """Reject connection to expired workspace."""


class TestPGWireQueries:
    """Test query execution."""

    def test_select_from_project_table(self, workspace_connection):
        """Can SELECT from ATTACHed project tables."""
        conn = psycopg2.connect(**workspace_connection)
        cur = conn.cursor()
        cur.execute("SELECT * FROM in_c_test.sample_table LIMIT 10")
        rows = cur.fetchall()
        assert len(rows) <= 10
        conn.close()

    def test_create_table_in_workspace(self, workspace_connection):
        """Can CREATE TABLE in workspace."""
        conn = psycopg2.connect(**workspace_connection)
        cur = conn.cursor()
        cur.execute("CREATE TABLE my_temp AS SELECT 1 as id")
        cur.execute("SELECT * FROM my_temp")
        assert cur.fetchone() == (1,)
        conn.close()

    def test_cannot_write_to_project_table(self, workspace_connection):
        """Cannot INSERT/UPDATE/DELETE project tables (READ_ONLY)."""
        conn = psycopg2.connect(**workspace_connection)
        cur = conn.cursor()
        with pytest.raises(psycopg2.Error):
            cur.execute("INSERT INTO in_c_test.sample_table VALUES (1, 'test')")
        conn.close()

    def test_cross_table_join(self, workspace_connection):
        """Can JOIN across multiple project tables."""

    def test_cte_query(self, workspace_connection):
        """CTEs work correctly."""

    def test_window_functions(self, workspace_connection):
        """Window functions work correctly."""


class TestPGWireConcurrency:
    """Test concurrent connections."""

    def test_multiple_connections_same_workspace(self, workspace_connection):
        """Multiple connections to same workspace work."""
        conns = [psycopg2.connect(**workspace_connection) for _ in range(5)]
        # Run queries on all
        for conn in conns:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        for conn in conns:
            conn.close()

    def test_connection_limit_enforced(self, workspace_connection):
        """Connection limit is enforced."""
        # Open max connections, then try one more

    def test_parallel_queries(self, workspace_connection):
        """Parallel queries from different connections."""


class TestPGWireIsolation:
    """Test workspace isolation."""

    def test_cannot_see_other_workspace_data(self, ...):
        """Workspace A cannot see data from Workspace B."""

    def test_cannot_see_other_project_data(self, ...):
        """Cannot access tables from other projects."""


class TestPGWireSSL:
    """Test SSL/TLS connections."""

    def test_ssl_connection(self, workspace_connection):
        """Can connect with SSL."""
        params = {**workspace_connection, "sslmode": "require"}
        conn = psycopg2.connect(**params)
        # Verify SSL is active
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()


class TestPGWireTimeouts:
    """Test timeout handling."""

    def test_query_timeout(self, workspace_connection):
        """Long query is terminated after timeout."""
        conn = psycopg2.connect(**workspace_connection)
        cur = conn.cursor()
        with pytest.raises(psycopg2.Error):
            # This should timeout (infinite loop simulation)
            cur.execute("SELECT * FROM range(1000000000)")
        conn.close()

    def test_idle_timeout(self, ...):
        """Idle connection is closed after timeout."""
```

### Acceptance Criteria

- [ ] Testy bezi v CI/CD (GitHub Actions)
- [ ] Pokryti: connect, query, isolation, SSL, timeouts
- [ ] Minimalne 20 novych E2E testu
- [ ] Vsechny testy PASS

---

## Task 6: Resource Limits Enforcement

**Priority:** MEDIUM | **Effort:** 8h | **Status:** TODO

### Popis
Aktivni vynucovani limitu na workspace - pamet, disk, velikost vysledku.

### Implementace

1. **Memory limit per session:**
   ```python
   # Pri inicializaci session
   conn.execute(f"SET memory_limit='{workspace.memory_limit}'")  # e.g., "4GB"
   ```

2. **Query result size limit:**
   ```python
   # Wrapper kolem execute
   def execute_with_limits(query, max_rows=100000, max_bytes=100_000_000):
       result = conn.execute(query)
       if result.rowcount > max_rows:
           raise QueryLimitExceeded(f"Result exceeds {max_rows} rows")
       # Check bytes...
   ```

3. **Workspace disk size:**
   ```python
   # Periodic check (background task)
   async def check_workspace_sizes():
       for ws in list_workspaces():
           size = get_file_size(ws.db_path)
           if size > ws.size_limit_bytes:
               logger.warning("workspace_size_exceeded", ...)
               # Option: block writes, alert admin
   ```

4. **Temp storage cleanup:**
   ```python
   # DuckDB temp directory
   conn.execute(f"SET temp_directory='{workspace_temp_dir}'")
   # Cleanup on session close
   shutil.rmtree(workspace_temp_dir)
   ```

5. **Config:**
   ```python
   # config.py
   workspace_default_memory_limit: str = "4GB"
   workspace_default_size_limit_gb: int = 10
   workspace_max_result_rows: int = 100000
   workspace_max_result_bytes: int = 100_000_000  # 100MB
   ```

### Testy

```python
def test_memory_limit_enforced():
    """Query exceeding memory limit fails gracefully."""

def test_result_row_limit():
    """Query returning too many rows is rejected."""

def test_workspace_size_tracking():
    """Workspace size is tracked and reported."""

def test_temp_storage_cleanup():
    """Temp files are cleaned up after session."""
```

### Acceptance Criteria

- [ ] Memory limit konfigurovatelny per workspace
- [ ] Result size limit (rows + bytes)
- [ ] Workspace disk size tracking
- [ ] Temp storage cleanup
- [ ] Graceful error messages pri prekroceni limitu

---

## Task 7: User Documentation

**Priority:** HIGH | **Effort:** 8h | **Status:** TODO

### Popis
Kompletni dokumentace pro koncove uzivatele - jak se pripojit z ruznych nastroju.

### Deliverables

1. **`docs/workspace-connection-guide.md`**

```markdown
# Workspace Connection Guide

## Connection Parameters

After creating a workspace, you receive:
- Host: `duckdb.keboola.local` (or your server IP)
- Port: `5432`
- Database: `workspace_{id}`
- Username: `ws_{id}_{random}`
- Password: (shown only once on creation)

## Connecting from Different Tools

### psql (Command Line)
\`\`\`bash
psql "host=localhost port=5432 dbname=workspace_abc user=ws_abc_xyz"
# Enter password when prompted
\`\`\`

### DBeaver
1. New Connection -> PostgreSQL
2. Host: localhost, Port: 5432
3. Database: workspace_abc
4. Username: ws_abc_xyz
5. Password: (your password)
6. SSL: Require (production) / Disable (development)

### Python (psycopg2)
\`\`\`python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="workspace_abc",
    user="ws_abc_xyz",
    password="your_password"
)

cur = conn.cursor()
cur.execute("SELECT * FROM in_c_sales.orders LIMIT 10")
rows = cur.fetchall()
\`\`\`

### Python (SQLAlchemy)
\`\`\`python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://ws_abc_xyz:password@localhost:5432/workspace_abc"
)

with engine.connect() as conn:
    result = conn.execute("SELECT * FROM in_c_sales.orders")
\`\`\`

### R (RPostgres)
\`\`\`r
library(DBI)
library(RPostgres)

con <- dbConnect(
    Postgres(),
    host = "localhost",
    port = 5432,
    dbname = "workspace_abc",
    user = "ws_abc_xyz",
    password = "your_password"
)

data <- dbGetQuery(con, "SELECT * FROM in_c_sales.orders")
\`\`\`

### Tableau
1. Connect -> PostgreSQL
2. Server: localhost
3. Port: 5432
4. Database: workspace_abc
5. Username/Password: (your credentials)

## Available Schemas

Your workspace has access to all project tables as READ-ONLY:

- `in_c_*` - Input buckets
- `out_c_*` - Output buckets

Example:
\`\`\`sql
SELECT * FROM in_c_sales.orders;
SELECT * FROM in_c_sales.customers;
SELECT * FROM out_c_reports.summary;
\`\`\`

## Creating Your Own Tables

You can create tables in your workspace (READ-WRITE):

\`\`\`sql
CREATE TABLE my_analysis AS
SELECT customer_id, SUM(amount) as total
FROM in_c_sales.orders
GROUP BY customer_id;
\`\`\`

## Limitations

- Cannot modify project tables (INSERT/UPDATE/DELETE blocked)
- Query timeout: 5 minutes (configurable)
- Memory limit: 4GB per session
- Workspace expires after 24 hours (configurable)
```

2. **`docs/workspace-troubleshooting.md`**
   - Common errors and solutions
   - Connection refused, auth failed, timeout, etc.

3. **`docs/workspace-performance.md`**
   - Tips for writing efficient queries
   - When to use COPY vs SELECT
   - Index usage

### Acceptance Criteria

- [ ] Connection guide pro 5+ nastroju
- [ ] Troubleshooting guide
- [ ] Performance tips
- [ ] Vsechny priklady otestovane

---

## Task 8: Load & Performance Tests

**Priority:** MEDIUM | **Effort:** 8h | **Status:** TODO

### Popis
Testy zateze pro zjisteni limitu systemu a identifikaci bottlenecku.

### Test Scenarios

1. **Concurrent connections:**
   - 10, 25, 50, 100 soucasnych spojeni
   - Merit: connection time, memory usage

2. **Query throughput:**
   - Simple SELECT (1 row)
   - Medium SELECT (1000 rows)
   - Large SELECT (100k rows)
   - JOIN across 2 tables
   - Aggregation (GROUP BY)

3. **Long-running queries:**
   - 1 min, 5 min, 10 min queries
   - Memory usage over time

4. **Mixed workload:**
   - 50% reads, 50% writes
   - Realistic usage pattern

### Tool: locust

```python
# tests/load/locustfile.py
from locust import User, task, between
import psycopg2

class WorkspaceUser(User):
    wait_time = between(1, 3)

    def on_start(self):
        # Create workspace, get connection
        self.conn = psycopg2.connect(...)

    @task(10)
    def simple_select(self):
        cur = self.conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()

    @task(5)
    def medium_select(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM in_c_test.orders LIMIT 1000")
        cur.fetchall()

    @task(1)
    def complex_join(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT o.*, c.name
            FROM in_c_test.orders o
            JOIN in_c_test.customers c ON o.customer_id = c.id
            LIMIT 1000
        """)
        cur.fetchall()
```

### Metrics to Capture

- Connections per second
- Queries per second
- P50, P95, P99 latency
- Error rate
- Memory usage (RSS)
- CPU usage
- File descriptors

### Deliverables

- [ ] `tests/load/locustfile.py`
- [ ] `tests/load/README.md` (how to run)
- [ ] Baseline report (results on reference hardware)
- [ ] Identified bottlenecks and recommendations

### Acceptance Criteria

- [ ] System handles 50 concurrent connections
- [ ] P95 latency < 500ms for simple queries
- [ ] No memory leaks over 1h test
- [ ] Graceful degradation under overload

---

## Completed Items (Reference)

### PG Wire Server Improvements - DONE

- [x] Prometheus metrics for connections and queries
- [x] Query timeout enforcement
- [x] Graceful shutdown
- [x] Structured logging

### Existing Tests

- 41 workspace REST API tests
- 26 pgwire_auth tests
- 62 E2E tests

---

## Success Criteria (Phase 11c Complete)

- [ ] All 8 tasks implemented and tested
- [ ] 100+ total tests (currently 62 E2E)
- [ ] Documentation complete
- [ ] Load test baseline established
- [ ] Zero critical bugs

## References

- [Phase 11a: Workspace REST API](phase-11-workspaces.md)
- [Phase 11b: PG Wire Server](phase-11b-pgwire.md)
- [ADR-010: SQL Interface](../adr/010-duckdb-sql-interface.md)
