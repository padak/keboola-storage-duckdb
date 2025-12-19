# ADR-010: SQL Interface pro Workspaces (PostgreSQL Wire Protocol)

## Status

Accepted

## Datum

2024-12-18

## Kontext

Workspaces v Keboola slouzi k interaktivni praci s daty - SQL transformace, analyzy, explorace. Uzivatele potrebuji:

1. **Pripojit se** k workspace z externich nastroju (DBeaver, DataGrip, Python, R, BI tools)
2. **Spoustet SQL** interaktivne
3. **Cist data** z cele Storage projektu (read-only)
4. **Zapisovat** do workspace (read-write)

### Problem

DuckDB je **embedded databaze** - nema nativni server mode. Na rozdil od PostgreSQL, MySQL ci Snowflake neexistuje zpusob, jak se k DuckDB pripojit "pres sit".

### Jak to funguje v Keboola dnes

Pro Snowflake backend uzivatele dostanou credentials:
- Host (Snowflake account)
- Username
- Password / Private Key
- Database, Schema, Warehouse

Pak se pripoji standardnim Snowflake driverem (JDBC, ODBC, Python connector).

## Rozhodnuti

**Pouzijeme PostgreSQL Wire Protocol** pro pristup k DuckDB workspacum.

Konkretne vyhodnotime a nasadime jednu z techto implementaci:
1. **[duckgres](https://github.com/PostHog/duckgres)** - PostHog, produkcni pouziti
2. **[duckdb-pgwire](https://github.com/euiko/duckdb-pgwire)** - DuckDB extension + server

### Architektura

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
                                │ DuckDB Python/C API
                                │
┌───────────────────────────────▼──────────────────────────────────────┐
│                     WORKSPACE SESSION                                 │
│                                                                       │
│  workspace_123.duckdb (RW)     ← User's working space                │
│      │                                                                │
│      ├── ATTACH 'project_1/in_c_sales/orders.duckdb'                 │
│      │         AS in_c_sales_orders (READ_ONLY)                      │
│      ├── ATTACH 'project_1/in_c_sales/customers.duckdb'              │
│      │         AS in_c_sales_customers (READ_ONLY)                   │
│      ├── ATTACH 'project_1/out_c_reports/summary.duckdb'             │
│      │         AS out_c_reports_summary (READ_ONLY)                  │
│      └── ... (all project tables attached read-only)                 │
│                                                                       │
│  User can:                                                            │
│  - SELECT FROM any attached table (project data, read-only)          │
│  - CREATE TABLE, INSERT, UPDATE in workspace schema (read-write)     │
│  - Run transformations, CTEs, window functions, etc.                 │
└───────────────────────────────────────────────────────────────────────┘
```

### Workspace Lifecycle

```
1. CreateWorkspace API call
   ├── Create workspace_123.duckdb file
   ├── Generate credentials (username, password)
   ├── Store in metadata.duckdb
   └── Return connection string

2. User connects via PG protocol
   ├── PG Wire server authenticates
   ├── Opens workspace_123.duckdb
   ├── ATTACHes all project tables (READ_ONLY)
   └── Session ready

3. User runs queries
   ├── SELECT from project tables → reads from ATTACHed files
   ├── CREATE TABLE in workspace → writes to workspace_123.duckdb
   └── Full SQL support (JOINs across tables, CTEs, etc.)

4. DropWorkspace API call
   ├── Close all sessions
   ├── Delete workspace_123.duckdb
   └── Remove from metadata
```

## Duvody

### Proc PostgreSQL Wire Protocol?

| Kriteria | PG Wire | REST API | Arrow Flight SQL |
|----------|---------|----------|------------------|
| **Kompatibilita** | Vsechny SQL nastroje | Omezena | Rastouci |
| **Interaktivita** | Nativni | Omezena | Dobra |
| **Ecosystem** | Obrovsk y | - | Mensi |
| **Latence** | Nizka | Stredni | Nizka |
| **Streaming** | Ano | Ne | Ano |
| **Implementace** | Existuje | Mame | Slozita |

**Vyherce: PG Wire Protocol**

1. **Maximalni kompatibilita**: psql, DBeaver, DataGrip, Tableau, Python (psycopg2), R (RPostgres), Go (pgx), Java (JDBC), .NET...
2. **Produkcni reference**: PostHog pouziva duckgres v produkci
3. **Uzivatelska zkusenost**: Stejna jako PostgreSQL - zadne uceni
4. **Existujici implementace**: duckgres, duckdb-pgwire

### Proc ne REST API?

- Uz mame REST API pro management operace
- **Ale**: REST neni vhodny pro interaktivni SQL sessions
- Chybi: streaming, cursors, prepared statements, transactions

### Proc ne Arrow Flight SQL?

- Moderni, efektivni (zero-copy)
- **Ale**: Mensi podpora v nastrojich (zatim)
- Slozitejsi implementace
- **Moznost**: Pridat jako alternativu v budoucnu

## Dusledky

### Pozitivni

- Uzivatele se pripoji s jakymkoli PostgreSQL klientem
- Zadne nove nastroje - pouziji co znaji
- Produkcne overene reseni (PostHog)
- ATTACH READ_ONLY zajistuje bezpecnost produkcnich dat

### Negativni

- Dalsi komponenta k provozovani (PG Wire server)
- PG Wire neni 100% PostgreSQL - nektere features nebudou fungovat
- Memory overhead pro ATTACH (file descriptors)

### Omezeni PG Wire implementaci

| Feature | Podpora |
|---------|---------|
| SELECT, INSERT, UPDATE, DELETE | Ano |
| CREATE/DROP TABLE | Ano |
| JOINs, CTEs, Window Functions | Ano |
| Prepared Statements | Castecna |
| Transactions (BEGIN/COMMIT) | DuckDB semantika |
| PostgreSQL-specific functions | Ne (DuckDB funkce) |
| COPY FROM/TO | Ano (DuckDB syntaxe) |
| pg_catalog views | Castecna |
| Extensions (PostGIS, etc.) | Ne |

## Implementace

### Connection String Format

```
postgresql://ws_123_user:password@host:5432/workspace_123

# Nebo s parametry
Host: duckdb.keboola.local
Port: 5432
Database: workspace_123
Username: ws_123_user
Password: <generated>
SSL: required
```

### Credentials v Metadata

```sql
-- metadata.duckdb
CREATE TABLE workspace_credentials (
    workspace_id VARCHAR PRIMARY KEY,
    username VARCHAR NOT NULL,          -- ws_{workspace_id}_{random}
    password_hash VARCHAR NOT NULL,     -- SHA256
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,

    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
);
```

### Session Initialization (pseudo-code)

```python
async def on_client_connect(username: str, password: str) -> DuckDBConnection:
    # 1. Authenticate
    workspace_id = extract_workspace_id(username)
    if not verify_password(workspace_id, password):
        raise AuthenticationError()

    # 2. Get workspace info
    workspace = get_workspace(workspace_id)
    project_id = workspace.project_id

    # 3. Open workspace database
    conn = duckdb.connect(workspace.db_path)

    # 4. ATTACH all project tables as READ_ONLY
    tables = list_project_tables(project_id)
    for table in tables:
        alias = f"{table.bucket}_{table.name}"
        conn.execute(f"""
            ATTACH '{table.db_path}' AS {alias} (READ_ONLY)
        """)

    # 5. Create convenient views in workspace
    for table in tables:
        alias = f"{table.bucket}_{table.name}"
        conn.execute(f"""
            CREATE OR REPLACE VIEW {table.bucket}.{table.name} AS
            SELECT * FROM {alias}.main.data
        """)

    return conn
```

### Resource Limits

```python
@dataclass
class WorkspaceConfig:
    max_attached_tables: int = 1000      # Max ATTACHed databases
    max_memory_per_session: str = "4GB"  # DuckDB memory limit
    max_temp_storage: str = "10GB"       # Temp files for large queries
    session_timeout: int = 3600          # 1 hour idle timeout
    query_timeout: int = 300             # 5 min per query
```

## Alternativy (zamitnuty)

### 1. Pouze REST API

- Zamitnut: Spatna uzivatelska zkusenost pro interaktivni SQL

### 2. Arrow Flight SQL

- Odlozeno: Mensi ecosystem, slozitejsi implementace
- Moznost pridat pozdeji jako alternativu

### 3. Vlastni protokol

- Zamitnut: Zadna kompatibilita s existujicimi nastroji

### 4. SSH tunel k DuckDB souboru

- Zamitnut: Bezpecnostni riziko, slozita sprava

## Budouci rozsireni

1. **Connection pooling**: Pro vyssi zatez
2. **Read replicas**: ATTACH na vice strojich
3. **Arrow Flight SQL**: Jako alternativni protokol pro Python/data science
4. **Query governor**: Limity na CPU, memory, IO per session

## Reference

- [duckgres (PostHog)](https://github.com/PostHog/duckgres)
- [duckdb-pgwire](https://github.com/euiko/duckdb-pgwire)
- [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [DuckDB ATTACH](https://duckdb.org/docs/sql/statements/attach.html)
- [ADR-009: File per Table](009-duckdb-file-per-table.md) - zaklad pro ATTACH architekturu
