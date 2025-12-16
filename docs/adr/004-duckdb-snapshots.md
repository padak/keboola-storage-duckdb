# ADR-004: Snapshoty jako Parquet soubory

## Status

**Accepted** - rozsireno 2024-12-16 o konfiguracni hierarchii

## Datum

2024-12-11 (puvodni), 2024-12-16 (rozsireni o konfiguraci)

## Kontext

Keboola Storage poskytuje snapshoty tabulek pro:
- Point-in-time recovery
- Ochrana pred omylnym smazanim dat
- Audit trail

DuckDB **nema nativni time-travel** jako Snowflake (`SELECT * FROM table AT TIMESTAMP`). Potrebujeme implementovat vlastni snapshot mechanismus.

### Moznosti ulozeni

1. **Kopirovani do jine tabulky** - snapshot jako tabulka `_snapshot_xxx`
2. **Export do Parquet** - snapshot jako externi soubor
3. **Kopirovani celeho DuckDB souboru** - snapshot na urovni databaze

## Rozhodnuti

### Cast 1: Format ulozeni

**Zvolili jsme export do Parquet souboru.**

### Cast 2: Konfigurace (rozsireni 2024-12-16)

**Zvolili jsme hierarchickou konfiguraci s dedenim: System → Project → Bucket → Table**

---

## Format ulozeni (Parquet)

### Struktura snapshotu

```
/data/snapshots/
├── project_123/
│   ├── snap_orders_20241211_143022/
│   │   ├── metadata.json
│   │   └── data.parquet
│   ├── snap_customers_20241211_150000/
│   │   ├── metadata.json
│   │   └── data.parquet
│   └── auto_predrop_orders_20241211_160000/
│       ├── metadata.json
│       └── data.parquet
```

> **Zmena oproti puvodnimu:** Snapshot je per-tabulka (1 snapshot = 1 tabulka),
> ne per-projekt s vice tabulkami. Lepe odpovida ADR-009 (per-table soubory).

### Metadata format

```json
{
  "snapshot_id": "snap_orders_20241211_143022",
  "project_id": "123",
  "bucket_name": "in_c_sales",
  "table_name": "orders",
  "snapshot_type": "manual",
  "created_at": "2024-12-11T14:30:22Z",
  "created_by": "user@example.com",
  "expires_at": "2025-03-11T14:30:22Z",
  "description": "Before major refactoring",
  "row_count": 150000,
  "size_bytes": 45000000,
  "schema": {
    "columns": [
      {"name": "id", "type": "BIGINT", "nullable": false},
      {"name": "name", "type": "VARCHAR", "nullable": true},
      {"name": "email", "type": "VARCHAR", "nullable": true}
    ],
    "primary_key": ["id"]
  }
}
```

### Snapshot registry (metadata.duckdb)

```sql
CREATE TABLE snapshots (
    id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    bucket_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    snapshot_type VARCHAR NOT NULL,      -- 'manual' | 'auto_predrop' | 'auto_pretruncate' | ...

    parquet_path VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    schema_json JSON NOT NULL,

    created_at TIMESTAMPTZ DEFAULT now(),
    created_by VARCHAR,
    expires_at TIMESTAMPTZ,
    description TEXT,

    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE INDEX idx_snapshots_project ON snapshots(project_id);
CREATE INDEX idx_snapshots_table ON snapshots(project_id, bucket_name, table_name);
CREATE INDEX idx_snapshots_expires ON snapshots(expires_at);
```

### Duvody pro Parquet

**Proc ne snapshot tabulky v DuckDB?**
1. Velikost databaze: Snapshoty by zvetsovaly hlavni DB soubor
2. Namespace kolize: Potreba komplexniho pojmenovani
3. Obtizna sprava: Mazani snapshotu = DELETE operace

**Proc ne kopirovani celeho DB souboru?**
1. Neefektivni: Kopirovat 500GB kvuli 1 tabulce
2. Pomale: Dlouha doba vytvoreni snapshotu
3. Nakladne na uloziste: Plytva mistem

**Proc Parquet?**
1. Komprese: 75-95% kompresni pomer (ZSTD)
2. Rychle: DuckDB exportuje Parquet velmi efektivne
3. Portabilni: Parquet lze cist kdekoliv (Python, Spark, ...)
4. Selektivni: Snapshot jen konkretni tabulky
5. Snadna obnova: `CREATE TABLE AS SELECT * FROM read_parquet(...)`

---

## Konfigurace snapshotu (rozsireni 2024-12-16)

### Hierarchie dedeni

```
SYSTEM DEFAULTS (hardcoded)
    ↓ prepsano
PROJECT settings
    ↓ prepsano
BUCKET settings
    ↓ prepsano
TABLE settings (nejvyssi priorita)
```

Kazda uroven muze prepsat nastaveni z vyssi urovne. Pokud neni nastaveno, dedi se.

### System defaults (hardcoded)

```python
SYSTEM_DEFAULTS = {
    "auto_snapshot_triggers": {
        "drop_table": True,       # Snapshot pred DROP TABLE
        "truncate_table": False,  # Snapshot pred TRUNCATE
        "delete_all_rows": False, # Snapshot pred DELETE FROM bez WHERE
        "drop_column": False      # Snapshot pred ALTER TABLE DROP COLUMN
    },
    "retention": {
        "manual_days": 90,        # Manualni snapshoty: 90 dni
        "auto_days": 7            # Automaticke snapshoty: 7 dni
    },
    "enabled": True               # Master switch
}
```

### Konfiguracni schema

```json
{
  "auto_snapshot_triggers": {
    "drop_table": true,
    "truncate_table": false,
    "delete_all_rows": false,
    "drop_column": false
  },
  "retention": {
    "manual_days": 90,
    "auto_days": 7
  },
  "enabled": true
}
```

Pri ukladani se ukladaji **pouze explicitne nastavene hodnoty** (partial config).
Hodnoty `null` nebo chybejici klic = dedi z vyssi urovne.

### Storage konfigurace (metadata.duckdb)

```sql
CREATE TABLE snapshot_settings (
    id VARCHAR PRIMARY KEY,
    entity_type VARCHAR NOT NULL,        -- 'project' | 'bucket' | 'table'
    entity_id VARCHAR NOT NULL,          -- identifikator entity
    project_id VARCHAR NOT NULL,         -- vzdy vyplneno (pro FK a dotazy)

    config JSON NOT NULL,                -- partial config (jen explicitni hodnoty)

    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),

    UNIQUE (entity_type, entity_id),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE INDEX idx_snapshot_settings_entity ON snapshot_settings(entity_type, entity_id);
CREATE INDEX idx_snapshot_settings_project ON snapshot_settings(project_id);
```

### Entity ID format

| Entity Type | Entity ID Format | Priklad |
|-------------|------------------|---------|
| project | `{project_id}` | `123` |
| bucket | `{project_id}/{bucket_name}` | `123/in_c_sales` |
| table | `{project_id}/{bucket_name}/{table_name}` | `123/in_c_sales/orders` |

### API Endpoints

```
# Project level
GET    /projects/{id}/settings/snapshots
PUT    /projects/{id}/settings/snapshots
DELETE /projects/{id}/settings/snapshots

# Bucket level
GET    /projects/{id}/buckets/{bucket}/settings/snapshots
PUT    /projects/{id}/buckets/{bucket}/settings/snapshots
DELETE /projects/{id}/buckets/{bucket}/settings/snapshots

# Table level
GET    /projects/{id}/buckets/{bucket}/tables/{table}/settings/snapshots
PUT    /projects/{id}/buckets/{bucket}/tables/{table}/settings/snapshots
DELETE /projects/{id}/buckets/{bucket}/tables/{table}/settings/snapshots
```

### GET Response format

Vraci **effective config** (po aplikaci dedeni) + informace o zdroji:

```json
{
  "effective_config": {
    "auto_snapshot_triggers": {
      "drop_table": true,
      "truncate_table": true,
      "delete_all_rows": false,
      "drop_column": false
    },
    "retention": {
      "manual_days": 90,
      "auto_days": 7
    },
    "enabled": true
  },
  "inheritance": {
    "auto_snapshot_triggers.drop_table": "system",
    "auto_snapshot_triggers.truncate_table": "project",
    "auto_snapshot_triggers.delete_all_rows": "system",
    "auto_snapshot_triggers.drop_column": "system",
    "retention.manual_days": "system",
    "retention.auto_days": "system",
    "enabled": "system"
  },
  "local_config": {
    "auto_snapshot_triggers": {
      "truncate_table": true
    }
  }
}
```

### PUT Request format

Nastavuje **pouze lokalni konfiguraci** (partial update):

```json
{
  "auto_snapshot_triggers": {
    "truncate_table": true
  }
}
```

Prazdny objekt `{}` nebo `null` hodnoty = odebrani lokalni hodnoty (dedi se).

### DELETE

Smaze celou lokalni konfiguraci pro danou entitu. Entita bude plne dedit z vyssi urovne.

### Config Resolver implementace

```python
from copy import deepcopy

SYSTEM_DEFAULTS = {
    "auto_snapshot_triggers": {
        "drop_table": True,
        "truncate_table": False,
        "delete_all_rows": False,
        "drop_column": False
    },
    "retention": {
        "manual_days": 90,
        "auto_days": 7
    },
    "enabled": True
}

def deep_merge(base: dict, override: dict) -> dict:
    """Merge override into base, returning new dict."""
    result = deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        elif value is not None:
            result[key] = value
    return result

def resolve_snapshot_config(
    project_id: str,
    bucket_name: str | None = None,
    table_name: str | None = None
) -> tuple[dict, dict]:
    """
    Resolve effective config with inheritance.

    Returns:
        (effective_config, inheritance_sources)
    """
    config = deepcopy(SYSTEM_DEFAULTS)
    sources = {k: "system" for k in flatten_keys(config)}

    # Layer 1: Project settings
    project_settings = get_settings("project", project_id)
    if project_settings:
        config = deep_merge(config, project_settings)
        for key in flatten_keys(project_settings):
            sources[key] = "project"

    # Layer 2: Bucket settings
    if bucket_name:
        bucket_id = f"{project_id}/{bucket_name}"
        bucket_settings = get_settings("bucket", bucket_id)
        if bucket_settings:
            config = deep_merge(config, bucket_settings)
            for key in flatten_keys(bucket_settings):
                sources[key] = "bucket"

    # Layer 3: Table settings
    if table_name and bucket_name:
        table_id = f"{project_id}/{bucket_name}/{table_name}"
        table_settings = get_settings("table", table_id)
        if table_settings:
            config = deep_merge(config, table_settings)
            for key in flatten_keys(table_settings):
                sources[key] = "table"

    return config, sources

def flatten_keys(d: dict, prefix: str = "") -> list[str]:
    """Flatten nested dict keys: {'a': {'b': 1}} -> ['a.b']"""
    keys = []
    for k, v in d.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            keys.extend(flatten_keys(v, full_key))
        else:
            keys.append(full_key)
    return keys
```

### Priklad pouziti

```bash
# 1. Projekt: zapnout TRUNCATE snapshots pro cely projekt
PUT /projects/123/settings/snapshots
{"auto_snapshot_triggers": {"truncate_table": true}}

# 2. Bucket "logs": vypnout snapshots uplne (logy nepotrebujeme zalohovat)
PUT /projects/123/buckets/in_c_logs/settings/snapshots
{"enabled": false}

# 3. Tabulka "orders": delsi retention (dulezita data)
PUT /projects/123/buckets/in_c_sales/tables/orders/settings/snapshots
{"retention": {"manual_days": 365, "auto_days": 30}}

# 4. Zjistit efektivni konfiguraci pro tabulku orders
GET /projects/123/buckets/in_c_sales/tables/orders/settings/snapshots
# Response:
# {
#   "effective_config": {
#     "auto_snapshot_triggers": {
#       "drop_table": true,         <- system
#       "truncate_table": true,     <- project
#       "delete_all_rows": false,   <- system
#       "drop_column": false        <- system
#     },
#     "retention": {
#       "manual_days": 365,         <- table (orders)
#       "auto_days": 30             <- table (orders)
#     },
#     "enabled": true               <- system
#   },
#   ...
# }

# 5. Zjistit konfiguraci pro bucket logs (snapshots vypnute)
GET /projects/123/buckets/in_c_logs/settings/snapshots
# Response:
# {
#   "effective_config": {
#     ...
#     "enabled": false              <- bucket (logs)
#   },
#   ...
# }
```

---

## Snapshot API Endpoints

### Vytvoreni a sprava snapshotu

```
POST   /projects/{id}/snapshots                    # Create snapshot
GET    /projects/{id}/snapshots                    # List snapshots
GET    /projects/{id}/snapshots/{snap_id}          # Get snapshot detail
DELETE /projects/{id}/snapshots/{snap_id}          # Delete snapshot
POST   /projects/{id}/snapshots/{snap_id}/restore  # Restore from snapshot
```

### POST /projects/{id}/snapshots

```json
// Request
{
  "bucket": "in_c_sales",
  "table": "orders",
  "description": "Before major update"
}

// Response
{
  "snapshot_id": "snap_orders_20241215_143022",
  "bucket_name": "in_c_sales",
  "table_name": "orders",
  "snapshot_type": "manual",
  "row_count": 50000,
  "size_bytes": 10485760,
  "created_at": "2024-12-15T14:30:22Z",
  "expires_at": "2025-03-15T14:30:22Z"
}
```

### GET /projects/{id}/snapshots

Query params: `?bucket=in_c_sales&table=orders&type=manual&limit=10&offset=0`

```json
// Response
{
  "snapshots": [
    {
      "snapshot_id": "snap_orders_20241215_143022",
      "bucket_name": "in_c_sales",
      "table_name": "orders",
      "snapshot_type": "manual",
      "row_count": 50000,
      "size_bytes": 10485760,
      "created_at": "2024-12-15T14:30:22Z",
      "expires_at": "2025-03-15T14:30:22Z",
      "description": "Before major update"
    }
  ],
  "total": 15,
  "limit": 10,
  "offset": 0
}
```

### POST /projects/{id}/snapshots/{snap_id}/restore

```json
// Request
{
  "target_bucket": "in_c_sales",
  "target_table": "orders_restored"
}

// Response
{
  "restored_to": {
    "bucket": "in_c_sales",
    "table": "orders_restored"
  },
  "row_count": 50000
}
```

---

## Implementace

### Vytvoreni snapshotu

```python
async def create_snapshot(
    project_id: str,
    bucket: str,
    table: str,
    snapshot_type: str = "manual",
    description: str | None = None
) -> dict:
    """Create a snapshot of a table."""

    # Get effective config to determine retention
    config, _ = resolve_snapshot_config(project_id, bucket, table)

    if not config["enabled"]:
        raise SnapshotsDisabledError(f"Snapshots disabled for {bucket}.{table}")

    # Generate snapshot ID
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_id = f"snap_{table}_{timestamp}"

    # Paths
    table_path = get_table_path(project_id, bucket, table)
    snapshot_dir = Path(f"/data/snapshots/{project_id}/{snapshot_id}")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = snapshot_dir / "data.parquet"

    # Export to Parquet
    conn = duckdb.connect(str(table_path), read_only=True)
    try:
        conn.execute(f"""
            COPY main.data TO '{parquet_path}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Get stats
        row_count = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()[0]
        schema = get_table_schema(conn)
    finally:
        conn.close()

    size_bytes = parquet_path.stat().st_size

    # Calculate expiration
    retention_days = (
        config["retention"]["manual_days"]
        if snapshot_type == "manual"
        else config["retention"]["auto_days"]
    )
    expires_at = datetime.now() + timedelta(days=retention_days)

    # Save metadata JSON (redundant copy for recovery)
    metadata = {
        "snapshot_id": snapshot_id,
        "project_id": project_id,
        "bucket_name": bucket,
        "table_name": table,
        "snapshot_type": snapshot_type,
        "created_at": datetime.now().isoformat(),
        "expires_at": expires_at.isoformat(),
        "description": description,
        "row_count": row_count,
        "size_bytes": size_bytes,
        "schema": schema
    }

    with open(snapshot_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    # Register in metadata.duckdb
    metadata_db.register_snapshot(metadata)

    return metadata
```

### Automaticky snapshot pred destruktivni operaci

```python
async def drop_table_with_snapshot(
    project_id: str,
    bucket: str,
    table: str
) -> None:
    """Drop table with automatic snapshot if configured."""

    # Check if auto-snapshot is enabled for this operation
    config, _ = resolve_snapshot_config(project_id, bucket, table)

    if config["enabled"] and config["auto_snapshot_triggers"]["drop_table"]:
        await create_snapshot(
            project_id=project_id,
            bucket=bucket,
            table=table,
            snapshot_type="auto_predrop",
            description=f"Auto-backup before DROP TABLE {bucket}.{table}"
        )

    # Proceed with drop
    await drop_table(project_id, bucket, table)
```

### Obnova ze snapshotu

```python
async def restore_from_snapshot(
    project_id: str,
    snapshot_id: str,
    target_bucket: str | None = None,
    target_table: str | None = None
) -> dict:
    """Restore a table from snapshot."""

    # Get snapshot metadata
    snapshot = metadata_db.get_snapshot(snapshot_id)
    if not snapshot or snapshot["project_id"] != project_id:
        raise SnapshotNotFoundError(snapshot_id)

    # Default to original location
    if target_bucket is None:
        target_bucket = snapshot["bucket_name"]
    if target_table is None:
        target_table = snapshot["table_name"]

    parquet_path = f"/data/snapshots/{project_id}/{snapshot_id}/data.parquet"
    target_path = get_table_path(project_id, target_bucket, target_table)

    # Create table from Parquet
    conn = duckdb.connect(str(target_path))
    try:
        conn.execute(f"""
            CREATE OR REPLACE TABLE main.data AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        row_count = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()[0]
    finally:
        conn.close()

    # Register table if new
    if not metadata_db.table_exists(project_id, target_bucket, target_table):
        metadata_db.register_table(project_id, target_bucket, target_table)

    return {
        "restored_to": {"bucket": target_bucket, "table": target_table},
        "row_count": row_count
    }
```

### Retention cleanup job

```python
async def cleanup_expired_snapshots() -> int:
    """Run periodically to delete expired snapshots. Returns count deleted."""

    expired = metadata_db.query("""
        SELECT id, project_id, parquet_path
        FROM snapshots
        WHERE expires_at < now()
    """)

    deleted = 0
    for snapshot in expired:
        snapshot_dir = Path(f"/data/snapshots/{snapshot['project_id']}/{snapshot['id']}")

        # Delete files
        if snapshot_dir.exists():
            shutil.rmtree(snapshot_dir)

        # Delete from registry
        metadata_db.execute("DELETE FROM snapshots WHERE id = ?", snapshot["id"])
        deleted += 1

    return deleted
```

---

## Dusledky

### Pozitivni

**Format (Parquet):**
- Efektivni vyuziti uloziste (ZSTD komprese 75-95%)
- Rychle vytvoreni snapshotu
- Snadna obnova jednotlivych tabulek
- Nezavisle na DuckDB souboru
- Moznost archivace do cold storage

**Konfigurace (hierarchie):**
- Flexibilni nastaveni na libovolne urovni
- Rozumne defaulty (pouze DROP TABLE)
- Moznost vypnout snapshoty pro nepodstatna data (logs, temp)
- Delsi retention pro kriticka data

### Negativni

- Externi zavislost (filesystem pro snapshoty)
- Potreba spravovat snapshot lifecycle (cleanup job)
- Slozitejsi logika resolvovani konfigurace
- Vice API endpointu pro spravu

---

## Reference

- ADR-009: 1 DuckDB soubor per tabulka
- ADR-008: Centralni metadata databaze
- [DuckDB Parquet Export](https://duckdb.org/docs/data/parquet/overview)
- [ZSTD Compression](https://duckdb.org/docs/data/parquet/overview#compression)
