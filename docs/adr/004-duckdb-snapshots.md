# ADR-004: Snapshoty jako Parquet soubory

## Status

Accepted

## Datum

2024-12-11

## Kontext

Keboola Storage poskytuje snapshoty tabulek pro:
- Point-in-time recovery
- Ochrana pred omylnym smazanim dat
- Audit trail

DuckDB **nema nativni time-travel** jako Snowflake (`SELECT * FROM table AT TIMESTAMP`). Potrebujeme implementovat vlastni snapshot mechanismus.

### Moznosti

1. **Kopirovani do jine tabulky** - snapshot jako tabulka `_snapshot_xxx`
2. **Export do Parquet** - snapshot jako externi soubor
3. **Kopirovani celeho DuckDB souboru** - snapshot na urovni databaze

## Rozhodnuti

**Zvolili jsme export do Parquet souboru.**

### Struktura snapshotu

```
/data/snapshots/
├── project_123/
│   ├── snap_20241211_143022/
│   │   ├── metadata.json
│   │   ├── in_c_customers.customers.parquet
│   │   └── in_c_orders.orders.parquet
│   ├── snap_manual_backup/
│   │   ├── metadata.json
│   │   └── in_c_important.data.parquet
│   └── auto_predrop_20241211_150000/
│       ├── metadata.json
│       └── deleted_table.parquet
```

### Metadata format

```json
{
  "snapshot_id": "snap_20241211_143022",
  "project_id": "123",
  "branch": "main",
  "created_at": "2024-12-11T14:30:22Z",
  "created_by": "user@example.com",
  "type": "manual|auto|pre_drop",
  "description": "Before major refactoring",
  "tables": [
    {
      "bucket": "in_c_customers",
      "table": "customers",
      "file": "in_c_customers.customers.parquet",
      "row_count": 150000,
      "size_bytes": 45000000,
      "schema": {
        "columns": [
          {"name": "id", "type": "BIGINT"},
          {"name": "name", "type": "VARCHAR"},
          {"name": "email", "type": "VARCHAR"}
        ],
        "primary_key": ["id"]
      }
    }
  ]
}
```

## Duvody

### Proc ne snapshot tabulky v DuckDB?

1. **Velikost databaze**: Snapshoty by zvetsovaly hlavni DB soubor
2. **Namespace kolize**: Potreba komplexniho pojmenovani `_snap_xxx_bucket_table`
3. **Obtizna sprava**: Mazani snapshotu = DELETE operace

### Proc ne kopirovani celeho DB souboru?

1. **Neefektivni**: Kopirovat 500GB kvuli 1 tabulce
2. **Pomale**: Dlouha doba vytvoreni snapshotu
3. **Nakladne na uloziste**: Plytva mistem

### Proc Parquet?

1. **Komprese**: 75-95% kompresni pomer (ZSTD)
2. **Rychle**: DuckDB exportuje Parquet velmi efektivne
3. **Portabilni**: Parquet lze cist kdekoliv (Python, Spark, ...)
4. **Selektivni**: Snapshot jen konkretni tabulky
5. **Snadna obnova**: `CREATE TABLE AS SELECT * FROM read_parquet(...)`

## Dusledky

### Pozitivni

- Efektivni vyuziti uloziste (komprese)
- Rychle vytvoreni snapshotu
- Snadna obnova jednotlivych tabulek
- Nezavisle na DuckDB souboru (muze prezit i ztratu DB)
- Moznost archivace do cold storage

### Negativni

- Externi zavislost (filesystem pro snapshoty)
- Potreba spravovat snapshot lifecycle
- Zadna nativni integrace s DuckDB (rucni management)

## Implementace

### Vytvoreni snapshotu

```python
def create_snapshot(project_id: str, tables: list[tuple[str, str]],
                   snapshot_id: str, description: str = None):
    conn = duckdb.connect(f'project_{project_id}_main.duckdb')
    snapshot_dir = f'/data/snapshots/{project_id}/{snapshot_id}'
    os.makedirs(snapshot_dir, exist_ok=True)

    metadata = {
        'snapshot_id': snapshot_id,
        'project_id': project_id,
        'created_at': datetime.now().isoformat(),
        'description': description,
        'tables': []
    }

    for bucket, table in tables:
        filename = f'{bucket}.{table}.parquet'
        conn.execute(f"""
            COPY {bucket}.{table} TO '{snapshot_dir}/{filename}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Get table info
        row_count = conn.execute(f"SELECT COUNT(*) FROM {bucket}.{table}").fetchone()[0]
        schema = get_table_schema(conn, bucket, table)

        metadata['tables'].append({
            'bucket': bucket,
            'table': table,
            'file': filename,
            'row_count': row_count,
            'schema': schema
        })

    with open(f'{snapshot_dir}/metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)

    return metadata
```

### Obnova ze snapshotu

```python
def restore_from_snapshot(project_id: str, snapshot_id: str,
                         source_table: tuple[str, str],
                         target_table: tuple[str, str] = None):
    if target_table is None:
        target_table = source_table

    snapshot_dir = f'/data/snapshots/{project_id}/{snapshot_id}'

    with open(f'{snapshot_dir}/metadata.json') as f:
        metadata = json.load(f)

    # Find table in snapshot
    table_meta = next(
        t for t in metadata['tables']
        if t['bucket'] == source_table[0] and t['table'] == source_table[1]
    )

    conn = duckdb.connect(f'project_{project_id}_main.duckdb')

    # Restore
    conn.execute(f"""
        CREATE OR REPLACE TABLE {target_table[0]}.{target_table[1]} AS
        SELECT * FROM read_parquet('{snapshot_dir}/{table_meta['file']}')
    """)
```

### Automaticky snapshot pred destruktivni operaci

```python
def safe_drop_table(project_id: str, bucket: str, table: str):
    snapshot_id = f"auto_predrop_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    create_snapshot(
        project_id,
        [(bucket, table)],
        snapshot_id,
        f"Auto-backup before DROP TABLE {bucket}.{table}"
    )

    conn = duckdb.connect(f'project_{project_id}_main.duckdb')
    conn.execute(f"DROP TABLE {bucket}.{table}")
```

## Reference

- [DuckDB Parquet Export](https://duckdb.org/docs/data/parquet/overview)
- [ZSTD Compression](https://duckdb.org/docs/data/parquet/overview#compression)
