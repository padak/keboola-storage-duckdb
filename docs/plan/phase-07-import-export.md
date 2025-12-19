# Phase 7: Import/Export - DONE

## Status
- **Completed:** 2024-12-16
- **Tests:** 17

## Implemented
- 3-stage import pipeline (STAGING -> TRANSFORM -> CLEANUP)
- POST /tables/{table}/import/file (COPY FROM CSV/Parquet)
- POST /tables/{table}/export (COPY TO CSV/Parquet)
- CSV + Parquet support
- Deduplication with primary keys (INSERT ON CONFLICT)
- Incremental import (merge/upsert mode)
- Column filtering, WHERE filter, LIMIT for export
- Compression support (gzip for CSV, gzip/zstd/snappy for Parquet)

## Import API

### Endpoint
```
POST /projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/import/file
```

### Request Body
```json
{
  "file_id": "uuid-from-files-api",
  "format": "csv",
  "csv_options": {
    "delimiter": ",",
    "header": true,
    "quote": "\"",
    "escape": "\"",
    "null_string": ""
  },
  "import_options": {
    "incremental": false,
    "dedup_mode": "update_duplicates",
    "columns": null
  }
}
```

### Import Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `incremental` | boolean | `false` | `false` = TRUNCATE table before import, `true` = merge/append to existing data |
| `dedup_mode` | string | `"update_duplicates"` | How to handle duplicate primary keys |
| `columns` | array | `null` | Specific columns to import (`null` = all columns) |

### Dedup Modes

| Mode | Behavior |
|------|----------|
| `update_duplicates` | **UPSERT** - Update existing rows with matching PK (INSERT ON CONFLICT DO UPDATE) |
| `insert_duplicates` | **APPEND** - Insert all rows, allow duplicates (no dedup) |
| `fail_on_duplicates` | **STRICT** - Fail import if any duplicate PK found |

### Import Behavior Matrix

| incremental | dedup_mode | Result |
|-------------|------------|--------|
| `false` | any | TRUNCATE + INSERT (full replace) |
| `true` | `update_duplicates` | UPSERT - update existing, insert new |
| `true` | `insert_duplicates` | APPEND - insert all rows (may create duplicates) |
| `true` | `fail_on_duplicates` | FAIL if duplicate PK exists |

### CSV Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `delimiter` | string | `","` | Column separator |
| `header` | boolean | `true` | First row is header |
| `quote` | string | `"\""` | Quote character |
| `escape` | string | `"\""` | Escape character |
| `null_string` | string | `""` | String representing NULL |

## Export API

### Endpoint
```
POST /projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/export
```

### Request Body
```json
{
  "format": "csv",
  "compression": "gzip",
  "columns": ["id", "name", "price"],
  "where_filter": "price > 100",
  "limit": 1000
}
```

### Export Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `format` | string | `"csv"` | Output format: `csv` or `parquet` |
| `compression` | string | `null` | Compression: `gzip` for CSV, `gzip`/`zstd`/`snappy` for Parquet |
| `columns` | array | `null` | Specific columns to export (`null` = all) |
| `where_filter` | string | `null` | SQL WHERE clause (without WHERE keyword) |
| `limit` | integer | `null` | Max rows to export |

## Pipeline

```
Stage 1: STAGING
  - Create staging table (temp schema)
  - COPY FROM file to staging

Stage 2: TRANSFORM
  - Deduplicate by PK (if exists)
  - INSERT INTO target FROM staging

Stage 3: CLEANUP
  - DROP staging table
  - Return statistics
```

## Key Decisions
- Staging location: `_staging/{uuid}.duckdb`
- Dedup strategy: INSERT ON CONFLICT (DuckDB native)
- Incremental mode: Full MERGE (INSERT/UPDATE/DELETE)
- File source: File ID from Files API

## Reference
- Code: `routers/table_import.py`
- Models: `models/responses.py` (ImportOptions, ExportRequest)
- Tests: `tests/test_import_export.py`
