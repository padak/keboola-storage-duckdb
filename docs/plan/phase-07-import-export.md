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

## Key Decisions
- Staging location: `_staging/{uuid}.duckdb`
- Dedup strategy: INSERT ON CONFLICT (DuckDB native)
- Incremental mode: Full MERGE (INSERT/UPDATE/DELETE)
- File source: File ID from Files API

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

## Reference
- Code: `routers/table_import.py`
- Tests: `tests/test_import_export.py`
