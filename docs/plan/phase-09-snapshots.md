# Phase 9: Snapshots + Settings - DONE

## Status
- **Completed:** 2024-12-16
- **Tests:** 34

## Implemented
- Snapshot registry in metadata.duckdb (`snapshots` table)
- Snapshot settings registry (`snapshot_settings` table)
- Hierarchical configuration (System -> Project -> Bucket -> Table)
- POST /snapshots (Parquet export with ZSTD)
- GET /snapshots (list with filters)
- GET /snapshots/{id} (including schema)
- DELETE /snapshots/{id}
- POST /snapshots/{id}/restore
- Auto-snapshot before DROP TABLE (default enabled)
- Auto-snapshot before DROP COLUMN (configurable)
- Configurable retention policy (per-project/bucket/table)
- GET/PUT/DELETE /settings/snapshots at all levels

## Key Decisions
- Snapshot ID format: `snap_{table}_{timestamp}`
- Manual retention: 90 days
- Auto retention: 7 days
- Auto-snapshot triggers: Per-project configurable, default only DROP TABLE

## Hierarchical Config

```python
# Default policy (conservative)
default_snapshot_policy = {
    "drop_table": True,       # Always snapshot before DROP TABLE
    "truncate_table": False,  # Optional
    "delete_rows": False,     # Optional
    "drop_column": False,     # Optional
}
```

Settings cascade: System -> Project -> Bucket -> Table

## Directory Structure

```
/data/snapshots/
└── project_123/
    ├── snap_orders_20241215_143022/
    │   ├── metadata.json
    │   └── data.parquet
    └── snap_customers_20241214_091500/
        ├── metadata.json
        └── data.parquet
```

## Reference
- Code: `routers/snapshots.py`
- Tests: `tests/test_snapshots.py`
- ADR: `docs/adr/004-duckdb-snapshots.md`
