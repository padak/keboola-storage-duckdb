# Phase 4: Tables + Preview - DONE

## Status
- **Completed:** 2024-12-16
- **Tests:** 34

## Implemented
- POST /tables (CREATE TABLE in .duckdb file)
- DELETE /tables/{schema}/{table}
- GET /tables/{schema}/{table} (ObjectInfo)
- GET /tables (list)
- GET /tables/{schema}/{table}/preview (LIMIT)
- Primary key support (enforced, not just metadata)

## Key Decisions
- Table = .duckdb file (ADR-009)
- Primary keys are real constraints (unlike BigQuery)
- Preview respects PK ordering

## Reference
- Code: `routers/tables.py`, `database.py`
- Tests: `tests/test_tables.py`
