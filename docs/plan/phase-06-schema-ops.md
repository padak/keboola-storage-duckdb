# Phase 6: Table Schema Operations - DONE

## Status
- **Completed:** 2024-12-16
- **Tests:** 33

## Implemented
- POST /tables/{table}/columns (AddColumn)
- DELETE /tables/{table}/columns/{name} (DropColumn)
- PUT /tables/{table}/columns/{name} (AlterColumn)
- POST /tables/{table}/primary-key (AddPrimaryKey)
- DELETE /tables/{table}/primary-key (DropPrimaryKey)
- DELETE /tables/{table}/rows (DeleteTableRows with WHERE)
- POST /tables/{table}/profile (ProfileTable - SUMMARIZE)

## Key Decisions
- DuckDB doesn't support `ALTER TABLE ADD COLUMN` with `NOT NULL`
- Columns must be added as nullable, then changed via ALTER COLUMN
- Profile uses DuckDB's SUMMARIZE for statistics

## Reference
- Code: `routers/table_schema.py`
- Tests: `tests/test_table_schema.py`
