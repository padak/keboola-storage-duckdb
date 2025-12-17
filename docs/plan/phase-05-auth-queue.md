# Phase 5: Auth + Write Queue - DONE

## Status
- **Completed:** 2024-12-16
- **Tests:** 46 (14 lock + 24 auth + 8 idempotency)

## Implemented

### TableLockManager
- Per-table mutex (not per-project queue)
- Simple asyncio.Lock per .duckdb file
- Parallel writes to different tables OK

### Auth Middleware
- ADMIN_API_KEY in ENV for creating projects
- PROJECT_ADMIN_API_KEY returned on POST /projects
- SHA256 hash of keys stored in metadata.duckdb
- FastAPI dependencies: `require_admin`, `require_project_access`
- All endpoints protected (except /health, /metrics)

### Idempotency Middleware
- X-Idempotency-Key header
- TTL 10 minutes
- Background cleanup task

## Key Decisions
- In-memory queue (not persistent) - client waits, retry on Keboola side
- Per-table locking simplified by ADR-009
- Hierarchical API key model (ADMIN > PROJECT)

## Reference
- Code: `auth.py`, `dependencies.py`, `middleware/idempotency.py`
- Tests: `tests/test_auth.py`, `tests/test_table_lock.py`, `tests/test_idempotency.py`
