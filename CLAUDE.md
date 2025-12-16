# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goal

Build an **on-premise Keboola with DuckDB backend** - a lightweight, self-contained version of Keboola Storage without cloud dependencies (no Snowflake, no S3).

## Architecture (ADR-009)

**1 DuckDB file per table** - validated by Codex GPT-5 (4096 ATTACH test OK)

```
/data/duckdb/
├── project_123/                    # Project = directory
│   ├── in_c_sales/                 # Bucket = directory
│   │   ├── orders.duckdb           # Table = file
│   │   └── customers.duckdb
│   └── out_c_reports/
│       └── summary.duckdb
├── project_123_branch_456/         # Dev branch = directory copy
└── _staging/                       # Atomic operations staging
```

**Benefits:**
- Parallel writes to different tables (no project-level serialization)
- Natural CoW for dev branches (copy directory)
- Simplified Write Queue (per-table lock, not per-project queue)
- Industry standard (Delta Lake, Iceberg, MotherDuck use same pattern)

## Repository Structure

```
docs/                         # Project documentation
  ├── duckdb-driver-plan.md  # MAIN PLAN - implementation phases, decisions, specs
  ├── local-connection.md    # Local Connection setup guide
  ├── bigquery-driver-research.md  # BigQuery driver analysis
  └── adr/                   # Architecture Decision Records (001-009)

duckdb-api-service/          # Python FastAPI service for DuckDB operations
  ├── src/
  │   ├── main.py            # FastAPI app
  │   ├── config.py          # Settings (pydantic-settings)
  │   ├── database.py        # MetadataDB + ProjectDBManager
  │   ├── auth.py            # API key generation/verification
  │   ├── dependencies.py    # FastAPI auth dependencies
  │   ├── metrics.py         # Prometheus metrics definitions
  │   ├── middleware/        # HTTP middleware
  │   │   ├── idempotency.py # X-Idempotency-Key handling
  │   │   └── metrics.py     # Request instrumentation
  │   └── routers/           # API endpoints
  │       ├── backend.py     # Health, init, remove
  │       ├── projects.py    # Project CRUD
  │       ├── buckets.py     # Bucket CRUD
  │       ├── bucket_sharing.py  # Share, link, readonly
  │       ├── tables.py      # Table CRUD + preview
  │       └── metrics.py     # Prometheus /metrics endpoint
  └── tests/                 # pytest tests (180 tests)

connection/                   # Keboola Connection (git submodule/clone)
```

## Current Status (2024-12-16)

**Strategy: Python API first, PHP Driver last**

| Component | Status | Tests |
|-----------|--------|-------|
| Project CRUD | DONE | 20 |
| Bucket CRUD + Sharing | DONE | 40 |
| Table CRUD + Preview | DONE | 34 |
| ADR-009 Refactor | DONE | - |
| Write Queue (mutex) | DONE | 14 |
| Auth Middleware | DONE | 25 |
| Idempotency Middleware | DONE | 21 |
| **Prometheus /metrics** | **DONE** | 15 |
| Table Schema Ops | TODO | - |
| Import/Export | TODO | - |
| Files API | TODO | - |
| Snapshots | TODO | - |
| Dev Branches | TODO | - |
| Schema Migrations | TODO | - |
| PHP Driver | TODO (last) | - |

**Total: 180 tests PASS**

**Next implementation order:**
1. ~~REFACTOR to ADR-009 (per-table files)~~ - DONE
2. ~~Write Queue (simplified with ADR-009)~~ - DONE
3. ~~Auth middleware (hierarchical API keys)~~ - DONE
4. ~~Idempotency middleware (X-Idempotency-Key)~~ - DONE
5. ~~Prometheus /metrics endpoint~~ - DONE
6. **Table Schema Operations** - NEXT
7. Files API
8. Import/Export
9. Snapshots
10. Dev Branches (simplified with ADR-009)
11. PHP Driver

## Key Decisions (APPROVED)

All decisions documented in `docs/duckdb-driver-plan.md` section "PREHLED ROZHODNUTI".

| Area | Decision | Value |
|------|----------|-------|
| **Architecture** | **File organization** | **1 DuckDB file per table (ADR-009)** |
| Write Queue | Simplified | Per-table lock (not per-project queue) |
| Write Queue | Idempotency | X-Idempotency-Key header (TTL 10 min) |
| Import/Export | Staging | `_staging/{uuid}.duckdb` |
| Import/Export | Dedup | INSERT ON CONFLICT |
| Import/Export | Incremental | Full MERGE |
| Files API | Upload | Multipart POST |
| Files API | Staging TTL | 24 hours |
| Files API | Max size | 10GB |
| Snapshots | Manual retention | 90 days |
| Snapshots | Auto retention | 7 days |
| Snapshots | Auto triggers | Per-projekt konfigurovatelne, default DROP TABLE |
| Security | Auth model | Hierarchical API keys |
| Observability | Metrics | Prometheus from start |
| Schema migrations | Strategy | Verzovani v DB + migrace pri startu |
| Dev Branches | Strategy | Directory copy (simplified by ADR-009) |

## Authentication Model (IMPLEMENTED)

```
ADMIN_API_KEY (ENV variable)
├── Endpoints: POST /projects, GET /projects, /backend/*
└── Has access to ALL projects

PROJECT_ADMIN_API_KEY (returned on POST /projects - SAVE IT!)
├── Format: proj_{project_id}_admin_{random_hex_32}
├── Storage: SHA256 hash in metadata.duckdb
└── Endpoints: Everything in /projects/{id}/*

Usage:
  curl -H "Authorization: Bearer $ADMIN_API_KEY" ...
  curl -H "Authorization: Bearer $PROJECT_KEY" ...
```

## Auto-Snapshot Triggers (Per-projekt konfigurovatelne)

**Default (konzervativni):** Snapshot pouze pred `DROP TABLE`

**Volitelne (lze zapnout per-projekt):**
- TRUNCATE TABLE
- DELETE FROM (with or without WHERE)
- ALTER TABLE DROP COLUMN

## Accepted Risks for MVP

| Risk | Mitigation |
|------|------------|
| Cross-DB konzistence | Stats = cache, prepocet on-demand |
| Write Queue volatilita | Idempotency middleware |
| Single FastAPI instance | HA post-MVP |
| Jednoduchy auth model | Enterprise rozsireni post-MVP |
| Bucket sharing bez ACL | App-layer enforcement |
| Dev branches full copy | CoW (ADR-007) post-MVP |
| Bez DR/Backup API | Dokumentace, post-MVP |
| Bez encryption at rest | Filesystem-level (LUKS) |

## DuckDB API Service Quick Reference

```bash
cd duckdb-api-service
source .venv/bin/activate
pytest tests/ -v           # Run tests
python -m src.main         # Run server
docker compose up --build  # Docker
```

### Implemented Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics (no auth) |
| `/backend/init` | POST | Initialize storage |
| `/backend/remove` | POST | Remove backend |
| `/projects` | GET/POST | List/Create projects |
| `/projects/{id}` | GET/PUT/DELETE | Project CRUD |
| `/projects/{id}/stats` | GET | Live statistics |
| `/projects/{id}/buckets` | GET/POST | List/Create buckets |
| `/projects/{id}/buckets/{name}` | GET/DELETE | Bucket CRUD |
| `/projects/{id}/buckets/{name}/share` | POST/DELETE | Share bucket |
| `/projects/{id}/buckets/{name}/link` | POST/DELETE | Link bucket |
| `/projects/{id}/buckets/{name}/grant-readonly` | POST/DELETE | Readonly |
| `/projects/{id}/buckets/{bucket}/tables` | GET/POST | List/Create tables |
| `/projects/{id}/buckets/{bucket}/tables/{table}` | GET/DELETE | Table CRUD |
| `/projects/{id}/buckets/{bucket}/tables/{table}/preview` | GET | Preview data |

### TODO Endpoints (see duckdb-driver-plan.md for specs)

- `POST /projects/{id}/query` - Write Queue
- Table schema: columns, primary-key, rows
- Import/Export: import/file, import/table, export
- Files: prepare, upload, register, download, delete
- Snapshots: create, list, get, restore, delete
- Dev Branches: create, delete, merge

## Development Notes

### DuckDB + Python Tips (ADR-009)

1. **Per-table files** - each table is its own `.duckdb` file
2. **ATTACH for cross-table queries** - workspace sessions ATTACH needed tables
3. **Parallel writes OK** - different tables can be written simultaneously
4. **File descriptors** - set `ulimit -n 65536` for large projects
5. **DuckDB requires `pytz`** for TIMESTAMPTZ columns
6. **JSON columns** returned as strings - parse with `json.loads()`
7. **Use `@property` for paths** in singletons (for test overrides)

### Testing with pytest

- Use `monkeypatch.setattr(settings, "path", new_path)`
- Fixtures in `conftest.py` create temp directories
- Each test gets isolated metadata.duckdb

### Phase Completion Checklist

When completing an implementation phase, ALWAYS:

1. **Write tests** - both functional (API works) and structural (architecture verified)
2. **Run all tests** - ensure 100% pass rate
3. **Update docs/duckdb-driver-plan.md**:
   - Change status to DONE
   - Update test counts
   - Add changelog entry with new version
4. **Update CLAUDE.md** - refresh test counts and status table

This ensures each phase has documented progress and test coverage for future sessions.

## Documentation

- **Main plan**: `docs/duckdb-driver-plan.md` - phases, decisions, detailed specs
- **ADRs**: `docs/adr/001-009` - architecture decisions
- **Key ADR**: `docs/adr/009-duckdb-file-per-table.md` - current architecture
- **Research**: `docs/duckdb-technical-research.md`, `docs/bigquery-driver-research.md`

## Local Connection

```bash
cd connection
docker compose up apache supervisor
# URL: https://localhost:8700/admin
# Login: dev@keboola.com / devdevdev
```
