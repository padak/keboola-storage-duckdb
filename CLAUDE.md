# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goal

Build an **on-premise Keboola with DuckDB backend** - a lightweight, self-contained version of Keboola Storage without cloud dependencies (no Snowflake, no S3).

## Repository Structure

```
docs/                         # Project documentation
  ├── duckdb-driver-plan.md  # MAIN PLAN - implementation phases, decisions, specs
  ├── local-connection.md    # Local Connection setup guide
  ├── bigquery-driver-research.md  # BigQuery driver analysis
  └── adr/                   # Architecture Decision Records (001-008)

duckdb-api-service/          # Python FastAPI service for DuckDB operations
  ├── src/
  │   ├── main.py            # FastAPI app
  │   ├── config.py          # Settings (pydantic-settings)
  │   ├── database.py        # MetadataDB + ProjectDBManager
  │   └── routers/           # API endpoints
  │       ├── backend.py     # Health, init, remove
  │       ├── projects.py    # Project CRUD
  │       ├── buckets.py     # Bucket CRUD
  │       ├── bucket_sharing.py  # Share, link, readonly
  │       └── tables.py      # Table CRUD + preview
  └── tests/                 # pytest tests (98 tests)

connection/                   # Keboola Connection (git submodule/clone)
```

## Current Status (2024-12-16)

**Strategy: Python API first, PHP Driver last**

| Component | Status | Tests |
|-----------|--------|-------|
| Project CRUD | DONE | 32 |
| Bucket CRUD + Sharing | DONE | 37 |
| Table CRUD + Preview | DONE | 29 |
| Auth Middleware | TODO | - |
| Idempotency Middleware | TODO | - |
| Prometheus /metrics | TODO | - |
| Write Queue | TODO | - |
| Table Schema Ops | TODO | - |
| Import/Export | TODO | - |
| Files API | TODO | - |
| Snapshots | TODO | - |
| Dev Branches | TODO | - |
| Schema Migrations | TODO | - |
| PHP Driver | TODO (last) | - |

**Next implementation order:**
1. Auth middleware (hierarchicky API keys)
2. Idempotency middleware (X-Idempotency-Key)
3. Prometheus /metrics endpoint
4. Write Queue
5. Table Schema Operations
6. Files API
7. Import/Export
8. Snapshots
9. Dev Branches (full copy, CoW later)
10. PHP Driver

## Key Decisions (APPROVED)

All decisions documented in `docs/duckdb-driver-plan.md` section "PREHLED ROZHODNUTI".

| Area | Decision | Value |
|------|----------|-------|
| Write Queue | Durability | In-memory (klient ceka, Keboola retry) |
| Write Queue | Max size | 1000 |
| Write Queue | Priority | Normal + High |
| Write Queue | Idempotency | X-Idempotency-Key header (TTL 5-10 min) |
| Import/Export | Staging | Temp schema `_staging_{uuid}` |
| Import/Export | Dedup | INSERT ON CONFLICT |
| Import/Export | Incremental | Full MERGE |
| Files API | Upload | Multipart POST |
| Files API | Staging TTL | 24 hours |
| Files API | Max size | 10GB |
| Snapshots | Manual retention | 90 days |
| Snapshots | Auto retention | 7 days |
| Snapshots | Auto triggers | **Per-projekt konfigurovatelne**, default pouze DROP TABLE |
| Security | Auth model | Hierarchical API keys |
| Observability | Metrics | Prometheus from start |
| Schema migrations | Strategy | Verzovani v DB + migrace pri startu |
| Dev Branches | Strategy | Full copy pro MVP, CoW (ADR-007) post-MVP |

## Authentication Model (APPROVED)

```
ADMIN_API_KEY (ENV)
└── Can: POST /projects

PROJECT_ADMIN_API_KEY (returned on project creation)
└── Can: Everything in project
└── Format: proj_{project_id}_admin_{random}

PROJECT_API_KEY (future extension)
└── Can: Everything in project (full access for now)
└── Foundation for future RBAC
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

- `/metrics` - Prometheus metrics
- `POST /projects/{id}/query` - Write Queue
- Table schema: columns, primary-key, rows
- Import/Export: import/file, import/table, export
- Files: prepare, upload, register, download, delete
- Snapshots: create, list, get, restore, delete
- Dev Branches: create, delete, merge

## Development Notes

### DuckDB + Python Tips

1. **DuckDB requires `pytz`** for TIMESTAMPTZ columns
2. **JSON columns** returned as strings - parse with `json.loads()`
3. **ATTACH is session-specific** - do all ops in one connection
4. **Use `@property` for paths** in singletons (for test overrides)
5. **Single-writer limitation** - use write queue for serialization

### Testing with pytest

- Use `monkeypatch.setattr(settings, "path", new_path)`
- Fixtures in `conftest.py` create temp directories
- Each test gets isolated metadata.duckdb

## Documentation

- **Main plan**: `docs/duckdb-driver-plan.md` - phases, decisions, detailed specs
- **ADRs**: `docs/adr/001-008` - architecture decisions
- **Research**: `docs/duckdb-technical-research.md`, `docs/bigquery-driver-research.md`

## Local Connection

```bash
cd connection
docker compose up apache supervisor
# URL: https://localhost:8700/admin
# Login: dev@keboola.com / devdevdev
```
