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
  ├── plan/                  # MAIN PLAN - modular structure
  │   ├── README.md          # Index with phase status and links
  │   ├── phase-01-backend.md ... phase-12-php-driver.md
  │   ├── decisions.md       # All approved decisions
  │   └── risks.md           # Accepted MVP risks
  ├── local-connection.md    # Local Connection setup guide
  ├── bigquery-driver-research.md  # BigQuery driver analysis
  └── adr/                   # Architecture Decision Records (001-013)

duckdb-api-service/          # Python FastAPI service for DuckDB operations
  ├── src/
  │   ├── main.py            # FastAPI app
  │   ├── config.py          # Settings (pydantic-settings)
  │   ├── database.py        # MetadataDB + ProjectDBManager
  │   ├── auth.py            # API key generation/verification
  │   ├── dependencies.py    # FastAPI auth dependencies
  │   ├── metrics.py         # Prometheus metrics definitions
  │   ├── branch_utils.py    # Branch resolution utilities (ADR-012)
  │   ├── snapshot_config.py # Hierarchical snapshot config resolver
  │   ├── middleware/        # HTTP middleware
  │   │   ├── idempotency.py # X-Idempotency-Key handling
  │   │   └── metrics.py     # Request instrumentation
  │   └── routers/           # API endpoints
  │       ├── backend.py     # Health, init, remove
  │       ├── projects.py    # Project CRUD
  │       ├── buckets.py     # Bucket CRUD
  │       ├── bucket_sharing.py  # Share, link, readonly
  │       ├── tables.py      # Table CRUD + preview
  │       ├── table_schema.py    # Column/PK operations
  │       ├── table_import.py    # Import/Export
  │       ├── files.py       # Files API (on-prem S3)
  │       ├── snapshots.py       # Snapshots CRUD + restore
  │       ├── snapshot_settings.py  # Hierarchical snapshot config
  │       ├── branches.py    # Dev branches CRUD + pull
  │       ├── workspaces.py  # Workspace management
  │       ├── pgwire_auth.py # PG Wire auth bridge (internal)
  │       └── metrics.py     # Prometheus /metrics endpoint
  └── tests/                 # pytest tests (439 tests)

connection/                   # Keboola Connection (git submodule/clone)
```

## Current Status (2024-12-19)

**Strategy: Python API first, PHP Driver last**

| Component | Status | Tests |
|-----------|--------|-------|
| Backend + Health | DONE | 12 |
| Project CRUD | DONE | 20 |
| Bucket CRUD + Sharing | DONE | 40 |
| Table CRUD + Preview | DONE | 34 |
| ADR-009 Refactor | DONE | - |
| Write Queue (TableLockManager) | DONE | 14 |
| Auth Middleware | DONE | 24 |
| Idempotency Middleware | DONE | 21 |
| Prometheus /metrics | DONE | 15 |
| Table Schema Ops | DONE | 33 |
| Files API | DONE | 20 |
| Import/Export | DONE | 17 |
| Snapshots + Settings | DONE | 34 |
| Dev Branches + Branch-First API | DONE | 26 |
| Workspaces (REST API) | DONE | 41 |
| PG Wire Server | DONE | 26 |
| E2E Tests (Phase 11c) | DONE | 62 |
| Schema Migrations | TODO | - |
| PHP Driver | TODO (last) | - |

**Total: 438 tests PASS** (including 62 comprehensive E2E tests)

**Current: Phase 12 - PHP Driver (TODO)**

All bucket/table operations now use branch-first URLs (ADR-012):
- `/projects/{id}/branches/{branch_id}/buckets/...`
- `default` = main (production project)
- `source` field in TableResponse: `"main"` or `"branch"`

**Completed implementation:**
1. ~~REFACTOR to ADR-009 (per-table files)~~ - DONE
2. ~~Write Queue (simplified with ADR-009)~~ - DONE
3. ~~Auth middleware (hierarchical API keys)~~ - DONE
4. ~~Idempotency middleware (X-Idempotency-Key)~~ - DONE
5. ~~Prometheus /metrics endpoint~~ - DONE
6. ~~Table Schema Operations~~ - DONE
7. ~~Files API~~ - DONE
8. ~~Import/Export~~ - DONE
9. ~~Snapshots + Settings~~ - DONE
10. ~~Dev Branches + Branch-First API (ADR-012)~~ - DONE
11. ~~Workspaces (REST API)~~ - DONE
12. ~~PG Wire Server (buenavista)~~ - DONE
13. ~~E2E Tests + PG Wire Polish~~ - DONE
14. **PHP Driver** - NEXT

## Key Decisions (APPROVED)

All decisions documented in `docs/plan/decisions.md`.

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
| Dev Branches | Storage | Directory copy (simplified by ADR-009) |
| **Dev Branches** | **API Design** | **Branch-First URL (`/branches/{branch_id}/`) - ADR-012** |
| Dev Branches | Default branch | `default` = main (production) |

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

## Auto-Snapshot Triggers (Hierarchical Configuration - ADR-004)

**Inheritance:** System -> Project -> Bucket -> Table (each level can override)

**System Defaults (konzervativni):**
- `enabled`: true
- `drop_table`: true (creates snapshot before DROP TABLE)
- `truncate_table`: false
- `delete_all_rows`: false
- `drop_column`: false
- `retention.manual_days`: 90
- `retention.auto_days`: 7

**Configuration API:** `GET/PUT/DELETE /settings/snapshots` at each level

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
open dashboard.html        # Metrics dashboard (auto-refresh)
```

### Metrics Dashboard

`dashboard.html` - standalone HTML dashboard for Prometheus metrics visualization:
- **Service Health**: Status, uptime, total requests, error rate (4xx/5xx)
- **Storage**: Projects/buckets/tables count, size breakdown (metadata/tables/staging/files)
- **Latency**: P50/P90/P95/P99 percentiles + average (calculated from histograms)
- **Concurrency**: Active locks, lock acquisitions, lock wait P95, idempotency cache
- **Dev Branches**: Total branches, CoW operations, CoW duration metrics
- **Workspaces & PG Wire**: Total workspaces, active sessions, auth rate, query P95
- **Charts**: Request distribution by status code and HTTP method
- **Tables**: Endpoint details with latency, table lock activity, Python GC stats

Auto-refreshes every 5s, works with API running on `localhost:8000`.

### Implemented Endpoints (Current)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics (no auth) |
| `/backend/init` | POST | Initialize storage |
| `/backend/remove` | POST | Remove backend |
| `/projects` | GET/POST | List/Create projects |
| `/projects/{id}` | GET/PUT/DELETE | Project CRUD |
| `/projects/{id}/stats` | GET | Live statistics |
| `/projects/{id}/branches` | GET/POST | List/Create branches |
| `/projects/{id}/branches/{branch_id}` | GET/DELETE | Branch CRUD |
| `/projects/{id}/files/prepare` | POST | Prepare file upload |
| `/projects/{id}/files/upload/{key}` | POST | Upload file |
| `/projects/{id}/files` | GET/POST | List/Register files |
| `/projects/{id}/files/{id}` | GET/DELETE | Get/Delete file |
| `/projects/{id}/files/{id}/download` | GET | Download file |
| `/projects/{id}/settings/snapshots` | GET/PUT/DELETE | Project snapshot config |

**Branch-First Endpoints (ADR-012)** - all bucket/table operations include `/branches/{branch_id}/`:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `.../branches/{branch}/buckets` | GET/POST | List/Create buckets |
| `.../branches/{branch}/buckets/{name}` | GET/DELETE | Bucket CRUD |
| `.../branches/{branch}/buckets/{name}/share` | POST/DELETE | Share bucket |
| `.../branches/{branch}/buckets/{name}/link` | POST/DELETE | Link bucket |
| `.../branches/{branch}/buckets/{name}/grant-readonly` | POST/DELETE | Readonly |
| `.../branches/{branch}/buckets/{b}/tables` | GET/POST | List/Create tables |
| `.../branches/{branch}/buckets/{b}/tables/{t}` | GET/DELETE | Table CRUD |
| `.../branches/{branch}/buckets/{b}/tables/{t}/preview` | GET | Preview data |
| `.../branches/{branch}/buckets/{b}/tables/{t}/columns` | POST | Add column |
| `.../branches/{branch}/buckets/{b}/tables/{t}/columns/{c}` | DELETE/PUT | Drop/Alter column |
| `.../branches/{branch}/buckets/{b}/tables/{t}/primary-key` | POST/DELETE | Add/Drop PK |
| `.../branches/{branch}/buckets/{b}/tables/{t}/rows` | DELETE | Delete rows |
| `.../branches/{branch}/buckets/{b}/tables/{t}/profile` | POST | Table profiling |
| `.../branches/{branch}/buckets/{b}/tables/{t}/import/file` | POST | Import from file |
| `.../branches/{branch}/buckets/{b}/tables/{t}/export` | POST | Export to file |
| `.../branches/{branch}/buckets/{b}/settings/snapshots` | GET/PUT/DELETE | Bucket snapshot config |
| `.../branches/{branch}/buckets/{b}/tables/{t}/settings/snapshots` | GET/PUT/DELETE | Table snapshot config |
| `.../branches/{branch}/buckets/{b}/tables/{t}/snapshots` | GET/POST | List/Create snapshots |
| `.../branches/{branch}/buckets/{b}/tables/{t}/snapshots/{id}` | GET/DELETE | Get/Delete snapshot |
| `.../branches/{branch}/buckets/{b}/tables/{t}/snapshots/{id}/restore` | POST | Restore snapshot |

Where `branch_id`:
- `default` = main (production project)
- `{uuid}` = dev branch

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
3. **Update docs/plan/**:
   - Update phase file (e.g., `phase-10-branches.md`) with DONE status
   - Update `README.md` - change status in table, update test count
   - Add changelog entry
4. **Update CLAUDE.md** - refresh test counts and status table

This ensures each phase has documented progress and test coverage for future sessions.

## Documentation

- **Main plan**: `docs/plan/README.md` - index with phase status and links
- **Phase specs**: `docs/plan/phase-*.md` - detailed specs per phase
- **Decisions**: `docs/plan/decisions.md` - all approved decisions
- **Risks**: `docs/plan/risks.md` - accepted MVP risks
- **ADRs**: `docs/adr/001-013` - architecture decisions
- **Key ADRs**:
  - `docs/adr/009-duckdb-file-per-table.md` - per-table file architecture
  - `docs/adr/012-branch-first-api-design.md` - Branch-First API design
- **Research**: `docs/duckdb-technical-research.md`, `docs/bigquery-driver-research.md`

## Local Connection

```bash
cd connection
docker compose up apache supervisor
# URL: https://localhost:8700/admin
# Login: dev@keboola.com / devdevdev
```
