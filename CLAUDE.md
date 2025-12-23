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
  │   ├── unified_server.py  # REST + gRPC unified server
  │   ├── middleware/        # HTTP middleware
  │   │   ├── idempotency.py # X-Idempotency-Key handling
  │   │   └── metrics.py     # Request instrumentation
  │   ├── grpc/              # gRPC layer (Phase 12a)
  │   │   ├── server.py      # gRPC server
  │   │   ├── servicer.py    # StorageDriverServicer
  │   │   ├── utils.py       # LogMessageCollector, helpers
  │   │   └── handlers/      # Command handlers (26 handlers)
  │   │       ├── base.py       # BaseCommandHandler
  │   │       ├── backend.py    # InitBackend, RemoveBackend
  │   │       ├── project.py    # CreateProject, DropProject
  │   │       ├── bucket.py     # CreateBucket, DropBucket
  │   │       ├── table.py      # CreateTable, DropTable, PreviewTable
  │   │       ├── info.py       # ObjectInfo
  │   │       ├── import_export.py # TableImportFromFile, TableExportToFile
  │   │       ├── schema.py     # AddColumn, DropColumn, AlterColumn, Add/DropPK, DeleteRows
  │   │       └── workspace.py  # Create/Drop/Clear/ResetPwd/DropObject/Grant/Revoke/Load
  │   └── routers/           # REST API endpoints
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
  │       ├── driver.py      # HTTP bridge for driver commands (Phase 12a.1)
  │       ├── s3_compat.py   # S3-compatible API (Phase 12h.1)
  │       └── metrics.py     # Prometheus /metrics endpoint
  ├── proto/                 # Protocol Buffer definitions
  ├── generated/             # Generated Python protobuf code
  └── tests/                 # pytest tests (630 tests)

connection/                   # Keboola Connection (git submodule/clone)
```

## Current Status (2024-12-21)

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
| **gRPC Server (Phase 12a)** | **DONE** | 17 |
| **gRPC Core Handlers (Phase 12c)** | **DONE** | 23 |
| **gRPC Schema Handlers (Phase 12d)** | **DONE** | 18 |
| **gRPC Workspace Handlers (Phase 12e)** | **DONE** | 17 |
| **Connection Backend Registration (Phase 12b)** | **DONE** | - |
| **Connection Full Integration (Phase 12b.1)** | **DONE** | - |
| **Secure Project API Keys (Phase 12b.2)** | **DONE** | - |
| **S3-Compatible API (Phase 12h.1)** | **DONE** | 38 |
| **Connection File Integration (Phase 12h.2-6)** | **DONE** | - |
| **Async Table Creation (Phase 12h.7)** | **DONE** | - |
| **Backend Audit (Phase 12h.8)** | **DONE** | 5 fixes |
| **Import/Preview Fixes (Phase 12h.9-12)** | **DONE** | - |
| **Complete Observability (Phase 13)** | **DONE** | - |
| **Bucket Sharing Handlers (Phase 12f)** | **DONE** | 15 |
| **Branch & Query Handlers (Phase 12g)** | **DONE** | - |
| Schema Migrations | TODO | - |
| **Phase 15: E2E Test Suite** | **DONE** | 630 |

**Total: 630 tests PASS** (including 62 E2E + 90 gRPC + 38 S3 tests)

## Post-MVP TODO & Technical Debt

### Phase 15: Comprehensive E2E Test Suite (DONE)

Kompletni E2E test suite - viz `docs/plan/phase-15-e2e-tests.md`:
- **93 API endpointu** - 100% pokryto
- **630 testu** - 100% pass rate
- 10 workflow testu pokryvajicich vsechny endpointy
- Real HTTP E2E testy (uvicorn + httpx)
- S3 testy s boto3 klientem
- Auto-snapshot pri TRUNCATE/DELETE ALL

### Phase 11c: Workspace Polish (Partially TODO)

Viz `docs/plan/phase-11c-workspace-polish.md`:
- [ ] Admin force-disconnect sessions
- [ ] Query audit logging
- [ ] Query performance tracing
- [ ] Real PG Wire E2E tests (ne mock)
- [ ] Resource limits enforcement
- [ ] Load & performance tests

### Post-MVP Technical Debt (from risks.md & ADRs)

| Item | Current State | Target | Reference |
|------|---------------|--------|-----------|
| **Dev branches full copy** | Directory copy (slow for large projects) | CoW (Copy-on-Write) | ADR-007 |
| **HA / Multi-instance** | Single FastAPI instance | Leader election / Read replicas | risks.md |
| **Auth model** | Static API keys | Key rotation, RBAC, mTLS | risks.md |
| **Encryption at rest** | None (relies on LUKS) | DuckDB-level encryption | risks.md |
| **DR/Backup API** | Manual (filesystem snapshots) | Built-in backup/restore endpoints | risks.md |
| **Schema migrations** | Manual | Versioning + startup migration | risks.md |

**ADR-007 CoW Branching** - APPROVED but not implemented:
- Branch vidi LIVE data z main (ne snapshot)
- Copy-on-Write pri prvnim zapisu do tabulky
- Merge = pouze konfigurace, NE tabulky
- Post-MVP optimalizace pro velke projekty

**Current: ALL PHASES DONE - MVP Complete!**

**SUCCESS (2024-12-21):** Table creation via Connection works end-to-end with secure project isolation!

**What works:**
- Create DuckDB project via Manage API (`defaultBackend: duckdb`)
- Create Storage API token for DuckDB project
- Create bucket with DuckDB backend
- **CREATE TABLE via CSV upload** - Connection calls DuckDB API via HTTP bridge
- List tables shows created tables with `backend: duckdb`
- **Secure project isolation** - Each project has its own API key stored in `bi_connectionsCredentials`
- **S3-Compatible API** - GET/PUT/DELETE/HEAD/ListObjectsV2 + pre-signed URLs

**Phase 12b.2 Implementation:**
- Project API keys stored encrypted in `bi_connectionsCredentials.password`
- `DuckDBCredentialsResolver` reads and decrypts project API key
- `DuckDBDriverClient` uses project API key in Authorization header
- HTTP bridge validates that API key matches `project_id` in request

**See:** `docs/plan/phase-12-php-driver.md` for detailed integration notes

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
14. ~~gRPC Server (Phase 12a)~~ - DONE
15. ~~Connection Backend Registration (Phase 12b)~~ - DONE
16. ~~gRPC Core Handlers (Phase 12c)~~ - DONE
17. ~~gRPC Schema Handlers (Phase 12d)~~ - DONE
18. ~~gRPC Workspace Handlers (Phase 12e)~~ - DONE
19. ~~Connection Full Integration (Phase 12b.1)~~ - DONE
20. ~~Secure Project API Keys (Phase 12b.2)~~ - DONE
21. ~~S3-Compatible API (Phase 12h.1)~~ - DONE
22. ~~Connection File Integration (Phase 12h.2-6)~~ - DONE
23. ~~Backend Audit (Phase 12h.8)~~ - DONE
24. **Async Table Creation (Phase 12h.7)** - NEXT

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
| **UI file upload nefunguje** | **Pouzivat Storage API** (viz nize) |

## Known Limitation: Connection UI File Upload

**Problem:** Connection UI pouziva zastaraly `POST /upload-file` endpoint pro nahravani CSV souboru. Tento endpoint pro DuckDB backend neexistuje a vraci 404.

**Pricina:** DuckDB adapter nepodporuje legacy form upload (viz `DuckDbAdapter::createLegacyFormUploadParams()`). Moderni flow pouziva pre-signed URLs.

**Workaround:** Pro vytvareni tabulek pouzivat Storage API primo:

```bash
# 1. Pripravit soubor pro upload
curl -X POST "https://localhost:8700/v2/storage/files/prepare" \
  -H "X-StorageApi-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "myfile.csv", "federationToken": true}'

# 2. Uploadnout soubor na pre-signed URL (z odpovedi)
curl -X PUT "$PRESIGNED_URL" \
  -H "Content-Type: text/csv" \
  --data-binary @myfile.csv

# 3. Vytvorit tabulku z uploadovaneho souboru
curl -X POST "https://localhost:8700/v2/storage/buckets/in.c-test/tables-async" \
  -H "X-StorageApi-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "mytable", "dataFileId": "<file_id>"}'
```

**Reseni (post-MVP):** Upravit Connection UI aby pro DuckDB backend pouzivalo pre-signed URL flow misto legacy form upload.

## DuckDB API Service Quick Reference

```bash
cd duckdb-api-service
source .venv/bin/activate
pytest tests/ -v                  # Run tests (575 total)
python -m src.main                # Run REST API only (port 8000)
python -m src.unified_server      # Run REST + gRPC (ports 8000, 50051)
python -m src.grpc.server         # Run gRPC only (port 50051)
docker compose up --build         # Docker
open dashboard2.html              # Metrics dashboard - tab-based (recommended)
open dashboard.html               # Metrics dashboard - single-page view
```

### gRPC Testing with grpcurl

```bash
# List services
grpcurl -plaintext -import-path . -proto proto/service.proto localhost:50051 list

# InitBackendCommand
grpcurl -plaintext -import-path . -proto proto/service.proto \
  -proto proto/common.proto -proto proto/backend.proto \
  -d '{"command": {"@type": "type.googleapis.com/keboola.storageDriver.command.backend.InitBackendCommand"}}' \
  localhost:50051 keboola.storageDriver.service.StorageDriverService/Execute

# CreateProjectCommand
grpcurl -plaintext -import-path . -proto proto/service.proto \
  -proto proto/common.proto -proto proto/project.proto \
  -d '{"command": {"@type": "type.googleapis.com/keboola.storageDriver.command.project.CreateProjectCommand", "projectId": "test-123"}}' \
  localhost:50051 keboola.storageDriver.service.StorageDriverService/Execute
```

### Metrics Dashboards

Two standalone HTML dashboards for Prometheus metrics visualization:

**`dashboard.html`** - Full single-page view with all sections vertically
- All metrics visible on one scrollable page
- 14 sections: Service Health, Storage, Metadata DB, Import/Export, Files & S3, Snapshots, Concurrency, Dev Branches, Workspaces & PG Wire, gRPC, Schema & Sharing, Charts, Tables

**`dashboard2.html`** - Tab-based navigation (recommended)
- **Always-visible KPI row**: Status, Uptime, Requests, Error Rate, P95 Latency, Tables, Storage
- **5 tabs** for organized navigation:
  - **Overview**: Storage breakdown, latency percentiles, request charts
  - **Data**: Import/Export, Metadata DB, Schema operations
  - **APIs**: HTTP REST, gRPC, PG Wire, S3-Compatible (all interfaces together)
  - **Operations**: Locks, Write Queue, Cache, Branches, Snapshots, Files
  - **Details**: Endpoints table, Lock activity, Python runtime
- Error badge on APIs tab when 5xx errors occur

Both dashboards auto-refresh every 5s and work with API on `localhost:8000`.

### Implemented Endpoints (Current)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics (no auth) |
| `/driver/execute` | POST | Execute driver command (admin auth) |
| `/driver/commands` | GET | List supported driver commands |
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
| `/s3/{bucket}/{key}` | GET/PUT/DELETE/HEAD | S3-compatible file operations |
| `/s3/{bucket}` | GET | S3 ListObjectsV2 |
| `/s3/{bucket}/presign` | POST | Generate pre-signed URL |

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
8. **File size after close** - DuckDB uses WAL, always read file size AFTER `conn.close()` (size can be 60x larger after close)
9. **Connection caches sizes** - Table `dataSizeBytes` is cached in MySQL `bi_metadata_tables`, update via SQL if needed:
   ```sql
   UPDATE bi_metadata_tables SET dataSizeBytes = <actual_size> WHERE idBucket = <id> AND name = '<table>';
   ```

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

### Prerequisites

1. **Mount MySQL data disk** (macOS sparse image):
   ```bash
   hdiutil attach -mountpoint ./docker/.mysql-accounts-datadir ~/ocker-image-mysql-accounts.sparseimage
   ```

2. **Start services:**
   ```bash
   cd connection
   docker compose up apache supervisor
   # URL: https://localhost:8700/admin
   # Login: dev@keboola.com / devdevdev
   ```

### DuckDB Backend Configuration

DuckDB environment variables are configured in `connection/.env.local`:

```bash
# Already set in .env.local:
DUCKDB_SERVICE_URL=http://host.docker.internal:8000
DUCKDB_ADMIN_API_KEY=xxx
```

Note: `host.docker.internal` allows Docker containers to reach services on the host machine.

### DuckDB Integration via API (Phase 12b.1)

After running the migration and SQL changes (see `docs/plan/phase-12-php-driver.md`):

```bash
# 1. Create maintainer with DuckDB (then set via SQL)
curl -s -k -X POST "https://localhost:8700/manage/maintainers" \
  -H "X-KBC-ManageApiToken: $MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "DuckDB Services"}'

# 2. Set DuckDB connection via SQL (ID 3 = DuckDB connection)
docker compose exec mysql-accounts mysql -u root -proot accounts \
  -e "UPDATE bi_maintainers SET idDefaultConnectionDuckdb = 3 WHERE name = 'DuckDB Services';"

# 3. Create organization under DuckDB maintainer
curl -s -k -X POST "https://localhost:8700/manage/maintainers/{maintainer_id}/organizations" \
  -H "X-KBC-ManageApiToken: $MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "DuckDB Org"}'

# 4. Create project with DuckDB backend
curl -s -k -X POST "https://localhost:8700/manage/organizations/{org_id}/projects" \
  -H "X-KBC-ManageApiToken: $MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "DuckDB Test Project", "defaultBackend": "duckdb"}'

# 5. Set DuckDB connection for project via SQL
docker compose exec mysql-accounts mysql -u root -proot accounts \
  -e "UPDATE bi_projects SET idDefaultConnectionDuckdb = 3 WHERE name = 'DuckDB Test Project';"

# 6. Create Storage API token
curl -s -k -X POST "https://localhost:8700/manage/projects/{project_id}/tokens" \
  -H "X-KBC-ManageApiToken: $MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"description": "DuckDB Test Token", "canManageBuckets": true}'

# 7. Use Storage API token
curl -s -k "https://localhost:8700/v2/storage/buckets" \
  -H "X-StorageApi-Token: $STORAGE_TOKEN"
```
