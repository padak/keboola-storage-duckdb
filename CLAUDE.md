# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goal

Build an **on-premise Keboola with DuckDB backend** - a lightweight, self-contained version of Keboola Storage without cloud dependencies (no Snowflake, no S3).

## Repository Structure

```
docs/                         # Project documentation
  ├── local-connection.md    # Local Connection setup guide (COMPLETE)
  ├── duckdb-driver-plan.md  # DuckDB driver implementation plan
  ├── bigquery-driver-research.md  # BigQuery driver analysis
  └── adr/                   # Architecture Decision Records

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
  └── vendor/keboola/storage-driver-bigquery/  # Reference driver
```

## Current Status

1. **Local Connection**: Running at https://localhost:8700
   - Snowflake backend: Working
   - BigQuery backend: Working (creates GCP project per Keboola project)
   - S3 file storage: Working
   - GCS file storage: Working

2. **BigQuery Driver Research**: DONE (see `docs/bigquery-driver-research.md`)

3. **DuckDB API Service**: IN PROGRESS
   - FastAPI skeleton: DONE
   - Central metadata database: DONE (ADR-008)
   - Project CRUD API: DONE (32 tests)
   - Bucket CRUD API: DONE (37 tests) - includes share/link/readonly
   - Table CRUD API: DONE (29 tests) - includes preview, primary keys
   - Next: PHP Driver Package

4. **PHP Driver Package**: TODO (next step)

## Key Learnings (BigQuery Driver)

- Each Keboola project creates a **separate GCP project** in a GCP Folder
- Drivers communicate via **Protocol Buffers**, not REST
- Driver code is in `vendor/keboola/storage-driver-bigquery/`
- Implements `ClientInterface` from `storage-driver-common`
- 27 handlers dispatched via `HandlerFactory` match expression
- 3-stage import pipeline: staging -> transform/dedup -> cleanup
- Primary keys are metadata-only in BigQuery (not enforced)

## DuckDB vs BigQuery - Key Differences

| Aspect | BigQuery | DuckDB |
|--------|----------|--------|
| Project | GCP project | `.duckdb` file |
| Bucket | Dataset | Schema |
| Primary Key | Metadata only | Enforced constraint |
| Sharing | Analytics Hub | ATTACH (READ_ONLY) |
| File formats | CSV only | CSV + Parquet |

## DuckDB API Service Quick Reference

```bash
# Navigate to service
cd duckdb-api-service

# Setup (first time)
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run tests
source .venv/bin/activate
pytest tests/ -v

# Run server (development)
source .venv/bin/activate
python -m src.main

# Docker
docker compose up --build
```

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| MetadataDB | `src/database.py` | Central metadata (projects, files, audit log) |
| ProjectDBManager | `src/database.py` | Per-project DuckDB file management |
| Settings | `src/config.py` | pydantic-settings configuration |

### API Endpoints (implemented)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/backend/init` | POST | Initialize storage directories |
| `/backend/remove` | POST | Remove backend (no-op) |
| `/projects` | GET | List projects |
| `/projects` | POST | Create project |
| `/projects/{id}` | GET | Get project |
| `/projects/{id}` | PUT | Update project |
| `/projects/{id}` | DELETE | Delete project |
| `/projects/{id}/stats` | GET | Live statistics |
| `/projects/{id}/buckets` | GET | List buckets |
| `/projects/{id}/buckets` | POST | Create bucket (CREATE SCHEMA) |
| `/projects/{id}/buckets/{name}` | GET | Get bucket info |
| `/projects/{id}/buckets/{name}` | DELETE | Delete bucket (DROP SCHEMA) |
| `/projects/{id}/buckets/{name}/share` | POST | Share bucket |
| `/projects/{id}/buckets/{name}/share` | DELETE | Unshare bucket |
| `/projects/{id}/buckets/{name}/link` | POST | Link bucket (ATTACH + views) |
| `/projects/{id}/buckets/{name}/link` | DELETE | Unlink bucket |
| `/projects/{id}/buckets/{name}/grant-readonly` | POST | Grant readonly |
| `/projects/{id}/buckets/{name}/grant-readonly` | DELETE | Revoke readonly |
| `/projects/{id}/buckets/{bucket}/tables` | GET | List tables |
| `/projects/{id}/buckets/{bucket}/tables` | POST | Create table (CREATE TABLE) |
| `/projects/{id}/buckets/{bucket}/tables/{table}` | GET | Get table info (ObjectInfo) |
| `/projects/{id}/buckets/{bucket}/tables/{table}` | DELETE | Delete table (DROP TABLE) |
| `/projects/{id}/buckets/{bucket}/tables/{table}/preview` | GET | Preview table data (LIMIT) |

## Local Connection Quick Reference

```bash
# Start Connection
cd connection
docker compose up apache supervisor

# Access
URL: https://localhost:8700/admin
Login: dev@keboola.com / devdevdev

# Get Manage API token
# Go to: https://localhost:8700/admin/account/access-tokens
```

## API Endpoints (Manage API)

| Operation | Endpoint | Method |
|-----------|----------|--------|
| List S3 storage | `/manage/file-storage-s3` | GET |
| List GCS storage | `/manage/file-storage-gcs` | GET |
| List backends | `/manage/storage-backend` | GET |
| Create BigQuery backend | `/manage/storage-backend/bigquery` | POST |
| Assign backend to project | `/manage/projects/{id}/storage-backend` | POST |

## Documentation

### Main Docs
- `docs/README.md` - Documentation navigation guide
- `docs/local-connection.md` - Complete local Connection setup with troubleshooting
- `docs/duckdb-driver-plan.md` - DuckDB driver architecture and implementation plan
- `docs/bigquery-driver-research.md` - BigQuery driver analysis (reference for DuckDB)

### Research
- `docs/duckdb-technical-research.md` - DuckDB capabilities analysis
- `docs/duckdb-keboola-features.md` - Keboola feature mapping to DuckDB
- `docs/duckdb-api-endpoints.md` - Storage API endpoints to implement

### Architecture Decision Records (ADRs)
- `docs/adr/001-duckdb-microservice-architecture.md` - Python microservice approach
- `docs/adr/002-duckdb-file-organization.md` - File/directory structure
- `docs/adr/003-duckdb-branch-strategy.md` - Dev branches implementation
- `docs/adr/004-duckdb-snapshots.md` - Table snapshots design
- `docs/adr/005-duckdb-write-serialization.md` - Concurrent write handling
- `docs/adr/006-duckdb-on-prem-storage.md` - On-premise file storage
- `docs/adr/007-duckdb-cow-branching.md` - Copy-on-Write branching strategy
- `docs/adr/008-central-metadata-database.md` - Central metadata DB for projects/files

## Development Notes

### DuckDB + Python Tips

1. **DuckDB requires `pytz`** for TIMESTAMPTZ columns - add to requirements.txt
2. **JSON columns** are returned as strings - parse with `json.loads()` when reading
3. **ATTACH is session-specific** - each connection needs its own ATTACH, so do all operations in one connection
4. **Singleton pattern in tests** - use `@property` for paths to allow runtime override:
   ```python
   # BAD: path set at init time, can't override in tests
   def __init__(self):
       self._db_path = settings.metadata_db_path

   # GOOD: path read from settings on each access
   @property
   def _db_path(self):
       return settings.metadata_db_path
   ```

### Testing with pytest

- Use `monkeypatch.setattr(settings, "path", new_path)` to override settings
- Fixtures in `conftest.py` create temp directories for each test
- Each test gets isolated metadata.duckdb and project files
