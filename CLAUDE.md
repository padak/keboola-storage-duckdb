# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goal

Build an **on-premise Keboola with DuckDB backend** - a lightweight, self-contained version of Keboola Storage without cloud dependencies (no Snowflake, no S3).

## Repository Structure

```
docs/                         # Project documentation
  ├── local-connection.md    # Local Connection setup guide (COMPLETE)
  ├── duckdb-driver-plan.md  # DuckDB driver implementation plan
  └── README.md              # Documentation navigation

connection/                   # Keboola Connection (git submodule/clone)
  └── vendor/keboola/storage-driver-bigquery/  # Reference driver

php-storage-driver-bigquery/  # BigQuery driver source (for study)
```

## Current Status

1. **Local Connection**: Running at https://localhost:8700
   - Snowflake backend: Working
   - BigQuery backend: Working (creates GCP project per Keboola project)
   - S3 file storage: Working
   - GCS file storage: Working

2. **BigQuery Driver Research**: DONE (see `docs/bigquery-driver-research.md`)

3. **Next Step**: Implement DuckDB driver (Python API + PHP Driver)

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
