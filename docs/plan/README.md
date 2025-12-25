# DuckDB Storage Backend - Implementation Plan

> **Goal:** On-premise Keboola without Snowflake and S3

## Current Status: ALL PHASES DONE - MVP Complete!

**Total Tests: 640**

| Phase | Name | Status | Tests | Details |
|-------|------|--------|-------|---------|
| 1 | Backend + Observability | DONE | 12 | [phase-01-backend.md](phase-01-backend.md) |
| 2 | Projects | DONE | 20 | [phase-02-projects.md](phase-02-projects.md) |
| 3 | Buckets + Sharing | DONE | 40 | [phase-03-buckets.md](phase-03-buckets.md) |
| 4 | Tables + Preview | DONE | 34 | [phase-04-tables.md](phase-04-tables.md) |
| 5 | Auth + Write Queue | DONE | 59 | [phase-05-auth-queue.md](phase-05-auth-queue.md) |
| 6 | Schema Operations | DONE | 33 | [phase-06-schema-ops.md](phase-06-schema-ops.md) |
| 7 | Import/Export | DONE | 17 | [phase-07-import-export.md](phase-07-import-export.md) |
| 8 | Files API | DONE | 20 | [phase-08-files.md](phase-08-files.md) |
| 9 | Snapshots + Settings | DONE | 34 | [phase-09-snapshots.md](phase-09-snapshots.md) |
| 10 | Dev Branches + Branch-First API | DONE | 34 | [phase-10-branches.md](phase-10-branches.md) |
| 11a | Workspaces REST API | DONE | 41 | [phase-11-workspaces.md](phase-11-workspaces.md) |
| 11b | PG Wire Server | DONE | 26 | [phase-11b-pgwire.md](phase-11b-pgwire.md) |
| 11c | Workspace Polish | DONE | 62 E2E | [phase-11c-workspace-polish.md](phase-11c-workspace-polish.md) |
| 12a | gRPC Server | DONE | 17 | [phase-12-php-driver.md](phase-12-php-driver.md) |
| 12b | Connection Backend Registration | DONE | - | [phase-12-php-driver.md](phase-12-php-driver.md) |
| 12c | gRPC Core Handlers | DONE | 23 | [phase-12c-core-handlers.md](phase-12c-core-handlers.md) |
| 12d | gRPC Schema Handlers | DONE | 18 | [phase-12-php-driver.md](phase-12-php-driver.md) |
| 12e | gRPC Workspace Handlers | DONE | 17 | [phase-12-php-driver.md](phase-12-php-driver.md) |
| 12h.1 | S3-Compatible API | DONE | 38 | [phase-12h-duckdb-files-in-connection.md](phase-12h-duckdb-files-in-connection.md) |
| 12h.2-5 | Connection File Integration | DONE | - | [phase-12h-duckdb-files-in-connection.md](phase-12h-duckdb-files-in-connection.md) |
| 12h.6 | File Routing Fix | DONE | - | [phase-12h-duckdb-files-in-connection.md](phase-12h-duckdb-files-in-connection.md) |
| 12h.7 | Async Table Creation | DONE | - | [phase-12h-duckdb-files-in-connection.md](phase-12h-duckdb-files-in-connection.md) |
| **12h.8** | **Backend Audit** | **DONE** | 5 fixes | [phase-12h-duckdb-files-in-connection.md](phase-12h-duckdb-files-in-connection.md) |
| 12f | Bucket Sharing Handlers | DONE | 15 | [phase-12-php-driver.md](phase-12-php-driver.md) |
| 12g | Branch & Query Handlers | DONE | - | [phase-12-php-driver.md](phase-12-php-driver.md) |
| 13 | Complete Observability | DONE | - | [phase-13-observability.md](phase-13-observability.md) |
| 14 | Backend Plugin Architecture | PROPOSAL | - | [phase-14-backend-registry.md](phase-14-backend-registry.md) |
| 15 | Comprehensive E2E Test Suite | DONE | 19 workflow | [phase-15-e2e-tests.md](phase-15-e2e-tests.md) |
| **16** | **Bug Fixes (E2E)** | **TODO** | - | [phase-16-bugfixes.md](phase-16-bugfixes.md) |
| **17** | **CLI & Python SDK** | **DONE** | 118 | [phase-17-cli-sdk.md](phase-17-cli-sdk.md) |
| **18** | **AWS Signature V4** | **DONE** | 10 | [phase-18-aws-sig-v4.md](phase-18-aws-sig-v4.md) |
| **19** | **Advanced Table Profiling** | **DONE** | 4 | [phase-19-advanced-profiling.md](phase-19-advanced-profiling.md) |

### Phase 19: Advanced Table Profiling - DONE

Data scientist-friendly table profiling:
- Skewness, Kurtosis, extended percentiles (Q01-Q99)
- Cardinality analysis (unique/high/medium/low/constant)
- Outlier detection (IQR method)
- Quality score (0-100) with recommendations
- Pattern detection (email, UUID, URL, phone)
- Column correlations (Pearson)
- CLI flags: `-q` (quality), `-r` (correlations), `-d` (distribution)

### Phase 18: AWS Signature V4 - DONE

boto3/aws-cli/rclone kompatibilita pro S3-compatible API:
- AWS Signature V4 verifikace
- 10 boto3 integration testu
- Funguje s existujicimi S3 klienty

### Phase 17: CLI & Python SDK - CLI DONE, SDK TODO

Python CLI a SDK pro externi vyvojare:
- `keboola-duckdb-cli` - CLI nastroj (typer, rich)
- `keboola-duckdb-sdk` - Python SDK (httpx, pydantic)
- 91 REST endpointu pokryto
- Async-first design

### Phase 16: Bug Fixes from E2E Testing - TODO

Opravy dvou bugu nalezenych pri E2E testovani:
1. **Linked bucket access** - GET linked bucket vraci 404 (Phase 3/12f)
2. **Auto-snapshot triggers** - Config inheritance nefunguje (Phase 9)

### Phase 15: Comprehensive E2E Test Suite - DONE

19 passing workflow testu pokryvajicich vsech 93 API endpointu:
- Workflows 1-10: Project, Data, Snapshot, Branch, Sharing, Workspace, S3, Files, Driver, PGWire
- 640 testu celkem, 100% pass rate

### Phase 14: Backend Plugin Architecture - PROPOSAL

Navrh na refactoring Connection codebase pro snadnejsi pridavani novych backendu:
- `BackendRegistry` - centralni registr backendu
- `BackendCapabilities` - feature flags per backend
- Cil: novy backend = 1 PHP trida + DI config (misto 15+ zmen)

### Phase 10: Branch-First API (ADR-012) - DONE

All bucket/table endpoints now use branch-first URL pattern:
- `/projects/{id}/branches/{branch_id}/buckets/...`
- `branch_id = "default"` for main, UUID for dev branches
- `source` field in TableResponse: `"main"` or `"branch"`

## Architecture (ADR-009)

**1 DuckDB file per table** - validated by Codex GPT-5 (4096 ATTACH test OK)

```
/data/duckdb/
├── project_123/              # Project = directory
│   ├── in_c_sales/           # Bucket = directory
│   │   ├── orders.duckdb     # Table = file
│   │   └── customers.duckdb
│   └── out_c_reports/
│       └── summary.duckdb
├── project_123_branch_456/   # Dev branch = directory copy
└── _staging/                 # Atomic operations staging
```

## Progress

```
[DONE] Backend + Observability (Prometheus /metrics)
       ↓
[DONE] Project CRUD (20 tests)
       ↓
[DONE] Bucket CRUD + Sharing (40 tests)
       ↓
[DONE] Table CRUD + Preview (34 tests)
       ↓
[DONE] ADR-009 Refactor (per-table files)
       ↓
[DONE] Auth + Write Queue (59 tests)
       ↓
[DONE] Table Schema Operations (33 tests)
       ↓
[DONE] Import/Export (17 tests)
       ↓
[DONE] Files API (20 tests)
       ↓
[DONE] Snapshots + Settings (34 tests)
       ↓
[DONE] Dev Branches (34 tests)
       ↓
[DONE] Workspaces REST API (41 tests)
       ↓
[DONE] PG Wire Server (26 tests)
       ↓
[DONE] Workspace Polish + E2E Tests (62 tests)
       ↓
[DONE] gRPC Server - Phase 12a (17 tests)
       ↓
[DONE] gRPC Core Handlers - Phase 12c (23 tests)
       ↓
[DONE] gRPC Schema Handlers - Phase 12d (18 tests)
       ↓
[DONE] gRPC Workspace Handlers - Phase 12e (17 tests)
       ↓
[DONE] S3-Compatible API - Phase 12h.1 (38 tests)
       ↓
[DONE] Connection File Integration - Phase 12h.2-5
       ↓
[DONE] File Routing Fix - Phase 12h.6 (Upload to DuckDB works!)
       ↓
[DONE] Backend Audit - Phase 12h.8 (5 critical files fixed)
       ↓
[DONE] Async Table Creation - Phase 12h.7 (Fixed via 12h.11)
       ↓
[DONE] Bucket Sharing Handlers - Phase 12f (15 tests)
       ↓
[DONE] Branch & Query Handlers - Phase 12g (3 handlers)
       ↓
[DONE] Complete Observability - Phase 13 (all metrics implemented)
       ↓
*** MVP COMPLETE! ***
       ↓
[DONE] Comprehensive E2E Test Suite - Phase 15 (630 tests)
       ↓
[DONE] AWS Signature V4 - Phase 18 (10 boto3 tests)
       ↓
[DONE] CLI (keboola-duckdb) - Phase 17 (118 tests)
       ↓
[DONE] Advanced Table Profiling - Phase 19 (skewness, correlations, quality score)
```

## E2E Test Coverage (Phase 11c)

| Test File | Tests | Coverage |
|-----------|-------|----------|
| test_data_pipeline_e2e.py | 18 | Import/Export CSV/Parquet, roundtrip, large files |
| test_snapshots_e2e.py | 8 | Create/Restore, auto-triggers, retention |
| test_branches_e2e.py | 8 | CoW, isolation, pull-to-main |
| test_bucket_sharing_e2e.py | 13 | Share/Link, readonly, cross-project |
| test_table_lifecycle_e2e.py | 6 | CRUD, data types, schema evolution |
| test_workspaces_e2e.py | 9 | Workspace + PG Wire auth workflow |

## Quick Links

- [Decisions](decisions.md) - all APPROVED/DEFERRED decisions
- [Risks](risks.md) - accepted MVP risks
- [ADR-009](../adr/009-duckdb-file-per-table.md) - per-table file architecture
- [ADR-012](../adr/012-branch-first-api-design.md) - Branch-First API design

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| v7.18 | 2025-12-25 | CLI profile fix: mode handling (-m full/quality/distribution), histogram visualization |
| v7.17 | 2025-12-25 | Phase 19 DONE: Advanced Table Profiling - skewness, kurtosis, cardinality, outliers, quality score, correlations |
| v7.16 | 2025-12-25 | Phase 17 DONE: CLI (keboola-duckdb) - projects, buckets, tables, files, profile commands (118 tests) |
| v7.15 | 2025-12-24 | Phase 18 DONE: AWS Signature V4 - boto3/aws-cli/rclone compatibility (10 tests) |
| v7.14 | 2025-12-23 | Phase 17 PLANNED: CLI & Python SDK (keboola-duckdb-cli, keboola-duckdb-sdk) |
| v7.13 | 2025-12-23 | Phase 15 DONE: Comprehensive E2E Test Suite - 630 tests, 100% pass rate, 93 API endpoints covered |
| v7.12 | 2025-12-21 | Phase 12h.8 DONE: Backend audit - fixed 5 files (getAssignedBackends, removeBackend, getRootCredentialsForBackend, getDefaultConnectionForBackend, File PHPDoc) |
| v7.11 | 2025-12-21 | Phase 12h.6 DONE: File upload to DuckDB works! Fixed getFileStorage(), Provider enum, File DTO |
| v7.10 | 2024-12-21 | Phase 12h.2 VERIFIED: DuckDB project file storage auto-assignment tested (Project 7) |
| v7.9 | 2024-12-21 | Phase 12h.2 DONE: Connection File Integration (migration, models, BackendAssign) |
| v7.8 | 2024-12-21 | Phase 12h.1 DONE: S3-Compatible API + Pre-signed URLs (38 tests), 575 total |
| v7.7 | 2024-12-21 | Phase 12e DONE: Workspace Handlers (17 tests) |
| v7.6 | 2024-12-21 | Phase 12d DONE: Schema Handlers (18 tests), 521 total tests |
| v7.5 | 2024-12-21 | Phase 12c DONE: Core Handlers (23 tests), Connection registration |
| v7.4 | 2024-12-20 | Phase 12a DONE: gRPC Server (17 tests), unified server, 480 total tests |
| v7.3 | 2024-12-19 | Update test count to 463, ADR-010 updated for buenavista |
| v7.2 | 2024-12-19 | Phase 10 DONE: Branch-First API refactoring complete |
| v7.1 | 2024-12-19 | ADR-012: Branch-First API design, Phase 10 refactoring |
| v7.0 | 2024-12-18 | Phase 11c: E2E tests (62), PG Wire metrics, graceful shutdown |
| v6.9 | 2024-12-18 | PG Wire Server: 26 tests, buenavista integration |
| v6.8 | 2024-12-17 | Dev Branches: 34 tests, CoW branching with Live View |
| v6.7 | 2024-12-16 | Snapshots + Settings: 34 tests, hierarchical config |
| v6.6 | 2024-12-16 | Prometheus /metrics: 180 tests |
| v6.5 | 2024-12-16 | Idempotency Middleware: 165 tests |
| v6.4 | 2024-12-16 | Auth + Write Queue: 144 tests |
| v6.1 | 2024-12-16 | ADR-009 Refactor: per-table files |
