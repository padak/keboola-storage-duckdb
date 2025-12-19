# DuckDB Storage Backend - Implementation Plan

> **Goal:** On-premise Keboola without Snowflake and S3

## Current Status: Phase 12 TODO (PHP Driver)

**Total Tests: 438**

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
| 10 | Dev Branches + Branch-First API | DONE | 26 | [phase-10-branches.md](phase-10-branches.md) |
| 11a | Workspaces REST API | DONE | 41 | [phase-11-workspaces.md](phase-11-workspaces.md) |
| 11b | PG Wire Server | DONE | 26 | [phase-11b-pgwire.md](phase-11b-pgwire.md) |
| 11c | Workspace Polish | DONE | 62 E2E | [phase-11c-workspace-polish.md](phase-11c-workspace-polish.md) |
| **12** | **PHP Driver** | **TODO** | - | [phase-12-php-driver.md](phase-12-php-driver.md) |

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
[LAST] PHP Driver Package
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
| v7.2 | 2024-12-19 | Phase 10 DONE: Branch-First API refactoring complete (438 tests) |
| v7.1 | 2024-12-19 | ADR-012: Branch-First API design, Phase 10 refactoring |
| v7.0 | 2024-12-18 | Phase 11c: E2E tests (62), PG Wire metrics, graceful shutdown |
| v6.9 | 2024-12-18 | PG Wire Server: 26 tests, buenavista integration |
| v6.8 | 2024-12-17 | Dev Branches: 34 tests, CoW branching with Live View |
| v6.7 | 2024-12-16 | Snapshots + Settings: 34 tests, hierarchical config |
| v6.6 | 2024-12-16 | Prometheus /metrics: 180 tests |
| v6.5 | 2024-12-16 | Idempotency Middleware: 165 tests |
| v6.4 | 2024-12-16 | Auth + Write Queue: 144 tests |
| v6.1 | 2024-12-16 | ADR-009 Refactor: per-table files |
