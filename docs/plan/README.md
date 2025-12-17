# DuckDB Storage Backend - Implementation Plan

> **Goal:** On-premise Keboola without Snowflake and S3

## Current Status: Phase 11 (Workspaces)

**Total Tests: 312**

| Phase | Name | Status | Tests | Details |
|-------|------|--------|-------|---------|
| 1 | Backend + Observability | DONE | 12 | [phase-01-backend.md](phase-01-backend.md) |
| 2 | Projects | DONE | 20 | [phase-02-projects.md](phase-02-projects.md) |
| 3 | Buckets + Sharing | DONE | 40 | [phase-03-buckets.md](phase-03-buckets.md) |
| 4 | Tables + Preview | DONE | 34 | [phase-04-tables.md](phase-04-tables.md) |
| 5 | Auth + Write Queue | DONE | 46 | [phase-05-auth-queue.md](phase-05-auth-queue.md) |
| 6 | Schema Operations | DONE | 33 | [phase-06-schema-ops.md](phase-06-schema-ops.md) |
| 7 | Import/Export | DONE | 17 | [phase-07-import-export.md](phase-07-import-export.md) |
| 8 | Files API | DONE | 20 | [phase-08-files.md](phase-08-files.md) |
| 9 | Snapshots + Settings | DONE | 34 | [phase-09-snapshots.md](phase-09-snapshots.md) |
| 10 | Dev Branches | DONE | 28 | [phase-10-branches.md](phase-10-branches.md) |
| **11** | **Workspaces** | **NOW** | - | **[phase-11-workspaces.md](phase-11-workspaces.md)** |
| 12 | PHP Driver | TODO | - | [phase-12-php-driver.md](phase-12-php-driver.md) |

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
[DONE] Auth + Write Queue (46 tests)
       ↓
[DONE] Table Schema Operations (33 tests)
       ↓
[DONE] Import/Export (17 tests)
       ↓
[DONE] Files API (20 tests)
       ↓
[DONE] Snapshots + Settings (34 tests)
       ↓
[DONE] Dev Branches (28 tests)
       ↓
[NOW]  *** Workspaces ***
       ↓
[LAST] PHP Driver Package
```

## Quick Links

- [Decisions](decisions.md) - all APPROVED/DEFERRED decisions
- [Risks](risks.md) - accepted MVP risks
- [ADR-009](../adr/009-duckdb-file-per-table.md) - current architecture

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| v6.8 | 2024-12-17 | Dev Branches: 28 tests, CoW branching with Live View |
| v6.7 | 2024-12-16 | Snapshots + Settings: 34 tests, hierarchical config |
| v6.6 | 2024-12-16 | Prometheus /metrics: 180 tests |
| v6.5 | 2024-12-16 | Idempotency Middleware: 165 tests |
| v6.4 | 2024-12-16 | Auth + Write Queue: 144 tests |
| v6.1 | 2024-12-16 | ADR-009 Refactor: per-table files |
