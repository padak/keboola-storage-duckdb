# Accepted Risks for MVP

## Overview

These risks have been evaluated and accepted for the MVP phase. Each has documented mitigation strategies.

## Risk Matrix

| Risk | Severity | Probability | Mitigation | Status |
|------|----------|-------------|------------|--------|
| Cross-DB consistency | Medium | Low | Cache + on-demand recalc | **ACCEPTED** |
| Write Queue volatility | Medium | Low | Idempotency middleware | **ACCEPTED** |
| Single FastAPI instance | Medium | Medium | HA post-MVP | **ACCEPTED** |
| Simple auth model | Low | Low | Enterprise extension | **ACCEPTED** |
| Bucket sharing without ACL | Low | Low | App-layer enforcement | **ACCEPTED** |
| Dev branches full copy | Low | Medium | CoW optimization | **ACCEPTED** |
| No DR/Backup API | Medium | Low | Documentation | **ACCEPTED** |
| No encryption at rest | Medium | Low | LUKS filesystem | **ACCEPTED** |
| Schema migrations | Medium | Medium | Versioning + startup migration | **ACCEPTED** |

## Detailed Analysis

### Cross-DB Consistency

**Problem:** metadata.duckdb and project .duckdb files cannot be in single transaction.

**Mitigation:**
```
metadata.duckdb contains:
├── projects          → CRITICAL (project registry)
├── bucket_shares     → CRITICAL (sharing relations)
├── bucket_links      → CRITICAL (linking relations)
├── files             → CRITICAL (file registry)
├── operations_log    → NON-CRITICAL (audit, loss acceptable)
└── stats             → NON-CRITICAL (cache, recalculate on-demand)
```

If project DB write succeeds but metadata fails:
- Stats will be stale -> recalculate on GET /projects/{id}/stats
- Audit log will be missing -> acceptable for MVP

### Write Queue Volatility

**Problem:** In-memory queue is lost on crash.

**Mitigation:**
- Client waits for response (synchronous)
- Retry is on Keboola Storage API side
- X-Idempotency-Key header prevents duplicate operations
- TTL 10 minutes for idempotency cache

### Single FastAPI Instance

**Problem:** DuckDB single-writer doesn't allow multiple writers.

**Mitigation:**
- MVP runs on 1 instance
- HA solutions for enterprise:
  - Leader election
  - Sticky sessions
  - Read replicas

### Simple Auth Model

**Problem:** Static API keys, no rotation, no per-user scopes.

**Mitigation:**
- Hierarchical model (ADMIN + PROJECT keys) sufficient for MVP
- Extension for enterprise:
  - Key rotation
  - RBAC (role-based access control)
  - mTLS

### Bucket Sharing Without DB-level ACL

**Problem:** DuckDB has no per-schema access rights.

**Mitigation:**
- App-layer enforcement via API auth
- Filesystem permissions (700) on .duckdb files
- Sharing registry in metadata.duckdb

### Dev Branches Full Copy

**Problem:** Each branch = full directory copy.

**Mitigation:**
- OK for MVP (small projects)
- CoW optimization (ADR-007) post-MVP
- Lazy copy on first write

### No DR/Backup API

**Problem:** No backup/restore endpoints.

**Mitigation:**
- Document recommendation: filesystem snapshots (ZFS, btrfs, rsync)
- Snapshots API provides table-level backup
- Full implementation post-MVP

### No Encryption at Rest

**Problem:** DuckDB files are unencrypted.

**Mitigation:**
- Filesystem-level encryption (LUKS)
- Implementation post-MVP
- Optional AES-256 for sensitive data

### Schema Migrations

**Problem:** metadata.duckdb schema may change.

**Mitigation:**
- Versioning in DB (`schema_version` table)
- Migration scripts run on FastAPI startup
- Backward-compatible changes preferred
