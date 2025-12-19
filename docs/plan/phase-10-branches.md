# Phase 10: Dev Branches - REFACTORING

## Status
- **Status:** REFACTORING (2024-12-19)
- **Previous:** DONE (2024-12-17) - basic branch CRUD + CoW
- **Tests:** 28 tests (will need update after refactoring)
- **Architecture:** ADR-007 (CoW branching), **ADR-012 (Branch-First API)**

## Goal

Implement **Branch-First API Design** (ADR-012) - all bucket/table operations go through branches, with `default` representing main.

## Current State (Before Refactoring)

### What Works
- Branch CRUD: create, list, get, delete
- Copy-on-Write for existing tables
- Pull table from main
- Branch isolation after CoW

### What's Missing
- Cannot specify branch for bucket/table operations
- Cannot create table only in branch (not in main)
- No branch-aware bucket/table listing
- "Main" is implicit, not explicit branch

## New API Design (ADR-012)

### URL Structure

**All resources accessed through branches:**

```
/projects/{project_id}/branches/{branch_id}/buckets
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/preview
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/import/file
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/export
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/columns
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/snapshots
...
```

### Special Branch ID: `default`

- `branch_id = "default"` = production project (main)
- Dev branches use UUID/short-id
- Example: `/projects/123/branches/default/buckets` = main project

### Endpoints to Implement/Migrate

| Old Endpoint | New Endpoint | Status |
|--------------|--------------|--------|
| `GET /projects/{id}/buckets` | `GET /projects/{id}/branches/{branch}/buckets` | TODO |
| `POST /projects/{id}/buckets` | `POST /projects/{id}/branches/{branch}/buckets` | TODO |
| `GET /projects/{id}/buckets/{b}` | `GET /projects/{id}/branches/{branch}/buckets/{b}` | TODO |
| `DELETE /projects/{id}/buckets/{b}` | `DELETE /projects/{id}/branches/{branch}/buckets/{b}` | TODO |
| `GET /projects/{id}/buckets/{b}/tables` | `GET /projects/{id}/branches/{branch}/buckets/{b}/tables` | TODO |
| `POST /projects/{id}/buckets/{b}/tables` | `POST /projects/{id}/branches/{branch}/buckets/{b}/tables` | TODO |
| ... | ... | ... |

### Response Extensions

Table responses include `source` field:

```json
{
  "name": "orders",
  "bucket_name": "sales",
  "source": "main",      // "main" | "branch" | "branch_only"
  "row_count": 1500
}
```

- `main`: Table exists in main, branch reads from main (Live View)
- `branch`: Table was copied to branch (CoW performed)
- `branch_only`: Table exists only in branch (created in branch)

### Branch Behavior

| Operation | Default Branch | Dev Branch |
|-----------|----------------|------------|
| READ | Direct read | Live View (main) or branch copy |
| WRITE | Direct write | CoW trigger, then write |
| CREATE table | Create in main | Create only in branch |
| DELETE table | Delete from main | Delete from branch (main unaffected) |

## Implementation Plan

### Phase 10a: Router Refactoring

1. Create new router file `routers/branch_resources.py`
2. Add `branch_id` path parameter to all bucket/table endpoints
3. Implement branch resolution (`default` -> main, else -> branch)
4. Update dependencies for branch-aware auth

### Phase 10b: Storage Layer Updates

1. Extend `ProjectDBManager` with branch-aware methods:
   - `get_table_path(project_id, branch_id, bucket, table)`
   - `table_exists_in_branch(project_id, branch_id, bucket, table)`
   - `get_table_source(project_id, branch_id, bucket, table)` -> main|branch|branch_only
2. Implement Live View read logic
3. Implement CoW trigger on write
4. Support branch-only table creation

### Phase 10c: Metadata Updates

1. Track branch-only tables in `branch_tables` with flag
2. Update bucket metadata for branch-specific buckets
3. Extend listing queries to merge main + branch resources

### Phase 10d: Test Migration

1. Update all existing tests to use `/branches/default/` paths
2. Add branch-specific test scenarios:
   - Create table in branch only
   - List tables shows main + branch
   - Delete branch-only table
   - CoW on first write to existing table

## Test Plan

| Test | Description |
|------|-------------|
| `test_default_branch_buckets` | CRUD buckets via default branch |
| `test_default_branch_tables` | CRUD tables via default branch |
| `test_dev_branch_live_view` | Branch reads main data before CoW |
| `test_dev_branch_cow_on_write` | First write triggers CoW |
| `test_branch_only_table_create` | Create table only in branch |
| `test_branch_only_table_delete` | Delete branch-only table |
| `test_branch_table_listing_merged` | List shows main + branch tables |
| `test_branch_table_source_field` | Response includes source field |
| `test_branch_isolation` | Branch changes don't affect main |
| `test_delete_branch_cleans_tables` | Delete branch removes all branch tables |

## Migration Notes

### Existing Data

- No data migration needed
- `/data/duckdb/project_{id}/` = default branch storage
- `/data/duckdb/project_{id}_branch_{branch_id}/` = dev branch storage

### Breaking Changes

- All bucket/table URLs change (add `/branches/{branch_id}/`)
- Old URLs will return 404

## Key Decisions

| Decision | Value | Source |
|----------|-------|--------|
| Branch URL position | In path (not query param) | ADR-012 |
| Main branch ID | `default` | ADR-012 |
| Branch storage | Directory per branch | ADR-007, ADR-009 |
| Table source tracking | `source` field in response | ADR-012 |

## Reference

- **ADR-007**: `docs/adr/007-duckdb-cow-branching.md` - CoW implementation
- **ADR-009**: `docs/adr/009-duckdb-file-per-table.md` - Per-table files
- **ADR-012**: `docs/adr/012-branch-first-api-design.md` - Branch-First API
