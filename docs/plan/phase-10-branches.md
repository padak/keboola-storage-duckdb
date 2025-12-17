# Phase 10: Dev Branches - NOW

## Status
- **Status:** In Progress
- **Architecture:** ADR-007 (CoW branching with Live View)
- **Simplified by:** ADR-009 (per-table files)

## Goal

Implement dev branches matching Keboola Storage production behavior:
- **Live View**: Branch sees current main data until table is modified
- **Copy-on-Write**: First write to table copies it to branch
- **No table merge**: Merge = only configurations, branch tables are deleted

## Key Behavior (from Keboola Production)

```
1. CREATE BRANCH
   - Empty directory created
   - Branch sees LIVE data from main (via ATTACH READ_ONLY)

2. READ from branch
   - Table NOT in branch -> read from main (live!)
   - Table IN branch -> read from branch copy

3. WRITE to branch (CoW trigger)
   - Table NOT in branch -> COPY current main state, then write
   - Table IN branch -> write directly

4. MERGE branch
   - Merges ONLY configurations (handled by Connection, not Storage API)
   - Storage tables are NOT merged back to main!

5. DELETE branch
   - All branch tables are DELETED
   - Data changes in branch are LOST (unless manually exported)
```

## Endpoints to Implement

```
POST   /projects/{id}/branches                     # CreateDevBranch
GET    /projects/{id}/branches                     # ListBranches
GET    /projects/{id}/branches/{branch_id}         # BranchDetail
DELETE /projects/{id}/branches/{branch_id}         # DropDevBranch
POST   /projects/{id}/branches/{branch_id}/tables/{bucket}/{table}/pull  # PullTable (refresh from main)
```

**Note:** No `/merge` endpoint for tables - merge only handles configurations in Connection layer.

## Implementation Strategy

With ADR-009 (per-table files), branching is simplified:

```
/data/duckdb/
├── project_123/                    # Main branch
│   ├── in_c_sales/
│   │   ├── orders.duckdb
│   │   └── customers.duckdb
│   └── out_c_reports/
│       └── summary.duckdb
│
├── project_123_branch_456/         # Dev branch = directory (only copied tables)
│   └── in_c_sales/
│       └── orders.duckdb           # Only tables that were WRITTEN to
```

### CreateDevBranch
1. Create branch directory `project_{id}_branch_{branch_id}/`
2. Register in metadata.duckdb (`branches` table)
3. **NO data copy** - tables read from main via ATTACH

### Read from Branch
1. Check if table exists in branch directory
2. If YES -> read from branch `.duckdb` file
3. If NO -> ATTACH main table READ_ONLY, read from there

### Write to Branch (Copy-on-Write)
1. Check if table exists in branch directory
2. If NO -> copy `.duckdb` file from main to branch (CoW)
3. Write to branch copy

### PullTable (refresh from main)
1. Delete table from branch directory (if exists)
2. Table now reads from main again (live view restored)

### DeleteBranch
1. Delete entire branch directory (all .duckdb files)
2. Remove from metadata.duckdb
3. **Tables are LOST** - this is expected behavior

## Metadata Schema

```sql
CREATE TABLE branches (
    id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    created_by VARCHAR,
    description TEXT,

    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE INDEX idx_branches_project ON branches(project_id);

-- Track which tables have been copied to branch (for informational purposes)
CREATE TABLE branch_tables (
    branch_id VARCHAR NOT NULL,
    bucket_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    copied_at TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (branch_id, bucket_name, table_name),
    FOREIGN KEY (branch_id) REFERENCES branches(id)
);
```

## Key Decisions

| Decision | Value | Source |
|----------|-------|--------|
| Branch storage | Directory with per-table files (ADR-009) | ADR-009 |
| Read strategy | **Live View** - read from main until write | Keboola prod |
| Write strategy | **Copy-on-Write** - copy on first write | ADR-007 |
| Merge behavior | **NO table merge** - only configurations | Keboola prod |
| Delete behavior | Delete all branch tables | Keboola prod |

## Example Flow

```python
# T0: Create branch
POST /projects/123/branches
{"name": "feature-new-report", "description": "Testing new report logic"}
# Result: branch_id = "456", empty directory created

# T1: Read table (not modified yet) - reads from MAIN
GET /projects/123/branches/456/buckets/in_c_sales/tables/orders/preview
# Returns: current main data (live view)

# T2: Main gets updated (100 -> 150 rows)
# (some other process writes to main)

# T3: Read from branch again - still live!
GET /projects/123/branches/456/buckets/in_c_sales/tables/orders/preview
# Returns: 150 rows (current main state)

# T4: Write to branch table (triggers CoW)
POST /projects/123/branches/456/buckets/in_c_sales/tables/orders/import
# CoW: copies orders.duckdb from main (150 rows) to branch
# Then: applies the import

# T5: Main gets updated again (150 -> 170 rows)
# (some other process writes to main)

# T6: Read from branch - now isolated!
GET /projects/123/branches/456/buckets/in_c_sales/tables/orders/preview
# Returns: data from branch copy (150 + import changes)
# Main changes (170) NOT visible

# T7: Delete branch
DELETE /projects/123/branches/456
# Result: branch directory deleted, all branch tables LOST
```

## Test Plan

| Test | Description |
|------|-------------|
| `test_create_branch` | Creates branch, verifies empty directory |
| `test_branch_read_from_main` | Branch reads live data from main |
| `test_branch_cow_on_write` | First write triggers copy |
| `test_branch_isolation_after_cow` | After CoW, main changes not visible |
| `test_pull_table` | Pull restores live view |
| `test_delete_branch` | Deletes all branch tables |
| `test_main_changes_visible_before_cow` | Main changes visible until CoW |

## Reference

- **ADR-007**: `docs/adr/007-duckdb-cow-branching.md` - CoW implementation details
- **ADR-009**: `docs/adr/009-duckdb-file-per-table.md` - Per-table file architecture
- **Keboola Source**: `DevBranchCreate.php`, `DevBranchDelete.php`, `MergeConfigurationsService`
- **Knowledge Sharing**: Martin Zajic - branchovana storage (2024-11-28)
