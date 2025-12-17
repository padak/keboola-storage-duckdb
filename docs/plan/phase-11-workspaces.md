# Phase 11: Workspaces - TODO

## Status
- **Status:** Not Started
- **Specification:** 30%

## Goal

Implement isolated workspaces for data transformation.

## Endpoints to Implement

```
POST   /projects/{id}/workspaces                   # CreateWorkspace
GET    /projects/{id}/workspaces                   # ListWorkspaces
GET    /projects/{id}/workspaces/{ws_id}           # WorkspaceDetail
DELETE /projects/{id}/workspaces/{ws_id}           # DropWorkspace
POST   /projects/{id}/workspaces/{ws_id}/clear     # ClearWorkspace
DELETE /projects/{id}/workspaces/{ws_id}/objects/{name} # DropWorkspaceObject
POST   /projects/{id}/workspaces/{ws_id}/load      # LoadDataToWorkspace
POST   /projects/{id}/workspaces/{ws_id}/query     # ExecuteInWorkspace

# Dev branch workspaces
POST   /projects/{id}/branches/{branch_id}/workspaces           # CreateBranchWorkspace
POST   /projects/{id}/branches/{branch_id}/workspaces/{ws}/query# QueryInBranchWorkspace
```

## Implementation Strategy

With ADR-009, workspaces are separate .duckdb files:

```
/data/duckdb/
├── project_123/
│   ├── in_c_sales/
│   │   └── orders.duckdb
│   └── _workspaces/                # Workspace directory
│       ├── ws_789.duckdb           # Workspace = isolated DB
│       └── ws_790.duckdb
```

### CreateWorkspace
1. Create workspace .duckdb file in `_workspaces/`
2. Register in metadata.duckdb
3. Return connection credentials

### LoadDataToWorkspace
1. ATTACH source table
2. CREATE TABLE AS SELECT into workspace
3. DETACH source

### ExecuteInWorkspace
1. Connect to workspace .duckdb
2. Execute SQL
3. Return results

## Metadata Schema

```sql
CREATE TABLE workspaces (
    id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    branch_id VARCHAR,              -- NULL for main branch
    name VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,         -- Optional TTL

    FOREIGN KEY (project_id) REFERENCES projects(id)
);
```

## Key Decisions (TBD)

| Decision | Options | Recommendation |
|----------|---------|----------------|
| Workspace isolation | Schema / File | **File** (per ADR-009) |
| TTL | None / 24h / 7d | **24h** default |
| Size limit | None / 10GB / 100GB | **10GB** per workspace |
