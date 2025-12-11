# Storage API Endpointy relevantni pro DuckDB Driver

> Extrahovano z keboola.apib

## Endpointy ktere RESI DRIVER

### Buckets
```
POST   /buckets                              # Create Bucket
GET    /buckets                              # List all buckets
GET    /buckets/{bucket_id}                  # Bucket Detail
PUT    /buckets/{bucket_id}                  # Bucket Update
DELETE /buckets/{bucket_id}                  # Drop Bucket
POST   /buckets (async, link)                # Link Shared Bucket
POST   /buckets/{bucket_id}/share-*          # Share Bucket (organization, projects, users)
PUT    /buckets/{bucket_id}/share            # Change Bucket Sharing
DELETE /buckets/{bucket_id}/share            # Stop Bucket Sharing
DELETE /buckets/{bucket_id}/links/{proj}     # Force unlink bucket
```

### Tables
```
POST   /buckets/{bucket_id}/tables           # Create table from CSV
POST   /buckets/{bucket_id}/tables-async     # Create table async (CSV, snapshot, workspace, time-travel)
POST   /buckets/{bucket_id}/tables-definition # Create table definition (schema only)
GET    /buckets/{bucket_id}/tables           # Tables in bucket
GET    /tables/{table_id}                    # Table detail
PUT    /tables/{table_id}                    # Table update
DELETE /tables/{table_id}                    # Drop table
POST   /tables/{table_id}/import-async       # Import data (CSV, workspace)
POST   /tables/{table_id}/export-async       # Export data
GET    /tables/{table_id}/data-preview       # Data preview
POST   /tables/{table_id}/columns            # Add Column
DELETE /tables/{table_id}/columns/{name}     # Delete Column
PUT    /tables/{table_id}/columns/{name}/definition  # Update Column Definition
POST   /tables/{table_id}/primary-key        # Create Primary Key
DELETE /tables/{table_id}/primary-key        # Remove Primary Key
DELETE /tables/{table_id}/rows               # Delete Table Rows
POST   /tables/{table_id}/profile            # Create profile
GET    /tables/{table_id}/profile/latest     # Get profile
POST   /tables/{table_id}/optimize           # Optimize table
POST   /branch/{branch_id}/tables/{id}/pull  # Pull table from default to dev branch
```

### Table Aliases
```
POST   /buckets/{bucket_id}/table-aliases    # Create alias table
POST   /tables/{table_id}/alias-filter       # Update Alias Filter
DELETE /tables/{table_id}/alias-filter       # Remove Alias Filter
POST   /tables/{table_id}/alias-columns-auto-sync    # Enable Column Sync
DELETE /tables/{table_id}/alias-columns-auto-sync    # Disable Column Sync
```

### Snapshots
```
POST   /tables/{table_id}/snapshots          # Create Table Snapshot
GET    /tables/{table_id}/snapshots          # List Table Snapshots
GET    /snapshots/{snapshot_id}              # Snapshot Detail
DELETE /snapshots/{snapshot_id}              # Delete Table Snapshot
POST   /buckets/{id}/tables-async (snapshot) # Create table from snapshot
```

### Workspaces
```
POST   /workspaces                           # Create Workspace
POST   /branch/{branch_id}/workspaces        # Create Dev Branch Workspace
GET    /workspaces                           # List Workspaces
GET    /workspaces/{workspace_id}            # Workspace Detail
DELETE /workspaces/{workspace_id}            # Delete Workspace
POST   /workspaces/{workspace_id}/load       # Load Data into workspace
POST   /workspaces/{workspace_id}/password   # Password Reset
POST   /workspaces/{workspace_id}/public-key # Set public-key
POST   /branch/{id}/workspaces/{id}/query    # Execute Query
POST   /workspaces/{id}/credentials          # Create credentials
DELETE /workspaces/{id}/credentials/{id}     # Delete credentials
POST   /workspaces/{workspace_id}/unload     # Unload workspace data
```

### Files (pouze cteni pro import)
```
POST   /files/prepare                        # Create File Resource (staging)
PUT    /files/{file_id}/refresh              # Refresh File Credentials
GET    /files/{file_id}                      # File detail
```

---

## Endpointy ktere RESI CONNECTION (ne driver)

### Tokens & Permissions
- CRUD tokenu, verifikace, refresh

### Events
- Audit log operaci

### Metadata
- Bucket/table/column metadata

### Jobs
- Async job management

### Components & Configurations
- Component configs, rows, versions

### Dev Branches (lifecycle)
- Create/delete/update branch
- Merge requests

### Files (metadata)
- List, tags, delete (metadata only)

---

## Mapovani na BigQuery Driver Commands

| API Endpoint | Driver Command |
|--------------|----------------|
| POST /buckets | CreateBucketCommand |
| DELETE /buckets/{id} | DropBucketCommand |
| POST /buckets/{id}/share-* | ShareBucketCommand |
| DELETE /buckets/{id}/share | UnshareBucketCommand |
| POST /buckets (link) | LinkBucketCommand |
| DELETE /buckets/{id}/links | UnlinkBucketCommand |
| POST /tables-definition | CreateTableCommand |
| DELETE /tables/{id} | DropTableCommand |
| POST /tables/{id}/import-async | TableImportFromFileCommand |
| POST /tables/{id}/import-async (workspace) | TableImportFromTableCommand |
| POST /tables/{id}/export-async | TableExportToFileCommand |
| GET /tables/{id}/data-preview | PreviewTableCommand |
| POST /tables/{id}/columns | AddColumnCommand |
| DELETE /tables/{id}/columns | DropColumnCommand |
| PUT /tables/{id}/columns/{name}/definition | AlterColumnCommand |
| POST /tables/{id}/primary-key | AddPrimaryKeyCommand |
| DELETE /tables/{id}/primary-key | DropPrimaryKeyCommand |
| DELETE /tables/{id}/rows | DeleteTableRowsCommand |
| POST /tables/{id}/profile | CreateProfileTableCommand |
| POST /tables-async (time-travel) | CreateTableFromTimeTravelCommand |
| POST /workspaces | CreateWorkspaceCommand |
| DELETE /workspaces/{id} | DropWorkspaceCommand |
| POST /workspaces/{id}/load | (handled in Connection) |
| POST /workspaces/{id}/password | ResetWorkspacePasswordCommand |
| DELETE /workspaces (clear) | ClearWorkspaceCommand |
| POST /workspaces/{id}/query | ExecuteQueryCommand |
| GET /tables/{id} (info) | ObjectInfoCommand |
| POST /backend/init | InitBackendCommand |
| POST /backend/remove | RemoveBackendCommand |
