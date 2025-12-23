"""Response models for API endpoints."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(description="Service status: 'healthy' or 'unhealthy'")
    version: str = Field(description="API version")
    storage_available: bool = Field(description="Whether storage paths are accessible")
    details: dict[str, bool] | None = Field(
        default=None, description="Detailed status of each storage path"
    )


class InitBackendResponse(BaseModel):
    """Response for backend initialization."""

    success: bool = Field(description="Whether initialization was successful")
    message: str = Field(description="Status message")
    storage_paths: dict[str, str] | None = Field(
        default=None, description="Configured storage paths"
    )


class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str = Field(description="Error type")
    message: str = Field(description="Error message")
    details: dict | None = Field(default=None, description="Additional error details")


# ============================================
# Project models
# ============================================


class ProjectCreate(BaseModel):
    """Request to create a new project."""

    id: str = Field(description="Unique project identifier (from Keboola Connection)")
    name: str | None = Field(default=None, description="Human-readable project name")
    settings: dict[str, Any] | None = Field(
        default=None, description="Project-specific settings"
    )


class ProjectUpdate(BaseModel):
    """Request to update a project."""

    name: str | None = Field(default=None, description="Updated project name")
    settings: dict[str, Any] | None = Field(
        default=None, description="Updated project settings"
    )


class ProjectResponse(BaseModel):
    """Project information response."""

    id: str = Field(description="Project identifier")
    name: str | None = Field(default=None, description="Project name")
    db_path: str = Field(description="Relative path to DuckDB file")
    created_at: str | None = Field(default=None, description="Creation timestamp (ISO)")
    updated_at: str | None = Field(default=None, description="Last update timestamp (ISO)")
    size_bytes: int = Field(default=0, description="Database file size in bytes")
    table_count: int = Field(default=0, description="Number of tables")
    bucket_count: int = Field(default=0, description="Number of buckets/schemas")
    status: str = Field(default="active", description="Project status")
    settings: dict[str, Any] | None = Field(default=None, description="Project settings")


class ProjectCreateResponse(BaseModel):
    """Response for project creation - includes API key (shown only once)."""

    id: str = Field(description="Project identifier")
    name: str | None = Field(default=None, description="Project name")
    db_path: str = Field(description="Relative path to DuckDB file")
    created_at: str | None = Field(default=None, description="Creation timestamp (ISO)")
    updated_at: str | None = Field(default=None, description="Last update timestamp (ISO)")
    size_bytes: int = Field(default=0, description="Database file size in bytes")
    table_count: int = Field(default=0, description="Number of tables")
    bucket_count: int = Field(default=0, description="Number of buckets/schemas")
    status: str = Field(default="active", description="Project status")
    settings: dict[str, Any] | None = Field(default=None, description="Project settings")
    api_key: str = Field(
        description="Project admin API key - SAVE THIS! It will not be shown again."
    )


class ProjectListResponse(BaseModel):
    """List of projects response."""

    projects: list[ProjectResponse] = Field(description="List of projects")
    total: int = Field(description="Total number of projects matching filter")


class ProjectStatsResponse(BaseModel):
    """Project statistics response."""

    id: str = Field(description="Project identifier")
    size_bytes: int = Field(description="Database file size in bytes")
    table_count: int = Field(description="Number of tables")
    bucket_count: int = Field(description="Number of buckets/schemas")


# ============================================
# Bucket models
# ============================================


class BucketCreate(BaseModel):
    """Request to create a new bucket."""

    name: str = Field(description="Bucket name (will be used as DuckDB schema name)")
    description: str | None = Field(default=None, description="Bucket description")


class BucketResponse(BaseModel):
    """Bucket information response."""

    name: str = Field(description="Bucket name (schema name)")
    table_count: int = Field(default=0, description="Number of tables in bucket")
    description: str | None = Field(default=None, description="Bucket description")
    is_linked: bool = Field(default=False, description="True if bucket is linked from another project")
    source_project_id: str | None = Field(default=None, description="Source project ID if linked")
    source_bucket_name: str | None = Field(default=None, description="Source bucket name if linked")


class BucketListResponse(BaseModel):
    """List of buckets response."""

    buckets: list[BucketResponse] = Field(description="List of buckets")
    total: int = Field(description="Total number of buckets")


class BucketShareRequest(BaseModel):
    """Request to share a bucket."""

    target_project_id: str = Field(description="ID of project to share with")


class BucketLinkRequest(BaseModel):
    """Request to link a bucket from another project."""

    source_project_id: str = Field(description="ID of source project")
    source_bucket_name: str = Field(description="Name of bucket in source project")


class BucketShareInfo(BaseModel):
    """Information about a shared bucket."""

    shared_with: list[str] = Field(
        default_factory=list, description="List of project IDs this bucket is shared with"
    )
    is_linked: bool = Field(default=False, description="Whether this is a linked bucket")
    source_project_id: str | None = Field(
        default=None, description="Source project ID if linked"
    )
    source_bucket_name: str | None = Field(
        default=None, description="Source bucket name if linked"
    )


# ============================================
# Table models
# ============================================


class ColumnDefinition(BaseModel):
    """Column definition for table creation."""

    name: str = Field(description="Column name")
    type: str = Field(
        description="DuckDB data type (VARCHAR, INTEGER, DOUBLE, BOOLEAN, TIMESTAMP, etc.)"
    )
    nullable: bool = Field(default=True, description="Whether column allows NULL values")
    default: str | None = Field(default=None, description="Default value expression")


class ColumnInfo(BaseModel):
    """Column information from existing table."""

    name: str = Field(description="Column name")
    type: str = Field(description="DuckDB data type")
    nullable: bool = Field(description="Whether column allows NULL values")
    ordinal_position: int = Field(description="Column position (1-based)")


class TableCreate(BaseModel):
    """Request to create a new table."""

    name: str = Field(description="Table name")
    columns: list[ColumnDefinition] = Field(
        description="List of column definitions", min_length=1
    )
    primary_key: list[str] | None = Field(
        default=None, description="List of column names forming the primary key"
    )


class TableResponse(BaseModel):
    """Table information response (ObjectInfo)."""

    name: str = Field(description="Table name")
    bucket: str = Field(description="Bucket (schema) name")
    columns: list[ColumnInfo] = Field(description="List of columns")
    row_count: int = Field(description="Number of rows in table")
    size_bytes: int = Field(default=0, description="Estimated table size in bytes")
    primary_key: list[str] = Field(
        default_factory=list, description="Primary key column names"
    )
    created_at: str | None = Field(default=None, description="Creation timestamp (ISO)")
    source: Literal["main", "branch"] = Field(
        default="main",
        description="Data source: 'main' for production, 'branch' for CoW copy"
    )


class TableListResponse(BaseModel):
    """List of tables response."""

    tables: list[TableResponse] = Field(description="List of tables")
    total: int = Field(description="Total number of tables")


class TablePreviewResponse(BaseModel):
    """Table preview response."""

    columns: list[ColumnInfo] = Field(description="Column information")
    rows: list[dict[str, Any]] = Field(description="Row data as list of dictionaries")
    total_row_count: int = Field(description="Total rows in table")
    preview_row_count: int = Field(description="Number of rows in preview")


# ============================================
# Table Schema Operations models
# ============================================


class AddColumnRequest(BaseModel):
    """Request to add a column to a table."""

    name: str = Field(description="Column name")
    type: str = Field(
        description="DuckDB data type (VARCHAR, INTEGER, DOUBLE, BOOLEAN, TIMESTAMP, etc.)"
    )
    nullable: bool = Field(default=True, description="Whether column allows NULL values")
    default: str | None = Field(default=None, description="Default value expression")


class AlterColumnRequest(BaseModel):
    """Request to alter a column in a table."""

    new_name: str | None = Field(default=None, description="New column name (for rename)")
    new_type: str | None = Field(default=None, description="New data type (for type change)")
    set_not_null: bool | None = Field(
        default=None, description="Set NOT NULL constraint (True) or DROP NOT NULL (False)"
    )
    set_default: str | None = Field(
        default=None, description="New default value expression (use empty string to drop)"
    )


class SetPrimaryKeyRequest(BaseModel):
    """Request to set primary key on a table."""

    columns: list[str] = Field(
        description="List of column names to form the primary key", min_length=1
    )


class DeleteRowsRequest(BaseModel):
    """Request to delete rows from a table."""

    where_clause: str = Field(
        description="SQL WHERE clause condition (without 'WHERE' keyword). "
        "Example: \"status = 'deleted'\" or \"created_at < '2024-01-01'\""
    )


class DeleteRowsResponse(BaseModel):
    """Response for delete rows operation."""

    deleted_rows: int = Field(description="Number of rows deleted")
    table_rows_after: int = Field(description="Total rows remaining in table")


class ColumnStatistics(BaseModel):
    """Statistics for a single column from SUMMARIZE."""

    column_name: str = Field(description="Column name")
    column_type: str = Field(description="Column data type")
    min: Any | None = Field(default=None, description="Minimum value")
    max: Any | None = Field(default=None, description="Maximum value")
    approx_unique: int | None = Field(default=None, description="Approximate unique count")
    avg: float | None = Field(default=None, description="Average value (numeric columns)")
    std: float | None = Field(default=None, description="Standard deviation (numeric columns)")
    q25: Any | None = Field(default=None, description="25th percentile")
    q50: Any | None = Field(default=None, description="50th percentile (median)")
    q75: Any | None = Field(default=None, description="75th percentile")
    count: int | None = Field(default=None, description="Non-null count")
    null_percentage: float | None = Field(default=None, description="Percentage of null values")


class TableProfileResponse(BaseModel):
    """Response for table profiling (SUMMARIZE)."""

    table_name: str = Field(description="Table name")
    bucket_name: str = Field(description="Bucket name")
    row_count: int = Field(description="Total rows in table")
    column_count: int = Field(description="Number of columns")
    statistics: list[ColumnStatistics] = Field(description="Per-column statistics")


# ============================================
# Files API models
# ============================================


class FilePrepareRequest(BaseModel):
    """Request to prepare a file upload."""

    filename: str = Field(description="Original filename")
    content_type: str | None = Field(
        default=None, description="MIME type (e.g., 'text/csv', 'application/x-parquet')"
    )
    size_bytes: int | None = Field(
        default=None, description="Expected file size in bytes (optional, for validation)"
    )
    tags: dict[str, str] | None = Field(
        default=None, description="Optional tags/metadata for the file"
    )


class FilePrepareResponse(BaseModel):
    """Response for file upload preparation."""

    upload_key: str = Field(description="Unique key for the upload session")
    upload_url: str = Field(description="URL to upload the file to")
    expires_at: str = Field(description="When the upload session expires (ISO timestamp)")


class FileRegisterRequest(BaseModel):
    """Request to register an uploaded file."""

    upload_key: str = Field(description="The upload key from prepare step")
    name: str | None = Field(
        default=None, description="Final filename (defaults to original filename)"
    )
    tags: dict[str, str] | None = Field(
        default=None, description="Optional tags/metadata for the file"
    )


class FileResponse(BaseModel):
    """File information response."""

    id: str = Field(description="Unique file identifier")
    project_id: str = Field(description="Project the file belongs to")
    name: str = Field(description="Filename")
    path: str = Field(description="Relative path in storage")
    size_bytes: int = Field(description="File size in bytes")
    content_type: str | None = Field(default=None, description="MIME type")
    checksum_sha256: str | None = Field(default=None, description="SHA256 checksum")
    is_staged: bool = Field(description="Whether file is still in staging")
    created_at: str = Field(description="Creation timestamp (ISO)")
    expires_at: str | None = Field(
        default=None, description="Expiration timestamp for staging files (ISO)"
    )
    tags: dict[str, str] | None = Field(default=None, description="File tags/metadata")


class FileListResponse(BaseModel):
    """List of files response."""

    files: list[FileResponse] = Field(description="List of files")
    total: int = Field(description="Total number of files")


class FileUploadResponse(BaseModel):
    """Response after successful file upload."""

    upload_key: str = Field(description="Upload key for registration")
    staging_path: str = Field(description="Path in staging area")
    size_bytes: int = Field(description="Uploaded file size")
    checksum_sha256: str = Field(description="SHA256 checksum of uploaded file")


# ============================================
# Import/Export API models
# ============================================


class CsvOptions(BaseModel):
    """CSV format options for import/export."""

    delimiter: str = Field(default=",", description="Field delimiter character")
    quote: str = Field(default='"', description="Quote character")
    escape: str = Field(default="\\", description="Escape character")
    header: bool = Field(default=True, description="Whether CSV has header row")
    null_string: str = Field(default="", description="String representing NULL values")


class ImportOptions(BaseModel):
    """Options for table import."""

    incremental: bool = Field(
        default=False,
        description="If False, truncate table before import. If True, merge/upsert."
    )
    dedup_mode: str = Field(
        default="update_duplicates",
        description="How to handle duplicates: 'update_duplicates', 'insert_duplicates', 'fail_on_duplicates'"
    )
    columns: list[str] | None = Field(
        default=None,
        description="Specific columns to import (None = all columns)"
    )


class ImportFromFileRequest(BaseModel):
    """Request to import data from a file into a table."""

    file_id: str = Field(description="ID of file to import (from Files API)")
    format: str = Field(
        default="csv",
        description="File format: 'csv' or 'parquet'"
    )
    csv_options: CsvOptions | None = Field(
        default=None, description="CSV-specific options (only for CSV format)"
    )
    import_options: ImportOptions = Field(
        default_factory=ImportOptions, description="Import behavior options"
    )


class ImportResponse(BaseModel):
    """Response for import operation."""

    imported_rows: int = Field(description="Number of rows imported")
    table_rows_after: int = Field(description="Total rows in table after import")
    table_size_bytes: int = Field(description="Table size after import")
    warnings: list[str] = Field(default_factory=list, description="Any warnings during import")


class ExportRequest(BaseModel):
    """Request to export table data to a file."""

    format: str = Field(
        default="csv",
        description="Export format: 'csv' or 'parquet'"
    )
    compression: str | None = Field(
        default=None,
        description="Compression: 'gzip' for CSV, 'gzip'/'zstd'/'snappy' for Parquet"
    )
    columns: list[str] | None = Field(
        default=None,
        description="Specific columns to export (None = all columns)"
    )
    where_filter: str | None = Field(
        default=None,
        description="SQL WHERE clause to filter rows (without 'WHERE' keyword)"
    )
    limit: int | None = Field(
        default=None,
        description="Maximum number of rows to export"
    )


class ExportResponse(BaseModel):
    """Response for export operation."""

    file_id: str = Field(description="ID of the exported file (use Files API to download)")
    file_path: str = Field(description="Relative path to exported file")
    rows_exported: int = Field(description="Number of rows exported")
    file_size_bytes: int = Field(description="Size of exported file")


# ============================================
# Snapshot Settings API models (ADR-004)
# ============================================


class SnapshotTriggersConfig(BaseModel):
    """Configuration for automatic snapshot triggers."""

    model_config = {"extra": "forbid"}

    drop_table: bool | None = Field(
        default=None, description="Create snapshot before DROP TABLE"
    )
    truncate_table: bool | None = Field(
        default=None, description="Create snapshot before TRUNCATE"
    )
    delete_all_rows: bool | None = Field(
        default=None, description="Create snapshot before DELETE FROM without WHERE"
    )
    drop_column: bool | None = Field(
        default=None, description="Create snapshot before ALTER TABLE DROP COLUMN"
    )


class SnapshotRetentionConfig(BaseModel):
    """Configuration for snapshot retention periods."""

    manual_days: int | None = Field(
        default=None, description="Retention period for manual snapshots (days)"
    )
    auto_days: int | None = Field(
        default=None, description="Retention period for automatic snapshots (days)"
    )


class SnapshotConfigRequest(BaseModel):
    """Request to update snapshot configuration."""

    auto_snapshot_triggers: SnapshotTriggersConfig | None = Field(
        default=None, description="Automatic snapshot trigger settings"
    )
    retention: SnapshotRetentionConfig | None = Field(
        default=None, description="Retention period settings"
    )
    enabled: bool | None = Field(
        default=None, description="Master switch to enable/disable snapshots"
    )


class SnapshotSettingsResponse(BaseModel):
    """Response with effective snapshot settings and inheritance info."""

    effective_config: dict[str, Any] = Field(
        description="Merged configuration after applying inheritance"
    )
    inheritance: dict[str, str] = Field(
        description="Source of each config value ('system', 'project', 'bucket', 'table')"
    )
    local_config: dict[str, Any] | None = Field(
        default=None, description="Only the locally set configuration (without inheritance)"
    )


# ============================================
# Snapshots API models (ADR-004)
# ============================================


class SnapshotCreateRequest(BaseModel):
    """Request to create a manual snapshot."""

    bucket: str = Field(description="Bucket containing the table")
    table: str = Field(description="Table name to snapshot")
    description: str | None = Field(default=None, description="Optional description")


class SnapshotRestoreRequest(BaseModel):
    """Request to restore from a snapshot."""

    target_bucket: str | None = Field(
        default=None, description="Target bucket (defaults to original)"
    )
    target_table: str | None = Field(
        default=None, description="Target table name (defaults to original)"
    )


class SnapshotSchemaColumn(BaseModel):
    """Column definition in snapshot schema."""

    name: str = Field(description="Column name")
    type: str = Field(description="Column data type")
    nullable: bool = Field(description="Whether column allows NULL")


class SnapshotResponse(BaseModel):
    """Snapshot information response."""

    id: str = Field(description="Snapshot identifier")
    project_id: str = Field(description="Project ID")
    bucket_name: str = Field(description="Source bucket name")
    table_name: str = Field(description="Source table name")
    snapshot_type: str = Field(description="'manual' or 'auto_*' type")
    row_count: int = Field(description="Number of rows in snapshot")
    size_bytes: int = Field(description="Parquet file size")
    created_at: str = Field(description="Creation timestamp (ISO)")
    created_by: str | None = Field(default=None, description="Who created the snapshot")
    expires_at: str | None = Field(default=None, description="Expiration timestamp (ISO)")
    description: str | None = Field(default=None, description="Optional description")


class SnapshotDetailResponse(SnapshotResponse):
    """Detailed snapshot information including schema."""

    schema_columns: list[SnapshotSchemaColumn] = Field(
        description="Column definitions for restore"
    )
    primary_key: list[str] = Field(
        default_factory=list, description="Primary key columns"
    )


class SnapshotListResponse(BaseModel):
    """List of snapshots response."""

    snapshots: list[SnapshotResponse] = Field(description="List of snapshots")
    total: int = Field(description="Total number of snapshots matching filter")


class SnapshotRestoreResponse(BaseModel):
    """Response for snapshot restore operation."""

    restored_to: dict[str, str] = Field(
        description="Target location {'bucket': '...', 'table': '...'}"
    )
    row_count: int = Field(description="Number of rows restored")


# ============================================
# Branch Models (ADR-007: CoW branching)
# ============================================


class BranchCreateRequest(BaseModel):
    """Request to create a dev branch."""

    name: str = Field(..., description="Branch name")
    description: str | None = Field(None, description="Optional description")


class BranchResponse(BaseModel):
    """Response for a single branch."""

    id: str
    project_id: str
    name: str
    created_at: str | None
    created_by: str | None
    description: str | None
    table_count: int = Field(default=0, description="Number of tables copied to branch")
    size_bytes: int = Field(default=0, description="Total size of branch tables")


class BranchDetailResponse(BranchResponse):
    """Detailed branch response including copied tables list."""

    copied_tables: list[dict] = Field(
        default_factory=list,
        description="List of tables that have been copied to branch (CoW triggered)"
    )


class BranchListResponse(BaseModel):
    """Response for listing branches."""

    branches: list[BranchResponse]
    count: int


class BranchTableInfo(BaseModel):
    """Information about a table in a branch context."""

    bucket_name: str
    table_name: str
    is_local: bool = Field(description="True if table has been copied to branch (CoW)")
    copied_at: str | None = Field(None, description="When table was copied to branch")


class PullTableRequest(BaseModel):
    """Request to pull (refresh) a table from main - restores live view."""

    pass  # No parameters needed, table path is in URL


class PullTableResponse(BaseModel):
    """Response for pull table operation."""

    bucket_name: str
    table_name: str
    message: str = Field(description="Confirmation message")
    was_local: bool = Field(description="Whether table was in branch before pull")


# ============================================
# Workspace Models
# ============================================


class WorkspaceCreateRequest(BaseModel):
    """Request to create a new workspace."""

    name: str = Field(..., description="Workspace name")
    ttl_hours: int | None = Field(default=None, ge=1, le=168, description="Time to live in hours (1-168), None for no expiration")
    size_limit_gb: int = Field(default=10, ge=1, le=100, description="Size limit in GB (1-100)")
    preload_tables: list[str] = Field(default_factory=list, description="Tables to preload (format: bucket.table)")


class WorkspaceConnectionInfo(BaseModel):
    """Connection information for workspace."""

    host: str = Field(..., description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Username")
    password: str | None = Field(None, description="Password (only shown on create)")
    ssl_mode: str = Field(default="prefer", description="SSL mode")
    connection_string: str | None = Field(None, description="Full connection string")


class WorkspaceTableInfo(BaseModel):
    """Information about an attached table."""

    schema_name: str = Field(..., alias="schema", description="Schema/bucket name")
    table: str = Field(..., description="Table name")
    rows: int = Field(default=0, description="Row count")


class WorkspaceObjectInfo(BaseModel):
    """Information about workspace object."""

    name: str = Field(..., description="Object name")
    type: str = Field(default="table", description="Object type (table, view)")
    rows: int = Field(default=0, description="Row count")


class WorkspaceResponse(BaseModel):
    """Response for workspace operations."""

    id: str = Field(..., description="Workspace ID")
    name: str = Field(..., description="Workspace name")
    project_id: str = Field(..., description="Project ID")
    branch_id: str | None = Field(None, description="Branch ID (null for main)")
    created_at: str | None = Field(None, description="Creation timestamp")
    expires_at: str | None = Field(None, description="Expiration timestamp")
    size_bytes: int = Field(default=0, description="Current size in bytes")
    size_limit_gb: int = Field(default=10, description="Size limit in GB")
    status: str = Field(default="active", description="Status: active, expired, error")
    connection: WorkspaceConnectionInfo | None = Field(None, description="Connection info")


class WorkspaceDetailResponse(WorkspaceResponse):
    """Detailed workspace response including objects."""

    active_sessions: int = Field(default=0, description="Active session count")
    attached_tables: list[WorkspaceTableInfo] = Field(default_factory=list, description="Attached project tables")
    workspace_objects: list[WorkspaceObjectInfo] = Field(default_factory=list, description="Objects in workspace")


class WorkspaceListResponse(BaseModel):
    """Response for listing workspaces."""

    workspaces: list[WorkspaceResponse] = Field(default_factory=list)
    count: int = Field(default=0, description="Total count")


class WorkspaceLoadRequest(BaseModel):
    """Request to load data into workspace."""

    tables: list[dict] = Field(..., description="Tables to load with source, destination, columns, where")


class WorkspaceLoadTableResult(BaseModel):
    """Result of loading a table."""

    source: str = Field(..., description="Source table")
    destination: str = Field(..., description="Destination table name")
    rows: int = Field(default=0, description="Rows loaded")
    size_bytes: int = Field(default=0, description="Size in bytes")


class WorkspaceLoadResponse(BaseModel):
    """Response for load operation."""

    loaded: list[WorkspaceLoadTableResult] = Field(default_factory=list)
    workspace_size_bytes: int = Field(default=0, description="Total workspace size")


# ============================================
# PG Wire Auth Models (Phase 11b)
# ============================================


class PGWireAuthRequest(BaseModel):
    """Request from PG Wire server to authenticate a user."""

    username: str = Field(..., description="Workspace username (ws_xxx_xxx)")
    password: str = Field(..., description="Workspace password")
    client_ip: str | None = Field(None, description="Client IP address")


class PGWireTableInfo(BaseModel):
    """Information about a table to attach."""

    bucket: str = Field(..., description="Bucket name")
    name: str = Field(..., description="Table name")
    path: str = Field(..., description="Path to DuckDB file")
    rows: int = Field(default=0, description="Row count")


class PGWireAuthResponse(BaseModel):
    """Response with workspace info for session initialization."""

    workspace_id: str = Field(..., description="Workspace ID")
    project_id: str = Field(..., description="Project ID")
    branch_id: str | None = Field(None, description="Branch ID (null for main)")
    db_path: str = Field(..., description="Path to workspace DuckDB file")
    tables: list[PGWireTableInfo] = Field(default_factory=list, description="Tables to ATTACH")
    memory_limit: str = Field(default="4GB", description="Memory limit for session")
    query_timeout_seconds: int = Field(default=300, description="Query timeout")


class PGWireSessionInfo(BaseModel):
    """Information about a PG Wire session."""

    session_id: str = Field(..., description="Session ID")
    workspace_id: str = Field(..., description="Workspace ID")
    client_ip: str | None = Field(None, description="Client IP")
    connected_at: str | None = Field(None, description="Connection timestamp")
    last_activity_at: str | None = Field(None, description="Last activity timestamp")
    query_count: int = Field(default=0, description="Number of queries executed")
    status: str = Field(default="active", description="Session status")


class PGWireSessionCreateRequest(BaseModel):
    """Request to create/register a session."""

    session_id: str = Field(..., description="Session ID from PG Wire server")
    workspace_id: str = Field(..., description="Workspace ID")
    client_ip: str | None = Field(None, description="Client IP address")


class PGWireSessionUpdateRequest(BaseModel):
    """Request to update session activity."""

    increment_queries: bool = Field(default=True, description="Increment query count")


# ============================================
# API Key models
# ============================================


class ApiKeyCreateRequest(BaseModel):
    """Request to create a new API key."""

    description: str = Field(..., description="Description of the API key purpose")
    branch_id: str | None = Field(None, description="Branch ID for branch-scoped keys")
    scope: Literal["project_admin", "branch_admin", "branch_read"] = Field(
        "project_admin",
        description="Key scope: project_admin (default), branch_admin, or branch_read",
    )
    expires_in_days: int | None = Field(
        None, ge=1, le=365, description="Expiration in days (1-365, optional)"
    )


class ApiKeyResponse(BaseModel):
    """API key information response (without raw key)."""

    id: str = Field(..., description="API key identifier")
    project_id: str = Field(..., description="Project ID")
    branch_id: str | None = Field(None, description="Branch ID (null for project-wide keys)")
    key_prefix: str = Field(..., description="Key prefix for identification")
    scope: str = Field(..., description="Key scope (project_admin, branch_admin, branch_read)")
    description: str | None = Field(None, description="Key description")
    created_at: str | None = Field(None, description="Creation timestamp (ISO)")
    last_used_at: str | None = Field(None, description="Last usage timestamp (ISO)")
    expires_at: str | None = Field(None, description="Expiration timestamp (ISO)")
    is_revoked: bool = Field(False, description="Whether the key is revoked")


class ApiKeyCreateResponse(BaseModel):
    """Response for API key creation - includes raw key (shown only once)."""

    id: str = Field(..., description="API key identifier")
    project_id: str = Field(..., description="Project ID")
    branch_id: str | None = Field(None, description="Branch ID (null for project-wide keys)")
    key_prefix: str = Field(..., description="Key prefix for identification")
    scope: str = Field(..., description="Key scope (project_admin, branch_admin, branch_read)")
    description: str | None = Field(None, description="Key description")
    created_at: str | None = Field(None, description="Creation timestamp (ISO)")
    expires_at: str | None = Field(None, description="Expiration timestamp (ISO)")
    api_key: str = Field(..., description="Full API key - SAVE THIS! It will not be shown again.")


class ApiKeyListResponse(BaseModel):
    """List of API keys response."""

    api_keys: list[ApiKeyResponse] = Field(..., description="List of API keys")
    count: int = Field(..., description="Total number of API keys")
