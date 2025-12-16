"""Response models for API endpoints."""

from datetime import datetime
from typing import Any

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
