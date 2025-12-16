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
