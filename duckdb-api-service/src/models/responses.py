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
