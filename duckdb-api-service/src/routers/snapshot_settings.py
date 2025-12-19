"""Snapshot settings endpoints: hierarchical configuration for snapshots (ADR-004, ADR-012).

ADR-012: Branch-First API
- Bucket/table settings use branch-first URL pattern
- Settings operations only allowed on default branch (MVP restriction)
- Project-level settings remain at /projects/{project_id}/settings/snapshots
"""

import structlog
from fastapi import APIRouter, Depends, HTTPException, status

from src.branch_utils import require_default_branch, resolve_branch, validate_project_and_bucket
from src.database import metadata_db, project_db_manager
from src.dependencies import require_project_access
from src.models.responses import (
    ErrorResponse,
    SnapshotConfigRequest,
    SnapshotSettingsResponse,
)
from src.snapshot_config import (
    get_entity_id,
    get_local_config,
    resolve_snapshot_config,
    validate_config,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["snapshot-settings"])


def _validate_project_exists(project_id: str) -> None:
    """Validate that project exists."""
    project = metadata_db.get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )


def _validate_table_exists(
    project_id: str, branch_id: str | None, bucket_name: str, table_name: str
) -> None:
    """Validate that table exists in the given branch context."""
    # Use branch_utils for project/bucket validation
    validate_project_and_bucket(project_id, branch_id, bucket_name)

    # Determine effective project for table check
    effective_project_id = (
        f"{project_id}_branch_{branch_id}" if branch_id else project_id
    )

    if not project_db_manager.table_exists(effective_project_id, bucket_name, table_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "table_not_found",
                "message": f"Table {table_name} not found in bucket {bucket_name}",
                "details": {
                    "project_id": project_id,
                    "branch_id": branch_id,
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            },
        )


def _request_to_config_dict(request: SnapshotConfigRequest) -> dict:
    """Convert Pydantic request to config dict, excluding None values."""
    config = {}

    if request.auto_snapshot_triggers is not None:
        triggers = {}
        triggers_model = request.auto_snapshot_triggers
        if triggers_model.drop_table is not None:
            triggers["drop_table"] = triggers_model.drop_table
        if triggers_model.truncate_table is not None:
            triggers["truncate_table"] = triggers_model.truncate_table
        if triggers_model.delete_all_rows is not None:
            triggers["delete_all_rows"] = triggers_model.delete_all_rows
        if triggers_model.drop_column is not None:
            triggers["drop_column"] = triggers_model.drop_column
        if triggers:
            config["auto_snapshot_triggers"] = triggers

    if request.retention is not None:
        retention = {}
        if request.retention.manual_days is not None:
            retention["manual_days"] = request.retention.manual_days
        if request.retention.auto_days is not None:
            retention["auto_days"] = request.retention.auto_days
        if retention:
            config["retention"] = retention

    if request.enabled is not None:
        config["enabled"] = request.enabled

    return config


# ============================================
# Project-level snapshot settings
# ============================================


@router.get(
    "/projects/{project_id}/settings/snapshots",
    response_model=SnapshotSettingsResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get project snapshot settings",
    description="Get effective snapshot settings for a project with inheritance info.",
    dependencies=[Depends(require_project_access)],
)
async def get_project_snapshot_settings(project_id: str) -> SnapshotSettingsResponse:
    """Get snapshot settings for a project."""
    _validate_project_exists(project_id)

    effective_config, inheritance = resolve_snapshot_config(project_id)
    local_config = get_local_config(project_id)

    logger.info(
        "get_project_snapshot_settings",
        project_id=project_id,
        has_local_config=local_config is not None,
    )

    return SnapshotSettingsResponse(
        effective_config=effective_config,
        inheritance=inheritance,
        local_config=local_config,
    )


@router.put(
    "/projects/{project_id}/settings/snapshots",
    response_model=SnapshotSettingsResponse,
    responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
    summary="Update project snapshot settings",
    description="Set snapshot configuration for a project (partial update).",
    dependencies=[Depends(require_project_access)],
)
async def update_project_snapshot_settings(
    project_id: str, request: SnapshotConfigRequest
) -> SnapshotSettingsResponse:
    """Update snapshot settings for a project."""
    _validate_project_exists(project_id)

    config = _request_to_config_dict(request)

    # Validate config
    errors = validate_config(config)
    if errors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_config",
                "message": "Invalid snapshot configuration",
                "details": {"errors": errors},
            },
        )

    # Save settings
    entity_type, entity_id = get_entity_id(project_id)
    metadata_db.set_snapshot_settings(entity_type, entity_id, project_id, config)

    logger.info(
        "update_project_snapshot_settings",
        project_id=project_id,
        config=config,
    )

    # Return updated settings
    effective_config, inheritance = resolve_snapshot_config(project_id)
    local_config = get_local_config(project_id)

    return SnapshotSettingsResponse(
        effective_config=effective_config,
        inheritance=inheritance,
        local_config=local_config,
    )


@router.delete(
    "/projects/{project_id}/settings/snapshots",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Reset project snapshot settings",
    description="Delete local project settings (inherit from system defaults).",
    dependencies=[Depends(require_project_access)],
)
async def delete_project_snapshot_settings(project_id: str) -> None:
    """Delete snapshot settings for a project (reset to system defaults)."""
    _validate_project_exists(project_id)

    entity_type, entity_id = get_entity_id(project_id)
    metadata_db.delete_snapshot_settings(entity_type, entity_id)

    logger.info("delete_project_snapshot_settings", project_id=project_id)


# ============================================
# Bucket-level snapshot settings
# ============================================


@router.get(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/settings/snapshots",
    response_model=SnapshotSettingsResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get bucket snapshot settings",
    description="Get effective snapshot settings for a bucket with inheritance info. Only allowed on default branch.",
    dependencies=[Depends(require_project_access)],
)
async def get_bucket_snapshot_settings(
    project_id: str, branch_id: str, bucket_name: str
) -> SnapshotSettingsResponse:
    """Get snapshot settings for a bucket."""
    # Resolve branch and ensure default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "manage snapshot settings")

    # Validate bucket exists
    validate_project_and_bucket(resolved_project_id, resolved_branch_id, bucket_name)

    effective_config, inheritance = resolve_snapshot_config(project_id, bucket_name)
    local_config = get_local_config(project_id, bucket_name)

    logger.info(
        "get_bucket_snapshot_settings",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        has_local_config=local_config is not None,
    )

    return SnapshotSettingsResponse(
        effective_config=effective_config,
        inheritance=inheritance,
        local_config=local_config,
    )


@router.put(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/settings/snapshots",
    response_model=SnapshotSettingsResponse,
    responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
    summary="Update bucket snapshot settings",
    description="Set snapshot configuration for a bucket (partial update). Only allowed on default branch.",
    dependencies=[Depends(require_project_access)],
)
async def update_bucket_snapshot_settings(
    project_id: str, branch_id: str, bucket_name: str, request: SnapshotConfigRequest
) -> SnapshotSettingsResponse:
    """Update snapshot settings for a bucket."""
    # Resolve branch and ensure default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "manage snapshot settings")

    # Validate bucket exists
    validate_project_and_bucket(resolved_project_id, resolved_branch_id, bucket_name)

    config = _request_to_config_dict(request)

    # Validate config
    errors = validate_config(config)
    if errors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_config",
                "message": "Invalid snapshot configuration",
                "details": {"errors": errors},
            },
        )

    # Save settings
    entity_type, entity_id = get_entity_id(project_id, bucket_name)
    metadata_db.set_snapshot_settings(entity_type, entity_id, project_id, config)

    logger.info(
        "update_bucket_snapshot_settings",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        config=config,
    )

    # Return updated settings
    effective_config, inheritance = resolve_snapshot_config(project_id, bucket_name)
    local_config = get_local_config(project_id, bucket_name)

    return SnapshotSettingsResponse(
        effective_config=effective_config,
        inheritance=inheritance,
        local_config=local_config,
    )


@router.delete(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/settings/snapshots",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Reset bucket snapshot settings",
    description="Delete local bucket settings (inherit from project). Only allowed on default branch.",
    dependencies=[Depends(require_project_access)],
)
async def delete_bucket_snapshot_settings(
    project_id: str, branch_id: str, bucket_name: str
) -> None:
    """Delete snapshot settings for a bucket (inherit from project)."""
    # Resolve branch and ensure default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "manage snapshot settings")

    # Validate bucket exists
    validate_project_and_bucket(resolved_project_id, resolved_branch_id, bucket_name)

    entity_type, entity_id = get_entity_id(project_id, bucket_name)
    metadata_db.delete_snapshot_settings(entity_type, entity_id)

    logger.info(
        "delete_bucket_snapshot_settings",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
    )


# ============================================
# Table-level snapshot settings
# ============================================


@router.get(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots",
    response_model=SnapshotSettingsResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Get table snapshot settings",
    description="Get effective snapshot settings for a table with inheritance info. Only allowed on default branch.",
    dependencies=[Depends(require_project_access)],
)
async def get_table_snapshot_settings(
    project_id: str, branch_id: str, bucket_name: str, table_name: str
) -> SnapshotSettingsResponse:
    """Get snapshot settings for a table."""
    # Resolve branch and ensure default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "manage snapshot settings")

    # Validate table exists
    _validate_table_exists(resolved_project_id, resolved_branch_id, bucket_name, table_name)

    effective_config, inheritance = resolve_snapshot_config(
        project_id, bucket_name, table_name
    )
    local_config = get_local_config(project_id, bucket_name, table_name)

    logger.info(
        "get_table_snapshot_settings",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table_name,
        has_local_config=local_config is not None,
    )

    return SnapshotSettingsResponse(
        effective_config=effective_config,
        inheritance=inheritance,
        local_config=local_config,
    )


@router.put(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots",
    response_model=SnapshotSettingsResponse,
    responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
    summary="Update table snapshot settings",
    description="Set snapshot configuration for a table (partial update). Only allowed on default branch.",
    dependencies=[Depends(require_project_access)],
)
async def update_table_snapshot_settings(
    project_id: str,
    branch_id: str,
    bucket_name: str,
    table_name: str,
    request: SnapshotConfigRequest,
) -> SnapshotSettingsResponse:
    """Update snapshot settings for a table."""
    # Resolve branch and ensure default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "manage snapshot settings")

    # Validate table exists
    _validate_table_exists(resolved_project_id, resolved_branch_id, bucket_name, table_name)

    config = _request_to_config_dict(request)

    # Validate config
    errors = validate_config(config)
    if errors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_config",
                "message": "Invalid snapshot configuration",
                "details": {"errors": errors},
            },
        )

    # Save settings
    entity_type, entity_id = get_entity_id(project_id, bucket_name, table_name)
    metadata_db.set_snapshot_settings(entity_type, entity_id, project_id, config)

    logger.info(
        "update_table_snapshot_settings",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table_name,
        config=config,
    )

    # Return updated settings
    effective_config, inheritance = resolve_snapshot_config(
        project_id, bucket_name, table_name
    )
    local_config = get_local_config(project_id, bucket_name, table_name)

    return SnapshotSettingsResponse(
        effective_config=effective_config,
        inheritance=inheritance,
        local_config=local_config,
    )


@router.delete(
    "/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
    summary="Reset table snapshot settings",
    description="Delete local table settings (inherit from bucket). Only allowed on default branch.",
    dependencies=[Depends(require_project_access)],
)
async def delete_table_snapshot_settings(
    project_id: str, branch_id: str, bucket_name: str, table_name: str
) -> None:
    """Delete snapshot settings for a table (inherit from bucket)."""
    # Resolve branch and ensure default
    resolved_project_id, resolved_branch_id = resolve_branch(project_id, branch_id)
    require_default_branch(resolved_branch_id, "manage snapshot settings")

    # Validate table exists
    _validate_table_exists(resolved_project_id, resolved_branch_id, bucket_name, table_name)

    entity_type, entity_id = get_entity_id(project_id, bucket_name, table_name)
    metadata_db.delete_snapshot_settings(entity_type, entity_id)

    logger.info(
        "delete_table_snapshot_settings",
        project_id=project_id,
        branch_id=branch_id,
        bucket_name=bucket_name,
        table_name=table_name,
    )
