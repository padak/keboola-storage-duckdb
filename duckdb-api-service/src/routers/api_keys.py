"""API key management endpoints.

Provides API for managing project API keys:
- Create new keys with different scopes (project_admin, branch_admin, branch_read)
- List all keys for a project
- Get key details
- Revoke keys (soft delete)
- Rotate keys (create new, revoke old)
"""

import secrets
import uuid
from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, status

from src.auth import generate_api_key, generate_branch_key, get_key_prefix, hash_key
from src.config import settings
from src.database import metadata_db
from src.dependencies import require_project_access
from src.models.responses import (
    ApiKeyCreateRequest,
    ApiKeyCreateResponse,
    ApiKeyListResponse,
    ApiKeyResponse,
    ErrorResponse,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["api-keys"])


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


def _validate_branch_exists(project_id: str, branch_id: str) -> dict:
    """Validate that branch exists and belongs to project. Returns branch dict."""
    _validate_project_exists(project_id)
    branch = metadata_db.get_branch(branch_id)
    if not branch or branch.get("project_id") != project_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "branch_not_found",
                "message": f"Branch {branch_id} not found in project {project_id}",
                "details": {"project_id": project_id, "branch_id": branch_id},
            },
        )
    return branch


def _validate_key_exists(project_id: str, key_id: str) -> dict:
    """Validate that API key exists and belongs to project. Returns key dict."""
    key = metadata_db.get_api_key_by_id(key_id)
    if not key or key.get("project_id") != project_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "api_key_not_found",
                "message": f"API key {key_id} not found in project {project_id}",
                "details": {"project_id": project_id, "key_id": key_id},
            },
        )
    return key


def _key_to_response(key: dict) -> ApiKeyResponse:
    """Convert key dict to response model."""
    return ApiKeyResponse(
        id=key["id"],
        project_id=key["project_id"],
        branch_id=key.get("branch_id"),
        key_prefix=key["key_prefix"],
        scope=key["scope"],
        description=key.get("description"),
        created_at=key.get("created_at"),
        last_used_at=key.get("last_used_at"),
        expires_at=key.get("expires_at"),
        is_revoked=key.get("revoked_at") is not None,
    )


@router.post(
    "/projects/{project_id}/api-keys",
    response_model=ApiKeyCreateResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "API key created successfully"},
        400: {"model": ErrorResponse, "description": "Invalid request"},
        404: {"model": ErrorResponse, "description": "Project or branch not found"},
    },
    summary="Create a new API key",
    description="""
    Create a new API key with specified scope and optional expiration.

    **Scopes:**
    - `project_admin` (default): Full access to project (no branch_id required)
    - `branch_admin`: Full access to specific branch (requires branch_id)
    - `branch_read`: Read-only access to specific branch (requires branch_id)

    **IMPORTANT**: The full API key is returned ONLY on creation. Save it securely.

    **Note**: Requires project_admin access to create keys.
    """,
)
async def create_api_key(
    project_id: str,
    request: ApiKeyCreateRequest,
    _auth: None = Depends(require_project_access),
) -> ApiKeyCreateResponse:
    """Create a new API key."""
    _validate_project_exists(project_id)

    # Validate scope + branch_id combination
    if request.scope in ("branch_admin", "branch_read"):
        if not request.branch_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "invalid_request",
                    "message": f"branch_id is required for scope '{request.scope}'",
                    "details": {"scope": request.scope},
                },
            )
        # Validate branch exists
        _validate_branch_exists(project_id, request.branch_id)
    elif request.scope == "project_admin" and request.branch_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_request",
                "message": "branch_id must be null for scope 'project_admin'",
                "details": {"scope": request.scope},
            },
        )

    # Generate API key
    if request.scope == "project_admin":
        api_key = generate_api_key(project_id)
    else:
        # Convert scope to format expected by generate_branch_key
        scope_suffix = "admin" if request.scope == "branch_admin" else "read"
        api_key = generate_branch_key(project_id, request.branch_id, scope_suffix)

    # Hash the key
    key_hash = hash_key(api_key)
    key_prefix = get_key_prefix(api_key)

    # Calculate expiration
    expires_at = None
    if request.expires_in_days:
        expires_at = datetime.now(timezone.utc) + timedelta(days=request.expires_in_days)

    # Generate key ID
    key_id = f"key_{str(uuid.uuid4())[:8]}"

    # Store in database
    key_record = metadata_db.create_api_key(
        key_id=key_id,
        project_id=project_id,
        key_hash=key_hash,
        key_prefix=key_prefix,
        description=request.description,
        scope=request.scope,
        branch_id=request.branch_id,
        expires_at=expires_at,
    )

    # Log operation
    metadata_db.log_operation(
        operation="create_api_key",
        status="success",
        project_id=project_id,
        resource_type="api_key",
        resource_id=key_id,
        details={
            "scope": request.scope,
            "branch_id": request.branch_id,
            "description": request.description,
            "expires_at": expires_at.isoformat() if expires_at else None,
        },
    )

    logger.info(
        "api_key_created",
        project_id=project_id,
        key_id=key_id,
        scope=request.scope,
        branch_id=request.branch_id,
        key_prefix=key_prefix,
    )

    # Return response with full key (only time it's shown)
    return ApiKeyCreateResponse(
        id=key_record["id"],
        project_id=key_record["project_id"],
        branch_id=key_record.get("branch_id"),
        key_prefix=key_record["key_prefix"],
        scope=key_record["scope"],
        description=key_record.get("description"),
        created_at=key_record.get("created_at"),
        expires_at=key_record.get("expires_at"),
        api_key=api_key,
    )


@router.get(
    "/projects/{project_id}/api-keys",
    response_model=ApiKeyListResponse,
    responses={
        200: {"description": "List of API keys"},
        404: {"model": ErrorResponse, "description": "Project not found"},
    },
    summary="List all API keys",
    description="""
    List all API keys for a project (excluding revoked keys by default).

    Returns key metadata without the actual key value (keys are only shown once during creation).
    """,
)
async def list_api_keys(
    project_id: str,
    _auth: None = Depends(require_project_access),
) -> ApiKeyListResponse:
    """List all API keys for a project."""
    _validate_project_exists(project_id)

    # Get all non-revoked keys
    keys = metadata_db.get_api_keys_for_project(project_id, include_revoked=False)

    # Convert to response models
    api_keys = [_key_to_response(key) for key in keys]

    return ApiKeyListResponse(
        api_keys=api_keys,
        count=len(api_keys),
    )


@router.get(
    "/projects/{project_id}/api-keys/{key_id}",
    response_model=ApiKeyResponse,
    responses={
        200: {"description": "API key details"},
        404: {"model": ErrorResponse, "description": "API key not found"},
    },
    summary="Get API key details",
    description="Get detailed information about a specific API key (without the actual key value).",
)
async def get_api_key(
    project_id: str,
    key_id: str,
    _auth: None = Depends(require_project_access),
) -> ApiKeyResponse:
    """Get API key details."""
    key = _validate_key_exists(project_id, key_id)
    return _key_to_response(key)


@router.delete(
    "/projects/{project_id}/api-keys/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "API key revoked successfully"},
        400: {"model": ErrorResponse, "description": "Cannot revoke last project_admin key"},
        404: {"model": ErrorResponse, "description": "API key not found"},
    },
    summary="Revoke an API key",
    description="""
    Revoke an API key (soft delete).

    The key will be immediately invalidated and cannot be used for authentication.
    This operation cannot be undone.

    **Note**: You cannot revoke the last active project_admin key to prevent lockout.
    """,
)
async def revoke_api_key(
    project_id: str,
    key_id: str,
    _auth: None = Depends(require_project_access),
) -> None:
    """Revoke an API key."""
    key = _validate_key_exists(project_id, key_id)

    # Check if this is a project_admin key
    if key["scope"] == "project_admin":
        # Count active project_admin keys
        active_count = metadata_db.count_active_project_admin_keys(project_id)

        # Prevent revoking the last project_admin key
        if active_count <= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "cannot_revoke_last_admin_key",
                    "message": "Cannot revoke the last active project_admin key. Create a new one first.",
                    "details": {
                        "project_id": project_id,
                        "key_id": key_id,
                        "active_project_admin_keys": active_count,
                    },
                },
            )

    # Revoke the key
    success = metadata_db.revoke_api_key(key_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "api_key_not_found",
                "message": f"API key {key_id} not found",
                "details": {"key_id": key_id},
            },
        )

    # Log operation
    metadata_db.log_operation(
        operation="revoke_api_key",
        status="success",
        project_id=project_id,
        resource_type="api_key",
        resource_id=key_id,
        details={
            "scope": key["scope"],
            "branch_id": key.get("branch_id"),
            "key_prefix": key["key_prefix"],
        },
    )

    logger.info(
        "api_key_revoked",
        project_id=project_id,
        key_id=key_id,
        scope=key["scope"],
        branch_id=key.get("branch_id"),
    )


@router.post(
    "/projects/{project_id}/api-keys/{key_id}/rotate",
    response_model=ApiKeyCreateResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "API key rotated successfully"},
        400: {"model": ErrorResponse, "description": "Cannot rotate revoked key"},
        404: {"model": ErrorResponse, "description": "API key not found"},
    },
    summary="Rotate an API key",
    description="""
    Rotate an API key by creating a new key with the same settings and revoking the old one.

    The new key will have:
    - Same scope as the old key
    - Same branch_id (if applicable)
    - Same description with " (rotated)" suffix
    - New expiration (if original had one)

    **IMPORTANT**: The new API key is returned ONLY once. Save it securely.
    The old key is immediately revoked and cannot be used.
    """,
)
async def rotate_api_key(
    project_id: str,
    key_id: str,
    _auth: None = Depends(require_project_access),
) -> ApiKeyCreateResponse:
    """Rotate an API key."""
    old_key = _validate_key_exists(project_id, key_id)

    # Cannot rotate a revoked key
    if old_key.get("revoked_at"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "cannot_rotate_revoked_key",
                "message": "Cannot rotate a revoked key. Create a new key instead.",
                "details": {"key_id": key_id},
            },
        )

    # Generate new API key with same settings
    scope = old_key["scope"]
    branch_id = old_key.get("branch_id")

    if scope == "project_admin":
        new_api_key = generate_api_key(project_id)
    else:
        # Convert scope to format expected by generate_branch_key
        scope_suffix = "admin" if scope == "branch_admin" else "read"
        new_api_key = generate_branch_key(project_id, branch_id, scope_suffix)

    # Hash the new key
    new_key_hash = hash_key(new_api_key)
    new_key_prefix = get_key_prefix(new_api_key)

    # Calculate new expiration (if original had one)
    new_expires_at = None
    if old_key.get("expires_at"):
        old_expires = datetime.fromisoformat(old_key["expires_at"])
        old_created = datetime.fromisoformat(old_key["created_at"])
        ttl_days = (old_expires - old_created).days
        new_expires_at = datetime.now(timezone.utc) + timedelta(days=ttl_days)

    # Generate new key ID
    new_key_id = f"key_{str(uuid.uuid4())[:8]}"

    # Create new key
    new_key_record = metadata_db.create_api_key(
        key_id=new_key_id,
        project_id=project_id,
        key_hash=new_key_hash,
        key_prefix=new_key_prefix,
        description=f"{old_key.get('description', 'API Key')} (rotated)",
        scope=scope,
        branch_id=branch_id,
        expires_at=new_expires_at,
    )

    # Revoke old key
    metadata_db.revoke_api_key(key_id)

    # Log operation
    metadata_db.log_operation(
        operation="rotate_api_key",
        status="success",
        project_id=project_id,
        resource_type="api_key",
        resource_id=new_key_id,
        details={
            "old_key_id": key_id,
            "scope": scope,
            "branch_id": branch_id,
            "expires_at": new_expires_at.isoformat() if new_expires_at else None,
        },
    )

    logger.info(
        "api_key_rotated",
        project_id=project_id,
        old_key_id=key_id,
        new_key_id=new_key_id,
        scope=scope,
        branch_id=branch_id,
    )

    # Return response with full key (only time it's shown)
    return ApiKeyCreateResponse(
        id=new_key_record["id"],
        project_id=new_key_record["project_id"],
        branch_id=new_key_record.get("branch_id"),
        key_prefix=new_key_record["key_prefix"],
        scope=new_key_record["scope"],
        description=new_key_record.get("description"),
        created_at=new_key_record.get("created_at"),
        expires_at=new_key_record.get("expires_at"),
        api_key=new_api_key,
    )
