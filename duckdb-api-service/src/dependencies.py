"""FastAPI dependencies for authentication and authorization.

This module provides dependency injection functions for securing API endpoints.
It implements a hierarchical API key model:

1. ADMIN_API_KEY (from ENV) - Can create projects, list all projects
2. PROJECT_ADMIN_API_KEY (stored in DB) - Full access to a specific project

Usage in routers:
    @router.post("/projects", dependencies=[Depends(require_admin)])
    async def create_project(...):
        ...

    @router.get("/projects/{project_id}/buckets", dependencies=[Depends(require_project_access)])
    async def list_buckets(project_id: str, ...):
        ...
"""

from typing import Annotated

import structlog
from fastapi import Depends, HTTPException, Path, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.auth import get_key_prefix, verify_key_hash
from src.config import settings
from src.database import metadata_db

logger = structlog.get_logger(__name__)

# Security scheme for Swagger UI
security = HTTPBearer(
    scheme_name="Bearer Auth",
    description="Enter your API key (ADMIN_API_KEY or project-specific key)",
)


class AuthenticationError(HTTPException):
    """Raised when authentication fails."""

    def __init__(self, detail: str = "Invalid or missing API key"):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "unauthorized", "message": detail},
            headers={"WWW-Authenticate": "Bearer"},
        )


class AuthorizationError(HTTPException):
    """Raised when authorization fails (valid key but wrong project)."""

    def __init__(self, detail: str = "Access denied to this resource"):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "message": detail},
        )


def get_api_key_from_header(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> str:
    """
    Extract API key from Authorization header using HTTPBearer.

    This integrates with Swagger UI's Authorize button.

    Args:
        credentials: The HTTP Bearer credentials from FastAPI security

    Returns:
        The extracted API key

    Raises:
        AuthenticationError: If credentials are invalid
    """
    if not credentials or not credentials.credentials:
        logger.warning("auth_missing_credentials")
        raise AuthenticationError("Missing or invalid credentials")

    return credentials.credentials


def verify_admin_key(api_key: str) -> bool:
    """
    Verify if the API key is the admin key.

    Args:
        api_key: The API key to verify

    Returns:
        True if this is the admin key, False otherwise
    """
    if not settings.admin_api_key:
        logger.warning("auth_admin_key_not_configured")
        return False

    # Simple string comparison for admin key (it's not hashed)
    return api_key == settings.admin_api_key


def verify_project_key(api_key: str, project_id: str) -> bool:
    """
    Verify if the API key is valid for the given project.

    Args:
        api_key: The API key to verify
        project_id: The project ID to check access for

    Returns:
        True if the key is valid for this project, False otherwise
    """
    # Get the key prefix for database lookup
    key_prefix = get_key_prefix(api_key)

    # Look up key by prefix
    key_record = metadata_db.get_api_key_by_prefix(key_prefix)

    if not key_record:
        logger.debug("auth_key_not_found", key_prefix=key_prefix)
        return False

    # Verify the key hash
    if not verify_key_hash(api_key, key_record["key_hash"]):
        logger.warning("auth_key_hash_mismatch", key_prefix=key_prefix)
        return False

    # Check project ID matches
    if key_record["project_id"] != project_id:
        logger.warning(
            "auth_project_mismatch",
            key_prefix=key_prefix,
            key_project=key_record["project_id"],
            requested_project=project_id,
        )
        return False

    # Update last_used_at timestamp
    try:
        metadata_db.update_api_key_last_used(key_record["id"])
    except Exception as e:
        # Don't fail auth if we can't update timestamp
        logger.warning("auth_update_last_used_failed", error=str(e))

    logger.debug(
        "auth_project_key_verified",
        key_prefix=key_prefix,
        project_id=project_id,
    )
    return True


async def require_admin(
    api_key: Annotated[str, Depends(get_api_key_from_header)],
) -> str:
    """
    Dependency that requires admin-level access.

    Use this for endpoints that should only be accessible with the admin key:
    - POST /projects (create project)
    - GET /projects (list all projects)
    - POST /backend/init
    - POST /backend/remove

    Args:
        api_key: The API key from the Authorization header

    Returns:
        The verified API key

    Raises:
        AuthenticationError: If the key is not the admin key
    """
    if verify_admin_key(api_key):
        logger.info("auth_admin_access_granted")
        return api_key

    # If admin key is not set, log error
    if not settings.admin_api_key:
        logger.error("auth_admin_key_not_configured")
        raise AuthenticationError("Admin API key not configured on server")

    logger.warning("auth_admin_access_denied", key_prefix=get_key_prefix(api_key))
    raise AuthenticationError("Invalid admin API key")


async def require_project_access(
    api_key: Annotated[str, Depends(get_api_key_from_header)],
    project_id: Annotated[str, Path(description="Project ID")],
) -> str:
    """
    Dependency that requires access to a specific project.

    This accepts both:
    1. Admin key (has access to all projects)
    2. Project-specific key (has access only to its project)

    Use this for all project-scoped endpoints:
    - GET/PUT/DELETE /projects/{project_id}
    - /projects/{project_id}/buckets/*
    - /projects/{project_id}/buckets/*/tables/*

    Args:
        api_key: The API key from the Authorization header
        project_id: The project ID from the URL path

    Returns:
        The verified API key

    Raises:
        AuthenticationError: If the key is invalid
        AuthorizationError: If the key doesn't have access to this project
    """
    # Admin key has access to everything
    if verify_admin_key(api_key):
        logger.info("auth_admin_project_access", project_id=project_id)
        return api_key

    # Check project-specific key
    if verify_project_key(api_key, project_id):
        logger.info(
            "auth_project_access_granted",
            project_id=project_id,
            key_prefix=get_key_prefix(api_key),
        )
        return api_key

    # Key is valid but doesn't have access to this project
    logger.warning(
        "auth_project_access_denied",
        project_id=project_id,
        key_prefix=get_key_prefix(api_key),
    )
    raise AuthorizationError(f"Access denied to project {project_id}")


# Type aliases for cleaner router signatures
AdminKey = Annotated[str, Depends(require_admin)]
ProjectKey = Annotated[str, Depends(require_project_access)]
