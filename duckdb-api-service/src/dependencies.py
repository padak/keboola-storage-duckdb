"""FastAPI dependencies for authentication and authorization.

This module provides dependency injection functions for securing API endpoints.
It implements a hierarchical API key model:

1. ADMIN_API_KEY (from ENV) - Can create projects, list all projects
2. PROJECT_ADMIN_API_KEY (stored in DB) - Full access to a specific project
3. BRANCH_ADMIN_API_KEY (stored in DB) - Full access to a specific branch
4. BRANCH_READ_API_KEY (stored in DB) - Read-only access to a specific branch

Usage in routers:
    @router.post("/projects", dependencies=[Depends(require_admin)])
    async def create_project(...):
        ...

    @router.get("/projects/{project_id}/buckets", dependencies=[Depends(require_project_access)])
    async def list_buckets(project_id: str, ...):
        ...

    @router.get("/projects/{project_id}/branches/{branch_id}", dependencies=[Depends(require_branch_access)])
    async def get_branch(project_id: str, branch_id: str, ...):
        ...
"""

from typing import Annotated, Any

import structlog
from fastapi import Depends, HTTPException, Path, Request, status
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
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
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


async def get_api_key_flexible(request: Request) -> str:
    """
    Extract API key from multiple sources for S3-compatible API.

    Supports:
    1. Authorization: Bearer {api_key}
    2. X-Api-Key: {api_key}
    3. x-amz-security-token (for STS-like flow)

    This allows AWS SDK to work with our API using custom handlers.

    Args:
        request: The FastAPI request

    Returns:
        The extracted API key

    Raises:
        AuthenticationError: If no valid API key found
    """
    # Try Authorization header (Bearer)
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]

    # Try X-Api-Key header (for custom S3 handlers)
    api_key = request.headers.get("X-Api-Key")
    if api_key:
        return api_key

    # Try x-amz-security-token (for STS-like flow)
    security_token = request.headers.get("x-amz-security-token")
    if security_token:
        return security_token

    logger.warning("auth_no_api_key_found", headers=list(request.headers.keys()))
    raise AuthenticationError("Missing API key. Use Authorization: Bearer, X-Api-Key, or x-amz-security-token header")


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


def verify_project_key(api_key: str, project_id: str) -> dict[str, Any] | None:
    """
    Verify if the API key is valid for the given project.

    Args:
        api_key: The API key to verify
        project_id: The project ID to check access for

    Returns:
        The key record dict if valid for this project, None otherwise.
        Key record includes: id, project_id, branch_id, scope, key_hash, etc.
    """
    # Get the key prefix for database lookup
    key_prefix = get_key_prefix(api_key)

    # Look up key by prefix (already filters revoked/expired keys)
    key_record = metadata_db.get_api_key_by_prefix(key_prefix)

    if not key_record:
        logger.debug("auth_key_not_found", key_prefix=key_prefix)
        return None

    # Verify the key hash
    if not verify_key_hash(api_key, key_record["key_hash"]):
        logger.warning("auth_key_hash_mismatch", key_prefix=key_prefix)
        return None

    # Check project ID matches
    if key_record["project_id"] != project_id:
        logger.warning(
            "auth_project_mismatch",
            key_prefix=key_prefix,
            key_project=key_record["project_id"],
            requested_project=project_id,
        )
        return None

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
        scope=key_record["scope"],
    )
    return key_record


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
    key_record = verify_project_key(api_key, project_id)
    if key_record:
        logger.info(
            "auth_project_access_granted",
            project_id=project_id,
            key_prefix=get_key_prefix(api_key),
            scope=key_record["scope"],
        )
        return api_key

    # Key is valid but doesn't have access to this project
    logger.warning(
        "auth_project_access_denied",
        project_id=project_id,
        key_prefix=get_key_prefix(api_key),
    )
    raise AuthorizationError(f"Access denied to project {project_id}")


def verify_branch_key(api_key: str, project_id: str, branch_id: str) -> dict[str, Any] | None:
    """
    Verify if the API key has access to the given branch.

    Args:
        api_key: The API key to verify
        project_id: The project ID to check access for
        branch_id: The branch ID to check access for

    Returns:
        The key record dict if valid for this branch, None otherwise.
        Key record includes: id, project_id, branch_id, scope, key_hash, etc.

    Scope-based access:
        - project_admin: Has access to all branches in the project
        - branch_admin: Has access only to the specific branch_id
        - branch_read: Has read-only access to the specific branch_id
    """
    # Get the key prefix for database lookup
    key_prefix = get_key_prefix(api_key)

    # Look up key by prefix (already filters revoked/expired keys)
    key_record = metadata_db.get_api_key_by_prefix(key_prefix)

    if not key_record:
        logger.debug("auth_key_not_found", key_prefix=key_prefix)
        return None

    # Verify the key hash
    if not verify_key_hash(api_key, key_record["key_hash"]):
        logger.warning("auth_key_hash_mismatch", key_prefix=key_prefix)
        return None

    # Check project ID matches
    if key_record["project_id"] != project_id:
        logger.warning(
            "auth_branch_project_mismatch",
            key_prefix=key_prefix,
            key_project=key_record["project_id"],
            requested_project=project_id,
        )
        return None

    # Check scope-based access
    scope = key_record["scope"]

    if scope == "project_admin":
        # project_admin has access to all branches
        logger.debug(
            "auth_branch_access_via_project_admin",
            key_prefix=key_prefix,
            project_id=project_id,
            branch_id=branch_id,
        )
    elif scope in ("branch_admin", "branch_read"):
        # branch_admin/branch_read only have access to their specific branch
        if key_record["branch_id"] != branch_id:
            logger.warning(
                "auth_branch_mismatch",
                key_prefix=key_prefix,
                key_branch=key_record["branch_id"],
                requested_branch=branch_id,
                scope=scope,
            )
            return None
    else:
        logger.warning(
            "auth_invalid_scope_for_branch",
            key_prefix=key_prefix,
            scope=scope,
        )
        return None

    # Update last_used_at timestamp
    try:
        metadata_db.update_api_key_last_used(key_record["id"])
    except Exception as e:
        # Don't fail auth if we can't update timestamp
        logger.warning("auth_update_last_used_failed", error=str(e))

    logger.debug(
        "auth_branch_key_verified",
        key_prefix=key_prefix,
        project_id=project_id,
        branch_id=branch_id,
        scope=scope,
    )
    return key_record


async def require_branch_access(
    api_key: Annotated[str, Depends(get_api_key_from_header)],
    project_id: Annotated[str, Path(description="Project ID")],
    branch_id: Annotated[str, Path(description="Branch ID")],
) -> str:
    """
    Dependency that requires access to a specific branch.

    This accepts:
    1. Admin key (has access to all projects and branches)
    2. Project admin key (has access to all branches in the project)
    3. Branch admin key (has full access to the specific branch)
    4. Branch read key (has read-only access to the specific branch)

    Note: For branch_read keys, the router is responsible for enforcing
    read-only restrictions (GET operations only). This dependency only
    validates that the key has access to the branch.

    Use this for all branch-scoped endpoints:
    - GET/PUT/DELETE /projects/{project_id}/branches/{branch_id}
    - /projects/{project_id}/branches/{branch_id}/*

    Args:
        api_key: The API key from the Authorization header
        project_id: The project ID from the URL path
        branch_id: The branch ID from the URL path

    Returns:
        The verified API key

    Raises:
        AuthenticationError: If the key is invalid
        AuthorizationError: If the key doesn't have access to this branch
    """
    # Admin key has access to everything
    if verify_admin_key(api_key):
        logger.info("auth_admin_branch_access", project_id=project_id, branch_id=branch_id)
        return api_key

    # Check branch-specific key
    key_record = verify_branch_key(api_key, project_id, branch_id)
    if key_record:
        logger.info(
            "auth_branch_access_granted",
            project_id=project_id,
            branch_id=branch_id,
            key_prefix=get_key_prefix(api_key),
            scope=key_record["scope"],
        )
        return api_key

    # Key is valid but doesn't have access to this branch
    logger.warning(
        "auth_branch_access_denied",
        project_id=project_id,
        branch_id=branch_id,
        key_prefix=get_key_prefix(api_key),
    )
    raise AuthorizationError(f"Access denied to branch {branch_id}")


async def require_driver_auth(
    api_key: Annotated[str, Depends(get_api_key_from_header)],
) -> str:
    """
    Dependency for driver execute endpoint.

    This accepts:
    1. Admin key (for admin operations: CreateProject, DropProject, InitBackend, RemoveBackend)
    2. Project key (for project operations: all other commands)

    The actual project_id validation is done at the command handler level,
    since it comes from the request body, not URL path.

    This dependency just validates that the key is either:
    - A valid admin key, OR
    - A valid project key (exists in database, not revoked/expired)

    Args:
        api_key: The API key from the Authorization header

    Returns:
        The API key

    Raises:
        AuthenticationError: If the key is invalid
    """
    # Admin key is always valid
    if verify_admin_key(api_key):
        logger.info("auth_driver_admin_access")
        return api_key

    # Check if it's a valid project key (any project)
    key_prefix = get_key_prefix(api_key)
    key_record = metadata_db.get_api_key_by_prefix(key_prefix)

    if key_record and verify_key_hash(api_key, key_record["key_hash"]):
        logger.info(
            "auth_driver_project_key",
            key_prefix=key_prefix,
            project_id=key_record["project_id"],
            scope=key_record["scope"],
        )
        return api_key

    logger.warning("auth_driver_access_denied", key_prefix=key_prefix)
    raise AuthenticationError("Invalid API key")


def get_project_id_from_driver_key(api_key: str) -> str | None:
    """
    Get the project_id associated with a driver API key.

    Returns:
        The project_id if this is a project key, None if admin key.
    """
    if verify_admin_key(api_key):
        return None  # Admin key has access to all projects

    key_prefix = get_key_prefix(api_key)
    key_record = metadata_db.get_api_key_by_prefix(key_prefix)

    if key_record and verify_key_hash(api_key, key_record["key_hash"]):
        return key_record["project_id"]

    return None


async def require_s3_bucket_access(
    request: Request,
    bucket: Annotated[str, Path(description="S3 bucket name (project_id or project_{id})")],
) -> str:
    """
    Dependency for S3-compatible API endpoints.

    Supports multiple authentication methods (in order):
    1. AWS Signature V4: Authorization: AWS4-HMAC-SHA256 ... (for boto3/aws-cli)
    2. Authorization: Bearer {api_key}
    3. X-Api-Key: {api_key}
    4. x-amz-security-token: {api_key}

    This accepts both:
    1. Admin key (has access to all buckets/projects)
    2. Project-specific key (has access only to its project)

    The bucket name is mapped to project_id:
    - "project_123" -> "123"
    - "123" -> "123"

    Args:
        request: The FastAPI request (for flexible header extraction)
        bucket: The S3 bucket name from the URL path

    Returns:
        The verified API key

    Raises:
        AuthenticationError: If the key is invalid
        AuthorizationError: If the key doesn't have access to this bucket
    """
    # Extract project_id from bucket name
    if bucket.startswith("project_"):
        project_id = bucket[8:]
    else:
        project_id = bucket

    # Check for AWS Signature V4 (boto3/aws-cli/rclone)
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("AWS4-HMAC-SHA256"):
        # Import here to avoid circular import
        from src.routers.s3_compat import _verify_aws_sig_v4

        # For bucket-level operations, key is empty
        is_valid, access_key = _verify_aws_sig_v4(request, bucket, "")
        if is_valid:
            # Validate project exists
            project = metadata_db.get_project(project_id)
            if not project:
                raise AuthorizationError(f"Bucket {bucket} does not exist")
            logger.info("auth_s3_sig_v4_access", bucket=bucket, project_id=project_id, access_key=access_key)
            return f"aws_sig_v4:{access_key}"  # Return marker for logging
        else:
            raise AuthenticationError("Invalid AWS Signature V4")

    # Extract API key from flexible sources (Bearer, X-Api-Key, etc.)
    api_key = await get_api_key_flexible(request)

    # Admin key has access to everything
    if verify_admin_key(api_key):
        logger.info("auth_s3_admin_access", bucket=bucket, project_id=project_id)
        return api_key

    # Check project-specific key
    key_record = verify_project_key(api_key, project_id)
    if key_record:
        logger.info(
            "auth_s3_access_granted",
            bucket=bucket,
            project_id=project_id,
            key_prefix=get_key_prefix(api_key),
            scope=key_record["scope"],
        )
        return api_key

    # Key is valid but doesn't have access to this bucket/project
    logger.warning(
        "auth_s3_access_denied",
        bucket=bucket,
        project_id=project_id,
        key_prefix=get_key_prefix(api_key),
    )
    raise AuthorizationError(f"Access denied to bucket {bucket}")


# Type aliases for cleaner router signatures
AdminKey = Annotated[str, Depends(require_admin)]
ProjectKey = Annotated[str, Depends(require_project_access)]
BranchKey = Annotated[str, Depends(require_branch_access)]
DriverKey = Annotated[str, Depends(require_driver_auth)]
S3BucketKey = Annotated[str, Depends(require_s3_bucket_access)]
