"""Backend management endpoints: health check and initialization."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from pathlib import Path

from src.config import settings
from src.dependencies import require_admin
from src.models.responses import HealthResponse, InitBackendResponse, ErrorResponse

logger = structlog.get_logger()
router = APIRouter(tags=["backend"])


def _check_path_accessible(path: Path) -> bool:
    """Check if a path exists and is accessible."""
    try:
        return path.exists() and path.is_dir()
    except (OSError, PermissionError):
        return False


def _check_path_writable(path: Path) -> bool:
    """Check if a path is writable by creating a test file."""
    try:
        if not path.exists():
            return False
        test_file = path / ".write_test"
        test_file.touch()
        test_file.unlink()
        return True
    except (OSError, PermissionError):
        return False


@router.get(
    "/health",
    response_model=HealthResponse,
    responses={503: {"model": ErrorResponse}},
    summary="Health check",
    description="Check if the service is healthy and storage is accessible.",
)
async def health_check() -> HealthResponse:
    """
    Perform health check.

    Validates:
    - Service is running
    - Storage paths are accessible
    """
    path_status = {}
    all_healthy = True

    for name, path in settings.storage_paths.items():
        is_accessible = _check_path_accessible(path)
        path_status[name] = is_accessible
        if not is_accessible:
            all_healthy = False

    logger.info(
        "health_check",
        status="healthy" if all_healthy else "unhealthy",
        path_status=path_status,
    )

    if not all_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "storage_unavailable",
                "message": "One or more storage paths are not accessible",
                "details": path_status,
            },
        )

    return HealthResponse(
        status="healthy",
        version=settings.api_version,
        storage_available=True,
        details=path_status,
    )


@router.post(
    "/backend/init",
    response_model=InitBackendResponse,
    responses={500: {"model": ErrorResponse}},
    summary="Initialize backend",
    description="Initialize the DuckDB storage backend. Creates required directories if needed.",
    dependencies=[Depends(require_admin)],
)
async def init_backend() -> InitBackendResponse:
    """
    Initialize the storage backend.

    This endpoint:
    1. Validates storage paths exist and are writable
    2. Creates directories if they don't exist
    3. Returns configured storage paths

    Equivalent to BigQuery's InitBackendHandler but much simpler:
    - No GCP folder validation
    - No IAM permission checks
    - No billing account validation
    - Just local filesystem checks
    """
    errors = []
    created_paths = []

    # Ensure all storage directories exist
    for name, path in settings.storage_paths.items():
        try:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
                created_paths.append(str(path))
                logger.info("created_directory", path=str(path), name=name)

            # Verify writable
            if not _check_path_writable(path):
                errors.append(f"{name}: not writable ({path})")

        except (OSError, PermissionError) as e:
            errors.append(f"{name}: {e}")
            logger.error("init_backend_error", path=str(path), error=str(e))

    if errors:
        logger.error("init_backend_failed", errors=errors)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "init_failed",
                "message": "Backend initialization failed",
                "details": {"errors": errors},
            },
        )

    logger.info(
        "init_backend_success",
        created_paths=created_paths,
        storage_paths={k: str(v) for k, v in settings.storage_paths.items()},
    )

    return InitBackendResponse(
        success=True,
        message="Backend initialized successfully"
        + (f" (created: {', '.join(created_paths)})" if created_paths else ""),
        storage_paths={k: str(v) for k, v in settings.storage_paths.items()},
    )


@router.post(
    "/backend/remove",
    response_model=InitBackendResponse,
    summary="Remove backend",
    description="Remove/cleanup the storage backend. Currently a no-op (same as BigQuery).",
    dependencies=[Depends(require_admin)],
)
async def remove_backend() -> InitBackendResponse:
    """
    Remove the storage backend.

    This is a NO-OP, same as BigQuery's RemoveBackendHandler.
    Cleanup is handled at a higher level (service lifecycle).
    """
    logger.info("remove_backend_called")

    return InitBackendResponse(
        success=True,
        message="Backend removal acknowledged (no-op)",
        storage_paths=None,
    )
