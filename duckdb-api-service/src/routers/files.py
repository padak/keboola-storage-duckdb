"""Files API endpoints: on-prem S3 replacement for file staging and storage.

This module implements a 3-stage file upload workflow:
1. PREPARE: Get upload key and URL
2. UPLOAD: Upload file to staging area
3. REGISTER: Finalize file and move from staging to permanent storage

Files can then be used for import operations or downloaded for export.
"""

import hashlib
import shutil
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse

from src.config import settings
from src.database import metadata_db
from src.dependencies import require_project_access
from src import metrics
from src.models.responses import (
    ErrorResponse,
    FileListResponse,
    FilePrepareRequest,
    FilePrepareResponse,
    FileRegisterRequest,
    FileResponse as FileInfoResponse,
    FileUploadResponse,
)

logger = structlog.get_logger()
router = APIRouter(prefix="", tags=["files"])

# Constants
STAGING_TTL_HOURS = 24
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024 * 1024  # 10GB


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        import structlog
        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


def _get_project_files_dir(project_id: str) -> Path:
    """Get the files directory for a project."""
    return settings.files_dir / f"project_{project_id}"


def _get_staging_dir(project_id: str) -> Path:
    """Get the staging directory for a project."""
    return _get_project_files_dir(project_id) / "staging"


def _get_permanent_dir(project_id: str) -> Path:
    """Get the permanent storage directory for a project (date-organized)."""
    now = datetime.now(timezone.utc)
    return _get_project_files_dir(project_id) / now.strftime("%Y/%m/%d")


def _compute_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def _validate_project_exists(project_id: str) -> dict[str, Any]:
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
    return project


# In-memory upload session storage (for MVP - could use Redis in production)
_upload_sessions: dict[str, dict[str, Any]] = {}


@router.post(
    "/projects/{project_id}/files/prepare",
    response_model=FilePrepareResponse,
    status_code=status.HTTP_200_OK,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Prepare file upload",
    description="Prepare to upload a file. Returns an upload key and URL.",
    dependencies=[Depends(require_project_access)],
)
async def prepare_upload(
    project_id: str,
    request: FilePrepareRequest,
) -> FilePrepareResponse:
    """
    Prepare a file upload session.

    This is stage 1 of the 3-stage upload workflow:
    1. PREPARE (this endpoint) - get upload credentials
    2. UPLOAD - upload file to staging
    3. REGISTER - finalize and move to permanent storage

    The upload session expires after 24 hours if not completed.
    """
    request_id = _get_request_id()

    logger.info(
        "prepare_upload_start",
        project_id=project_id,
        filename=request.filename,
        content_type=request.content_type,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Validate file size if provided
    if request.size_bytes and request.size_bytes > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "file_too_large",
                "message": f"File size exceeds maximum of {MAX_FILE_SIZE_BYTES} bytes",
                "details": {
                    "max_size_bytes": MAX_FILE_SIZE_BYTES,
                    "requested_size_bytes": request.size_bytes,
                },
            },
        )

    # Generate upload key
    upload_key = str(uuid.uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(hours=STAGING_TTL_HOURS)

    # Store upload session
    _upload_sessions[upload_key] = {
        "project_id": project_id,
        "filename": request.filename,
        "content_type": request.content_type,
        "expected_size": request.size_bytes,
        "tags": request.tags,
        "created_at": datetime.now(timezone.utc),
        "expires_at": expires_at,
    }

    # Build upload URL
    upload_url = f"/projects/{project_id}/files/upload/{upload_key}"

    logger.info(
        "prepare_upload_complete",
        project_id=project_id,
        upload_key=upload_key,
        expires_at=expires_at.isoformat(),
        request_id=request_id,
    )

    return FilePrepareResponse(
        upload_key=upload_key,
        upload_url=upload_url,
        expires_at=expires_at.isoformat(),
    )


@router.post(
    "/projects/{project_id}/files/upload/{upload_key}",
    response_model=FileUploadResponse,
    status_code=status.HTTP_200_OK,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Upload file",
    description="Upload a file to staging area using a prepared upload key.",
    dependencies=[Depends(require_project_access)],
)
async def upload_file(
    project_id: str,
    upload_key: str,
    file: UploadFile = File(...),
) -> FileUploadResponse:
    """
    Upload a file to the staging area.

    This is stage 2 of the 3-stage upload workflow.
    The file is saved to staging and a checksum is computed.
    Call the register endpoint to finalize the upload.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "upload_file_start",
        project_id=project_id,
        upload_key=upload_key,
        filename=file.filename,
        request_id=request_id,
    )

    # Validate upload session
    session = _upload_sessions.get(upload_key)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "upload_session_not_found",
                "message": "Upload session not found or expired",
                "details": {"upload_key": upload_key},
            },
        )

    # Validate session belongs to project
    if session["project_id"] != project_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "upload_session_mismatch",
                "message": "Upload session does not belong to this project",
                "details": {"upload_key": upload_key, "project_id": project_id},
            },
        )

    # Check if session expired
    if datetime.now(timezone.utc) > session["expires_at"]:
        del _upload_sessions[upload_key]
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail={
                "error": "upload_session_expired",
                "message": "Upload session has expired",
                "details": {"upload_key": upload_key},
            },
        )

    # Create staging directory
    staging_dir = _get_staging_dir(project_id)
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Save file to staging with upload_key as filename
    staging_filename = f"{upload_key}_{session['filename']}"
    staging_path = staging_dir / staging_filename

    try:
        # Stream file to disk
        size_bytes = 0
        sha256_hash = hashlib.sha256()

        with open(staging_path, "wb") as f:
            while chunk := await file.read(8192):
                size_bytes += len(chunk)
                sha256_hash.update(chunk)
                f.write(chunk)

                # Check size limit
                if size_bytes > MAX_FILE_SIZE_BYTES:
                    staging_path.unlink()
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail={
                            "error": "file_too_large",
                            "message": f"File exceeds maximum size of {MAX_FILE_SIZE_BYTES} bytes",
                            "details": {"max_size_bytes": MAX_FILE_SIZE_BYTES},
                        },
                    )

        checksum_sha256 = sha256_hash.hexdigest()

        # Update session with actual file info
        session["staging_path"] = str(staging_path)
        session["size_bytes"] = size_bytes
        session["checksum_sha256"] = checksum_sha256
        session["uploaded_at"] = datetime.now(timezone.utc)

        duration_ms = int((time.time() - start_time) * 1000)

        logger.info(
            "upload_file_complete",
            project_id=project_id,
            upload_key=upload_key,
            size_bytes=size_bytes,
            checksum_sha256=checksum_sha256[:16] + "...",
            duration_ms=duration_ms,
            request_id=request_id,
        )

        metrics.FILES_UPLOADS_TOTAL.labels(status="success").inc()
        metrics.FILES_UPLOAD_BYTES_TOTAL.inc(size_bytes)
        metrics.FILES_UPLOAD_DURATION.observe((time.time() - start_time))

        return FileUploadResponse(
            upload_key=upload_key,
            staging_path=f"staging/{staging_filename}",
            size_bytes=size_bytes,
            checksum_sha256=checksum_sha256,
        )

    except HTTPException:
        metrics.FILES_UPLOADS_TOTAL.labels(status="error").inc()
        raise
    except Exception as e:
        logger.error(
            "upload_file_error",
            project_id=project_id,
            upload_key=upload_key,
            error=str(e),
            request_id=request_id,
        )
        metrics.FILES_UPLOADS_TOTAL.labels(status="error").inc()
        if staging_path.exists():
            staging_path.unlink()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "upload_failed",
                "message": f"Failed to upload file: {str(e)}",
            },
        )


@router.post(
    "/projects/{project_id}/files",
    response_model=FileInfoResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Register uploaded file",
    description="Register an uploaded file, moving it from staging to permanent storage.",
    dependencies=[Depends(require_project_access)],
)
async def register_file(
    project_id: str,
    request: FileRegisterRequest,
) -> FileInfoResponse:
    """
    Register an uploaded file (stage 3 of upload workflow).

    This moves the file from staging to permanent storage and
    creates a database record for it.
    """
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "register_file_start",
        project_id=project_id,
        upload_key=request.upload_key,
        request_id=request_id,
    )

    # Validate upload session
    session = _upload_sessions.get(request.upload_key)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "upload_session_not_found",
                "message": "Upload session not found or expired",
                "details": {"upload_key": request.upload_key},
            },
        )

    # Check if file was actually uploaded
    if "staging_path" not in session:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "file_not_uploaded",
                "message": "File has not been uploaded yet",
                "details": {"upload_key": request.upload_key},
            },
        )

    staging_path = Path(session["staging_path"])
    if not staging_path.exists():
        del _upload_sessions[request.upload_key]
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "staged_file_not_found",
                "message": "Staged file not found (may have been cleaned up)",
                "details": {"upload_key": request.upload_key},
            },
        )

    # Determine final filename and path
    final_name = request.name or session["filename"]
    permanent_dir = _get_permanent_dir(project_id)
    permanent_dir.mkdir(parents=True, exist_ok=True)

    # Generate unique file ID
    file_id = str(uuid.uuid4())

    # Build permanent path with file_id to ensure uniqueness
    permanent_filename = f"{file_id}_{final_name}"
    permanent_path = permanent_dir / permanent_filename

    try:
        # Move file from staging to permanent storage
        shutil.move(str(staging_path), str(permanent_path))

        # Compute relative path for storage
        relative_path = str(permanent_path.relative_to(settings.files_dir))

        # Merge tags from prepare and register
        tags = {**(session.get("tags") or {}), **(request.tags or {})}

        # Create database record
        file_record = metadata_db.create_file_record(
            file_id=file_id,
            project_id=project_id,
            name=final_name,
            path=relative_path,
            size_bytes=session["size_bytes"],
            content_type=session.get("content_type"),
            checksum_sha256=session["checksum_sha256"],
            is_staged=False,
            expires_at=None,  # Permanent file - no expiration
            tags=tags if tags else None,
        )

        # Clean up session
        del _upload_sessions[request.upload_key]

        duration_ms = int((time.time() - start_time) * 1000)

        logger.info(
            "register_file_complete",
            project_id=project_id,
            file_id=file_id,
            name=final_name,
            size_bytes=session["size_bytes"],
            duration_ms=duration_ms,
            request_id=request_id,
        )

        # Log operation
        metadata_db.log_operation(
            operation="register_file",
            status="success",
            project_id=project_id,
            request_id=request_id,
            resource_type="file",
            resource_id=file_id,
            details={"name": final_name, "size_bytes": session["size_bytes"]},
            duration_ms=duration_ms,
        )

        return FileInfoResponse(
            id=file_record["id"],
            project_id=file_record["project_id"],
            name=file_record["name"],
            path=file_record["path"],
            size_bytes=file_record["size_bytes"],
            content_type=file_record.get("content_type"),
            checksum_sha256=file_record.get("checksum_sha256"),
            is_staged=file_record["is_staged"],
            created_at=file_record["created_at"],
            expires_at=file_record.get("expires_at"),
            tags=file_record.get("tags"),
        )

    except Exception as e:
        logger.error(
            "register_file_error",
            project_id=project_id,
            upload_key=request.upload_key,
            error=str(e),
            request_id=request_id,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "registration_failed",
                "message": f"Failed to register file: {str(e)}",
            },
        )


@router.get(
    "/projects/{project_id}/files",
    response_model=FileListResponse,
    responses={
        404: {"model": ErrorResponse},
    },
    summary="List files",
    description="List all registered files for a project.",
    dependencies=[Depends(require_project_access)],
)
async def list_files(
    project_id: str,
    include_staged: bool = Query(False, description="Include staging files"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum files to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> FileListResponse:
    """List all files for a project."""
    request_id = _get_request_id()

    logger.info(
        "list_files",
        project_id=project_id,
        include_staged=include_staged,
        limit=limit,
        offset=offset,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Get files from database
    files = metadata_db.list_files(
        project_id=project_id,
        include_staged=include_staged,
        limit=limit,
        offset=offset,
    )

    total = metadata_db.count_files(project_id, include_staged=include_staged)

    return FileListResponse(
        files=[
            FileInfoResponse(
                id=f["id"],
                project_id=f["project_id"],
                name=f["name"],
                path=f["path"],
                size_bytes=f["size_bytes"],
                content_type=f.get("content_type"),
                checksum_sha256=f.get("checksum_sha256"),
                is_staged=f["is_staged"],
                created_at=f["created_at"],
                expires_at=f.get("expires_at"),
                tags=f.get("tags"),
            )
            for f in files
        ],
        total=total,
    )


@router.get(
    "/projects/{project_id}/files/{file_id}",
    response_model=FileInfoResponse,
    responses={
        404: {"model": ErrorResponse},
    },
    summary="Get file info",
    description="Get information about a specific file.",
    dependencies=[Depends(require_project_access)],
)
async def get_file(
    project_id: str,
    file_id: str,
) -> FileInfoResponse:
    """Get file information by ID."""
    request_id = _get_request_id()

    logger.info(
        "get_file",
        project_id=project_id,
        file_id=file_id,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Get file from database
    file_record = metadata_db.get_file_by_project(project_id, file_id)
    if not file_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "file_not_found",
                "message": f"File {file_id} not found in project {project_id}",
                "details": {"project_id": project_id, "file_id": file_id},
            },
        )

    return FileInfoResponse(
        id=file_record["id"],
        project_id=file_record["project_id"],
        name=file_record["name"],
        path=file_record["path"],
        size_bytes=file_record["size_bytes"],
        content_type=file_record.get("content_type"),
        checksum_sha256=file_record.get("checksum_sha256"),
        is_staged=file_record["is_staged"],
        created_at=file_record["created_at"],
        expires_at=file_record.get("expires_at"),
        tags=file_record.get("tags"),
    )


@router.get(
    "/projects/{project_id}/files/{file_id}/download",
    responses={
        404: {"model": ErrorResponse},
    },
    summary="Download file",
    description="Download a file's content.",
    dependencies=[Depends(require_project_access)],
)
async def download_file(
    project_id: str,
    file_id: str,
) -> FileResponse:
    """Download a file."""
    request_id = _get_request_id()

    logger.info(
        "download_file",
        project_id=project_id,
        file_id=file_id,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Get file from database
    file_record = metadata_db.get_file_by_project(project_id, file_id)
    if not file_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "file_not_found",
                "message": f"File {file_id} not found in project {project_id}",
                "details": {"project_id": project_id, "file_id": file_id},
            },
        )

    # Build full path
    file_path = settings.files_dir / file_record["path"]

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "file_content_not_found",
                "message": "File content not found on disk",
                "details": {"file_id": file_id},
            },
        )

    metrics.FILES_DOWNLOADS_TOTAL.labels(status="success").inc()
    metrics.FILES_DOWNLOAD_BYTES_TOTAL.inc(file_record["size_bytes"])

    return FileResponse(
        path=str(file_path),
        filename=file_record["name"],
        media_type=file_record.get("content_type") or "application/octet-stream",
    )


@router.delete(
    "/projects/{project_id}/files/{file_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"model": ErrorResponse},
    },
    summary="Delete file",
    description="Delete a file from storage.",
    dependencies=[Depends(require_project_access)],
)
async def delete_file(
    project_id: str,
    file_id: str,
) -> None:
    """Delete a file."""
    start_time = time.time()
    request_id = _get_request_id()

    logger.info(
        "delete_file_start",
        project_id=project_id,
        file_id=file_id,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Get file from database
    file_record = metadata_db.get_file_by_project(project_id, file_id)
    if not file_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "file_not_found",
                "message": f"File {file_id} not found in project {project_id}",
                "details": {"project_id": project_id, "file_id": file_id},
            },
        )

    # Delete physical file
    file_path = settings.files_dir / file_record["path"]
    if file_path.exists():
        file_path.unlink()

    # Delete database record
    metadata_db.delete_file(file_id)

    duration_ms = int((time.time() - start_time) * 1000)

    logger.info(
        "delete_file_complete",
        project_id=project_id,
        file_id=file_id,
        duration_ms=duration_ms,
        request_id=request_id,
    )

    # Log operation
    metadata_db.log_operation(
        operation="delete_file",
        status="success",
        project_id=project_id,
        request_id=request_id,
        resource_type="file",
        resource_id=file_id,
        duration_ms=duration_ms,
    )
