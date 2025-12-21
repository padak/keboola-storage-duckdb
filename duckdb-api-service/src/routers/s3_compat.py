"""S3-Compatible API endpoints for DuckDB file storage.

This module implements a subset of the AWS S3 API to enable Keboola Connection
to use DuckDB API Service for file storage instead of AWS S3/Azure ABS/GCS.

Bucket mapping:
- S3 bucket name = project_id (e.g., "project_123")
- S3 key = file path within project

Supported operations:
- GetObject (GET /{bucket}/{key})
- PutObject (PUT /{bucket}/{key})
- DeleteObject (DELETE /{bucket}/{key})
- HeadObject (HEAD /{bucket}/{key})
- ListObjectsV2 (GET /{bucket}?list-type=2)

Authentication:
- Authorization: Bearer {api_key} (same as REST API)
"""

import base64
import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, StreamingResponse

from src.config import settings
from src.database import metadata_db
from src.dependencies import require_s3_bucket_access

logger = structlog.get_logger()

router = APIRouter(prefix="/s3", tags=["s3-compat"])


def _get_request_id() -> str | None:
    """Get current request ID from context (if available)."""
    try:
        return structlog.contextvars.get_contextvars().get("request_id")
    except Exception:
        return None


def _get_project_files_dir(project_id: str) -> Path:
    """Get the files directory for a project."""
    return settings.files_dir / f"project_{project_id}"


def _extract_project_id(bucket: str) -> str:
    """Extract project ID from S3 bucket name.

    Bucket can be:
    - "project_123" -> "123"
    - "123" -> "123"
    """
    if bucket.startswith("project_"):
        return bucket[8:]  # Remove "project_" prefix
    return bucket


def _compute_md5(content: bytes) -> str:
    """Compute MD5 hash of content (for ETag)."""
    return hashlib.md5(content).hexdigest()


def _compute_file_md5(file_path: Path) -> str:
    """Compute MD5 hash of a file."""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def _format_s3_timestamp(dt: datetime) -> str:
    """Format datetime for S3 XML response."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _format_http_date(timestamp: float) -> str:
    """Format timestamp as HTTP-date (RFC 7231)."""
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


def _build_list_objects_xml(
    bucket: str,
    objects: list[dict[str, Any]],
    prefix: str = "",
    is_truncated: bool = False,
    continuation_token: str | None = None,
    next_continuation_token: str | None = None,
    max_keys: int = 1000,
    common_prefixes: list[str] | None = None,
) -> str:
    """Build S3 ListObjectsV2 XML response."""
    root = ET.Element("ListBucketResult")

    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    ET.SubElement(root, "KeyCount").text = str(len(objects))
    ET.SubElement(root, "IsTruncated").text = str(is_truncated).lower()

    if continuation_token:
        ET.SubElement(root, "ContinuationToken").text = continuation_token
    if next_continuation_token:
        ET.SubElement(root, "NextContinuationToken").text = next_continuation_token

    for obj in objects:
        contents = ET.SubElement(root, "Contents")
        ET.SubElement(contents, "Key").text = obj["Key"]
        ET.SubElement(contents, "LastModified").text = _format_s3_timestamp(obj["LastModified"])
        ET.SubElement(contents, "ETag").text = f'"{obj["ETag"]}"'
        ET.SubElement(contents, "Size").text = str(obj["Size"])
        ET.SubElement(contents, "StorageClass").text = "STANDARD"

    if common_prefixes:
        for cp in common_prefixes:
            cp_elem = ET.SubElement(root, "CommonPrefixes")
            ET.SubElement(cp_elem, "Prefix").text = cp

    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _build_error_xml(code: str, message: str, resource: str = "", request_id: str = "") -> str:
    """Build S3 error XML response."""
    root = ET.Element("Error")
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    ET.SubElement(root, "Resource").text = resource
    ET.SubElement(root, "RequestId").text = request_id
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _validate_project_exists(project_id: str) -> dict[str, Any]:
    """Validate that project exists."""
    project = metadata_db.get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"Code": "NoSuchBucket", "Message": f"The specified bucket does not exist"},
        )
    return project


# ============================================================================
# S3 API Endpoints
# ============================================================================


@router.get(
    "/{bucket}/{key:path}",
    summary="GetObject",
    description="Download an object from the bucket.",
    responses={
        200: {"description": "Object content"},
        404: {"description": "NoSuchKey - Object not found"},
    },
    dependencies=[Depends(require_s3_bucket_access)],
)
async def get_object(
    bucket: str,
    key: str,
    request: Request,
) -> FileResponse:
    """S3 GetObject - Download a file."""
    request_id = _get_request_id()
    project_id = _extract_project_id(bucket)

    logger.info(
        "s3_get_object",
        bucket=bucket,
        key=key,
        project_id=project_id,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Build file path
    file_path = _get_project_files_dir(project_id) / key

    if not file_path.exists():
        return Response(
            content=_build_error_xml(
                "NoSuchKey",
                "The specified key does not exist.",
                f"/{bucket}/{key}",
                request_id or "",
            ),
            status_code=404,
            media_type="application/xml",
        )

    if not file_path.is_file():
        return Response(
            content=_build_error_xml(
                "NoSuchKey",
                "The specified key does not exist.",
                f"/{bucket}/{key}",
                request_id or "",
            ),
            status_code=404,
            media_type="application/xml",
        )

    # Compute ETag
    etag = _compute_file_md5(file_path)
    stat = file_path.stat()

    return FileResponse(
        path=str(file_path),
        headers={
            "ETag": f'"{etag}"',
            "Content-Length": str(stat.st_size),
            "Last-Modified": _format_http_date(stat.st_mtime),
            "Accept-Ranges": "bytes",
        },
    )


@router.put(
    "/{bucket}/{key:path}",
    summary="PutObject",
    description="Upload an object to the bucket.",
    responses={
        200: {"description": "Object created successfully"},
        400: {"description": "BadDigest - Content-MD5 mismatch"},
    },
    dependencies=[Depends(require_s3_bucket_access)],
)
async def put_object(
    bucket: str,
    key: str,
    request: Request,
) -> Response:
    """S3 PutObject - Upload a file."""
    start_time = time.time()
    request_id = _get_request_id()
    project_id = _extract_project_id(bucket)

    logger.info(
        "s3_put_object_start",
        bucket=bucket,
        key=key,
        project_id=project_id,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Build file path
    file_path = _get_project_files_dir(project_id) / key

    # Create parent directories
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read request body
    content = await request.body()

    # Verify Content-MD5 if provided
    content_md5_header = request.headers.get("Content-MD5")
    if content_md5_header:
        actual_md5_b64 = base64.b64encode(hashlib.md5(content).digest()).decode()
        if actual_md5_b64 != content_md5_header:
            return Response(
                content=_build_error_xml(
                    "BadDigest",
                    "The Content-MD5 you specified did not match what we received.",
                    f"/{bucket}/{key}",
                    request_id or "",
                ),
                status_code=400,
                media_type="application/xml",
            )

    # Write file
    file_path.write_bytes(content)

    # Compute ETag
    etag = _compute_md5(content)

    duration_ms = int((time.time() - start_time) * 1000)

    logger.info(
        "s3_put_object_complete",
        bucket=bucket,
        key=key,
        project_id=project_id,
        size_bytes=len(content),
        duration_ms=duration_ms,
        request_id=request_id,
    )

    return Response(
        status_code=200,
        headers={"ETag": f'"{etag}"'},
    )


@router.delete(
    "/{bucket}/{key:path}",
    summary="DeleteObject",
    description="Delete an object from the bucket.",
    responses={
        204: {"description": "Object deleted successfully"},
    },
    dependencies=[Depends(require_s3_bucket_access)],
)
async def delete_object(
    bucket: str,
    key: str,
    request: Request,
) -> Response:
    """S3 DeleteObject - Delete a file.

    Note: S3 returns 204 even if the key doesn't exist.
    """
    request_id = _get_request_id()
    project_id = _extract_project_id(bucket)

    logger.info(
        "s3_delete_object",
        bucket=bucket,
        key=key,
        project_id=project_id,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Build file path
    file_path = _get_project_files_dir(project_id) / key

    # Delete file if exists (S3 is idempotent - returns 204 even if not found)
    if file_path.exists() and file_path.is_file():
        file_path.unlink()

        # Try to remove empty parent directories
        try:
            parent = file_path.parent
            project_dir = _get_project_files_dir(project_id)
            while parent != project_dir:
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent
                else:
                    break
        except Exception:
            pass  # Ignore errors during cleanup

    return Response(status_code=204)


@router.head(
    "/{bucket}/{key:path}",
    summary="HeadObject",
    description="Get metadata about an object without downloading it.",
    responses={
        200: {"description": "Object metadata"},
        404: {"description": "NoSuchKey - Object not found"},
    },
    dependencies=[Depends(require_s3_bucket_access)],
)
async def head_object(
    bucket: str,
    key: str,
    request: Request,
) -> Response:
    """S3 HeadObject - Get file metadata without content."""
    request_id = _get_request_id()
    project_id = _extract_project_id(bucket)

    logger.info(
        "s3_head_object",
        bucket=bucket,
        key=key,
        project_id=project_id,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Build file path
    file_path = _get_project_files_dir(project_id) / key

    if not file_path.exists() or not file_path.is_file():
        return Response(status_code=404)

    # Compute ETag and get stats
    etag = _compute_file_md5(file_path)
    stat = file_path.stat()

    return Response(
        status_code=200,
        headers={
            "ETag": f'"{etag}"',
            "Content-Length": str(stat.st_size),
            "Content-Type": "application/octet-stream",
            "Last-Modified": _format_http_date(stat.st_mtime),
            "Accept-Ranges": "bytes",
        },
    )


@router.get(
    "/{bucket}",
    summary="ListObjectsV2",
    description="List objects in a bucket.",
    responses={
        200: {"description": "XML list of objects"},
        404: {"description": "NoSuchBucket - Bucket not found"},
    },
    dependencies=[Depends(require_s3_bucket_access)],
)
async def list_objects_v2(
    bucket: str,
    request: Request,
    list_type: int = Query(2, alias="list-type"),
    prefix: str = Query("", alias="prefix"),
    delimiter: str = Query("", alias="delimiter"),
    max_keys: int = Query(1000, alias="max-keys", ge=1, le=1000),
    continuation_token: str = Query(None, alias="continuation-token"),
    start_after: str = Query(None, alias="start-after"),
) -> Response:
    """S3 ListObjectsV2 - List files in bucket."""
    request_id = _get_request_id()
    project_id = _extract_project_id(bucket)

    logger.info(
        "s3_list_objects",
        bucket=bucket,
        project_id=project_id,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Get project files directory
    project_dir = _get_project_files_dir(project_id)

    if not project_dir.exists():
        # Empty bucket - return empty list
        xml_content = _build_list_objects_xml(
            bucket=bucket,
            objects=[],
            prefix=prefix,
            is_truncated=False,
            max_keys=max_keys,
        )
        return Response(content=xml_content, media_type="application/xml")

    # Collect all files
    objects: list[dict[str, Any]] = []
    common_prefixes: set[str] = set()

    for file_path in project_dir.rglob("*"):
        if not file_path.is_file():
            continue

        # Get relative key
        key = str(file_path.relative_to(project_dir))

        # Filter by prefix
        if prefix and not key.startswith(prefix):
            continue

        # Handle start_after
        if start_after and key <= start_after:
            continue

        # Handle delimiter (for virtual directories)
        if delimiter:
            # Find the part after prefix
            suffix = key[len(prefix):]
            if delimiter in suffix:
                # This is a "directory" - add to common prefixes
                common_prefix = prefix + suffix.split(delimiter)[0] + delimiter
                common_prefixes.add(common_prefix)
                continue

        # Get file stats
        stat = file_path.stat()

        objects.append({
            "Key": key,
            "Size": stat.st_size,
            "LastModified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            "ETag": _compute_file_md5(file_path),
        })

    # Sort by key
    objects.sort(key=lambda x: x["Key"])

    # Handle pagination
    is_truncated = len(objects) > max_keys
    if is_truncated:
        objects = objects[:max_keys]
        next_continuation_token = objects[-1]["Key"] if objects else None
    else:
        next_continuation_token = None

    # Build XML response
    xml_content = _build_list_objects_xml(
        bucket=bucket,
        objects=objects,
        prefix=prefix,
        is_truncated=is_truncated,
        continuation_token=continuation_token,
        next_continuation_token=next_continuation_token,
        max_keys=max_keys,
        common_prefixes=sorted(common_prefixes) if common_prefixes else None,
    )

    return Response(content=xml_content, media_type="application/xml")
