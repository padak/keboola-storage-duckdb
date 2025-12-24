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
- Presign (POST /{bucket}/presign) - Generate pre-signed URLs

Authentication (in order of precedence):
1. AWS Signature V4 - Authorization: AWS4-HMAC-SHA256 ...
2. Pre-signed URL with signature query parameter
3. Authorization: Bearer {api_key}
4. X-Api-Key header
"""

import base64
import hashlib
import hmac
import re
import secrets
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from src import metrics
from src.config import settings
from src.database import metadata_db
from src.dependencies import require_s3_bucket_access, get_api_key_flexible, verify_admin_key, verify_project_key

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
# Pre-signed URL Support
# ============================================================================


def _get_signing_key() -> bytes:
    """Get the secret key for signing URLs.

    If not configured, generates a random key (persisted in memory).
    """
    if settings.presign_secret_key:
        return settings.presign_secret_key.encode()

    # Generate a random key if not configured
    # Note: This means signed URLs won't survive server restarts
    if not hasattr(_get_signing_key, "_cached_key"):
        _get_signing_key._cached_key = secrets.token_bytes(32)
        logger.warning("presign_using_generated_key", note="URLs won't survive restart")
    return _get_signing_key._cached_key


class PresignMethod(str, Enum):
    """HTTP methods that can be pre-signed."""

    GET = "GET"
    PUT = "PUT"
    DELETE = "DELETE"
    HEAD = "HEAD"


class PresignRequest(BaseModel):
    """Request body for generating pre-signed URLs."""

    key: str = Field(..., description="The object key (path)")
    method: PresignMethod = Field(PresignMethod.GET, description="HTTP method")
    expires_in: int = Field(
        default=3600,
        ge=1,
        le=604800,
        description="URL expiry in seconds (1-604800, default 3600)",
    )
    content_type: str | None = Field(None, description="Content-Type for PUT requests")


class PresignResponse(BaseModel):
    """Response from pre-signed URL generation."""

    url: str = Field(..., description="The pre-signed URL")
    expires_at: str = Field(..., description="Expiration timestamp (ISO 8601)")
    method: str = Field(..., description="HTTP method this URL is valid for")


def _sign_url(
    method: str,
    bucket: str,
    key: str,
    expires_at: int,
    content_type: str | None = None,
) -> str:
    """Generate HMAC signature for a pre-signed URL.

    Args:
        method: HTTP method (GET, PUT, DELETE, HEAD)
        bucket: S3 bucket name
        key: Object key
        expires_at: Unix timestamp when URL expires
        content_type: Optional content type (for PUT)

    Returns:
        Base64-encoded HMAC-SHA256 signature
    """
    # Build string to sign
    string_to_sign = f"{method}\n{bucket}\n{key}\n{expires_at}"
    if content_type:
        string_to_sign += f"\n{content_type}"

    # Sign with HMAC-SHA256
    signature = hmac.new(
        _get_signing_key(),
        string_to_sign.encode(),
        hashlib.sha256,
    ).digest()

    # URL-safe base64 encoding
    return base64.urlsafe_b64encode(signature).decode().rstrip("=")


def _verify_signature(
    method: str,
    bucket: str,
    key: str,
    expires_at: int,
    signature: str,
    content_type: str | None = None,
) -> bool:
    """Verify a pre-signed URL signature.

    Args:
        method: HTTP method
        bucket: S3 bucket name
        key: Object key
        expires_at: Unix timestamp when URL expires
        signature: The signature to verify
        content_type: Optional content type

    Returns:
        True if signature is valid and not expired
    """
    # Check expiration
    now = int(time.time())
    if now > expires_at:
        logger.debug("presign_expired", expires_at=expires_at, now=now)
        return False

    # Compute expected signature
    expected = _sign_url(method, bucket, key, expires_at, content_type)

    # Constant-time comparison
    return hmac.compare_digest(signature, expected)


# ============================================================================
# AWS Signature V4 Support
# ============================================================================


def _parse_aws_auth_header(auth_header: str) -> dict | None:
    """Parse AWS4-HMAC-SHA256 Authorization header.

    Expected format:
        AWS4-HMAC-SHA256
        Credential={access_key}/{date}/{region}/{service}/aws4_request,
        SignedHeaders={header_list},
        Signature={signature}

    Returns:
        Dictionary with parsed components or None if invalid format.
    """
    if not auth_header.startswith("AWS4-HMAC-SHA256"):
        return None

    # Remove "AWS4-HMAC-SHA256 " prefix and parse key=value pairs
    # Handle both space-separated and comma-separated formats
    content = auth_header[len("AWS4-HMAC-SHA256"):].strip()

    # Extract Credential
    cred_match = re.search(r"Credential=([^,\s]+)", content)
    if not cred_match:
        return None
    credential = cred_match.group(1)

    # Parse credential: access_key/date/region/service/aws4_request
    cred_parts = credential.split("/")
    if len(cred_parts) != 5 or cred_parts[4] != "aws4_request":
        return None

    # Extract SignedHeaders
    headers_match = re.search(r"SignedHeaders=([^,\s]+)", content)
    if not headers_match:
        return None
    signed_headers = headers_match.group(1).split(";")

    # Extract Signature
    sig_match = re.search(r"Signature=([a-fA-F0-9]+)", content)
    if not sig_match:
        return None
    signature = sig_match.group(1)

    return {
        "access_key": cred_parts[0],
        "date": cred_parts[1],  # YYYYMMDD format
        "region": cred_parts[2],
        "service": cred_parts[3],
        "signed_headers": signed_headers,
        "signature": signature.lower(),  # Normalize to lowercase
    }


def _derive_signing_key(secret_key: str, date: str, region: str, service: str) -> bytes:
    """Derive AWS Sig V4 signing key.

    The signing key is derived as:
        kDate = HMAC("AWS4" + secret_key, date)
        kRegion = HMAC(kDate, region)
        kService = HMAC(kRegion, service)
        kSigning = HMAC(kService, "aws4_request")

    Args:
        secret_key: The AWS secret access key
        date: Date in YYYYMMDD format
        region: AWS region (e.g., "us-east-1" or "local")
        service: Service name (e.g., "s3")

    Returns:
        The derived signing key as bytes
    """
    k_date = hmac.new(f"AWS4{secret_key}".encode(), date.encode(), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    return k_signing


def _build_canonical_request(
    method: str,
    uri: str,
    query_string: str,
    headers: dict[str, str],
    signed_headers: list[str],
    payload_hash: str,
) -> str:
    """Build AWS Sig V4 canonical request.

    Format:
        {HTTP_METHOD}\\n
        {URI}\\n
        {QUERY_STRING}\\n
        {CANONICAL_HEADERS}\\n
        {SIGNED_HEADERS}\\n
        {HASHED_PAYLOAD}
    """
    # Sort signed headers
    sorted_headers = sorted(signed_headers)

    # Build canonical headers (lowercase name: trimmed value\n)
    canonical_headers = ""
    for h in sorted_headers:
        value = headers.get(h.lower(), "")
        # Trim whitespace and collapse multiple spaces
        value = " ".join(value.split())
        canonical_headers += f"{h.lower()}:{value}\n"

    signed_headers_str = ";".join(sorted_headers)

    # URL-encode URI path (but not the slashes)
    # boto3 sends URI already encoded, so we need to handle that
    canonical_uri = uri
    if not canonical_uri.startswith("/"):
        canonical_uri = "/" + canonical_uri

    # Sort query string parameters
    if query_string:
        params = urllib.parse.parse_qsl(query_string, keep_blank_values=True)
        sorted_params = sorted(params, key=lambda x: (x[0], x[1]))
        canonical_qs = urllib.parse.urlencode(sorted_params, quote_via=urllib.parse.quote)
    else:
        canonical_qs = ""

    return f"{method}\n{canonical_uri}\n{canonical_qs}\n{canonical_headers}\n{signed_headers_str}\n{payload_hash}"


def _verify_aws_sig_v4(request: Request, bucket: str, key: str) -> tuple[bool, str | None]:
    """Verify AWS Signature V4 authentication.

    Args:
        request: The FastAPI request
        bucket: S3 bucket name
        key: Object key (can be empty for bucket operations)

    Returns:
        Tuple of (is_valid, access_key) where access_key is None if invalid
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("AWS4-HMAC-SHA256"):
        return False, None

    parsed = _parse_aws_auth_header(auth_header)
    if not parsed:
        logger.debug("aws_sig_v4_parse_failed", auth_header=auth_header[:50])
        return False, None

    access_key = parsed["access_key"]

    # Get the S3 secret key from settings
    # We use a single S3 credential pair for simplicity
    expected_access_key = settings.s3_access_key_id
    secret_key = settings.s3_secret_access_key or settings.admin_api_key

    if not secret_key:
        logger.warning("aws_sig_v4_no_secret_key", msg="S3_SECRET_ACCESS_KEY not configured")
        return False, None

    # Verify access key matches
    if access_key != expected_access_key:
        logger.debug("aws_sig_v4_wrong_access_key", provided=access_key, expected=expected_access_key)
        return False, None

    # Get required headers
    x_amz_date = request.headers.get("x-amz-date", "")
    x_amz_content_sha256 = request.headers.get("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

    if not x_amz_date:
        logger.debug("aws_sig_v4_missing_date")
        return False, None

    # Check request age (prevent replay attacks)
    try:
        # x-amz-date format: YYYYMMDDTHHMMSSZ
        request_time = datetime.strptime(x_amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        age_seconds = abs((now - request_time).total_seconds())
        if age_seconds > settings.s3_sig_v4_max_age_seconds:
            logger.debug("aws_sig_v4_request_too_old", age_seconds=age_seconds)
            return False, None
    except ValueError:
        logger.debug("aws_sig_v4_invalid_date_format", x_amz_date=x_amz_date)
        return False, None

    # Build canonical URI
    if key:
        uri = f"/s3/{bucket}/{key}"
    else:
        uri = f"/s3/{bucket}"

    # Get query string (excluding signature-related params which boto3 doesn't use in header auth)
    query_string = str(request.url.query) if request.url.query else ""

    # Build headers dict with lowercase keys
    headers_dict = {k.lower(): v for k, v in request.headers.items()}

    # Build canonical request
    canonical_request = _build_canonical_request(
        method=request.method,
        uri=uri,
        query_string=query_string,
        headers=headers_dict,
        signed_headers=parsed["signed_headers"],
        payload_hash=x_amz_content_sha256,
    )

    # Build string to sign
    scope = f"{parsed['date']}/{parsed['region']}/{parsed['service']}/aws4_request"
    canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
    string_to_sign = f"AWS4-HMAC-SHA256\n{x_amz_date}\n{scope}\n{canonical_request_hash}"

    # Derive signing key and compute expected signature
    signing_key = _derive_signing_key(secret_key, parsed["date"], parsed["region"], parsed["service"])
    expected_sig = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()

    # Constant-time comparison
    if hmac.compare_digest(expected_sig, parsed["signature"]):
        logger.info(
            "aws_sig_v4_access_granted",
            bucket=bucket,
            key=key,
            method=request.method,
            access_key=access_key,
        )
        return True, access_key

    logger.debug(
        "aws_sig_v4_signature_mismatch",
        expected=expected_sig[:16] + "...",
        provided=parsed["signature"][:16] + "...",
    )
    return False, None


async def _check_presign_or_auth(
    request: Request,
    bucket: str,
    key: str,
    method: str,
) -> bool:
    """Check if request has valid authentication.

    Authentication methods (checked in order):
    1. AWS Signature V4 (Authorization: AWS4-HMAC-SHA256 ...)
    2. Pre-signed URL (?signature=...&expires=...)
    3. Bearer token (Authorization: Bearer ...)
    4. X-Api-Key header

    Args:
        request: The FastAPI request
        bucket: S3 bucket name
        key: Object key
        method: HTTP method

    Returns:
        True if authorized

    Raises:
        HTTPException: If not authenticated
    """
    # 1. Check for AWS Signature V4 (boto3, aws-cli, rclone)
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("AWS4-HMAC-SHA256"):
        is_valid, access_key = _verify_aws_sig_v4(request, bucket, key)
        if is_valid:
            # Validate that project exists (bucket = project_xxx)
            project_id = _extract_project_id(bucket)
            project = metadata_db.get_project(project_id)
            if not project:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={"Code": "NoSuchBucket", "Message": "The specified bucket does not exist"},
                )
            return True
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"Code": "SignatureDoesNotMatch", "Message": "The request signature we calculated does not match the signature you provided"},
            )

    # 2. Check for pre-signed URL parameters
    signature = request.query_params.get("X-Amz-Signature") or request.query_params.get("signature")
    expires = request.query_params.get("X-Amz-Expires") or request.query_params.get("expires")
    content_type = request.query_params.get("X-Amz-Content-Type") or request.query_params.get("content_type")

    if signature and expires:
        try:
            expires_at = int(float(expires))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"Code": "InvalidArgument", "Message": "Invalid expires value"},
            )

        if _verify_signature(method, bucket, key, expires_at, signature, content_type):
            logger.info("presign_access_granted", bucket=bucket, key=key, method=method)
            return True
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"Code": "SignatureDoesNotMatch", "Message": "The request signature is invalid or expired"},
            )

    # 3. Fall back to Bearer token / X-Api-Key header-based auth
    try:
        api_key = await get_api_key_flexible(request)
    except HTTPException:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"Code": "AccessDenied", "Message": "Missing authentication"},
        )

    # Extract project_id from bucket name
    project_id = _extract_project_id(bucket)

    # Verify access
    if verify_admin_key(api_key):
        return True

    if verify_project_key(api_key, project_id):
        return True

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={"Code": "AccessDenied", "Message": "Access denied to this bucket"},
    )


# ============================================================================
# S3 API Endpoints
# ============================================================================


@router.get(
    "/{bucket}/{key:path}",
    summary="GetObject",
    description="Download an object from the bucket. Supports pre-signed URLs.",
    responses={
        200: {"description": "Object content"},
        401: {"description": "Unauthorized - Missing authentication"},
        403: {"description": "Forbidden - Invalid signature or access denied"},
        404: {"description": "NoSuchKey - Object not found"},
    },
)
async def get_object(
    bucket: str,
    key: str,
    request: Request,
) -> FileResponse:
    """S3 GetObject - Download a file.

    Supports both header-based auth and pre-signed URLs.
    """
    # Check auth (pre-signed URL or header)
    await _check_presign_or_auth(request, bucket, key, "GET")

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

    # Record metrics
    metrics.S3_OPERATIONS_TOTAL.labels(operation="GetObject", status="success").inc()
    metrics.S3_BYTES_OUT_TOTAL.inc(stat.st_size)

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
    description="Upload an object to the bucket. Supports pre-signed URLs.",
    responses={
        200: {"description": "Object created successfully"},
        400: {"description": "BadDigest - Content-MD5 mismatch"},
        401: {"description": "Unauthorized - Missing authentication"},
        403: {"description": "Forbidden - Invalid signature or access denied"},
    },
)
async def put_object(
    bucket: str,
    key: str,
    request: Request,
) -> Response:
    """S3 PutObject - Upload a file.

    Supports both header-based auth and pre-signed URLs.
    """
    # Check auth (pre-signed URL or header)
    await _check_presign_or_auth(request, bucket, key, "PUT")

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

    # Record metrics
    metrics.S3_OPERATIONS_TOTAL.labels(operation="PutObject", status="success").inc()
    metrics.S3_OPERATION_DURATION.labels(operation="PutObject").observe(time.time() - start_time)
    metrics.S3_BYTES_IN_TOTAL.inc(len(content))

    return Response(
        status_code=200,
        headers={"ETag": f'"{etag}"'},
    )


@router.delete(
    "/{bucket}/{key:path}",
    summary="DeleteObject",
    description="Delete an object from the bucket. Supports pre-signed URLs.",
    responses={
        204: {"description": "Object deleted successfully"},
        401: {"description": "Unauthorized - Missing authentication"},
        403: {"description": "Forbidden - Invalid signature or access denied"},
    },
)
async def delete_object(
    bucket: str,
    key: str,
    request: Request,
) -> Response:
    """S3 DeleteObject - Delete a file.

    Supports both header-based auth and pre-signed URLs.
    Note: S3 returns 204 even if the key doesn't exist.
    """
    # Check auth (pre-signed URL or header)
    await _check_presign_or_auth(request, bucket, key, "DELETE")

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

    # Record metrics
    metrics.S3_OPERATIONS_TOTAL.labels(operation="DeleteObject", status="success").inc()

    return Response(status_code=204)


@router.head(
    "/{bucket}/{key:path}",
    summary="HeadObject",
    description="Get metadata about an object without downloading it. Supports pre-signed URLs.",
    responses={
        200: {"description": "Object metadata"},
        401: {"description": "Unauthorized - Missing authentication"},
        403: {"description": "Forbidden - Invalid signature or access denied"},
        404: {"description": "NoSuchKey - Object not found"},
    },
)
async def head_object(
    bucket: str,
    key: str,
    request: Request,
) -> Response:
    """S3 HeadObject - Get file metadata without content.

    Supports both header-based auth and pre-signed URLs.
    """
    # Check auth (pre-signed URL or header)
    await _check_presign_or_auth(request, bucket, key, "HEAD")

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

    # Record metrics
    metrics.S3_OPERATIONS_TOTAL.labels(operation="HeadObject", status="success").inc()

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

    # Record metrics
    metrics.S3_OPERATIONS_TOTAL.labels(operation="ListObjectsV2", status="success").inc()

    return Response(content=xml_content, media_type="application/xml")


# ============================================================================
# Pre-signed URL Endpoint
# ============================================================================


@router.post(
    "/{bucket}/presign",
    summary="Generate Pre-signed URL",
    description="Generate a pre-signed URL for S3 operations.",
    response_model=PresignResponse,
    responses={
        200: {"description": "Pre-signed URL generated"},
        403: {"description": "Access denied"},
        404: {"description": "NoSuchBucket - Bucket not found"},
    },
    dependencies=[Depends(require_s3_bucket_access)],
)
async def create_presigned_url(
    bucket: str,
    request_body: PresignRequest,
    request: Request,
) -> PresignResponse:
    """Generate a pre-signed URL for S3 operations.

    The generated URL can be used without authentication headers.
    It includes a signature that validates access to the specific
    bucket, key, and method combination.
    """
    request_id = _get_request_id()
    project_id = _extract_project_id(bucket)

    logger.info(
        "s3_presign_request",
        bucket=bucket,
        key=request_body.key,
        method=request_body.method,
        expires_in=request_body.expires_in,
        project_id=project_id,
        request_id=request_id,
    )

    # Validate project exists
    _validate_project_exists(project_id)

    # Validate expiry is within limits
    max_expiry = settings.presign_max_expiry
    if request_body.expires_in > max_expiry:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "Code": "InvalidArgument",
                "Message": f"Expiry cannot exceed {max_expiry} seconds",
            },
        )

    # Calculate expiration timestamp
    expires_at = int(time.time()) + request_body.expires_in
    expires_at_dt = datetime.fromtimestamp(expires_at, tz=timezone.utc)

    # Generate signature
    signature = _sign_url(
        method=request_body.method.value,
        bucket=bucket,
        key=request_body.key,
        expires_at=expires_at,
        content_type=request_body.content_type,
    )

    # Build URL with query parameters
    base_url = settings.base_url.rstrip("/")
    encoded_key = urllib.parse.quote(request_body.key, safe="/")

    query_params = {
        "signature": signature,
        "expires": str(expires_at),
    }
    if request_body.content_type:
        query_params["content_type"] = request_body.content_type

    query_string = urllib.parse.urlencode(query_params)
    url = f"{base_url}/s3/{bucket}/{encoded_key}?{query_string}"

    logger.info(
        "s3_presign_generated",
        bucket=bucket,
        key=request_body.key,
        method=request_body.method,
        expires_at=expires_at_dt.isoformat(),
        request_id=request_id,
    )

    # Record metrics
    metrics.S3_PRESIGN_REQUESTS_TOTAL.labels(method=request_body.method.value).inc()

    return PresignResponse(
        url=url,
        expires_at=expires_at_dt.isoformat(),
        method=request_body.method.value,
    )
