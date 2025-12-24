# Phase 18: AWS Signature V4 for S3-Compatible API

**Status:** IN PROGRESS
**Priority:** Medium
**Prerequisites:** Phase 12h.1 (S3-Compatible API) - DONE

## Goal

Implement AWS Signature V4 authentication for the S3-compatible API to enable native boto3/aws-cli/rclone compatibility.

## Background

### Current State

The S3-compatible API (`/s3/...`) supports three authentication methods:
1. **Bearer token** - `Authorization: Bearer {api_key}`
2. **X-Api-Key header** - `X-Api-Key: {api_key}`
3. **Pre-signed URLs** - `?signature={hmac}&expires={timestamp}`

These work for Connection (PHP) which uses pre-signed URLs, but **not** for standard S3 clients like boto3 which use AWS Signature V4.

### Why AWS Signature V4?

boto3 and other S3 clients automatically sign every request with AWS Sig V4:

```
Authorization: AWS4-HMAC-SHA256
  Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,
  SignedHeaders=host;x-amz-content-sha256;x-amz-date,
  Signature=34b48302e7b5fa45bde8...
```

Supporting this enables:
- **boto3** - Python AWS SDK
- **aws-cli** - AWS command line
- **rclone** - Multi-cloud sync tool
- **MinIO client** - S3 client library
- Any S3-compatible tooling

## Implementation

### Credential Mapping

Map our project credentials to AWS-style credentials:

| AWS Concept | Our Mapping |
|-------------|-------------|
| `aws_access_key_id` | Project ID (e.g., `"123"`) or `"duckdb"` for admin |
| `aws_secret_access_key` | Project API key (e.g., `"proj_123_admin_xxx"`) |
| `region` | `"local"` (ignored, but required by boto3) |

### AWS Signature V4 Algorithm

1. **Parse Authorization header:**
   ```
   AWS4-HMAC-SHA256
   Credential={access_key}/{date}/{region}/{service}/aws4_request,
   SignedHeaders={header_list},
   Signature={signature}
   ```

2. **Build Canonical Request:**
   ```
   {HTTP_METHOD}\n
   {URI}\n
   {QUERY_STRING}\n
   {CANONICAL_HEADERS}\n
   {SIGNED_HEADERS}\n
   {HASHED_PAYLOAD}
   ```

3. **Build String to Sign:**
   ```
   AWS4-HMAC-SHA256\n
   {TIMESTAMP}\n
   {SCOPE}\n
   {HASH(CANONICAL_REQUEST)}
   ```

4. **Derive Signing Key:**
   ```python
   kDate = HMAC("AWS4" + secret_key, date)
   kRegion = HMAC(kDate, region)
   kService = HMAC(kRegion, service)
   kSigning = HMAC(kService, "aws4_request")
   ```

5. **Verify Signature:**
   ```python
   expected = HMAC(kSigning, string_to_sign)
   return hmac.compare_digest(expected, provided_signature)
   ```

## Files to Modify

| File | Changes |
|------|---------|
| `src/routers/s3_compat.py` | Add `_verify_aws_sig_v4()` function |
| `src/config.py` | Add `s3_access_key_id`, `s3_secret_access_key` settings |
| `tests/test_s3_boto3_integration.py` | Remove `@pytest.mark.skip`, update credentials |
| `tests/test_s3_compat.py` | Add AWS Sig V4 unit tests |

## Implementation Steps

### Step 1: Add AWS Sig V4 Verification (~150 lines)

```python
# src/routers/s3_compat.py

import re
from urllib.parse import parse_qs, urlparse

def _parse_aws_auth_header(auth_header: str) -> dict | None:
    """Parse AWS4-HMAC-SHA256 Authorization header."""
    pattern = r'AWS4-HMAC-SHA256\s+Credential=([^,]+),\s*SignedHeaders=([^,]+),\s*Signature=(\w+)'
    match = re.match(pattern, auth_header)
    if not match:
        return None

    credential = match.group(1)
    cred_parts = credential.split('/')

    return {
        'access_key': cred_parts[0],
        'date': cred_parts[1],
        'region': cred_parts[2],
        'service': cred_parts[3],
        'signed_headers': match.group(2).split(';'),
        'signature': match.group(3),
    }


def _derive_signing_key(secret_key: str, date: str, region: str, service: str) -> bytes:
    """Derive AWS Sig V4 signing key."""
    k_date = hmac.new(f"AWS4{secret_key}".encode(), date.encode(), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    return k_signing


def _build_canonical_request(
    method: str,
    uri: str,
    query_string: str,
    headers: dict,
    signed_headers: list[str],
    payload_hash: str,
) -> str:
    """Build AWS Sig V4 canonical request."""
    # Canonical headers
    canonical_headers = ""
    for h in sorted(signed_headers):
        value = headers.get(h, "")
        canonical_headers += f"{h}:{value.strip()}\n"

    signed_headers_str = ";".join(sorted(signed_headers))

    return f"{method}\n{uri}\n{query_string}\n{canonical_headers}\n{signed_headers_str}\n{payload_hash}"


def _verify_aws_sig_v4(
    request: Request,
    bucket: str,
    key: str,
) -> tuple[bool, str | None]:
    """Verify AWS Signature V4 authentication.

    Returns:
        (is_valid, project_id) - project_id is None if invalid
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("AWS4-HMAC-SHA256"):
        return False, None

    parsed = _parse_aws_auth_header(auth_header)
    if not parsed:
        return False, None

    access_key = parsed['access_key']

    # Map access_key to project_id and get secret
    if access_key == "duckdb":
        project_id = None  # Admin access
        secret_key = settings.admin_api_key
    else:
        project_id = access_key
        # Look up project API key from database
        project = metadata_db.get_project(project_id)
        if not project:
            return False, None
        secret_key = project.get('api_key_hash')  # Need to handle this differently

    # Get required headers
    x_amz_date = request.headers.get("x-amz-date", "")
    x_amz_content_sha256 = request.headers.get("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

    # Build canonical request
    uri = f"/s3/{bucket}/{key}" if key else f"/s3/{bucket}"
    query_string = str(request.url.query) if request.url.query else ""

    headers_dict = {k.lower(): v for k, v in request.headers.items()}

    canonical_request = _build_canonical_request(
        method=request.method,
        uri=uri,
        query_string=query_string,
        headers=headers_dict,
        signed_headers=parsed['signed_headers'],
        payload_hash=x_amz_content_sha256,
    )

    # Build string to sign
    scope = f"{parsed['date']}/{parsed['region']}/{parsed['service']}/aws4_request"
    string_to_sign = f"AWS4-HMAC-SHA256\n{x_amz_date}\n{scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"

    # Derive signing key and compute signature
    signing_key = _derive_signing_key(secret_key, parsed['date'], parsed['region'], parsed['service'])
    expected_sig = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()

    if hmac.compare_digest(expected_sig, parsed['signature']):
        return True, project_id

    return False, None
```

### Step 2: Update `_check_presign_or_auth()`

Add AWS Sig V4 as third authentication method:

```python
async def _check_presign_or_auth(request, bucket, key, method) -> bool:
    # 1. Check pre-signed URL
    # 2. Check AWS Sig V4  <-- NEW
    # 3. Check Bearer token / X-Api-Key
```

### Step 3: Update boto3 Tests

```python
@pytest.fixture
def s3_client(server, project_with_api_key):
    """Create boto3 S3 client with AWS Sig V4."""
    return boto3.client(
        "s3",
        endpoint_url=f"{server}/s3",
        aws_access_key_id=project_with_api_key['id'],
        aws_secret_access_key=project_with_api_key['api_key'],
        region_name="local",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
```

## Testing

### Unit Tests (test_s3_compat.py)

```python
def test_aws_sig_v4_valid_signature():
    """Valid AWS Sig V4 signature is accepted."""

def test_aws_sig_v4_invalid_signature():
    """Invalid signature is rejected with 403."""

def test_aws_sig_v4_expired_request():
    """Request with old timestamp is rejected."""

def test_aws_sig_v4_wrong_access_key():
    """Unknown access key is rejected with 403."""
```

### Integration Tests (test_s3_boto3_integration.py)

Remove `@pytest.mark.skip` and verify all 7 tests pass:
- `test_put_and_get_object`
- `test_head_object`
- `test_delete_object`
- `test_list_objects_v2`
- `test_upload_large_file`
- `test_binary_content`
- `test_nested_keys`

## Success Criteria

| Metric | Target |
|--------|--------|
| boto3 tests | 7/7 passing |
| Existing S3 tests | 38/38 passing (no regression) |
| Pre-signed URLs | Still working |
| Bearer token auth | Still working |

## Risks

1. **Credential storage** - Need to store unhashed API keys or use different approach
2. **Clock skew** - AWS Sig V4 validates timestamp (typically 15 min window)
3. **Multipart uploads** - May need special handling (chunked signing)

## References

- [AWS Signature Version 4 Signing Process](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
- [MinIO Signature Verification](https://github.com/minio/minio/blob/master/cmd/signature-v4.go)
- [Phase 12h.1: S3-Compatible API](phase-12h-duckdb-files-in-connection.md)
