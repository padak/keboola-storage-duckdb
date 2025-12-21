# Phase 12h: DuckDB Files Backend in Connection

**Status:** PHASE 12h.1 DONE - S3-Compatible API implemented
**Goal:** Enable Keboola projects with DuckDB backend to use DuckDB for file storage (instead of AWS S3/Azure ABS/GCP GCS)
**Prerequisites:** Phase 12b.2 (Secure Project API Keys) - DONE

## Progress

| Sub-phase | Status | Tests |
|-----------|--------|-------|
| **Phase 12h.1: S3-Compatible API** | **DONE** | 26 |
| Phase 12h.2: Connection Integration | TODO | - |
| Phase 12h.3: End-to-End Testing | TODO | - |

---

## Executive Summary

Currently, DuckDB projects can store tables in DuckDB, but file operations still require external cloud storage (S3, ABS, GCS). This phase adds native file storage to DuckDB API Service, eliminating all cloud dependencies for on-premise deployments.

**Key Insight from Phase 12b:** DuckDB file storage is already registered in Connection as `PROVIDER_DUCKDB`, and `getProviderInstance()` returns `S3Provider`. This means Connection expects an S3-compatible API from DuckDB.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CURRENT vs TARGET ARCHITECTURE                            │
│                                                                              │
│   CURRENT (requires cloud):                                                  │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│   │  Connection  │────►│  DuckDB API  │     │   AWS S3     │                │
│   │   (tables)   │     │  (tables)    │     │   (files)    │◄───Connection  │
│   └──────────────┘     └──────────────┘     └──────────────┘                │
│                                                                              │
│   TARGET (fully on-premise):                                                 │
│   ┌──────────────┐     ┌──────────────────────────────────┐                 │
│   │  Connection  │────►│         DuckDB API Service        │                 │
│   │              │     │  ┌─────────┐  ┌────────────────┐ │                 │
│   │              │     │  │ Tables  │  │ S3-Compatible  │ │                 │
│   │              │     │  │  API    │  │   Files API    │ │                 │
│   │              │     │  └─────────┘  └────────────────┘ │                 │
│   └──────────────┘     └──────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Current State Analysis

### What's Already Implemented

**Python DuckDB API Service (`duckdb-api-service/src/routers/files.py`):**
- 3-stage upload workflow: PREPARE → UPLOAD → REGISTER
- File metadata in DuckDB (`metadata.duckdb` files table)
- File storage on local filesystem (`/data/files/project_{id}/`)
- REST endpoints: `/projects/{id}/files/*`

**Connection PHP Side:**
- `PROVIDER_DUCKDB = 'duckdb'` constant added
- `getProviderInstance()` returns `S3Provider` for DuckDB
- `toApiResponse()` handles DuckDB provider
- `getFilesBucketForCurrentProvider()` handles DuckDB

### What's Missing

| Component | Status | Description |
|-----------|--------|-------------|
| **S3-Compatible API** | **DONE** | Endpoints compatible with AWS S3 SDK (`/s3/{bucket}/{key}`) |
| **DuckDB FileStorage Assignment** | NOT DONE | Assign file storage when creating DuckDB project |
| **DuckDB S3 Adapter** | NOT DONE | Connection adapter for DuckDB file operations |
| **Pre-signed URLs** | TODO | S3-compatible pre-signed URL generation |
| **STS-like Credentials** | NOT DONE | Temporary credentials for file uploads |

---

## Architecture Decision

### Option A: Full S3-Compatible API (Recommended)
Implement MinIO-like S3-compatible endpoints that work with AWS S3 SDK.

**Pros:**
- Connection's existing S3 logic works unchanged
- AWS SDK handles retries, multipart uploads, etc.
- Standard interface (S3 is de-facto standard)

**Cons:**
- More complex to implement
- Need to handle S3 signature verification

### Option B: DuckDB-specific Adapter
Create new `Service_FileStorage_DuckDBAdapter` in Connection.

**Pros:**
- Simpler auth (just API key)
- Direct HTTP calls

**Cons:**
- Duplicate logic in Connection
- Need to maintain two code paths
- Breaking changes if API changes

### Decision: Option A - S3-Compatible API

Rationale:
1. Connection already expects S3Provider for DuckDB
2. Standard interface = less maintenance
3. Works with existing file upload clients
4. Future-proof (could swap to MinIO/real S3)

---

## Implementation Plan

### Phase 12h.1: S3-Compatible API in Python

**Goal:** Add S3-compatible endpoints to DuckDB API Service

#### New File: `src/routers/s3_compat.py`

```
duckdb-api-service/src/routers/s3_compat.py
```

**Required S3 Operations:**

| Operation | HTTP Method | Endpoint | Description |
|-----------|-------------|----------|-------------|
| GetObject | GET | `/{bucket}/{key}` | Download file |
| PutObject | PUT | `/{bucket}/{key}` | Upload file |
| DeleteObject | DELETE | `/{bucket}/{key}` | Delete file |
| HeadObject | HEAD | `/{bucket}/{key}` | Get file metadata |
| ListObjectsV2 | GET | `/{bucket}?list-type=2` | List files |
| CreateMultipartUpload | POST | `/{bucket}/{key}?uploads` | Start multipart |
| UploadPart | PUT | `/{bucket}/{key}?partNumber=N&uploadId=X` | Upload part |
| CompleteMultipartUpload | POST | `/{bucket}/{key}?uploadId=X` | Complete multipart |
| AbortMultipartUpload | DELETE | `/{bucket}/{key}?uploadId=X` | Abort multipart |

**Simplified MVP (without multipart):**

| Operation | Priority | Notes |
|-----------|----------|-------|
| GetObject | HIGH | Required for downloads |
| PutObject | HIGH | Required for uploads (single file) |
| DeleteObject | HIGH | Required for cleanup |
| HeadObject | MEDIUM | Used by some clients |
| ListObjectsV2 | MEDIUM | Required for sliced files |

**Authentication (flexible - all methods supported):**
- `Authorization: Bearer {api_key}` (same as REST API)
- `X-Api-Key: {api_key}` (for AWS SDK with custom handler)
- `x-amz-security-token: {api_key}` (for STS-like flow)

**Bucket Mapping:**
```
S3 bucket name → DuckDB project_id
S3 key         → file path within project
```

Example:
```
PUT /project_123/data/2024/12/21/file.csv
    ↓
Stored in: /data/files/project_123/data/2024/12/21/file.csv
```

#### Implementation Steps

**Step 1: Create S3 router with basic operations**

```python
# src/routers/s3_compat.py
from fastapi import APIRouter, Request, Response, HTTPException
from fastapi.responses import StreamingResponse
import hashlib
from pathlib import Path

router = APIRouter(prefix="/s3", tags=["s3-compat"])

@router.get("/{bucket}/{key:path}")
async def get_object(bucket: str, key: str):
    """S3 GetObject - download file."""
    # bucket = project_id
    # key = relative path
    file_path = settings.files_dir / f"project_{bucket}" / key
    if not file_path.exists():
        raise HTTPException(status_code=404, detail={"Code": "NoSuchKey"})

    return FileResponse(
        path=file_path,
        headers={
            "ETag": f'"{compute_etag(file_path)}"',
            "Content-Length": str(file_path.stat().st_size),
        }
    )

@router.put("/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, request: Request):
    """S3 PutObject - upload file."""
    file_path = settings.files_dir / f"project_{bucket}" / key
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Stream request body to file
    content = await request.body()

    # Verify Content-MD5 if provided
    content_md5 = request.headers.get("Content-MD5")
    if content_md5:
        actual_md5 = base64.b64encode(hashlib.md5(content).digest()).decode()
        if actual_md5 != content_md5:
            raise HTTPException(status_code=400, detail={"Code": "BadDigest"})

    file_path.write_bytes(content)

    etag = hashlib.md5(content).hexdigest()
    return Response(
        status_code=200,
        headers={"ETag": f'"{etag}"'}
    )

@router.delete("/{bucket}/{key:path}")
async def delete_object(bucket: str, key: str):
    """S3 DeleteObject - delete file."""
    file_path = settings.files_dir / f"project_{bucket}" / key
    if file_path.exists():
        file_path.unlink()
    return Response(status_code=204)

@router.head("/{bucket}/{key:path}")
async def head_object(bucket: str, key: str):
    """S3 HeadObject - get file metadata."""
    file_path = settings.files_dir / f"project_{bucket}" / key
    if not file_path.exists():
        raise HTTPException(status_code=404, detail={"Code": "NoSuchKey"})

    return Response(
        status_code=200,
        headers={
            "ETag": f'"{compute_etag(file_path)}"',
            "Content-Length": str(file_path.stat().st_size),
            "Content-Type": "application/octet-stream",
            "Last-Modified": file_path.stat().st_mtime,
        }
    )
```

**Step 2: Add ListObjectsV2 for sliced files**

```python
@router.get("/{bucket}")
async def list_objects(
    bucket: str,
    list_type: int = Query(2, alias="list-type"),
    prefix: str = Query("", alias="prefix"),
    delimiter: str = Query("", alias="delimiter"),
    max_keys: int = Query(1000, alias="max-keys"),
):
    """S3 ListObjectsV2 - list files in bucket."""
    project_dir = settings.files_dir / f"project_{bucket}"

    objects = []
    for file_path in project_dir.rglob("*"):
        if file_path.is_file():
            key = str(file_path.relative_to(project_dir))
            if prefix and not key.startswith(prefix):
                continue
            objects.append({
                "Key": key,
                "Size": file_path.stat().st_size,
                "LastModified": datetime.fromtimestamp(file_path.stat().st_mtime),
                "ETag": f'"{compute_etag(file_path)}"',
            })

    # Return XML response (S3 format)
    return Response(
        content=build_list_objects_xml(bucket, objects[:max_keys]),
        media_type="application/xml"
    )
```

**Step 3: Add pre-signed URL generation**

```python
# src/routers/s3_compat.py

@router.post("/{bucket}/presign")
async def create_presigned_url(
    bucket: str,
    request: PresignRequest,
    api_key: str = Depends(require_project_access),
):
    """Generate pre-signed URL for S3 operations."""
    # Generate signed URL with expiration
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=request.expires_in)
    signature = sign_url(
        method=request.method,
        bucket=bucket,
        key=request.key,
        expires_at=expires_at,
    )

    return {
        "url": f"{settings.base_url}/s3/{bucket}/{request.key}?signature={signature}&expires={expires_at.timestamp()}",
        "expires_at": expires_at.isoformat(),
    }
```

**Step 4: Add multipart upload support (for large files)**

```python
# Multipart uploads stored in staging
_multipart_uploads: dict[str, dict] = {}

@router.post("/{bucket}/{key:path}")
async def initiate_multipart_upload(
    bucket: str,
    key: str,
    uploads: str = Query(None),  # ?uploads query param
):
    """S3 CreateMultipartUpload."""
    if uploads is not None:
        upload_id = str(uuid.uuid4())
        _multipart_uploads[upload_id] = {
            "bucket": bucket,
            "key": key,
            "parts": {},
            "created_at": datetime.now(timezone.utc),
        }
        return Response(
            content=f"""<?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult>
                <Bucket>{bucket}</Bucket>
                <Key>{key}</Key>
                <UploadId>{upload_id}</UploadId>
            </InitiateMultipartUploadResult>""",
            media_type="application/xml",
        )
```

---

### Phase 12h.2: Connection FileStorage Assignment

**Goal:** Assign DuckDB file storage to projects when creating DuckDB backend

#### Database Migration

```php
// connection/legacy-app/sql/migrations/accounts/Version20251222100000.php

public function up(Schema $schema): void
{
    // Add idDefaultFileStorageDuckdb to bi_maintainers
    $this->addSql("
        ALTER TABLE bi_maintainers
        ADD COLUMN idDefaultFileStorageDuckdb INT UNSIGNED DEFAULT NULL,
        ADD KEY idDefaultFileStorageDuckdb (idDefaultFileStorageDuckdb),
        ADD CONSTRAINT bi_maintainers_ibfk_filestorage_duckdb
            FOREIGN KEY (idDefaultFileStorageDuckdb)
            REFERENCES bi_fileStorage (id)
    ");

    // Add idDuckdbFileStorage to bi_projects
    $this->addSql("
        ALTER TABLE bi_projects
        ADD COLUMN idDuckdbFileStorage INT UNSIGNED DEFAULT NULL,
        ADD KEY idDuckdbFileStorage (idDuckdbFileStorage),
        ADD CONSTRAINT bi_projects_ibfk_filestorage_duckdb
            FOREIGN KEY (idDuckdbFileStorage)
            REFERENCES bi_fileStorage (id)
    ");
}
```

#### Create DuckDB FileStorage Entry

```sql
-- Insert DuckDB file storage configuration
INSERT INTO bi_fileStorage (
    provider,
    region,
    filesBucket,
    owner,
    isDefault,
    created,
    creatorName
) VALUES (
    'duckdb',
    'local',
    'duckdb-files',  -- bucket name for S3 compatibility
    'Keboola',
    false,
    NOW(),
    'system'
);
```

#### Modify BackendAssign.php

```php
// connection/legacy-app/application/src/Storage/Service/Backend/Assign/BackendAssign.php

case BackendSupportsInterface::BACKEND_DUCKDB:
    // Assign storage backend
    $project->assignDuckdbBackend($credentials, $newBackendConnection);

    // Assign file storage (NEW)
    $fileStorage = $this->getDuckdbFileStorage($project);
    if ($fileStorage) {
        $project->idDuckdbFileStorage = $fileStorage->getId();
    }
    break;

private function getDuckdbFileStorage(Model_Row_Project $project): ?Model_Row_FileStorage
{
    // Get from maintainer's default DuckDB file storage
    $maintainer = $project->getMaintainer();
    if ($maintainer->idDefaultFileStorageDuckdb) {
        return $this->fileStorageModel->find($maintainer->idDefaultFileStorageDuckdb);
    }

    // Fallback: find any DuckDB file storage in region
    return $this->fileStorageModel->fetchRow([
        'provider' => 'duckdb',
        'region' => 'local',
    ]);
}
```

#### Add Model_Row_Project Methods

```php
// connection/legacy-app/application/modules/core/models/Row/Project.php

/**
 * @property int|null $idDuckdbFileStorage
 */

public function getDuckdbFileStorage(): ?Model_Row_FileStorage
{
    if ($this->idDuckdbFileStorage === null) {
        return null;
    }
    return $this->findParentRow(Model_FileStorage::class, 'DuckdbFileStorage');
}

public function hasDuckdbFileStorage(): bool
{
    return $this->idDuckdbFileStorage !== null;
}
```

---

### Phase 12h.3: DuckDB S3 Adapter (Optional Enhancement)

If we want more control over DuckDB file operations, we can create a dedicated adapter:

```php
// connection/legacy-app/application/modules/core/services/FileStorage/Adapter/DuckDBAdapter.php

class Service_FileStorage_DuckDBAdapter implements AdapterInterface
{
    private string $serviceUrl;
    private string $apiKey;
    private string $bucket;

    public function __construct(
        private readonly Model_Row_FileStorage $fileStorage,
        private readonly string $duckdbServiceUrl,
        private readonly string $duckdbApiKey,
    ) {
        $this->bucket = $fileStorage->getFilesBucket() ?? 'duckdb-files';
    }

    public function getS3Client(): S3Client
    {
        // Return S3 client configured to point to DuckDB S3-compat API
        return new S3Client([
            'endpoint' => $this->duckdbServiceUrl . '/s3',
            'use_path_style_endpoint' => true,
            'credentials' => [
                'key' => 'duckdb',
                'secret' => $this->duckdbApiKey,
            ],
            'region' => 'local',
            'version' => 'latest',
        ]);
    }

    // ... implement other methods using S3 client
}
```

However, since we're using S3-compatible API, the existing `S3Adapter` should work with minimal changes by just pointing it to the DuckDB endpoint.

---

## Configuration

### Environment Variables

```bash
# DuckDB API Service
DUCKDB_S3_ENABLED=true
DUCKDB_S3_BASE_URL=http://localhost:8000/s3

# Connection (for DuckDB file storage)
DUCKDB_SERVICE_URL=http://duckdb-service:8000
DUCKDB_ADMIN_API_KEY=your-admin-key
```

### FileStorage Configuration

When creating DuckDB file storage:

```json
{
    "provider": "duckdb",
    "region": "local",
    "filesBucket": "project_{project_id}",
    "awsKey": "duckdb",
    "awsSecret": "{DUCKDB_ADMIN_API_KEY}"
}
```

**Key Mapping:**
- `awsKey` → "duckdb" (placeholder, not used for real AWS)
- `awsSecret` → DuckDB API key (encrypted in DB)
- `filesBucket` → becomes S3 bucket = project directory

---

## Testing Plan

### Unit Tests

```python
# duckdb-api-service/tests/test_s3_compat.py

def test_put_object():
    """Test S3 PutObject."""
    response = client.put("/s3/project_123/data/test.csv", content=b"a,b\n1,2")
    assert response.status_code == 200
    assert "ETag" in response.headers

def test_get_object():
    """Test S3 GetObject."""
    # First upload
    client.put("/s3/project_123/data/test.csv", content=b"a,b\n1,2")

    # Then download
    response = client.get("/s3/project_123/data/test.csv")
    assert response.status_code == 200
    assert response.content == b"a,b\n1,2"

def test_delete_object():
    """Test S3 DeleteObject."""
    client.put("/s3/project_123/data/test.csv", content=b"a,b\n1,2")
    response = client.delete("/s3/project_123/data/test.csv")
    assert response.status_code == 204

def test_list_objects():
    """Test S3 ListObjectsV2."""
    client.put("/s3/project_123/data/a.csv", content=b"1")
    client.put("/s3/project_123/data/b.csv", content=b"2")

    response = client.get("/s3/project_123?list-type=2&prefix=data/")
    assert response.status_code == 200
    assert b"<Key>data/a.csv</Key>" in response.content

def test_head_object():
    """Test S3 HeadObject."""
    client.put("/s3/project_123/data/test.csv", content=b"a,b\n1,2")
    response = client.head("/s3/project_123/data/test.csv")
    assert response.status_code == 200
    assert response.headers["Content-Length"] == "7"
```

### Integration Tests

```python
# duckdb-api-service/tests/test_s3_integration.py

def test_aws_sdk_compatibility():
    """Test that AWS SDK can use DuckDB S3 API."""
    import boto3

    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:8000/s3',
        aws_access_key_id='duckdb',
        aws_secret_access_key='test-key',
        region_name='local',
    )

    # Upload
    s3.put_object(Bucket='project_123', Key='test.csv', Body=b'a,b\n1,2')

    # Download
    response = s3.get_object(Bucket='project_123', Key='test.csv')
    assert response['Body'].read() == b'a,b\n1,2'

    # Delete
    s3.delete_object(Bucket='project_123', Key='test.csv')
```

---

## Implementation Checklist

### Phase 12h.1: S3-Compatible API - DONE (2024-12-21)

- [x] Create `src/routers/s3_compat.py`
- [x] Implement GetObject (GET /{bucket}/{key})
- [x] Implement PutObject (PUT /{bucket}/{key})
- [x] Implement DeleteObject (DELETE /{bucket}/{key})
- [x] Implement HeadObject (HEAD /{bucket}/{key})
- [x] Implement ListObjectsV2 (GET /{bucket}?list-type=2)
- [x] Add Content-MD5 verification
- [x] Add ETag generation (MD5 hash)
- [ ] Add pre-signed URL support
- [x] Register router in main.py
- [x] Write unit tests (26 tests)
- [x] Test with AWS SDK (boto3 integration tests created)

### Phase 12h.2: Connection Integration

- [ ] Create migration Version20251222100000.php
- [ ] Add `idDefaultFileStorageDuckdb` to bi_maintainers
- [ ] Add `idDuckdbFileStorage` to bi_projects
- [ ] Insert DuckDB file storage record
- [ ] Update BackendAssign.php to assign file storage
- [ ] Update Model_Row_Project with file storage methods
- [ ] Update Model_Projects reference map
- [ ] Test project creation with file storage

### Phase 12h.3: End-to-End Testing

- [ ] Create DuckDB project via Manage API
- [ ] Verify file storage is assigned
- [ ] Upload file via Storage API
- [ ] Verify file stored in DuckDB API
- [ ] Download file via Storage API
- [ ] Delete file via Storage API
- [ ] Test table import from file
- [ ] Test table export to file

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| S3 SDK expects specific response format | Use XML responses matching S3 exactly |
| Multipart uploads complex | Start with single-file uploads, add multipart later |
| Pre-signed URLs require crypto | Use simple HMAC signing with API key |
| Large files may timeout | Add streaming support, chunked uploads |

---

## Related Documents

- [Phase 12b: Connection Backend Registration](phase-12-php-driver.md)
- [ADR-014: gRPC Driver Interface](../adr/014-grpc-driver-interface.md)
- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/)
- [MinIO S3 Compatibility](https://min.io/docs/minio/linux/reference/minio-mc-admin.html)
