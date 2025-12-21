# Phase 12h: DuckDB Files Backend in Connection

**Status:** PHASE 12h.12 DONE - Table preview working in Connection UI!
**Goal:** Enable Keboola projects with DuckDB backend to use DuckDB for file storage (instead of AWS S3/Azure ABS/GCP GCS)
**Prerequisites:** Phase 12b.2 (Secure Project API Keys) - DONE

## Progress

| Sub-phase | Status | Tests |
|-----------|--------|-------|
| **Phase 12h.1: S3-Compatible API** | **DONE** | 38 |
| **Phase 12h.2: Connection Integration** | **DONE & VERIFIED** | - |
| **Phase 12h.3: End-to-End Testing** | **DONE** | Bucket/Table OK |
| **Phase 12h.4: File Import Flow** | **DONE** | Proto + handlers updated |
| **Phase 12h.5: File Upload Adapter** | **DONE** | Adapter + Request classes |
| **Phase 12h.6: File Routing Fix** | **DONE** | Upload to DuckDB works! |
| Phase 12h.7: Async Table Creation | TODO | Job stuck in processing |
| **Phase 12h.8: Backend Audit** | **DONE** | 5 files fixed |
| **Phase 12h.9: Bucket Mismatch Fix** | **DONE** | 3 files fixed |
| **Phase 12h.10: Import URL Fix** | SUPERSEDED | See 12h.11 |
| **Phase 12h.11: Import Handler Fixes** | **DONE** | Job 1000008 SUCCESS! |
| **Phase 12h.12: Path Parsing Fix** | **DONE** | Table preview works! |

---

## Verification Results (2024-12-21)

**Phase 12h.2 has been verified working:**

1. **Migration executed successfully:**
   - `bi_maintainers.idDefaultFileStorageDuckdb` column added
   - `bi_projects.idDuckdbFileStorage` column added
   - DuckDB file storage record created (ID: 3, provider: 'duckdb', region: 'local')

2. **DuckDB project creation tested:**
   - Created project ID 7 via Manage API
   - Assigned DuckDB backend (storageBackendId: 3)
   - **Result:** `idDuckdbFileStorage = 3` automatically assigned!
   - **Result:** `idDuckdbCredentials = 3` created in DuckDB API

3. **Database verification:**
   ```sql
   SELECT id, name, defaultBackend, idFileStorage, idDuckdbFileStorage, idDuckdbCredentials
   FROM bi_projects WHERE id = 7;
   -- Result: 7, "DuckDB File Test 2", "duckdb", 1, 3, 3
   ```

### Fixes Applied During Testing

**Python `driver.py`:** Added snake_case to camelCase conversion
- PHP driver sends `stack_prefix`, `project_id` (snake_case)
- Protobuf expects `stackPrefix`, `projectId` (camelCase)
- Added `_convert_keys_to_camel_case()` function

**PHP `DuckDBDriverClient.php`:** Fixed response parsing
- API returns `commandResponse`, not `response`
- Updated to check both: `$data['commandResponse'] ?? $data['response']`
- Added `@type` field removal before JSON parsing

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
| **DuckDB FileStorage Assignment** | **DONE** | Assign file storage when creating DuckDB project |
| **DuckDB S3 Adapter** | NOT NEEDED | Connection uses existing `S3Provider` pointed to DuckDB endpoint |
| **Pre-signed URLs** | **DONE** | S3-compatible pre-signed URL generation (`POST /s3/{bucket}/presign`) |
| **STS-like Credentials** | NOT NEEDED | Using project API keys instead (simpler) |
| **End-to-End File Upload Test** | TODO | Test file upload via Storage API to DuckDB |

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

| Operation | Priority | Status |
|-----------|----------|--------|
| GetObject | HIGH | DONE |
| PutObject | HIGH | DONE |
| DeleteObject | HIGH | DONE |
| HeadObject | MEDIUM | DONE |
| ListObjectsV2 | MEDIUM | DONE |
| Presign | MEDIUM | DONE |

**Authentication (flexible - all methods supported):**
- `Authorization: Bearer {api_key}` (same as REST API)
- `X-Api-Key: {api_key}` (for AWS SDK with custom handler)
- `x-amz-security-token: {api_key}` (for STS-like flow)
- Pre-signed URL with `?signature=...&expires=...` query parameters

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
- [x] Add pre-signed URL support (POST /{bucket}/presign)
- [x] Register router in main.py
- [x] Write unit tests (38 tests - 28 original + 10 pre-signed URL tests)
- [x] Test with AWS SDK (boto3 integration tests created)

### Phase 12h.2: Connection Integration - DONE & VERIFIED (2024-12-21)

- [x] Create migration Version20251222100000.php
- [x] Add `idDefaultFileStorageDuckdb` to bi_maintainers
- [x] Add `idDuckdbFileStorage` to bi_projects
- [x] Insert DuckDB file storage record
- [x] Update BackendAssign.php to assign file storage
- [x] Update Model_Row_Project with file storage methods
- [x] Update Model_Projects reference map
- [x] Update Model_Maintainers reference map
- [x] Update Model_Row_Maintainer with file storage methods
- [x] Add createDuckDbFileStorageRow() to Model_FileStorage
- [x] Register Model_FileStorage in services.yaml
- [x] **VERIFIED:** Create DuckDB project and assign backend
- [x] **VERIFIED:** File storage automatically assigned (idDuckdbFileStorage = 3)

**PHP Files Modified:**
- `Package/StorageDriverDuckdb/src/DuckDBDriverClient.php` - Fixed response parsing
- `legacy-app/application/src/Storage/Service/Backend/Assign/BackendAssign.php` - Added file storage assignment
- `legacy-app/application/modules/core/models/FileStorage.php` - Added factory method
- `legacy-app/application/modules/core/models/Projects.php` - Added reference map
- `legacy-app/application/modules/core/models/Row/Project.php` - Added file storage methods
- `legacy-app/application/modules/core/models/Maintainers.php` - Added reference map
- `legacy-app/application/modules/core/models/Row/Maintainer.php` - Added file storage methods
- `legacy-app/application/src/services.yaml` - Registered Model_FileStorage

**Python Files Modified:**
- `duckdb-api-service/src/routers/driver.py` - Added snake_case to camelCase conversion

### Phase 12h.3: End-to-End Testing (2024-12-21)

#### CRITICAL PHP Fixes Made

During E2E testing, several PHP code issues were discovered and fixed:

1. **Doctrine Project Entity** (`src/Manage/Projects/Entity/Project.php`):
   - Added `idDuckdbCredentials` ORM Index
   - Added `duckdbCredentials` property with ManyToOne relation
   - Added `getDuckdbCredentials()` and `requireDuckdbCredentials()` methods
   - Added `hasDuckdbActivated()` method
   - Added DuckDB to `getAssignedBackends()` method

2. **Zend Project Model** (`legacy-app/application/modules/core/models/Row/Project.php`):
   - Added DuckDB case to `supportsBackend()` method
   - Added DuckDB case to `getDefaultConnectionForBackend()` method

3. **DuckDBCredentialsResolver** (`legacy-app/application/src/Storage/Service/Backend/CredentialsResolver/DuckDBCredentialsResolver.php`):
   - Changed from `MySQLStringEncryptor` to using Doctrine entity with `EncryptedValue`
   - Now uses `$credentials->getPassword()->getPlainValue()` for proper decryption

4. **StorageBucket Entity** (`src/Storage/Buckets/Entity/StorageBucket.php`):
   - Added `BACKEND_DUCKDB` case to `getFullPath()` method

#### E2E Test Results

| Test | Status | Notes |
|------|--------|-------|
| Create DuckDB project via Manage API | DONE | Project ID: 7 |
| Verify file storage assigned | DONE | `idDuckdbFileStorage = 3` |
| Create Storage API token | DONE | Token: 7-11-* |
| **Create bucket** | **DONE** | Bucket: `in.c-test2` with `backend: duckdb` |
| **Create table** | **DONE** | Table: `in.c-test2.users` created via CSV endpoint |
| Import data to table | FAILED | Python API path validation issue |
| Upload file via Storage API | TODO | Needs Connection file storage routing |
| Download/Delete file | TODO | Depends on upload fix |
| Table export to file | TODO | Similar path issue expected |

#### Issue: TableImportFromFileCommand Path Validation

The table creation works, but data import fails with:
```
"detail":"Destination path must contain [project_id, bucket_name]"
```

This is in the Python DuckDB API `import_export.py` handler. Connection sends a different
path format than what the handler expects. Needs investigation in:
- `duckdb-api-service/src/grpc/handlers/import_export.py`
- `connection/Package/StorageDriverDuckdb/src/DuckDBDriverClient.php`

**Note:** File upload via Storage API requires Connection to route file operations
to DuckDB's S3-compatible endpoint instead of AWS S3. This may require additional
configuration in Connection's file storage resolver to point to DuckDB service URL.

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| S3 SDK expects specific response format | Use XML responses matching S3 exactly |
| Multipart uploads complex | Start with single-file uploads, add multipart later |
| Pre-signed URLs require crypto | Use simple HMAC signing with API key |
| Large files may timeout | Add streaming support, chunked uploads |

---

---

### Phase 12h.4: File Import Flow Implementation (2024-12-21)

#### Goal
Enable Connection to import files from DuckDB's local file storage instead of AWS S3.

#### Changes Implemented

**1. Proto Extension (`duckdb-api-service/proto/table.proto`):**
- Added `HTTP = 3` to `FileProvider` enum
- Supports pre-signed URLs and local file paths

**2. DuckDB gRPC Handler (`duckdb-api-service/src/grpc/handlers/import_export.py`):**
- `_build_file_url()` now supports HTTP provider:
  - Local paths (starts with `/`) → read from filesystem
  - HTTP URLs → read via httpfs extension
  - Pre-signed URLs (contains `?`) → use as-is
- `_execute_import()` loads httpfs only for remote URLs
- Same changes in export handler

**3. Connection CredentialsProvider (`Package/Bridge/src/FileStorage/Credentials/CredentialsProvider.php`):**
- Added `PROVIDER_DUCKDB` case
- Returns empty S3Credentials (DuckDB doesn't need cloud credentials)

**4. Connection ImportExportAdapterFactory (`Package/Bridge/src/StorageBackend/ImportExport/ImportExportAdapterFactory.php`):**
- Added `PROVIDER_DUCKDB` case
- Returns S3ImportExportAdapter (since DuckDB uses S3-compatible API)

**5. Connection ImportTableCommandFactory (`legacy-app/application/src/Storage/Service/Backend/Driver/TableImport/ImportTableCommandFactory.php`):**
- Added `FILE_PROVIDER_HTTP = 3` constant (until storage-driver-common package is updated)
- Added `PROVIDER_DUCKDB` case in `resolveProvider()` → returns HTTP
- Added DuckDB-specific handling in `createImportTableFromFileCommand()`:
  - Uses file's absolute path directly
  - Sets HTTP provider
  - No credentials (file is in local storage)

#### What Still Needs to be Done

1. **File Upload Flow:**
   - Connection needs to route file uploads to DuckDB Files API
   - Currently files go to AWS S3, need to redirect for DuckDB projects

2. **Shared Filesystem:**
   - Connection and DuckDB service need access to same files directory
   - Docker compose needs shared volume configuration

3. **DuckDB FileStorage Configuration:**
   - DuckDB project needs proper `idDuckdbFileStorage` with correct endpoint URL
   - Region field should contain DuckDB service URL

4. **Testing:**
   - E2E test: Upload file → Import to table → Verify data

#### Architecture for File Upload (Correct Flow)

**IMPORTANT:** Klient uploaduje soubory PŘÍMO do storage, ne přes Connection!

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     FILE UPLOAD FLOW (EXTERNAL CLIENT)                   │
│                                                                          │
│   Klient (někde venku)          Connection              DuckDB API       │
│         │                           │                      │             │
│         │ 1. POST /files/prepare ──►│                      │             │
│         │◄── presigned upload URL ──│                      │             │
│         │                           │                      │             │
│         │ 2. PUT file (HTTP) ───────────────────────────────►│           │
│         │    (přímo do DuckDB S3 API)                      │             │
│         │                           │                      │             │
│         │ 3. File uploaded ────────►│                      │             │
│         │                           │ 4. gRPC Import ─────►│             │
│         │                           │    (local file path) │             │
└──────────────────────────────────────────────────────────────────────────┘
```

#### Connection File Upload Analysis

**Endpoint:** `POST /v2/storage/files/prepare`

**Controller:** `src/Controller/Storage/Files/FilePrepareAction.php`

**Key Components:**

1. **FileBackupFactory** (`legacy-app/application/modules/storage/services/FileBackupFactory.php`)
   - Vybírá adapter podle `fileStorage->getProvider()`
   - **POTŘEBA:** Přidat `case PROVIDER_DUCKDB: return new DuckDbAdapter(...)`

2. **AdapterInterface** (`legacy-app/application/modules/core/services/FileStorage/Adapter/AdapterInterface.php`)
   - `getPrepareRequestClassName()` - vrací request class
   - `createUploadParams()` - vrací upload credentials/URL
   - `getFileDownloadUrl()` - vrací download URL
   - `getReadData()` - vrací read credentials

3. **S3Adapter jako vzor** (`legacy-app/application/modules/core/services/FileStorage/Adapter/S3Adapter.php`)
   - `createUploadParams()` vrací S3 federation token
   - Pro DuckDB: vrátíme presigned URL místo federation tokenu

#### Files to Create for DuckDB File Upload

```
connection/
├── legacy-app/application/modules/core/services/FileStorage/Adapter/
│   └── DuckDbAdapter.php                    # NEW - hlavní adapter
├── legacy-app/application/modules/storage/services/
│   └── FileBackupFactory.php                # MODIFY - přidat PROVIDER_DUCKDB case
├── src/Storage/Files/FilePrepare/Request/
│   └── DuckDbFilePrepareRequest.php         # NEW - request class
└── src/Storage/Files/FilePrepare/JobParams/
    └── DuckDbFilePrepareJobParams.php       # NEW - job params
```

#### DuckDbAdapter Key Methods

```php
class Service_FileStorage_DuckDbAdapter implements AdapterInterface
{
    public function createUploadParams(Model_Row_File $file, bool $isEncrypted): array
    {
        // Zavolat DuckDB API pro presigned URL
        // POST /s3/{bucket}/presign
        $presignedUrl = $this->getPresignedUploadUrl($file);

        return [
            'uploadParams' => [
                'url' => $presignedUrl,
                'method' => 'PUT',
            ],
        ];
    }

    public function getFileDownloadUrl(Model_Row_File $file): string
    {
        // Presigned GET URL z DuckDB API
        return $this->getPresignedDownloadUrl($file);
    }
}
```

#### Response Format for DuckDB

```json
{
    "id": 12345,
    "provider": "duckdb",
    "uploadParams": {
        "url": "http://duckdb-service:8000/s3/project_123/files/12345?signature=xxx&expires=123",
        "method": "PUT"
    }
}
```

#### Import Flow After Upload

Po uploadu souboru klientem:
1. Connection volá `TableImportFromFileCommand` přes gRPC
2. `fileProvider: HTTP` (hodnota 3)
3. `filePath.root: /data/duckdb/files/project_123/...` (lokální cesta)
4. DuckDB handler čte soubor z lokálního filesystému

---

### Phase 12h.5: File Upload Adapter (2024-12-21)

#### Goal
Implement Connection file storage adapter for DuckDB to enable file uploads via Storage API.

#### Files Created

**1. DuckDbAdapter.php** (`legacy-app/application/modules/core/services/FileStorage/Adapter/`)
- Main file storage adapter implementing `Service_FileStorage_AdapterInterface`
- Uses pre-signed URLs instead of AWS STS federation tokens
- Communicates with DuckDB S3-compatible API (`POST /s3/{bucket}/presign`)
- Key methods:
  - `createUploadParams()` - generates pre-signed PUT URL
  - `getFileDownloadUrl()` - generates pre-signed GET URL
  - `deleteFile()` - deletes file via S3 DELETE
  - `getSizeBytesPath()` - gets file size via HEAD request

**2. DuckDbFilePrepareRequest.php** (`src/Storage/Files/FilePrepare/Request/`)
- Request class for file prepare endpoint
- Similar to `AbsFilePrepareRequest` (simpler than S3 - no encryption options)
- Always uses federation token mode (pre-signed URLs)

**3. DuckDbFilePrepareJobParams.php** (`src/Storage/Files/FilePrepare/JobParams/`)
- Job parameters for file prepare operations
- Extends `BaseFilePrepareJobParams`

#### Files Modified

**1. FileBackupFactory.php** (`legacy-app/application/modules/storage/services/`)
- Added `PROVIDER_DUCKDB` case to `createForProject()`
- Added DuckDB service URL and API key constructor parameters

**2. services.yml** (`legacy-app/application/configs/`)
- Added `$duckdbServiceUrl` and `$duckdbAdminApiKey` arguments
- Uses `%env(default::DUCKDB_SERVICE_URL)%` and `%env(default::DUCKDB_ADMIN_API_KEY)%`

#### Key Design Decisions

1. **Pre-signed URLs vs STS Tokens**: DuckDB uses simpler pre-signed URLs instead of complex AWS STS federation tokens
2. **S3-Compatible Path Mapper**: Reuses `S3LifeCyclePathMapper` since DuckDB storage is S3-compatible
3. **Bucket = project_id**: S3 bucket name maps to `project_{id}` directory structure

#### Upload Flow

```
Client                    Connection                DuckDB API
  |                           |                         |
  |--1. POST /files/prepare-->|                         |
  |                           |--2. POST /presign------>|
  |                           |<---presigned URL--------|
  |<---uploadParams (URL)-----|                         |
  |                           |                         |
  |--3. PUT file (direct)----------------------------->|
  |<---200 OK-----------------------------------------|
```

#### Environment Variables

```bash
# Required for DuckDB file storage
DUCKDB_SERVICE_URL=http://duckdb-service:8000
DUCKDB_ADMIN_API_KEY=your-admin-api-key
```

---

### Phase 12h.6: File Routing Fix (2025-12-21)

#### Goal
Fix Connection to route file uploads to DuckDB's S3-compatible API instead of AWS S3.

#### Problem Discovered
When calling `POST /v2/storage/files/prepare`, Connection was returning AWS S3 credentials even for DuckDB projects because:
1. `Model_Row_Project::getFileStorage()` always returned the regular `idFileStorage` (AWS) instead of `idDuckdbFileStorage` (DuckDB)
2. `Provider` enum in Symfony didn't have a `DUCKDB` case
3. `File::createArrayForElasticSearch()` didn't store `s3Path` for DuckDB files

#### Files Modified

**1. Model_Row_Project.php** (`legacy-app/application/modules/core/models/Row/Project.php`)
- Modified `getFileStorage()` to check for DuckDB file storage first:
```php
public function getFileStorage()
{
    // For DuckDB projects, prefer DuckDB file storage if set
    if ($this->hasDuckdbFileStorage()) {
        $duckdbStorage = $this->getDuckdbFileStorage();
        if ($duckdbStorage !== null) {
            return $duckdbStorage;
        }
    }
    // ... fallback to regular file storage
}
```

**2. Provider.php** (`src/Storage/Files/Provider.php`)
- Added `DUCKDB` case to the enum:
```php
enum Provider: string
{
    case AWS = 'aws';
    case GCP = 'gcp';
    case AZURE = 'azure';
    case DUCKDB = 'duckdb';  // NEW
}
```

**3. File.php** (`src/Storage/Files/Dto/File.php`)
- Modified `createArrayForElasticSearch()` to store `s3Path` for DuckDB:
```php
switch ($this->provider) {
    case Provider::AWS:
    case Provider::DUCKDB:  // DuckDB uses S3-compatible API
        $response['s3Path'] = $this->getPath();
        break;
    // ...
}
```

**4. DuckDbAdapter.php** (`legacy-app/application/modules/core/services/FileStorage/Adapter/DuckDbAdapter.php`)
- Changed from `getRelativePath()->getPathnameWithoutRoot()` to `getS3Key()` (5 occurrences)
- This matches how S3Adapter works

#### Verification Results

**Before fix:**
```json
POST /v2/storage/files/prepare
{
  "provider": "aws",
  "uploadParams": { "bucket": "padak-kbc-services-s3-files-storage-bucket", ... }
}
```

**After fix:**
```json
POST /v2/storage/files/prepare
{
  "id": 24,
  "provider": "duckdb",
  "url": "http://localhost:8000/s3/project_8/exp-15/8/files/2025/12/21/24.users.csv?signature=...",
  "uploadParams": {
    "url": "http://localhost:8000/s3/project_8/exp-15/8/files/2025/12/21/24.users.csv?signature=...",
    "method": "PUT",
    "expiresAt": "2025-12-22T07:50:46+00:00"
  }
}
```

**File upload test:**
```bash
# Upload CSV to DuckDB's S3-compatible API
PUT http://localhost:8000/s3/project_8/exp-15/8/files/.../24.users.csv?signature=...
Content-Type: text/csv
Body: "id","name","email"\n"1","Alice","alice@example.com"...

Response: 200 OK
Headers: ETag: "4af8d93389bc4a2c9d6128656281d0d9"
```

#### Current Test Environment

- **Maintainer:** ID 4 ("DuckDB Services") with `idDefaultConnectionDuckdb = 3`, `idDefaultFileStorageDuckdb = 3`
- **Organization:** ID 7 ("DuckDB Test Org")
- **Project:** ID 8 ("DuckDB E2E Test") with `idDuckdbCredentials = 4`, `idDuckdbFileStorage = 3`
- **Bucket:** `in.c-test` with `backend: duckdb`
- **Storage Token:** `8-13-bReTChYI6UwiOf797sitUgvqzDLWwlkEnnv9IQsr`
- **Manage Token:** `28-TQcEGHgG6YB4Qu56XMQjYlSjeHtwlca15ZZM79mJ`
- **DuckDB Admin Key:** `Loktibrada22`

#### Remaining Issue: Async Table Creation

When creating a table via `POST /v2/storage/buckets/in.c-test/tables-async` with `dataFileId: 24`:
- Job is created (ID 1000002) with status "waiting"
- Supervisor workers are failing with exit status 1
- Job never transitions to "processing" or "success"

This appears to be an issue with the async job processing for DuckDB tables, not with file storage itself.

**Next steps:**
1. Debug supervisor worker logs to find the actual error
2. Check if DuckDB-specific job processing code is missing
3. Alternatively, test sync table creation endpoint if available

---

### Phase 12h.8: BACKEND_BIGQUERY vs BACKEND_DUCKDB Audit (2025-12-21)

#### Goal
Audit all places where `BACKEND_BIGQUERY` is used and ensure `BACKEND_DUCKDB` is added where needed.

#### Analysis Results

**114 files** contain `BACKEND_BIGQUERY`, but only **12 files** had `BACKEND_DUCKDB`.

#### Fixes Applied

| File | Method | Change |
|------|--------|--------|
| `legacy-app/.../models/Row/Project.php` | `getAssignedBackends()` | Added `hasDuckdbActivated()` check |
| `legacy-app/.../models/Row/Project.php` | `removeBackend()` | Added `BACKEND_DUCKDB` case |
| `src/Manage/Projects/Entity/Project.php` | `getRootCredentialsForBackend()` | Added DuckDB to match expression |
| `src/Manage/Projects/Entity/Project.php` | `getDefaultConnectionForBackend()` | Added DuckDB to match expression |
| `src/Storage/Files/Dto/File.php` | `createArrayForElasticSearch()` | Added `'duckdb'` to provider PHPDoc |

#### Files Already Having DuckDB (12 files - OK)

- `BackendSupportsInterface.php` - SUPPORTED_BACKENDS, DEV_BRANCH_SUPPORTED_BACKENDS
- `Model_Row_Project.php` - supportsBackend(), getDefaultConnectionForBackend()
- `StorageBucket.php` - getFullPath()
- `CreateBucketRequest.php` - bucket creation validation
- `BackendAssign.php` - backend assignment + file storage
- `CredentialsResolver.php` - credentials routing
- `TableCreate.php` - table creation with column types
- `Model_Row_Bucket.php`, `Model_Buckets.php` - bucket models
- `CommonBackendConfigurationFactory.php`, `DriverClientFactory.php`

#### Files NOT Needing DuckDB (intentional)

| Category | Files | Reason |
|----------|-------|--------|
| Workspace provisioning | ~15 | DuckDB uses PG Wire, not DB credentials |
| BigQuery-specific | ~10 | BQ health check, GCP integration |
| Column definitions | ~5 | DuckDB uses gRPC with own types |
| E2E tests | ~30 | Will add DuckDB tests later |
| Provisioning commands | ~8 | BQ infrastructure scripts |

#### Pre-existing PHPStan Errors (Not Blocking)

These errors existed before our changes and don't block basic operations:

```
Model_Row_Project.php:477
  Property Model_Row_Project::$idDuckdbFileStorage (string|null) does not accept int.

ProjectService.php:92
  Missing parameter $idDefaultConnectionDuckdb (int|null) in call to
  Manage_Request_CreateMaintainerData constructor.

DuckDBCredentialsResolver.php:92
  Call to an undefined method StorageCredentials::getProject().

DuckDBDriverClient.php:176
  Argument of an invalid type object supplied for foreach.

DuckDBDriverClient.php:196
  Parameter #1 $string of function strtolower expects string, string|null given.

DuckDBDriverClient.php:262
  Parameter #1 $data of method mergeFromJsonString() expects string, string|false given.

DuckDBDriverClient.php:281
  Class CreateTableResponse not found.

DuckDBDriverClient.php:289
  Method getResponseClass() should return class-string|null but returns string|null.
```

**Note:** These are technical debt items to be fixed in a separate cleanup task.

---

### Phase 12h.9: Bucket Mismatch Fix (2025-12-21)

#### Problem Discovered

When listing files via `GET /v2/storage/files`, Connection crashed with:
```
Warning: Undefined array key 1
Model_Row_File->getS3Key() at line 79
```

**Root Cause:** Bucket name mismatch between path generation and S3 API calls:
- `DuckDbAdapter::getLifeCyclePathMapper()` used `fileStorage->getFilesBucket()` = `"duckdb-files"`
- `DuckDbAdapter::createUploadParams()` used `'project_' . $file->getIdProject()` = `"project_8"`

This caused `s3Path` in Elasticsearch to be stored as `duckdb-files/exp-15/...` but S3 API calls used `project_8/...`.

When `Model_Row_File::getS3Key()` tried to parse old files with wrong format, `explode('/', s3Path, 2)` returned only 1 element and destructuring failed.

#### Fixes Applied

| File | Change |
|------|--------|
| `DuckDbAdapter.php` | Added `$projectId` constructor parameter + `getProjectBucket()` method |
| `DuckDbAdapter.php` | `getLifeCyclePathMapper()` now uses `project_{id}` as bucket |
| `FileBackupFactory.php` | Pass `$project->getId()` to DuckDbAdapter constructor |
| `Model_Row_File.php` | Made `getS3Key()`/`getS3Bucket()` handle edge cases (null/empty s3Path) |

#### Verification

```bash
# File prepare now returns correct bucket
POST /v2/storage/files/prepare
# Response: url = "http://localhost:8000/s3/project_8/exp-15/8/files/.../27.test_data.csv"

# File list works (no crash)
GET /v2/storage/files?limit=5
# Response: [{"id":27, "provider":"duckdb", "url":"http://localhost:8000/s3/project_8/..."}]
```

---

### Phase 12h.10: Import URL Generation Fix (2025-12-21)

#### Problem Discovered

File import via `POST /v2/storage/tables/{tableId}/import-async` failed with:
```
IO Error: No files found that match the pattern "project_8/exp-15/8/files/..."
```

**Root Cause:** DuckDB import handler received file path as:
- `root = "project_8"`
- `path = "exp-15/8/files/2025/12/21"`
- `fileName = "27.test_data.csv"`

With `fileProvider = HTTP (3)`, the handler built a relative path `project_8/exp-15/...` instead of a full URL.

DuckDB's httpfs extension needs a proper HTTP URL to fetch the file from our S3-compatible API.

#### Fix Applied

**File:** `duckdb-api-service/src/grpc/handlers/import_export.py`

Added detection of DuckDB bucket format in `_build_file_url()`:
```python
# Check if this is a DuckDB bucket (project_N format)
if root.startswith('project_'):
    from src.config import settings
    # Build URL to our S3-compatible API
    base_url = settings.service_url.rstrip('/')
    url = f"{base_url}/s3/{root}"
    if path:
        url = f"{url}/{path.strip('/')}"
    if file_name:
        url = f"{url}/{file_name}"
    return url
```

**File:** `duckdb-api-service/src/config.py`

Added `service_url` setting:
```python
# Self-referential URL for S3-compatible API (used for file imports)
service_url: str = "http://localhost:8000"
```

#### Status: SUPERSEDED by Phase 12h.11

The HTTP URL approach caused issues (signature mismatch, self-calling deadlock).
See Phase 12h.11 for the correct solution using local filesystem paths.

---

### Phase 12h.11: Import Handler Fixes (2025-12-21)

#### Goal
Fix table import to work correctly with DuckDB's local file storage.

#### Problems Discovered

1. **Self-Calling Deadlock**: Import handler generated HTTP URL to `localhost:8000/s3/...`,
   then DuckDB's httpfs tried to fetch from it. But the server was blocked waiting for the
   import to complete, creating a circular wait.

2. **Signature Mismatch**: Even if no deadlock, different processes (Connection workers
   vs DuckDB API) had different signing keys, causing 403 Forbidden errors.

3. **Column Count Mismatch**: Table has 4 columns (id, name, email, _timestamp) but CSV
   has only 3 columns. The `INSERT ... SELECT *` syntax failed.

#### Fixes Applied

**File:** `duckdb-api-service/src/grpc/handlers/import_export.py`

**Fix 1: Use Local Filesystem Instead of HTTP**
```python
# Check if this is a DuckDB bucket (project_N format)
# These files are stored locally in files_dir, so read directly from filesystem
# to avoid self-calling deadlock (httpfs calling our own S3 API)
if root.startswith('project_'):
    from src.config import settings

    # Build the key (path within bucket)
    key_parts = []
    if path:
        key_parts.append(path.strip('/'))
    if file_name:
        key_parts.append(file_name)
    key = '/'.join(key_parts) if key_parts else ''

    # Build local filesystem path
    # files_dir/project_N/key
    local_path = settings.files_dir / root / key
    return str(local_path)
```

**Fix 2: Handle _timestamp Column**
```python
# Import using INSERT INTO ... SELECT from read_csv
# Exclude system columns (_timestamp) from the import
# The CSV contains user data columns, we add _timestamp automatically
data_columns = [c for c in columns if not c.startswith('_')]
columns_sql = ', '.join(data_columns)

copy_sql = f"""
    INSERT INTO main.{TABLE_DATA_NAME} ({columns_sql}, _timestamp)
    SELECT {columns_sql}, CURRENT_TIMESTAMP
    FROM read_csv('{file_url}', {', '.join(csv_read_opts)})
"""
```

#### Verification

Import job 1000008 completed successfully:
```json
{
    "id": 1000008,
    "status": "success",
    "results": {
        "totalRowsCount": 3,
        "importedColumns": ["id", "name", "email"],
        "totalDataSizeBytes": 798720
    }
}
```

Data in DuckDB table:
```
('1', 'Alice', 'alice@example.com', datetime.datetime(2025, 12, 21, 22, 33, 24))
('2', 'Bob', 'bob@example.com', datetime.datetime(2025, 12, 21, 22, 33, 24))
('3', 'Charlie', 'charlie@example.com', datetime.datetime(2025, 12, 21, 22, 33, 24))
```

#### Key Insights

1. **Local vs HTTP**: For on-premise DuckDB deployments, always use local filesystem
   paths instead of HTTP URLs to avoid deadlock and signature issues.

2. **System Columns**: DuckDB tables may have system columns like `_timestamp` that
   are not in the source CSV. The import must explicitly list columns and set
   system values.

3. **Incremental Import**: With primary keys, incremental imports fail on duplicates.
   Full import (incremental=false) deletes existing data first.

---

### Phase 12h.12: Path Parsing Fix (2025-12-21)

#### Goal
Fix gRPC handlers to correctly parse paths when Connection sends only bucket name.

#### Problem Discovered

When viewing table data preview in Connection UI, the request failed with:
```
{"detail":"Path must contain at least [project_id, bucket_name]"}
```

**Root Cause:** Connection's `PreviewTableDataService.php` sends path with only bucket schema:
```php
$path = new RepeatedField(GPBType::STRING);
$path[] = $sourceTable->getBucket()->getSchemaName();  // Only "in_c_test"
```

But `PreviewTableHandler` in Python required at least 2 path elements.

#### Fixes Applied

**File:** `duckdb-api-service/src/grpc/handlers/table.py`

Updated `PreviewTableHandler` and `DropTableHandler` to use flexible path parsing
(same pattern as `CreateTableHandler`):

```python
# Parse path flexibly:
# - [bucket_name] - project_id comes from credentials
# - [project_id, bucket_name]
# - [project_id, branch_id, bucket_name]
if len(path) == 1:
    # Path contains only bucket_name, get project_id from credentials
    if not credentials or 'project_id' not in credentials:
        raise ValueError("Path must contain [project_id, bucket_name] or credentials must have project_id")
    project_id = credentials['project_id']
    bucket_name = path[0]
    branch_id = "default"
elif len(path) == 2:
    project_id = path[0]
    bucket_name = path[1]
    branch_id = "default"
else:
    project_id = path[0]
    bucket_name = path[-1]
    branch_id = path[1] if len(path) > 2 else "default"
```

#### Verification

```bash
# Test PreviewTableCommand with single-element path
curl -X POST http://localhost:8000/driver/execute \
  -H "Authorization: Bearer proj_8_admin_..." \
  -d '{"command":{"type":"PreviewTableCommand","path":["in_c_test"],"tableName":"users"},
       "credentials":{"project_id":"8"}}'

# Response: 3 rows (Alice, Bob, Charlie) with all columns
```

#### Key Insight

Storage drivers use a **credential-based project_id** pattern:
- PHP sends bucket schema name in `path` (e.g., `["in_c_test"]`)
- PHP sends project_id in `credentials.host`
- Python extracts `project_id` from `credentials['project_id']`

This is the standard pattern used by all Keboola storage drivers (Snowflake, BigQuery).

---

## Related Documents

- [Phase 12b: Connection Backend Registration](phase-12-php-driver.md)
- [ADR-014: gRPC Driver Interface](../adr/014-grpc-driver-interface.md)
- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/)
- [MinIO S3 Compatibility](https://min.io/docs/minio/linux/reference/minio-mc-admin.html)
