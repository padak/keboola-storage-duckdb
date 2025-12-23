"""Tests for S3-Compatible API endpoints.

These tests verify that the S3-compatible API endpoints work correctly
and are compatible with AWS S3 SDK behavior.
"""

import base64
import hashlib
import io
from xml.etree import ElementTree as ET

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.config import settings


@pytest.fixture
def client(monkeypatch, tmp_path):
    """Create test client with temporary storage paths."""
    test_data_dir = tmp_path / "data"
    test_data_dir.mkdir()

    monkeypatch.setattr(settings, "data_dir", test_data_dir)
    monkeypatch.setattr(settings, "duckdb_dir", test_data_dir / "duckdb")
    monkeypatch.setattr(settings, "files_dir", test_data_dir / "files")
    monkeypatch.setattr(settings, "metadata_db_path", test_data_dir / "metadata.duckdb")
    monkeypatch.setattr(settings, "admin_api_key", "test-admin-key")

    with TestClient(app) as test_client:
        # Initialize storage
        response = test_client.post(
            "/backend/init",
            headers={"Authorization": "Bearer test-admin-key"},
        )
        assert response.status_code == 200

        yield test_client


@pytest.fixture
def project_with_auth(client):
    """Create a test project and return project_id and api_key."""
    response = client.post(
        "/projects",
        json={"id": "test-project", "name": "Test Project"},
        headers={"Authorization": "Bearer test-admin-key"},
    )
    assert response.status_code == 201
    data = response.json()
    return {"project_id": data["id"], "api_key": data["api_key"]}


class TestPutObject:
    """Test S3 PutObject operation."""

    def test_put_object_simple(self, client, project_with_auth):
        """Test simple file upload."""
        content = b"Hello, World!"
        project_id = project_with_auth["project_id"]

        response = client.put(
            f"/s3/project_{project_id}/test/hello.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        assert "ETag" in response.headers
        # ETag should be MD5 hash
        expected_etag = f'"{hashlib.md5(content).hexdigest()}"'
        assert response.headers["ETag"] == expected_etag

    def test_put_object_with_content_md5(self, client, project_with_auth):
        """Test upload with Content-MD5 verification."""
        content = b"Test data with MD5"
        content_md5 = base64.b64encode(hashlib.md5(content).digest()).decode()
        project_id = project_with_auth["project_id"]

        response = client.put(
            f"/s3/project_{project_id}/data/test.csv",
            content=content,
            headers={
                "Authorization": f"Bearer {project_with_auth['api_key']}",
                "Content-MD5": content_md5,
            },
        )

        assert response.status_code == 200

    def test_put_object_bad_md5(self, client, project_with_auth):
        """Test upload with incorrect Content-MD5 fails."""
        content = b"Test data with wrong MD5"
        wrong_md5 = base64.b64encode(b"wrong").decode()
        project_id = project_with_auth["project_id"]

        response = client.put(
            f"/s3/project_{project_id}/data/test.csv",
            content=content,
            headers={
                "Authorization": f"Bearer {project_with_auth['api_key']}",
                "Content-MD5": wrong_md5,
            },
        )

        assert response.status_code == 400
        # Response should be XML error
        root = ET.fromstring(response.content)
        assert root.find("Code").text == "BadDigest"

    def test_put_object_creates_directories(self, client, project_with_auth):
        """Test that nested directories are created automatically."""
        content = b"Nested file"
        project_id = project_with_auth["project_id"]

        response = client.put(
            f"/s3/project_{project_id}/deep/nested/path/file.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200

    def test_put_object_bucket_without_prefix(self, client, project_with_auth):
        """Test using bucket name without project_ prefix."""
        content = b"Test content"
        project_id = project_with_auth["project_id"]

        response = client.put(
            f"/s3/{project_id}/file.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200


class TestGetObject:
    """Test S3 GetObject operation."""

    def test_get_object_success(self, client, project_with_auth):
        """Test downloading a file."""
        content = b"File content to download"
        project_id = project_with_auth["project_id"]

        # Upload first
        client.put(
            f"/s3/project_{project_id}/download/test.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Download
        response = client.get(
            f"/s3/project_{project_id}/download/test.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        assert response.content == content
        assert "ETag" in response.headers
        assert "Content-Length" in response.headers
        assert "Last-Modified" in response.headers

    def test_get_object_not_found(self, client, project_with_auth):
        """Test downloading non-existent file returns 404."""
        project_id = project_with_auth["project_id"]

        response = client.get(
            f"/s3/project_{project_id}/nonexistent/file.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 404
        # Response should be XML error
        root = ET.fromstring(response.content)
        assert root.find("Code").text == "NoSuchKey"

    def test_get_object_etag_matches_content(self, client, project_with_auth):
        """Test that ETag matches content MD5."""
        content = b"Content for ETag verification"
        project_id = project_with_auth["project_id"]
        expected_etag = f'"{hashlib.md5(content).hexdigest()}"'

        # Upload
        client.put(
            f"/s3/project_{project_id}/etag/test.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Download and verify ETag
        response = client.get(
            f"/s3/project_{project_id}/etag/test.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.headers["ETag"] == expected_etag


class TestHeadObject:
    """Test S3 HeadObject operation."""

    def test_head_object_success(self, client, project_with_auth):
        """Test getting file metadata."""
        content = b"Metadata test content"
        project_id = project_with_auth["project_id"]

        # Upload
        client.put(
            f"/s3/project_{project_id}/meta/test.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Head
        response = client.head(
            f"/s3/project_{project_id}/meta/test.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        assert response.headers["Content-Length"] == str(len(content))
        assert "ETag" in response.headers
        assert "Last-Modified" in response.headers
        # HEAD should not return body
        assert response.content == b""

    def test_head_object_not_found(self, client, project_with_auth):
        """Test HEAD on non-existent file returns 404."""
        project_id = project_with_auth["project_id"]

        response = client.head(
            f"/s3/project_{project_id}/nonexistent/file.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 404


class TestDeleteObject:
    """Test S3 DeleteObject operation."""

    def test_delete_object_success(self, client, project_with_auth):
        """Test deleting a file."""
        content = b"File to delete"
        project_id = project_with_auth["project_id"]

        # Upload
        client.put(
            f"/s3/project_{project_id}/delete/test.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Verify exists
        response = client.head(
            f"/s3/project_{project_id}/delete/test.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200

        # Delete
        response = client.delete(
            f"/s3/project_{project_id}/delete/test.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 204

        # Verify deleted
        response = client.head(
            f"/s3/project_{project_id}/delete/test.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 404

    def test_delete_object_idempotent(self, client, project_with_auth):
        """Test that deleting non-existent file returns 204 (S3 behavior)."""
        project_id = project_with_auth["project_id"]

        # Delete non-existent file should succeed
        response = client.delete(
            f"/s3/project_{project_id}/nonexistent/file.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # S3 returns 204 even for non-existent keys
        assert response.status_code == 204


class TestListObjectsV2:
    """Test S3 ListObjectsV2 operation."""

    def test_list_objects_empty_bucket(self, client, project_with_auth):
        """Test listing objects in empty bucket."""
        project_id = project_with_auth["project_id"]

        response = client.get(
            f"/s3/project_{project_id}?list-type=2",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/xml"

        root = ET.fromstring(response.content)
        assert root.find("KeyCount").text == "0"
        assert root.find("IsTruncated").text == "false"

    def test_list_objects_with_files(self, client, project_with_auth):
        """Test listing objects with files."""
        project_id = project_with_auth["project_id"]

        # Upload some files
        for name in ["a.txt", "b.txt", "c.txt"]:
            client.put(
                f"/s3/project_{project_id}/list/{name}",
                content=f"Content of {name}".encode(),
                headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
            )

        response = client.get(
            f"/s3/project_{project_id}?list-type=2",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200

        root = ET.fromstring(response.content)
        assert root.find("KeyCount").text == "3"

        keys = [elem.text for elem in root.findall(".//Contents/Key")]
        assert len(keys) == 3
        assert all("list/" in key for key in keys)

    def test_list_objects_with_prefix(self, client, project_with_auth):
        """Test listing objects with prefix filter."""
        project_id = project_with_auth["project_id"]

        # Upload files in different directories
        client.put(
            f"/s3/project_{project_id}/data/file1.txt",
            content=b"1",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        client.put(
            f"/s3/project_{project_id}/data/file2.txt",
            content=b"2",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        client.put(
            f"/s3/project_{project_id}/other/file3.txt",
            content=b"3",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # List only data/ prefix
        response = client.get(
            f"/s3/project_{project_id}?list-type=2&prefix=data/",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200

        root = ET.fromstring(response.content)
        keys = [elem.text for elem in root.findall(".//Contents/Key")]

        assert len(keys) == 2
        assert all(key.startswith("data/") for key in keys)

    def test_list_objects_max_keys(self, client, project_with_auth):
        """Test max-keys parameter limits results."""
        project_id = project_with_auth["project_id"]

        # Upload 5 files
        for i in range(5):
            client.put(
                f"/s3/project_{project_id}/limit/file{i}.txt",
                content=f"Content {i}".encode(),
                headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
            )

        # Request with max-keys=2
        response = client.get(
            f"/s3/project_{project_id}?list-type=2&max-keys=2",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200

        root = ET.fromstring(response.content)
        keys = [elem.text for elem in root.findall(".//Contents/Key")]

        assert len(keys) == 2
        assert root.find("IsTruncated").text == "true"
        assert root.find("MaxKeys").text == "2"

    def test_list_objects_with_delimiter(self, client, project_with_auth):
        """Test delimiter parameter for virtual directories."""
        project_id = project_with_auth["project_id"]

        # Upload files in nested structure
        client.put(
            f"/s3/project_{project_id}/a/1.txt",
            content=b"1",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        client.put(
            f"/s3/project_{project_id}/b/2.txt",
            content=b"2",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        client.put(
            f"/s3/project_{project_id}/root.txt",
            content=b"root",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # List with delimiter (should return common prefixes)
        response = client.get(
            f"/s3/project_{project_id}?list-type=2&delimiter=/",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200

        root = ET.fromstring(response.content)

        # Should have common prefixes for a/ and b/
        common_prefixes = [elem.text for elem in root.findall(".//CommonPrefixes/Prefix")]
        assert "a/" in common_prefixes
        assert "b/" in common_prefixes

        # Root file should be in Contents
        keys = [elem.text for elem in root.findall(".//Contents/Key")]
        assert "root.txt" in keys


class TestAuthentication:
    """Test S3 API authentication."""

    def test_unauthorized_without_token(self, client, project_with_auth):
        """Test request without token is rejected."""
        project_id = project_with_auth["project_id"]

        response = client.get(f"/s3/project_{project_id}/test.txt")

        assert response.status_code == 401

    def test_x_api_key_header(self, client, project_with_auth):
        """Test authentication via X-Api-Key header."""
        project_id = project_with_auth["project_id"]
        content = b"X-Api-Key test"

        # Upload with X-Api-Key header
        response = client.put(
            f"/s3/project_{project_id}/xapikey/test.txt",
            content=content,
            headers={"X-Api-Key": project_with_auth["api_key"]},
        )

        assert response.status_code == 200

    def test_x_amz_security_token_header(self, client, project_with_auth):
        """Test authentication via x-amz-security-token header."""
        project_id = project_with_auth["project_id"]
        content = b"Security token test"

        # Upload with x-amz-security-token header
        response = client.put(
            f"/s3/project_{project_id}/sectoken/test.txt",
            content=content,
            headers={"x-amz-security-token": project_with_auth["api_key"]},
        )

        assert response.status_code == 200

    def test_unauthorized_wrong_project(self, client, project_with_auth):
        """Test accessing wrong project is rejected."""
        # Create second project
        response = client.post(
            "/projects",
            json={"id": "other-project", "name": "Other Project"},
            headers={"Authorization": "Bearer test-admin-key"},
        )
        assert response.status_code == 201

        # Try to access first project with second project's key
        other_key = response.json()["api_key"]
        project_id = project_with_auth["project_id"]

        response = client.get(
            f"/s3/project_{project_id}/test.txt",
            headers={"Authorization": f"Bearer {other_key}"},
        )

        assert response.status_code == 403

    def test_admin_key_accesses_any_project(self, client, project_with_auth):
        """Test admin key can access any project."""
        project_id = project_with_auth["project_id"]

        # Upload with admin key
        response = client.put(
            f"/s3/project_{project_id}/admin/test.txt",
            content=b"Admin uploaded",
            headers={"Authorization": "Bearer test-admin-key"},
        )

        assert response.status_code == 200

    def test_nonexistent_bucket_returns_404(self, client):
        """Test accessing non-existent bucket returns 404."""
        response = client.get(
            "/s3/nonexistent-bucket/file.txt",
            headers={"Authorization": "Bearer test-admin-key"},
        )

        assert response.status_code == 404


class TestPresignedUrls:
    """Test pre-signed URL generation and usage."""

    def test_presign_get_url(self, client, project_with_auth):
        """Test generating pre-signed GET URL."""
        project_id = project_with_auth["project_id"]
        content = b"Pre-signed content"

        # First upload a file
        client.put(
            f"/s3/project_{project_id}/presign/test.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Generate pre-signed URL
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "presign/test.txt", "method": "GET", "expires_in": 3600},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "url" in data
        assert "expires_at" in data
        assert data["method"] == "GET"
        assert "signature=" in data["url"]
        assert "expires=" in data["url"]

    def test_presign_url_works_without_auth(self, client, project_with_auth):
        """Test that pre-signed URL works without authentication headers."""
        project_id = project_with_auth["project_id"]
        content = b"Access without auth"

        # Upload a file
        client.put(
            f"/s3/project_{project_id}/presign/noauth.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Generate pre-signed URL
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "presign/noauth.txt", "method": "GET", "expires_in": 3600},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        presigned_url = response.json()["url"]

        # Extract path and query from URL
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(presigned_url)
        path_with_query = f"{parsed.path}?{parsed.query}"

        # Access without auth headers
        response = client.get(path_with_query)
        assert response.status_code == 200
        assert response.content == content

    def test_presign_put_url(self, client, project_with_auth):
        """Test generating and using pre-signed PUT URL."""
        project_id = project_with_auth["project_id"]

        # Generate pre-signed PUT URL
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "presign/upload.txt", "method": "PUT", "expires_in": 3600},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["method"] == "PUT"
        presigned_url = data["url"]

        # Upload using pre-signed URL (no auth headers)
        from urllib.parse import urlparse
        parsed = urlparse(presigned_url)
        path_with_query = f"{parsed.path}?{parsed.query}"

        content = b"Uploaded via presigned URL"
        response = client.put(path_with_query, content=content)
        assert response.status_code == 200

        # Verify content was uploaded
        response = client.get(
            f"/s3/project_{project_id}/presign/upload.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200
        assert response.content == content

    def test_presign_delete_url(self, client, project_with_auth):
        """Test generating and using pre-signed DELETE URL."""
        project_id = project_with_auth["project_id"]

        # Upload a file first
        client.put(
            f"/s3/project_{project_id}/presign/delete.txt",
            content=b"To be deleted",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Generate pre-signed DELETE URL
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "presign/delete.txt", "method": "DELETE", "expires_in": 3600},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        presigned_url = response.json()["url"]

        # Delete using pre-signed URL (no auth headers)
        from urllib.parse import urlparse
        parsed = urlparse(presigned_url)
        path_with_query = f"{parsed.path}?{parsed.query}"

        response = client.delete(path_with_query)
        assert response.status_code == 204

        # Verify file is deleted
        response = client.head(
            f"/s3/project_{project_id}/presign/delete.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 404

    def test_presign_head_url(self, client, project_with_auth):
        """Test generating and using pre-signed HEAD URL."""
        project_id = project_with_auth["project_id"]
        content = b"Head metadata"

        # Upload a file
        client.put(
            f"/s3/project_{project_id}/presign/head.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Generate pre-signed HEAD URL
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "presign/head.txt", "method": "HEAD", "expires_in": 3600},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        presigned_url = response.json()["url"]

        # HEAD using pre-signed URL (no auth headers)
        from urllib.parse import urlparse
        parsed = urlparse(presigned_url)
        path_with_query = f"{parsed.path}?{parsed.query}"

        response = client.head(path_with_query)
        assert response.status_code == 200
        assert response.headers["Content-Length"] == str(len(content))

    def test_presign_expired_url_rejected(self, client, project_with_auth, monkeypatch):
        """Test that expired pre-signed URLs are rejected."""
        import time as time_module
        project_id = project_with_auth["project_id"]

        # Upload a file
        client.put(
            f"/s3/project_{project_id}/presign/expired.txt",
            content=b"Expiring content",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Generate pre-signed URL with very short expiry
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "presign/expired.txt", "method": "GET", "expires_in": 1},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        presigned_url = response.json()["url"]

        # Wait for expiration
        time_module.sleep(2)

        # Try to use expired URL
        from urllib.parse import urlparse
        parsed = urlparse(presigned_url)
        path_with_query = f"{parsed.path}?{parsed.query}"

        response = client.get(path_with_query)
        assert response.status_code == 403

    def test_presign_wrong_method_rejected(self, client, project_with_auth):
        """Test that using wrong method with pre-signed URL is rejected."""
        project_id = project_with_auth["project_id"]

        # Upload a file
        client.put(
            f"/s3/project_{project_id}/presign/method.txt",
            content=b"Method test",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Generate pre-signed GET URL
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "presign/method.txt", "method": "GET", "expires_in": 3600},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        presigned_url = response.json()["url"]

        # Try to use it for DELETE (wrong method)
        from urllib.parse import urlparse
        parsed = urlparse(presigned_url)
        path_with_query = f"{parsed.path}?{parsed.query}"

        response = client.delete(path_with_query)
        assert response.status_code == 403

    def test_presign_tampered_signature_rejected(self, client, project_with_auth):
        """Test that tampered signatures are rejected."""
        project_id = project_with_auth["project_id"]

        # Upload a file
        client.put(
            f"/s3/project_{project_id}/presign/tamper.txt",
            content=b"Tamper test",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Generate pre-signed URL
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "presign/tamper.txt", "method": "GET", "expires_in": 3600},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        presigned_url = response.json()["url"]

        # Tamper with signature
        tampered_url = presigned_url.replace("signature=", "signature=tampered")

        from urllib.parse import urlparse
        parsed = urlparse(tampered_url)
        path_with_query = f"{parsed.path}?{parsed.query}"

        response = client.get(path_with_query)
        assert response.status_code == 403

    def test_presign_default_expiry(self, client, project_with_auth):
        """Test that default expiry is used when not specified."""
        project_id = project_with_auth["project_id"]

        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "test.txt", "method": "GET"},  # No expires_in
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "expires_at" in data

    def test_presign_requires_auth(self, client, project_with_auth):
        """Test that generating pre-signed URLs requires authentication."""
        project_id = project_with_auth["project_id"]

        # Try to generate without auth
        response = client.post(
            f"/s3/project_{project_id}/presign",
            json={"key": "test.txt", "method": "GET"},
        )

        assert response.status_code == 401


class TestS3Compatibility:
    """Test AWS S3 SDK compatibility features."""

    def test_etag_format(self, client, project_with_auth):
        """Test ETag is properly quoted (S3 format)."""
        content = b"ETag format test"
        project_id = project_with_auth["project_id"]

        response = client.put(
            f"/s3/project_{project_id}/etag/format.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        etag = response.headers["ETag"]
        # ETag should be quoted
        assert etag.startswith('"')
        assert etag.endswith('"')
        # Inner value should be MD5 hex
        inner = etag[1:-1]
        assert len(inner) == 32  # MD5 hex is 32 chars

    def test_last_modified_format(self, client, project_with_auth):
        """Test Last-Modified header format (HTTP-date)."""
        content = b"Last-Modified test"
        project_id = project_with_auth["project_id"]

        # Upload
        client.put(
            f"/s3/project_{project_id}/date/test.txt",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Get and check header
        response = client.get(
            f"/s3/project_{project_id}/date/test.txt",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        last_modified = response.headers.get("Last-Modified")
        assert last_modified is not None
        # Should contain GMT
        assert "GMT" in last_modified

    def test_xml_response_format(self, client, project_with_auth):
        """Test ListObjectsV2 returns proper XML."""
        project_id = project_with_auth["project_id"]

        response = client.get(
            f"/s3/project_{project_id}?list-type=2",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Check content type
        assert "application/xml" in response.headers["content-type"]

        # Parse and verify structure
        root = ET.fromstring(response.content)
        assert root.tag == "ListBucketResult"
        assert root.find("Name") is not None
        assert root.find("MaxKeys") is not None
        assert root.find("KeyCount") is not None
        assert root.find("IsTruncated") is not None

    def test_binary_content(self, client, project_with_auth):
        """Test uploading and downloading binary content."""
        # Create binary content with all byte values
        content = bytes(range(256))
        project_id = project_with_auth["project_id"]

        # Upload
        response = client.put(
            f"/s3/project_{project_id}/binary/data.bin",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200

        # Download and verify
        response = client.get(
            f"/s3/project_{project_id}/binary/data.bin",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200
        assert response.content == content

    def test_large_file(self, client, project_with_auth):
        """Test uploading larger file (1MB)."""
        content = b"x" * (1024 * 1024)  # 1MB
        project_id = project_with_auth["project_id"]

        response = client.put(
            f"/s3/project_{project_id}/large/1mb.bin",
            content=content,
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        assert response.status_code == 200

        # Verify content length in HEAD
        response = client.head(
            f"/s3/project_{project_id}/large/1mb.bin",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.headers["Content-Length"] == str(len(content))
