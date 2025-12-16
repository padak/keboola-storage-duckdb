"""Tests for Files API endpoints."""

import io
import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.config import settings


@pytest.fixture
def client(monkeypatch, tmp_path):
    """Create test client with temporary storage paths."""
    # Override settings paths for testing
    test_data_dir = tmp_path / "data"
    test_data_dir.mkdir()

    monkeypatch.setattr(settings, "data_dir", test_data_dir)
    monkeypatch.setattr(settings, "duckdb_dir", test_data_dir / "duckdb")
    monkeypatch.setattr(settings, "files_dir", test_data_dir / "files")
    monkeypatch.setattr(settings, "metadata_db_path", test_data_dir / "metadata.duckdb")
    monkeypatch.setattr(settings, "admin_api_key", "test-admin-key")

    # Initialize backend
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


class TestFilePrepare:
    """Test file upload preparation."""

    def test_prepare_upload_success(self, client, project_with_auth):
        """Test successful upload preparation."""
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "data.csv", "content_type": "text/csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "upload_key" in data
        assert "upload_url" in data
        assert "expires_at" in data
        assert data["upload_url"].startswith(f"/projects/{project_with_auth['project_id']}/files/upload/")

    def test_prepare_upload_with_size(self, client, project_with_auth):
        """Test upload preparation with file size."""
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={
                "filename": "data.csv",
                "content_type": "text/csv",
                "size_bytes": 1024,
            },
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200

    def test_prepare_upload_with_tags(self, client, project_with_auth):
        """Test upload preparation with tags."""
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={
                "filename": "data.csv",
                "tags": {"source": "test", "version": "1.0"},
            },
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200

    def test_prepare_upload_project_not_found(self, client):
        """Test upload preparation with non-existent project."""
        response = client.post(
            "/projects/nonexistent/files/prepare",
            json={"filename": "data.csv"},
            headers={"Authorization": "Bearer test-admin-key"},
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_prepare_upload_file_too_large(self, client, project_with_auth):
        """Test upload preparation rejects oversized files."""
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={
                "filename": "huge.csv",
                "size_bytes": 100 * 1024 * 1024 * 1024,  # 100GB
            },
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "file_too_large"


class TestFileUpload:
    """Test file upload."""

    def test_upload_file_success(self, client, project_with_auth):
        """Test successful file upload."""
        # Prepare upload
        prepare_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "test.csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        upload_key = prepare_response.json()["upload_key"]

        # Upload file
        csv_content = b"id,name\n1,Alice\n2,Bob\n"
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/upload/{upload_key}",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["upload_key"] == upload_key
        assert data["size_bytes"] == len(csv_content)
        assert "checksum_sha256" in data
        assert len(data["checksum_sha256"]) == 64

    def test_upload_file_invalid_key(self, client, project_with_auth):
        """Test upload with invalid upload key."""
        csv_content = b"id,name\n1,Alice\n"
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/upload/invalid-key",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "upload_session_not_found"


class TestFileRegister:
    """Test file registration."""

    def test_register_file_success(self, client, project_with_auth):
        """Test successful file registration."""
        # Prepare and upload
        prepare_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "test.csv", "content_type": "text/csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        upload_key = prepare_response.json()["upload_key"]

        csv_content = b"id,name\n1,Alice\n2,Bob\n"
        client.post(
            f"/projects/{project_with_auth['project_id']}/files/upload/{upload_key}",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Register file
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files",
            json={"upload_key": upload_key},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "test.csv"
        assert data["size_bytes"] == len(csv_content)
        assert data["is_staged"] is False
        assert data["content_type"] == "text/csv"

    def test_register_file_with_custom_name(self, client, project_with_auth):
        """Test file registration with custom name."""
        # Prepare and upload
        prepare_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "original.csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        upload_key = prepare_response.json()["upload_key"]

        csv_content = b"id,name\n1,Alice\n"
        client.post(
            f"/projects/{project_with_auth['project_id']}/files/upload/{upload_key}",
            files={"file": ("original.csv", io.BytesIO(csv_content), "text/csv")},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        # Register with custom name
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files",
            json={"upload_key": upload_key, "name": "renamed.csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 201
        assert response.json()["name"] == "renamed.csv"

    def test_register_file_not_uploaded(self, client, project_with_auth):
        """Test registration of non-uploaded file."""
        # Prepare but don't upload
        prepare_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "test.csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        upload_key = prepare_response.json()["upload_key"]

        # Try to register without uploading
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files",
            json={"upload_key": upload_key},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "file_not_uploaded"


class TestFileList:
    """Test file listing."""

    def test_list_files_empty(self, client, project_with_auth):
        """Test listing files when empty."""
        response = client.get(
            f"/projects/{project_with_auth['project_id']}/files",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["files"] == []
        assert data["total"] == 0

    def test_list_files_multiple(self, client, project_with_auth):
        """Test listing multiple files."""
        # Upload and register two files
        for i in range(2):
            prepare_response = client.post(
                f"/projects/{project_with_auth['project_id']}/files/prepare",
                json={"filename": f"file{i}.csv"},
                headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
            )
            upload_key = prepare_response.json()["upload_key"]

            csv_content = f"id,name\n{i},Test{i}\n".encode()
            client.post(
                f"/projects/{project_with_auth['project_id']}/files/upload/{upload_key}",
                files={"file": (f"file{i}.csv", io.BytesIO(csv_content), "text/csv")},
                headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
            )

            client.post(
                f"/projects/{project_with_auth['project_id']}/files",
                json={"upload_key": upload_key},
                headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
            )

        # List files
        response = client.get(
            f"/projects/{project_with_auth['project_id']}/files",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["files"]) == 2


class TestFileGet:
    """Test getting file info."""

    def test_get_file_success(self, client, project_with_auth):
        """Test getting file info."""
        # Upload and register file
        prepare_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "test.csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        upload_key = prepare_response.json()["upload_key"]

        csv_content = b"id,name\n1,Alice\n"
        client.post(
            f"/projects/{project_with_auth['project_id']}/files/upload/{upload_key}",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        register_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files",
            json={"upload_key": upload_key},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        file_id = register_response.json()["id"]

        # Get file info
        response = client.get(
            f"/projects/{project_with_auth['project_id']}/files/{file_id}",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == file_id
        assert data["name"] == "test.csv"

    def test_get_file_not_found(self, client, project_with_auth):
        """Test getting non-existent file."""
        response = client.get(
            f"/projects/{project_with_auth['project_id']}/files/nonexistent",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 404


class TestFileDownload:
    """Test file download."""

    def test_download_file_success(self, client, project_with_auth):
        """Test downloading a file."""
        # Upload and register file
        prepare_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "test.csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        upload_key = prepare_response.json()["upload_key"]

        csv_content = b"id,name\n1,Alice\n2,Bob\n"
        client.post(
            f"/projects/{project_with_auth['project_id']}/files/upload/{upload_key}",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        register_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files",
            json={"upload_key": upload_key},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        file_id = register_response.json()["id"]

        # Download file
        response = client.get(
            f"/projects/{project_with_auth['project_id']}/files/{file_id}/download",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 200
        assert response.content == csv_content


class TestFileDelete:
    """Test file deletion."""

    def test_delete_file_success(self, client, project_with_auth):
        """Test deleting a file."""
        # Upload and register file
        prepare_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "test.csv"},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        upload_key = prepare_response.json()["upload_key"]

        csv_content = b"id,name\n1,Alice\n"
        client.post(
            f"/projects/{project_with_auth['project_id']}/files/upload/{upload_key}",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )

        register_response = client.post(
            f"/projects/{project_with_auth['project_id']}/files",
            json={"upload_key": upload_key},
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        file_id = register_response.json()["id"]

        # Delete file
        response = client.delete(
            f"/projects/{project_with_auth['project_id']}/files/{file_id}",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 204

        # Verify file is gone
        get_response = client.get(
            f"/projects/{project_with_auth['project_id']}/files/{file_id}",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert get_response.status_code == 404

    def test_delete_file_not_found(self, client, project_with_auth):
        """Test deleting non-existent file."""
        response = client.delete(
            f"/projects/{project_with_auth['project_id']}/files/nonexistent",
            headers={"Authorization": f"Bearer {project_with_auth['api_key']}"},
        )
        assert response.status_code == 404


class TestFilesAuth:
    """Test Files API authentication."""

    def test_prepare_requires_auth(self, client, project_with_auth):
        """Test that prepare requires authentication."""
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "test.csv"},
        )
        assert response.status_code == 401

    def test_list_requires_auth(self, client, project_with_auth):
        """Test that list requires authentication."""
        response = client.get(
            f"/projects/{project_with_auth['project_id']}/files",
        )
        assert response.status_code == 401

    def test_wrong_project_key_rejected(self, client, project_with_auth):
        """Test that wrong project key is rejected."""
        # Create another project
        response = client.post(
            "/projects",
            json={"id": "other-project", "name": "Other Project"},
            headers={"Authorization": "Bearer test-admin-key"},
        )
        other_key = response.json()["api_key"]

        # Try to access first project with second key
        response = client.post(
            f"/projects/{project_with_auth['project_id']}/files/prepare",
            json={"filename": "test.csv"},
            headers={"Authorization": f"Bearer {other_key}"},
        )
        assert response.status_code == 403
