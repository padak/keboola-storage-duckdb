"""Tests for Table Import/Export API endpoints."""

import io
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
        response = test_client.post(
            "/backend/init",
            headers={"Authorization": "Bearer test-admin-key"},
        )
        assert response.status_code == 200
        yield test_client


@pytest.fixture
def project_with_table(client):
    """Create a test project with bucket and table."""
    # Create project
    response = client.post(
        "/projects",
        json={"id": "test-project", "name": "Test Project"},
        headers={"Authorization": "Bearer test-admin-key"},
    )
    assert response.status_code == 201
    api_key = response.json()["api_key"]

    # Create bucket
    response = client.post(
        "/projects/test-project/buckets",
        json={"name": "in_c_test"},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert response.status_code == 201

    # Create table
    response = client.post(
        "/projects/test-project/buckets/in_c_test/tables",
        json={
            "name": "users",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR"},
                {"name": "email", "type": "VARCHAR"},
            ],
            "primary_key": ["id"],
        },
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert response.status_code == 201

    return {
        "project_id": "test-project",
        "bucket_name": "in_c_test",
        "table_name": "users",
        "api_key": api_key,
    }


def _upload_file(client, project_id: str, api_key: str, content: bytes, filename: str = "data.csv") -> str:
    """Helper to upload a file and return file_id."""
    # Prepare
    prepare_response = client.post(
        f"/projects/{project_id}/files/prepare",
        json={"filename": filename},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    upload_key = prepare_response.json()["upload_key"]

    # Upload
    client.post(
        f"/projects/{project_id}/files/upload/{upload_key}",
        files={"file": (filename, io.BytesIO(content), "text/csv")},
        headers={"Authorization": f"Bearer {api_key}"},
    )

    # Register
    register_response = client.post(
        f"/projects/{project_id}/files",
        json={"upload_key": upload_key},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    return register_response.json()["id"]


class TestImportFromFile:
    """Test import from file endpoint."""

    def test_import_csv_success(self, client, project_with_table):
        """Test successful CSV import."""
        csv_content = b"id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        # Import file
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={
                "file_id": file_id,
                "format": "csv",
            },
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["imported_rows"] == 2
        assert data["table_rows_after"] == 2

    def test_import_csv_incremental(self, client, project_with_table):
        """Test incremental CSV import."""
        # First import
        csv_content1 = b"id,name,email\n1,Alice,alice@test.com\n"
        file_id1 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content1,
            "data1.csv",
        )

        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id1, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200
        assert response.json()["table_rows_after"] == 1

        # Second incremental import
        csv_content2 = b"id,name,email\n2,Bob,bob@test.com\n"
        file_id2 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content2,
            "data2.csv",
        )

        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={
                "file_id": file_id2,
                "format": "csv",
                "import_options": {"incremental": True},
            },
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["table_rows_after"] == 2  # Both rows present

    def test_import_csv_upsert(self, client, project_with_table):
        """Test CSV import with upsert (update duplicates)."""
        # First import
        csv_content1 = b"id,name,email\n1,Alice,alice@test.com\n"
        file_id1 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content1,
            "data1.csv",
        )

        client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id1, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )

        # Upsert import - update existing row
        csv_content2 = b"id,name,email\n1,Alice Updated,alice.new@test.com\n"
        file_id2 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content2,
            "data2.csv",
        )

        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={
                "file_id": file_id2,
                "format": "csv",
                "import_options": {
                    "incremental": True,
                    "dedup_mode": "update_duplicates",
                },
            },
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200
        assert response.json()["table_rows_after"] == 1  # Still one row, but updated

        # Verify data was updated
        preview = client.get(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/preview",
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        rows = preview.json()["rows"]
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice Updated"

    def test_import_file_not_found(self, client, project_with_table):
        """Test import with non-existent file."""
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": "nonexistent", "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "file_not_found"

    def test_import_table_not_found(self, client, project_with_table):
        """Test import to non-existent table."""
        csv_content = b"id,name\n1,Alice\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/nonexistent/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"

    def test_import_invalid_format(self, client, project_with_table):
        """Test import with invalid format."""
        csv_content = b"id,name,email\n1,Alice,alice@test.com\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id, "format": "xlsx"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_format"


class TestExportToFile:
    """Test export to file endpoint."""

    def test_export_csv_success(self, client, project_with_table):
        """Test successful CSV export."""
        # First import some data
        csv_content = b"id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )

        # Export to CSV
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["rows_exported"] == 2
        assert "file_id" in data
        assert data["file_size_bytes"] > 0

    def test_export_with_filter(self, client, project_with_table):
        """Test export with WHERE filter."""
        # Import data
        csv_content = b"id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )

        # Export with filter
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "csv", "where_filter": "id = 1"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200
        assert response.json()["rows_exported"] == 1

    def test_export_with_limit(self, client, project_with_table):
        """Test export with row limit."""
        # Import data
        csv_content = b"id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n3,Charlie,charlie@test.com\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )

        # Export with limit
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "csv", "limit": 2},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200
        assert response.json()["rows_exported"] == 2

    def test_export_specific_columns(self, client, project_with_table):
        """Test export with specific columns."""
        # Import data
        csv_content = b"id,name,email\n1,Alice,alice@test.com\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )

        # Export only id and name columns
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "csv", "columns": ["id", "name"]},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200

    def test_export_parquet(self, client, project_with_table):
        """Test Parquet export."""
        # Import data
        csv_content = b"id,name,email\n1,Alice,alice@test.com\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )

        # Export to Parquet
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "parquet"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 200
        assert response.json()["rows_exported"] == 1

    def test_export_table_not_found(self, client, project_with_table):
        """Test export from non-existent table."""
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/nonexistent/export",
            json={"format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"

    def test_export_invalid_compression(self, client, project_with_table):
        """Test export with invalid compression."""
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "csv", "compression": "zstd"},  # zstd not valid for CSV
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_compression"

    def test_export_invalid_where_clause(self, client, project_with_table):
        """Test export with SQL injection in WHERE clause."""
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "csv", "where_filter": "1=1; DROP TABLE users;"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_where_clause"


class TestImportExportAuth:
    """Test Import/Export authentication."""

    def test_import_requires_auth(self, client, project_with_table):
        """Test that import requires authentication."""
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": "some-id", "format": "csv"},
        )
        assert response.status_code == 401

    def test_export_requires_auth(self, client, project_with_table):
        """Test that export requires authentication."""
        response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "csv"},
        )
        assert response.status_code == 401


class TestImportExportE2E:
    """End-to-end import/export tests."""

    def test_roundtrip_csv(self, client, project_with_table):
        """Test full roundtrip: import CSV -> export CSV -> verify content."""
        # Import
        csv_content = b"id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        import_response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 2

        # Export
        export_response = client.post(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/export",
            json={"format": "csv"},
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert export_response.status_code == 200
        export_file_id = export_response.json()["file_id"]

        # Download exported file
        download_response = client.get(
            f"/projects/{project_with_table['project_id']}/files/{export_file_id}/download",
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert download_response.status_code == 200

        # Verify content (should have header + 2 rows)
        exported_content = download_response.content.decode()
        lines = exported_content.strip().split("\n")
        assert len(lines) == 3  # header + 2 data rows
        assert "id" in lines[0]
        assert "name" in lines[0]
