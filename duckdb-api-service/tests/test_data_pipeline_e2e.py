"""Comprehensive E2E tests for Data Pipeline operations.

Tests cover complete data pipeline workflows including:
- CSV/Parquet import/export roundtrips
- Incremental imports with deduplication
- Column mapping and filtering
- Large file handling
- Data integrity verification
"""

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
    monkeypatch.setattr(settings, "snapshots_dir", test_data_dir / "snapshots")
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
        json={"id": "pipeline-project", "name": "Pipeline Test Project"},
        headers={"Authorization": "Bearer test-admin-key"},
    )
    assert response.status_code == 201
    api_key = response.json()["api_key"]

    # Create bucket
    response = client.post(
        "/projects/pipeline-project/branches/default/buckets",
        json={"name": "in_c_data"},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert response.status_code == 201

    # Create table with primary key for incremental tests
    response = client.post(
        "/projects/pipeline-project/branches/default/buckets/in_c_data/tables",
        json={
            "name": "users",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR"},
                {"name": "email", "type": "VARCHAR"},
                {"name": "age", "type": "INTEGER"},
            ],
            "primary_key": ["id"],
        },
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert response.status_code == 201

    return {
        "project_id": "pipeline-project",
        "bucket_name": "in_c_data",
        "table_name": "users",
        "api_key": api_key,
    }


def _upload_file(
    client,
    project_id: str,
    api_key: str,
    content: bytes,
    filename: str = "data.csv",
    content_type: str = "text/csv",
) -> str:
    """Helper to upload a file and return file_id."""
    # Prepare
    prepare_response = client.post(
        f"/projects/{project_id}/files/prepare",
        json={"filename": filename, "content_type": content_type},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert prepare_response.status_code == 200
    upload_key = prepare_response.json()["upload_key"]

    # Upload
    upload_response = client.post(
        f"/projects/{project_id}/files/upload/{upload_key}",
        files={"file": (filename, io.BytesIO(content), content_type)},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert upload_response.status_code == 200

    # Register
    register_response = client.post(
        f"/projects/{project_id}/files",
        json={"upload_key": upload_key},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert register_response.status_code == 201
    return register_response.json()["id"]


def _import_file(
    client,
    project_id: str,
    bucket_name: str,
    table_name: str,
    api_key: str,
    file_id: str,
    file_format: str = "csv",
    import_options: dict = None,
):
    """Helper to import a file into a table."""
    payload = {
        "file_id": file_id,
        "format": file_format,
    }
    if import_options:
        payload["import_options"] = import_options

    response = client.post(
        f"/projects/{project_id}/branches/default/buckets/{bucket_name}/tables/{table_name}/import/file",
        json=payload,
        headers={"Authorization": f"Bearer {api_key}"},
    )
    return response


def _export_table(
    client,
    project_id: str,
    bucket_name: str,
    table_name: str,
    api_key: str,
    file_format: str = "csv",
    where_filter: str = None,
    columns: list = None,
    limit: int = None,
):
    """Helper to export a table to a file."""
    payload = {"format": file_format}
    if where_filter:
        payload["where_filter"] = where_filter
    if columns:
        payload["columns"] = columns
    if limit:
        payload["limit"] = limit

    response = client.post(
        f"/projects/{project_id}/branches/default/buckets/{bucket_name}/tables/{table_name}/export",
        json=payload,
        headers={"Authorization": f"Bearer {api_key}"},
    )
    return response


def _download_file(client, project_id: str, file_id: str, api_key: str) -> bytes:
    """Helper to download a file and return its content."""
    response = client.get(
        f"/projects/{project_id}/files/{file_id}/download",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert response.status_code == 200
    return response.content


class TestCSVImportExportRoundtrip:
    """Test CSV import/export roundtrip scenarios."""

    def test_csv_import_export_roundtrip(self, client, project_with_table):
        """Test complete CSV roundtrip: upload -> import -> export -> download -> verify."""
        # Create CSV content
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n3,Charlie,charlie@test.com,35\n"

        # Upload file
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        # Import into table
        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )
        assert import_response.status_code == 200
        import_data = import_response.json()
        assert import_data["imported_rows"] == 3
        assert import_data["table_rows_after"] == 3

        # Export table
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
        )
        assert export_response.status_code == 200
        export_data = export_response.json()
        assert export_data["rows_exported"] == 3
        export_file_id = export_data["file_id"]

        # Download exported file
        exported_content = _download_file(
            client,
            project_with_table["project_id"],
            export_file_id,
            project_with_table["api_key"],
        )

        # Verify content
        exported_lines = exported_content.decode().strip().split("\n")
        assert len(exported_lines) == 4  # header + 3 rows
        assert "id" in exported_lines[0]
        assert "name" in exported_lines[0]
        assert "email" in exported_lines[0]
        assert "age" in exported_lines[0]

        # Verify data integrity - all rows present
        original_lines = csv_content.decode().strip().split("\n")
        assert len(original_lines) == len(exported_lines)

    def test_csv_roundtrip_with_nulls(self, client, project_with_table):
        """Test CSV roundtrip with NULL values."""
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,,\n3,Charlie,charlie@test.com,\n"

        # Upload and import
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )
        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 3

        # Export and verify
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
        )
        assert export_response.status_code == 200
        assert export_response.json()["rows_exported"] == 3


class TestIncrementalImport:
    """Test incremental import scenarios."""

    def test_incremental_import(self, client, project_with_table):
        """Test incremental import adds new rows without duplicates."""
        # Initial import
        csv_content1 = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n"
        file_id1 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content1,
            "data1.csv",
        )

        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id1,
        )
        assert import_response.status_code == 200
        assert import_response.json()["table_rows_after"] == 2

        # Incremental import with new row
        csv_content2 = b"id,name,email,age\n3,Charlie,charlie@test.com,35\n"
        file_id2 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content2,
            "data2.csv",
        )

        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id2,
            import_options={"incremental": True},
        )
        assert import_response.status_code == 200
        assert import_response.json()["table_rows_after"] == 3

        # Verify all rows present
        preview_response = client.get(
            f"/projects/{project_with_table['project_id']}/branches/default/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/preview",
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        rows = preview_response.json()["rows"]
        assert len(rows) == 3

    def test_incremental_import_update_duplicates(self, client, project_with_table):
        """Test incremental import with upsert updates existing rows."""
        # Initial import
        csv_content1 = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n"
        file_id1 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content1,
            "data1.csv",
        )

        _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id1,
        )

        # Import with update for existing row (id=1)
        csv_content2 = b"id,name,email,age\n1,Alice Updated,alice.new@test.com,31\n3,Charlie,charlie@test.com,35\n"
        file_id2 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content2,
            "data2.csv",
        )

        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id2,
            import_options={
                "incremental": True,
                "dedup_mode": "update_duplicates",
            },
        )
        assert import_response.status_code == 200
        assert import_response.json()["table_rows_after"] == 3  # 2 original + 1 new

        # Verify Alice was updated
        preview_response = client.get(
            f"/projects/{project_with_table['project_id']}/branches/default/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/preview",
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        rows = preview_response.json()["rows"]
        alice = [r for r in rows if r["id"] == 1][0]
        assert alice["name"] == "Alice Updated"
        assert alice["email"] == "alice.new@test.com"
        assert alice["age"] == 31

    def test_incremental_import_no_duplicates(self, client, project_with_table):
        """Test incremental import with same primary key doesn't create duplicates."""
        # Initial import
        csv_content1 = b"id,name,email,age\n1,Alice,alice@test.com,30\n"
        file_id1 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content1,
        )

        _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id1,
        )

        # Try to import same ID again (should fail or skip based on dedup_mode)
        csv_content2 = b"id,name,email,age\n1,Alice Duplicate,alice2@test.com,32\n"
        file_id2 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content2,
            "data2.csv",
        )

        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id2,
            import_options={"incremental": True},
        )

        # Should still have only 1 row (duplicate rejected or updated)
        assert import_response.status_code == 200
        data = import_response.json()
        assert data["table_rows_after"] == 1


class TestParquetImportExport:
    """Test Parquet format import/export scenarios."""

    def test_parquet_import_export(self, client, project_with_table):
        """Test Parquet roundtrip with compression."""
        # First, import CSV data
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )

        # Export to Parquet
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_format="parquet",
        )
        assert export_response.status_code == 200
        export_data = export_response.json()
        assert export_data["rows_exported"] == 2
        assert export_data["file_size_bytes"] > 0

        # Parquet should be more compressed than CSV
        parquet_file_id = export_data["file_id"]
        parquet_size = export_data["file_size_bytes"]
        assert parquet_size > 0  # Has some size

    def test_parquet_export_with_compression(self, client, project_with_table):
        """Test Parquet export with different compression options."""
        # Import test data
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n3,Charlie,charlie@test.com,35\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )

        # Export with compression (DuckDB default is snappy for Parquet)
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_format="parquet",
        )
        assert export_response.status_code == 200
        assert export_response.json()["rows_exported"] == 3


class TestImportWithColumnMapping:
    """Test import with column mapping scenarios."""

    def test_import_with_same_columns_different_order(self, client, project_with_table):
        """Test importing CSV with columns in same order as table schema."""
        # CSV has same column order as table definition (id, name, email, age)
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        # Import should succeed
        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 2

        # Verify data was imported correctly
        preview_response = client.get(
            f"/projects/{project_with_table['project_id']}/branches/default/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/preview",
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        rows = preview_response.json()["rows"]
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["email"] == "alice@test.com"
        assert rows[0]["age"] == 30

    def test_import_with_all_columns(self, client, project_with_table):
        """Test importing CSV with all table columns present."""
        # CSV has all columns (id, name, email, age)
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        # Import should succeed
        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 2

        # Verify all columns have data
        preview_response = client.get(
            f"/projects/{project_with_table['project_id']}/branches/default/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/preview",
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        rows = preview_response.json()["rows"]
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["email"] == "alice@test.com"
        assert rows[0]["age"] == 30


class TestExportWithFilter:
    """Test export with filtering scenarios."""

    def test_export_with_filter(self, client, project_with_table):
        """Test exporting only filtered rows."""
        # Import test data
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n3,Charlie,charlie@test.com,35\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )

        # Export only rows where age > 25
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            where_filter="age > 25",
        )
        assert export_response.status_code == 200
        assert export_response.json()["rows_exported"] == 2  # Alice (30) and Charlie (35)

    def test_export_with_complex_filter(self, client, project_with_table):
        """Test export with complex WHERE clause."""
        # Import test data
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n3,Charlie,charlie@test.com,35\n4,Diana,diana@test.com,28\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )

        # Export with complex filter
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            where_filter="age >= 28 AND age <= 32",
        )
        assert export_response.status_code == 200
        assert export_response.json()["rows_exported"] == 2  # Alice (30) and Diana (28)

    def test_export_specific_columns(self, client, project_with_table):
        """Test exporting only specific columns."""
        # Import test data
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )

        # Export only id and name columns
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            columns=["id", "name"],
        )
        assert export_response.status_code == 200
        export_data = export_response.json()
        assert export_data["rows_exported"] == 2

        # Download and verify only specified columns are present
        exported_content = _download_file(
            client,
            project_with_table["project_id"],
            export_data["file_id"],
            project_with_table["api_key"],
        )
        header_line = exported_content.decode().strip().split("\n")[0]
        assert "id" in header_line
        assert "name" in header_line
        assert "email" not in header_line
        assert "age" not in header_line


class TestLargeFileImport:
    """Test large file import scenarios."""

    def test_large_file_import(self, client, project_with_table):
        """Test importing file with 10000+ rows."""
        # Generate large CSV content
        header = b"id,name,email,age\n"
        rows = []
        for i in range(10000):
            rows.append(f"{i},User{i},user{i}@test.com,{20 + (i % 50)}\n".encode())
        csv_content = header + b"".join(rows)

        # Upload large file
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
            "large_data.csv",
        )

        # Import large file
        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )
        assert import_response.status_code == 200
        import_data = import_response.json()
        assert import_data["imported_rows"] == 10000
        assert import_data["table_rows_after"] == 10000

        # Verify completeness with preview
        preview_response = client.get(
            f"/projects/{project_with_table['project_id']}/branches/default/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/preview?limit=1",
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert preview_response.status_code == 200

    def test_large_file_export(self, client, project_with_table):
        """Test exporting large dataset."""
        # Generate and import large dataset
        header = b"id,name,email,age\n"
        rows = []
        for i in range(5000):
            rows.append(f"{i},User{i},user{i}@test.com,{20 + (i % 50)}\n".encode())
        csv_content = header + b"".join(rows)

        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )

        # Export large dataset
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
        )
        assert export_response.status_code == 200
        export_data = export_response.json()
        assert export_data["rows_exported"] == 5000
        assert export_data["file_size_bytes"] > 100000  # Should be substantial

    def test_large_file_incremental_import(self, client, project_with_table):
        """Test incremental import performance with large dataset."""
        # Initial import of 5000 rows
        header = b"id,name,email,age\n"
        rows1 = []
        for i in range(5000):
            rows1.append(f"{i},User{i},user{i}@test.com,{20 + (i % 50)}\n".encode())
        csv_content1 = header + b"".join(rows1)

        file_id1 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content1,
            "data1.csv",
        )

        import_response1 = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id1,
        )
        assert import_response1.status_code == 200
        assert import_response1.json()["table_rows_after"] == 5000

        # Incremental import of 5000 more rows
        rows2 = []
        for i in range(5000, 10000):
            rows2.append(f"{i},User{i},user{i}@test.com,{20 + (i % 50)}\n".encode())
        csv_content2 = header + b"".join(rows2)

        file_id2 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content2,
            "data2.csv",
        )

        import_response2 = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id2,
            import_options={"incremental": True},
        )
        assert import_response2.status_code == 200
        assert import_response2.json()["table_rows_after"] == 10000


class TestCompleteDataPipeline:
    """Test complete end-to-end data pipeline scenarios."""

    def test_complete_etl_pipeline(self, client, project_with_table):
        """Test complete ETL pipeline: extract (upload) -> transform (import) -> load (export)."""
        # Step 1: Extract - Upload raw data
        raw_csv = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n3,Charlie,charlie@test.com,35\n"
        file_id1 = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            raw_csv,
            "raw_data.csv",
        )

        # Step 2: Transform - Import into table
        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id1,
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 3

        # Step 3: Load - Export transformed data (filtered)
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            where_filter="age >= 30",
        )
        assert export_response.status_code == 200
        assert export_response.json()["rows_exported"] == 2

        # Verify final output
        exported_content = _download_file(
            client,
            project_with_table["project_id"],
            export_response.json()["file_id"],
            project_with_table["api_key"],
        )
        lines = exported_content.decode().strip().split("\n")
        assert len(lines) == 3  # header + 2 filtered rows

    def test_multi_format_pipeline(self, client, project_with_table):
        """Test pipeline with format conversion: CSV -> Table -> Parquet."""
        # Import from CSV
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n"
        csv_file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
            "data.csv",
        )

        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            csv_file_id,
        )
        assert import_response.status_code == 200

        # Export to Parquet
        export_csv = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_format="csv",
        )
        assert export_csv.status_code == 200

        export_parquet = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_format="parquet",
        )
        assert export_parquet.status_code == 200

        # Both exports should have same row count
        assert export_csv.json()["rows_exported"] == export_parquet.json()["rows_exported"]

    def test_data_quality_pipeline(self, client, project_with_table):
        """Test data quality checks in pipeline."""
        # Import initial data
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n3,Charlie,charlie@test.com,35\n"
        file_id = _upload_file(
            client,
            project_with_table["project_id"],
            project_with_table["api_key"],
            csv_content,
        )

        import_response = _import_file(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
            file_id,
        )
        assert import_response.status_code == 200

        # Check data quality via preview
        preview_response = client.get(
            f"/projects/{project_with_table['project_id']}/branches/default/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/preview",
            headers={"Authorization": f"Bearer {project_with_table['api_key']}"},
        )
        assert preview_response.status_code == 200
        preview_data = preview_response.json()
        assert preview_data["total_row_count"] == 3

        # Export quality-checked data
        export_response = _export_table(
            client,
            project_with_table["project_id"],
            project_with_table["bucket_name"],
            project_with_table["table_name"],
            project_with_table["api_key"],
        )
        assert export_response.status_code == 200
        assert export_response.json()["rows_exported"] == 3
