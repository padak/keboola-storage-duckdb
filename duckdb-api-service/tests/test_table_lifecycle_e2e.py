"""End-to-End Tests for Table Lifecycle.

This module contains comprehensive E2E tests covering complete table lifecycle scenarios:
- CRUD operations with data manipulation
- Data type handling and preservation
- Primary key operations and behavior
- Table profiling workflows
- Concurrent operations with lock verification
- Schema evolution and backward compatibility
"""

import io
import json
import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi.testclient import TestClient
from pathlib import Path

from src.main import app
from src.config import settings
from src.database import project_db_manager


@pytest.fixture
def e2e_client(monkeypatch, tmp_path):
    """Create test client with temporary storage for E2E tests."""
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


def _create_project(client: TestClient, project_id: str = "e2e_project") -> str:
    """Helper to create a project and return API key."""
    response = client.post(
        "/projects",
        json={"id": project_id, "name": f"E2E Test {project_id}"},
        headers={"Authorization": "Bearer test-admin-key"},
    )
    assert response.status_code == 201
    return response.json()["api_key"]


def _create_bucket(client: TestClient, project_id: str, bucket_name: str, api_key: str):
    """Helper to create a bucket."""
    response = client.post(
        f"/projects/{project_id}/branches/default/buckets",
        json={"name": bucket_name},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert response.status_code == 201


def _upload_csv_file(client: TestClient, project_id: str, api_key: str, csv_content: str, filename: str = "data.csv") -> str:
    """Helper to upload CSV file and return file_id."""
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
        files={"file": (filename, io.BytesIO(csv_content.encode()), "text/csv")},
        headers={"Authorization": f"Bearer {api_key}"},
    )

    # Register
    register_response = client.post(
        f"/projects/{project_id}/files",
        json={"upload_key": upload_key},
        headers={"Authorization": f"Bearer {api_key}"},
    )
    return register_response.json()["id"]


class TestCompleteTableCRUDLifecycle:
    """Test complete table CRUD lifecycle with all operations."""

    def test_complete_table_crud_lifecycle(self, e2e_client):
        """Complete table lifecycle: create, import, query, modify schema, delete rows, drop."""
        project_id = "lifecycle_crud"
        api_key = _create_project(e2e_client, project_id)
        _create_bucket(e2e_client, project_id, "in_c_sales", api_key)

        # 1. Create table with columns and primary key
        create_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables",
            json={
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "customer_name", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"},
                    {"name": "status", "type": "VARCHAR"},
                ],
                "primary_key": ["id"],
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert create_response.status_code == 201
        assert create_response.json()["name"] == "orders"
        assert "id" in create_response.json()["primary_key"]

        # 2. Import data
        csv_content = "id,customer_name,amount,status\n1,Alice,100.50,active\n2,Bob,200.00,active\n3,Charlie,150.25,cancelled\n"
        file_id = _upload_csv_file(e2e_client, project_id, api_key, csv_content)

        import_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables/orders/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 3
        assert import_response.json()["table_rows_after"] == 3

        # 3. Query via preview
        preview_response = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables/orders/preview",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert preview_response.status_code == 200
        preview_data = preview_response.json()
        assert preview_data["total_row_count"] == 3
        assert len(preview_data["rows"]) == 3
        # Verify row data (order not guaranteed)
        customer_names = {row["customer_name"] for row in preview_data["rows"]}
        assert "Alice" in customer_names
        assert "Bob" in customer_names
        assert "Charlie" in customer_names

        # 4. Update column - Add new column
        add_col_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables/orders/columns",
            json={"name": "notes", "type": "VARCHAR", "nullable": True},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert add_col_response.status_code == 201
        assert len(add_col_response.json()["columns"]) == 5

        # 5. Alter column type
        alter_response = e2e_client.put(
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables/orders/columns/amount",
            json={"new_type": "DECIMAL(10,2)"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert alter_response.status_code == 200

        # 6. Delete rows with WHERE
        delete_response = e2e_client.request(
            "DELETE",
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables/orders/rows",
            json={"where_clause": "status = 'cancelled'"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert delete_response.status_code == 200
        assert delete_response.json()["deleted_rows"] == 1
        assert delete_response.json()["table_rows_after"] == 2

        # 7. Drop column
        drop_col_response = e2e_client.delete(
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables/orders/columns/notes",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert drop_col_response.status_code == 200
        assert len(drop_col_response.json()["columns"]) == 4

        # 8. Drop table
        drop_table_response = e2e_client.delete(
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables/orders",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert drop_table_response.status_code == 204

        # 9. Verify complete cleanup - table should not exist
        get_table_response = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/in_c_sales/tables/orders",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert get_table_response.status_code == 404

        # 10. Verify file cleanup - .duckdb file should be removed
        table_file = settings.duckdb_dir / f"project_{project_id}" / "in_c_sales" / "orders.duckdb"
        assert not table_file.exists(), "Table file should be deleted"

        # 11. Verify stats updated
        stats_response = e2e_client.get(
            f"/projects/{project_id}/stats",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert stats_response.json()["table_count"] == 0


class TestTableWithAllDataTypes:
    """Test table operations with all supported DuckDB data types."""

    def test_table_with_all_data_types(self, e2e_client):
        """Create table with all types, import, export, verify preservation."""
        project_id = "types_test"
        api_key = _create_project(e2e_client, project_id)
        _create_bucket(e2e_client, project_id, "test_bucket", api_key)

        # 1. Create table with all supported types
        create_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables",
            json={
                "name": "all_types",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "col_varchar", "type": "VARCHAR"},
                    {"name": "col_bigint", "type": "BIGINT"},
                    {"name": "col_decimal", "type": "DECIMAL(10,2)"},
                    {"name": "col_date", "type": "DATE"},
                    {"name": "col_timestamp", "type": "TIMESTAMP"},
                    {"name": "col_boolean", "type": "BOOLEAN"},
                    {"name": "col_json", "type": "JSON"},
                ],
                "primary_key": ["id"],
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert create_response.status_code == 201

        # 2. Import data with all types
        csv_content = """id,col_varchar,col_bigint,col_decimal,col_date,col_timestamp,col_boolean,col_json
1,Hello,9223372036854775807,12345.67,2024-01-15,2024-01-15 14:30:00,true,"{""key"": ""value""}"
2,World,123456789,999.99,2024-12-31,2024-12-31 23:59:59,false,"{""count"": 42}"
"""
        file_id = _upload_csv_file(e2e_client, project_id, api_key, csv_content)

        import_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/all_types/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 2

        # 3. Query and verify correct handling
        preview_response = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/all_types/preview",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert preview_response.status_code == 200
        rows = preview_response.json()["rows"]
        assert len(rows) == 2

        # Verify data types
        row1 = rows[0]
        assert row1["col_varchar"] == "Hello"
        assert row1["col_bigint"] == 9223372036854775807
        assert row1["col_boolean"] is True
        assert "2024-01-15" in str(row1["col_date"])
        assert "2024-01-15" in str(row1["col_timestamp"])

        # JSON is returned as string - verify it's valid JSON
        json_data = json.loads(row1["col_json"])
        assert json_data["key"] == "value"

        # 4. Export and verify type preservation
        export_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/all_types/export",
            json={"format": "csv"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert export_response.status_code == 200
        assert export_response.json()["rows_exported"] == 2

        # Download exported file and verify
        exported_file_id = export_response.json()["file_id"]
        download_response = e2e_client.get(
            f"/projects/{project_id}/files/{exported_file_id}/download",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert download_response.status_code == 200
        exported_content = download_response.content.decode()
        assert "Hello" in exported_content
        assert "9223372036854775807" in exported_content


class TestPrimaryKeyOperations:
    """Test primary key operations and behavior changes."""

    def test_primary_key_operations(self, e2e_client):
        """Test adding/dropping PK and behavior with duplicates."""
        project_id = "pk_ops"
        api_key = _create_project(e2e_client, project_id)
        _create_bucket(e2e_client, project_id, "test_bucket", api_key)

        # 1. Create table without PK
        e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables",
            json={
                "name": "users",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "email", "type": "VARCHAR"},
                ],
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )

        # 2. Import data without duplicates initially
        csv_content = "id,email\n1,alice@test.com\n2,bob@test.com\n"
        file_id = _upload_csv_file(e2e_client, project_id, api_key, csv_content)

        import_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/users/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 2

        # 3. Add primary key (should succeed now since no duplicates)
        add_pk_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/users/primary-key",
            json={"columns": ["id"]},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert add_pk_response.status_code == 201
        assert "id" in add_pk_response.json()["primary_key"]

        # 4. Import with PK + update_duplicates (should upsert)
        csv_dup = "id,email\n1,alice.updated@test.com\n"
        file_id_dup = _upload_csv_file(e2e_client, project_id, api_key, csv_dup, "dup.csv")

        # With incremental + update_duplicates, should upsert
        import_dup_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/users/import/file",
            json={
                "file_id": file_id_dup,
                "format": "csv",
                "import_options": {
                    "incremental": True,
                    "dedup_mode": "update_duplicates",
                },
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_dup_response.status_code == 200

        # Verify update happened
        preview = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/users/preview",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        rows = preview.json()["rows"]
        # Should still have 2 rows (upsert updated id=1)
        assert len(rows) == 2
        # Find id=1 row
        id1_row = next((r for r in rows if r["id"] == 1), None)
        assert id1_row is not None
        assert id1_row["email"] == "alice.updated@test.com"

        # 5. Drop primary key
        drop_pk_response = e2e_client.delete(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/users/primary-key",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert drop_pk_response.status_code == 200
        assert drop_pk_response.json()["primary_key"] == []

        # 6. Verify behavior changes - duplicates allowed again
        csv_dup2 = "id,email\n3,charlie@test.com\n3,charlie.duplicate@test.com\n"
        file_id_dup2 = _upload_csv_file(e2e_client, project_id, api_key, csv_dup2, "dup2.csv")

        import_dup2_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/users/import/file",
            json={
                "file_id": file_id_dup2,
                "format": "csv",
                "import_options": {"incremental": True},
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_dup2_response.status_code == 200
        # Should now have 4 rows (2 original + 2 duplicates)
        final_preview = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/users/preview",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert len(final_preview.json()["rows"]) == 4


class TestTableProfilingWorkflow:
    """Test table profiling workflow with varied data."""

    def test_table_profiling_workflow(self, e2e_client):
        """Create table, import varied data, profile, verify statistics."""
        project_id = "profile_test"
        api_key = _create_project(e2e_client, project_id)
        _create_bucket(e2e_client, project_id, "test_bucket", api_key)

        # 1. Create table
        e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables",
            json={
                "name": "analytics",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "category", "type": "VARCHAR"},
                    {"name": "score", "type": "DOUBLE"},
                    {"name": "active", "type": "BOOLEAN"},
                ],
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )

        # 2. Import varied data
        csv_content = """id,category,score,active
1,A,95.5,true
2,B,87.3,true
3,A,92.1,false
4,C,78.9,true
5,B,88.4,true
6,A,91.0,false
7,,85.2,true
8,C,,false
"""
        file_id = _upload_csv_file(e2e_client, project_id, api_key, csv_content)

        import_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/analytics/import/file",
            json={"file_id": file_id, "format": "csv"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_response.status_code == 200
        assert import_response.json()["imported_rows"] == 8

        # 3. Run profiling
        profile_response = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/analytics/profile",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert profile_response.status_code == 200

        # 4. Verify statistics
        profile_data = profile_response.json()
        assert profile_data["table_name"] == "analytics"
        assert profile_data["bucket_name"] == "test_bucket"
        assert profile_data["row_count"] == 8
        assert profile_data["column_count"] == 4

        statistics = {stat["column_name"]: stat for stat in profile_data["statistics"]}

        # Verify id column stats
        assert statistics["id"]["min"] == "1"
        assert statistics["id"]["max"] == "8"
        # approx_unique may include NULL in the count in some cases
        assert statistics["id"]["approx_unique"] >= 8

        # Verify category stats (has nulls) - DuckDB may include NULL in count
        assert statistics["category"]["approx_unique"] >= 3  # A, B, C (null may or may not be counted)

        # Verify score stats (has nulls)
        score_stats = statistics["score"]
        assert float(score_stats["min"]) > 78.0
        assert float(score_stats["max"]) < 96.0

        # Verify boolean stats
        assert statistics["active"]["approx_unique"] == 2  # true, false


class TestConcurrentTableOperations:
    """Test concurrent table operations with lock verification."""

    def test_concurrent_table_operations(self, e2e_client):
        """Run parallel imports with idempotency keys, verify no corruption."""
        project_id = "concurrent_test"
        api_key = _create_project(e2e_client, project_id)
        _create_bucket(e2e_client, project_id, "test_bucket", api_key)

        # Create table
        e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables",
            json={
                "name": "events",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "event_type", "type": "VARCHAR"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                ],
                "primary_key": ["id"],
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )

        # Prepare multiple CSV files
        csv_files = []
        for i in range(5):
            csv_content = f"id,event_type,timestamp\n{i*10+1},click,2024-01-01 10:00:00\n{i*10+2},view,2024-01-01 10:01:00\n"
            file_id = _upload_csv_file(e2e_client, project_id, api_key, csv_content, f"events_{i}.csv")
            csv_files.append((file_id, f"import-{i}"))

        # Run parallel imports with idempotency keys
        def import_file(file_id, idempotency_key):
            headers = {
                "Authorization": f"Bearer {api_key}",
                "X-Idempotency-Key": idempotency_key,
            }
            response = e2e_client.post(
                f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/events/import/file",
                json={
                    "file_id": file_id,
                    "format": "csv",
                    "import_options": {"incremental": True},
                },
                headers=headers,
            )
            return response.status_code, response.json()

        # Execute imports in parallel
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(import_file, file_id, idem_key) for file_id, idem_key in csv_files]
            for future in as_completed(futures):
                status, data = future.result()
                results.append((status, data))

        # Verify all succeeded
        for status, data in results:
            assert status == 200
            assert "imported_rows" in data

        # Verify no data corruption - all rows should be present
        preview = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/events/preview?limit=100",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert preview.status_code == 200
        assert preview.json()["total_row_count"] == 10  # 5 files * 2 rows each

        # Verify lock metrics were recorded
        metrics_response = e2e_client.get("/metrics")
        assert metrics_response.status_code == 200
        metrics_content = metrics_response.text
        assert "duckdb_table_locks_active" in metrics_content
        assert "duckdb_table_lock_acquisitions_total" in metrics_content


class TestTableSchemaEvolution:
    """Test table schema evolution and backward compatibility."""

    def test_table_schema_evolution(self, e2e_client):
        """Test schema changes over time with data preservation."""
        project_id = "evolution_test"
        api_key = _create_project(e2e_client, project_id)
        _create_bucket(e2e_client, project_id, "test_bucket", api_key)

        # 1. Create table v1 with initial schema
        e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables",
            json={
                "name": "products",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "price", "type": "INTEGER"},
                ],
                "primary_key": ["id"],
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )

        # 2. Import v1 data
        csv_v1 = "id,name,price\n1,Product A,100\n2,Product B,200\n"
        file_id_v1 = _upload_csv_file(e2e_client, project_id, api_key, csv_v1, "products_v1.csv")

        import_v1 = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/import/file",
            json={"file_id": file_id_v1, "format": "csv"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_v1.status_code == 200
        assert import_v1.json()["imported_rows"] == 2

        # 3. Add columns (v2 schema evolution)
        add_col1 = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/columns",
            json={"name": "category", "type": "VARCHAR", "nullable": True},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert add_col1.status_code == 201

        add_col2 = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/columns",
            json={"name": "in_stock", "type": "BOOLEAN", "nullable": True},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert add_col2.status_code == 201

        # 4. Import new data with new columns (v2 data)
        csv_v2 = "id,name,price,category,in_stock\n3,Product C,300,Electronics,true\n4,Product D,400,Books,false\n"
        file_id_v2 = _upload_csv_file(e2e_client, project_id, api_key, csv_v2, "products_v2.csv")

        import_v2 = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/import/file",
            json={
                "file_id": file_id_v2,
                "format": "csv",
                "import_options": {"incremental": True},
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_v2.status_code == 200
        assert import_v2.json()["imported_rows"] == 2

        # 5. Verify backward compatibility - old data should have NULLs for new columns
        preview = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/preview",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert preview.status_code == 200
        rows = preview.json()["rows"]
        assert len(rows) == 4

        # Old data (id 1,2) should have NULL for new columns
        old_rows = [r for r in rows if r["id"] in [1, 2]]
        for row in old_rows:
            assert row["category"] is None
            assert row["in_stock"] is None

        # New data (id 3,4) should have values
        new_rows = [r for r in rows if r["id"] in [3, 4]]
        for row in new_rows:
            assert row["category"] is not None
            assert row["in_stock"] is not None

        # 6. Alter column type (price: INTEGER -> DOUBLE)
        alter_response = e2e_client.put(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/columns/price",
            json={"new_type": "DOUBLE"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert alter_response.status_code == 200

        # 7. Verify data preservation after type change
        preview_after = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/preview",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert preview_after.status_code == 200
        rows_after = preview_after.json()["rows"]
        assert len(rows_after) == 4

        # Verify prices are still correct (now as DOUBLE)
        prices = {r["id"]: r["price"] for r in rows_after}
        assert prices[1] == 100.0 or prices[1] == 100
        assert prices[2] == 200.0 or prices[2] == 200

        # 8. Import data with decimal prices (utilizing new DOUBLE type)
        csv_v3 = "id,name,price,category,in_stock\n5,Product E,99.99,Electronics,true\n"
        file_id_v3 = _upload_csv_file(e2e_client, project_id, api_key, csv_v3, "products_v3.csv")

        import_v3 = e2e_client.post(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/import/file",
            json={
                "file_id": file_id_v3,
                "format": "csv",
                "import_options": {"incremental": True},
            },
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert import_v3.status_code == 200

        # Verify decimal price
        final_preview = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products/preview",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        final_rows = final_preview.json()["rows"]
        product_e = next(r for r in final_rows if r["id"] == 5)
        assert product_e["price"] == 99.99

        # 9. Verify table structure
        table_info = e2e_client.get(
            f"/projects/{project_id}/branches/default/buckets/test_bucket/tables/products",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert table_info.status_code == 200
        assert len(table_info.json()["columns"]) == 5
        assert table_info.json()["row_count"] == 5
