"""Tests for table CRUD endpoints."""

import pytest
from fastapi.testclient import TestClient


class TestCreateTable:
    """Tests for POST /projects/{project_id}/buckets/{bucket_name}/tables endpoint."""

    def test_create_table_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful table creation."""
        # Setup project and bucket
        client.post("/projects", json={"id": "table_test_1", "name": "Test Project"}, headers=admin_headers)
        client.post("/projects/table_test_1/buckets", json={"name": "in_c_sales"}, headers=admin_headers)

        # Create table
        response = client.post(
            "/projects/table_test_1/buckets/in_c_sales/tables",
            json={
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "customer_name", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"},
                    {"name": "created_at", "type": "TIMESTAMP"},
                ],
            },
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "orders"
        assert data["bucket"] == "in_c_sales"
        assert len(data["columns"]) == 4
        assert data["row_count"] == 0
        assert data["primary_key"] == []

    def test_create_table_with_primary_key(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test creating table with primary key."""
        client.post("/projects", json={"id": "table_test_2"}, headers=admin_headers)
        client.post("/projects/table_test_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.post(
            "/projects/table_test_2/buckets/test_bucket/tables",
            json={
                "name": "users",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "email", "type": "VARCHAR", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                ],
                "primary_key": ["id"],
            },
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "users"
        # Primary key is enforced in DuckDB (unlike BigQuery)
        assert "id" in data["primary_key"]

    def test_create_table_with_composite_primary_key(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test creating table with composite primary key."""
        client.post("/projects", json={"id": "table_test_3"}, headers=admin_headers)
        client.post("/projects/table_test_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.post(
            "/projects/table_test_3/buckets/test_bucket/tables",
            json={
                "name": "order_items",
                "columns": [
                    {"name": "order_id", "type": "INTEGER", "nullable": False},
                    {"name": "item_id", "type": "INTEGER", "nullable": False},
                    {"name": "quantity", "type": "INTEGER"},
                    {"name": "price", "type": "DOUBLE"},
                ],
                "primary_key": ["order_id", "item_id"],
            },
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        # Composite PK
        assert "order_id" in data["primary_key"]
        assert "item_id" in data["primary_key"]

    def test_create_table_with_default_values(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test creating table with default column values."""
        client.post("/projects", json={"id": "table_test_4"}, headers=admin_headers)
        client.post("/projects/table_test_4/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.post(
            "/projects/table_test_4/buckets/test_bucket/tables",
            json={
                "name": "products",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "active", "type": "BOOLEAN", "default": "true"},
                    {"name": "stock", "type": "INTEGER", "default": "0"},
                ],
            },
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "products"
        assert len(data["columns"]) == 4

    def test_create_table_updates_project_stats(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that creating a table updates project statistics."""
        client.post("/projects", json={"id": "table_test_5"}, headers=admin_headers)
        client.post("/projects/table_test_5/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        # Check initial stats
        stats_before = client.get("/projects/table_test_5/stats", headers=admin_headers).json()
        assert stats_before["table_count"] == 0

        # Create table
        client.post(
            "/projects/table_test_5/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Check updated stats
        stats_after = client.get("/projects/table_test_5/stats", headers=admin_headers).json()
        assert stats_after["table_count"] == 1

    def test_create_table_updates_bucket_table_count(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that creating a table updates bucket table count."""
        client.post("/projects", json={"id": "table_test_6"}, headers=admin_headers)
        client.post("/projects/table_test_6/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        # Check initial bucket
        bucket_before = client.get(
            "/projects/table_test_6/buckets/test_bucket", headers=admin_headers
        ).json()
        assert bucket_before["table_count"] == 0

        # Create table
        client.post(
            "/projects/table_test_6/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Check updated bucket
        bucket_after = client.get("/projects/table_test_6/buckets/test_bucket", headers=admin_headers).json()
        assert bucket_after["table_count"] == 1

    def test_create_table_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test creating table in non-existent project returns 404."""
        response = client.post(
            "/projects/nonexistent/buckets/any/tables",
            json={
                "name": "test",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_create_table_bucket_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test creating table in non-existent bucket returns 404."""
        client.post("/projects", json={"id": "table_test_7"}, headers=admin_headers)

        response = client.post(
            "/projects/table_test_7/buckets/nonexistent/tables",
            json={
                "name": "test",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_create_table_conflict(self, client: TestClient, initialized_backend, admin_headers):
        """Test creating duplicate table returns 409."""
        client.post("/projects", json={"id": "table_test_8"}, headers=admin_headers)
        client.post("/projects/table_test_8/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/table_test_8/buckets/test_bucket/tables",
            json={
                "name": "duplicate",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Try to create again
        response = client.post(
            "/projects/table_test_8/buckets/test_bucket/tables",
            json={
                "name": "duplicate",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "table_exists"

    def test_create_table_invalid_primary_key(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test creating table with invalid primary key column returns 400."""
        client.post("/projects", json={"id": "table_test_9"}, headers=admin_headers)
        client.post("/projects/table_test_9/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.post(
            "/projects/table_test_9/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
                "primary_key": ["nonexistent_column"],
            },
            headers=admin_headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_primary_key"


class TestGetTable:
    """Tests for GET /projects/{project_id}/buckets/{bucket_name}/tables/{table_name} endpoint."""

    def test_get_table_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test getting an existing table."""
        client.post("/projects", json={"id": "get_table_1"}, headers=admin_headers)
        client.post("/projects/get_table_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/get_table_1/buckets/test_bucket/tables",
            json={
                "name": "my_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                ],
                "primary_key": ["id"],
            },
            headers=admin_headers,
        )

        response = client.get(
            "/projects/get_table_1/buckets/test_bucket/tables/my_table", headers=admin_headers
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "my_table"
        assert data["bucket"] == "test_bucket"
        assert len(data["columns"]) == 2
        assert data["row_count"] == 0
        assert "id" in data["primary_key"]

    def test_get_table_column_details(self, client: TestClient, initialized_backend, admin_headers):
        """Test that column details are correct."""
        client.post("/projects", json={"id": "get_table_2"}, headers=admin_headers)
        client.post("/projects/get_table_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/get_table_2/buckets/test_bucket/tables",
            json={
                "name": "detailed_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR", "nullable": True},
                ],
            },
            headers=admin_headers,
        )

        response = client.get(
            "/projects/get_table_2/buckets/test_bucket/tables/detailed_table", headers=admin_headers
        )

        assert response.status_code == 200
        columns = response.json()["columns"]

        # Find columns by name
        id_col = next(c for c in columns if c["name"] == "id")
        name_col = next(c for c in columns if c["name"] == "name")

        assert id_col["nullable"] is False
        assert name_col["nullable"] is True
        assert id_col["ordinal_position"] == 1
        assert name_col["ordinal_position"] == 2

    def test_get_table_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test getting non-existent table returns 404."""
        client.post("/projects", json={"id": "get_table_3"}, headers=admin_headers)
        client.post("/projects/get_table_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.get(
            "/projects/get_table_3/buckets/test_bucket/tables/nonexistent", headers=admin_headers
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"

    def test_get_table_bucket_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test getting table from non-existent bucket returns 404."""
        client.post("/projects", json={"id": "get_table_4"}, headers=admin_headers)

        response = client.get(
            "/projects/get_table_4/buckets/nonexistent/tables/any", headers=admin_headers
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_get_table_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test getting table from non-existent project returns 404."""
        response = client.get("/projects/nonexistent/buckets/any/tables/any", headers=admin_headers)

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"


class TestListTables:
    """Tests for GET /projects/{project_id}/buckets/{bucket_name}/tables endpoint."""

    def test_list_tables_empty(self, client: TestClient, initialized_backend, admin_headers):
        """Test listing when no tables exist."""
        client.post("/projects", json={"id": "list_table_1"}, headers=admin_headers)
        client.post("/projects/list_table_1/buckets", json={"name": "empty_bucket"}, headers=admin_headers)

        response = client.get("/projects/list_table_1/buckets/empty_bucket/tables", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["tables"] == []
        assert data["total"] == 0

    def test_list_tables_multiple(self, client: TestClient, initialized_backend, admin_headers):
        """Test listing multiple tables."""
        client.post("/projects", json={"id": "list_table_2"}, headers=admin_headers)
        client.post("/projects/list_table_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        # Create multiple tables
        for name in ["table_a", "table_b", "table_c"]:
            client.post(
                "/projects/list_table_2/buckets/test_bucket/tables",
                json={
                    "name": name,
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
                headers=admin_headers,
            )

        response = client.get("/projects/list_table_2/buckets/test_bucket/tables", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert len(data["tables"]) == 3
        assert data["total"] == 3

        # Check alphabetical order
        names = [t["name"] for t in data["tables"]]
        assert names == sorted(names)

    def test_list_tables_bucket_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test listing tables from non-existent bucket returns 404."""
        client.post("/projects", json={"id": "list_table_3"}, headers=admin_headers)

        response = client.get("/projects/list_table_3/buckets/nonexistent/tables", headers=admin_headers)

        assert response.status_code == 404


class TestDeleteTable:
    """Tests for DELETE /projects/{project_id}/buckets/{bucket_name}/tables/{table_name} endpoint."""

    def test_delete_table_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test deleting a table."""
        client.post("/projects", json={"id": "delete_table_1"}, headers=admin_headers)
        client.post("/projects/delete_table_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/delete_table_1/buckets/test_bucket/tables",
            json={
                "name": "to_delete",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Verify exists
        assert (
            client.get(
                "/projects/delete_table_1/buckets/test_bucket/tables/to_delete", headers=admin_headers
            ).status_code
            == 200
        )

        # Delete
        response = client.delete(
            "/projects/delete_table_1/buckets/test_bucket/tables/to_delete", headers=admin_headers
        )
        assert response.status_code == 204

        # Verify deleted
        assert (
            client.get(
                "/projects/delete_table_1/buckets/test_bucket/tables/to_delete", headers=admin_headers
            ).status_code
            == 404
        )

    def test_delete_table_updates_stats(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that deleting a table updates project statistics."""
        client.post("/projects", json={"id": "delete_table_2"}, headers=admin_headers)
        client.post("/projects/delete_table_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/delete_table_2/buckets/test_bucket/tables",
            json={
                "name": "temp_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Check stats before
        stats = client.get("/projects/delete_table_2/stats", headers=admin_headers).json()
        assert stats["table_count"] == 1

        # Delete
        client.delete("/projects/delete_table_2/buckets/test_bucket/tables/temp_table", headers=admin_headers)

        # Check stats after
        stats = client.get("/projects/delete_table_2/stats", headers=admin_headers).json()
        assert stats["table_count"] == 0

    def test_delete_table_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test deleting non-existent table returns 404."""
        client.post("/projects", json={"id": "delete_table_3"}, headers=admin_headers)
        client.post("/projects/delete_table_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.delete(
            "/projects/delete_table_3/buckets/test_bucket/tables/nonexistent", headers=admin_headers
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"


class TestPreviewTable:
    """Tests for GET /projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/preview endpoint."""

    def test_preview_empty_table(self, client: TestClient, initialized_backend, admin_headers):
        """Test previewing an empty table."""
        client.post("/projects", json={"id": "preview_table_1"}, headers=admin_headers)
        client.post("/projects/preview_table_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/preview_table_1/buckets/test_bucket/tables",
            json={
                "name": "empty_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        response = client.get(
            "/projects/preview_table_1/buckets/test_bucket/tables/empty_table/preview", headers=admin_headers
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["columns"]) == 2
        assert data["rows"] == []
        assert data["total_row_count"] == 0
        assert data["preview_row_count"] == 0

    def test_preview_table_with_data(self, client: TestClient, initialized_backend, admin_headers):
        """Test previewing a table with data."""
        from src.database import project_db_manager

        # Setup
        client.post("/projects", json={"id": "preview_table_2"}, headers=admin_headers)
        client.post("/projects/preview_table_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/preview_table_2/buckets/test_bucket/tables",
            json={
                "name": "data_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        # ADR-009: Insert data using table_connection() and main.data
        with project_db_manager.table_connection(
            "preview_table_2", "test_bucket", "data_table"
        ) as conn:
            conn.execute(
                "INSERT INTO main.data VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
            )
            conn.commit()

        response = client.get(
            "/projects/preview_table_2/buckets/test_bucket/tables/data_table/preview", headers=admin_headers
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_row_count"] == 3
        assert data["preview_row_count"] == 3
        assert len(data["rows"]) == 3

        # Check row data
        names = [row["name"] for row in data["rows"]]
        assert "Alice" in names
        assert "Bob" in names

    def test_preview_table_with_limit(self, client: TestClient, initialized_backend, admin_headers):
        """Test preview with custom limit."""
        from src.database import project_db_manager

        client.post("/projects", json={"id": "preview_table_3"}, headers=admin_headers)
        client.post("/projects/preview_table_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/preview_table_3/buckets/test_bucket/tables",
            json={
                "name": "big_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # ADR-009: Insert 100 rows using table_connection() and main.data
        with project_db_manager.table_connection(
            "preview_table_3", "test_bucket", "big_table"
        ) as conn:
            for i in range(100):
                conn.execute(f"INSERT INTO main.data VALUES ({i})")
            conn.commit()

        # Preview with limit=10
        response = client.get(
            "/projects/preview_table_3/buckets/test_bucket/tables/big_table/preview?limit=10", headers=admin_headers
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_row_count"] == 100
        assert data["preview_row_count"] == 10
        assert len(data["rows"]) == 10

    def test_preview_table_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test previewing non-existent table returns 404."""
        client.post("/projects", json={"id": "preview_table_4"}, headers=admin_headers)
        client.post("/projects/preview_table_4/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.get(
            "/projects/preview_table_4/buckets/test_bucket/tables/nonexistent/preview", headers=admin_headers
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"

    def test_preview_limit_validation(self, client: TestClient, initialized_backend, admin_headers):
        """Test preview limit validation."""
        client.post("/projects", json={"id": "preview_table_5"}, headers=admin_headers)
        client.post("/projects/preview_table_5/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/preview_table_5/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Test limit too high
        response = client.get(
            "/projects/preview_table_5/buckets/test_bucket/tables/test_table/preview?limit=50000", headers=admin_headers
        )
        assert response.status_code == 422  # Validation error

        # Test limit too low
        response = client.get(
            "/projects/preview_table_5/buckets/test_bucket/tables/test_table/preview?limit=0", headers=admin_headers
        )
        assert response.status_code == 422


class TestTableOperationsLog:
    """Tests for table operations audit logging."""

    def test_create_table_logs_operation(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that creating a table logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "log_test_1"}, headers=admin_headers)
        client.post("/projects/log_test_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/log_test_1/buckets/test_bucket/tables",
            json={
                "name": "logged_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status, resource_type FROM operations_log WHERE project_id = ? AND resource_type = 'table'",
            ["log_test_1"],
        )

        assert len(logs) >= 1
        assert any(log[0] == "create_table" and log[1] == "success" for log in logs)

    def test_delete_table_logs_operation(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that deleting a table logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "log_test_2"}, headers=admin_headers)
        client.post("/projects/log_test_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/log_test_2/buckets/test_bucket/tables",
            json={
                "name": "to_delete",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )
        client.delete("/projects/log_test_2/buckets/test_bucket/tables/to_delete", headers=admin_headers)

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status FROM operations_log WHERE project_id = ? AND resource_type = 'table' ORDER BY timestamp",
            ["log_test_2"],
        )

        operations = [log[0] for log in logs]
        assert "create_table" in operations
        assert "delete_table" in operations


class TestTableDataTypes:
    """Tests for various DuckDB data types."""

    def test_all_common_data_types(self, client: TestClient, initialized_backend, admin_headers):
        """Test creating table with all common data types."""
        client.post("/projects", json={"id": "dtype_test_1"}, headers=admin_headers)
        client.post("/projects/dtype_test_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.post(
            "/projects/dtype_test_1/buckets/test_bucket/tables",
            json={
                "name": "all_types",
                "columns": [
                    {"name": "col_integer", "type": "INTEGER"},
                    {"name": "col_bigint", "type": "BIGINT"},
                    {"name": "col_double", "type": "DOUBLE"},
                    {"name": "col_varchar", "type": "VARCHAR"},
                    {"name": "col_boolean", "type": "BOOLEAN"},
                    {"name": "col_date", "type": "DATE"},
                    {"name": "col_timestamp", "type": "TIMESTAMP"},
                    {"name": "col_json", "type": "JSON"},
                ],
            },
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert len(data["columns"]) == 8

        # Verify types are preserved
        type_map = {col["name"]: col["type"] for col in data["columns"]}
        assert type_map["col_integer"] == "INTEGER"
        assert type_map["col_bigint"] == "BIGINT"
        assert type_map["col_varchar"] == "VARCHAR"
        assert type_map["col_boolean"] == "BOOLEAN"


class TestTableFilesystemADR009:
    """Tests for ADR-009 filesystem structure verification.

    ADR-009 defines: Table = individual .duckdb file within bucket directory.
    Path: /data/duckdb/project_{id}/{bucket_name}/{table_name}.duckdb
    """

    def test_table_creates_duckdb_file(self, client: TestClient, initialized_backend, admin_headers):
        """Test that creating a table creates a .duckdb file in bucket directory."""
        client.post("/projects", json={"id": "fs_table_1"}, headers=admin_headers)
        client.post("/projects/fs_table_1/buckets", json={"name": "in_c_sales"}, headers=admin_headers)

        # Create table
        response = client.post(
            "/projects/fs_table_1/buckets/in_c_sales/tables",
            json={
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "amount", "type": "DOUBLE"},
                ],
            },
            headers=admin_headers,
        )
        assert response.status_code == 201

        # ADR-009: Verify table .duckdb file exists
        project_dir = initialized_backend["duckdb_dir"] / "project_fs_table_1"
        bucket_dir = project_dir / "in_c_sales"
        table_file = bucket_dir / "orders.duckdb"
        assert table_file.is_file(), f"Table file should exist: {table_file}"
        assert table_file.suffix == ".duckdb"

    def test_table_delete_removes_file(self, client: TestClient, initialized_backend, admin_headers):
        """Test that deleting a table removes its .duckdb file."""
        client.post("/projects", json={"id": "fs_table_2"}, headers=admin_headers)
        client.post("/projects/fs_table_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/fs_table_2/buckets/test_bucket/tables",
            json={
                "name": "to_delete",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Verify table file exists before delete
        project_dir = initialized_backend["duckdb_dir"] / "project_fs_table_2"
        bucket_dir = project_dir / "test_bucket"
        table_file = bucket_dir / "to_delete.duckdb"
        assert table_file.is_file(), "Table file should exist before delete"

        # Delete table
        response = client.delete(
            "/projects/fs_table_2/buckets/test_bucket/tables/to_delete", headers=admin_headers
        )
        assert response.status_code == 204

        # ADR-009: Verify table file is removed
        assert not table_file.exists(), f"Table file should be deleted: {table_file}"
        # But bucket directory should still exist
        assert bucket_dir.is_dir(), "Bucket directory should remain after table delete"

    def test_multiple_tables_create_separate_files(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that multiple tables create separate .duckdb files."""
        client.post("/projects", json={"id": "fs_table_3"}, headers=admin_headers)
        client.post("/projects/fs_table_3/buckets", json={"name": "in_c_data"}, headers=admin_headers)

        # Create multiple tables
        for table_name in ["orders", "customers", "products"]:
            response = client.post(
                "/projects/fs_table_3/buckets/in_c_data/tables",
                json={
                    "name": table_name,
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
                headers=admin_headers,
            )
            assert response.status_code == 201

        # ADR-009: Verify each table has its own .duckdb file
        bucket_dir = (
            initialized_backend["duckdb_dir"] / "project_fs_table_3" / "in_c_data"
        )
        for table_name in ["orders", "customers", "products"]:
            table_file = bucket_dir / f"{table_name}.duckdb"
            assert table_file.is_file(), f"Table file should exist: {table_file}"

        # Verify total file count (3 tables)
        duckdb_files = list(bucket_dir.glob("*.duckdb"))
        assert len(duckdb_files) == 3

    def test_table_file_contains_data_table(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that table .duckdb file contains main.data table with correct schema."""
        import duckdb

        client.post("/projects", json={"id": "fs_table_4"}, headers=admin_headers)
        client.post("/projects/fs_table_4/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/fs_table_4/buckets/test_bucket/tables",
            json={
                "name": "schema_test",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        # ADR-009: Open the file directly and verify structure
        table_file = (
            initialized_backend["duckdb_dir"]
            / "project_fs_table_4"
            / "test_bucket"
            / "schema_test.duckdb"
        )
        conn = duckdb.connect(str(table_file))
        try:
            # Verify main.data table exists
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            table_names = [t[0] for t in tables]
            assert "data" in table_names, "Table should contain main.data"

            # Verify columns
            columns = conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = 'data' ORDER BY ordinal_position"
            ).fetchall()
            assert len(columns) == 2
            assert columns[0][0] == "id"
            assert columns[1][0] == "name"
        finally:
            conn.close()

    def test_bucket_with_tables_delete_cascade(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that deleting bucket removes all table files."""
        client.post("/projects", json={"id": "fs_table_5"}, headers=admin_headers)
        client.post("/projects/fs_table_5/buckets", json={"name": "cascade_bucket"}, headers=admin_headers)

        # Create multiple tables
        for table_name in ["table_a", "table_b"]:
            client.post(
                "/projects/fs_table_5/buckets/cascade_bucket/tables",
                json={
                    "name": table_name,
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
                headers=admin_headers,
            )

        # Verify files exist
        bucket_dir = (
            initialized_backend["duckdb_dir"] / "project_fs_table_5" / "cascade_bucket"
        )
        assert (bucket_dir / "table_a.duckdb").is_file()
        assert (bucket_dir / "table_b.duckdb").is_file()

        # Delete bucket (cascade)
        response = client.delete("/projects/fs_table_5/buckets/cascade_bucket", headers=admin_headers)
        assert response.status_code == 204

        # ADR-009: Verify entire bucket directory is removed
        assert not bucket_dir.exists(), "Bucket directory with all tables should be deleted"
