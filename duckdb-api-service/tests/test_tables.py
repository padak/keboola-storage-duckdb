"""Tests for table CRUD endpoints."""

import pytest
from fastapi.testclient import TestClient


class TestCreateTable:
    """Tests for POST /projects/{project_id}/buckets/{bucket_name}/tables endpoint."""

    def test_create_table_success(self, client: TestClient, initialized_backend):
        """Test successful table creation."""
        # Setup project and bucket
        client.post("/projects", json={"id": "table_test_1", "name": "Test Project"})
        client.post("/projects/table_test_1/buckets", json={"name": "in_c_sales"})

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
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "orders"
        assert data["bucket"] == "in_c_sales"
        assert len(data["columns"]) == 4
        assert data["row_count"] == 0
        assert data["primary_key"] == []

    def test_create_table_with_primary_key(
        self, client: TestClient, initialized_backend
    ):
        """Test creating table with primary key."""
        client.post("/projects", json={"id": "table_test_2"})
        client.post("/projects/table_test_2/buckets", json={"name": "test_bucket"})

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
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "users"
        # Primary key is enforced in DuckDB (unlike BigQuery)
        assert "id" in data["primary_key"]

    def test_create_table_with_composite_primary_key(
        self, client: TestClient, initialized_backend
    ):
        """Test creating table with composite primary key."""
        client.post("/projects", json={"id": "table_test_3"})
        client.post("/projects/table_test_3/buckets", json={"name": "test_bucket"})

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
        )

        assert response.status_code == 201
        data = response.json()
        # Composite PK
        assert "order_id" in data["primary_key"]
        assert "item_id" in data["primary_key"]

    def test_create_table_with_default_values(
        self, client: TestClient, initialized_backend
    ):
        """Test creating table with default column values."""
        client.post("/projects", json={"id": "table_test_4"})
        client.post("/projects/table_test_4/buckets", json={"name": "test_bucket"})

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
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "products"
        assert len(data["columns"]) == 4

    def test_create_table_updates_project_stats(
        self, client: TestClient, initialized_backend
    ):
        """Test that creating a table updates project statistics."""
        client.post("/projects", json={"id": "table_test_5"})
        client.post("/projects/table_test_5/buckets", json={"name": "test_bucket"})

        # Check initial stats
        stats_before = client.get("/projects/table_test_5/stats").json()
        assert stats_before["table_count"] == 0

        # Create table
        client.post(
            "/projects/table_test_5/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        # Check updated stats
        stats_after = client.get("/projects/table_test_5/stats").json()
        assert stats_after["table_count"] == 1

    def test_create_table_updates_bucket_table_count(
        self, client: TestClient, initialized_backend
    ):
        """Test that creating a table updates bucket table count."""
        client.post("/projects", json={"id": "table_test_6"})
        client.post("/projects/table_test_6/buckets", json={"name": "test_bucket"})

        # Check initial bucket
        bucket_before = client.get(
            "/projects/table_test_6/buckets/test_bucket"
        ).json()
        assert bucket_before["table_count"] == 0

        # Create table
        client.post(
            "/projects/table_test_6/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        # Check updated bucket
        bucket_after = client.get("/projects/table_test_6/buckets/test_bucket").json()
        assert bucket_after["table_count"] == 1

    def test_create_table_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test creating table in non-existent project returns 404."""
        response = client.post(
            "/projects/nonexistent/buckets/any/tables",
            json={
                "name": "test",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_create_table_bucket_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test creating table in non-existent bucket returns 404."""
        client.post("/projects", json={"id": "table_test_7"})

        response = client.post(
            "/projects/table_test_7/buckets/nonexistent/tables",
            json={
                "name": "test",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_create_table_conflict(self, client: TestClient, initialized_backend):
        """Test creating duplicate table returns 409."""
        client.post("/projects", json={"id": "table_test_8"})
        client.post("/projects/table_test_8/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/table_test_8/buckets/test_bucket/tables",
            json={
                "name": "duplicate",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        # Try to create again
        response = client.post(
            "/projects/table_test_8/buckets/test_bucket/tables",
            json={
                "name": "duplicate",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "table_exists"

    def test_create_table_invalid_primary_key(
        self, client: TestClient, initialized_backend
    ):
        """Test creating table with invalid primary key column returns 400."""
        client.post("/projects", json={"id": "table_test_9"})
        client.post("/projects/table_test_9/buckets", json={"name": "test_bucket"})

        response = client.post(
            "/projects/table_test_9/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
                "primary_key": ["nonexistent_column"],
            },
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_primary_key"


class TestGetTable:
    """Tests for GET /projects/{project_id}/buckets/{bucket_name}/tables/{table_name} endpoint."""

    def test_get_table_success(self, client: TestClient, initialized_backend):
        """Test getting an existing table."""
        client.post("/projects", json={"id": "get_table_1"})
        client.post("/projects/get_table_1/buckets", json={"name": "test_bucket"})
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
        )

        response = client.get(
            "/projects/get_table_1/buckets/test_bucket/tables/my_table"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "my_table"
        assert data["bucket"] == "test_bucket"
        assert len(data["columns"]) == 2
        assert data["row_count"] == 0
        assert "id" in data["primary_key"]

    def test_get_table_column_details(self, client: TestClient, initialized_backend):
        """Test that column details are correct."""
        client.post("/projects", json={"id": "get_table_2"})
        client.post("/projects/get_table_2/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/get_table_2/buckets/test_bucket/tables",
            json={
                "name": "detailed_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR", "nullable": True},
                ],
            },
        )

        response = client.get(
            "/projects/get_table_2/buckets/test_bucket/tables/detailed_table"
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

    def test_get_table_not_found(self, client: TestClient, initialized_backend):
        """Test getting non-existent table returns 404."""
        client.post("/projects", json={"id": "get_table_3"})
        client.post("/projects/get_table_3/buckets", json={"name": "test_bucket"})

        response = client.get(
            "/projects/get_table_3/buckets/test_bucket/tables/nonexistent"
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"

    def test_get_table_bucket_not_found(self, client: TestClient, initialized_backend):
        """Test getting table from non-existent bucket returns 404."""
        client.post("/projects", json={"id": "get_table_4"})

        response = client.get(
            "/projects/get_table_4/buckets/nonexistent/tables/any"
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_get_table_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test getting table from non-existent project returns 404."""
        response = client.get("/projects/nonexistent/buckets/any/tables/any")

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"


class TestListTables:
    """Tests for GET /projects/{project_id}/buckets/{bucket_name}/tables endpoint."""

    def test_list_tables_empty(self, client: TestClient, initialized_backend):
        """Test listing when no tables exist."""
        client.post("/projects", json={"id": "list_table_1"})
        client.post("/projects/list_table_1/buckets", json={"name": "empty_bucket"})

        response = client.get("/projects/list_table_1/buckets/empty_bucket/tables")

        assert response.status_code == 200
        data = response.json()
        assert data["tables"] == []
        assert data["total"] == 0

    def test_list_tables_multiple(self, client: TestClient, initialized_backend):
        """Test listing multiple tables."""
        client.post("/projects", json={"id": "list_table_2"})
        client.post("/projects/list_table_2/buckets", json={"name": "test_bucket"})

        # Create multiple tables
        for name in ["table_a", "table_b", "table_c"]:
            client.post(
                "/projects/list_table_2/buckets/test_bucket/tables",
                json={
                    "name": name,
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
            )

        response = client.get("/projects/list_table_2/buckets/test_bucket/tables")

        assert response.status_code == 200
        data = response.json()
        assert len(data["tables"]) == 3
        assert data["total"] == 3

        # Check alphabetical order
        names = [t["name"] for t in data["tables"]]
        assert names == sorted(names)

    def test_list_tables_bucket_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test listing tables from non-existent bucket returns 404."""
        client.post("/projects", json={"id": "list_table_3"})

        response = client.get("/projects/list_table_3/buckets/nonexistent/tables")

        assert response.status_code == 404


class TestDeleteTable:
    """Tests for DELETE /projects/{project_id}/buckets/{bucket_name}/tables/{table_name} endpoint."""

    def test_delete_table_success(self, client: TestClient, initialized_backend):
        """Test deleting a table."""
        client.post("/projects", json={"id": "delete_table_1"})
        client.post("/projects/delete_table_1/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/delete_table_1/buckets/test_bucket/tables",
            json={
                "name": "to_delete",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        # Verify exists
        assert (
            client.get(
                "/projects/delete_table_1/buckets/test_bucket/tables/to_delete"
            ).status_code
            == 200
        )

        # Delete
        response = client.delete(
            "/projects/delete_table_1/buckets/test_bucket/tables/to_delete"
        )
        assert response.status_code == 204

        # Verify deleted
        assert (
            client.get(
                "/projects/delete_table_1/buckets/test_bucket/tables/to_delete"
            ).status_code
            == 404
        )

    def test_delete_table_updates_stats(
        self, client: TestClient, initialized_backend
    ):
        """Test that deleting a table updates project statistics."""
        client.post("/projects", json={"id": "delete_table_2"})
        client.post("/projects/delete_table_2/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/delete_table_2/buckets/test_bucket/tables",
            json={
                "name": "temp_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        # Check stats before
        stats = client.get("/projects/delete_table_2/stats").json()
        assert stats["table_count"] == 1

        # Delete
        client.delete("/projects/delete_table_2/buckets/test_bucket/tables/temp_table")

        # Check stats after
        stats = client.get("/projects/delete_table_2/stats").json()
        assert stats["table_count"] == 0

    def test_delete_table_not_found(self, client: TestClient, initialized_backend):
        """Test deleting non-existent table returns 404."""
        client.post("/projects", json={"id": "delete_table_3"})
        client.post("/projects/delete_table_3/buckets", json={"name": "test_bucket"})

        response = client.delete(
            "/projects/delete_table_3/buckets/test_bucket/tables/nonexistent"
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"


class TestPreviewTable:
    """Tests for GET /projects/{project_id}/buckets/{bucket_name}/tables/{table_name}/preview endpoint."""

    def test_preview_empty_table(self, client: TestClient, initialized_backend):
        """Test previewing an empty table."""
        client.post("/projects", json={"id": "preview_table_1"})
        client.post("/projects/preview_table_1/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/preview_table_1/buckets/test_bucket/tables",
            json={
                "name": "empty_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
        )

        response = client.get(
            "/projects/preview_table_1/buckets/test_bucket/tables/empty_table/preview"
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["columns"]) == 2
        assert data["rows"] == []
        assert data["total_row_count"] == 0
        assert data["preview_row_count"] == 0

    def test_preview_table_with_data(self, client: TestClient, initialized_backend):
        """Test previewing a table with data."""
        from src.database import project_db_manager

        # Setup
        client.post("/projects", json={"id": "preview_table_2"})
        client.post("/projects/preview_table_2/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/preview_table_2/buckets/test_bucket/tables",
            json={
                "name": "data_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
        )

        # Insert some data directly
        with project_db_manager.connection("preview_table_2") as conn:
            conn.execute(
                "INSERT INTO test_bucket.data_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
            )
            conn.commit()

        response = client.get(
            "/projects/preview_table_2/buckets/test_bucket/tables/data_table/preview"
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

    def test_preview_table_with_limit(self, client: TestClient, initialized_backend):
        """Test preview with custom limit."""
        from src.database import project_db_manager

        client.post("/projects", json={"id": "preview_table_3"})
        client.post("/projects/preview_table_3/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/preview_table_3/buckets/test_bucket/tables",
            json={
                "name": "big_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        # Insert 100 rows
        with project_db_manager.connection("preview_table_3") as conn:
            for i in range(100):
                conn.execute(f"INSERT INTO test_bucket.big_table VALUES ({i})")
            conn.commit()

        # Preview with limit=10
        response = client.get(
            "/projects/preview_table_3/buckets/test_bucket/tables/big_table/preview?limit=10"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_row_count"] == 100
        assert data["preview_row_count"] == 10
        assert len(data["rows"]) == 10

    def test_preview_table_not_found(self, client: TestClient, initialized_backend):
        """Test previewing non-existent table returns 404."""
        client.post("/projects", json={"id": "preview_table_4"})
        client.post("/projects/preview_table_4/buckets", json={"name": "test_bucket"})

        response = client.get(
            "/projects/preview_table_4/buckets/test_bucket/tables/nonexistent/preview"
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"

    def test_preview_limit_validation(self, client: TestClient, initialized_backend):
        """Test preview limit validation."""
        client.post("/projects", json={"id": "preview_table_5"})
        client.post("/projects/preview_table_5/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/preview_table_5/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        # Test limit too high
        response = client.get(
            "/projects/preview_table_5/buckets/test_bucket/tables/test_table/preview?limit=50000"
        )
        assert response.status_code == 422  # Validation error

        # Test limit too low
        response = client.get(
            "/projects/preview_table_5/buckets/test_bucket/tables/test_table/preview?limit=0"
        )
        assert response.status_code == 422


class TestTableOperationsLog:
    """Tests for table operations audit logging."""

    def test_create_table_logs_operation(
        self, client: TestClient, initialized_backend
    ):
        """Test that creating a table logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "log_test_1"})
        client.post("/projects/log_test_1/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/log_test_1/buckets/test_bucket/tables",
            json={
                "name": "logged_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status, resource_type FROM operations_log WHERE project_id = ? AND resource_type = 'table'",
            ["log_test_1"],
        )

        assert len(logs) >= 1
        assert any(log[0] == "create_table" and log[1] == "success" for log in logs)

    def test_delete_table_logs_operation(
        self, client: TestClient, initialized_backend
    ):
        """Test that deleting a table logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "log_test_2"})
        client.post("/projects/log_test_2/buckets", json={"name": "test_bucket"})
        client.post(
            "/projects/log_test_2/buckets/test_bucket/tables",
            json={
                "name": "to_delete",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )
        client.delete("/projects/log_test_2/buckets/test_bucket/tables/to_delete")

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

    def test_all_common_data_types(self, client: TestClient, initialized_backend):
        """Test creating table with all common data types."""
        client.post("/projects", json={"id": "dtype_test_1"})
        client.post("/projects/dtype_test_1/buckets", json={"name": "test_bucket"})

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
