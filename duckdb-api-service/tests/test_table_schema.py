"""Tests for table schema operations: columns, primary keys, rows, profiling."""

import pytest
from fastapi.testclient import TestClient


class TestAddColumn:
    """Tests for POST /projects/{id}/buckets/{bucket}/tables/{table}/columns endpoint."""

    def test_add_column_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful column addition."""
        # Setup
        client.post("/projects", json={"id": "schema_test_1"}, headers=admin_headers)
        client.post("/projects/schema_test_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/schema_test_1/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        # Add column
        response = client.post(
            "/projects/schema_test_1/buckets/test_bucket/tables/test_table/columns",
            json={"name": "email", "type": "VARCHAR", "nullable": True},
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert len(data["columns"]) == 3
        column_names = [c["name"] for c in data["columns"]]
        assert "email" in column_names

    def test_add_column_with_default(self, client: TestClient, initialized_backend, admin_headers):
        """Test adding column with default value."""
        client.post("/projects", json={"id": "schema_test_2"}, headers=admin_headers)
        client.post("/projects/schema_test_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/schema_test_2/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        response = client.post(
            "/projects/schema_test_2/buckets/test_bucket/tables/test_table/columns",
            json={"name": "active", "type": "BOOLEAN", "default": "true"},
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert len(data["columns"]) == 2

    def test_add_column_not_null_unsupported(self, client: TestClient, initialized_backend, admin_headers):
        """Test that adding NOT NULL column fails (DuckDB limitation)."""
        # DuckDB doesn't support ALTER TABLE ADD COLUMN with NOT NULL constraint.
        # This is a known limitation - columns must be added nullable first,
        # then altered to NOT NULL if needed.
        client.post("/projects", json={"id": "schema_test_3"}, headers=admin_headers)
        client.post("/projects/schema_test_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/schema_test_3/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Attempting to add NOT NULL column should fail due to DuckDB limitation
        response = client.post(
            "/projects/schema_test_3/buckets/test_bucket/tables/test_table/columns",
            json={"name": "status", "type": "VARCHAR", "nullable": False, "default": "'active'"},
            headers=admin_headers,
        )

        # DuckDB returns error for NOT NULL constraint in ADD COLUMN
        assert response.status_code == 500
        assert "not yet supported" in response.json()["detail"]["message"].lower()

    def test_add_column_already_exists(self, client: TestClient, initialized_backend, admin_headers):
        """Test adding column that already exists returns 409."""
        client.post("/projects", json={"id": "schema_test_4"}, headers=admin_headers)
        client.post("/projects/schema_test_4/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/schema_test_4/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        response = client.post(
            "/projects/schema_test_4/buckets/test_bucket/tables/test_table/columns",
            json={"name": "id", "type": "VARCHAR"},
            headers=admin_headers,
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "column_exists"

    def test_add_column_table_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test adding column to non-existent table returns 404."""
        client.post("/projects", json={"id": "schema_test_5"}, headers=admin_headers)
        client.post("/projects/schema_test_5/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.post(
            "/projects/schema_test_5/buckets/test_bucket/tables/nonexistent/columns",
            json={"name": "col", "type": "VARCHAR"},
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"


class TestDropColumn:
    """Tests for DELETE /projects/{id}/buckets/{bucket}/tables/{table}/columns/{name} endpoint."""

    def test_drop_column_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful column removal."""
        client.post("/projects", json={"id": "drop_col_1"}, headers=admin_headers)
        client.post("/projects/drop_col_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/drop_col_1/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        response = client.delete(
            "/projects/drop_col_1/buckets/test_bucket/tables/test_table/columns/email",
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["columns"]) == 2
        column_names = [c["name"] for c in data["columns"]]
        assert "email" not in column_names

    def test_drop_column_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test dropping non-existent column returns 404."""
        client.post("/projects", json={"id": "drop_col_2"}, headers=admin_headers)
        client.post("/projects/drop_col_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/drop_col_2/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        response = client.delete(
            "/projects/drop_col_2/buckets/test_bucket/tables/test_table/columns/nonexistent",
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "column_not_found"

    def test_drop_last_column_fails(self, client: TestClient, initialized_backend, admin_headers):
        """Test dropping the last column fails."""
        client.post("/projects", json={"id": "drop_col_3"}, headers=admin_headers)
        client.post("/projects/drop_col_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/drop_col_3/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        response = client.delete(
            "/projects/drop_col_3/buckets/test_bucket/tables/test_table/columns/id",
            headers=admin_headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "cannot_drop_last_column"

    def test_drop_primary_key_column_fails(self, client: TestClient, initialized_backend, admin_headers):
        """Test dropping a primary key column fails."""
        client.post("/projects", json={"id": "drop_col_4"}, headers=admin_headers)
        client.post("/projects/drop_col_4/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/drop_col_4/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                ],
                "primary_key": ["id"],
            },
            headers=admin_headers,
        )

        response = client.delete(
            "/projects/drop_col_4/buckets/test_bucket/tables/test_table/columns/id",
            headers=admin_headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "column_in_primary_key"


class TestAlterColumn:
    """Tests for PUT /projects/{id}/buckets/{bucket}/tables/{table}/columns/{name} endpoint."""

    def test_alter_column_rename(self, client: TestClient, initialized_backend, admin_headers):
        """Test renaming a column."""
        client.post("/projects", json={"id": "alter_col_1"}, headers=admin_headers)
        client.post("/projects/alter_col_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/alter_col_1/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "old_name", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        response = client.put(
            "/projects/alter_col_1/buckets/test_bucket/tables/test_table/columns/old_name",
            json={"new_name": "new_name"},
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        column_names = [c["name"] for c in data["columns"]]
        assert "new_name" in column_names
        assert "old_name" not in column_names

    def test_alter_column_change_type(self, client: TestClient, initialized_backend, admin_headers):
        """Test changing column type."""
        client.post("/projects", json={"id": "alter_col_2"}, headers=admin_headers)
        client.post("/projects/alter_col_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/alter_col_2/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "amount", "type": "INTEGER"},
                ],
            },
            headers=admin_headers,
        )

        response = client.put(
            "/projects/alter_col_2/buckets/test_bucket/tables/test_table/columns/amount",
            json={"new_type": "DOUBLE"},
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        amount_col = next(c for c in data["columns"] if c["name"] == "amount")
        assert "DOUBLE" in amount_col["type"].upper()

    def test_alter_column_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test altering non-existent column returns 404."""
        client.post("/projects", json={"id": "alter_col_3"}, headers=admin_headers)
        client.post("/projects/alter_col_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/alter_col_3/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        response = client.put(
            "/projects/alter_col_3/buckets/test_bucket/tables/test_table/columns/nonexistent",
            json={"new_name": "renamed"},
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "column_not_found"

    def test_alter_column_no_changes(self, client: TestClient, initialized_backend, admin_headers):
        """Test altering without any changes returns 400."""
        client.post("/projects", json={"id": "alter_col_4"}, headers=admin_headers)
        client.post("/projects/alter_col_4/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/alter_col_4/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        response = client.put(
            "/projects/alter_col_4/buckets/test_bucket/tables/test_table/columns/id",
            json={},
            headers=admin_headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "no_changes_specified"

    def test_alter_column_rename_conflict(self, client: TestClient, initialized_backend, admin_headers):
        """Test renaming to existing column name returns 409."""
        client.post("/projects", json={"id": "alter_col_5"}, headers=admin_headers)
        client.post("/projects/alter_col_5/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/alter_col_5/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        response = client.put(
            "/projects/alter_col_5/buckets/test_bucket/tables/test_table/columns/name",
            json={"new_name": "id"},
            headers=admin_headers,
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "column_exists"


class TestAddPrimaryKey:
    """Tests for POST /projects/{id}/buckets/{bucket}/tables/{table}/primary-key endpoint."""

    def test_add_primary_key_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful primary key addition."""
        client.post("/projects", json={"id": "pk_test_1"}, headers=admin_headers)
        client.post("/projects/pk_test_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/pk_test_1/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        response = client.post(
            "/projects/pk_test_1/buckets/test_bucket/tables/test_table/primary-key",
            json={"columns": ["id"]},
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert "id" in data["primary_key"]

    def test_add_composite_primary_key(self, client: TestClient, initialized_backend, admin_headers):
        """Test adding composite primary key."""
        client.post("/projects", json={"id": "pk_test_2"}, headers=admin_headers)
        client.post("/projects/pk_test_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/pk_test_2/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "order_id", "type": "INTEGER", "nullable": False},
                    {"name": "item_id", "type": "INTEGER", "nullable": False},
                    {"name": "quantity", "type": "INTEGER"},
                ],
            },
            headers=admin_headers,
        )

        response = client.post(
            "/projects/pk_test_2/buckets/test_bucket/tables/test_table/primary-key",
            json={"columns": ["order_id", "item_id"]},
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert "order_id" in data["primary_key"]
        assert "item_id" in data["primary_key"]

    def test_add_primary_key_already_exists(self, client: TestClient, initialized_backend, admin_headers):
        """Test adding primary key when one already exists returns 409."""
        client.post("/projects", json={"id": "pk_test_3"}, headers=admin_headers)
        client.post("/projects/pk_test_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/pk_test_3/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                ],
                "primary_key": ["id"],
            },
            headers=admin_headers,
        )

        response = client.post(
            "/projects/pk_test_3/buckets/test_bucket/tables/test_table/primary-key",
            json={"columns": ["name"]},
            headers=admin_headers,
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "primary_key_exists"

    def test_add_primary_key_column_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test adding primary key with non-existent column returns 400."""
        client.post("/projects", json={"id": "pk_test_4"}, headers=admin_headers)
        client.post("/projects/pk_test_4/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/pk_test_4/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        response = client.post(
            "/projects/pk_test_4/buckets/test_bucket/tables/test_table/primary-key",
            json={"columns": ["nonexistent"]},
            headers=admin_headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "column_not_found"


class TestDropPrimaryKey:
    """Tests for DELETE /projects/{id}/buckets/{bucket}/tables/{table}/primary-key endpoint."""

    def test_drop_primary_key_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful primary key removal."""
        client.post("/projects", json={"id": "drop_pk_1"}, headers=admin_headers)
        client.post("/projects/drop_pk_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/drop_pk_1/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR"},
                ],
                "primary_key": ["id"],
            },
            headers=admin_headers,
        )

        response = client.delete(
            "/projects/drop_pk_1/buckets/test_bucket/tables/test_table/primary-key",
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["primary_key"] == []

    def test_drop_primary_key_not_exists(self, client: TestClient, initialized_backend, admin_headers):
        """Test dropping primary key when none exists returns 400."""
        client.post("/projects", json={"id": "drop_pk_2"}, headers=admin_headers)
        client.post("/projects/drop_pk_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/drop_pk_2/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        response = client.delete(
            "/projects/drop_pk_2/buckets/test_bucket/tables/test_table/primary-key",
            headers=admin_headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "no_primary_key"


class TestDeleteRows:
    """Tests for DELETE /projects/{id}/buckets/{bucket}/tables/{table}/rows endpoint."""

    def _insert_test_data(self, client, project_id, bucket, table, admin_headers):
        """Helper to insert test data directly via DuckDB."""
        from src.database import project_db_manager
        import duckdb

        table_path = project_db_manager.get_table_path(project_id, bucket, table)
        conn = duckdb.connect(str(table_path))
        try:
            conn.execute("""
                INSERT INTO main.data (id, status, amount) VALUES
                (1, 'active', 100),
                (2, 'active', 200),
                (3, 'deleted', 50),
                (4, 'deleted', 75),
                (5, 'active', 300)
            """)
            conn.commit()
        finally:
            conn.close()

    def test_delete_rows_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful row deletion."""
        client.post("/projects", json={"id": "del_rows_1"}, headers=admin_headers)
        client.post("/projects/del_rows_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/del_rows_1/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "status", "type": "VARCHAR"},
                    {"name": "amount", "type": "INTEGER"},
                ],
            },
            headers=admin_headers,
        )

        self._insert_test_data(client, "del_rows_1", "test_bucket", "test_table", admin_headers)

        response = client.request(
            "DELETE",
            "/projects/del_rows_1/buckets/test_bucket/tables/test_table/rows",
            json={"where_clause": "status = 'deleted'"},
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["deleted_rows"] == 2
        assert data["table_rows_after"] == 3

    def test_delete_rows_no_match(self, client: TestClient, initialized_backend, admin_headers):
        """Test deletion when no rows match."""
        client.post("/projects", json={"id": "del_rows_2"}, headers=admin_headers)
        client.post("/projects/del_rows_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/del_rows_2/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "status", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        response = client.request(
            "DELETE",
            "/projects/del_rows_2/buckets/test_bucket/tables/test_table/rows",
            json={"where_clause": "status = 'nonexistent'"},
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["deleted_rows"] == 0

    def test_delete_rows_sql_injection_prevention(self, client: TestClient, initialized_backend, admin_headers):
        """Test SQL injection prevention in WHERE clause."""
        client.post("/projects", json={"id": "del_rows_3"}, headers=admin_headers)
        client.post("/projects/del_rows_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/del_rows_3/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Try SQL injection with semicolon
        response = client.request(
            "DELETE",
            "/projects/del_rows_3/buckets/test_bucket/tables/test_table/rows",
            json={"where_clause": "1=1; DROP TABLE data;"},
            headers=admin_headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_where_clause"

    def test_delete_rows_comment_injection_prevention(self, client: TestClient, initialized_backend, admin_headers):
        """Test SQL comment injection prevention."""
        client.post("/projects", json={"id": "del_rows_4"}, headers=admin_headers)
        client.post("/projects/del_rows_4/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/del_rows_4/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Try SQL injection with comment
        response = client.request(
            "DELETE",
            "/projects/del_rows_4/buckets/test_bucket/tables/test_table/rows",
            json={"where_clause": "1=1 -- comment"},
            headers=admin_headers,
        )

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_where_clause"


class TestProfileTable:
    """Tests for POST /projects/{id}/buckets/{bucket}/tables/{table}/profile endpoint."""

    def _insert_profile_data(self, client, project_id, bucket, table):
        """Helper to insert test data for profiling."""
        from src.database import project_db_manager
        import duckdb

        table_path = project_db_manager.get_table_path(project_id, bucket, table)
        conn = duckdb.connect(str(table_path))
        try:
            conn.execute("""
                INSERT INTO main.data (id, name, amount, active) VALUES
                (1, 'Alice', 100.5, true),
                (2, 'Bob', 200.0, true),
                (3, 'Charlie', 150.25, false),
                (4, NULL, 75.0, true),
                (5, 'Eve', NULL, false)
            """)
            conn.commit()
        finally:
            conn.close()

    def test_profile_table_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful table profiling."""
        client.post("/projects", json={"id": "profile_1"}, headers=admin_headers)
        client.post("/projects/profile_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/profile_1/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"},
                    {"name": "active", "type": "BOOLEAN"},
                ],
            },
            headers=admin_headers,
        )

        self._insert_profile_data(client, "profile_1", "test_bucket", "test_table")

        response = client.post(
            "/projects/profile_1/buckets/test_bucket/tables/test_table/profile",
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "test_table"
        assert data["bucket_name"] == "test_bucket"
        assert data["row_count"] == 5
        assert data["column_count"] == 4
        assert len(data["statistics"]) == 4

        # Check statistics structure
        for stat in data["statistics"]:
            assert "column_name" in stat
            assert "column_type" in stat
            assert "min" in stat
            assert "max" in stat
            assert "approx_unique" in stat

    def test_profile_empty_table(self, client: TestClient, initialized_backend, admin_headers):
        """Test profiling an empty table."""
        client.post("/projects", json={"id": "profile_2"}, headers=admin_headers)
        client.post("/projects/profile_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/profile_2/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=admin_headers,
        )

        response = client.post(
            "/projects/profile_2/buckets/test_bucket/tables/test_table/profile",
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["row_count"] == 0
        assert data["column_count"] == 2

    def test_profile_table_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test profiling non-existent table returns 404."""
        client.post("/projects", json={"id": "profile_3"}, headers=admin_headers)
        client.post("/projects/profile_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        response = client.post(
            "/projects/profile_3/buckets/test_bucket/tables/nonexistent/profile",
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"


class TestSchemaOperationsAuth:
    """Tests for authentication on schema operations."""

    def test_add_column_requires_auth(self, client: TestClient, initialized_backend):
        """Test that add column requires authentication."""
        response = client.post(
            "/projects/any/buckets/any/tables/any/columns",
            json={"name": "col", "type": "VARCHAR"},
        )
        assert response.status_code == 401

    def test_drop_column_requires_auth(self, client: TestClient, initialized_backend):
        """Test that drop column requires authentication."""
        response = client.delete("/projects/any/buckets/any/tables/any/columns/col")
        assert response.status_code == 401

    def test_alter_column_requires_auth(self, client: TestClient, initialized_backend):
        """Test that alter column requires authentication."""
        response = client.put(
            "/projects/any/buckets/any/tables/any/columns/col",
            json={"new_name": "renamed"},
        )
        assert response.status_code == 401

    def test_primary_key_requires_auth(self, client: TestClient, initialized_backend):
        """Test that primary key operations require authentication."""
        response = client.post(
            "/projects/any/buckets/any/tables/any/primary-key",
            json={"columns": ["id"]},
        )
        assert response.status_code == 401

        response = client.delete("/projects/any/buckets/any/tables/any/primary-key")
        assert response.status_code == 401

    def test_delete_rows_requires_auth(self, client: TestClient, initialized_backend):
        """Test that delete rows requires authentication."""
        response = client.request(
            "DELETE",
            "/projects/any/buckets/any/tables/any/rows",
            json={"where_clause": "1=1"},
        )
        assert response.status_code == 401

    def test_profile_requires_auth(self, client: TestClient, initialized_backend):
        """Test that profile requires authentication."""
        response = client.post("/projects/any/buckets/any/tables/any/profile")
        assert response.status_code == 401
