"""Tests for workspaces API (temporary SQL sandboxes)."""

import pytest
from datetime import datetime, timedelta, timezone


@pytest.fixture
def project_with_tables(client, initialized_backend, admin_headers):
    """Create a project with buckets and tables containing data."""
    # Create project
    response = client.post(
        "/projects",
        json={"id": "workspace_proj", "name": "Workspace Test Project"},
        headers=admin_headers,
    )
    assert response.status_code == 201
    project_key = response.json()["api_key"]
    project_headers = {"Authorization": f"Bearer {project_key}"}

    # Create bucket
    response = client.post(
        "/projects/workspace_proj/branches/default/buckets",
        json={"name": "in_c_sales"},
        headers=project_headers,
    )
    assert response.status_code == 201

    # Create first table with data
    response = client.post(
        "/projects/workspace_proj/branches/default/buckets/in_c_sales/tables",
        json={
            "name": "orders",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "customer", "type": "VARCHAR"},
                {"name": "amount", "type": "DECIMAL(10,2)"},
            ],
            "primary_key": ["id"],
        },
        headers=project_headers,
    )
    assert response.status_code == 201

    # Create second table
    response = client.post(
        "/projects/workspace_proj/branches/default/buckets/in_c_sales/tables",
        json={
            "name": "customers",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR"},
                {"name": "email", "type": "VARCHAR"},
            ],
            "primary_key": ["id"],
        },
        headers=project_headers,
    )
    assert response.status_code == 201

    # Import data into orders table
    response = client.post(
        "/projects/workspace_proj/files/prepare",
        json={"filename": "orders.csv", "content_type": "text/csv"},
        headers=project_headers,
    )
    assert response.status_code == 200
    upload_key = response.json()["upload_key"]

    csv_content = "id,customer,amount\n1,Alice,100.50\n2,Bob,250.00\n3,Charlie,75.25"
    response = client.post(
        f"/projects/workspace_proj/files/upload/{upload_key}",
        files={"file": ("orders.csv", csv_content, "text/csv")},
        headers=project_headers,
    )
    assert response.status_code == 200

    response = client.post(
        "/projects/workspace_proj/files",
        json={"upload_key": upload_key, "name": "orders.csv"},
        headers=project_headers,
    )
    assert response.status_code == 201
    file_id = response.json()["id"]

    response = client.post(
        "/projects/workspace_proj/branches/default/buckets/in_c_sales/tables/orders/import/file",
        json={
            "file_id": file_id,
            "format": "csv",
            "import_options": {"incremental": False},
        },
        headers=project_headers,
    )
    assert response.status_code == 200

    return {
        "project_id": "workspace_proj",
        "bucket_name": "in_c_sales",
        "project_key": project_key,
        "project_headers": project_headers,
    }


class TestWorkspaceCreate:
    """Test creating workspaces."""

    def test_create_workspace(self, client, project_with_tables):
        """Create a new workspace with all fields."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "analytics-workspace", "ttl_hours": 24, "size_limit_gb": 5},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        data = response.json()

        assert data["id"] is not None
        assert data["id"].startswith("ws_")
        assert data["project_id"] == project_with_tables["project_id"]
        assert data["name"] == "analytics-workspace"
        assert data["created_at"] is not None
        assert data["expires_at"] is not None
        assert data["status"] == "active"
        assert data["size_bytes"] >= 0
        assert data["size_limit_gb"] == 5

        # Connection info
        assert data["connection"]["host"] == "localhost"
        assert data["connection"]["port"] == 5432
        assert data["connection"]["database"] == f"workspace_{data['id']}"
        assert data["connection"]["username"] is not None
        assert data["connection"]["username"].startswith(f"ws_{data['id']}_")

        # Password should be returned on creation
        assert data["connection"]["password"] is not None
        assert len(data["connection"]["password"]) == 32

    def test_create_workspace_minimal(self, client, project_with_tables):
        """Create a workspace with only required fields."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "quick-workspace"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        data = response.json()

        assert data["name"] == "quick-workspace"
        assert data["expires_at"] is None  # No TTL specified
        assert data["status"] == "active"
        assert data["size_limit_gb"] == 10  # Default

    def test_create_workspace_with_ttl(self, client, project_with_tables):
        """Create a workspace with custom TTL."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "short-lived", "ttl_hours": 1},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        data = response.json()

        assert data["expires_at"] is not None

        # Verify expiration is approximately 1 hour from now
        expires_at = datetime.fromisoformat(data["expires_at"].replace("Z", "+00:00"))
        expected_expires = datetime.now(timezone.utc) + timedelta(hours=1)
        # Allow 10 second tolerance
        time_diff = abs((expires_at - expected_expires).total_seconds())
        assert time_diff < 10

    def test_create_workspace_returns_password_once(self, client, project_with_tables):
        """Verify password is returned only on creation."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "password-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        data = response.json()
        workspace_id = data["id"]

        # Password should be present in creation response
        assert data["connection"]["password"] is not None
        password = data["connection"]["password"]
        assert len(password) == 32

        # Get workspace - password should NOT be returned
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["connection"]["password"] is None

    def test_create_workspace_project_not_found(self, client, initialized_backend, admin_headers):
        """Creating workspace in non-existent project returns 404."""
        response = client.post(
            "/projects/nonexistent/workspaces",
            json={"name": "test-workspace"},
            headers=admin_headers,
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_create_workspace_requires_auth(self, client, project_with_tables):
        """Creating workspace without auth returns 401."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "unauth-workspace"},
        )
        assert response.status_code == 401


class TestWorkspaceList:
    """Test listing workspaces."""

    def test_list_workspaces_empty(self, client, project_with_tables):
        """List workspaces when none exist."""
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["workspaces"] == []
        assert data["count"] == 0

    def test_list_workspaces_multiple(self, client, project_with_tables):
        """List workspaces after creating some."""
        # Create workspaces
        for name in ["workspace-a", "workspace-b", "workspace-c"]:
            response = client.post(
                f"/projects/{project_with_tables['project_id']}/workspaces",
                json={"name": name},
                headers=project_with_tables["project_headers"],
            )
            assert response.status_code == 201

        # List workspaces
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["count"] == 3
        assert len(data["workspaces"]) == 3

        # Verify names
        names = [ws["name"] for ws in data["workspaces"]]
        assert "workspace-a" in names
        assert "workspace-b" in names
        assert "workspace-c" in names

        # Each workspace should have connection info without password
        for ws in data["workspaces"]:
            assert ws["connection"]["username"] is not None
            assert ws["connection"]["password"] is None

    def test_list_workspaces_pagination(self, client, project_with_tables):
        """Test workspace list pagination."""
        # Create 5 workspaces
        for i in range(5):
            response = client.post(
                f"/projects/{project_with_tables['project_id']}/workspaces",
                json={"name": f"workspace-{i}"},
                headers=project_with_tables["project_headers"],
            )
            assert response.status_code == 201

        # Get first page
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces?limit=2&offset=0",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2

        # Get second page
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces?limit=2&offset=2",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2

    def test_list_workspaces_project_not_found(self, client, initialized_backend, admin_headers):
        """Listing workspaces in non-existent project returns 404."""
        response = client.get(
            "/projects/nonexistent/workspaces",
            headers=admin_headers,
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"


class TestWorkspaceGet:
    """Test getting workspace details."""

    def test_get_workspace_success(self, client, project_with_tables):
        """Get workspace details."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "detail-test", "ttl_hours": 12},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Get workspace
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == workspace_id
        assert data["name"] == "detail-test"
        assert data["expires_at"] is not None
        assert data["status"] == "active"
        assert data["size_bytes"] >= 0

    def test_get_workspace_includes_objects(self, client, project_with_tables):
        """Get workspace includes attached_tables and workspace_objects."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "objects-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Get workspace detail
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        # Should have attached_tables and workspace_objects fields
        assert "attached_tables" in data
        assert "workspace_objects" in data
        assert isinstance(data["attached_tables"], list)
        assert isinstance(data["workspace_objects"], list)

    def test_get_workspace_not_found(self, client, project_with_tables):
        """Getting non-existent workspace returns 404."""
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/ws_notfound",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "workspace_not_found"

    def test_get_workspace_no_password(self, client, project_with_tables):
        """Password not returned on get."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "no-password-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Get workspace
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        # Password should be None
        assert data["connection"]["password"] is None


class TestWorkspaceDelete:
    """Test deleting workspaces."""

    def test_delete_workspace_success(self, client, project_with_tables):
        """Delete a workspace."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "to-delete"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Verify workspace exists
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200

        # Delete workspace
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 204

        # Verify workspace is gone
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404

    def test_delete_workspace_not_found(self, client, project_with_tables):
        """Deleting non-existent workspace returns 404."""
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/workspaces/ws_notfound",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404

    def test_delete_workspace_removes_files(self, client, project_with_tables):
        """Deleting workspace removes its database file."""
        from src.config import settings

        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "file-delete-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Check workspace file exists (project dir has project_ prefix)
        workspace_path = (
            settings.duckdb_dir
            / f"project_{project_with_tables['project_id']}"
            / "_workspaces"
            / f"{workspace_id}.duckdb"
        )
        assert workspace_path.exists()

        # Delete workspace
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 204

        # File should be gone
        assert not workspace_path.exists()


class TestWorkspaceClear:
    """Test clearing workspace data."""

    def test_clear_workspace_success(self, client, project_with_tables):
        """Clear workspace removes all user objects."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "clear-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Load a table
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/load",
            json={
                "tables": [
                    {"source": f"{project_with_tables['bucket_name']}.orders"}
                ]
            },
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200

        # Verify object was created
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        initial_objects = len(data["workspace_objects"])
        assert initial_objects > 0

        # Clear workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/clear",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 204

        # Verify workspace is cleared
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["workspace_objects"]) == 0

    def test_clear_workspace_not_found(self, client, project_with_tables):
        """Clearing non-existent workspace returns 404."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/ws_notfound/clear",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404


class TestWorkspaceDropObject:
    """Test dropping individual workspace objects."""

    def test_drop_object_success(self, client, project_with_tables):
        """Drop a workspace object."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "drop-object-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Load a table
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/load",
            json={
                "tables": [
                    {"source": f"{project_with_tables['bucket_name']}.orders"}
                ]
            },
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200

        # Get objects
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        objects = data["workspace_objects"]
        assert len(objects) > 0
        object_name = objects[0]["name"]

        # Drop the object
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/objects/{object_name}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 204

        # Verify object is gone
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        remaining_objects = [obj["name"] for obj in data["workspace_objects"]]
        assert object_name not in remaining_objects

    def test_drop_object_not_found(self, client, project_with_tables):
        """Dropping non-existent object returns 404."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "drop-404-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Try to drop non-existent object
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/objects/nonexistent",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "object_not_found"


class TestWorkspaceLoad:
    """Test loading tables into workspace."""

    def test_load_table_success(self, client, project_with_tables):
        """Load table into workspace."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "load-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Load table
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/load",
            json={
                "tables": [
                    {"source": f"{project_with_tables['bucket_name']}.orders"}
                ]
            },
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert "loaded" in data
        assert len(data["loaded"]) == 1

        result = data["loaded"][0]
        assert result["source"] == f"{project_with_tables['bucket_name']}.orders"
        assert result["destination"] == "orders"
        assert result["rows"] == 3  # 3 rows in test data

    def test_load_table_with_destination(self, client, project_with_tables):
        """Load table with custom destination name."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "alias-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Load table with destination
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/load",
            json={
                "tables": [
                    {
                        "source": f"{project_with_tables['bucket_name']}.orders",
                        "destination": "my_orders",
                    }
                ]
            },
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        result = data["loaded"][0]
        assert result["destination"] == "my_orders"

        # Verify in workspace objects
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        object_names = [obj["name"] for obj in data["workspace_objects"]]
        assert "my_orders" in object_names

    def test_load_table_source_not_found(self, client, project_with_tables):
        """Loading non-existent table returns zero rows."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "load-404-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Try to load non-existent table
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/load",
            json={
                "tables": [
                    {"source": f"{project_with_tables['bucket_name']}.nonexistent"}
                ]
            },
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        result = data["loaded"][0]
        assert result["rows"] == 0  # No rows loaded for missing table


class TestWorkspaceCredentialsReset:
    """Test resetting workspace credentials."""

    def test_reset_credentials_success(self, client, project_with_tables):
        """Reset workspace credentials."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "reset-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]
        old_password = response.json()["connection"]["password"]

        # Reset credentials
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/credentials/reset",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        # Should have connection info
        assert "host" in data
        assert "port" in data
        assert "database" in data
        assert "username" in data
        assert "password" in data

        # Password should be different
        assert data["password"] != old_password
        assert data["password"] is not None
        assert len(data["password"]) == 32

    def test_reset_credentials_password_not_in_get(self, client, project_with_tables):
        """Reset credentials - new password not in GET response."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "reset-password-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Reset credentials
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/credentials/reset",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200

        # Get workspace - password should NOT be in response
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["connection"]["password"] is None


class TestWorkspaceExpiration:
    """Test workspace expiration behavior."""

    def test_expired_workspace_returns_410(self, client, project_with_tables):
        """Accessing expired workspace returns 410 Gone."""
        from src.database import metadata_db
        import duckdb

        # Create workspace with 1 hour TTL
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "expire-test", "ttl_hours": 1},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Manually set expiration to the past
        past_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

        # Update workspace expiration directly in metadata
        # Note: DuckDB has FK constraint limitations - it checks FK even for UPDATE
        # of non-key columns, so we need to delete credentials first
        with metadata_db.connection() as conn:
            conn.execute(
                "DELETE FROM workspace_credentials WHERE workspace_id = ?",
                [workspace_id]
            )
            conn.execute(
                "UPDATE workspaces SET expires_at = ?::TIMESTAMPTZ WHERE id = ?",
                [past_time, workspace_id]
            )

        # Try to get workspace - should return 410
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 410
        assert response.json()["detail"]["error"] == "workspace_expired"

        # Try to clear workspace - should return 410
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/clear",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 410

        # Try to load data - should return 410
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/load",
            json={
                "tables": [
                    {"source": f"{project_with_tables['bucket_name']}.orders"}
                ]
            },
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 410


class TestWorkspaceFileStructure:
    """Test workspace file structure (ADR-009 compliance)."""

    def test_workspace_creates_in_workspaces_dir(self, client, project_with_tables):
        """Workspace file is created in _workspaces directory."""
        from src.config import settings

        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "dir-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Check file exists in correct location (project dir has project_ prefix)
        workspace_path = (
            settings.duckdb_dir
            / f"project_{project_with_tables['project_id']}"
            / "_workspaces"
            / f"{workspace_id}.duckdb"
        )
        assert workspace_path.exists()
        assert workspace_path.is_file()

    def test_workspace_delete_removes_file(self, client, project_with_tables):
        """Deleting workspace removes the .duckdb file."""
        from src.config import settings

        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "file-delete-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        workspace_path = (
            settings.duckdb_dir
            / f"project_{project_with_tables['project_id']}"
            / "_workspaces"
            / f"{workspace_id}.duckdb"
        )
        assert workspace_path.exists()

        # Delete workspace
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 204

        # File should be gone
        assert not workspace_path.exists()


class TestWorkspaceMetrics:
    """Test workspace metrics."""

    def test_workspace_count_metric(self, client, project_with_tables):
        """Workspace count metric is updated."""
        from src.database import metadata_db

        initial_count = metadata_db.count_workspaces()

        # Create workspaces
        for name in ["count-a", "count-b"]:
            response = client.post(
                f"/projects/{project_with_tables['project_id']}/workspaces",
                json={"name": name},
                headers=project_with_tables["project_headers"],
            )
            assert response.status_code == 201

        # Count should increase
        new_count = metadata_db.count_workspaces()
        assert new_count == initial_count + 2


class TestWorkspaceMultiTableLoad:
    """Test loading multiple tables into workspace."""

    def test_load_multiple_tables(self, client, project_with_tables):
        """Load multiple tables at once."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "multi-load-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Load multiple tables
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/load",
            json={
                "tables": [
                    {"source": f"{project_with_tables['bucket_name']}.orders"},
                    {"source": f"{project_with_tables['bucket_name']}.customers"},
                ]
            },
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert len(data["loaded"]) == 2

        # Verify workspace has both tables
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        object_names = [obj["name"] for obj in data["workspace_objects"]]
        assert "orders" in object_names
        assert "customers" in object_names

    def test_load_mixed_success_failure(self, client, project_with_tables):
        """Load mix of valid and invalid tables."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces",
            json={"name": "mixed-load-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Load one valid, one invalid table
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/workspaces/{workspace_id}/load",
            json={
                "tables": [
                    {"source": f"{project_with_tables['bucket_name']}.orders"},
                    {"source": f"{project_with_tables['bucket_name']}.nonexistent"},
                ]
            },
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert len(data["loaded"]) == 2

        # First should succeed (has rows)
        assert data["loaded"][0]["rows"] == 3
        assert data["loaded"][0]["source"] == f"{project_with_tables['bucket_name']}.orders"

        # Second should fail (no rows)
        assert data["loaded"][1]["rows"] == 0


# ====================================================================================
# Branch Workspace Tests
# ====================================================================================


@pytest.fixture
def project_with_branch(client, project_with_tables):
    """Create a project with a development branch."""
    # Create branch
    response = client.post(
        f"/projects/{project_with_tables['project_id']}/branches",
        json={"name": "dev-branch", "description": "Development branch for testing"},
        headers=project_with_tables["project_headers"],
    )
    assert response.status_code == 201
    branch_id = response.json()["id"]

    return {
        **project_with_tables,
        "branch_id": branch_id,
    }


class TestBranchWorkspaceCreate:
    """Test branch workspace creation."""

    def test_create_branch_workspace(self, client, project_with_branch):
        """Create a workspace in a branch."""
        response = client.post(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces",
            json={"name": "Branch Workspace"},
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 201
        data = response.json()

        assert data["name"] == "Branch Workspace"
        assert data["project_id"] == project_with_branch["project_id"]
        assert data["branch_id"] == project_with_branch["branch_id"]
        assert data["connection"]["password"] is not None  # Password returned on create

    def test_create_branch_workspace_branch_not_found(self, client, project_with_tables):
        """Cannot create workspace in non-existent branch."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches/nonexistent/workspaces",
            json={"name": "Test Workspace"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "branch_not_found"


class TestBranchWorkspaceList:
    """Test branch workspace listing."""

    def test_list_branch_workspaces_empty(self, client, project_with_branch):
        """List workspaces in branch with no workspaces."""
        response = client.get(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces",
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["workspaces"] == []
        assert response.json()["count"] == 0

    def test_list_branch_workspaces_returns_only_branch_workspaces(
        self, client, project_with_branch
    ):
        """Branch workspace list only includes workspaces for that branch."""
        # Create workspace in main project
        response = client.post(
            f"/projects/{project_with_branch['project_id']}/workspaces",
            json={"name": "Main Workspace"},
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 201

        # Create workspace in branch
        response = client.post(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces",
            json={"name": "Branch Workspace"},
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 201

        # List branch workspaces - should only see branch workspace
        response = client.get(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces",
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["workspaces"][0]["name"] == "Branch Workspace"
        assert data["workspaces"][0]["branch_id"] == project_with_branch["branch_id"]


class TestBranchWorkspaceGet:
    """Test branch workspace retrieval."""

    def test_get_branch_workspace_success(self, client, project_with_branch):
        """Get branch workspace details."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces",
            json={"name": "Detail Test"},
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Get workspace details
        response = client.get(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces/{workspace_id}",
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == workspace_id
        assert data["name"] == "Detail Test"
        assert data["branch_id"] == project_with_branch["branch_id"]
        assert data["connection"]["password"] is None  # Password not returned on GET

    def test_get_branch_workspace_not_found(self, client, project_with_branch):
        """Cannot get non-existent branch workspace."""
        response = client.get(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces/nonexistent",
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 404


class TestBranchWorkspaceDelete:
    """Test branch workspace deletion."""

    def test_delete_branch_workspace_success(self, client, project_with_branch):
        """Delete a branch workspace."""
        # Create workspace
        response = client.post(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces",
            json={"name": "Delete Test"},
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Delete workspace
        response = client.delete(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces/{workspace_id}",
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 204

        # Verify it's gone
        response = client.get(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces/{workspace_id}",
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 404

    def test_delete_branch_workspace_not_found(self, client, project_with_branch):
        """Cannot delete non-existent branch workspace."""
        response = client.delete(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces/nonexistent",
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 404


class TestBranchWorkspaceFileStructure:
    """Test branch workspace file structure."""

    def test_branch_workspace_creates_in_branch_workspaces_dir(
        self, client, project_with_branch
    ):
        """Branch workspace file is created in branch's _workspaces directory."""
        from src.config import settings

        # Create workspace
        response = client.post(
            f"/projects/{project_with_branch['project_id']}/branches/{project_with_branch['branch_id']}/workspaces",
            json={"name": "dir-test"},
            headers=project_with_branch["project_headers"],
        )
        assert response.status_code == 201
        workspace_id = response.json()["id"]

        # Check file exists in correct location (branch dir has special naming)
        branch_dir_name = f"project_{project_with_branch['project_id']}_branch_{project_with_branch['branch_id']}"
        workspace_path = (
            settings.duckdb_dir
            / branch_dir_name
            / "_workspaces"
            / f"{workspace_id}.duckdb"
        )
        assert workspace_path.exists()
        assert workspace_path.is_file()
