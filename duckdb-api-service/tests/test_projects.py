"""Tests for project CRUD endpoints."""

import pytest
from fastapi.testclient import TestClient


class TestCreateProject:
    """Tests for POST /projects endpoint."""

    def test_create_project_success(self, client: TestClient, initialized_backend):
        """Test successful project creation."""
        response = client.post(
            "/projects",
            json={"id": "123", "name": "Test Project"},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["id"] == "123"
        assert data["name"] == "Test Project"
        assert data["status"] == "active"
        # ADR-009: db_path is now directory, not file
        assert data["db_path"] == "project_123"
        # ADR-009: Empty project has no tables, so size is 0
        assert data["size_bytes"] == 0

    def test_create_project_minimal(self, client: TestClient, initialized_backend):
        """Test creating project with only ID (name optional)."""
        response = client.post(
            "/projects",
            json={"id": "456"},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["id"] == "456"
        assert data["name"] is None

    def test_create_project_with_settings(self, client: TestClient, initialized_backend):
        """Test creating project with custom settings."""
        response = client.post(
            "/projects",
            json={
                "id": "789",
                "name": "Project with settings",
                "settings": {"feature_flags": ["beta"], "max_tables": 100},
            },
        )

        assert response.status_code == 201
        data = response.json()
        assert data["settings"]["feature_flags"] == ["beta"]
        assert data["settings"]["max_tables"] == 100

    def test_create_project_creates_db_file(
        self, client: TestClient, initialized_backend
    ):
        """Test that creating a project creates the project directory."""
        response = client.post(
            "/projects",
            json={"id": "file_test"},
        )

        assert response.status_code == 201

        # ADR-009: Check project directory exists (not file)
        project_dir = initialized_backend["duckdb_dir"] / "project_file_test"
        assert project_dir.is_dir()

    def test_create_project_conflict(self, client: TestClient, initialized_backend):
        """Test creating duplicate project returns 409."""
        # Create first project
        client.post("/projects", json={"id": "duplicate"})

        # Try to create again
        response = client.post("/projects", json={"id": "duplicate"})

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "project_exists"

    def test_create_project_returns_request_id(
        self, client: TestClient, initialized_backend
    ):
        """Test that X-Request-ID is returned."""
        response = client.post(
            "/projects",
            json={"id": "req_id_test"},
            headers={"X-Request-ID": "my-request-123"},
        )

        assert response.headers["X-Request-ID"] == "my-request-123"


class TestGetProject:
    """Tests for GET /projects/{project_id} endpoint."""

    def test_get_project_success(self, client: TestClient, initialized_backend):
        """Test getting an existing project."""
        # Create project first
        client.post("/projects", json={"id": "get_test", "name": "Get Test"})

        # Get it
        response = client.get("/projects/get_test")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "get_test"
        assert data["name"] == "Get Test"

    def test_get_project_not_found(self, client: TestClient, initialized_backend):
        """Test getting non-existent project returns 404."""
        response = client.get("/projects/nonexistent")

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "not_found"


class TestListProjects:
    """Tests for GET /projects endpoint."""

    def test_list_projects_empty(self, client: TestClient, initialized_backend):
        """Test listing when no projects exist."""
        response = client.get("/projects")

        assert response.status_code == 200
        data = response.json()
        assert data["projects"] == []
        assert data["total"] == 0

    def test_list_projects_multiple(self, client: TestClient, initialized_backend):
        """Test listing multiple projects."""
        # Create projects
        client.post("/projects", json={"id": "list_1", "name": "Project 1"})
        client.post("/projects", json={"id": "list_2", "name": "Project 2"})
        client.post("/projects", json={"id": "list_3", "name": "Project 3"})

        response = client.get("/projects")

        assert response.status_code == 200
        data = response.json()
        assert len(data["projects"]) == 3
        assert data["total"] == 3

    def test_list_projects_filter_by_status(
        self, client: TestClient, initialized_backend
    ):
        """Test filtering projects by status."""
        # Create and delete one project
        client.post("/projects", json={"id": "active_proj"})
        client.post("/projects", json={"id": "deleted_proj"})
        client.delete("/projects/deleted_proj")

        # List only active
        response = client.get("/projects?status=active")

        assert response.status_code == 200
        data = response.json()
        assert len(data["projects"]) == 1
        assert data["projects"][0]["id"] == "active_proj"

    def test_list_projects_pagination(self, client: TestClient, initialized_backend):
        """Test pagination with limit and offset."""
        # Create 5 projects
        for i in range(5):
            client.post("/projects", json={"id": f"page_{i}"})

        # Get first 2
        response = client.get("/projects?limit=2&offset=0")
        data = response.json()
        assert len(data["projects"]) == 2
        assert data["total"] == 5

        # Get next 2
        response = client.get("/projects?limit=2&offset=2")
        data = response.json()
        assert len(data["projects"]) == 2


class TestUpdateProject:
    """Tests for PUT /projects/{project_id} endpoint."""

    def test_update_project_name(self, client: TestClient, initialized_backend):
        """Test updating project name."""
        # Create project
        client.post("/projects", json={"id": "update_test", "name": "Original"})

        # Update
        response = client.put(
            "/projects/update_test",
            json={"name": "Updated Name"},
        )

        assert response.status_code == 200
        assert response.json()["name"] == "Updated Name"

    def test_update_project_not_found(self, client: TestClient, initialized_backend):
        """Test updating non-existent project returns 404."""
        response = client.put(
            "/projects/nonexistent",
            json={"name": "New Name"},
        )

        assert response.status_code == 404


class TestDeleteProject:
    """Tests for DELETE /projects/{project_id} endpoint."""

    def test_delete_project_success(self, client: TestClient, initialized_backend):
        """Test deleting a project."""
        # Create project
        client.post("/projects", json={"id": "delete_test"})

        # ADR-009: Verify project directory exists
        project_dir = initialized_backend["duckdb_dir"] / "project_delete_test"
        assert project_dir.is_dir()

        # Delete
        response = client.delete("/projects/delete_test")
        assert response.status_code == 204

        # ADR-009: Project directory should be deleted
        assert not project_dir.exists()

        # Project should be marked as deleted in metadata
        get_response = client.get("/projects/delete_test")
        assert get_response.status_code == 200
        assert get_response.json()["status"] == "deleted"

    def test_delete_project_not_found(self, client: TestClient, initialized_backend):
        """Test deleting non-existent project returns 404."""
        response = client.delete("/projects/nonexistent")

        assert response.status_code == 404


class TestProjectStats:
    """Tests for GET /projects/{project_id}/stats endpoint."""

    def test_get_project_stats(self, client: TestClient, initialized_backend):
        """Test getting project statistics."""
        # Create project
        client.post("/projects", json={"id": "stats_test"})

        # Get stats
        response = client.get("/projects/stats_test/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "stats_test"
        # ADR-009: Empty project has no tables, so size is 0
        assert data["size_bytes"] == 0
        assert data["table_count"] == 0
        assert data["bucket_count"] == 0

    def test_get_stats_not_found(self, client: TestClient, initialized_backend):
        """Test getting stats for non-existent project returns 404."""
        response = client.get("/projects/nonexistent/stats")

        assert response.status_code == 404


class TestOperationsLog:
    """Tests for operations audit logging."""

    def test_create_logs_operation(self, client: TestClient, initialized_backend):
        """Test that creating a project logs the operation."""
        from src.database import metadata_db

        # Create project
        client.post("/projects", json={"id": "log_test"})

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status, project_id FROM operations_log WHERE project_id = ?",
            ["log_test"],
        )

        assert len(logs) >= 1
        assert any(log[0] == "create_project" and log[1] == "success" for log in logs)

    def test_delete_logs_operation(self, client: TestClient, initialized_backend):
        """Test that deleting a project logs the operation."""
        from src.database import metadata_db

        # Create and delete project
        client.post("/projects", json={"id": "delete_log_test"})
        client.delete("/projects/delete_log_test")

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status FROM operations_log WHERE project_id = ? ORDER BY timestamp",
            ["delete_log_test"],
        )

        operations = [log[0] for log in logs]
        assert "create_project" in operations
        assert "delete_project" in operations
