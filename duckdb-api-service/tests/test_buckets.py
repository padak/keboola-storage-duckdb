"""Tests for bucket CRUD endpoints."""

import pytest
from fastapi.testclient import TestClient


class TestCreateBucket:
    """Tests for POST /projects/{project_id}/buckets endpoint."""

    def test_create_bucket_success(self, client: TestClient, initialized_backend):
        """Test successful bucket creation."""
        # First create a project
        client.post("/projects", json={"id": "bucket_test_1", "name": "Test Project"})

        # Create bucket
        response = client.post(
            "/projects/bucket_test_1/buckets",
            json={"name": "in_c_sales", "description": "Sales data bucket"},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "in_c_sales"
        assert data["table_count"] == 0
        assert data["description"] == "Sales data bucket"

    def test_create_bucket_minimal(self, client: TestClient, initialized_backend):
        """Test creating bucket with only name (description optional)."""
        client.post("/projects", json={"id": "bucket_test_2"})

        response = client.post(
            "/projects/bucket_test_2/buckets",
            json={"name": "out_c_reports"},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "out_c_reports"
        assert data["description"] is None

    def test_create_bucket_updates_project_stats(
        self, client: TestClient, initialized_backend
    ):
        """Test that creating a bucket updates project statistics."""
        client.post("/projects", json={"id": "bucket_test_3"})

        # Check initial stats
        stats_before = client.get("/projects/bucket_test_3/stats").json()
        assert stats_before["bucket_count"] == 0

        # Create bucket
        client.post(
            "/projects/bucket_test_3/buckets",
            json={"name": "test_bucket"},
        )

        # Check updated stats
        stats_after = client.get("/projects/bucket_test_3/stats").json()
        assert stats_after["bucket_count"] == 1

    def test_create_bucket_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test creating bucket in non-existent project returns 404."""
        response = client.post(
            "/projects/nonexistent/buckets",
            json={"name": "test_bucket"},
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_create_bucket_conflict(self, client: TestClient, initialized_backend):
        """Test creating duplicate bucket returns 409."""
        client.post("/projects", json={"id": "bucket_test_4"})
        client.post(
            "/projects/bucket_test_4/buckets",
            json={"name": "duplicate"},
        )

        # Try to create again
        response = client.post(
            "/projects/bucket_test_4/buckets",
            json={"name": "duplicate"},
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "bucket_exists"


class TestGetBucket:
    """Tests for GET /projects/{project_id}/buckets/{bucket_name} endpoint."""

    def test_get_bucket_success(self, client: TestClient, initialized_backend):
        """Test getting an existing bucket."""
        client.post("/projects", json={"id": "get_bucket_1"})
        client.post(
            "/projects/get_bucket_1/buckets",
            json={"name": "my_bucket", "description": "Test bucket"},
        )

        response = client.get("/projects/get_bucket_1/buckets/my_bucket")

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "my_bucket"
        assert data["table_count"] == 0

    def test_get_bucket_not_found(self, client: TestClient, initialized_backend):
        """Test getting non-existent bucket returns 404."""
        client.post("/projects", json={"id": "get_bucket_2"})

        response = client.get("/projects/get_bucket_2/buckets/nonexistent")

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_get_bucket_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test getting bucket from non-existent project returns 404."""
        response = client.get("/projects/nonexistent/buckets/any")

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"


class TestListBuckets:
    """Tests for GET /projects/{project_id}/buckets endpoint."""

    def test_list_buckets_empty(self, client: TestClient, initialized_backend):
        """Test listing when no buckets exist."""
        client.post("/projects", json={"id": "list_bucket_1"})

        response = client.get("/projects/list_bucket_1/buckets")

        assert response.status_code == 200
        data = response.json()
        assert data["buckets"] == []
        assert data["total"] == 0

    def test_list_buckets_multiple(self, client: TestClient, initialized_backend):
        """Test listing multiple buckets."""
        client.post("/projects", json={"id": "list_bucket_2"})
        client.post("/projects/list_bucket_2/buckets", json={"name": "bucket_a"})
        client.post("/projects/list_bucket_2/buckets", json={"name": "bucket_b"})
        client.post("/projects/list_bucket_2/buckets", json={"name": "bucket_c"})

        response = client.get("/projects/list_bucket_2/buckets")

        assert response.status_code == 200
        data = response.json()
        assert len(data["buckets"]) == 3
        assert data["total"] == 3

        # Check alphabetical order
        names = [b["name"] for b in data["buckets"]]
        assert names == sorted(names)

    def test_list_buckets_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test listing buckets from non-existent project returns 404."""
        response = client.get("/projects/nonexistent/buckets")

        assert response.status_code == 404


class TestDeleteBucket:
    """Tests for DELETE /projects/{project_id}/buckets/{bucket_name} endpoint."""

    def test_delete_bucket_success(self, client: TestClient, initialized_backend):
        """Test deleting a bucket."""
        client.post("/projects", json={"id": "delete_bucket_1"})
        client.post("/projects/delete_bucket_1/buckets", json={"name": "to_delete"})

        # Verify exists
        assert (
            client.get("/projects/delete_bucket_1/buckets/to_delete").status_code == 200
        )

        # Delete
        response = client.delete("/projects/delete_bucket_1/buckets/to_delete")
        assert response.status_code == 204

        # Verify deleted
        assert (
            client.get("/projects/delete_bucket_1/buckets/to_delete").status_code == 404
        )

    def test_delete_bucket_updates_stats(
        self, client: TestClient, initialized_backend
    ):
        """Test that deleting a bucket updates project statistics."""
        client.post("/projects", json={"id": "delete_bucket_2"})
        client.post("/projects/delete_bucket_2/buckets", json={"name": "temp_bucket"})

        # Check stats before
        stats = client.get("/projects/delete_bucket_2/stats").json()
        assert stats["bucket_count"] == 1

        # Delete
        client.delete("/projects/delete_bucket_2/buckets/temp_bucket")

        # Check stats after
        stats = client.get("/projects/delete_bucket_2/stats").json()
        assert stats["bucket_count"] == 0

    def test_delete_bucket_not_found(self, client: TestClient, initialized_backend):
        """Test deleting non-existent bucket returns 404."""
        client.post("/projects", json={"id": "delete_bucket_3"})

        response = client.delete("/projects/delete_bucket_3/buckets/nonexistent")

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_delete_bucket_cascade(self, client: TestClient, initialized_backend):
        """Test delete with cascade=True (default)."""
        client.post("/projects", json={"id": "delete_bucket_4"})
        client.post("/projects/delete_bucket_4/buckets", json={"name": "cascade_test"})

        # Delete with explicit cascade
        response = client.delete(
            "/projects/delete_bucket_4/buckets/cascade_test?cascade=true"
        )
        assert response.status_code == 204


class TestBucketOperationsLog:
    """Tests for bucket operations audit logging."""

    def test_create_bucket_logs_operation(
        self, client: TestClient, initialized_backend
    ):
        """Test that creating a bucket logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "log_test_1"})
        client.post("/projects/log_test_1/buckets", json={"name": "logged_bucket"})

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status, resource_type FROM operations_log WHERE project_id = ? AND resource_type = 'bucket'",
            ["log_test_1"],
        )

        assert len(logs) >= 1
        assert any(log[0] == "create_bucket" and log[1] == "success" for log in logs)

    def test_delete_bucket_logs_operation(
        self, client: TestClient, initialized_backend
    ):
        """Test that deleting a bucket logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "log_test_2"})
        client.post("/projects/log_test_2/buckets", json={"name": "to_delete"})
        client.delete("/projects/log_test_2/buckets/to_delete")

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status FROM operations_log WHERE project_id = ? AND resource_type = 'bucket' ORDER BY timestamp",
            ["log_test_2"],
        )

        operations = [log[0] for log in logs]
        assert "create_bucket" in operations
        assert "delete_bucket" in operations
