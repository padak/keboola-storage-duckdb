"""Tests for bucket CRUD endpoints."""

import pytest
from fastapi.testclient import TestClient


class TestCreateBucket:
    """Tests for POST /projects/{project_id}/buckets endpoint."""

    def test_create_bucket_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful bucket creation."""
        # First create a project
        client.post("/projects", json={"id": "bucket_test_1", "name": "Test Project"}, headers=admin_headers)

        # Create bucket
        response = client.post(
            "/projects/bucket_test_1/buckets",
            json={"name": "in_c_sales", "description": "Sales data bucket"},
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "in_c_sales"
        assert data["table_count"] == 0
        assert data["description"] == "Sales data bucket"

    def test_create_bucket_minimal(self, client: TestClient, initialized_backend, admin_headers):
        """Test creating bucket with only name (description optional)."""
        client.post("/projects", json={"id": "bucket_test_2"}, headers=admin_headers)

        response = client.post(
            "/projects/bucket_test_2/buckets",
            json={"name": "out_c_reports"},
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "out_c_reports"
        assert data["description"] is None

    def test_create_bucket_updates_project_stats(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that creating a bucket updates project statistics."""
        client.post("/projects", json={"id": "bucket_test_3"}, headers=admin_headers)

        # Check initial stats
        stats_before = client.get("/projects/bucket_test_3/stats", headers=admin_headers).json()
        assert stats_before["bucket_count"] == 0

        # Create bucket
        client.post(
            "/projects/bucket_test_3/buckets",
            json={"name": "test_bucket"},
            headers=admin_headers,
        )

        # Check updated stats
        stats_after = client.get("/projects/bucket_test_3/stats", headers=admin_headers).json()
        assert stats_after["bucket_count"] == 1

    def test_create_bucket_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test creating bucket in non-existent project returns 404."""
        response = client.post(
            "/projects/nonexistent/buckets",
            json={"name": "test_bucket"},
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_create_bucket_conflict(self, client: TestClient, initialized_backend, admin_headers):
        """Test creating duplicate bucket returns 409."""
        client.post("/projects", json={"id": "bucket_test_4"}, headers=admin_headers)
        client.post(
            "/projects/bucket_test_4/buckets",
            json={"name": "duplicate"},
            headers=admin_headers,
        )

        # Try to create again
        response = client.post(
            "/projects/bucket_test_4/buckets",
            json={"name": "duplicate"},
            headers=admin_headers,
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "bucket_exists"


class TestGetBucket:
    """Tests for GET /projects/{project_id}/buckets/{bucket_name} endpoint."""

    def test_get_bucket_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test getting an existing bucket."""
        client.post("/projects", json={"id": "get_bucket_1"}, headers=admin_headers)
        client.post(
            "/projects/get_bucket_1/buckets",
            json={"name": "my_bucket", "description": "Test bucket"},
            headers=admin_headers,
        )

        response = client.get("/projects/get_bucket_1/buckets/my_bucket", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "my_bucket"
        assert data["table_count"] == 0

    def test_get_bucket_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test getting non-existent bucket returns 404."""
        client.post("/projects", json={"id": "get_bucket_2"}, headers=admin_headers)

        response = client.get("/projects/get_bucket_2/buckets/nonexistent", headers=admin_headers)

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_get_bucket_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test getting bucket from non-existent project returns 404."""
        response = client.get("/projects/nonexistent/buckets/any", headers=admin_headers)

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"


class TestListBuckets:
    """Tests for GET /projects/{project_id}/buckets endpoint."""

    def test_list_buckets_empty(self, client: TestClient, initialized_backend, admin_headers):
        """Test listing when no buckets exist."""
        client.post("/projects", json={"id": "list_bucket_1"}, headers=admin_headers)

        response = client.get("/projects/list_bucket_1/buckets", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["buckets"] == []
        assert data["total"] == 0

    def test_list_buckets_multiple(self, client: TestClient, initialized_backend, admin_headers):
        """Test listing multiple buckets."""
        client.post("/projects", json={"id": "list_bucket_2"}, headers=admin_headers)
        client.post("/projects/list_bucket_2/buckets", json={"name": "bucket_a"}, headers=admin_headers)
        client.post("/projects/list_bucket_2/buckets", json={"name": "bucket_b"}, headers=admin_headers)
        client.post("/projects/list_bucket_2/buckets", json={"name": "bucket_c"}, headers=admin_headers)

        response = client.get("/projects/list_bucket_2/buckets", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert len(data["buckets"]) == 3
        assert data["total"] == 3

        # Check alphabetical order
        names = [b["name"] for b in data["buckets"]]
        assert names == sorted(names)

    def test_list_buckets_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test listing buckets from non-existent project returns 404."""
        response = client.get("/projects/nonexistent/buckets", headers=admin_headers)

        assert response.status_code == 404


class TestDeleteBucket:
    """Tests for DELETE /projects/{project_id}/buckets/{bucket_name} endpoint."""

    def test_delete_bucket_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test deleting a bucket."""
        client.post("/projects", json={"id": "delete_bucket_1"}, headers=admin_headers)
        client.post("/projects/delete_bucket_1/buckets", json={"name": "to_delete"}, headers=admin_headers)

        # Verify exists
        assert (
            client.get("/projects/delete_bucket_1/buckets/to_delete", headers=admin_headers).status_code == 200
        )

        # Delete
        response = client.delete("/projects/delete_bucket_1/buckets/to_delete", headers=admin_headers)
        assert response.status_code == 204

        # Verify deleted
        assert (
            client.get("/projects/delete_bucket_1/buckets/to_delete", headers=admin_headers).status_code == 404
        )

    def test_delete_bucket_updates_stats(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that deleting a bucket updates project statistics."""
        client.post("/projects", json={"id": "delete_bucket_2"}, headers=admin_headers)
        client.post("/projects/delete_bucket_2/buckets", json={"name": "temp_bucket"}, headers=admin_headers)

        # Check stats before
        stats = client.get("/projects/delete_bucket_2/stats", headers=admin_headers).json()
        assert stats["bucket_count"] == 1

        # Delete
        client.delete("/projects/delete_bucket_2/buckets/temp_bucket", headers=admin_headers)

        # Check stats after
        stats = client.get("/projects/delete_bucket_2/stats", headers=admin_headers).json()
        assert stats["bucket_count"] == 0

    def test_delete_bucket_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test deleting non-existent bucket returns 404."""
        client.post("/projects", json={"id": "delete_bucket_3"}, headers=admin_headers)

        response = client.delete("/projects/delete_bucket_3/buckets/nonexistent", headers=admin_headers)

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_delete_bucket_cascade(self, client: TestClient, initialized_backend, admin_headers):
        """Test delete with cascade=True (default)."""
        client.post("/projects", json={"id": "delete_bucket_4"}, headers=admin_headers)
        client.post("/projects/delete_bucket_4/buckets", json={"name": "cascade_test"}, headers=admin_headers)

        # Delete with explicit cascade
        response = client.delete(
            "/projects/delete_bucket_4/buckets/cascade_test?cascade=true",
            headers=admin_headers,
        )
        assert response.status_code == 204


class TestBucketOperationsLog:
    """Tests for bucket operations audit logging."""

    def test_create_bucket_logs_operation(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that creating a bucket logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "log_test_1"}, headers=admin_headers)
        client.post("/projects/log_test_1/buckets", json={"name": "logged_bucket"}, headers=admin_headers)

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status, resource_type FROM operations_log WHERE project_id = ? AND resource_type = 'bucket'",
            ["log_test_1"],
        )

        assert len(logs) >= 1
        assert any(log[0] == "create_bucket" and log[1] == "success" for log in logs)

    def test_delete_bucket_logs_operation(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that deleting a bucket logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "log_test_2"}, headers=admin_headers)
        client.post("/projects/log_test_2/buckets", json={"name": "to_delete"}, headers=admin_headers)
        client.delete("/projects/log_test_2/buckets/to_delete", headers=admin_headers)

        # Check operations log
        logs = metadata_db.execute(
            "SELECT operation, status FROM operations_log WHERE project_id = ? AND resource_type = 'bucket' ORDER BY timestamp",
            ["log_test_2"],
        )

        operations = [log[0] for log in logs]
        assert "create_bucket" in operations
        assert "delete_bucket" in operations


class TestBucketFilesystemADR009:
    """Tests for ADR-009 filesystem structure verification.

    ADR-009 defines: Bucket = directory within project directory.
    Path: /data/duckdb/project_{id}/{bucket_name}/
    """

    def test_bucket_creates_directory(self, client: TestClient, initialized_backend, admin_headers):
        """Test that creating a bucket creates a subdirectory in project directory."""
        client.post("/projects", json={"id": "fs_bucket_1"}, headers=admin_headers)

        # Create bucket
        response = client.post(
            "/projects/fs_bucket_1/buckets",
            json={"name": "in_c_sales"},
            headers=admin_headers,
        )
        assert response.status_code == 201

        # ADR-009: Verify bucket directory exists
        project_dir = initialized_backend["duckdb_dir"] / "project_fs_bucket_1"
        bucket_dir = project_dir / "in_c_sales"
        assert bucket_dir.is_dir(), f"Bucket directory should exist: {bucket_dir}"

    def test_bucket_delete_removes_directory(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that deleting a bucket removes its directory."""
        client.post("/projects", json={"id": "fs_bucket_2"}, headers=admin_headers)
        client.post("/projects/fs_bucket_2/buckets", json={"name": "to_delete"}, headers=admin_headers)

        # Verify bucket directory exists before delete
        project_dir = initialized_backend["duckdb_dir"] / "project_fs_bucket_2"
        bucket_dir = project_dir / "to_delete"
        assert bucket_dir.is_dir(), "Bucket directory should exist before delete"

        # Delete bucket
        response = client.delete("/projects/fs_bucket_2/buckets/to_delete", headers=admin_headers)
        assert response.status_code == 204

        # ADR-009: Verify bucket directory is removed
        assert not bucket_dir.exists(), f"Bucket directory should be deleted: {bucket_dir}"

    def test_multiple_buckets_create_separate_directories(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that multiple buckets create separate directories."""
        client.post("/projects", json={"id": "fs_bucket_3"}, headers=admin_headers)

        # Create multiple buckets
        for bucket_name in ["in_c_sales", "out_c_reports", "in_c_customers"]:
            response = client.post(
                "/projects/fs_bucket_3/buckets",
                json={"name": bucket_name},
                headers=admin_headers,
            )
            assert response.status_code == 201

        # ADR-009: Verify each bucket has its own directory
        project_dir = initialized_backend["duckdb_dir"] / "project_fs_bucket_3"
        for bucket_name in ["in_c_sales", "out_c_reports", "in_c_customers"]:
            bucket_dir = project_dir / bucket_name
            assert bucket_dir.is_dir(), f"Bucket directory should exist: {bucket_dir}"

        # Verify total directory count (3 buckets)
        subdirs = [d for d in project_dir.iterdir() if d.is_dir()]
        assert len(subdirs) == 3
