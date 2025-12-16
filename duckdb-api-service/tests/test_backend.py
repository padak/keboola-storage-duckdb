"""Tests for backend endpoints."""

import pytest
from fastapi.testclient import TestClient


class TestHealthEndpoint:
    """Tests for GET /health endpoint."""

    def test_health_check_success(self, client: TestClient, temp_data_dir):
        """Test successful health check when storage is available."""
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["storage_available"] is True
        assert "version" in data
        assert "details" in data

    def test_health_check_includes_all_paths(self, client: TestClient, temp_data_dir):
        """Test that health check reports all storage paths."""
        response = client.get("/health")

        assert response.status_code == 200
        details = response.json()["details"]

        assert "data_dir" in details
        assert "duckdb_dir" in details
        assert "files_dir" in details
        assert "snapshots_dir" in details

    def test_health_check_fails_when_storage_missing(
        self, client: TestClient, missing_data_dir
    ):
        """Test health check fails when storage paths don't exist."""
        response = client.get("/health")

        assert response.status_code == 503
        data = response.json()
        assert "detail" in data

    def test_health_check_returns_request_id(self, client: TestClient, temp_data_dir):
        """Test that health check returns X-Request-ID header."""
        response = client.get("/health")

        assert "X-Request-ID" in response.headers

    def test_health_check_uses_provided_request_id(
        self, client: TestClient, temp_data_dir
    ):
        """Test that provided X-Request-ID is echoed back."""
        request_id = "test-request-id-123"
        response = client.get("/health", headers={"X-Request-ID": request_id})

        assert response.headers["X-Request-ID"] == request_id


class TestInitBackendEndpoint:
    """Tests for POST /backend/init endpoint."""

    def test_init_backend_success(self, client: TestClient, temp_data_dir, admin_headers):
        """Test successful backend initialization."""
        response = client.post("/backend/init", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "message" in data
        assert "storage_paths" in data

    def test_init_backend_returns_storage_paths(
        self, client: TestClient, temp_data_dir, admin_headers
    ):
        """Test that init returns configured storage paths."""
        response = client.post("/backend/init", headers=admin_headers)

        assert response.status_code == 200
        paths = response.json()["storage_paths"]

        assert "data_dir" in paths
        assert "duckdb_dir" in paths
        assert "files_dir" in paths
        assert "snapshots_dir" in paths

    def test_init_backend_creates_missing_directories(
        self, client: TestClient, temp_data_dir, admin_headers
    ):
        """Test that init creates directories if they don't exist."""
        import shutil

        # Remove one directory
        shutil.rmtree(temp_data_dir["snapshots_dir"])
        assert not temp_data_dir["snapshots_dir"].exists()

        # Call init
        response = client.post("/backend/init", headers=admin_headers)

        assert response.status_code == 200
        assert "created" in response.json()["message"]
        assert temp_data_dir["snapshots_dir"].exists()

    def test_init_backend_idempotent(self, client: TestClient, temp_data_dir, admin_headers):
        """Test that init can be called multiple times safely."""
        # Call twice
        response1 = client.post("/backend/init", headers=admin_headers)
        response2 = client.post("/backend/init", headers=admin_headers)

        assert response1.status_code == 200
        assert response2.status_code == 200


class TestRemoveBackendEndpoint:
    """Tests for POST /backend/remove endpoint."""

    def test_remove_backend_is_noop(self, client: TestClient, temp_data_dir, admin_headers):
        """Test that remove backend is a no-op (same as BigQuery)."""
        # Initialize first
        client.post("/backend/init", headers=admin_headers)

        # Remove
        response = client.post("/backend/remove", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "no-op" in data["message"].lower()

    def test_remove_backend_does_not_delete_data(
        self, client: TestClient, temp_data_dir, admin_headers
    ):
        """Test that remove does not actually delete data directories."""
        # Initialize first
        client.post("/backend/init", headers=admin_headers)

        # Remove
        client.post("/backend/remove", headers=admin_headers)

        # Directories should still exist
        assert temp_data_dir["duckdb_dir"].exists()
        assert temp_data_dir["files_dir"].exists()


class TestRootEndpoint:
    """Tests for GET / endpoint."""

    def test_root_returns_service_info(self, client: TestClient, temp_data_dir):
        """Test root endpoint returns service information."""
        response = client.get("/")

        assert response.status_code == 200
        data = response.json()
        assert "service" in data
        assert "version" in data
        assert "health" in data
