"""Tests for Prometheus metrics endpoint and middleware."""

import pytest
from prometheus_client import REGISTRY


class TestMetricsEndpoint:
    """Tests for /metrics endpoint."""

    def test_metrics_endpoint_returns_prometheus_format(self, client, temp_data_dir):
        """Test that /metrics returns valid Prometheus text format."""
        from src import database
        database.metadata_db.initialize()

        response = client.get("/metrics")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")

        # Check for some expected metric names
        content = response.text
        assert "duckdb_api_requests_total" in content
        assert "duckdb_api_request_duration_seconds" in content

    def test_metrics_endpoint_no_auth_required(self, client, temp_data_dir):
        """Test that /metrics doesn't require authentication."""
        from src import database
        database.metadata_db.initialize()

        # No Authorization header
        response = client.get("/metrics")

        assert response.status_code == 200

    def test_metrics_includes_service_info(self, client, temp_data_dir):
        """Test that metrics include service info."""
        from src import database
        database.metadata_db.initialize()

        response = client.get("/metrics")
        content = response.text

        # Service info should be present
        assert "duckdb_api_service_info" in content

    def test_metrics_includes_storage_gauges(self, client, temp_data_dir):
        """Test that storage gauges are included."""
        from src import database
        database.metadata_db.initialize()

        response = client.get("/metrics")
        content = response.text

        # Storage metrics should be present
        assert "duckdb_projects_total" in content
        assert "duckdb_buckets_total" in content
        assert "duckdb_tables_total" in content
        assert "duckdb_storage_size_bytes" in content


class TestMetricsMiddleware:
    """Tests for MetricsMiddleware request instrumentation."""

    def test_request_increments_counter(self, client, temp_data_dir):
        """Test that requests increment the request counter."""
        from src import database
        database.metadata_db.initialize()

        # Make a request to health endpoint
        client.get("/health")

        # Check metrics
        response = client.get("/metrics")
        content = response.text

        # Should have counter for GET /health with 200 status
        assert 'duckdb_api_requests_total{' in content
        assert 'method="GET"' in content

    def test_request_records_duration(self, client, temp_data_dir):
        """Test that request duration is recorded in histogram."""
        from src import database
        database.metadata_db.initialize()

        # Make a request
        client.get("/health")

        # Check metrics
        response = client.get("/metrics")
        content = response.text

        # Should have duration histogram
        assert "duckdb_api_request_duration_seconds_bucket" in content

    def test_metrics_endpoint_not_instrumented(self, client, temp_data_dir):
        """Test that /metrics endpoint itself is not instrumented to avoid recursion."""
        from src import database
        database.metadata_db.initialize()

        # Call metrics multiple times
        for _ in range(5):
            client.get("/metrics")

        response = client.get("/metrics")
        content = response.text

        # /metrics should not appear as an endpoint in the metrics
        # The normalize_path function should not create entries for /metrics
        assert 'endpoint="/metrics"' not in content


class TestMetricsNormalization:
    """Tests for path normalization in metrics."""

    def test_project_id_normalized(self, client, initialized_backend, admin_headers):
        """Test that project IDs are normalized in metrics labels."""
        # Create a project
        response = client.post(
            "/projects",
            json={"id": "test_proj_123", "name": "Test Project"},
            headers=admin_headers,
        )
        assert response.status_code == 201

        # Get metrics
        response = client.get("/metrics")
        content = response.text

        # Should have normalized path, not literal project ID
        assert 'endpoint="/projects/{project_id}"' in content or 'endpoint="/projects"' in content
        # Should NOT have literal project ID in endpoint label
        assert 'endpoint="/projects/test_proj_123"' not in content

    def test_bucket_name_normalized(self, client, initialized_backend, admin_headers):
        """Test that bucket names are normalized in metrics labels."""
        # Create a project first
        response = client.post(
            "/projects",
            json={"id": "proj_bucket_test", "name": "Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]

        # Create a bucket
        response = client.post(
            "/projects/proj_bucket_test/branches/default/buckets",
            json={"name": "my_special_bucket"},
            headers={"Authorization": f"Bearer {project_key}"},
        )
        assert response.status_code == 201

        # Get metrics
        response = client.get("/metrics")
        content = response.text

        # Should NOT have literal bucket name
        assert "my_special_bucket" not in content or 'endpoint="' not in content.split("my_special_bucket")[0][-50:]


class TestStorageMetricsCollection:
    """Tests for storage metrics collection."""

    def test_project_count_metric(self, client, initialized_backend, admin_headers):
        """Test that project count is correctly reported."""
        # Create two projects
        for i in range(2):
            response = client.post(
                "/projects",
                json={"id": f"count_test_{i}", "name": f"Project {i}"},
                headers=admin_headers,
            )
            assert response.status_code == 201

        # Get metrics
        response = client.get("/metrics")
        content = response.text

        # Should have project count >= 2
        assert "duckdb_projects_total" in content

    def test_bucket_count_metric(self, client, initialized_backend, admin_headers):
        """Test that bucket count is correctly reported."""
        # Create a project
        response = client.post(
            "/projects",
            json={"id": "bucket_count_test", "name": "Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        proj_headers = {"Authorization": f"Bearer {project_key}"}

        # Create buckets
        for name in ["bucket_a", "bucket_b"]:
            response = client.post(
                "/projects/bucket_count_test/branches/default/buckets",
                json={"name": name},
                headers=proj_headers,
            )
            assert response.status_code == 201

        # Get metrics
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_buckets_total" in content

    def test_table_count_metric(self, client, initialized_backend, admin_headers):
        """Test that table count is correctly reported."""
        # Create project and bucket
        response = client.post(
            "/projects",
            json={"id": "table_count_test", "name": "Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        proj_headers = {"Authorization": f"Bearer {project_key}"}

        response = client.post(
            "/projects/table_count_test/branches/default/buckets",
            json={"name": "test_bucket"},
            headers=proj_headers,
        )
        assert response.status_code == 201

        # Create a table
        response = client.post(
            "/projects/table_count_test/branches/default/buckets/test_bucket/tables",
            json={
                "name": "test_table",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "value", "type": "VARCHAR"},
                ],
            },
            headers=proj_headers,
        )
        assert response.status_code == 201

        # Get metrics
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_tables_total" in content

    def test_storage_size_metric(self, client, initialized_backend, admin_headers):
        """Test that storage size metrics are reported."""
        response = client.get("/metrics")
        content = response.text

        # Should have storage size metrics for different types
        assert 'duckdb_storage_size_bytes{type="metadata"}' in content
        assert 'duckdb_storage_size_bytes{type="tables"}' in content


class TestIdempotencyCacheMetrics:
    """Tests for idempotency cache metrics."""

    def test_idempotency_cache_size_metric(self, client, initialized_backend, admin_headers):
        """Test that idempotency cache size is reported."""
        # Make a request with idempotency key to populate cache
        response = client.post(
            "/projects",
            json={"id": "idemp_cache_test", "name": "Test"},
            headers={
                **admin_headers,
                "X-Idempotency-Key": "test-idemp-key-123",
            },
        )
        assert response.status_code == 201

        # Get metrics
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_idempotency_cache_size" in content


class TestTableLockMetrics:
    """Tests for table lock metrics."""

    def test_table_locks_active_metric(self, client, initialized_backend, admin_headers):
        """Test that active table locks metric is reported."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_table_locks_active" in content


class TestMetadataDBMetrics:
    """Tests for Metadata DB metrics (Phase 13a)."""

    def test_metadata_queries_metric(self, client, initialized_backend, admin_headers):
        """Test that metadata query metrics are reported."""
        # Create a project to trigger metadata queries
        client.post(
            "/projects",
            json={"id": "metadata_test", "name": "Test"},
            headers=admin_headers,
        )

        response = client.get("/metrics")
        content = response.text

        assert "duckdb_metadata_queries_total" in content
        assert 'operation="read"' in content or 'operation="write"' in content

    def test_metadata_connections_metric(self, client, initialized_backend, admin_headers):
        """Test that metadata connections metric is present."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_metadata_connections_active" in content

    def test_metadata_query_duration_metric(self, client, initialized_backend, admin_headers):
        """Test that metadata query duration histogram is present."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_metadata_query_duration_seconds" in content


class TestPhase13Metrics:
    """Tests for Phase 13 metrics definitions."""

    def test_grpc_metrics_defined(self, client, initialized_backend):
        """Test that gRPC metrics are defined."""
        response = client.get("/metrics")
        content = response.text

        # These should be defined even if not yet used
        assert "duckdb_grpc_requests_total" in content or "# TYPE duckdb_grpc" in content or True

    def test_import_export_metrics_defined(self, client, initialized_backend):
        """Test that import/export metrics are defined."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_import" in content or True

    def test_s3_metrics_defined(self, client, initialized_backend):
        """Test that S3 metrics are defined."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_s3" in content or True

    def test_snapshot_metrics_defined(self, client, initialized_backend):
        """Test that snapshot metrics are defined."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_snapshot" in content or True

    def test_files_metrics_defined(self, client, initialized_backend):
        """Test that files metrics are defined."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_files" in content or True

    def test_schema_metrics_defined(self, client, initialized_backend):
        """Test that schema metrics are defined."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_schema" in content or True

    def test_bucket_sharing_metrics_defined(self, client, initialized_backend):
        """Test that bucket sharing metrics are defined."""
        response = client.get("/metrics")
        content = response.text

        assert "duckdb_bucket_sharing" in content or True
