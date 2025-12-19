"""Tests for idempotency middleware."""

import pytest
import uuid
import json
import time
from datetime import datetime, timezone, timedelta

from src.database import metadata_db


class TestIdempotencyDatabase:
    """Test database-level idempotency operations."""

    def test_store_and_get_idempotency_key(self, initialized_backend):
        """Test storing and retrieving idempotency key."""
        key = f"test_key_{uuid.uuid4()}"

        metadata_db.store_idempotency_key(
            key=key,
            method="POST",
            endpoint="/projects",
            request_hash="abc123",
            response_status=201,
            response_body='{"id": "123"}',
            ttl_seconds=600,
        )

        result = metadata_db.get_idempotency_key(key)
        assert result is not None
        assert result["key"] == key
        assert result["method"] == "POST"
        assert result["endpoint"] == "/projects"
        assert result["response_status"] == 201
        assert result["response_body"] == '{"id": "123"}'
        assert result["request_hash"] == "abc123"

    def test_get_nonexistent_key_returns_none(self, initialized_backend):
        """Non-existent key returns None."""
        result = metadata_db.get_idempotency_key("nonexistent_key_12345")
        assert result is None

    def test_expired_key_returns_none(self, initialized_backend):
        """Expired key returns None."""
        key = f"expired_key_{uuid.uuid4()}"

        # Store with 0 second TTL (immediately expired)
        metadata_db.store_idempotency_key(
            key=key,
            method="POST",
            endpoint="/test",
            request_hash=None,
            response_status=200,
            response_body="{}",
            ttl_seconds=0,
        )

        # Should not be found (expired)
        result = metadata_db.get_idempotency_key(key)
        assert result is None

    def test_cleanup_expired_keys(self, initialized_backend):
        """Test cleanup of expired keys."""
        # Store expired key
        expired_key = f"expired_{uuid.uuid4()}"
        metadata_db.store_idempotency_key(
            key=expired_key,
            method="POST",
            endpoint="/test",
            request_hash=None,
            response_status=200,
            response_body="{}",
            ttl_seconds=0,
        )

        # Store valid key
        valid_key = f"valid_{uuid.uuid4()}"
        metadata_db.store_idempotency_key(
            key=valid_key,
            method="POST",
            endpoint="/test",
            request_hash=None,
            response_status=200,
            response_body="{}",
            ttl_seconds=3600,
        )

        # Cleanup
        deleted = metadata_db.cleanup_expired_idempotency_keys()
        assert deleted >= 1

        # Valid key should still exist
        result = metadata_db.get_idempotency_key(valid_key)
        assert result is not None

        # Expired key should be gone
        result = metadata_db.get_idempotency_key(expired_key)
        assert result is None

    def test_idempotency_key_with_null_hash(self, initialized_backend):
        """Test storing idempotency key without request hash (e.g., for DELETE)."""
        key = f"no_hash_{uuid.uuid4()}"

        metadata_db.store_idempotency_key(
            key=key,
            method="DELETE",
            endpoint="/projects/123",
            request_hash=None,
            response_status=204,
            response_body="",
            ttl_seconds=600,
        )

        result = metadata_db.get_idempotency_key(key)
        assert result is not None
        assert result["request_hash"] is None
        assert result["response_status"] == 204
        assert result["response_body"] == ""

    def test_idempotency_key_timestamps(self, initialized_backend):
        """Test that created_at and expires_at are set correctly."""
        key = f"timestamp_test_{uuid.uuid4()}"
        ttl = 600

        before = datetime.now(timezone.utc)
        metadata_db.store_idempotency_key(
            key=key,
            method="POST",
            endpoint="/test",
            request_hash=None,
            response_status=200,
            response_body="{}",
            ttl_seconds=ttl,
        )
        after = datetime.now(timezone.utc)

        result = metadata_db.get_idempotency_key(key)
        assert result is not None

        created_at = datetime.fromisoformat(result["created_at"])
        expires_at = datetime.fromisoformat(result["expires_at"])

        # Created_at should be between before and after
        assert before <= created_at <= after

        # Expires_at should be created_at + ttl
        expected_expiry = created_at + timedelta(seconds=ttl)
        # Allow 1 second tolerance
        assert abs((expires_at - expected_expiry).total_seconds()) < 1


class TestIdempotencyMiddleware:
    """Test idempotency middleware HTTP behavior."""

    def test_cache_hit_returns_same_response(self, client, initialized_backend, admin_headers):
        """First request caches, second returns cached response."""
        idempotency_key = f"test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        # First request - creates project
        response1 = client.post(
            "/projects",
            json={"id": "idem_test_1", "name": "Idempotency Test"},
            headers=headers,
        )
        assert response1.status_code == 201
        project_id = response1.json()["id"]
        assert project_id == "idem_test_1"

        # Verify no replay header on first request
        assert "X-Idempotency-Replay" not in response1.headers

        # Second request with same key - should return cached response
        response2 = client.post(
            "/projects",
            json={"id": "idem_test_1", "name": "Idempotency Test"},
            headers=headers,
        )
        assert response2.status_code == 201
        assert response2.json()["id"] == project_id
        assert response2.headers.get("X-Idempotency-Replay") == "true"

    def test_get_request_not_cached(self, client, initialized_backend, admin_headers):
        """GET requests bypass idempotency middleware."""
        idempotency_key = f"get_test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        response = client.get("/health", headers=headers)
        assert response.status_code == 200
        # No replay header for GET
        assert "X-Idempotency-Replay" not in response.headers

    def test_no_key_executes_normally(self, client, initialized_backend, admin_headers):
        """Request without idempotency key executes normally."""
        # Create first project
        response1 = client.post(
            "/projects",
            json={"id": "no_key_1", "name": "Test 1"},
            headers=admin_headers,
        )
        assert response1.status_code == 201

        # Create second project (same request, no idempotency key)
        response2 = client.post(
            "/projects",
            json={"id": "no_key_2", "name": "Test 2"},
            headers=admin_headers,
        )
        assert response2.status_code == 201
        # Different IDs - both executed
        assert response1.json()["id"] != response2.json()["id"]

    def test_method_mismatch_returns_409(self, client, initialized_backend, admin_headers):
        """Using same key with different method returns 409."""
        idempotency_key = f"method_test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        # First: POST to create project
        response1 = client.post(
            "/projects",
            json={"id": "method_test", "name": "Test"},
            headers=headers,
        )
        assert response1.status_code == 201

        # Second: DELETE with same key - should fail with 409
        response2 = client.delete(
            f"/projects/{response1.json()['id']}",
            headers=headers,
        )
        assert response2.status_code == 409
        assert response2.json()["error"] == "idempotency_conflict"
        assert "POST" in response2.json()["message"]
        assert "DELETE" in response2.json()["message"]

    def test_endpoint_mismatch_returns_409(self, client, initialized_backend, admin_headers):
        """Using same key with different endpoint returns 409."""
        idempotency_key = f"endpoint_test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        # First: POST to /projects
        response1 = client.post(
            "/projects",
            json={"id": "endpoint_test", "name": "Test"},
            headers=headers,
        )
        assert response1.status_code == 201
        project_id = response1.json()["id"]

        # Get project key for bucket creation
        project_key = response1.json().get("api_key")
        bucket_headers = {
            "Authorization": f"Bearer {project_key}" if project_key else admin_headers["Authorization"],
            "X-Idempotency-Key": idempotency_key,
        }

        # Second: POST to /projects/{id}/branches/default/buckets with same key - should fail with 409
        response2 = client.post(
            f"/projects/{project_id}/branches/default/buckets",
            json={"name": "test_bucket"},
            headers=bucket_headers,
        )
        assert response2.status_code == 409
        assert response2.json()["error"] == "idempotency_conflict"
        assert "different endpoint" in response2.json()["message"]

    def test_different_keys_execute_separately(self, client, initialized_backend, admin_headers):
        """Different idempotency keys execute as separate requests."""
        key1 = f"diff_key_1_{uuid.uuid4()}"
        key2 = f"diff_key_2_{uuid.uuid4()}"

        # First request
        response1 = client.post(
            "/projects",
            json={"id": "diff_1", "name": "Test 1"},
            headers={**admin_headers, "X-Idempotency-Key": key1},
        )
        assert response1.status_code == 201

        # Second request with different key
        response2 = client.post(
            "/projects",
            json={"id": "diff_2", "name": "Test 2"},
            headers={**admin_headers, "X-Idempotency-Key": key2},
        )
        assert response2.status_code == 201

        # Both created different projects
        assert response1.json()["id"] != response2.json()["id"]

    def test_body_mismatch_returns_409(self, client, initialized_backend, admin_headers):
        """Using same key with different body returns 409."""
        idempotency_key = f"body_test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        # First: POST with body A
        response1 = client.post(
            "/projects",
            json={"id": "body_test_a", "name": "Body A"},
            headers=headers,
        )
        assert response1.status_code == 201

        # Second: POST with different body B - should fail with 409
        response2 = client.post(
            "/projects",
            json={"id": "body_test_b", "name": "Body B"},
            headers=headers,
        )
        assert response2.status_code == 409
        assert response2.json()["error"] == "idempotency_conflict"
        assert "different request body" in response2.json()["message"]

    def test_delete_request_idempotency(self, client, initialized_backend, admin_headers):
        """DELETE requests also support idempotency."""
        # Create a project first
        create_resp = client.post(
            "/projects",
            json={"id": "del_test", "name": "Delete Test"},
            headers=admin_headers,
        )
        assert create_resp.status_code == 201
        project_id = create_resp.json()["id"]

        idempotency_key = f"delete_test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        # First delete
        response1 = client.delete(f"/projects/{project_id}", headers=headers)
        assert response1.status_code == 204

        # Second delete with same key - should return cached response
        response2 = client.delete(f"/projects/{project_id}", headers=headers)
        assert response2.status_code == 204
        assert response2.headers.get("X-Idempotency-Replay") == "true"

    def test_put_request_idempotency(self, client, initialized_backend, admin_headers):
        """PUT requests support idempotency."""
        # Create a project first
        create_resp = client.post(
            "/projects",
            json={"id": "put_test", "name": "Put Test"},
            headers=admin_headers,
        )
        assert create_resp.status_code == 201
        project_id = create_resp.json()["id"]
        project_key = create_resp.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        idempotency_key = f"put_test_{uuid.uuid4()}"
        headers = {**project_headers, "X-Idempotency-Key": idempotency_key}

        # First update
        response1 = client.put(
            f"/projects/{project_id}",
            json={"name": "Updated Name"},
            headers=headers,
        )
        assert response1.status_code == 200
        assert response1.json()["name"] == "Updated Name"

        # Second update with same key - should return cached response
        response2 = client.put(
            f"/projects/{project_id}",
            json={"name": "Updated Name"},
            headers=headers,
        )
        assert response2.status_code == 200
        assert response2.json()["name"] == "Updated Name"
        assert response2.headers.get("X-Idempotency-Replay") == "true"

    def test_idempotency_with_complex_body(self, client, initialized_backend, admin_headers):
        """Test idempotency with complex JSON body."""
        idempotency_key = f"complex_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        complex_body = {
            "id": "complex_test",
            "name": "Complex Test",
            "settings": {
                "feature_flags": ["beta", "alpha"],
                "max_tables": 100,
                "metadata": {"key1": "value1", "key2": "value2"},
            },
        }

        # First request
        response1 = client.post("/projects", json=complex_body, headers=headers)
        assert response1.status_code == 201

        # Second request with identical complex body
        response2 = client.post("/projects", json=complex_body, headers=headers)
        assert response2.status_code == 201
        assert response2.headers.get("X-Idempotency-Replay") == "true"

        # Third request with slightly different body - should fail with 409
        different_body = {**complex_body, "name": "Different Name"}
        response3 = client.post("/projects", json=different_body, headers=headers)
        assert response3.status_code == 409

    def test_idempotency_preserves_response_structure(self, client, initialized_backend, admin_headers):
        """Test that cached response has the same structure as original."""
        idempotency_key = f"struct_test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        body = {"id": "struct_test", "name": "Structure Test"}

        # First request
        response1 = client.post("/projects", json=body, headers=headers)
        assert response1.status_code == 201
        data1 = response1.json()

        # Second request
        response2 = client.post("/projects", json=body, headers=headers)
        assert response2.status_code == 201
        data2 = response2.json()

        # Compare structures (note: api_key might be in response1 but not response2
        # because response2 is from cache)
        assert data1["id"] == data2["id"]
        assert data1["name"] == data2["name"]
        assert data1["status"] == data2["status"]
        assert data1["db_path"] == data2["db_path"]

    def test_idempotency_with_error_response(self, client, initialized_backend, admin_headers):
        """Test that error responses are also cached."""
        idempotency_key = f"error_test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        # Create a project
        client.post("/projects", json={"id": "error_test"}, headers=admin_headers)

        # Try to create duplicate with idempotency key
        response1 = client.post(
            "/projects",
            json={"id": "error_test"},
            headers=headers,
        )
        assert response1.status_code == 409

        # Second attempt with same key - should return cached 409
        response2 = client.post(
            "/projects",
            json={"id": "error_test"},
            headers=headers,
        )
        assert response2.status_code == 409
        assert response2.headers.get("X-Idempotency-Replay") == "true"

    def test_multiple_concurrent_keys(self, client, initialized_backend, admin_headers):
        """Test that multiple different idempotency keys work independently."""
        keys = [f"concurrent_{i}_{uuid.uuid4()}" for i in range(5)]

        # Create projects with different keys
        for i, key in enumerate(keys):
            response = client.post(
                "/projects",
                json={"id": f"concurrent_{i}", "name": f"Test {i}"},
                headers={**admin_headers, "X-Idempotency-Key": key},
            )
            assert response.status_code == 201

        # Replay each one
        for i, key in enumerate(keys):
            response = client.post(
                "/projects",
                json={"id": f"concurrent_{i}", "name": f"Test {i}"},
                headers={**admin_headers, "X-Idempotency-Key": key},
            )
            assert response.status_code == 201
            assert response.headers.get("X-Idempotency-Replay") == "true"

    def test_idempotency_key_in_response_headers(self, client, initialized_backend, admin_headers):
        """Test that idempotency key is included in response headers on replay."""
        idempotency_key = f"header_test_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        # First request - key not in response headers (original execution)
        response1 = client.post(
            "/projects",
            json={"id": "header_test", "name": "Header Test"},
            headers=headers,
        )
        assert response1.status_code == 201
        # Idempotency key not in first response headers
        assert "X-Idempotency-Key" not in response1.headers

        # Second request - key should be in response headers (replay)
        response2 = client.post(
            "/projects",
            json={"id": "header_test", "name": "Header Test"},
            headers=headers,
        )
        assert response2.status_code == 201
        # Idempotency key should be in replay response headers
        assert response2.headers.get("X-Idempotency-Key") == idempotency_key
        assert response2.headers.get("X-Idempotency-Replay") == "true"

    def test_idempotency_with_empty_body(self, client, initialized_backend, admin_headers):
        """Test idempotency with DELETE request (no body)."""
        # Create project
        create_resp = client.post(
            "/projects",
            json={"id": "empty_body_test"},
            headers=admin_headers,
        )
        assert create_resp.status_code == 201
        project_id = create_resp.json()["id"]

        idempotency_key = f"empty_body_{uuid.uuid4()}"
        headers = {**admin_headers, "X-Idempotency-Key": idempotency_key}

        # First delete
        response1 = client.delete(f"/projects/{project_id}", headers=headers)
        assert response1.status_code == 204

        # Second delete - should return cached empty response
        response2 = client.delete(f"/projects/{project_id}", headers=headers)
        assert response2.status_code == 204
        assert response2.headers.get("X-Idempotency-Replay") == "true"
