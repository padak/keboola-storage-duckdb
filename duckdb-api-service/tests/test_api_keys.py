"""Test API key storage in database layer."""

import hashlib
import uuid

import pytest

from src.database import metadata_db


class TestAPIKeyStorage:
    """Test API key CRUD operations in metadata database."""

    def test_create_api_key(self, initialized_backend):
        """Test creating an API key."""
        # Create a project first
        project = metadata_db.create_project("test_proj_1", "Test Project")

        # Create API key
        key_id = str(uuid.uuid4())
        key_hash = hashlib.sha256(b"proj_123_admin_secret123").hexdigest()
        key_prefix = "proj_123_admin_secret12"  # First ~23 chars

        result = metadata_db.create_api_key(
            key_id=key_id,
            project_id=project["id"],
            key_hash=key_hash,
            key_prefix=key_prefix,
            description="Test API key",
        )

        assert result["id"] == key_id
        assert result["project_id"] == project["id"]
        assert result["key_prefix"] == key_prefix
        assert result["description"] == "Test API key"
        assert result["created_at"] is not None
        assert result["last_used_at"] is None
        # key_hash should not be in the returned dict
        assert "key_hash" not in result

    def test_get_api_key_by_prefix(self, initialized_backend):
        """Test finding API key by prefix."""
        # Create project and API key
        project = metadata_db.create_project("test_proj_2", "Test Project")
        key_id = str(uuid.uuid4())
        key_hash = hashlib.sha256(b"proj_456_admin_secret456").hexdigest()
        key_prefix = "proj_456_admin_secret45"

        metadata_db.create_api_key(
            key_id=key_id,
            project_id=project["id"],
            key_hash=key_hash,
            key_prefix=key_prefix,
        )

        # Find by prefix
        found = metadata_db.get_api_key_by_prefix(key_prefix)

        assert found is not None
        assert found["id"] == key_id
        assert found["project_id"] == project["id"]
        assert found["key_hash"] == key_hash
        assert found["key_prefix"] == key_prefix

    def test_get_api_key_by_prefix_not_found(self, initialized_backend):
        """Test finding non-existent API key returns None."""
        result = metadata_db.get_api_key_by_prefix("nonexistent_key_prefix")
        assert result is None

    def test_get_api_keys_for_project(self, initialized_backend):
        """Test listing all API keys for a project."""
        # Create project
        project = metadata_db.create_project("test_proj_3", "Test Project")

        # Create multiple API keys
        key1_id = str(uuid.uuid4())
        key2_id = str(uuid.uuid4())

        metadata_db.create_api_key(
            key_id=key1_id,
            project_id=project["id"],
            key_hash=hashlib.sha256(b"key1").hexdigest(),
            key_prefix="proj_3_admin_key1",
            description="First key",
        )

        metadata_db.create_api_key(
            key_id=key2_id,
            project_id=project["id"],
            key_hash=hashlib.sha256(b"key2").hexdigest(),
            key_prefix="proj_3_admin_key2",
            description="Second key",
        )

        # List keys
        keys = metadata_db.get_api_keys_for_project(project["id"])

        assert len(keys) == 2
        assert all("key_hash" not in key for key in keys)  # Security check
        assert keys[0]["description"] == "Second key"  # Ordered by created_at DESC
        assert keys[1]["description"] == "First key"

    def test_get_api_keys_for_project_empty(self, initialized_backend):
        """Test listing API keys for project with no keys."""
        project = metadata_db.create_project("test_proj_4", "Test Project")
        keys = metadata_db.get_api_keys_for_project(project["id"])
        assert keys == []

    def test_update_api_key_last_used(self, initialized_backend):
        """Test updating last_used_at timestamp."""
        # Create project and API key
        project = metadata_db.create_project("test_proj_5", "Test Project")
        key_id = str(uuid.uuid4())

        created = metadata_db.create_api_key(
            key_id=key_id,
            project_id=project["id"],
            key_hash=hashlib.sha256(b"key").hexdigest(),
            key_prefix="proj_5_admin_key",
        )

        assert created["last_used_at"] is None

        # Update last_used_at
        metadata_db.update_api_key_last_used(key_id)

        # Verify update
        key = metadata_db.get_api_key_by_prefix("proj_5_admin_key")
        assert key["last_used_at"] is not None

    def test_delete_api_key(self, initialized_backend):
        """Test deleting an API key."""
        # Create project and API key
        project = metadata_db.create_project("test_proj_6", "Test Project")
        key_id = str(uuid.uuid4())
        key_prefix = "proj_6_admin_key"

        metadata_db.create_api_key(
            key_id=key_id,
            project_id=project["id"],
            key_hash=hashlib.sha256(b"key").hexdigest(),
            key_prefix=key_prefix,
        )

        # Verify it exists
        assert metadata_db.get_api_key_by_prefix(key_prefix) is not None

        # Delete it
        result = metadata_db.delete_api_key(key_id)
        assert result is True

        # Verify it's gone
        assert metadata_db.get_api_key_by_prefix(key_prefix) is None

    def test_delete_project_api_keys(self, initialized_backend):
        """Test deleting all API keys for a project."""
        # Create project with multiple keys
        project = metadata_db.create_project("test_proj_7", "Test Project")

        for i in range(3):
            metadata_db.create_api_key(
                key_id=str(uuid.uuid4()),
                project_id=project["id"],
                key_hash=hashlib.sha256(f"key{i}".encode()).hexdigest(),
                key_prefix=f"proj_7_admin_key{i}",
            )

        # Verify keys exist
        assert len(metadata_db.get_api_keys_for_project(project["id"])) == 3

        # Delete all keys
        count = metadata_db.delete_project_api_keys(project["id"])
        assert count == 3

        # Verify all gone
        assert metadata_db.get_api_keys_for_project(project["id"]) == []

    def test_foreign_key_constraint(self, initialized_backend):
        """Test that API keys must reference valid projects."""
        from _duckdb import ConstraintException

        key_id = str(uuid.uuid4())

        # Attempting to create an API key with a non-existent project should fail
        with pytest.raises(ConstraintException, match="Violates foreign key constraint"):
            metadata_db.create_api_key(
                key_id=key_id,
                project_id="nonexistent_project",
                key_hash=hashlib.sha256(b"key").hexdigest(),
                key_prefix="test_key_prefix",
            )


class TestAPIKeyIndexes:
    """Test that API key indexes work correctly."""

    def test_prefix_index_lookup(self, initialized_backend):
        """Test that prefix lookups are efficient (use index)."""
        # Create project and many keys
        project = metadata_db.create_project("test_proj_idx", "Test Project")

        for i in range(10):
            metadata_db.create_api_key(
                key_id=str(uuid.uuid4()),
                project_id=project["id"],
                key_hash=hashlib.sha256(f"key{i}".encode()).hexdigest(),
                key_prefix=f"proj_idx_key_{i:03d}",
            )

        # Lookup should be fast (index scan)
        result = metadata_db.get_api_key_by_prefix("proj_idx_key_005")
        assert result is not None

    def test_project_index_lookup(self, initialized_backend):
        """Test that project-based lookups are efficient (use index)."""
        # Create multiple projects with keys
        for p in range(3):
            project = metadata_db.create_project(f"test_proj_{p}", f"Project {p}")
            for k in range(5):
                metadata_db.create_api_key(
                    key_id=str(uuid.uuid4()),
                    project_id=project["id"],
                    key_hash=hashlib.sha256(f"key{k}".encode()).hexdigest(),
                    key_prefix=f"proj_{p}_key_{k}",
                )

        # Project lookup should be fast (index scan)
        keys = metadata_db.get_api_keys_for_project("test_proj_1")
        assert len(keys) == 5


# ============================================
# API Key Router/Endpoint Tests
# ============================================


class TestAPIKeyEndpoints:
    """Test API key management REST endpoints."""

    def test_create_project_admin_key(self, client, initialized_backend, admin_headers):
        """Test creating a project_admin API key."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_1", "name": "API Key Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create additional API key
        response = client.post(
            "/projects/apikey_proj_1/api-keys",
            json={
                "description": "Secondary admin key",
                "scope": "project_admin",
            },
            headers=project_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["scope"] == "project_admin"
        assert data["description"] == "Secondary admin key"
        assert data["project_id"] == "apikey_proj_1"
        assert data["branch_id"] is None
        assert "api_key" in data  # Full key returned
        assert data["api_key"].startswith("proj_apikey_proj_1_admin_")

    def test_create_branch_admin_key(self, client, initialized_backend, admin_headers):
        """Test creating a branch_admin API key."""
        # Create project and branch
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_2", "name": "Branch Key Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create branch
        response = client.post(
            "/projects/apikey_proj_2/branches",
            json={"name": "feature-x", "description": "Feature branch"},
            headers=project_headers,
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Create branch admin key
        response = client.post(
            "/projects/apikey_proj_2/api-keys",
            json={
                "description": "Feature X admin key",
                "scope": "branch_admin",
                "branch_id": branch_id,
            },
            headers=project_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["scope"] == "branch_admin"
        assert data["branch_id"] == branch_id
        assert data["api_key"].startswith(f"proj_apikey_proj_2_branch_{branch_id}_admin_")

    def test_create_branch_read_key(self, client, initialized_backend, admin_headers):
        """Test creating a branch_read API key."""
        # Create project and branch
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_3", "name": "Read Key Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create branch
        response = client.post(
            "/projects/apikey_proj_3/branches",
            json={"name": "staging", "description": "Staging branch"},
            headers=project_headers,
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Create branch read-only key
        response = client.post(
            "/projects/apikey_proj_3/api-keys",
            json={
                "description": "Staging read-only key",
                "scope": "branch_read",
                "branch_id": branch_id,
            },
            headers=project_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["scope"] == "branch_read"
        assert data["branch_id"] == branch_id
        assert data["api_key"].startswith(f"proj_apikey_proj_3_branch_{branch_id}_read_")

    def test_create_key_with_expiration(self, client, initialized_backend, admin_headers):
        """Test creating an API key with expiration."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_4", "name": "Expiration Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create key with 30-day expiration
        response = client.post(
            "/projects/apikey_proj_4/api-keys",
            json={
                "description": "Temporary key",
                "scope": "project_admin",
                "expires_in_days": 30,
            },
            headers=project_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["expires_at"] is not None
        # Verify expiration is roughly 30 days from now
        from datetime import datetime, timezone
        expires = datetime.fromisoformat(data["expires_at"])
        now = datetime.now(timezone.utc)
        delta = (expires - now).days
        assert 29 <= delta <= 31  # Allow small variance

    def test_create_branch_key_without_branch_id_fails(self, client, initialized_backend, admin_headers):
        """Test that creating branch-scoped key without branch_id fails."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_5", "name": "Validation Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Try to create branch key without branch_id
        response = client.post(
            "/projects/apikey_proj_5/api-keys",
            json={
                "description": "Invalid branch key",
                "scope": "branch_admin",
                # branch_id missing
            },
            headers=project_headers,
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert "branch_id is required" in detail["message"]

    def test_create_project_key_with_branch_id_fails(self, client, initialized_backend, admin_headers):
        """Test that creating project_admin key with branch_id fails."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_6", "name": "Validation Test 2"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Try to create project_admin key with branch_id
        response = client.post(
            "/projects/apikey_proj_6/api-keys",
            json={
                "description": "Invalid project key",
                "scope": "project_admin",
                "branch_id": "some_branch",
            },
            headers=project_headers,
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert "branch_id must be null" in detail["message"]

    def test_list_api_keys(self, client, initialized_backend, admin_headers):
        """Test listing API keys."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_7", "name": "List Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create multiple keys
        for i in range(3):
            response = client.post(
                "/projects/apikey_proj_7/api-keys",
                json={
                    "description": f"Key {i}",
                    "scope": "project_admin",
                },
                headers=project_headers,
            )
            assert response.status_code == 201

        # List keys
        response = client.get(
            "/projects/apikey_proj_7/api-keys",
            headers=project_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["count"] >= 3  # At least 3 created + initial project key
        assert len(data["api_keys"]) >= 3
        # Verify no full keys returned
        for key in data["api_keys"]:
            assert "api_key" not in key
            assert "key_prefix" in key
            assert "scope" in key

    def test_get_api_key_details(self, client, initialized_backend, admin_headers):
        """Test getting API key details."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_8", "name": "Get Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create key
        response = client.post(
            "/projects/apikey_proj_8/api-keys",
            json={
                "description": "Test key for details",
                "scope": "project_admin",
            },
            headers=project_headers,
        )
        assert response.status_code == 201
        key_id = response.json()["id"]

        # Get details
        response = client.get(
            f"/projects/apikey_proj_8/api-keys/{key_id}",
            headers=project_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == key_id
        assert data["description"] == "Test key for details"
        assert "api_key" not in data  # Full key not returned

    def test_revoke_api_key(self, client, initialized_backend, admin_headers):
        """Test revoking an API key."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_9", "name": "Revoke Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create key
        response = client.post(
            "/projects/apikey_proj_9/api-keys",
            json={
                "description": "Key to revoke",
                "scope": "project_admin",
            },
            headers=project_headers,
        )
        assert response.status_code == 201
        key_id = response.json()["id"]

        # Revoke it
        response = client.delete(
            f"/projects/apikey_proj_9/api-keys/{key_id}",
            headers=project_headers,
        )

        assert response.status_code == 204

        # Verify it's not in active list
        response = client.get(
            "/projects/apikey_proj_9/api-keys",
            headers=project_headers,
        )
        assert response.status_code == 200
        keys = response.json()["api_keys"]
        assert not any(k["id"] == key_id for k in keys)

    def test_cannot_revoke_last_project_admin_key(self, client, initialized_backend, admin_headers):
        """Test that the last project_admin key cannot be revoked."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_10", "name": "Last Key Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Get the initial project key ID
        response = client.get(
            "/projects/apikey_proj_10/api-keys",
            headers=project_headers,
        )
        assert response.status_code == 200
        keys = response.json()["api_keys"]
        initial_key_id = keys[0]["id"]

        # Try to revoke the only project_admin key
        response = client.delete(
            f"/projects/apikey_proj_10/api-keys/{initial_key_id}",
            headers=project_headers,
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert "cannot_revoke_last_admin_key" in detail["error"]

    def test_rotate_api_key(self, client, initialized_backend, admin_headers):
        """Test rotating an API key."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_11", "name": "Rotate Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create key
        response = client.post(
            "/projects/apikey_proj_11/api-keys",
            json={
                "description": "Original key",
                "scope": "project_admin",
            },
            headers=project_headers,
        )
        assert response.status_code == 201
        old_key_id = response.json()["id"]
        old_api_key = response.json()["api_key"]

        # Rotate it
        response = client.post(
            f"/projects/apikey_proj_11/api-keys/{old_key_id}/rotate",
            headers=project_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["id"] != old_key_id  # New ID
        assert data["api_key"] != old_api_key  # New key
        assert data["description"] == "Original key (rotated)"
        assert data["scope"] == "project_admin"

        # Verify old key is revoked
        response = client.get(
            "/projects/apikey_proj_11/api-keys",
            headers=project_headers,
        )
        assert response.status_code == 200
        keys = response.json()["api_keys"]
        assert not any(k["id"] == old_key_id for k in keys)  # Old key not in list

    def test_rotate_preserves_expiration_ttl(self, client, initialized_backend, admin_headers):
        """Test that rotation preserves the original TTL."""
        # Create project
        response = client.post(
            "/projects",
            json={"id": "apikey_proj_12", "name": "Rotate TTL Test"},
            headers=admin_headers,
        )
        assert response.status_code == 201
        project_key = response.json()["api_key"]
        project_headers = {"Authorization": f"Bearer {project_key}"}

        # Create key with 60-day expiration
        response = client.post(
            "/projects/apikey_proj_12/api-keys",
            json={
                "description": "Temporary key",
                "scope": "project_admin",
                "expires_in_days": 60,
            },
            headers=project_headers,
        )
        assert response.status_code == 201
        old_key_id = response.json()["id"]

        # Rotate it
        response = client.post(
            f"/projects/apikey_proj_12/api-keys/{old_key_id}/rotate",
            headers=project_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["expires_at"] is not None
        # Verify new expiration is roughly 60 days from now
        from datetime import datetime, timezone
        expires = datetime.fromisoformat(data["expires_at"])
        now = datetime.now(timezone.utc)
        delta = (expires - now).days
        assert 58 <= delta <= 61  # Allow small variance (can be 58-61 due to timing)
