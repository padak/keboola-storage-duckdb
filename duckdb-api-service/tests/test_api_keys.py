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
