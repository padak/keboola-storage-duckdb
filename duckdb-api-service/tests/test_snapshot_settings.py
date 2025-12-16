"""Tests for snapshot settings API (ADR-004 hierarchical configuration)."""

import pytest
from tests.conftest import TEST_ADMIN_API_KEY


@pytest.fixture
def project_with_table(client, initialized_backend, admin_headers):
    """Create a project with a bucket and table for testing."""
    # Create project
    response = client.post(
        "/projects",
        json={"id": "test_proj", "name": "Test Project"},
        headers=admin_headers,
    )
    assert response.status_code == 201
    project_key = response.json()["api_key"]

    # Create bucket
    project_headers = {"Authorization": f"Bearer {project_key}"}
    response = client.post(
        "/projects/test_proj/buckets",
        json={"name": "test_bucket"},
        headers=project_headers,
    )
    assert response.status_code == 201

    # Create table
    response = client.post(
        "/projects/test_proj/buckets/test_bucket/tables",
        json={
            "name": "test_table",
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR"},
            ],
        },
        headers=project_headers,
    )
    assert response.status_code == 201

    return {
        "project_id": "test_proj",
        "bucket_name": "test_bucket",
        "table_name": "test_table",
        "project_key": project_key,
        "project_headers": project_headers,
    }


class TestSnapshotSettingsSystemDefaults:
    """Test system default configuration."""

    def test_get_project_settings_returns_system_defaults(
        self, client, project_with_table
    ):
        """GET project settings returns system defaults when no local config."""
        response = client.get(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        # Check effective config has system defaults
        assert data["effective_config"]["enabled"] is True
        assert data["effective_config"]["auto_snapshot_triggers"]["drop_table"] is True
        assert data["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is False
        assert data["effective_config"]["auto_snapshot_triggers"]["delete_all_rows"] is False
        assert data["effective_config"]["auto_snapshot_triggers"]["drop_column"] is False
        assert data["effective_config"]["retention"]["manual_days"] == 90
        assert data["effective_config"]["retention"]["auto_days"] == 7

        # Check inheritance shows all from system
        assert data["inheritance"]["enabled"] == "system"
        assert data["inheritance"]["auto_snapshot_triggers.drop_table"] == "system"
        assert data["inheritance"]["retention.manual_days"] == "system"

        # No local config should be set
        assert data["local_config"] is None


class TestSnapshotSettingsProjectLevel:
    """Test project-level settings."""

    def test_set_project_settings(self, client, project_with_table):
        """PUT project settings updates configuration."""
        response = client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json={
                "auto_snapshot_triggers": {"truncate_table": True},
                "retention": {"manual_days": 180},
            },
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        # Check effective config has updated values
        assert data["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is True
        assert data["effective_config"]["retention"]["manual_days"] == 180

        # Other values should still be system defaults
        assert data["effective_config"]["auto_snapshot_triggers"]["drop_table"] is True
        assert data["effective_config"]["retention"]["auto_days"] == 7

        # Check inheritance shows project for changed values
        assert data["inheritance"]["auto_snapshot_triggers.truncate_table"] == "project"
        assert data["inheritance"]["retention.manual_days"] == "project"
        assert data["inheritance"]["auto_snapshot_triggers.drop_table"] == "system"

        # Local config should have only set values
        assert data["local_config"]["auto_snapshot_triggers"]["truncate_table"] is True
        assert data["local_config"]["retention"]["manual_days"] == 180

    def test_delete_project_settings_resets_to_system_defaults(
        self, client, project_with_table
    ):
        """DELETE project settings resets to system defaults."""
        # First set some settings
        client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json={"enabled": False},
            headers=project_with_table["project_headers"],
        )

        # Delete settings
        response = client.delete(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 204

        # Verify reset to defaults
        response = client.get(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["effective_config"]["enabled"] is True
        assert data["local_config"] is None


class TestSnapshotSettingsBucketLevel:
    """Test bucket-level settings with inheritance from project."""

    def test_bucket_inherits_from_project(self, client, project_with_table):
        """Bucket settings inherit from project."""
        # Set project settings
        client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"truncate_table": True}},
            headers=project_with_table["project_headers"],
        )

        # Get bucket settings (should inherit)
        response = client.get(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/settings/snapshots",
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is True
        assert data["inheritance"]["auto_snapshot_triggers.truncate_table"] == "project"
        assert data["local_config"] is None

    def test_bucket_overrides_project(self, client, project_with_table):
        """Bucket settings can override project settings."""
        # Set project settings
        client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"truncate_table": True}},
            headers=project_with_table["project_headers"],
        )

        # Set bucket settings to override
        response = client.put(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"truncate_table": False}},
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        # Bucket should override project
        assert data["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is False
        assert data["inheritance"]["auto_snapshot_triggers.truncate_table"] == "bucket"

    def test_bucket_disables_snapshots(self, client, project_with_table):
        """Bucket can disable snapshots entirely."""
        response = client.put(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/settings/snapshots",
            json={"enabled": False},
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["effective_config"]["enabled"] is False
        assert data["inheritance"]["enabled"] == "bucket"


class TestSnapshotSettingsTableLevel:
    """Test table-level settings with full inheritance chain."""

    def test_table_inherits_full_chain(self, client, project_with_table):
        """Table inherits from bucket which inherits from project."""
        # Set project settings
        client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json={"retention": {"manual_days": 180}},
            headers=project_with_table["project_headers"],
        )

        # Set bucket settings
        client.put(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"truncate_table": True}},
            headers=project_with_table["project_headers"],
        )

        # Get table settings (should inherit from both)
        response = client.get(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/settings/snapshots",
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        # Check inheritance chain
        assert data["effective_config"]["retention"]["manual_days"] == 180
        assert data["inheritance"]["retention.manual_days"] == "project"

        assert data["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is True
        assert data["inheritance"]["auto_snapshot_triggers.truncate_table"] == "bucket"

        assert data["effective_config"]["enabled"] is True
        assert data["inheritance"]["enabled"] == "system"

    def test_table_overrides_bucket(self, client, project_with_table):
        """Table can override bucket settings."""
        # Set bucket to disable snapshots
        client.put(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/settings/snapshots",
            json={"enabled": False},
            headers=project_with_table["project_headers"],
        )

        # Table overrides to enable
        response = client.put(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/{project_with_table['table_name']}/settings/snapshots",
            json={"enabled": True, "retention": {"auto_days": 30}},
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["effective_config"]["enabled"] is True
        assert data["inheritance"]["enabled"] == "table"
        assert data["effective_config"]["retention"]["auto_days"] == 30
        assert data["inheritance"]["retention.auto_days"] == "table"


class TestSnapshotSettingsValidation:
    """Test configuration validation."""

    def test_invalid_trigger_name_rejected(self, client, project_with_table):
        """Invalid trigger names are rejected by Pydantic."""
        response = client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"invalid_trigger": True}},
            headers=project_with_table["project_headers"],
        )
        # Pydantic returns 422 for extra fields
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert any("invalid_trigger" in str(err) for err in detail)

    def test_negative_retention_rejected(self, client, project_with_table):
        """Negative retention days are rejected."""
        response = client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json={"retention": {"manual_days": -1}},
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["error"] == "invalid_config"
        assert "positive integer" in str(detail["details"]["errors"])

    def test_excessive_retention_rejected(self, client, project_with_table):
        """Excessive retention (>10 years) is rejected."""
        response = client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json={"retention": {"manual_days": 5000}},
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["error"] == "invalid_config"
        assert "exceed 3650" in str(detail["details"]["errors"])


class TestSnapshotSettingsNotFound:
    """Test 404 errors for missing entities."""

    def test_project_not_found(self, client, initialized_backend, admin_headers):
        """GET settings for non-existent project returns 404."""
        response = client.get(
            "/projects/nonexistent/settings/snapshots",
            headers=admin_headers,
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_bucket_not_found(self, client, project_with_table):
        """GET settings for non-existent bucket returns 404."""
        response = client.get(
            f"/projects/{project_with_table['project_id']}/buckets/nonexistent/settings/snapshots",
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_table_not_found(self, client, project_with_table):
        """GET settings for non-existent table returns 404."""
        response = client.get(
            f"/projects/{project_with_table['project_id']}/buckets/{project_with_table['bucket_name']}/tables/nonexistent/settings/snapshots",
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"


class TestSnapshotSettingsIdempotency:
    """Test idempotent operations."""

    def test_put_same_config_is_idempotent(self, client, project_with_table):
        """PUT with same config is idempotent."""
        config = {"auto_snapshot_triggers": {"truncate_table": True}}

        # First PUT
        response1 = client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json=config,
            headers=project_with_table["project_headers"],
        )
        assert response1.status_code == 200

        # Second PUT with same config
        response2 = client.put(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            json=config,
            headers=project_with_table["project_headers"],
        )
        assert response2.status_code == 200

        # Should have same effective config
        assert response1.json()["effective_config"] == response2.json()["effective_config"]

    def test_delete_nonexistent_settings_is_204(self, client, project_with_table):
        """DELETE settings that don't exist returns 204 (no content)."""
        response = client.delete(
            f"/projects/{project_with_table['project_id']}/settings/snapshots",
            headers=project_with_table["project_headers"],
        )
        assert response.status_code == 204
