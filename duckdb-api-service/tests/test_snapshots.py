"""Tests for snapshots API (ADR-004 CRUD operations and restore)."""

import pytest
from tests.conftest import TEST_ADMIN_API_KEY


@pytest.fixture
def project_with_data(client, initialized_backend, admin_headers):
    """Create a project with a bucket and table containing data."""
    # Create project
    response = client.post(
        "/projects",
        json={"id": "snap_proj", "name": "Snapshot Test Project"},
        headers=admin_headers,
    )
    assert response.status_code == 201
    project_key = response.json()["api_key"]
    project_headers = {"Authorization": f"Bearer {project_key}"}

    # Create bucket
    response = client.post(
        "/projects/snap_proj/branches/default/buckets",
        json={"name": "data_bucket"},
        headers=project_headers,
    )
    assert response.status_code == 201

    # Create table with primary key
    response = client.post(
        "/projects/snap_proj/branches/default/buckets/data_bucket/tables",
        json={
            "name": "users",
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

    # Import some test data
    # First prepare a file
    response = client.post(
        "/projects/snap_proj/files/prepare",
        json={"filename": "users.csv", "content_type": "text/csv"},
        headers=project_headers,
    )
    assert response.status_code == 200
    upload_key = response.json()["upload_key"]

    # Upload test data
    csv_content = "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n3,Charlie,charlie@example.com"
    response = client.post(
        f"/projects/snap_proj/files/upload/{upload_key}",
        files={"file": ("users.csv", csv_content, "text/csv")},
        headers=project_headers,
    )
    assert response.status_code == 200

    # Register file
    response = client.post(
        "/projects/snap_proj/files",
        json={"upload_key": upload_key, "name": "users.csv"},
        headers=project_headers,
    )
    assert response.status_code == 201
    file_id = response.json()["id"]

    # Import data
    response = client.post(
        "/projects/snap_proj/branches/default/buckets/data_bucket/tables/users/import/file",
        json={
            "file_id": file_id,
            "format": "csv",
            "import_options": {"incremental": False},
        },
        headers=project_headers,
    )
    assert response.status_code == 200

    return {
        "project_id": "snap_proj",
        "bucket_name": "data_bucket",
        "table_name": "users",
        "project_key": project_key,
        "project_headers": project_headers,
    }


class TestSnapshotCreate:
    """Test creating snapshots."""

    def test_create_manual_snapshot(self, client, project_with_data):
        """Create a manual snapshot of a table."""
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Test snapshot before update",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        data = response.json()

        assert data["id"].startswith("snap_users_")
        assert data["project_id"] == project_with_data["project_id"]
        assert data["bucket_name"] == project_with_data["bucket_name"]
        assert data["table_name"] == project_with_data["table_name"]
        assert data["snapshot_type"] == "manual"
        assert data["row_count"] == 3
        assert data["size_bytes"] > 0
        assert data["description"] == "Test snapshot before update"
        assert data["expires_at"] is not None

    def test_create_snapshot_table_not_found(self, client, project_with_data):
        """Creating snapshot of non-existent table returns 404."""
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": "nonexistent",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"

    def test_create_snapshot_when_disabled(self, client, project_with_data):
        """Creating snapshot when disabled returns 400."""
        # Disable snapshots for the table
        client.put(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/settings/snapshots",
            json={"enabled": False},
            headers=project_with_data["project_headers"],
        )

        # Try to create snapshot
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "snapshots_disabled"


class TestSnapshotList:
    """Test listing snapshots."""

    def test_list_snapshots_empty(self, client, project_with_data):
        """List snapshots returns empty list when none exist."""
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["snapshots"] == []
        assert data["total"] == 0

    def test_list_snapshots_with_data(self, client, project_with_data):
        """List snapshots returns created snapshots."""
        # Create multiple snapshots
        for i in range(3):
            response = client.post(
                f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
                json={
                    "bucket": project_with_data["bucket_name"],
                    "table": project_with_data["table_name"],
                    "description": f"Snapshot {i+1}",
                },
                headers=project_with_data["project_headers"],
            )
            assert response.status_code == 201

        # List all
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["snapshots"]) == 3
        assert data["total"] == 3

    def test_list_snapshots_filter_by_bucket(self, client, project_with_data):
        """List snapshots can filter by bucket."""
        # Create snapshot
        client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
            },
            headers=project_with_data["project_headers"],
        )

        # Filter by bucket
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots?bucket={project_with_data['bucket_name']}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["snapshots"]) == 1

        # Filter by different bucket
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots?bucket=other_bucket",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["snapshots"]) == 0

    def test_list_snapshots_pagination(self, client, project_with_data):
        """List snapshots supports pagination."""
        # Create 5 snapshots
        for i in range(5):
            client.post(
                f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
                json={
                    "bucket": project_with_data["bucket_name"],
                    "table": project_with_data["table_name"],
                },
                headers=project_with_data["project_headers"],
            )

        # Get first page
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots?limit=2&offset=0",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["snapshots"]) == 2
        assert data["total"] == 5

        # Get second page
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots?limit=2&offset=2",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["snapshots"]) == 2


class TestSnapshotGet:
    """Test getting snapshot details."""

    def test_get_snapshot_detail(self, client, project_with_data):
        """Get snapshot returns detailed information including schema."""
        # Create snapshot
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Detailed snapshot",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot_id = response.json()["id"]

        # Get detail
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == snapshot_id
        assert data["description"] == "Detailed snapshot"
        assert "schema_columns" in data
        assert len(data["schema_columns"]) == 3
        assert any(col["name"] == "id" for col in data["schema_columns"])
        assert data["primary_key"] == ["id"]

    def test_get_snapshot_not_found(self, client, project_with_data):
        """Get non-existent snapshot returns 404."""
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/snap_nonexistent_123",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "snapshot_not_found"


class TestSnapshotDelete:
    """Test deleting snapshots."""

    def test_delete_snapshot(self, client, project_with_data):
        """Delete snapshot removes it from list."""
        # Create snapshot
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot_id = response.json()["id"]

        # Delete snapshot
        response = client.delete(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 204

        # Verify deleted
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 404

    def test_delete_snapshot_not_found(self, client, project_with_data):
        """Delete non-existent snapshot returns 404."""
        response = client.delete(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/snap_nonexistent_123",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 404


class TestSnapshotRestore:
    """Test restoring from snapshots."""

    def test_restore_to_new_table(self, client, project_with_data):
        """Restore snapshot to a new table."""
        # Create snapshot
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot_id = response.json()["id"]

        # Restore to new table
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}/restore",
            json={
                "target_bucket": project_with_data["bucket_name"],
                "target_table": "users_restored",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["restored_to"]["bucket"] == project_with_data["bucket_name"]
        assert data["restored_to"]["table"] == "users_restored"
        assert data["row_count"] == 3

        # Verify table exists with data
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/users_restored/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 3

    def test_restore_to_original_location(self, client, project_with_data):
        """Restore snapshot to original location (replace existing data)."""
        # Create snapshot
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot_id = response.json()["id"]
        original_row_count = response.json()["row_count"]

        # Restore to original location (should work even when table exists)
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}/restore",
            json={},  # Defaults to original location
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["row_count"] == original_row_count
        assert response.json()["restored_to"]["bucket"] == project_with_data["bucket_name"]
        assert response.json()["restored_to"]["table"] == project_with_data["table_name"]

        # Verify data still exists
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == original_row_count

    def test_restore_to_existing_different_table_fails(self, client, project_with_data):
        """Restore to different existing table returns 409."""
        # Create second table
        client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables",
            json={
                "name": "other_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=project_with_data["project_headers"],
        )

        # Create snapshot
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
            },
            headers=project_with_data["project_headers"],
        )
        snapshot_id = response.json()["id"]

        # Try to restore to existing different table
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}/restore",
            json={
                "target_bucket": project_with_data["bucket_name"],
                "target_table": "other_table",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "table_exists"

    def test_restore_snapshot_not_found(self, client, project_with_data):
        """Restore non-existent snapshot returns 404."""
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/snap_nonexistent_123/restore",
            json={},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 404


class TestAutoSnapshot:
    """Test automatic snapshot creation on destructive operations."""

    def test_auto_snapshot_on_drop_table(self, client, project_with_data):
        """Auto-snapshot is created before DROP TABLE (default enabled)."""
        # Verify snapshots list is empty
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total"] == 0

        # Delete table (should trigger auto-snapshot)
        response = client.delete(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 204

        # Check auto-snapshot was created
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots?type=auto_predrop",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["snapshots"][0]["snapshot_type"] == "auto_predrop"
        assert "Auto-backup before DROP TABLE" in data["snapshots"][0].get("description", "")

    def test_no_auto_snapshot_when_disabled(self, client, project_with_data):
        """No auto-snapshot when drop_table trigger is disabled."""
        # Disable drop_table trigger
        client.put(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"drop_table": False}},
            headers=project_with_data["project_headers"],
        )

        # Delete table
        response = client.delete(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 204

        # Verify no snapshots created
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total"] == 0

    def test_auto_snapshot_on_drop_column_when_enabled(self, client, project_with_data):
        """Auto-snapshot on DROP COLUMN when trigger is enabled."""
        # Enable drop_column trigger
        client.put(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"drop_column": True}},
            headers=project_with_data["project_headers"],
        )

        # Drop a column
        response = client.delete(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/columns/email",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Check auto-snapshot was created
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots?type=auto_predrop_column",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["snapshots"][0]["snapshot_type"] == "auto_predrop_column"
