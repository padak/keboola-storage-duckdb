"""E2E tests for Snapshots workflow (ADR-004: hierarchical config + restore)."""

import time
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from src.config import settings
from src.database import metadata_db, project_db_manager
from tests.conftest import TEST_ADMIN_API_KEY


@pytest.fixture
def project_with_data(client, initialized_backend, admin_headers):
    """Create a project with a bucket and table containing data."""
    # Create project
    response = client.post(
        "/projects",
        json={"id": "snap_e2e_proj", "name": "Snapshot E2E Project"},
        headers=admin_headers,
    )
    assert response.status_code == 201
    project_key = response.json()["api_key"]
    project_headers = {"Authorization": f"Bearer {project_key}"}

    # Create bucket
    response = client.post(
        "/projects/snap_e2e_proj/branches/default/buckets",
        json={"name": "data_bucket"},
        headers=project_headers,
    )
    assert response.status_code == 201

    # Create table with primary key
    response = client.post(
        "/projects/snap_e2e_proj/branches/default/buckets/data_bucket/tables",
        json={
            "name": "users",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR"},
                {"name": "email", "type": "VARCHAR"},
                {"name": "status", "type": "VARCHAR"},
            ],
            "primary_key": ["id"],
        },
        headers=project_headers,
    )
    assert response.status_code == 201

    # Import some test data
    response = client.post(
        "/projects/snap_e2e_proj/files/prepare",
        json={"filename": "users.csv", "content_type": "text/csv"},
        headers=project_headers,
    )
    assert response.status_code == 200
    upload_key = response.json()["upload_key"]

    csv_content = "id,name,email,status\n1,Alice,alice@example.com,active\n2,Bob,bob@example.com,active\n3,Charlie,charlie@example.com,inactive"
    response = client.post(
        f"/projects/snap_e2e_proj/files/upload/{upload_key}",
        files={"file": ("users.csv", csv_content, "text/csv")},
        headers=project_headers,
    )
    assert response.status_code == 200

    response = client.post(
        "/projects/snap_e2e_proj/files",
        json={"upload_key": upload_key, "name": "users.csv"},
        headers=project_headers,
    )
    assert response.status_code == 201
    file_id = response.json()["id"]

    response = client.post(
        "/projects/snap_e2e_proj/branches/default/buckets/data_bucket/tables/users/import/file",
        json={
            "file_id": file_id,
            "format": "csv",
            "import_options": {"incremental": False},
        },
        headers=project_headers,
    )
    assert response.status_code == 200

    return {
        "project_id": "snap_e2e_proj",
        "bucket_name": "data_bucket",
        "table_name": "users",
        "project_key": project_key,
        "project_headers": project_headers,
    }


class TestManualSnapshotCreateRestore:
    """Test manual snapshot creation and data restoration workflow."""

    def test_manual_snapshot_create_restore(self, client, project_with_data):
        """Complete workflow: create table, snapshot, modify data, restore original."""
        # Verify initial data
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 3

        # Create manual snapshot before modifications
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Before data modifications",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot_id = response.json()["id"]
        assert response.json()["snapshot_type"] == "manual"
        assert response.json()["row_count"] == 3
        assert response.json()["description"] == "Before data modifications"

        # Modify table data - delete rows
        table_path = project_db_manager.get_table_path(
            project_with_data["project_id"],
            project_with_data["bucket_name"],
            project_with_data["table_name"]
        )
        conn = duckdb.connect(str(table_path))
        try:
            conn.execute("DELETE FROM main.data WHERE id IN (2, 3)")
            conn.execute("UPDATE main.data SET status = 'modified' WHERE id = 1")
            modified_count = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()[0]
        finally:
            conn.close()

        assert modified_count == 1

        # Verify modified data
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 1
        assert response.json()["rows"][0]["status"] == "modified"

        # Restore from snapshot to original location
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}/restore",
            json={},  # Restore to original location
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["row_count"] == 3
        assert response.json()["restored_to"]["bucket"] == project_with_data["bucket_name"]
        assert response.json()["restored_to"]["table"] == project_with_data["table_name"]

        # Verify original data restored
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 3

        # Verify all original rows exist with correct data
        rows = {row["id"]: row for row in response.json()["rows"]}
        assert rows[1]["name"] == "Alice"
        assert rows[1]["status"] == "active"
        assert rows[2]["name"] == "Bob"
        assert rows[3]["name"] == "Charlie"
        assert rows[3]["status"] == "inactive"


class TestSnapshotBeforeDropTable:
    """Test automatic snapshot creation before destructive operations."""

    def test_snapshot_before_drop_table(self, client, project_with_data):
        """Auto-snapshot is created when DROP TABLE is executed, then restore table."""
        # Verify drop_table trigger is enabled (system default)
        response = client.get(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["effective_config"]["auto_snapshot_triggers"]["drop_table"] is True

        # Verify no snapshots exist initially
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total"] == 0

        # Drop the table (should trigger auto-snapshot)
        response = client.delete(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 204

        # Verify table is gone
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 404

        # Verify auto-snapshot was created
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots?type=auto_predrop",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        snapshot = data["snapshots"][0]
        assert snapshot["snapshot_type"] == "auto_predrop"
        assert snapshot["row_count"] == 3
        assert "Auto-backup before DROP TABLE" in snapshot.get("description", "")

        # Restore table from auto-snapshot
        snapshot_id = snapshot["id"]
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}/restore",
            json={},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["row_count"] == 3

        # Verify table is back with all data
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 3

    def test_no_snapshot_when_drop_table_disabled(self, client, project_with_data):
        """No auto-snapshot when drop_table trigger is disabled."""
        # Disable drop_table trigger at project level
        response = client.put(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"drop_table": False}},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Drop table
        response = client.delete(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 204

        # Verify no snapshots were created
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total"] == 0


class TestSnapshotBeforeTruncate:
    """Test automatic snapshot creation before TRUNCATE (DELETE ALL ROWS).

    This tests the actual execution of auto-snapshot when truncate_table
    or delete_all_rows triggers are enabled.
    """

    def test_auto_snapshot_before_truncate_via_delete_all(self, client, project_with_data):
        """Auto-snapshot is created when DELETE ALL ROWS (truncate) is executed."""
        # 1. Enable truncate_table trigger at project level
        response = client.put(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"truncate_table": True}},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # 2. Verify trigger is enabled
        response = client.get(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is True

        # 3. Verify table has data (3 rows from fixture)
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 3

        # 4. Verify no snapshots exist initially
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        initial_snapshots = response.json()["total"]

        # 5. TRUNCATE table via DELETE with where_clause = "1=1" (deletes all)
        import json
        response = client.request(
            "DELETE",
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/rows",
            content=json.dumps({"where_clause": "1=1"}),
            headers={**project_with_data["project_headers"], "Content-Type": "application/json"},
        )
        assert response.status_code == 200
        assert response.json()["deleted_rows"] == 3
        assert response.json()["table_rows_after"] == 0

        # 6. Verify auto-snapshot was created
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == initial_snapshots + 1

        # Find the auto-snapshot
        auto_snapshots = [s for s in data["snapshots"] if "auto_" in s.get("snapshot_type", "")]
        assert len(auto_snapshots) >= 1
        snapshot = auto_snapshots[0]
        assert snapshot["row_count"] == 3  # Had 3 rows before truncate
        assert "truncate" in snapshot.get("description", "").lower() or "delete" in snapshot.get("description", "").lower()

        # 7. Verify table is empty now
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 0

        # 8. Restore from auto-snapshot
        snapshot_id = snapshot["id"]
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}/restore",
            json={},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["row_count"] == 3

        # 9. Verify data is restored
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 3

    def test_no_auto_snapshot_when_truncate_disabled(self, client, project_with_data):
        """No auto-snapshot when truncate_table trigger is disabled (default)."""
        # Verify truncate trigger is disabled by default
        response = client.get(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is False

        # Get initial snapshot count
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        initial_count = response.json()["total"]

        # Delete all rows (truncate)
        import json
        response = client.request(
            "DELETE",
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/rows",
            content=json.dumps({"where_clause": "1=1"}),
            headers={**project_with_data["project_headers"], "Content-Type": "application/json"},
        )
        assert response.status_code == 200

        # Verify no new snapshots were created
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.json()["total"] == initial_count

    def test_auto_snapshot_delete_all_rows_trigger(self, client, project_with_data):
        """Test delete_all_rows trigger specifically (separate from truncate_table)."""
        # Enable delete_all_rows trigger
        response = client.put(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            json={"auto_snapshot_triggers": {"delete_all_rows": True}},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Verify trigger is enabled
        response = client.get(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.json()["effective_config"]["auto_snapshot_triggers"]["delete_all_rows"] is True

        # Get initial snapshot count
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        initial_count = response.json()["total"]

        # Delete all rows
        import json
        response = client.request(
            "DELETE",
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/rows",
            content=json.dumps({"where_clause": "TRUE"}),  # Another way to select all
            headers={**project_with_data["project_headers"], "Content-Type": "application/json"},
        )
        assert response.status_code == 200
        assert response.json()["deleted_rows"] == 3

        # Verify auto-snapshot was created
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.json()["total"] == initial_count + 1


class TestSnapshotRetention:
    """Test snapshot retention and expiration."""

    def test_snapshot_retention(self, client, project_with_data, monkeypatch):
        """Create multiple snapshots and verify retention settings control expiration."""
        # Set custom retention: manual=30 days, auto=1 day
        # Note: Must configure BEFORE creating snapshots, as retention is set at creation time
        response = client.put(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            json={
                "retention": {"manual_days": 30, "auto_days": 1},
                "auto_snapshot_triggers": {"drop_column": True}  # Enable drop_column trigger
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Create manual snapshot
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Manual snapshot",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        manual_snapshot_id = response.json()["id"]
        manual_expires = datetime.fromisoformat(response.json()["expires_at"])

        # Verify manual snapshot expires in ~30 days
        now = datetime.now(timezone.utc)
        days_diff = (manual_expires - now).days
        assert 29 <= days_diff <= 30

        # Trigger auto-snapshot by dropping column
        response = client.delete(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/columns/status",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Get auto-snapshot
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots?type=auto_predrop_column",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        auto_snapshot = response.json()["snapshots"][0]
        auto_snapshot_id = auto_snapshot["id"]
        auto_expires = datetime.fromisoformat(auto_snapshot["expires_at"])

        # Verify auto-snapshot expires in ~1 day
        hours_diff = (auto_expires - now).total_seconds() / 3600
        assert 23 <= hours_diff <= 25  # Allow some margin for test execution time

        # Simulate expiration by directly updating expires_at in database
        past_time = datetime.now(timezone.utc) - timedelta(hours=1)

        # Update auto-snapshot to be expired
        conn = duckdb.connect(str(settings.metadata_db_path))
        try:
            conn.execute(
                "UPDATE snapshots SET expires_at = ? WHERE id = ?",
                [past_time, auto_snapshot_id]
            )
        finally:
            conn.close()

        # Run cleanup
        expired_snapshots = metadata_db.cleanup_expired_snapshots()
        assert len(expired_snapshots) == 1
        assert expired_snapshots[0]["id"] == auto_snapshot_id

        # Verify auto-snapshot is deleted
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{auto_snapshot_id}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 404

        # Verify manual snapshot still exists
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{manual_snapshot_id}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200


class TestHierarchicalSnapshotSettings:
    """Test hierarchical snapshot settings inheritance."""

    def test_hierarchical_snapshot_settings(self, client, project_with_data):
        """Verify settings inheritance: system -> project -> bucket -> table."""
        # 1. Verify system defaults (no overrides)
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/settings/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        # All should inherit from system
        assert data["effective_config"]["enabled"] is True
        assert data["effective_config"]["retention"]["manual_days"] == 90
        assert data["effective_config"]["retention"]["auto_days"] == 7
        assert data["effective_config"]["auto_snapshot_triggers"]["drop_table"] is True
        assert data["inheritance"]["enabled"] == "system"
        assert data["inheritance"]["retention.manual_days"] == "system"

        # 2. Set project-level override
        response = client.put(
            f"/projects/{project_with_data['project_id']}/settings/snapshots",
            json={
                "retention": {"manual_days": 180},
                "auto_snapshot_triggers": {"truncate_table": True}
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Verify table inherits from project
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/settings/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["effective_config"]["retention"]["manual_days"] == 180
        assert data["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is True
        assert data["inheritance"]["retention.manual_days"] == "project"
        assert data["inheritance"]["auto_snapshot_triggers.truncate_table"] == "project"

        # 3. Set bucket-level override
        response = client.put(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/settings/snapshots",
            json={
                "retention": {"auto_days": 14},
                "auto_snapshot_triggers": {"delete_all_rows": True}
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Verify table inherits from bucket
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/settings/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["effective_config"]["retention"]["manual_days"] == 180  # from project
        assert data["effective_config"]["retention"]["auto_days"] == 14  # from bucket
        assert data["effective_config"]["auto_snapshot_triggers"]["truncate_table"] is True  # from project
        assert data["effective_config"]["auto_snapshot_triggers"]["delete_all_rows"] is True  # from bucket
        assert data["inheritance"]["retention.manual_days"] == "project"
        assert data["inheritance"]["retention.auto_days"] == "bucket"
        assert data["inheritance"]["auto_snapshot_triggers.delete_all_rows"] == "bucket"

        # 4. Set table-level override
        response = client.put(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/settings/snapshots",
            json={
                "enabled": False,  # Disable snapshots for this specific table
                "retention": {"manual_days": 365}
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Verify table overrides all levels
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/settings/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["effective_config"]["enabled"] is False  # from table
        assert data["effective_config"]["retention"]["manual_days"] == 365  # from table
        assert data["effective_config"]["retention"]["auto_days"] == 14  # from bucket
        assert data["inheritance"]["enabled"] == "table"
        assert data["inheritance"]["retention.manual_days"] == "table"
        assert data["inheritance"]["retention.auto_days"] == "bucket"

        # 5. Verify snapshot creation respects table-level "enabled=False"
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


class TestSnapshotAcrossBranches:
    """Test snapshot workflow with dev branches."""

    def test_snapshot_with_branches(self, client, project_with_data):
        """Verify snapshots work properly when using dev branches."""
        # Create snapshot in main project before branching
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Before branching",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot1_id = response.json()["id"]
        assert response.json()["row_count"] == 3

        # Create dev branch
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches",
            json={"name": "feature-branch", "description": "Testing snapshot with branches"},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Modify table in main (not in branch)
        table_path = project_db_manager.get_table_path(
            project_with_data["project_id"],
            project_with_data["bucket_name"],
            project_with_data["table_name"]
        )
        conn = duckdb.connect(str(table_path))
        try:
            conn.execute("INSERT INTO main.data VALUES (4, 'David', 'david@example.com', 'active')")
            main_row_count = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()[0]
        finally:
            conn.close()
        assert main_row_count == 4

        # Create another snapshot after modification
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "After adding David",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot2_id = response.json()["id"]
        assert response.json()["row_count"] == 4

        # Verify main project has 2 snapshots
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total"] == 2

        # Restore from first snapshot (3 rows)
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot1_id}/restore",
            json={"target_table": "users_restored"},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["row_count"] == 3

        # Verify restored table has correct data
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/users_restored/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 3

        # Verify original table still has 4 rows
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 4


class TestSnapshotComplexWorkflow:
    """Test complex real-world snapshot workflows."""

    def test_snapshot_before_schema_change(self, client, project_with_data):
        """Create snapshot before schema change, then restore if needed."""
        # Create snapshot before schema change
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Before adding new column",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot_id = response.json()["id"]

        # Add new column
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}/columns",
            json={"name": "created_at", "type": "TIMESTAMP", "nullable": True},
            headers=project_with_data["project_headers"],
        )
        # Could be 200 (column added) or 201 (created)
        assert response.status_code in [200, 201]

        # Verify new schema
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/{project_with_data['table_name']}",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        columns = response.json()["columns"]
        assert len(columns) == 5  # id, name, email, status, created_at

        # Restore to new table to compare schemas
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot_id}/restore",
            json={"target_table": "users_old_schema"},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200

        # Verify old schema (without created_at)
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/users_old_schema",
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        old_columns = response.json()["columns"]
        assert len(old_columns) == 4  # id, name, email, status
        assert not any(col["name"] == "created_at" for col in old_columns)

    def test_snapshot_chain_multiple_restores(self, client, project_with_data):
        """Create multiple snapshots and restore to different points in time."""
        # Snapshot 1: Original state (3 rows)
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Snapshot 1: Original 3 users",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot1_id = response.json()["id"]

        # Add more data
        table_path = project_db_manager.get_table_path(
            project_with_data["project_id"],
            project_with_data["bucket_name"],
            project_with_data["table_name"]
        )
        conn = duckdb.connect(str(table_path))
        try:
            conn.execute("INSERT INTO main.data VALUES (4, 'David', 'david@example.com', 'active')")
            conn.execute("INSERT INTO main.data VALUES (5, 'Eve', 'eve@example.com', 'active')")
        finally:
            conn.close()

        # Snapshot 2: After adding 2 users (5 rows)
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Snapshot 2: Added David and Eve",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot2_id = response.json()["id"]
        assert response.json()["row_count"] == 5

        # Delete some data
        conn = duckdb.connect(str(table_path))
        try:
            conn.execute("DELETE FROM main.data WHERE id > 3")
        finally:
            conn.close()

        # Snapshot 3: After deletion (3 rows again)
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots",
            json={
                "bucket": project_with_data["bucket_name"],
                "table": project_with_data["table_name"],
                "description": "Snapshot 3: Deleted David and Eve",
            },
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 201
        snapshot3_id = response.json()["id"]

        # Restore to snapshot 2 (5 rows)
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot2_id}/restore",
            json={"target_table": "users_snapshot2"},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["row_count"] == 5

        # Restore to snapshot 1 (3 rows)
        response = client.post(
            f"/projects/{project_with_data['project_id']}/branches/default/snapshots/{snapshot1_id}/restore",
            json={"target_table": "users_snapshot1"},
            headers=project_with_data["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["row_count"] == 3

        # Verify all 3 versions exist
        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/users/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.json()["total_row_count"] == 3

        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/users_snapshot1/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.json()["total_row_count"] == 3

        response = client.get(
            f"/projects/{project_with_data['project_id']}/branches/default/buckets/{project_with_data['bucket_name']}/tables/users_snapshot2/preview",
            headers=project_with_data["project_headers"],
        )
        assert response.json()["total_row_count"] == 5
