"""Tests for dev branches API (ADR-007: CoW branching)."""

import pytest
from pathlib import Path
from tests.conftest import TEST_ADMIN_API_KEY


@pytest.fixture
def project_with_tables(client, initialized_backend, admin_headers):
    """Create a project with buckets and tables containing data."""
    # Create project
    response = client.post(
        "/projects",
        json={"id": "branch_proj", "name": "Branch Test Project"},
        headers=admin_headers,
    )
    assert response.status_code == 201
    project_key = response.json()["api_key"]
    project_headers = {"Authorization": f"Bearer {project_key}"}

    # Create bucket
    response = client.post(
        "/projects/branch_proj/buckets",
        json={"name": "in_c_sales"},
        headers=project_headers,
    )
    assert response.status_code == 201

    # Create first table with data
    response = client.post(
        "/projects/branch_proj/buckets/in_c_sales/tables",
        json={
            "name": "orders",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "customer", "type": "VARCHAR"},
                {"name": "amount", "type": "DECIMAL(10,2)"},
            ],
            "primary_key": ["id"],
        },
        headers=project_headers,
    )
    assert response.status_code == 201

    # Create second table
    response = client.post(
        "/projects/branch_proj/buckets/in_c_sales/tables",
        json={
            "name": "customers",
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

    # Import data into orders table
    response = client.post(
        "/projects/branch_proj/files/prepare",
        json={"filename": "orders.csv", "content_type": "text/csv"},
        headers=project_headers,
    )
    assert response.status_code == 200
    upload_key = response.json()["upload_key"]

    csv_content = "id,customer,amount\n1,Alice,100.50\n2,Bob,250.00\n3,Charlie,75.25"
    response = client.post(
        f"/projects/branch_proj/files/upload/{upload_key}",
        files={"file": ("orders.csv", csv_content, "text/csv")},
        headers=project_headers,
    )
    assert response.status_code == 200

    response = client.post(
        "/projects/branch_proj/files",
        json={"upload_key": upload_key, "name": "orders.csv"},
        headers=project_headers,
    )
    assert response.status_code == 201
    file_id = response.json()["id"]

    response = client.post(
        "/projects/branch_proj/buckets/in_c_sales/tables/orders/import/file",
        json={
            "file_id": file_id,
            "format": "csv",
            "import_options": {"incremental": False},
        },
        headers=project_headers,
    )
    assert response.status_code == 200

    return {
        "project_id": "branch_proj",
        "bucket_name": "in_c_sales",
        "project_key": project_key,
        "project_headers": project_headers,
    }


class TestBranchCreate:
    """Test creating dev branches."""

    def test_create_branch(self, client, project_with_tables):
        """Create a new dev branch."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "feature-new-report", "description": "Testing new report logic"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        data = response.json()

        assert data["id"] is not None
        assert len(data["id"]) == 8  # Short UUID
        assert data["project_id"] == project_with_tables["project_id"]
        assert data["name"] == "feature-new-report"
        assert data["description"] == "Testing new report logic"
        assert data["created_at"] is not None
        assert data["table_count"] == 0  # Starts empty (CoW)
        assert data["size_bytes"] == 0

    def test_create_branch_minimal(self, client, project_with_tables):
        """Create a branch with only required fields."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "quick-fix"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        data = response.json()

        assert data["name"] == "quick-fix"
        assert data["description"] is None

    def test_create_branch_duplicate_name(self, client, project_with_tables):
        """Creating branch with duplicate name returns 409."""
        # Create first branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "same-name"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201

        # Try to create second branch with same name
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "same-name"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "branch_name_exists"

    def test_create_branch_project_not_found(self, client, initialized_backend, admin_headers):
        """Creating branch in non-existent project returns 404 (with admin key)."""
        # Note: With admin key, we can access any project endpoint
        # The auth check passes, then the project existence check fails
        response = client.post(
            "/projects/nonexistent/branches",
            json={"name": "test-branch"},
            headers=admin_headers,
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_create_branch_unauthorized(self, client, project_with_tables):
        """Creating branch without auth returns 401."""
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "unauth-branch"},
        )
        assert response.status_code == 401


class TestBranchList:
    """Test listing dev branches."""

    def test_list_branches_empty(self, client, project_with_tables):
        """List branches when none exist."""
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/branches",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["branches"] == []
        assert data["count"] == 0

    def test_list_branches(self, client, project_with_tables):
        """List branches after creating some."""
        # Create branches
        for name in ["branch-a", "branch-b", "branch-c"]:
            response = client.post(
                f"/projects/{project_with_tables['project_id']}/branches",
                json={"name": name},
                headers=project_with_tables["project_headers"],
            )
            assert response.status_code == 201

        # List branches
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/branches",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["count"] == 3
        assert len(data["branches"]) == 3

        # Should be sorted by created_at DESC
        names = [b["name"] for b in data["branches"]]
        assert "branch-a" in names
        assert "branch-b" in names
        assert "branch-c" in names

    def test_list_branches_pagination(self, client, project_with_tables):
        """Test branch list pagination."""
        # Create 5 branches
        for i in range(5):
            response = client.post(
                f"/projects/{project_with_tables['project_id']}/branches",
                json={"name": f"branch-{i}"},
                headers=project_with_tables["project_headers"],
            )
            assert response.status_code == 201

        # Get first page
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/branches?limit=2&offset=0",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2

        # Get second page
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/branches?limit=2&offset=2",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2


class TestBranchGet:
    """Test getting branch details."""

    def test_get_branch(self, client, project_with_tables):
        """Get branch details."""
        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "detail-test", "description": "Branch for testing details"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Get branch
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/branches/{branch_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == branch_id
        assert data["name"] == "detail-test"
        assert data["description"] == "Branch for testing details"
        assert data["copied_tables"] == []  # No CoW yet
        assert data["table_count"] == 0
        assert data["size_bytes"] == 0

    def test_get_branch_not_found(self, client, project_with_tables):
        """Getting non-existent branch returns 404."""
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/branches/nonexistent",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "branch_not_found"


class TestBranchDelete:
    """Test deleting dev branches."""

    def test_delete_branch(self, client, project_with_tables, initialized_backend):
        """Delete a branch."""
        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "to-delete"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Verify branch exists
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/branches/{branch_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200

        # Delete branch
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/branches/{branch_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 204

        # Verify branch is gone
        response = client.get(
            f"/projects/{project_with_tables['project_id']}/branches/{branch_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404

    def test_delete_branch_not_found(self, client, project_with_tables):
        """Deleting non-existent branch returns 404."""
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/branches/nonexistent",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404


class TestBranchDirectoryStructure:
    """Test branch directory structure (ADR-009 + ADR-007)."""

    def test_branch_creates_empty_directory(
        self, client, project_with_tables, initialized_backend
    ):
        """Creating branch creates empty directory."""
        from src.config import settings

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "dir-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Check directory exists
        branch_dir = (
            settings.duckdb_dir
            / f"project_{project_with_tables['project_id']}_branch_{branch_id}"
        )
        assert branch_dir.exists()
        assert branch_dir.is_dir()

        # Directory should be empty (no CoW yet)
        contents = list(branch_dir.iterdir())
        assert len(contents) == 0

    def test_delete_branch_removes_directory(
        self, client, project_with_tables, initialized_backend
    ):
        """Deleting branch removes its directory."""
        from src.config import settings

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "dir-delete-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        branch_dir = (
            settings.duckdb_dir
            / f"project_{project_with_tables['project_id']}_branch_{branch_id}"
        )
        assert branch_dir.exists()

        # Delete branch
        response = client.delete(
            f"/projects/{project_with_tables['project_id']}/branches/{branch_id}",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 204

        # Directory should be gone
        assert not branch_dir.exists()


class TestPullTable:
    """Test pulling (refreshing) tables from main."""

    def test_pull_table_not_in_branch(self, client, project_with_tables):
        """Pull table that was never copied to branch."""
        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "pull-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Pull table (not in branch yet)
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches/{branch_id}/tables/{project_with_tables['bucket_name']}/orders/pull",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["bucket_name"] == project_with_tables["bucket_name"]
        assert data["table_name"] == "orders"
        assert data["was_local"] is False
        assert "already reading from main" in data["message"]

    def test_pull_table_not_found(self, client, project_with_tables):
        """Pull non-existent table returns 404."""
        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "pull-404-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Pull non-existent table
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches/{branch_id}/tables/{project_with_tables['bucket_name']}/nonexistent/pull",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "table_not_found"


class TestBranchMetadata:
    """Test branch metadata operations."""

    def test_branch_table_tracking(self, client, project_with_tables, initialized_backend):
        """Test that branch tables are properly tracked in metadata."""
        from src.database import metadata_db

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "tracking-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Initially no tables in branch
        tables = metadata_db.get_branch_tables(branch_id)
        assert len(tables) == 0

        # Manually mark a table as copied (simulating CoW)
        metadata_db.mark_table_copied_to_branch(
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )

        # Now table should be tracked
        tables = metadata_db.get_branch_tables(branch_id)
        assert len(tables) == 1
        assert tables[0]["bucket_name"] == project_with_tables["bucket_name"]
        assert tables[0]["table_name"] == "orders"

        # Check is_table_in_branch
        assert metadata_db.is_table_in_branch(
            branch_id, project_with_tables["bucket_name"], "orders"
        )
        assert not metadata_db.is_table_in_branch(
            branch_id, project_with_tables["bucket_name"], "customers"
        )

    def test_branch_count(self, client, project_with_tables, initialized_backend):
        """Test branch counting."""
        from src.database import metadata_db

        initial_count = metadata_db.count_branches(project_with_tables["project_id"])

        # Create branches
        for name in ["count-a", "count-b"]:
            response = client.post(
                f"/projects/{project_with_tables['project_id']}/branches",
                json={"name": name},
                headers=project_with_tables["project_headers"],
            )
            assert response.status_code == 201

        # Count should increase
        new_count = metadata_db.count_branches(project_with_tables["project_id"])
        assert new_count == initial_count + 2


class TestBranchCopyOnWrite:
    """Test Copy-on-Write functionality."""

    def test_cow_copies_table_file(
        self, client, project_with_tables, initialized_backend
    ):
        """Test that CoW creates a copy of the table file."""
        from src.config import settings
        from src.database import project_db_manager, metadata_db

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "cow-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Verify table not in branch yet
        branch_table_path = project_db_manager.get_branch_table_path(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )
        assert not branch_table_path.exists()

        # Perform CoW
        target_path = project_db_manager.copy_table_to_branch(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )

        # Mark in metadata
        metadata_db.mark_table_copied_to_branch(
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )

        # Verify table now exists in branch
        assert target_path.exists()
        assert target_path == branch_table_path

        # Verify it's a valid DuckDB file with data
        import duckdb

        conn = duckdb.connect(str(target_path), read_only=True)
        try:
            result = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()
            assert result[0] == 3  # Same as main
        finally:
            conn.close()

    def test_cow_preserves_main_data(
        self, client, project_with_tables, initialized_backend
    ):
        """Test that CoW doesn't affect main table."""
        from src.database import project_db_manager

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "cow-preserve-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Get main table row count
        main_table_path = project_db_manager.get_table_path(
            project_with_tables["project_id"],
            project_with_tables["bucket_name"],
            "orders",
        )

        import duckdb

        conn = duckdb.connect(str(main_table_path), read_only=True)
        main_count_before = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()[0]
        conn.close()

        # Perform CoW
        project_db_manager.copy_table_to_branch(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )

        # Main table should be unchanged
        conn = duckdb.connect(str(main_table_path), read_only=True)
        main_count_after = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()[0]
        conn.close()

        assert main_count_before == main_count_after


class TestEnsureTableInBranch:
    """Test the ensure_table_in_branch helper function."""

    def test_ensure_table_copies_if_not_present(
        self, client, project_with_tables, initialized_backend
    ):
        """ensure_table_in_branch copies table if not in branch."""
        from src.routers.branches import ensure_table_in_branch
        from src.database import project_db_manager, metadata_db

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "ensure-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Table should not be in branch
        assert not metadata_db.is_table_in_branch(
            branch_id, project_with_tables["bucket_name"], "orders"
        )

        # Call ensure_table_in_branch
        cow_performed = ensure_table_in_branch(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )

        assert cow_performed is True

        # Now table should be in branch
        assert metadata_db.is_table_in_branch(
            branch_id, project_with_tables["bucket_name"], "orders"
        )

        # File should exist
        assert project_db_manager.branch_table_exists(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )

    def test_ensure_table_skips_if_present(
        self, client, project_with_tables, initialized_backend
    ):
        """ensure_table_in_branch skips copy if already in branch."""
        from src.routers.branches import ensure_table_in_branch
        from src.database import metadata_db

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "ensure-skip-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # First call - should copy
        cow_performed = ensure_table_in_branch(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )
        assert cow_performed is True

        # Second call - should skip
        cow_performed = ensure_table_in_branch(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )
        assert cow_performed is False

    def test_ensure_table_nonexistent_table(
        self, client, project_with_tables, initialized_backend
    ):
        """ensure_table_in_branch raises 404 for non-existent table."""
        from src.routers.branches import ensure_table_in_branch
        from fastapi import HTTPException

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "ensure-404-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Try to ensure non-existent table
        with pytest.raises(HTTPException) as exc_info:
            ensure_table_in_branch(
                project_with_tables["project_id"],
                branch_id,
                project_with_tables["bucket_name"],
                "nonexistent",
            )

        assert exc_info.value.status_code == 404


class TestBranchStats:
    """Test branch statistics."""

    def test_branch_stats_empty(self, client, project_with_tables, initialized_backend):
        """Empty branch has zero stats."""
        from src.database import project_db_manager

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "stats-empty"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Get stats
        stats = project_db_manager.get_branch_stats(
            project_with_tables["project_id"], branch_id
        )

        assert stats["bucket_count"] == 0
        assert stats["table_count"] == 0
        assert stats["size_bytes"] == 0

    def test_branch_stats_with_tables(
        self, client, project_with_tables, initialized_backend
    ):
        """Branch stats reflect copied tables."""
        from src.database import project_db_manager, metadata_db

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "stats-tables"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Copy a table
        project_db_manager.copy_table_to_branch(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )
        metadata_db.mark_table_copied_to_branch(
            branch_id, project_with_tables["bucket_name"], "orders"
        )

        # Get stats
        stats = project_db_manager.get_branch_stats(
            project_with_tables["project_id"], branch_id
        )

        assert stats["bucket_count"] == 1  # One bucket directory
        assert stats["table_count"] == 1  # One table file
        assert stats["size_bytes"] > 0  # File has some size


class TestPullTableWithCoW:
    """Test pull table after CoW has occurred."""

    def test_pull_removes_branch_copy(
        self, client, project_with_tables, initialized_backend
    ):
        """Pull table removes branch copy and restores live view."""
        from src.database import project_db_manager, metadata_db

        # Create branch
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches",
            json={"name": "pull-cow-test"},
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Copy table to branch (CoW)
        project_db_manager.copy_table_to_branch(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )
        metadata_db.mark_table_copied_to_branch(
            branch_id, project_with_tables["bucket_name"], "orders"
        )

        # Verify table is in branch
        assert metadata_db.is_table_in_branch(
            branch_id, project_with_tables["bucket_name"], "orders"
        )

        # Pull table
        response = client.post(
            f"/projects/{project_with_tables['project_id']}/branches/{branch_id}/tables/{project_with_tables['bucket_name']}/orders/pull",
            headers=project_with_tables["project_headers"],
        )
        assert response.status_code == 200
        data = response.json()

        assert data["was_local"] is True
        assert "now reads from main" in data["message"]

        # Table should no longer be in branch
        assert not metadata_db.is_table_in_branch(
            branch_id, project_with_tables["bucket_name"], "orders"
        )

        # Branch table file should be deleted
        assert not project_db_manager.branch_table_exists(
            project_with_tables["project_id"],
            branch_id,
            project_with_tables["bucket_name"],
            "orders",
        )
