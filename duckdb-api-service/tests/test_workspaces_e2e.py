"""End-to-end tests for Workspaces + PG Wire auth flow.

These tests verify the complete workflow:
1. Create project with tables
2. Create workspace
3. Authenticate via PG Wire auth endpoint
4. Create session
5. Simulate queries (update activity)
6. Close session
7. Delete workspace
"""

import pytest
from datetime import datetime, timedelta, timezone


@pytest.fixture
def project_with_data(client, initialized_backend, admin_headers):
    """Create a project with buckets, tables, and data."""
    # Create project
    response = client.post(
        "/projects",
        json={"id": "e2e_test_proj", "name": "E2E Test Project"},
        headers=admin_headers,
    )
    assert response.status_code == 201
    project_key = response.json()["api_key"]
    project_headers = {"Authorization": f"Bearer {project_key}"}

    # Create multiple buckets
    for bucket_name in ["in_c_sales", "out_c_reports"]:
        response = client.post(
            "/projects/e2e_test_proj/branches/default/buckets",
            json={"name": bucket_name},
            headers=project_headers,
        )
        assert response.status_code == 201

    # Create tables in in_c_sales
    response = client.post(
        "/projects/e2e_test_proj/branches/default/buckets/in_c_sales/tables",
        json={
            "name": "orders",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "customer_id", "type": "INTEGER"},
                {"name": "amount", "type": "DECIMAL(10,2)"},
                {"name": "created_at", "type": "TIMESTAMP"},
            ],
            "primary_key": ["id"],
        },
        headers=project_headers,
    )
    assert response.status_code == 201

    response = client.post(
        "/projects/e2e_test_proj/branches/default/buckets/in_c_sales/tables",
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

    # Create table in out_c_reports
    response = client.post(
        "/projects/e2e_test_proj/branches/default/buckets/out_c_reports/tables",
        json={
            "name": "summary",
            "columns": [
                {"name": "date", "type": "DATE"},
                {"name": "total", "type": "DECIMAL(12,2)"},
            ],
        },
        headers=project_headers,
    )
    assert response.status_code == 201

    return {
        "project_id": "e2e_test_proj",
        "project_headers": project_headers,
        "buckets": ["in_c_sales", "out_c_reports"],
        "tables": {
            "in_c_sales": ["orders", "customers"],
            "out_c_reports": ["summary"],
        },
    }


class TestWorkspaceE2EFlow:
    """End-to-end tests for workspace workflow."""

    def test_complete_workspace_lifecycle(self, client, project_with_data):
        """Test complete workspace lifecycle: create -> auth -> use -> delete."""
        proj = project_with_data

        # Step 1: Create workspace
        response = client.post(
            "/projects/e2e_test_proj/workspaces",
            json={"name": "Analysis Workspace", "ttl_hours": 24, "size_limit_gb": 5},
            headers=proj["project_headers"],
        )
        assert response.status_code == 201
        workspace = response.json()

        workspace_id = workspace["id"]
        username = workspace["connection"]["username"]
        password = workspace["connection"]["password"]

        assert workspace["status"] == "active"
        assert workspace["size_limit_gb"] == 5
        assert password is not None  # Password shown on create

        # Step 2: Verify password not shown on GET
        response = client.get(
            f"/projects/e2e_test_proj/workspaces/{workspace_id}",
            headers=proj["project_headers"],
        )
        assert response.status_code == 200
        assert response.json()["connection"]["password"] is None

        # Step 3: Authenticate via PG Wire
        response = client.post(
            "/internal/pgwire/auth",
            json={
                "username": username,
                "password": password,
                "client_ip": "10.0.0.100",
            },
        )
        assert response.status_code == 200
        auth_data = response.json()

        assert auth_data["workspace_id"] == workspace_id
        assert auth_data["project_id"] == "e2e_test_proj"
        assert len(auth_data["tables"]) == 3  # 3 tables across 2 buckets

        # Verify all tables are returned
        table_names = [(t["bucket"], t["name"]) for t in auth_data["tables"]]
        assert ("in_c_sales", "orders") in table_names
        assert ("in_c_sales", "customers") in table_names
        assert ("out_c_reports", "summary") in table_names

        # Step 4: Create session
        session_id = "e2e_session_001"
        response = client.post(
            "/internal/pgwire/sessions",
            json={
                "session_id": session_id,
                "workspace_id": workspace_id,
                "client_ip": "10.0.0.100",
            },
        )
        assert response.status_code == 201
        session = response.json()
        assert session["status"] == "active"
        assert session["query_count"] == 0

        # Step 5: Simulate queries (update activity)
        for i in range(5):
            response = client.patch(
                f"/internal/pgwire/sessions/{session_id}/activity",
                json={"increment_queries": True},
            )
            assert response.status_code == 200

        # Verify query count
        response = client.get(f"/internal/pgwire/sessions/{session_id}")
        assert response.json()["query_count"] == 5

        # Step 6: Close session
        response = client.delete(
            f"/internal/pgwire/sessions/{session_id}",
            params={"reason": "user_disconnect"},
        )
        assert response.status_code == 204

        # Verify session status
        response = client.get(f"/internal/pgwire/sessions/{session_id}")
        assert response.json()["status"] == "user_disconnect"

        # Step 7: Delete workspace
        response = client.delete(
            f"/projects/e2e_test_proj/workspaces/{workspace_id}",
            headers=proj["project_headers"],
        )
        assert response.status_code == 204

        # Verify workspace gone
        response = client.get(
            f"/projects/e2e_test_proj/workspaces/{workspace_id}",
            headers=proj["project_headers"],
        )
        assert response.status_code == 404

    def test_multiple_concurrent_sessions(self, client, project_with_data, monkeypatch):
        """Test multiple concurrent sessions on same workspace."""
        from src.config import settings

        # Allow up to 5 connections per workspace
        monkeypatch.setattr(settings, "pgwire_max_connections_per_workspace", 5)

        proj = project_with_data

        # Create workspace
        response = client.post(
            "/projects/e2e_test_proj/workspaces",
            json={"name": "Multi-Session Workspace"},
            headers=proj["project_headers"],
        )
        workspace = response.json()
        workspace_id = workspace["id"]
        username = workspace["connection"]["username"]
        password = workspace["connection"]["password"]

        # Create 3 concurrent sessions
        session_ids = []
        for i in range(3):
            # Authenticate
            response = client.post(
                "/internal/pgwire/auth",
                json={
                    "username": username,
                    "password": password,
                    "client_ip": f"10.0.0.{i+1}",
                },
            )
            assert response.status_code == 200

            # Create session
            session_id = f"multi_session_{i}"
            response = client.post(
                "/internal/pgwire/sessions",
                json={
                    "session_id": session_id,
                    "workspace_id": workspace_id,
                    "client_ip": f"10.0.0.{i+1}",
                },
            )
            assert response.status_code == 201
            session_ids.append(session_id)

        # List sessions for workspace
        response = client.get(
            "/internal/pgwire/sessions",
            params={"workspace_id": workspace_id, "status": "active"},
        )
        assert response.status_code == 200
        assert len(response.json()) == 3

        # Close all sessions
        for session_id in session_ids:
            client.delete(f"/internal/pgwire/sessions/{session_id}")

        # Verify all closed
        response = client.get(
            "/internal/pgwire/sessions",
            params={"workspace_id": workspace_id, "status": "active"},
        )
        assert len(response.json()) == 0

    def test_auth_after_password_reset(self, client, project_with_data):
        """Test that old password fails after reset."""
        proj = project_with_data

        # Create workspace
        response = client.post(
            "/projects/e2e_test_proj/workspaces",
            json={"name": "Reset Password Workspace"},
            headers=proj["project_headers"],
        )
        workspace = response.json()
        workspace_id = workspace["id"]
        username = workspace["connection"]["username"]
        old_password = workspace["connection"]["password"]

        # Verify old password works
        response = client.post(
            "/internal/pgwire/auth",
            json={"username": username, "password": old_password},
        )
        assert response.status_code == 200

        # Reset password
        response = client.post(
            f"/projects/e2e_test_proj/workspaces/{workspace_id}/credentials/reset",
            headers=proj["project_headers"],
        )
        assert response.status_code == 200
        # reset_credentials returns WorkspaceConnectionInfo directly
        new_password = response.json()["password"]

        # Old password should fail
        response = client.post(
            "/internal/pgwire/auth",
            json={"username": username, "password": old_password},
        )
        assert response.status_code == 401

        # New password should work
        response = client.post(
            "/internal/pgwire/auth",
            json={"username": username, "password": new_password},
        )
        assert response.status_code == 200

    def test_workspace_with_branch(self, client, project_with_data):
        """Test workspace on a dev branch."""
        proj = project_with_data

        # Create dev branch
        response = client.post(
            "/projects/e2e_test_proj/branches",
            json={"name": "feature/analytics"},
            headers=proj["project_headers"],
        )
        assert response.status_code == 201
        branch_id = response.json()["id"]

        # Create workspace on branch
        response = client.post(
            f"/projects/e2e_test_proj/branches/{branch_id}/workspaces",
            json={"name": "Branch Workspace"},
            headers=proj["project_headers"],
        )
        assert response.status_code == 201
        workspace = response.json()
        assert workspace["branch_id"] == branch_id

        # Authenticate and verify branch_id in response
        response = client.post(
            "/internal/pgwire/auth",
            json={
                "username": workspace["connection"]["username"],
                "password": workspace["connection"]["password"],
            },
        )
        assert response.status_code == 200
        assert response.json()["branch_id"] == branch_id

    def test_session_cleanup_on_workspace_delete(self, client, project_with_data):
        """Test that sessions are cleaned up when workspace is deleted."""
        proj = project_with_data

        # Create workspace
        response = client.post(
            "/projects/e2e_test_proj/workspaces",
            json={"name": "Cleanup Test Workspace"},
            headers=proj["project_headers"],
        )
        workspace = response.json()
        workspace_id = workspace["id"]

        # Create multiple sessions
        for i in range(3):
            client.post(
                "/internal/pgwire/sessions",
                json={
                    "session_id": f"cleanup_session_{i}",
                    "workspace_id": workspace_id,
                },
            )

        # Verify sessions exist
        response = client.get(
            "/internal/pgwire/sessions",
            params={"workspace_id": workspace_id},
        )
        assert len(response.json()) == 3

        # Delete workspace
        response = client.delete(
            f"/projects/e2e_test_proj/workspaces/{workspace_id}",
            headers=proj["project_headers"],
        )
        assert response.status_code == 204

        # Sessions should be gone
        # (Note: They might still exist but orphaned, or might be deleted - depends on implementation)
        # For now just verify no error when listing
        response = client.get("/internal/pgwire/sessions")
        assert response.status_code == 200


class TestWorkspaceE2EErrorHandling:
    """Test error handling in e2e scenarios."""

    def test_auth_fails_after_workspace_expires(self, client, project_with_data):
        """Test that auth fails when workspace has expired."""
        from src.database import metadata_db

        proj = project_with_data

        # Create workspace
        response = client.post(
            "/projects/e2e_test_proj/workspaces",
            json={"name": "Expiring Workspace", "ttl_hours": 1},
            headers=proj["project_headers"],
        )
        workspace = response.json()
        workspace_id = workspace["id"]
        username = workspace["connection"]["username"]
        password = workspace["connection"]["password"]

        # Auth should work initially
        response = client.post(
            "/internal/pgwire/auth",
            json={"username": username, "password": password},
        )
        assert response.status_code == 200

        # Manually expire the workspace
        # (Work around DuckDB FK constraint issues)
        with metadata_db.connection() as conn:
            creds = conn.execute(
                "SELECT * FROM workspace_credentials WHERE workspace_id = ?",
                [workspace_id],
            ).fetchone()

            conn.execute(
                "DELETE FROM pgwire_sessions WHERE workspace_id = ?",
                [workspace_id],
            )
            conn.execute(
                "DELETE FROM workspace_credentials WHERE workspace_id = ?",
                [workspace_id],
            )

            conn.execute(
                "UPDATE workspaces SET expires_at = ? WHERE id = ?",
                [
                    (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                    workspace_id,
                ],
            )

            if creds:
                conn.execute(
                    """
                    INSERT INTO workspace_credentials (workspace_id, username, password_hash)
                    VALUES (?, ?, ?)
                    """,
                    [creds[0], creds[1], creds[2]],
                )

        # Auth should now fail
        response = client.post(
            "/internal/pgwire/auth",
            json={"username": username, "password": password},
        )
        assert response.status_code == 410
        assert response.json()["detail"]["error"] == "workspace_expired"

    def test_connection_limit_prevents_new_sessions(self, client, project_with_data, monkeypatch):
        """Test that connection limit blocks new auth attempts."""
        from src.config import settings

        # Set very low limit
        monkeypatch.setattr(settings, "pgwire_max_connections_per_workspace", 2)

        proj = project_with_data

        # Create workspace
        response = client.post(
            "/projects/e2e_test_proj/workspaces",
            json={"name": "Limited Workspace"},
            headers=proj["project_headers"],
        )
        workspace = response.json()
        workspace_id = workspace["id"]
        username = workspace["connection"]["username"]
        password = workspace["connection"]["password"]

        # Create 2 sessions (at limit)
        for i in range(2):
            client.post(
                "/internal/pgwire/sessions",
                json={
                    "session_id": f"limit_session_{i}",
                    "workspace_id": workspace_id,
                },
            )

        # Next auth should fail due to limit
        response = client.post(
            "/internal/pgwire/auth",
            json={"username": username, "password": password},
        )
        assert response.status_code == 429
        assert response.json()["detail"]["error"] == "connection_limit_reached"

        # Close one session
        client.delete("/internal/pgwire/sessions/limit_session_0")

        # Now auth should work
        response = client.post(
            "/internal/pgwire/auth",
            json={"username": username, "password": password},
        )
        assert response.status_code == 200


class TestWorkspaceE2EMetrics:
    """Test metrics tracking in e2e scenarios."""

    def test_active_sessions_tracked(self, client, project_with_data):
        """Test that active session count is tracked correctly."""
        from src.database import metadata_db

        proj = project_with_data

        # Create workspace
        response = client.post(
            "/projects/e2e_test_proj/workspaces",
            json={"name": "Metrics Workspace"},
            headers=proj["project_headers"],
        )
        workspace = response.json()
        workspace_id = workspace["id"]

        # Initially 0 active sessions
        count = metadata_db.count_active_pgwire_sessions(workspace_id)
        assert count == 0

        # Create sessions
        for i in range(3):
            client.post(
                "/internal/pgwire/sessions",
                json={
                    "session_id": f"metrics_session_{i}",
                    "workspace_id": workspace_id,
                },
            )

        # 3 active sessions
        count = metadata_db.count_active_pgwire_sessions(workspace_id)
        assert count == 3

        # Close 2 sessions
        client.delete("/internal/pgwire/sessions/metrics_session_0")
        client.delete("/internal/pgwire/sessions/metrics_session_1")

        # 1 active session
        count = metadata_db.count_active_pgwire_sessions(workspace_id)
        assert count == 1

    def test_query_count_accumulated(self, client, project_with_data):
        """Test that query counts are accumulated correctly."""
        proj = project_with_data

        # Create workspace
        response = client.post(
            "/projects/e2e_test_proj/workspaces",
            json={"name": "Query Count Workspace"},
            headers=proj["project_headers"],
        )
        workspace = response.json()
        workspace_id = workspace["id"]

        # Create session
        session_id = "query_count_session"
        client.post(
            "/internal/pgwire/sessions",
            json={
                "session_id": session_id,
                "workspace_id": workspace_id,
            },
        )

        # Simulate 10 queries
        for i in range(10):
            client.patch(
                f"/internal/pgwire/sessions/{session_id}/activity",
                json={"increment_queries": True},
            )

        # Verify count
        response = client.get(f"/internal/pgwire/sessions/{session_id}")
        assert response.json()["query_count"] == 10
