"""Tests for bucket sharing and linking endpoints."""

import pytest
from fastapi.testclient import TestClient


class TestShareBucket:
    """Tests for POST /projects/{project_id}/branches/default/buckets/{bucket_name}/share endpoint."""

    def test_share_bucket_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful bucket sharing."""
        # Create source and target projects
        client.post("/projects", json={"id": "share_source_1"}, headers=admin_headers)
        client.post("/projects", json={"id": "share_target_1"}, headers=admin_headers)

        # Create bucket in source project
        client.post(
            "/projects/share_source_1/branches/default/buckets",
            json={"name": "shared_bucket"},
            headers=admin_headers,
        )

        # Share the bucket
        response = client.post(
            "/projects/share_source_1/branches/default/buckets/shared_bucket/share",
            json={"target_project_id": "share_target_1"},
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert "share_target_1" in data["shared_with"]

    def test_share_bucket_source_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test sharing bucket from non-existent project returns 404."""
        response = client.post(
            "/projects/nonexistent/branches/default/buckets/any/share",
            json={"target_project_id": "target"},
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_share_bucket_target_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test sharing to non-existent target project returns 404."""
        client.post("/projects", json={"id": "share_source_2"}, headers=admin_headers)
        client.post("/projects/share_source_2/branches/default/buckets", json={"name": "bucket"}, headers=admin_headers)

        response = client.post(
            "/projects/share_source_2/branches/default/buckets/bucket/share",
            json={"target_project_id": "nonexistent"},
            headers=admin_headers,
        )

        assert response.status_code == 404

    def test_share_bucket_not_found(self, client: TestClient, initialized_backend, admin_headers):
        """Test sharing non-existent bucket returns 404."""
        client.post("/projects", json={"id": "share_source_3"}, headers=admin_headers)
        client.post("/projects", json={"id": "share_target_3"}, headers=admin_headers)

        response = client.post(
            "/projects/share_source_3/branches/default/buckets/nonexistent/share",
            json={"target_project_id": "share_target_3"},
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_share_bucket_already_shared(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test sharing already shared bucket returns 409."""
        client.post("/projects", json={"id": "share_source_4"}, headers=admin_headers)
        client.post("/projects", json={"id": "share_target_4"}, headers=admin_headers)
        client.post("/projects/share_source_4/branches/default/buckets", json={"name": "bucket"}, headers=admin_headers)

        # Share first time
        client.post(
            "/projects/share_source_4/branches/default/buckets/bucket/share",
            json={"target_project_id": "share_target_4"},
            headers=admin_headers,
        )

        # Try to share again
        response = client.post(
            "/projects/share_source_4/branches/default/buckets/bucket/share",
            json={"target_project_id": "share_target_4"},
            headers=admin_headers,
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "already_shared"


class TestUnshareBucket:
    """Tests for DELETE /projects/{project_id}/branches/default/buckets/{bucket_name}/share endpoint."""

    def test_unshare_bucket_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful bucket unsharing."""
        client.post("/projects", json={"id": "unshare_source_1"}, headers=admin_headers)
        client.post("/projects", json={"id": "unshare_target_1"}, headers=admin_headers)
        client.post("/projects/unshare_source_1/branches/default/buckets", json={"name": "bucket"}, headers=admin_headers)

        # Share first
        client.post(
            "/projects/unshare_source_1/branches/default/buckets/bucket/share",
            json={"target_project_id": "unshare_target_1"},
            headers=admin_headers,
        )

        # Unshare
        response = client.delete(
            "/projects/unshare_source_1/branches/default/buckets/bucket/share?target_project_id=unshare_target_1",
            headers=admin_headers,
        )

        assert response.status_code == 204

    def test_unshare_bucket_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test unsharing from non-existent project returns 404."""
        response = client.delete(
            "/projects/nonexistent/branches/default/buckets/any/share?target_project_id=target",
            headers=admin_headers,
        )

        assert response.status_code == 404


class TestLinkBucket:
    """Tests for POST /projects/{project_id}/branches/default/buckets/{bucket_name}/link endpoint."""

    def test_link_bucket_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test successful bucket linking."""
        # Create source project with bucket
        client.post("/projects", json={"id": "link_source_1"}, headers=admin_headers)
        client.post("/projects/link_source_1/branches/default/buckets", json={"name": "source_bucket"}, headers=admin_headers)

        # Create target project
        client.post("/projects", json={"id": "link_target_1"}, headers=admin_headers)

        # Link the bucket
        response = client.post(
            "/projects/link_target_1/branches/default/buckets/linked_bucket/link",
            json={
                "source_project_id": "link_source_1",
                "source_bucket_name": "source_bucket",
            },
            headers=admin_headers,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "linked_bucket"
        assert "Linked from" in data["description"]

    def test_link_bucket_target_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test linking in non-existent target project returns 404."""
        response = client.post(
            "/projects/nonexistent/branches/default/buckets/bucket/link",
            json={
                "source_project_id": "source",
                "source_bucket_name": "bucket",
            },
            headers=admin_headers,
        )

        assert response.status_code == 404

    def test_link_bucket_source_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test linking from non-existent source project returns 404."""
        client.post("/projects", json={"id": "link_target_2"}, headers=admin_headers)

        response = client.post(
            "/projects/link_target_2/branches/default/buckets/bucket/link",
            json={
                "source_project_id": "nonexistent",
                "source_bucket_name": "bucket",
            },
            headers=admin_headers,
        )

        assert response.status_code == 404

    def test_link_bucket_source_bucket_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test linking non-existent source bucket returns 404."""
        client.post("/projects", json={"id": "link_source_3"}, headers=admin_headers)
        client.post("/projects", json={"id": "link_target_3"}, headers=admin_headers)

        response = client.post(
            "/projects/link_target_3/branches/default/buckets/bucket/link",
            json={
                "source_project_id": "link_source_3",
                "source_bucket_name": "nonexistent",
            },
            headers=admin_headers,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_link_bucket_target_exists(self, client: TestClient, initialized_backend, admin_headers):
        """Test linking when target bucket already exists returns 409."""
        client.post("/projects", json={"id": "link_source_4"}, headers=admin_headers)
        client.post("/projects", json={"id": "link_target_4"}, headers=admin_headers)
        client.post("/projects/link_source_4/branches/default/buckets", json={"name": "source"}, headers=admin_headers)
        client.post(
            "/projects/link_target_4/branches/default/buckets", json={"name": "existing_bucket"},
            headers=admin_headers,
        )

        response = client.post(
            "/projects/link_target_4/branches/default/buckets/existing_bucket/link",
            json={
                "source_project_id": "link_source_4",
                "source_bucket_name": "source",
            },
            headers=admin_headers,
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "bucket_exists"


class TestUnlinkBucket:
    """Tests for DELETE /projects/{project_id}/branches/default/buckets/{bucket_name}/link endpoint."""

    def test_unlink_bucket_link_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test unlinking non-linked bucket returns 404."""
        client.post("/projects", json={"id": "unlink_test_1"}, headers=admin_headers)

        response = client.delete("/projects/unlink_test_1/branches/default/buckets/not_linked/link", headers=admin_headers)

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "link_not_found"

    def test_unlink_bucket_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test unlinking from non-existent project returns 404."""
        response = client.delete("/projects/nonexistent/branches/default/buckets/any/link", headers=admin_headers)

        assert response.status_code == 404


class TestGrantReadonly:
    """Tests for POST /projects/{project_id}/branches/default/buckets/{bucket_name}/grant-readonly endpoint."""

    def test_grant_readonly_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test granting readonly access (metadata operation)."""
        client.post("/projects", json={"id": "readonly_test_1"}, headers=admin_headers)
        client.post("/projects/readonly_test_1/branches/default/buckets", json={"name": "bucket"}, headers=admin_headers)

        response = client.post("/projects/readonly_test_1/branches/default/buckets/bucket/grant-readonly", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"

    def test_grant_readonly_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test granting readonly on non-existent project returns 404."""
        response = client.post("/projects/nonexistent/branches/default/buckets/any/grant-readonly", headers=admin_headers)

        assert response.status_code == 404

    def test_grant_readonly_bucket_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test granting readonly on non-existent bucket returns 404."""
        client.post("/projects", json={"id": "readonly_test_2"}, headers=admin_headers)

        response = client.post("/projects/readonly_test_2/branches/default/buckets/nonexistent/grant-readonly", headers=admin_headers)

        assert response.status_code == 404


class TestRevokeReadonly:
    """Tests for DELETE /projects/{project_id}/branches/default/buckets/{bucket_name}/grant-readonly endpoint."""

    def test_revoke_readonly_success(self, client: TestClient, initialized_backend, admin_headers):
        """Test revoking readonly access (metadata operation)."""
        client.post("/projects", json={"id": "revoke_test_1"}, headers=admin_headers)

        response = client.delete("/projects/revoke_test_1/branches/default/buckets/any/grant-readonly", headers=admin_headers)

        assert response.status_code == 204

    def test_revoke_readonly_project_not_found(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test revoking readonly on non-existent project returns 404."""
        response = client.delete("/projects/nonexistent/branches/default/buckets/any/grant-readonly", headers=admin_headers)

        assert response.status_code == 404


class TestBucketSharingOperationsLog:
    """Tests for bucket sharing operations audit logging."""

    def test_share_logs_operation(self, client: TestClient, initialized_backend, admin_headers):
        """Test that sharing logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "share_log_1"}, headers=admin_headers)
        client.post("/projects", json={"id": "share_log_target"}, headers=admin_headers)
        client.post("/projects/share_log_1/branches/default/buckets", json={"name": "bucket"}, headers=admin_headers)
        client.post(
            "/projects/share_log_1/branches/default/buckets/bucket/share",
            json={"target_project_id": "share_log_target"},
            headers=admin_headers,
        )

        logs = metadata_db.execute(
            "SELECT operation, status FROM operations_log WHERE project_id = ? AND operation = 'share_bucket'",
            ["share_log_1"],
        )

        assert len(logs) >= 1
        assert any(log[0] == "share_bucket" and log[1] == "success" for log in logs)
