"""Tests for bucket sharing and linking endpoints."""

import pytest
from fastapi.testclient import TestClient


class TestShareBucket:
    """Tests for POST /projects/{project_id}/buckets/{bucket_name}/share endpoint."""

    def test_share_bucket_success(self, client: TestClient, initialized_backend):
        """Test successful bucket sharing."""
        # Create source and target projects
        client.post("/projects", json={"id": "share_source_1"})
        client.post("/projects", json={"id": "share_target_1"})

        # Create bucket in source project
        client.post(
            "/projects/share_source_1/buckets",
            json={"name": "shared_bucket"},
        )

        # Share the bucket
        response = client.post(
            "/projects/share_source_1/buckets/shared_bucket/share",
            json={"target_project_id": "share_target_1"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "share_target_1" in data["shared_with"]

    def test_share_bucket_source_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test sharing bucket from non-existent project returns 404."""
        response = client.post(
            "/projects/nonexistent/buckets/any/share",
            json={"target_project_id": "target"},
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "project_not_found"

    def test_share_bucket_target_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test sharing to non-existent target project returns 404."""
        client.post("/projects", json={"id": "share_source_2"})
        client.post("/projects/share_source_2/buckets", json={"name": "bucket"})

        response = client.post(
            "/projects/share_source_2/buckets/bucket/share",
            json={"target_project_id": "nonexistent"},
        )

        assert response.status_code == 404

    def test_share_bucket_not_found(self, client: TestClient, initialized_backend):
        """Test sharing non-existent bucket returns 404."""
        client.post("/projects", json={"id": "share_source_3"})
        client.post("/projects", json={"id": "share_target_3"})

        response = client.post(
            "/projects/share_source_3/buckets/nonexistent/share",
            json={"target_project_id": "share_target_3"},
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_share_bucket_already_shared(
        self, client: TestClient, initialized_backend
    ):
        """Test sharing already shared bucket returns 409."""
        client.post("/projects", json={"id": "share_source_4"})
        client.post("/projects", json={"id": "share_target_4"})
        client.post("/projects/share_source_4/buckets", json={"name": "bucket"})

        # Share first time
        client.post(
            "/projects/share_source_4/buckets/bucket/share",
            json={"target_project_id": "share_target_4"},
        )

        # Try to share again
        response = client.post(
            "/projects/share_source_4/buckets/bucket/share",
            json={"target_project_id": "share_target_4"},
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "already_shared"


class TestUnshareBucket:
    """Tests for DELETE /projects/{project_id}/buckets/{bucket_name}/share endpoint."""

    def test_unshare_bucket_success(self, client: TestClient, initialized_backend):
        """Test successful bucket unsharing."""
        client.post("/projects", json={"id": "unshare_source_1"})
        client.post("/projects", json={"id": "unshare_target_1"})
        client.post("/projects/unshare_source_1/buckets", json={"name": "bucket"})

        # Share first
        client.post(
            "/projects/unshare_source_1/buckets/bucket/share",
            json={"target_project_id": "unshare_target_1"},
        )

        # Unshare
        response = client.delete(
            "/projects/unshare_source_1/buckets/bucket/share?target_project_id=unshare_target_1"
        )

        assert response.status_code == 204

    def test_unshare_bucket_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test unsharing from non-existent project returns 404."""
        response = client.delete(
            "/projects/nonexistent/buckets/any/share?target_project_id=target"
        )

        assert response.status_code == 404


class TestLinkBucket:
    """Tests for POST /projects/{project_id}/buckets/{bucket_name}/link endpoint."""

    def test_link_bucket_success(self, client: TestClient, initialized_backend):
        """Test successful bucket linking."""
        # Create source project with bucket
        client.post("/projects", json={"id": "link_source_1"})
        client.post("/projects/link_source_1/buckets", json={"name": "source_bucket"})

        # Create target project
        client.post("/projects", json={"id": "link_target_1"})

        # Link the bucket
        response = client.post(
            "/projects/link_target_1/buckets/linked_bucket/link",
            json={
                "source_project_id": "link_source_1",
                "source_bucket_name": "source_bucket",
            },
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "linked_bucket"
        assert "Linked from" in data["description"]

    def test_link_bucket_target_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test linking in non-existent target project returns 404."""
        response = client.post(
            "/projects/nonexistent/buckets/bucket/link",
            json={
                "source_project_id": "source",
                "source_bucket_name": "bucket",
            },
        )

        assert response.status_code == 404

    def test_link_bucket_source_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test linking from non-existent source project returns 404."""
        client.post("/projects", json={"id": "link_target_2"})

        response = client.post(
            "/projects/link_target_2/buckets/bucket/link",
            json={
                "source_project_id": "nonexistent",
                "source_bucket_name": "bucket",
            },
        )

        assert response.status_code == 404

    def test_link_bucket_source_bucket_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test linking non-existent source bucket returns 404."""
        client.post("/projects", json={"id": "link_source_3"})
        client.post("/projects", json={"id": "link_target_3"})

        response = client.post(
            "/projects/link_target_3/buckets/bucket/link",
            json={
                "source_project_id": "link_source_3",
                "source_bucket_name": "nonexistent",
            },
        )

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "bucket_not_found"

    def test_link_bucket_target_exists(self, client: TestClient, initialized_backend):
        """Test linking when target bucket already exists returns 409."""
        client.post("/projects", json={"id": "link_source_4"})
        client.post("/projects", json={"id": "link_target_4"})
        client.post("/projects/link_source_4/buckets", json={"name": "source"})
        client.post(
            "/projects/link_target_4/buckets", json={"name": "existing_bucket"}
        )

        response = client.post(
            "/projects/link_target_4/buckets/existing_bucket/link",
            json={
                "source_project_id": "link_source_4",
                "source_bucket_name": "source",
            },
        )

        assert response.status_code == 409
        assert response.json()["detail"]["error"] == "bucket_exists"


class TestUnlinkBucket:
    """Tests for DELETE /projects/{project_id}/buckets/{bucket_name}/link endpoint."""

    def test_unlink_bucket_link_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test unlinking non-linked bucket returns 404."""
        client.post("/projects", json={"id": "unlink_test_1"})

        response = client.delete("/projects/unlink_test_1/buckets/not_linked/link")

        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "link_not_found"

    def test_unlink_bucket_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test unlinking from non-existent project returns 404."""
        response = client.delete("/projects/nonexistent/buckets/any/link")

        assert response.status_code == 404


class TestGrantReadonly:
    """Tests for POST /projects/{project_id}/buckets/{bucket_name}/grant-readonly endpoint."""

    def test_grant_readonly_success(self, client: TestClient, initialized_backend):
        """Test granting readonly access (metadata operation)."""
        client.post("/projects", json={"id": "readonly_test_1"})
        client.post("/projects/readonly_test_1/buckets", json={"name": "bucket"})

        response = client.post("/projects/readonly_test_1/buckets/bucket/grant-readonly")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"

    def test_grant_readonly_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test granting readonly on non-existent project returns 404."""
        response = client.post("/projects/nonexistent/buckets/any/grant-readonly")

        assert response.status_code == 404

    def test_grant_readonly_bucket_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test granting readonly on non-existent bucket returns 404."""
        client.post("/projects", json={"id": "readonly_test_2"})

        response = client.post("/projects/readonly_test_2/buckets/nonexistent/grant-readonly")

        assert response.status_code == 404


class TestRevokeReadonly:
    """Tests for DELETE /projects/{project_id}/buckets/{bucket_name}/grant-readonly endpoint."""

    def test_revoke_readonly_success(self, client: TestClient, initialized_backend):
        """Test revoking readonly access (metadata operation)."""
        client.post("/projects", json={"id": "revoke_test_1"})

        response = client.delete("/projects/revoke_test_1/buckets/any/grant-readonly")

        assert response.status_code == 204

    def test_revoke_readonly_project_not_found(
        self, client: TestClient, initialized_backend
    ):
        """Test revoking readonly on non-existent project returns 404."""
        response = client.delete("/projects/nonexistent/buckets/any/grant-readonly")

        assert response.status_code == 404


class TestBucketSharingOperationsLog:
    """Tests for bucket sharing operations audit logging."""

    def test_share_logs_operation(self, client: TestClient, initialized_backend):
        """Test that sharing logs the operation."""
        from src.database import metadata_db

        client.post("/projects", json={"id": "share_log_1"})
        client.post("/projects", json={"id": "share_log_target"})
        client.post("/projects/share_log_1/buckets", json={"name": "bucket"})
        client.post(
            "/projects/share_log_1/buckets/bucket/share",
            json={"target_project_id": "share_log_target"},
        )

        logs = metadata_db.execute(
            "SELECT operation, status FROM operations_log WHERE project_id = ? AND operation = 'share_bucket'",
            ["share_log_1"],
        )

        assert len(logs) >= 1
        assert any(log[0] == "share_bucket" and log[1] == "success" for log in logs)
