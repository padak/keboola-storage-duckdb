"""Integration tests for S3-Compatible API with boto3.

These tests verify that the S3-compatible API works with the real AWS SDK (boto3).
They use a real HTTP server and boto3 client to ensure full AWS Signature V4 compatibility.

The S3-compatible API now supports:
1. AWS Signature V4 (Authorization: AWS4-HMAC-SHA256 ...) - for boto3/aws-cli/rclone
2. Pre-signed URLs (?signature=...&expires=...) - for Keboola Connection
3. Bearer token (Authorization: Bearer ...) - for direct API access
4. X-Api-Key header - for programmatic access
"""

import threading
import time

import boto3
import pytest
import uvicorn
from botocore.config import Config
from botocore.exceptions import ClientError

from src.config import settings
from src.main import app


class ServerThread(threading.Thread):
    """Thread to run uvicorn server for testing."""

    def __init__(self, host: str, port: int, app):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.app = app
        self.server = None

    def run(self):
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="warning",
        )
        self.server = uvicorn.Server(config)
        self.server.run()

    def stop(self):
        if self.server:
            self.server.should_exit = True


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    """Start a test server for boto3 integration tests."""
    import os

    # Create temp directory for data
    tmp_path = tmp_path_factory.mktemp("data")

    # Override settings - including S3 credentials
    os.environ["DATA_DIR"] = str(tmp_path)
    os.environ["ADMIN_API_KEY"] = "test-admin-key"
    os.environ["S3_ACCESS_KEY_ID"] = "duckdb"
    os.environ["S3_SECRET_ACCESS_KEY"] = "test-admin-key"

    # Reload settings
    settings.data_dir = tmp_path
    settings.duckdb_dir = tmp_path / "duckdb"
    settings.files_dir = tmp_path / "files"
    settings.metadata_db_path = tmp_path / "metadata.duckdb"
    settings.admin_api_key = "test-admin-key"
    settings.s3_access_key_id = "duckdb"
    settings.s3_secret_access_key = "test-admin-key"

    # Create directories
    settings.duckdb_dir.mkdir(parents=True, exist_ok=True)
    settings.files_dir.mkdir(parents=True, exist_ok=True)

    # Initialize database
    from src.database import MetadataDB
    MetadataDB._instance = None
    from src.database import metadata_db
    metadata_db.initialize()

    # Start server
    port = 18765  # Use high port to avoid conflicts
    server_thread = ServerThread("127.0.0.1", port, app)
    server_thread.start()

    # Wait for server to start
    time.sleep(1)

    yield f"http://127.0.0.1:{port}"

    # Stop server
    server_thread.stop()


@pytest.fixture
def s3_client(server):
    """Create boto3 S3 client configured for local server with AWS Sig V4."""
    client = boto3.client(
        "s3",
        endpoint_url=f"{server}/s3",
        aws_access_key_id="duckdb",
        aws_secret_access_key="test-admin-key",
        region_name="local",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )
    return client


@pytest.fixture(scope="module")
def project_bucket(server):
    """Create a test project and return bucket name.

    Uses module scope to match server fixture and avoid 409 conflicts.
    """
    import httpx

    # Create project (ignore 409 if already exists from previous test run)
    response = httpx.post(
        f"{server}/projects",
        json={"id": "boto3-test", "name": "Boto3 Test Project"},
        headers={"Authorization": "Bearer test-admin-key"},
    )
    assert response.status_code in (201, 409), f"Unexpected status: {response.status_code}"

    yield "project_boto3-test"

    # Cleanup would go here


@pytest.mark.integration
class TestBoto3Compatibility:
    """Test boto3 SDK compatibility with AWS Signature V4.

    These tests verify that standard boto3 operations work correctly
    with our S3-compatible API using AWS Signature V4 authentication.
    """

    def test_put_and_get_object(self, s3_client, project_bucket):
        """Test uploading and downloading with boto3."""
        key = "boto3/test.txt"
        content = b"Hello from boto3!"

        # Upload
        s3_client.put_object(
            Bucket=project_bucket,
            Key=key,
            Body=content,
        )

        # Download
        response = s3_client.get_object(
            Bucket=project_bucket,
            Key=key,
        )
        downloaded = response["Body"].read()

        assert downloaded == content

    def test_head_object(self, s3_client, project_bucket):
        """Test head_object with boto3."""
        key = "boto3/head-test.txt"
        content = b"Content for head test"

        # Upload
        s3_client.put_object(
            Bucket=project_bucket,
            Key=key,
            Body=content,
        )

        # Head
        response = s3_client.head_object(
            Bucket=project_bucket,
            Key=key,
        )

        assert response["ContentLength"] == len(content)
        assert "ETag" in response

    def test_delete_object(self, s3_client, project_bucket):
        """Test delete_object with boto3."""
        key = "boto3/delete-test.txt"
        content = b"Content to delete"

        # Upload
        s3_client.put_object(
            Bucket=project_bucket,
            Key=key,
            Body=content,
        )

        # Delete
        s3_client.delete_object(
            Bucket=project_bucket,
            Key=key,
        )

        # Verify deleted - boto3 raises ClientError with NoSuchKey
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket=project_bucket,
                Key=key,
            )
        # S3 returns "NoSuchKey" error code for missing objects
        assert exc_info.value.response["Error"]["Code"] in ("404", "NoSuchKey")

    def test_list_objects_v2(self, s3_client, project_bucket):
        """Test list_objects_v2 with boto3."""
        # Upload some files
        for i in range(3):
            s3_client.put_object(
                Bucket=project_bucket,
                Key=f"list/file{i}.txt",
                Body=f"Content {i}".encode(),
            )

        # List
        response = s3_client.list_objects_v2(
            Bucket=project_bucket,
            Prefix="list/",
        )

        assert response["KeyCount"] == 3
        keys = [obj["Key"] for obj in response["Contents"]]
        assert len(keys) == 3
        assert all(k.startswith("list/") for k in keys)

    def test_upload_large_file(self, s3_client, project_bucket):
        """Test uploading larger file with boto3."""
        key = "boto3/large.bin"
        content = b"x" * (1024 * 1024)  # 1MB

        # Upload
        s3_client.put_object(
            Bucket=project_bucket,
            Key=key,
            Body=content,
        )

        # Verify size
        response = s3_client.head_object(
            Bucket=project_bucket,
            Key=key,
        )

        assert response["ContentLength"] == len(content)

    def test_binary_content(self, s3_client, project_bucket):
        """Test binary content with boto3."""
        key = "boto3/binary.bin"
        content = bytes(range(256))  # All byte values

        # Upload
        s3_client.put_object(
            Bucket=project_bucket,
            Key=key,
            Body=content,
        )

        # Download
        response = s3_client.get_object(
            Bucket=project_bucket,
            Key=key,
        )
        downloaded = response["Body"].read()

        assert downloaded == content

    def test_nested_keys(self, s3_client, project_bucket):
        """Test deeply nested keys with boto3."""
        key = "a/b/c/d/e/f/deep.txt"
        content = b"Deep content"

        # Upload
        s3_client.put_object(
            Bucket=project_bucket,
            Key=key,
            Body=content,
        )

        # Download
        response = s3_client.get_object(
            Bucket=project_bucket,
            Key=key,
        )
        downloaded = response["Body"].read()

        assert downloaded == content


class TestAwsSigV4Authentication:
    """Test AWS Signature V4 specific authentication scenarios."""

    def test_wrong_access_key_rejected(self, server, project_bucket):
        """Test that wrong access key is rejected."""
        client = boto3.client(
            "s3",
            endpoint_url=f"{server}/s3",
            aws_access_key_id="wrong-key",
            aws_secret_access_key="test-admin-key",
            region_name="local",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

        with pytest.raises(ClientError) as exc_info:
            client.put_object(
                Bucket=project_bucket,
                Key="test.txt",
                Body=b"test",
            )
        assert exc_info.value.response["Error"]["Code"] == "403"

    def test_wrong_secret_key_rejected(self, server, project_bucket):
        """Test that wrong secret key is rejected."""
        client = boto3.client(
            "s3",
            endpoint_url=f"{server}/s3",
            aws_access_key_id="duckdb",
            aws_secret_access_key="wrong-secret",
            region_name="local",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

        with pytest.raises(ClientError) as exc_info:
            client.put_object(
                Bucket=project_bucket,
                Key="test.txt",
                Body=b"test",
            )
        assert exc_info.value.response["Error"]["Code"] == "403"

    def test_nonexistent_bucket_rejected(self, server, s3_client):
        """Test that nonexistent bucket returns 404."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.put_object(
                Bucket="project_nonexistent",
                Key="test.txt",
                Body=b"test",
            )
        assert exc_info.value.response["Error"]["Code"] == "404"
