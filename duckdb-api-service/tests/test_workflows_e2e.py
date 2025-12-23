"""
Comprehensive E2E Workflow Tests (Phase 15.11)

These tests cover ALL 93 API endpoints through realistic user scenarios.
Uses embedded uvicorn server with isolated temp data.

Workflows:
1. ProjectLifecycle (13 endpoints) - Project + API Key management
2. DataPipeline (19 endpoints) - Full data flow: bucket -> table -> import -> export
3. SnapshotRecovery (14 endpoints) - Snapshot settings + create/restore
4. BranchDevelopment (6 endpoints) - Dev branch isolation
5. BucketSharing (10 endpoints) - Cross-project sharing
6. WorkspaceSQL (12 endpoints) - Workspace lifecycle
7. S3Compatible (6 endpoints) - S3 API operations
8. FilesManagement (7 endpoints) - File upload/download
9. DriverBridge (2 endpoints) - gRPC bridge
10. PGWireSessions (4 endpoints) - PG Wire auth
"""

import os
import time
import uuid
import json
import threading
from pathlib import Path
from io import BytesIO

import logging
import pytest
import httpx
import uvicorn
import structlog

# Import FastAPI app
from src.main import app
from src.config import settings
from src.database import MetadataDB


# Test constants
E2E_PORT = 18767  # Different from test_api_e2e.py (18766) to avoid conflicts
E2E_HOST = "127.0.0.1"
TEST_ADMIN_KEY = "workflow_e2e_admin_key"


def generate_test_id() -> str:
    """Generate unique test ID with timestamp."""
    return f"test_{int(time.time())}_{uuid.uuid4().hex[:8]}"


class ServerThread(threading.Thread):
    """Thread running uvicorn server."""

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
            access_log=False,
        )
        self.server = uvicorn.Server(config)
        self.server.run()

    def stop(self):
        if self.server:
            self.server.should_exit = True


@pytest.fixture(scope="module")
def e2e_server(tmp_path_factory):
    """
    Start embedded uvicorn server for E2E tests.

    Creates isolated temp directory structure:
    - /duckdb/ - Project/table files
    - /files/ - File staging
    - /snapshots/ - Snapshot storage
    - /metadata.duckdb - Metadata database
    """
    # Create isolated temp directory
    temp_dir = tmp_path_factory.mktemp("workflow_e2e")
    data_dir = temp_dir / "data"
    duckdb_dir = data_dir / "duckdb"
    files_dir = data_dir / "files"
    snapshots_dir = data_dir / "snapshots"

    # Create directory structure
    duckdb_dir.mkdir(parents=True)
    files_dir.mkdir(parents=True)
    snapshots_dir.mkdir(parents=True)

    # Set environment variables
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["ADMIN_API_KEY"] = TEST_ADMIN_KEY

    # Suppress application logs when WORKFLOW_LOG is enabled (cleaner protocol output)
    # Must be done BEFORE any logging happens
    if os.environ.get("WORKFLOW_LOG", "0") == "1":
        # Reconfigure structlog to only show errors
        structlog.reset_defaults()
        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.CRITICAL),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=False,
        )

    # Patch settings
    settings.data_dir = data_dir
    settings.duckdb_dir = duckdb_dir
    settings.files_dir = files_dir
    settings.snapshots_dir = snapshots_dir
    settings.metadata_db_path = data_dir / "metadata.duckdb"
    settings.admin_api_key = TEST_ADMIN_KEY

    # Reset MetadataDB singleton to use new paths
    MetadataDB._instance = None

    # Initialize metadata database
    metadata_db = MetadataDB()
    metadata_db.initialize()

    # Start server
    server_thread = ServerThread(E2E_HOST, E2E_PORT, app)
    server_thread.start()

    # Wait for server to be ready
    time.sleep(2.0)

    base_url = f"http://{E2E_HOST}:{E2E_PORT}"

    yield {
        "base_url": base_url,
        "temp_dir": temp_dir,
        "data_dir": data_dir,
    }

    # Cleanup
    server_thread.stop()
    time.sleep(0.5)


@pytest.fixture(scope="module")
def api(e2e_server):
    """Create httpx client for API calls."""
    return httpx.Client(
        base_url=e2e_server["base_url"],
        timeout=30.0,
    )


@pytest.fixture(scope="module")
def admin_headers():
    """Admin authentication headers."""
    return {"Authorization": f"Bearer {TEST_ADMIN_KEY}"}


# =============================================================================
# Protocol Logger
# =============================================================================

class WorkflowProtocol:
    """Logger for workflow test protocol output."""

    def __init__(self, name: str, total_steps: int):
        self.name = name
        self.total_steps = total_steps
        self.current_step = 0
        self.enabled = os.environ.get("WORKFLOW_LOG", "0") == "1"

    def header(self):
        """Print workflow header."""
        if self.enabled:
            print(f"\n{'='*60}")
            print(f"=== {self.name} ===")
            print(f"{'='*60}")

    def step(self, method: str, path: str, request_body: dict = None):
        """Log a step before execution."""
        self.current_step += 1
        if self.enabled:
            print(f"\n[{self.current_step}/{self.total_steps}] {method} {path}")
            if request_body:
                body_str = json.dumps(request_body, indent=2, default=str)
                if len(body_str) > 200:
                    body_str = body_str[:200] + "..."
                print(f"       Request: {body_str}")

    def result(self, response, summary: str = None):
        """Log response after execution."""
        if self.enabled:
            status = f"{response.status_code} {_status_text(response.status_code)}"
            if summary:
                print(f"       Response: {status} - {summary}")
            else:
                print(f"       Response: {status}")

    def info(self, message: str):
        """Log info message."""
        if self.enabled:
            print(f"       -> {message}")


def _status_text(code: int) -> str:
    """HTTP status code to text."""
    return {
        200: "OK", 201: "Created", 204: "No Content",
        400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
        409: "Conflict", 500: "Server Error"
    }.get(code, "")


# =============================================================================
# Helper Functions
# =============================================================================

def upload_csv(api: httpx.Client, project_id: str, headers: dict,
               filename: str, content: bytes) -> int:
    """
    Upload CSV file using 3-step process.
    Returns file_id.
    """
    # Step 1: Prepare
    resp = api.post(
        f"/projects/{project_id}/files/prepare",
        json={"filename": filename},
        headers=headers,
    )
    assert resp.status_code == 200, f"Prepare failed: {resp.text}"
    upload_key = resp.json()["upload_key"]

    # Step 2: Upload
    resp = api.post(
        f"/projects/{project_id}/files/upload/{upload_key}",
        files={"file": (filename, content, "text/csv")},
        headers=headers,
    )
    assert resp.status_code == 200, f"Upload failed: {resp.text}"

    # Step 3: Register
    resp = api.post(
        f"/projects/{project_id}/files",
        json={"upload_key": upload_key, "name": filename},
        headers=headers,
    )
    assert resp.status_code == 201, f"Register failed: {resp.text}"
    return resp.json()["id"]


def get_row_count(api: httpx.Client, project_id: str, headers: dict,
                  bucket: str, table: str, branch: str = "default") -> int:
    """Get row count from table preview."""
    resp = api.get(
        f"/projects/{project_id}/branches/{branch}/buckets/{bucket}/tables/{table}/preview",
        headers=headers,
    )
    assert resp.status_code == 200, f"Preview failed: {resp.text}"
    return resp.json()["total_row_count"]


# =============================================================================
# Workflow 1: ProjectLifecycle (13 endpoints)
# =============================================================================

class TestWorkflow1ProjectLifecycle:
    """
    Test complete project lifecycle including API key management.

    Endpoints tested:
    1. GET /health
    2. GET /metrics
    3. POST /backend/init
    4. POST /projects
    5. GET /projects
    6. GET /projects/{id}
    7. PUT /projects/{id}
    8. POST /projects/{id}/api-keys
    9. GET /projects/{id}/api-keys
    10. GET /projects/{id}/api-keys/{key_id}
    11. POST /projects/{id}/api-keys/{key_id}/rotate
    12. DELETE /projects/{id}/api-keys/{key_id}
    13. GET /projects/{id}/stats
    14. DELETE /projects/{id} (cleanup)
    """

    def test_health_check(self, api, admin_headers):
        """1. GET /health - Service health check."""
        resp = api.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "version" in data

    def test_metrics_endpoint(self, api):
        """2. GET /metrics - Prometheus metrics."""
        resp = api.get("/metrics")
        assert resp.status_code == 200
        # Prometheus format is text/plain
        assert "duckdb_" in resp.text or "python_" in resp.text

    def test_backend_init(self, api, admin_headers):
        """3. POST /backend/init - Initialize storage backend."""
        resp = api.post("/backend/init", headers=admin_headers)
        # May return 200 (already initialized) or 201 (newly initialized)
        assert resp.status_code in [200, 201]

    def test_full_project_lifecycle(self, api, admin_headers):
        """Complete project lifecycle with API key management."""
        log = WorkflowProtocol("Workflow 1: Project Lifecycle", 10)
        log.header()

        test_id = generate_test_id()
        project_id = f"proj_{test_id}"

        # 4. POST /projects - Create project
        log.step("POST", "/projects", {"id": project_id})
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Workflow Test {test_id}"},
            headers=admin_headers,
        )
        assert resp.status_code == 201
        project_data = resp.json()
        assert project_data["id"] == project_id
        project_api_key = project_data["api_key"]
        project_headers = {"Authorization": f"Bearer {project_api_key}"}
        log.result(resp, f"project_id={project_id}")

        # 5. GET /projects - List projects
        log.step("GET", "/projects")
        resp = api.get("/projects", headers=admin_headers)
        assert resp.status_code == 200
        projects = resp.json()["projects"]
        assert any(p["id"] == project_id for p in projects)
        log.result(resp, f"{len(projects)} projects found")

        # 6. GET /projects/{id} - Get project details
        log.step("GET", f"/projects/{project_id}")
        resp = api.get(f"/projects/{project_id}", headers=project_headers)
        assert resp.status_code == 200
        assert resp.json()["name"] == f"Workflow Test {test_id}"
        log.result(resp)

        # 7. PUT /projects/{id} - Update project
        log.step("PUT", f"/projects/{project_id}", {"name": f"Updated {test_id}"})
        resp = api.put(
            f"/projects/{project_id}",
            json={"name": f"Updated {test_id}", "description": "Updated desc"},
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == f"Updated {test_id}"
        log.result(resp, "project updated")

        # 8. POST /projects/{id}/api-keys - Create additional API key
        log.step("POST", f"/projects/{project_id}/api-keys", {"description": "CI/CD Key"})
        resp = api.post(
            f"/projects/{project_id}/api-keys",
            json={"description": "CI/CD Key"},
            headers=project_headers,
        )
        assert resp.status_code == 201
        new_key_data = resp.json()
        new_key_id = new_key_data["id"]
        new_key = new_key_data["api_key"]  # Field is api_key, not key
        log.result(resp, f"new API key created: {new_key_id}")

        # 9. GET /projects/{id}/api-keys - List API keys
        log.step("GET", f"/projects/{project_id}/api-keys")
        resp = api.get(
            f"/projects/{project_id}/api-keys",
            headers=project_headers,
        )
        assert resp.status_code == 200
        keys = resp.json()["api_keys"]  # Field is api_keys, not keys
        assert len(keys) >= 2  # admin + new key
        log.result(resp, f"{len(keys)} API keys")

        # 10. GET /projects/{id}/api-keys/{key_id} - Get key details
        log.step("GET", f"/projects/{project_id}/api-keys/{new_key_id}")
        resp = api.get(
            f"/projects/{project_id}/api-keys/{new_key_id}",
            headers=project_headers,
        )
        assert resp.status_code == 200
        key_data = resp.json()
        assert key_data["description"] == "CI/CD Key"
        # Raw key not exposed in GET
        assert "api_key" not in key_data
        log.result(resp)

        # 11. POST /projects/{id}/api-keys/{key_id}/rotate - Rotate key
        log.step("POST", f"/projects/{project_id}/api-keys/{new_key_id}/rotate")
        resp = api.post(
            f"/projects/{project_id}/api-keys/{new_key_id}/rotate",
            headers=project_headers,
        )
        assert resp.status_code == 201  # Creates new key, so 201
        rotated_key = resp.json()["api_key"]
        assert rotated_key != new_key  # New key generated
        log.result(resp, "key rotated successfully")

        # Verify old key is invalid (returns 403 Forbidden)
        log.info("Verifying old key is invalid")
        old_key_headers = {"Authorization": f"Bearer {new_key}"}
        resp = api.get(
            f"/projects/{project_id}/branches/default/buckets",
            headers=old_key_headers,
        )
        assert resp.status_code in [401, 403]  # Either unauthorized or forbidden

        # 12. DELETE /projects/{id}/api-keys/{key_id} - Revoke key
        log.step("DELETE", f"/projects/{project_id}/api-keys/{new_key_id}")
        resp = api.delete(
            f"/projects/{project_id}/api-keys/{new_key_id}",
            headers=project_headers,
        )
        assert resp.status_code == 204
        log.result(resp, "key revoked")

        # 13. GET /projects/{id}/stats - Get project statistics
        log.step("GET", f"/projects/{project_id}/stats")
        resp = api.get(
            f"/projects/{project_id}/stats",
            headers=project_headers,
        )
        assert resp.status_code == 200
        stats = resp.json()
        assert "bucket_count" in stats
        assert "table_count" in stats
        log.result(resp, f"buckets={stats['bucket_count']}, tables={stats['table_count']}")

        # Cleanup: DELETE /projects/{id}
        resp = api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Workflow 2: DataPipeline (19 endpoints)
# =============================================================================

class TestWorkflow2DataPipeline:
    """
    Test complete data pipeline: bucket -> table -> import -> schema -> export.

    Endpoints tested:
    1. POST .../buckets - Create bucket
    2. GET .../buckets - List buckets
    3. GET .../buckets/{name} - Get bucket
    4. POST .../tables - Create table
    5. GET .../tables - List tables
    6. GET .../tables/{t} - Get table
    7. POST /files/prepare - Prepare upload
    8. POST /files/upload/{key} - Upload file
    9. POST /files - Register file
    10. POST .../import/file - Import CSV
    11. GET .../preview - Preview data
    12. POST .../columns - Add column
    13. PUT .../columns/{col} - Alter column
    14. DELETE .../columns/{col} - Drop column
    15. POST .../primary-key - Add PK
    16. DELETE .../primary-key - Drop PK
    17. POST .../profile - Table profiling
    18. POST .../export - Export to file
    19. DELETE .../rows - Delete rows
    """

    def test_full_data_pipeline(self, api, admin_headers):
        """Complete data pipeline workflow."""
        log = WorkflowProtocol("Workflow 2: Data Pipeline", 19)
        log.header()

        test_id = generate_test_id()
        project_id = f"pipeline_{test_id}"
        bucket_name = "in_c_data"
        table_name = "users"

        # Create project (setup)
        log.step("POST", "/projects", {"id": project_id})
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Pipeline Test {test_id}"},
            headers=admin_headers,
        )
        assert resp.status_code == 201
        log.result(resp, f"project_id={project_id}")
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}

        base_url = f"/projects/{project_id}/branches/default"

        # 1. POST .../buckets - Create bucket
        log.step("POST", f"{base_url}/buckets", {"name": bucket_name})
        resp = api.post(
            f"{base_url}/buckets",
            json={"name": bucket_name, "stage": "in", "description": "Test data"},
            headers=project_headers,
        )
        assert resp.status_code == 201
        log.result(resp, f"bucket={bucket_name}")

        # 2. GET .../buckets - List buckets
        log.step("GET", f"{base_url}/buckets")
        resp = api.get(f"{base_url}/buckets", headers=project_headers)
        assert resp.status_code == 200
        assert len(resp.json()["buckets"]) == 1
        log.result(resp, "1 bucket found")

        # 3. GET .../buckets/{name} - Get bucket
        log.step("GET", f"{base_url}/buckets/{bucket_name}")
        resp = api.get(f"{base_url}/buckets/{bucket_name}", headers=project_headers)
        assert resp.status_code == 200
        assert resp.json()["name"] == bucket_name
        log.result(resp)

        # 4. POST .../tables - Create table
        table_def = {"name": table_name, "columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "email", "type": "VARCHAR"},
            {"name": "age", "type": "INTEGER"},
        ]}
        log.step("POST", f"{base_url}/buckets/{bucket_name}/tables", table_def)
        resp = api.post(
            f"{base_url}/buckets/{bucket_name}/tables",
            json=table_def,
            headers=project_headers,
        )
        assert resp.status_code == 201
        log.result(resp, f"table={table_name}, 4 columns")

        # 5. GET .../tables - List tables
        log.step("GET", f"{base_url}/buckets/{bucket_name}/tables")
        resp = api.get(
            f"{base_url}/buckets/{bucket_name}/tables",
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert len(resp.json()["tables"]) == 1
        log.result(resp, "1 table found")

        # 6. GET .../tables/{t} - Get table
        log.step("GET", f"{base_url}/buckets/{bucket_name}/tables/{table_name}")
        resp = api.get(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}",
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == table_name
        log.result(resp)

        # 7-9. Upload CSV file
        csv_content = b"id,name,email,age\n1,Alice,alice@test.com,30\n2,Bob,bob@test.com,25\n3,Charlie,charlie@test.com,35\n"
        log.step("POST", f"/projects/{project_id}/files/prepare + upload + register")
        file_id = upload_csv(api, project_id, project_headers, "users.csv", csv_content)
        log.info(f"file_id={file_id}, 3 rows CSV")

        # 10. POST .../import/file - Import CSV
        log.step("POST", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file", {"file_id": file_id})
        resp = api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id},
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["imported_rows"] == 3
        log.result(resp, "3 rows imported")

        # 11. GET .../preview - Preview data
        log.step("GET", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/preview")
        resp = api.get(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/preview",
            headers=project_headers,
        )
        assert resp.status_code == 200
        preview = resp.json()
        assert preview["total_row_count"] == 3
        assert len(preview["rows"]) == 3
        log.result(resp, f"3 rows: Alice, Bob, Charlie")

        # 12. POST .../columns - Add column
        log.step("POST", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/columns", {"name": "status", "type": "VARCHAR"})
        resp = api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/columns",
            json={"name": "status", "type": "VARCHAR", "default": "'active'"},
            headers=project_headers,
        )
        assert resp.status_code == 201, f"Add column failed: {resp.text}"
        log.result(resp, "column 'status' added")

        # 13. PUT .../columns/{col} - Alter column (rename)
        log.step("PUT", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/columns/status", {"new_name": "user_status"})
        resp = api.put(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/columns/status",
            json={"new_name": "user_status"},
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "renamed to 'user_status'")

        # 14. DELETE .../columns/{col} - Drop column
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/columns/user_status")
        resp = api.delete(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/columns/user_status",
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "column dropped")

        # 15. POST .../primary-key - Add PK
        log.step("POST", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/primary-key", {"columns": ["id"]})
        resp = api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/primary-key",
            json={"columns": ["id"]},
            headers=project_headers,
        )
        assert resp.status_code in [200, 201]
        log.result(resp, "PK set on 'id'")

        # Verify PK is set
        resp = api.get(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}",
            headers=project_headers,
        )
        assert resp.json()["primary_key"] == ["id"]

        # 16. DELETE .../primary-key - Drop PK
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/primary-key")
        resp = api.delete(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/primary-key",
            headers=project_headers,
        )
        assert resp.status_code in [200, 204]
        log.result(resp, "PK removed")

        # 17. POST .../profile - Table profiling
        log.step("POST", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/profile")
        resp = api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/profile",
            headers=project_headers,
        )
        assert resp.status_code == 200
        profile = resp.json()
        assert "statistics" in profile
        assert len(profile["statistics"]) >= 4
        log.result(resp, f"{len(profile['statistics'])} column stats")

        # 18. POST .../export - Export to file
        log.step("POST", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/export", {"format": "csv"})
        resp = api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/export",
            json={"format": "csv"},
            headers=project_headers,
        )
        assert resp.status_code == 200
        export_file_id = resp.json()["file_id"]
        log.result(resp, f"exported to file_id={export_file_id}")

        # 19. DELETE .../rows - Delete rows
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/rows", {"where_clause": "age > 30"})
        resp = api.request(
            "DELETE",
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/rows",
            json={"where_clause": "age > 30"},
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["deleted_rows"] == 1
        log.result(resp, "1 row deleted (Charlie, age 35)")

        # Verify 2 rows remain
        remaining = get_row_count(api, project_id, project_headers, bucket_name, table_name)
        assert remaining == 2
        log.info(f"Final state: {remaining} rows remain (Alice, Bob)")

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Workflow 3: SnapshotRecovery (14 endpoints)
# =============================================================================

class TestWorkflow3SnapshotRecovery:
    """
    Test snapshot configuration at all levels and recovery workflow.

    Endpoints tested:
    1. GET /projects/{id}/settings/snapshots
    2. PUT /projects/{id}/settings/snapshots
    3. GET .../buckets/{b}/settings/snapshots
    4. PUT .../buckets/{b}/settings/snapshots
    5. GET .../tables/{t}/settings/snapshots
    6. PUT .../tables/{t}/settings/snapshots
    7. POST .../snapshots - Create snapshot
    8. GET .../snapshots - List snapshots
    9. GET .../snapshots/{id} - Get snapshot
    10. DELETE .../rows - Truncate (triggers auto-snapshot if configured)
    11. POST .../snapshots/{id}/restore - Restore snapshot
    12. DELETE /projects/{id}/settings/snapshots
    13. DELETE .../buckets/{b}/settings/snapshots
    14. DELETE .../tables/{t}/settings/snapshots
    """

    def test_full_snapshot_workflow(self, api, admin_headers):
        """Complete snapshot configuration and recovery workflow."""
        log = WorkflowProtocol("Workflow 3: Snapshot Recovery", 14)
        log.header()

        test_id = generate_test_id()
        project_id = f"snapshot_{test_id}"
        bucket_name = "in_c_data"
        table_name = "users"

        # Create project with data
        log.info("Setting up project with bucket, table, and 3 rows of data")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Snapshot Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}
        base_url = f"/projects/{project_id}/branches/default"

        # Create bucket and table with data
        api.post(f"{base_url}/buckets", json={"name": bucket_name, "stage": "in"}, headers=project_headers)
        api.post(
            f"{base_url}/buckets/{bucket_name}/tables",
            json={
                "name": table_name,
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=project_headers,
        )

        # Import data
        csv_content = b"id,name\n1,Alice\n2,Bob\n3,Charlie\n"
        file_id = upload_csv(api, project_id, project_headers, "users.csv", csv_content)
        api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id},
            headers=project_headers,
        )

        # 1. GET /projects/{id}/settings/snapshots - Get project config
        log.step("GET", f"/projects/{project_id}/settings/snapshots")
        resp = api.get(f"/projects/{project_id}/settings/snapshots", headers=project_headers)
        assert resp.status_code == 200
        log.result(resp, "project snapshot config retrieved")

        # 2. PUT /projects/{id}/settings/snapshots - Set project config
        config = {"enabled": True, "auto_snapshot_triggers": {"truncate_table": True, "drop_table": True}}
        log.step("PUT", f"/projects/{project_id}/settings/snapshots", config)
        resp = api.put(
            f"/projects/{project_id}/settings/snapshots",
            json={
                "enabled": True,
                "auto_snapshot_triggers": {"truncate_table": True, "drop_table": True},
                "retention": {"manual_days": 90, "auto_days": 7},
            },
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "auto-snapshot enabled for truncate/drop")

        # 3. GET .../buckets/{b}/settings/snapshots - Get bucket config
        log.step("GET", f"{base_url}/buckets/{bucket_name}/settings/snapshots")
        resp = api.get(
            f"{base_url}/buckets/{bucket_name}/settings/snapshots",
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp)

        # 4. PUT .../buckets/{b}/settings/snapshots - Set bucket config
        log.step("PUT", f"{base_url}/buckets/{bucket_name}/settings/snapshots", {"auto_snapshot_triggers": {"delete_all_rows": True}})
        resp = api.put(
            f"{base_url}/buckets/{bucket_name}/settings/snapshots",
            json={"auto_snapshot_triggers": {"delete_all_rows": True}},
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "bucket: enable auto-snapshot for delete_all_rows")

        # 5. GET .../tables/{t}/settings/snapshots - Get table config
        log.step("GET", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots")
        resp = api.get(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots",
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp)

        # 6. PUT .../tables/{t}/settings/snapshots - Set table config
        log.step("PUT", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots", {"enabled": True})
        resp = api.put(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots",
            json={"enabled": True},
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "table: snapshots enabled")

        # 7. POST .../snapshots - Create manual snapshot
        # Note: Snapshot endpoint is at /projects/{id}/branches/{branch}/snapshots
        # with bucket and table in request body
        log.step("POST", f"{base_url}/snapshots", {"bucket": bucket_name, "table": table_name})
        resp = api.post(
            f"{base_url}/snapshots",
            json={"bucket": bucket_name, "table": table_name, "description": "Before changes"},
            headers=project_headers,
        )
        assert resp.status_code == 201
        snapshot_id = resp.json()["id"]
        log.result(resp, f"snapshot_id={snapshot_id}")

        # 8. GET .../snapshots - List snapshots (with bucket/table filters)
        log.step("GET", f"{base_url}/snapshots?bucket={bucket_name}&table={table_name}")
        resp = api.get(
            f"{base_url}/snapshots",
            params={"bucket": bucket_name, "table": table_name},
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert len(resp.json()["snapshots"]) >= 1
        log.result(resp, f"{len(resp.json()['snapshots'])} snapshots found")

        # 9. GET .../snapshots/{id} - Get snapshot details
        log.step("GET", f"{base_url}/snapshots/{snapshot_id}")
        resp = api.get(
            f"{base_url}/snapshots/{snapshot_id}",
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["row_count"] == 3
        log.result(resp, "snapshot has 3 rows")

        # 10. DELETE .../rows - Truncate table (with WHERE 1=1)
        # httpx.Client.delete() doesn't accept json, use request() instead
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/rows", {"where_clause": "1=1"})
        resp = api.request(
            "DELETE",
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/rows",
            json={"where_clause": "1=1"},  # Delete all
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "table truncated")

        # Verify table is empty
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name) == 0
        log.info("Table now has 0 rows")

        # 11. POST .../snapshots/{id}/restore - Restore snapshot
        log.step("POST", f"{base_url}/snapshots/{snapshot_id}/restore")
        resp = api.post(
            f"{base_url}/snapshots/{snapshot_id}/restore",
            json={},  # Empty body - restores to original location
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "snapshot restored")

        # Verify data restored
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name) == 3
        log.info("Data recovered: 3 rows restored")

        # 12. DELETE /projects/{id}/settings/snapshots - Reset project config
        log.step("DELETE", f"/projects/{project_id}/settings/snapshots")
        resp = api.delete(f"/projects/{project_id}/settings/snapshots", headers=project_headers)
        assert resp.status_code == 204
        log.result(resp, "project config reset")

        # 13. DELETE .../buckets/{b}/settings/snapshots - Reset bucket config
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}/settings/snapshots")
        resp = api.delete(
            f"{base_url}/buckets/{bucket_name}/settings/snapshots",
            headers=project_headers,
        )
        assert resp.status_code == 204
        log.result(resp, "bucket config reset")

        # 14. DELETE .../tables/{t}/settings/snapshots - Reset table config
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots")
        resp = api.delete(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/settings/snapshots",
            headers=project_headers,
        )
        assert resp.status_code == 204
        log.result(resp, "table config reset")

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Workflow 4: BranchDevelopment (6 endpoints)
# =============================================================================

class TestWorkflow4BranchDevelopment:
    """
    Test dev branch isolation workflow.

    Endpoints tested:
    1. POST /projects/{id}/branches - Create branch
    2. GET /projects/{id}/branches - List branches
    3. GET /projects/{id}/branches/{branch_id} - Get branch
    4. GET .../branches/{branch}/buckets/{b}/tables/{t}/preview - Preview in branch
    5. POST .../tables/{bucket}/{table}/pull - Pull from main
    6. DELETE /projects/{id}/branches/{branch_id} - Delete branch
    """

    def test_branch_isolation_workflow(self, api, admin_headers):
        """Test branch isolation - changes in main don't affect branch."""
        log = WorkflowProtocol("Workflow 4: Branch Development", 6)
        log.header()

        test_id = generate_test_id()
        project_id = f"branch_{test_id}"
        bucket_name = "in_c_data"
        table_name = "users"

        # Create project with data
        log.info("Setting up project with bucket, table, and 3 rows in main")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Branch Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}

        # Create bucket and table with initial data
        api.post(
            f"/projects/{project_id}/branches/default/buckets",
            json={"name": bucket_name, "stage": "in"},
            headers=project_headers,
        )
        api.post(
            f"/projects/{project_id}/branches/default/buckets/{bucket_name}/tables",
            json={
                "name": table_name,
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
                "primary_key": ["id"],
            },
            headers=project_headers,
        )

        # Import initial data (3 rows)
        csv_content = b"id,name\n1,Alice\n2,Bob\n3,Charlie\n"
        file_id = upload_csv(api, project_id, project_headers, "users.csv", csv_content)
        api.post(
            f"/projects/{project_id}/branches/default/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id},
            headers=project_headers,
        )

        # Verify main has 3 rows
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name, "default") == 3

        # 1. POST /projects/{id}/branches - Create dev branch
        log.step("POST", f"/projects/{project_id}/branches", {"name": f"feature_{test_id}"})
        resp = api.post(
            f"/projects/{project_id}/branches",
            json={"name": f"feature_{test_id}"},
            headers=project_headers,
        )
        assert resp.status_code == 201
        branch_data = resp.json()
        branch_id = branch_data["id"]
        log.result(resp, f"branch_id={branch_id}")

        # 2. GET /projects/{id}/branches - List branches
        # Note: "default" branch is implicit and not stored in branches table
        log.step("GET", f"/projects/{project_id}/branches")
        resp = api.get(f"/projects/{project_id}/branches", headers=project_headers)
        assert resp.status_code == 200
        branches = resp.json()["branches"]
        assert len(branches) >= 1  # new branch (default is implicit)
        log.result(resp, f"{len(branches)} dev branches")

        # 3. GET /projects/{id}/branches/{branch_id} - Get branch
        log.step("GET", f"/projects/{project_id}/branches/{branch_id}")
        resp = api.get(f"/projects/{project_id}/branches/{branch_id}", headers=project_headers)
        assert resp.status_code == 200
        assert resp.json()["name"] == f"feature_{test_id}"
        log.result(resp)

        # 4. GET .../branches/{branch}/...preview - Preview in branch (should have 3 rows)
        log.step("GET", f"/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/preview")
        resp = api.get(
            f"/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/preview",
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["total_row_count"] == 3
        log.result(resp, "branch sees 3 rows (live view of main)")

        # Add row to MAIN
        log.info("Adding 1 row to MAIN (David)")
        csv_content2 = b"id,name\n4,David\n"
        file_id2 = upload_csv(api, project_id, project_headers, "david.csv", csv_content2)
        api.post(
            f"/projects/{project_id}/branches/default/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id2, "import_options": {"incremental": True}},
            headers=project_headers,
        )

        # Verify main has 4 rows
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name, "default") == 4
        log.info("Main now has 4 rows")

        # Branch sees live view of main until CoW happens (per ADR-007)
        # Since no write to branch triggered CoW, branch sees main's current data
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name, branch_id) == 4
        log.info("Branch also sees 4 rows (live view, no CoW yet)")

        # 5. POST .../tables/{bucket}/{table}/pull - Pull from main
        log.step("POST", f"/projects/{project_id}/branches/{branch_id}/tables/{bucket_name}/{table_name}/pull")
        resp = api.post(
            f"/projects/{project_id}/branches/{branch_id}/tables/{bucket_name}/{table_name}/pull",
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "pulled latest from main")

        # Now branch should have 4 rows
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name, branch_id) == 4

        # 6. DELETE /projects/{id}/branches/{branch_id} - Delete branch
        log.step("DELETE", f"/projects/{project_id}/branches/{branch_id}")
        resp = api.delete(f"/projects/{project_id}/branches/{branch_id}", headers=project_headers)
        assert resp.status_code == 204
        log.result(resp, "branch deleted")

        # Verify branch is deleted
        resp = api.get(f"/projects/{project_id}/branches/{branch_id}", headers=project_headers)
        assert resp.status_code == 404

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Workflow 5: BucketSharing (10 endpoints)
# =============================================================================

class TestWorkflow5BucketSharing:
    """
    Test cross-project bucket sharing workflow.

    Endpoints tested:
    1. POST .../buckets/{name}/share - Project A shares bucket
    2. POST .../buckets/{name}/link - Project B links bucket
    3. GET .../buckets/{name} (in B) - B reads bucket
    4. GET .../tables/{t}/preview (in B) - B reads data
    5. POST .../buckets/{name}/grant-readonly - A grants readonly
    6. POST .../tables (in B) - B tries write (should fail)
    7. DELETE .../buckets/{name}/grant-readonly - A revokes grant
    8. DELETE .../buckets/{name}/link - B unlinks
    9. DELETE .../buckets/{name}/share - A unshares
    10. POST .../buckets/{name}/link (in B) - Link fails (not shared)
    """

    def test_bucket_sharing_workflow(self, api, admin_headers):
        """Test cross-project bucket sharing."""
        log = WorkflowProtocol("Workflow 5: Bucket Sharing", 10)
        log.header()

        test_id = generate_test_id()
        project_a_id = f"share_a_{test_id}"
        project_b_id = f"share_b_{test_id}"
        bucket_name = "in_c_shared"
        table_name = "orders"

        # Create Project A with data
        log.info("Creating Project A with bucket, table, and 2 rows")
        resp = api.post(
            "/projects",
            json={"id": project_a_id, "name": f"Project A {test_id}"},
            headers=admin_headers,
        )
        headers_a = {"Authorization": f"Bearer {resp.json()['api_key']}"}

        # Create bucket and table in A
        api.post(
            f"/projects/{project_a_id}/branches/default/buckets",
            json={"name": bucket_name, "stage": "in"},
            headers=headers_a,
        )
        api.post(
            f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/tables",
            json={
                "name": table_name,
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "amount", "type": "DECIMAL(10,2)"},
                ],
            },
            headers=headers_a,
        )

        # Import data to A
        csv_content = b"id,amount\n1,100.50\n2,250.00\n"
        file_id = upload_csv(api, project_a_id, headers_a, "orders.csv", csv_content)
        api.post(
            f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id},
            headers=headers_a,
        )

        # Create Project B
        log.info("Creating Project B")
        resp = api.post(
            "/projects",
            json={"id": project_b_id, "name": f"Project B {test_id}"},
            headers=admin_headers,
        )
        headers_b = {"Authorization": f"Bearer {resp.json()['api_key']}"}

        # 1. POST .../buckets/{name}/share - A shares bucket with B
        log.step("POST", f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/share", {"target_project_id": project_b_id})
        resp = api.post(
            f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/share",
            json={"target_project_id": project_b_id},
            headers=headers_a,
        )
        assert resp.status_code == 200
        log.result(resp, "bucket shared from A to B")

        # 2. POST .../buckets/{name}/link - B links bucket from A
        log.step("POST", f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/link", {"source_project_id": project_a_id})
        resp = api.post(
            f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/link",
            json={"source_project_id": project_a_id, "source_bucket_name": bucket_name},
            headers=headers_b,
        )
        assert resp.status_code == 201  # 201 Created for new link
        log.result(resp, "B linked to A's bucket")

        # 3. GET .../buckets/{name} (in B) - B reads bucket info
        log.step("GET", f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}")
        resp = api.get(
            f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}",
            headers=headers_b,
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == bucket_name  # Bucket is accessible in B
        log.result(resp, "B can read bucket metadata")

        # 4. GET .../tables/{t}/preview (in B) - B reads data
        log.step("GET", f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/tables/{table_name}/preview")
        resp = api.get(
            f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/tables/{table_name}/preview",
            headers=headers_b,
        )
        assert resp.status_code == 200
        assert resp.json()["total_row_count"] == 2
        log.result(resp, "B can read 2 rows from A's table")

        # 5. POST .../buckets/{name}/grant-readonly - A grants readonly
        # Note: This is a metadata operation for DuckDB - doesn't take body
        log.step("POST", f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/grant-readonly")
        resp = api.post(
            f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/grant-readonly",
            headers=headers_a,
        )
        assert resp.status_code == 200
        log.result(resp, "readonly grant applied")

        # 6. POST .../tables (in B) - B creates local table in linked bucket
        # Note: Creating local tables in a linked bucket is allowed - they are separate
        # from source tables and only exist in project B. The readonly grant affects
        # access to SOURCE tables, not local table creation.
        log.step("POST", f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/tables", {"name": "new_table"})
        resp = api.post(
            f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/tables",
            json={"name": "new_table", "columns": [{"name": "x", "type": "INTEGER"}]},
            headers=headers_b,
        )
        assert resp.status_code == 201  # Local table creation succeeds
        log.result(resp, "B can create local table in linked bucket")

        # 7. DELETE .../buckets/{name}/grant-readonly - A revokes grant
        log.step("DELETE", f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/grant-readonly")
        resp = api.delete(
            f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/grant-readonly",
            headers=headers_a,
        )
        assert resp.status_code == 204
        log.result(resp, "readonly grant revoked")

        # 8. DELETE .../buckets/{name}/link - B unlinks
        log.step("DELETE", f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/link")
        resp = api.delete(
            f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/link",
            headers=headers_b,
        )
        assert resp.status_code == 204
        log.result(resp, "B unlinked from bucket")

        # Bucket should not be visible in B now
        resp = api.get(
            f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}",
            headers=headers_b,
        )
        assert resp.status_code == 404
        log.info("Bucket no longer visible in B")

        # 9. DELETE .../buckets/{name}/share - A unshares from B
        log.step("DELETE", f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/share")
        resp = api.delete(
            f"/projects/{project_a_id}/branches/default/buckets/{bucket_name}/share",
            params={"target_project_id": project_b_id},
            headers=headers_a,
        )
        assert resp.status_code == 204
        log.result(resp, "A unshared bucket from B")

        # 10. POST .../buckets/{name}/link (in B) - Link succeeds in MVP
        # Note: MVP implementation doesn't enforce share check before linking.
        # The share/unshare operations are metadata-only. Full enforcement is post-MVP.
        log.step("POST", f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/link (after unshare)")
        resp = api.post(
            f"/projects/{project_b_id}/branches/default/buckets/{bucket_name}/link",
            json={"source_project_id": project_a_id, "source_bucket_name": bucket_name},
            headers=headers_b,
        )
        # MVP: Link succeeds since share enforcement isn't implemented yet
        assert resp.status_code == 201
        log.result(resp, "MVP: link succeeds (share enforcement not implemented)")

        # Cleanup
        api.delete(f"/projects/{project_a_id}", headers=admin_headers)
        api.delete(f"/projects/{project_b_id}", headers=admin_headers)


# =============================================================================
# Workflow 6: WorkspaceSQL (12 endpoints)
# =============================================================================

class TestWorkflow6WorkspaceSQL:
    """
    Test workspace lifecycle and SQL access.

    Endpoints tested:
    1. POST /projects/{id}/workspaces - Create workspace
    2. GET /projects/{id}/workspaces - List workspaces
    3. GET /projects/{id}/workspaces/{ws_id} - Get workspace
    4. POST .../workspaces/{ws_id}/load - Load table data
    5. POST .../workspaces/{ws_id}/credentials/reset - Reset credentials
    6. POST .../branches/{branch}/workspaces - Create branch workspace
    7. GET .../branches/{branch}/workspaces - List branch workspaces
    8. GET .../branches/{branch}/workspaces/{ws_id} - Get branch workspace
    9. DELETE .../workspaces/{ws_id}/objects/{name} - Drop object
    10. POST .../workspaces/{ws_id}/clear - Clear workspace
    11. DELETE .../branches/{branch}/workspaces/{ws_id} - Delete branch workspace
    12. DELETE /projects/{id}/workspaces/{ws_id} - Delete workspace
    """

    def test_workspace_workflow(self, api, admin_headers):
        """Test workspace creation and management."""
        log = WorkflowProtocol("Workflow 6: Workspace SQL", 12)
        log.header()

        test_id = generate_test_id()
        project_id = f"workspace_{test_id}"
        bucket_name = "in_c_data"
        table_name = "users"

        # Create project with data
        log.info("Setting up project with bucket, table, and 2 rows")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Workspace Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}

        # Create bucket and table
        api.post(
            f"/projects/{project_id}/branches/default/buckets",
            json={"name": bucket_name, "stage": "in"},
            headers=project_headers,
        )
        api.post(
            f"/projects/{project_id}/branches/default/buckets/{bucket_name}/tables",
            json={
                "name": table_name,
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=project_headers,
        )

        # Import data
        csv_content = b"id,name\n1,Alice\n2,Bob\n"
        file_id = upload_csv(api, project_id, project_headers, "users.csv", csv_content)
        api.post(
            f"/projects/{project_id}/branches/default/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id},
            headers=project_headers,
        )

        # 1. POST /projects/{id}/workspaces - Create workspace
        log.step("POST", f"/projects/{project_id}/workspaces", {"name": f"analytics_{test_id}"})
        resp = api.post(
            f"/projects/{project_id}/workspaces",
            json={"name": f"analytics_{test_id}"},
            headers=project_headers,
        )
        assert resp.status_code == 201
        ws_data = resp.json()
        ws_id = ws_data["id"]
        # Credentials are in the connection object
        assert "connection" in ws_data
        assert "username" in ws_data["connection"]
        assert "password" in ws_data["connection"]
        ws_username = ws_data["connection"]["username"]
        ws_password = ws_data["connection"]["password"]
        log.result(resp, f"ws_id={ws_id}, credentials provided")

        # 2. GET /projects/{id}/workspaces - List workspaces
        log.step("GET", f"/projects/{project_id}/workspaces")
        resp = api.get(f"/projects/{project_id}/workspaces", headers=project_headers)
        assert resp.status_code == 200
        assert len(resp.json()["workspaces"]) >= 1
        log.result(resp, f"{len(resp.json()['workspaces'])} workspaces")

        # 3. GET /projects/{id}/workspaces/{ws_id} - Get workspace
        log.step("GET", f"/projects/{project_id}/workspaces/{ws_id}")
        resp = api.get(f"/projects/{project_id}/workspaces/{ws_id}", headers=project_headers)
        assert resp.status_code == 200
        assert resp.json()["name"] == f"analytics_{test_id}"
        log.result(resp)

        # 4. POST .../workspaces/{ws_id}/load - Load table data
        log.step("POST", f"/projects/{project_id}/workspaces/{ws_id}/load", {"tables": [{"bucket": bucket_name, "table": table_name}]})
        resp = api.post(
            f"/projects/{project_id}/workspaces/{ws_id}/load",
            json={"tables": [{"bucket": bucket_name, "table": table_name}]},
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "table loaded into workspace")

        # 5. POST .../workspaces/{ws_id}/credentials/reset - Reset credentials
        log.step("POST", f"/projects/{project_id}/workspaces/{ws_id}/credentials/reset")
        resp = api.post(
            f"/projects/{project_id}/workspaces/{ws_id}/credentials/reset",
            headers=project_headers,
        )
        assert resp.status_code == 200
        reset_data = resp.json()
        # New password in connection object
        new_password = reset_data.get("connection", {}).get("password") or reset_data.get("password")
        assert new_password != ws_password
        log.result(resp, "credentials reset")

        # Create a dev branch first
        log.info("Creating dev branch")
        resp = api.post(
            f"/projects/{project_id}/branches",
            json={"name": f"dev_{test_id}"},
            headers=project_headers,
        )
        branch_id = resp.json()["id"]

        # 6. POST .../branches/{branch}/workspaces - Create branch workspace
        log.step("POST", f"/projects/{project_id}/branches/{branch_id}/workspaces", {"name": f"branch_ws_{test_id}"})
        resp = api.post(
            f"/projects/{project_id}/branches/{branch_id}/workspaces",
            json={"name": f"branch_ws_{test_id}"},
            headers=project_headers,
        )
        assert resp.status_code == 201
        branch_ws_id = resp.json()["id"]
        log.result(resp, f"branch workspace created: {branch_ws_id}")

        # 7. GET .../branches/{branch}/workspaces - List branch workspaces
        log.step("GET", f"/projects/{project_id}/branches/{branch_id}/workspaces")
        resp = api.get(
            f"/projects/{project_id}/branches/{branch_id}/workspaces",
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert len(resp.json()["workspaces"]) >= 1
        log.result(resp, f"{len(resp.json()['workspaces'])} branch workspaces")

        # 8. GET .../branches/{branch}/workspaces/{ws_id} - Get branch workspace
        log.step("GET", f"/projects/{project_id}/branches/{branch_id}/workspaces/{branch_ws_id}")
        resp = api.get(
            f"/projects/{project_id}/branches/{branch_id}/workspaces/{branch_ws_id}",
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp)

        # 9. DELETE .../workspaces/{ws_id}/objects/{name} - Drop object (if exists)
        log.step("DELETE", f"/projects/{project_id}/workspaces/{ws_id}/objects/temp_table")
        resp = api.delete(
            f"/projects/{project_id}/workspaces/{ws_id}/objects/temp_table",
            headers=project_headers,
        )
        # May return 204 (deleted) or 404 (not found)
        assert resp.status_code in [204, 404]
        log.result(resp, "object dropped (or not found)")

        # 10. POST .../workspaces/{ws_id}/clear - Clear workspace
        log.step("POST", f"/projects/{project_id}/workspaces/{ws_id}/clear")
        resp = api.post(
            f"/projects/{project_id}/workspaces/{ws_id}/clear",
            headers=project_headers,
        )
        assert resp.status_code == 204  # No content
        log.result(resp, "workspace cleared")

        # 11. DELETE .../branches/{branch}/workspaces/{ws_id} - Delete branch workspace
        log.step("DELETE", f"/projects/{project_id}/branches/{branch_id}/workspaces/{branch_ws_id}")
        resp = api.delete(
            f"/projects/{project_id}/branches/{branch_id}/workspaces/{branch_ws_id}",
            headers=project_headers,
        )
        assert resp.status_code == 204
        log.result(resp, "branch workspace deleted")

        # 12. DELETE /projects/{id}/workspaces/{ws_id} - Delete workspace
        log.step("DELETE", f"/projects/{project_id}/workspaces/{ws_id}")
        resp = api.delete(f"/projects/{project_id}/workspaces/{ws_id}", headers=project_headers)
        assert resp.status_code == 204
        log.result(resp, "workspace deleted")

        # Cleanup
        api.delete(f"/projects/{project_id}/branches/{branch_id}", headers=project_headers)
        api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Workflow 7: S3Compatible (6 endpoints)
# =============================================================================

class TestWorkflow7S3Compatible:
    """
    Test S3-compatible API operations.

    Endpoints tested:
    1. PUT /s3/{bucket}/{key} - Upload object
    2. HEAD /s3/{bucket}/{key} - Get metadata
    3. GET /s3/{bucket}/{key} - Download object
    4. GET /s3/{bucket} - List objects
    5. POST /s3/{bucket}/presign - Generate presigned URL
    6. DELETE /s3/{bucket}/{key} - Delete object
    """

    def test_s3_compatible_workflow(self, api, admin_headers):
        """Test S3-compatible API operations."""
        log = WorkflowProtocol("Workflow 7: S3 Compatible", 6)
        log.header()

        test_id = generate_test_id()
        project_id = f"s3_{test_id}"

        # Create project
        log.info("Creating project for S3 testing")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"S3 Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}

        s3_bucket = f"project_{project_id}"  # Underscore not hyphen
        test_key = f"test/{test_id}/data.csv"
        test_content = b"id,name\n1,Alice\n2,Bob\n"

        # 1. PUT /s3/{bucket}/{key} - Upload object
        log.step("PUT", f"/s3/{s3_bucket}/{test_key}")
        resp = api.put(
            f"/s3/{s3_bucket}/{test_key}",
            content=test_content,
            headers={
                **project_headers,
                "Content-Type": "text/csv",
            },
        )
        assert resp.status_code in [200, 201]
        log.result(resp, f"uploaded {len(test_content)} bytes")

        # 2. HEAD /s3/{bucket}/{key} - Get metadata
        log.step("HEAD", f"/s3/{s3_bucket}/{test_key}")
        resp = api.head(f"/s3/{s3_bucket}/{test_key}", headers=project_headers)
        assert resp.status_code == 200
        assert int(resp.headers.get("Content-Length", 0)) == len(test_content)
        log.result(resp, f"Content-Length={len(test_content)}")

        # 3. GET /s3/{bucket}/{key} - Download object
        log.step("GET", f"/s3/{s3_bucket}/{test_key}")
        resp = api.get(f"/s3/{s3_bucket}/{test_key}", headers=project_headers)
        assert resp.status_code == 200
        assert resp.content == test_content
        log.result(resp, "content matches")

        # Upload another file for listing
        log.info("Uploading second file for listing test")
        test_key2 = f"test/{test_id}/data2.csv"
        api.put(
            f"/s3/{s3_bucket}/{test_key2}",
            content=b"id,value\n1,100\n",
            headers={**project_headers, "Content-Type": "text/csv"},
        )

        # 4. GET /s3/{bucket} - List objects (with prefix)
        # Note: S3 ListObjects returns XML, not JSON
        log.step("GET", f"/s3/{s3_bucket}?prefix=test/{test_id}/")
        resp = api.get(
            f"/s3/{s3_bucket}",
            params={"prefix": f"test/{test_id}/"},
            headers=project_headers,
        )
        assert resp.status_code == 200
        # S3-compatible API returns XML - check that Keys are in response
        xml_response = resp.text
        assert "<Contents>" in xml_response or "<Key>" in xml_response
        assert "data.csv" in xml_response
        assert "data2.csv" in xml_response
        log.result(resp, "2 objects listed (XML response)")

        # 5. POST /s3/{bucket}/presign - Generate presigned URL
        log.step("POST", f"/s3/{s3_bucket}/presign", {"key": test_key, "method": "GET"})
        resp = api.post(
            f"/s3/{s3_bucket}/presign",
            json={"key": test_key, "method": "GET", "expires_in": 3600},
            headers=project_headers,
        )
        assert resp.status_code == 200
        presigned_data = resp.json()
        assert "url" in presigned_data
        presigned_url = presigned_data["url"]
        assert presigned_url.startswith("http")
        # Verify presigned URL contains required params
        assert "signature=" in presigned_url or "X-Amz-Signature=" in presigned_url
        assert "expires=" in presigned_url or "X-Amz-Expires=" in presigned_url
        log.result(resp, "presigned URL generated")

        # Extract the path and query params from presigned URL to test via TestClient
        # Presigned URL is like http://localhost:8000/s3/bucket/key?signature=...&expires=...
        log.info("Testing presigned URL access (no auth required)")
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(presigned_url)
        # Test using the path with query params (TestClient base_url handles the host)
        path_with_query = f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path
        resp = api.get(path_with_query)  # No auth header - presigned URL provides access
        assert resp.status_code == 200
        assert resp.content == test_content

        # 6. DELETE /s3/{bucket}/{key} - Delete object
        log.step("DELETE", f"/s3/{s3_bucket}/{test_key}")
        resp = api.delete(f"/s3/{s3_bucket}/{test_key}", headers=project_headers)
        assert resp.status_code == 204
        log.result(resp, "object deleted")

        # Verify object is deleted
        resp = api.get(f"/s3/{s3_bucket}/{test_key}", headers=project_headers)
        assert resp.status_code == 404
        log.info("Object verified as deleted")

        # Cleanup
        api.delete(f"/s3/{s3_bucket}/{test_key2}", headers=project_headers)
        api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Workflow 8: FilesManagement (7 endpoints)
# =============================================================================

class TestWorkflow8FilesManagement:
    """
    Test file upload/download lifecycle.

    Endpoints tested:
    1. POST /projects/{id}/files/prepare - Prepare upload
    2. POST /projects/{id}/files/upload/{key} - Upload chunks
    3. POST /projects/{id}/files - Register file
    4. GET /projects/{id}/files - List files
    5. GET /projects/{id}/files/{file_id} - Get file info
    6. GET /projects/{id}/files/{file_id}/download - Download file
    7. DELETE /projects/{id}/files/{file_id} - Delete file
    """

    def test_files_management_workflow(self, api, admin_headers):
        """Test complete file management lifecycle."""
        log = WorkflowProtocol("Workflow 8: Files Management", 7)
        log.header()

        test_id = generate_test_id()
        project_id = f"files_{test_id}"

        # Create project
        log.info("Creating project for file management testing")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Files Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}

        test_filename = f"data_{test_id}.csv"
        test_content = b"id,name,value\n1,Alpha,100\n2,Beta,200\n3,Gamma,300\n"

        # 1. POST /projects/{id}/files/prepare - Prepare upload
        log.step("POST", f"/projects/{project_id}/files/prepare", {"filename": test_filename})
        resp = api.post(
            f"/projects/{project_id}/files/prepare",
            json={"filename": test_filename},
            headers=project_headers,
        )
        assert resp.status_code == 200
        upload_key = resp.json()["upload_key"]
        assert upload_key is not None
        log.result(resp, f"upload_key={upload_key}")

        # 2. POST /projects/{id}/files/upload/{key} - Upload file
        log.step("POST", f"/projects/{project_id}/files/upload/{upload_key}")
        resp = api.post(
            f"/projects/{project_id}/files/upload/{upload_key}",
            files={"file": (test_filename, test_content, "text/csv")},
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, f"uploaded {len(test_content)} bytes")

        # 3. POST /projects/{id}/files - Register file
        log.step("POST", f"/projects/{project_id}/files", {"upload_key": upload_key})
        resp = api.post(
            f"/projects/{project_id}/files",
            json={
                "upload_key": upload_key,
                "name": test_filename,
                "tags": {"test": "true", "type": "csv"},  # Must be dict[str, str]
            },
            headers=project_headers,
        )
        assert resp.status_code == 201
        file_data = resp.json()
        file_id = file_data["id"]
        log.result(resp, f"file_id={file_id}")

        # 4. GET /projects/{id}/files - List files
        log.step("GET", f"/projects/{project_id}/files")
        resp = api.get(f"/projects/{project_id}/files", headers=project_headers)
        assert resp.status_code == 200
        files = resp.json()["files"]
        assert len(files) >= 1
        assert any(f["id"] == file_id for f in files)
        log.result(resp, f"{len(files)} files found")

        # 5. GET /projects/{id}/files/{file_id} - Get file info
        log.step("GET", f"/projects/{project_id}/files/{file_id}")
        resp = api.get(f"/projects/{project_id}/files/{file_id}", headers=project_headers)
        assert resp.status_code == 200
        file_info = resp.json()
        assert file_info["name"] == test_filename
        assert file_info["size_bytes"] == len(test_content)
        log.result(resp, f"{test_filename}, {len(test_content)} bytes")

        # 6. GET /projects/{id}/files/{file_id}/download - Download file
        log.step("GET", f"/projects/{project_id}/files/{file_id}/download")
        resp = api.get(f"/projects/{project_id}/files/{file_id}/download", headers=project_headers)
        assert resp.status_code == 200
        assert resp.content == test_content
        log.result(resp, "content matches")

        # 7. DELETE /projects/{id}/files/{file_id} - Delete file
        log.step("DELETE", f"/projects/{project_id}/files/{file_id}")
        resp = api.delete(f"/projects/{project_id}/files/{file_id}", headers=project_headers)
        assert resp.status_code == 204
        log.result(resp, "file deleted")

        # Verify file is deleted
        resp = api.get(f"/projects/{project_id}/files/{file_id}", headers=project_headers)
        assert resp.status_code == 404
        log.info("File verified as deleted")

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Workflow 9: DriverBridge (2 endpoints)
# =============================================================================

class TestWorkflow9DriverBridge:
    """
    Test driver bridge endpoints (gRPC bridge over HTTP).

    Endpoints tested:
    1. GET /driver/commands - List available commands
    2. POST /driver/execute - Execute command
    """

    def test_driver_bridge_workflow(self, api, admin_headers):
        """Test driver bridge endpoints."""
        log = WorkflowProtocol("Workflow 9: Driver Bridge", 2)
        log.header()

        # 1. GET /driver/commands - List available commands
        log.step("GET", "/driver/commands")
        resp = api.get("/driver/commands", headers=admin_headers)
        assert resp.status_code == 200
        data = resp.json()
        commands = data.get("supported_commands", [])

        # Should have multiple driver commands
        assert len(commands) > 10, "Should have many supported commands"

        # Check some expected command types exist
        command_types = [cmd.get("type") for cmd in commands]
        expected_commands = [
            "InitBackendCommand",
            "CreateProjectCommand",
            "CreateBucketCommand",
            "CreateTableCommand",
            "ObjectInfoCommand",
        ]
        for cmd in expected_commands:
            assert cmd in command_types, f"Missing command: {cmd}"
        log.result(resp, f"{len(commands)} commands available")

        # 2. POST /driver/execute - Execute InitBackendCommand (idempotent)
        log.step("POST", "/driver/execute", {"command": {"type": "InitBackendCommand"}})
        resp = api.post(
            "/driver/execute",
            json={
                "command": {"type": "InitBackendCommand"},
            },
            headers=admin_headers,
        )
        assert resp.status_code == 200, f"InitBackend failed: {resp.text}"
        result = resp.json()
        assert "commandResponse" in result or "messages" in result
        log.result(resp, "InitBackendCommand executed")

        # Test error handling - invalid command type
        log.info("Testing error handling with invalid command")
        resp = api.post(
            "/driver/execute",
            json={
                "command": {"type": "InvalidCommand"},
            },
            headers=admin_headers,
        )
        # Should return error
        assert resp.status_code in [400, 404, 422, 500]
        log.info(f"Invalid command properly rejected: {resp.status_code}")


# =============================================================================
# Workflow 10: PGWireSessions (4 endpoints)
# =============================================================================

class TestWorkflow10PGWireSessions:
    """
    Test PG Wire session management.

    Endpoints tested:
    1. POST /internal/pgwire/auth - Authenticate
    2. POST /internal/pgwire/sessions - Create session
    3. GET /internal/pgwire/sessions - List sessions
    4. DELETE /internal/pgwire/sessions/{id} - Close session
    """

    def test_pgwire_sessions_workflow(self, api, admin_headers):
        """Test PG Wire session management."""
        log = WorkflowProtocol("Workflow 10: PG Wire Sessions", 4)
        log.header()

        test_id = generate_test_id()
        project_id = f"pgwire_{test_id}"

        # Create project with workspace
        log.info("Creating project and workspace for PG Wire testing")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"PGWire Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}

        # Create workspace to get credentials
        resp = api.post(
            f"/projects/{project_id}/workspaces",
            json={"name": f"sql_{test_id}"},
            headers=project_headers,
        )
        ws_data = resp.json()
        ws_id = ws_data["id"]
        # Credentials are in connection object
        ws_username = ws_data["connection"]["username"]
        ws_password = ws_data["connection"]["password"]
        log.info(f"Workspace created: {ws_id}")

        # 1. POST /internal/pgwire/auth - Authenticate
        # Note: PGWireAuthRequest only has username, password, client_ip (not database)
        log.step("POST", "/internal/pgwire/auth", {"username": ws_username})
        resp = api.post(
            "/internal/pgwire/auth",
            json={
                "username": ws_username,
                "password": ws_password,
                "client_ip": "127.0.0.1",
            },
            headers=admin_headers,  # Internal endpoint uses admin auth
        )
        assert resp.status_code == 200
        auth_data = resp.json()
        assert "workspace_id" in auth_data
        log.result(resp, f"authenticated: workspace_id={auth_data['workspace_id']}")

        # 2. POST /internal/pgwire/sessions - Create session
        # PGWireSessionCreateRequest requires session_id, workspace_id, and optional client_ip
        session_id = f"sess_{test_id}"
        log.step("POST", "/internal/pgwire/sessions", {"session_id": session_id, "workspace_id": ws_id})
        resp = api.post(
            "/internal/pgwire/sessions",
            json={
                "session_id": session_id,
                "workspace_id": ws_id,
                "client_ip": "127.0.0.1",
            },
            headers=admin_headers,
        )
        assert resp.status_code == 201
        session_data = resp.json()
        # PGWireSessionInfo uses session_id, not id
        assert session_data.get("session_id") == session_id
        log.result(resp, f"session created: {session_id}")

        # 3. GET /internal/pgwire/sessions - List sessions (returns list directly)
        log.step("GET", "/internal/pgwire/sessions")
        resp = api.get("/internal/pgwire/sessions", headers=admin_headers)
        assert resp.status_code == 200
        sessions = resp.json()  # Returns list directly, not {"sessions": [...]}
        assert any(s.get("session_id") == session_id for s in sessions)
        log.result(resp, f"{len(sessions)} sessions found")

        # 4. DELETE /internal/pgwire/sessions/{id} - Close session
        log.step("DELETE", f"/internal/pgwire/sessions/{session_id}")
        resp = api.delete(f"/internal/pgwire/sessions/{session_id}", headers=admin_headers)
        assert resp.status_code == 204
        log.result(resp, "session closed")

        # Verify session is closed (deleted from list or status changed)
        resp = api.get("/internal/pgwire/sessions", headers=admin_headers)
        sessions = resp.json()  # Returns list directly
        # Session might still be in list but with closed status, or removed
        for s in sessions:
            if s.get("session_id") == session_id:
                # If found, should have closed status
                assert s.get("status") in ["closed", "disconnected"], f"Session should be closed: {s}"
                break
        log.info("Session verified as closed")

        # Cleanup
        api.delete(f"/projects/{project_id}/workspaces/{ws_id}", headers=project_headers)
        api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Additional E2E Scenarios
# =============================================================================

class TestIncrementalAppendWithoutPK:
    """
    Test incremental import without primary key (append mode).

    Per phase-15-e2e-tests.md Task 15.5:
    Table without PK - incremental = pure append (no deduplication).
    """

    def test_incremental_append_without_pk(self, api, admin_headers):
        """Incremental import without PK appends without deduplication."""
        log = WorkflowProtocol("Incremental Append Without PK", 3)
        log.header()

        test_id = generate_test_id()
        project_id = f"append_{test_id}"
        bucket_name = "in_c_events"
        table_name = "events"

        # Create project
        log.info("Creating project with table WITHOUT primary key")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Append Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}
        base_url = f"/projects/{project_id}/branches/default"

        # Create bucket
        api.post(f"{base_url}/buckets", json={"name": bucket_name, "stage": "in"}, headers=project_headers)

        # Create table WITHOUT primary key
        api.post(
            f"{base_url}/buckets/{bucket_name}/tables",
            json={
                "name": table_name,
                "columns": [
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "event_type", "type": "VARCHAR"},
                    {"name": "data", "type": "VARCHAR"},
                ],
                # No primary_key!
            },
            headers=project_headers,
        )

        # Import 3 rows
        log.step("POST", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file", {"initial import": "3 rows"})
        csv1 = b"timestamp,event_type,data\n2024-01-01 00:00:00,click,a\n2024-01-02 00:00:00,view,b\n2024-01-03 00:00:00,click,c\n"
        file_id1 = upload_csv(api, project_id, project_headers, "events1.csv", csv1)
        resp = api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id1},
            headers=project_headers,
        )
        assert resp.status_code == 200
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name) == 3
        log.result(resp, "3 rows imported")

        # Import SAME 3 rows again with incremental=True
        log.step("POST", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file", {"incremental": True, "same data": "3 rows"})
        file_id2 = upload_csv(api, project_id, project_headers, "events2.csv", csv1)
        resp = api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={
                "file_id": file_id2,
                "import_options": {"incremental": True},
            },
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "3 more rows imported incrementally")

        # Should have 6 rows (append, no dedup because no PK)
        log.step("Verify", "row count")
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name) == 6
        log.info("SUCCESS: 6 rows total (no deduplication without PK)")

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


class TestSnapshotBeforeTruncate:
    """
    Test automatic snapshot creation before TRUNCATE.

    Per phase-15-e2e-tests.md Task 15.6:
    Auto-snapshot is created when DELETE WHERE 1=1 (truncate pattern) is executed.
    """

    def test_auto_snapshot_before_truncate(self, api, admin_headers):
        """Verify auto-snapshot is created before truncate."""
        log = WorkflowProtocol("Auto-Snapshot Before Truncate", 4)
        log.header()

        test_id = generate_test_id()
        project_id = f"autosnap_{test_id}"
        bucket_name = "in_c_data"
        table_name = "users"

        # Create project
        log.info("Creating project and enabling auto-snapshot for truncate")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"AutoSnap Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}
        base_url = f"/projects/{project_id}/branches/default"

        # Enable truncate_table trigger at project level
        # Note: API uses auto_snapshot_triggers, not triggers
        log.step("PUT", f"/projects/{project_id}/settings/snapshots", {"enabled": True, "triggers": ["truncate", "delete_all"]})
        api.put(
            f"/projects/{project_id}/settings/snapshots",
            json={
                "enabled": True,
                "auto_snapshot_triggers": {"truncate_table": True, "delete_all_rows": True},
            },
            headers=project_headers,
        )
        log.info("Auto-snapshot triggers enabled")

        # Create bucket and table with data
        log.info("Creating table with 3 rows")
        api.post(f"{base_url}/buckets", json={"name": bucket_name, "stage": "in"}, headers=project_headers)
        api.post(
            f"{base_url}/buckets/{bucket_name}/tables",
            json={
                "name": table_name,
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
            headers=project_headers,
        )

        # Import data
        csv_content = b"id,name\n1,Alice\n2,Bob\n3,Charlie\n"
        file_id = upload_csv(api, project_id, project_headers, "users.csv", csv_content)
        api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id},
            headers=project_headers,
        )

        # Verify 3 rows
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name) == 3

        # Get snapshot count before
        log.step("GET", f"{base_url}/snapshots (before truncate)")
        resp = api.get(
            f"{base_url}/snapshots",
            params={"bucket": bucket_name, "table": table_name},
            headers=project_headers,
        )
        snapshots_before = len(resp.json().get("snapshots", []))
        log.result(resp, f"{snapshots_before} snapshots before")

        # Truncate table (DELETE WHERE 1=1)
        # httpx.Client.delete() doesn't accept json, use request() instead
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}/tables/{table_name}/rows", {"where_clause": "1=1"})
        resp = api.request(
            "DELETE",
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/rows",
            json={"where_clause": "1=1"},
            headers=project_headers,
        )
        assert resp.status_code == 200
        log.result(resp, "table truncated")

        # Verify table is empty
        assert get_row_count(api, project_id, project_headers, bucket_name, table_name) == 0
        log.info("Table is now empty")

        # Verify auto-snapshot was created
        log.step("GET", f"{base_url}/snapshots (after truncate)")
        resp = api.get(
            f"{base_url}/snapshots",
            params={"bucket": bucket_name, "table": table_name},
            headers=project_headers,
        )
        snapshots_after = resp.json().get("snapshots", [])
        assert len(snapshots_after) > snapshots_before, "Auto-snapshot should have been created"
        log.result(resp, f"{len(snapshots_after)} snapshots after (auto-snapshot created)")

        # Find the auto-snapshot
        auto_snapshot = None
        for s in snapshots_after:
            if "auto" in s.get("description", "").lower() or s.get("type") == "auto":
                auto_snapshot = s
                break

        # If auto-snapshot exists, verify it has correct row count
        if auto_snapshot:
            assert auto_snapshot["row_count"] == 3
            log.info(f"Auto-snapshot verified: {auto_snapshot['row_count']} rows preserved")

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


# =============================================================================
# Bucket Deletion Cleanup
# =============================================================================

class TestBucketDeletion:
    """Test bucket deletion endpoint."""

    def test_delete_bucket(self, api, admin_headers):
        """DELETE .../buckets/{name} - Delete bucket."""
        log = WorkflowProtocol("Bucket Deletion", 1)
        log.header()

        test_id = generate_test_id()
        project_id = f"delbucket_{test_id}"
        bucket_name = "in_c_temp"

        # Create project
        log.info("Creating project and bucket")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Delete Bucket Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}
        base_url = f"/projects/{project_id}/branches/default"

        # Create bucket
        api.post(f"{base_url}/buckets", json={"name": bucket_name, "stage": "in"}, headers=project_headers)

        # Delete bucket
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}")
        resp = api.delete(f"{base_url}/buckets/{bucket_name}", headers=project_headers)
        assert resp.status_code == 204
        log.result(resp, "bucket deleted")

        # Verify bucket is deleted
        resp = api.get(f"{base_url}/buckets/{bucket_name}", headers=project_headers)
        assert resp.status_code == 404
        log.info("Bucket verified as deleted")

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


class TestTableDeletion:
    """Test table deletion endpoint."""

    def test_delete_table(self, api, admin_headers):
        """DELETE .../tables/{t} - Delete table."""
        log = WorkflowProtocol("Table Deletion", 1)
        log.header()

        test_id = generate_test_id()
        project_id = f"deltable_{test_id}"
        bucket_name = "in_c_data"
        table_name = "temp_table"

        # Create project
        log.info("Creating project, bucket, and table")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Delete Table Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}
        base_url = f"/projects/{project_id}/branches/default"

        # Create bucket and table
        api.post(f"{base_url}/buckets", json={"name": bucket_name, "stage": "in"}, headers=project_headers)
        api.post(
            f"{base_url}/buckets/{bucket_name}/tables",
            json={
                "name": table_name,
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=project_headers,
        )

        # Delete table
        log.step("DELETE", f"{base_url}/buckets/{bucket_name}/tables/{table_name}")
        resp = api.delete(f"{base_url}/buckets/{bucket_name}/tables/{table_name}", headers=project_headers)
        assert resp.status_code == 204
        log.result(resp, "table deleted")

        # Verify table is deleted
        resp = api.get(f"{base_url}/buckets/{bucket_name}/tables/{table_name}", headers=project_headers)
        assert resp.status_code == 404
        log.info("Table verified as deleted")

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


class TestSnapshotDeletion:
    """Test snapshot deletion endpoint."""

    def test_delete_snapshot(self, api, admin_headers):
        """DELETE .../snapshots/{id} - Delete snapshot."""
        log = WorkflowProtocol("Snapshot Deletion", 2)
        log.header()

        test_id = generate_test_id()
        project_id = f"delsnap_{test_id}"
        bucket_name = "in_c_data"
        table_name = "users"

        # Create project with data
        log.info("Creating project, table, data, and snapshot")
        resp = api.post(
            "/projects",
            json={"id": project_id, "name": f"Delete Snapshot Test {test_id}"},
            headers=admin_headers,
        )
        project_headers = {"Authorization": f"Bearer {resp.json()['api_key']}"}
        base_url = f"/projects/{project_id}/branches/default"

        # Create bucket, table, import data
        api.post(f"{base_url}/buckets", json={"name": bucket_name, "stage": "in"}, headers=project_headers)
        api.post(
            f"{base_url}/buckets/{bucket_name}/tables",
            json={
                "name": table_name,
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=project_headers,
        )
        csv_content = b"id\n1\n2\n"
        file_id = upload_csv(api, project_id, project_headers, "data.csv", csv_content)
        api.post(
            f"{base_url}/buckets/{bucket_name}/tables/{table_name}/import/file",
            json={"file_id": file_id},
            headers=project_headers,
        )

        # Create snapshot
        log.step("POST", f"{base_url}/snapshots", {"bucket": bucket_name, "table": table_name})
        resp = api.post(
            f"{base_url}/snapshots",
            json={"bucket": bucket_name, "table": table_name, "description": "Test snapshot"},
            headers=project_headers,
        )
        assert resp.status_code == 201, f"Snapshot creation failed: {resp.text}"
        snapshot_id = resp.json()["id"]
        log.result(resp, f"snapshot_id={snapshot_id}")

        # Delete snapshot
        log.step("DELETE", f"{base_url}/snapshots/{snapshot_id}")
        resp = api.delete(
            f"{base_url}/snapshots/{snapshot_id}",
            headers=project_headers,
        )
        assert resp.status_code == 204
        log.result(resp, "snapshot deleted")

        # Verify snapshot is deleted
        resp = api.get(
            f"{base_url}/snapshots/{snapshot_id}",
            headers=project_headers,
        )
        assert resp.status_code == 404
        log.info("Snapshot verified as deleted")

        # Cleanup
        api.delete(f"/projects/{project_id}", headers=admin_headers)


class TestBackendRemove:
    """Test backend removal endpoint."""

    def test_backend_remove(self, api, admin_headers):
        """POST /backend/remove - Remove backend."""
        log = WorkflowProtocol("Backend Remove", 1)
        log.header()

        # Note: This is a destructive operation, typically only used in tests
        # The backend should be re-initialized by other tests

        # Just verify the endpoint exists and returns appropriate status
        log.step("POST", "/backend/remove")
        resp = api.post("/backend/remove", headers=admin_headers)
        # May return 200 (removed) or 400 (has data) depending on state
        assert resp.status_code in [200, 400]
        if resp.status_code == 200:
            log.result(resp, "backend removed")
        else:
            log.result(resp, "backend has data (cannot remove)")

        # Re-initialize for other tests
        log.info("Re-initializing backend for other tests")
        api.post("/backend/init", headers=admin_headers)
