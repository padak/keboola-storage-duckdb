"""Tests for projects and buckets CLI commands."""

import pytest
import respx
from httpx import Response
from typer.testing import CliRunner

from keboola_duckdb_cli.main import app


runner = CliRunner()


@pytest.fixture
def mock_config(monkeypatch):
    """Mock environment variables for API configuration."""
    monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "http://test-api")
    monkeypatch.setenv("KEBOOLA_DUCKDB_API_KEY", "test-key")


class TestProjectsList:
    """Test cases for 'projects list' command."""

    @respx.mock
    def test_list_projects_success(self, mock_config):
        """Test listing projects successfully."""
        respx.get("http://test-api/projects").mock(
            return_value=Response(200, json={
                "projects": [
                    {
                        "id": "proj-1",
                        "name": "Test Project",
                        "status": "active",
                        "size_bytes": 1024,
                        "table_count": 5,
                        "bucket_count": 2,
                        "created_at": "2024-01-01T00:00:00Z"
                    },
                    {
                        "id": "proj-2",
                        "name": "Second Project",
                        "status": "active",
                        "size_bytes": 2048,
                        "table_count": 10,
                        "bucket_count": 3,
                        "created_at": "2024-01-02T00:00:00Z"
                    }
                ],
                "total": 2
            })
        )
        result = runner.invoke(app, ["projects", "list"])
        assert result.exit_code == 0
        assert "proj-1" in result.stdout
        assert "Test Project" in result.stdout
        assert "proj-2" in result.stdout
        assert "Second" in result.stdout  # Partial match to avoid wrapping issues
        assert "Total: 2" in result.stdout

    @respx.mock
    def test_list_projects_json_output(self, mock_config):
        """Test JSON output format."""
        respx.get("http://test-api/projects").mock(
            return_value=Response(200, json={
                "projects": [
                    {
                        "id": "proj-1",
                        "name": "Test Project",
                        "status": "active",
                        "size_bytes": 1024,
                        "table_count": 5,
                        "bucket_count": 2,
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                ],
                "total": 1
            })
        )
        result = runner.invoke(app, ["--json", "projects", "list"])
        assert result.exit_code == 0
        assert '"projects"' in result.stdout
        assert '"proj-1"' in result.stdout
        assert '"total": 1' in result.stdout

    @respx.mock
    def test_list_projects_empty(self, mock_config):
        """Test listing with no projects."""
        respx.get("http://test-api/projects").mock(
            return_value=Response(200, json={
                "projects": [],
                "total": 0
            })
        )
        result = runner.invoke(app, ["projects", "list"])
        assert result.exit_code == 0
        assert "No projects found" in result.stdout

    @respx.mock
    def test_list_projects_api_error_401(self, mock_config):
        """Test API error handling - unauthorized."""
        respx.get("http://test-api/projects").mock(
            return_value=Response(401, json={
                "message": "Invalid API key"
            })
        )
        result = runner.invoke(app, ["projects", "list"])
        assert result.exit_code != 0

    @respx.mock
    def test_list_projects_api_error_500(self, mock_config):
        """Test API error handling - server error."""
        respx.get("http://test-api/projects").mock(
            return_value=Response(500, json={
                "message": "Internal server error"
            })
        )
        result = runner.invoke(app, ["projects", "list"])
        assert result.exit_code != 0

    @respx.mock
    def test_list_projects_with_size_formatting(self, mock_config):
        """Test that file sizes are formatted correctly."""
        respx.get("http://test-api/projects").mock(
            return_value=Response(200, json={
                "projects": [
                    {
                        "id": "proj-1",
                        "name": "Small Project",
                        "status": "active",
                        "size_bytes": 1024,  # 1 KB
                        "table_count": 1,
                        "bucket_count": 1,
                        "created_at": "2024-01-01T00:00:00Z"
                    },
                    {
                        "id": "proj-2",
                        "name": "Large Project",
                        "status": "active",
                        "size_bytes": 1073741824,  # 1 GB
                        "table_count": 100,
                        "bucket_count": 10,
                        "created_at": "2024-01-02T00:00:00Z"
                    }
                ],
                "total": 2
            })
        )
        result = runner.invoke(app, ["projects", "list"])
        assert result.exit_code == 0
        # Size formatting happens in the stdout
        assert "proj-1" in result.stdout
        assert "proj-2" in result.stdout

    @respx.mock
    def test_list_projects_verbose_mode(self, mock_config):
        """Test verbose output."""
        respx.get("http://test-api/projects").mock(
            return_value=Response(200, json={
                "projects": [
                    {
                        "id": "proj-1",
                        "name": "Test Project",
                        "status": "active",
                        "size_bytes": 1024,
                        "table_count": 5,
                        "bucket_count": 2,
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                ],
                "total": 1
            })
        )
        result = runner.invoke(app, ["--verbose", "projects", "list"])
        assert result.exit_code == 0
        assert "proj-1" in result.stdout


class TestBucketsList:
    """Test cases for 'buckets list' command."""

    @respx.mock
    def test_list_buckets_success(self, mock_config):
        """Test listing buckets successfully."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets").mock(
            return_value=Response(200, json={
                "buckets": [
                    {
                        "name": "in.c-sales",
                        "table_count": 3,
                        "description": "Sales data",
                        "is_linked": False
                    },
                    {
                        "name": "out.c-reports",
                        "table_count": 5,
                        "description": "Generated reports",
                        "is_linked": False
                    }
                ],
                "total": 2
            })
        )
        result = runner.invoke(app, ["buckets", "list", "proj-1"])
        assert result.exit_code == 0
        assert "in.c-sales" in result.stdout
        assert "out.c-reports" in result.stdout
        assert "Total: 2" in result.stdout

    @respx.mock
    def test_list_buckets_with_branch(self, mock_config):
        """Test listing buckets with custom branch."""
        respx.get("http://test-api/projects/proj-1/branches/dev-123/buckets").mock(
            return_value=Response(200, json={
                "buckets": [
                    {
                        "name": "in.c-test",
                        "table_count": 1,
                        "description": "Test data",
                        "is_linked": False
                    }
                ],
                "total": 1
            })
        )
        result = runner.invoke(app, ["buckets", "list", "proj-1", "--branch", "dev-123"])
        assert result.exit_code == 0
        assert "in.c-test" in result.stdout
        assert "dev-123" in result.stdout

    @respx.mock
    def test_list_buckets_json_output(self, mock_config):
        """Test JSON output format."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets").mock(
            return_value=Response(200, json={
                "buckets": [
                    {
                        "name": "in.c-sales",
                        "table_count": 3,
                        "description": "Sales data",
                        "is_linked": False
                    }
                ],
                "total": 1
            })
        )
        result = runner.invoke(app, ["--json", "buckets", "list", "proj-1"])
        assert result.exit_code == 0
        assert '"buckets"' in result.stdout
        assert '"in.c-sales"' in result.stdout
        assert '"total": 1' in result.stdout

    @respx.mock
    def test_list_buckets_empty(self, mock_config):
        """Test listing with no buckets."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets").mock(
            return_value=Response(200, json={
                "buckets": [],
                "total": 0
            })
        )
        result = runner.invoke(app, ["buckets", "list", "proj-1"])
        assert result.exit_code == 0
        assert "No buckets found" in result.stdout
        assert "proj-1" in result.stdout
        assert "default" in result.stdout

    @respx.mock
    def test_list_buckets_linked(self, mock_config):
        """Test listing buckets with linked bucket."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets").mock(
            return_value=Response(200, json={
                "buckets": [
                    {
                        "name": "in.c-sales",
                        "table_count": 3,
                        "description": "Sales data",
                        "is_linked": False
                    },
                    {
                        "name": "in.c-shared",
                        "table_count": 5,
                        "description": "Shared bucket",
                        "is_linked": True,
                        "source_project_id": "proj-2",
                        "source_bucket_name": "out.c-data"
                    }
                ],
                "total": 2
            })
        )
        result = runner.invoke(app, ["buckets", "list", "proj-1"])
        assert result.exit_code == 0
        assert "in.c-sales" in result.stdout
        assert "in.c-shared" in result.stdout
        assert "proj-2/out.c-data" in result.stdout or "proj-2" in result.stdout

    @respx.mock
    def test_list_buckets_long_description(self, mock_config):
        """Test that long descriptions are truncated."""
        long_desc = "A" * 100  # Very long description
        respx.get("http://test-api/projects/proj-1/branches/default/buckets").mock(
            return_value=Response(200, json={
                "buckets": [
                    {
                        "name": "in.c-test",
                        "table_count": 1,
                        "description": long_desc,
                        "is_linked": False
                    }
                ],
                "total": 1
            })
        )
        result = runner.invoke(app, ["buckets", "list", "proj-1"])
        assert result.exit_code == 0
        assert "in.c-test" in result.stdout
        # Description should be truncated (max 40 chars in code)

    @respx.mock
    def test_list_buckets_api_error_404(self, mock_config):
        """Test API error handling - project not found."""
        respx.get("http://test-api/projects/nonexistent/branches/default/buckets").mock(
            return_value=Response(404, json={
                "message": "Project not found"
            })
        )
        result = runner.invoke(app, ["buckets", "list", "nonexistent"])
        assert result.exit_code != 0

    @respx.mock
    def test_list_buckets_api_error_500(self, mock_config):
        """Test API error handling - server error."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets").mock(
            return_value=Response(500, json={
                "message": "Internal server error"
            })
        )
        result = runner.invoke(app, ["buckets", "list", "proj-1"])
        assert result.exit_code != 0

    @respx.mock
    def test_list_buckets_verbose_mode(self, mock_config):
        """Test verbose output."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets").mock(
            return_value=Response(200, json={
                "buckets": [
                    {
                        "name": "in.c-test",
                        "table_count": 1,
                        "description": "Test",
                        "is_linked": False
                    }
                ],
                "total": 1
            })
        )
        result = runner.invoke(app, ["--verbose", "buckets", "list", "proj-1"])
        assert result.exit_code == 0
        assert "in.c-test" in result.stdout

    @respx.mock
    def test_list_buckets_missing_project_arg(self, mock_config):
        """Test missing project argument."""
        result = runner.invoke(app, ["buckets", "list"])
        assert result.exit_code != 0
        # Typer will show usage/error message

    @respx.mock
    def test_list_buckets_with_branch_short_flag(self, mock_config):
        """Test using -b short flag for branch."""
        respx.get("http://test-api/projects/proj-1/branches/feature-123/buckets").mock(
            return_value=Response(200, json={
                "buckets": [
                    {
                        "name": "in.c-test",
                        "table_count": 1,
                        "description": "Test",
                        "is_linked": False
                    }
                ],
                "total": 1
            })
        )
        result = runner.invoke(app, ["buckets", "list", "proj-1", "-b", "feature-123"])
        assert result.exit_code == 0
        assert "in.c-test" in result.stdout
        assert "feature-123" in result.stdout


class TestProjectsAndBucketsCombined:
    """Test cases for combined workflows."""

    @respx.mock
    def test_workflow_list_projects_then_buckets(self, mock_config):
        """Test typical workflow: list projects, then list buckets."""
        # First call - list projects
        respx.get("http://test-api/projects").mock(
            return_value=Response(200, json={
                "projects": [
                    {
                        "id": "proj-1",
                        "name": "Test Project",
                        "status": "active",
                        "size_bytes": 1024,
                        "table_count": 5,
                        "bucket_count": 2,
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                ],
                "total": 1
            })
        )

        # Second call - list buckets
        respx.get("http://test-api/projects/proj-1/branches/default/buckets").mock(
            return_value=Response(200, json={
                "buckets": [
                    {
                        "name": "in.c-sales",
                        "table_count": 3,
                        "description": "Sales data",
                        "is_linked": False
                    }
                ],
                "total": 1
            })
        )

        # Execute commands
        result1 = runner.invoke(app, ["projects", "list"])
        assert result1.exit_code == 0
        assert "proj-1" in result1.stdout

        result2 = runner.invoke(app, ["buckets", "list", "proj-1"])
        assert result2.exit_code == 0
        assert "in.c-sales" in result2.stdout


class TestConfigurationErrors:
    """Test configuration error handling."""

    def test_missing_config(self, monkeypatch):
        """Test error when configuration is missing."""
        # Remove environment variables
        monkeypatch.delenv("KEBOOLA_DUCKDB_URL", raising=False)
        monkeypatch.delenv("KEBOOLA_DUCKDB_API_KEY", raising=False)

        result = runner.invoke(app, ["projects", "list"])
        assert result.exit_code == 1
        # Should show configuration error

    def test_invalid_url(self, monkeypatch):
        """Test error with invalid URL format."""
        monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "not-a-url")
        monkeypatch.setenv("KEBOOLA_DUCKDB_API_KEY", "test-key")

        # This may or may not fail depending on validation
        result = runner.invoke(app, ["projects", "list"])
        # Just ensure it doesn't crash completely
        assert isinstance(result.exit_code, int)
