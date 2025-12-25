"""Tests for file management commands."""

from pathlib import Path
from unittest.mock import Mock, patch, mock_open

import pytest
import respx
from httpx import Response
from typer.testing import CliRunner

from keboola_duckdb_cli.main import app


runner = CliRunner()


@pytest.fixture
def mock_config(monkeypatch):
    """Mock configuration via environment variables."""
    monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "http://test-api")
    monkeypatch.setenv("KEBOOLA_DUCKDB_API_KEY", "test-key")


class TestFilesList:
    """Tests for 'files list' command."""

    @respx.mock
    def test_list_files_success(self, mock_config):
        """Test listing files with results."""
        respx.get("http://test-api/projects/proj-1/files").mock(
            return_value=Response(
                200,
                json={
                    "files": [
                        {
                            "id": "file-1",
                            "name": "data.csv",
                            "size_bytes": 1024,
                            "content_type": "text/csv",
                            "is_staged": False,
                            "created_at": "2024-01-01T10:30:00Z",
                        },
                        {
                            "id": "file-2",
                            "name": "output.json",
                            "size_bytes": 2048,
                            "content_type": "application/json",
                            "is_staged": True,
                            "created_at": "2024-01-02T15:45:00Z",
                        },
                    ],
                    "total": 2,
                },
            )
        )

        result = runner.invoke(app, ["files", "list", "proj-1"])

        assert result.exit_code == 0
        assert "data.csv" in result.stdout
        assert "output.json" in result.stdout
        assert "file-1" in result.stdout
        assert "file-2" in result.stdout
        assert "Total: 2" in result.stdout

    @respx.mock
    def test_list_files_empty(self, mock_config):
        """Test listing files with no results."""
        respx.get("http://test-api/projects/proj-1/files").mock(
            return_value=Response(200, json={"files": [], "total": 0})
        )

        result = runner.invoke(app, ["files", "list", "proj-1"])

        assert result.exit_code == 0
        assert "No files found" in result.stdout

    @respx.mock
    def test_list_files_json_output(self, mock_config):
        """Test listing files with JSON output."""
        respx.get("http://test-api/projects/proj-1/files").mock(
            return_value=Response(
                200,
                json={
                    "files": [
                        {
                            "id": "file-1",
                            "name": "data.csv",
                            "size_bytes": 1024,
                            "content_type": "text/csv",
                            "is_staged": False,
                            "created_at": "2024-01-01T00:00:00Z",
                        }
                    ],
                    "total": 1,
                },
            )
        )

        result = runner.invoke(app, ["--json", "files", "list", "proj-1"])

        assert result.exit_code == 0
        assert '"files"' in result.stdout
        assert '"total": 1' in result.stdout
        assert '"file-1"' in result.stdout

    @respx.mock
    def test_list_files_with_limit(self, mock_config):
        """Test listing files with custom limit."""
        route = respx.get("http://test-api/projects/proj-1/files").mock(
            return_value=Response(200, json={"files": [], "total": 0})
        )

        result = runner.invoke(app, ["files", "list", "proj-1", "--limit", "50"])

        assert result.exit_code == 0
        assert route.calls[0].request.url.params["limit"] == "50"

    @respx.mock
    def test_list_files_api_error(self, mock_config):
        """Test listing files with API error."""
        respx.get("http://test-api/projects/proj-1/files").mock(
            return_value=Response(
                404,
                json={"message": "Project not found", "detail": "Project proj-1 does not exist"},
            )
        )

        result = runner.invoke(app, ["files", "list", "proj-1"])

        assert result.exit_code != 0


class TestFilesUpload:
    """Tests for 'files upload' command."""

    def test_upload_file_success(self, mock_config, tmp_path):
        """Test uploading a file successfully."""
        # Create temp file
        test_file = tmp_path / "test.csv"
        test_file.write_text("id,name\n1,test\n2,data")

        # Mock the client's upload_file_3stage method
        mock_file_info = {
            "id": "file-abc",
            "name": "test.csv",
            "size_bytes": 24,
            "content_type": "text/csv",
            "is_staged": True,
            "created_at": "2024-01-01T00:00:00Z",
        }

        with patch("keboola_duckdb_cli.commands.files.get_client") as mock_get_client:
            mock_client = Mock()
            mock_client.upload_file_3stage.return_value = mock_file_info
            mock_get_client.return_value = mock_client

            result = runner.invoke(app, ["files", "upload", "proj-1", str(test_file)])

            assert result.exit_code == 0
            assert "file-abc" in result.stdout
            assert "uploaded successfully" in result.stdout.lower()
            mock_client.upload_file_3stage.assert_called_once()

    def test_upload_file_json_output(self, mock_config, tmp_path):
        """Test uploading a file with JSON output."""
        # Create temp file
        test_file = tmp_path / "data.json"
        test_file.write_text('{"key": "value"}')

        mock_file_info = {
            "id": "file-xyz",
            "name": "data.json",
            "size_bytes": 16,
            "content_type": "application/json",
        }

        with patch("keboola_duckdb_cli.commands.files.get_client") as mock_get_client:
            mock_client = Mock()
            mock_client.upload_file_3stage.return_value = mock_file_info
            mock_get_client.return_value = mock_client

            result = runner.invoke(app, ["--json", "files", "upload", "proj-1", str(test_file)])

            assert result.exit_code == 0
            assert '"id": "file-xyz"' in result.stdout
            assert '"name": "data.json"' in result.stdout

    def test_upload_nonexistent_file(self, mock_config, tmp_path):
        """Test uploading a file that doesn't exist."""
        nonexistent = tmp_path / "doesnt-exist.csv"

        result = runner.invoke(app, ["files", "upload", "proj-1", str(nonexistent)])

        # Typer validation returns exit code 2
        assert result.exit_code == 2

    def test_upload_api_error(self, mock_config, tmp_path):
        """Test upload when API call fails."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        from keboola_duckdb_cli.client import APIError

        with patch("keboola_duckdb_cli.commands.files.get_client") as mock_get_client:
            mock_client = Mock()
            mock_client.upload_file_3stage.side_effect = APIError(
                400, "Invalid filename", {"detail": "Filename contains invalid characters"}
            )
            mock_get_client.return_value = mock_client

            result = runner.invoke(app, ["files", "upload", "proj-1", str(test_file)])

            assert result.exit_code != 0


class TestFilesDownload:
    """Tests for 'files download' command."""

    @respx.mock
    def test_download_file_success(self, mock_config, tmp_path):
        """Test downloading a file successfully."""
        output_path = tmp_path / "downloaded.csv"

        respx.get("http://test-api/projects/proj-1/files/file-123/download").mock(
            return_value=Response(200, content=b"id,name\n1,test\n2,data")
        )

        result = runner.invoke(
            app, ["files", "download", "proj-1", "file-123", str(output_path)]
        )

        assert result.exit_code == 0
        assert output_path.exists()
        assert output_path.read_text() == "id,name\n1,test\n2,data"
        assert "downloaded successfully" in result.stdout.lower()

    @respx.mock
    def test_download_file_json_output(self, mock_config, tmp_path):
        """Test downloading a file with JSON output."""
        output_path = tmp_path / "output.txt"

        respx.get("http://test-api/projects/proj-1/files/file-abc/download").mock(
            return_value=Response(200, content=b"test data")
        )

        result = runner.invoke(
            app, ["--json", "files", "download", "proj-1", "file-abc", str(output_path)]
        )

        assert result.exit_code == 0
        assert output_path.exists()
        assert '"path"' in result.stdout
        assert '"size_bytes"' in result.stdout

    @respx.mock
    def test_download_file_overwrite_confirm_yes(self, mock_config, tmp_path):
        """Test downloading with overwrite confirmation (yes)."""
        output_path = tmp_path / "existing.csv"
        output_path.write_text("old content")

        respx.get("http://test-api/projects/proj-1/files/file-456/download").mock(
            return_value=Response(200, content=b"new content")
        )

        result = runner.invoke(
            app,
            ["files", "download", "proj-1", "file-456", str(output_path)],
            input="y\n",
        )

        assert result.exit_code == 0
        assert output_path.read_text() == "new content"

    @respx.mock
    def test_download_file_overwrite_confirm_no(self, mock_config, tmp_path):
        """Test downloading with overwrite confirmation (no)."""
        output_path = tmp_path / "existing.csv"
        output_path.write_text("old content")

        result = runner.invoke(
            app,
            ["files", "download", "proj-1", "file-789", str(output_path)],
            input="n\n",
        )

        assert result.exit_code == 0
        assert "cancelled" in result.stdout.lower()
        assert output_path.read_text() == "old content"  # Not overwritten

    def test_download_to_directory_fails(self, mock_config, tmp_path):
        """Test that downloading to a directory path fails."""
        dir_path = tmp_path / "subdir"
        dir_path.mkdir()

        result = runner.invoke(app, ["files", "download", "proj-1", "file-x", str(dir_path)])

        assert result.exit_code == 1

    @respx.mock
    def test_download_file_not_found(self, mock_config, tmp_path):
        """Test downloading a file that doesn't exist."""
        output_path = tmp_path / "output.csv"

        respx.get("http://test-api/projects/proj-1/files/nonexistent/download").mock(
            return_value=Response(
                404,
                json={"message": "File not found", "detail": "File nonexistent does not exist"},
            )
        )

        result = runner.invoke(
            app, ["files", "download", "proj-1", "nonexistent", str(output_path)]
        )

        assert result.exit_code != 0
        assert not output_path.exists()


class TestFilesDelete:
    """Tests for 'files delete' command."""

    @respx.mock
    def test_delete_file_with_yes_flag(self, mock_config):
        """Test deleting a file with --yes flag (no confirmation)."""
        respx.delete("http://test-api/projects/proj-1/files/file-abc").mock(
            return_value=Response(204)
        )

        result = runner.invoke(app, ["files", "delete", "proj-1", "file-abc", "--yes"])

        assert result.exit_code == 0
        assert "deleted successfully" in result.stdout.lower()

    @respx.mock
    def test_delete_file_with_confirmation_yes(self, mock_config):
        """Test deleting a file with confirmation (yes)."""
        respx.delete("http://test-api/projects/proj-1/files/file-123").mock(
            return_value=Response(204)
        )

        result = runner.invoke(
            app, ["files", "delete", "proj-1", "file-123"], input="y\n"
        )

        assert result.exit_code == 0
        assert "deleted successfully" in result.stdout.lower()

    @respx.mock
    def test_delete_file_with_confirmation_no(self, mock_config):
        """Test deleting a file with confirmation (no)."""
        result = runner.invoke(
            app, ["files", "delete", "proj-1", "file-456"], input="n\n"
        )

        assert result.exit_code == 0
        assert "cancelled" in result.stdout.lower()

    @respx.mock
    def test_delete_file_json_output(self, mock_config):
        """Test deleting a file with JSON output."""
        respx.delete("http://test-api/projects/proj-1/files/file-xyz").mock(
            return_value=Response(204)
        )

        result = runner.invoke(
            app, ["--json", "files", "delete", "proj-1", "file-xyz", "--yes"]
        )

        assert result.exit_code == 0
        assert '"file_id": "file-xyz"' in result.stdout
        assert '"deleted": true' in result.stdout

    @respx.mock
    def test_delete_file_not_found(self, mock_config):
        """Test deleting a file that doesn't exist."""
        respx.delete("http://test-api/projects/proj-1/files/nonexistent").mock(
            return_value=Response(
                404,
                json={"message": "File not found", "detail": "File nonexistent does not exist"},
            )
        )

        result = runner.invoke(
            app, ["files", "delete", "proj-1", "nonexistent", "--yes"]
        )

        assert result.exit_code != 0

    @respx.mock
    def test_delete_file_with_y_short_flag(self, mock_config):
        """Test deleting a file with -y short flag."""
        respx.delete("http://test-api/projects/proj-1/files/file-short").mock(
            return_value=Response(204)
        )

        result = runner.invoke(app, ["files", "delete", "proj-1", "file-short", "-y"])

        assert result.exit_code == 0
        assert "deleted successfully" in result.stdout.lower()


class TestFilesIntegration:
    """Integration tests combining multiple file operations."""

    @respx.mock
    def test_upload_then_delete_workflow(self, mock_config, tmp_path):
        """Test workflow of uploading a file then deleting it."""
        # Create temp file
        test_file = tmp_path / "workflow.csv"
        test_file.write_text("col1,col2\nval1,val2")

        mock_file_info = {
            "id": "workflow-file",
            "name": "workflow.csv",
            "size_bytes": 20,
        }

        # Mock delete
        respx.delete("http://test-api/projects/proj-1/files/workflow-file").mock(
            return_value=Response(204)
        )

        # Upload with mocked client
        with patch("keboola_duckdb_cli.commands.files.get_client") as mock_get_client:
            mock_client = Mock()
            mock_client.upload_file_3stage.return_value = mock_file_info
            mock_get_client.return_value = mock_client

            upload_result = runner.invoke(app, ["files", "upload", "proj-1", str(test_file)])
            assert upload_result.exit_code == 0
            assert "workflow-file" in upload_result.stdout

        # Delete
        delete_result = runner.invoke(
            app, ["files", "delete", "proj-1", "workflow-file", "--yes"]
        )
        assert delete_result.exit_code == 0
        assert "deleted successfully" in delete_result.stdout.lower()

    @respx.mock
    def test_upload_download_roundtrip(self, mock_config, tmp_path):
        """Test uploading a file and downloading it back."""
        # Create source file
        source_file = tmp_path / "source.txt"
        source_file.write_text("original content")

        mock_file_info = {
            "id": "round-file",
            "name": "source.txt",
            "size_bytes": 16,
        }

        # Mock download
        respx.get("http://test-api/projects/proj-1/files/round-file/download").mock(
            return_value=Response(200, content=b"original content")
        )

        # Upload with mocked client
        with patch("keboola_duckdb_cli.commands.files.get_client") as mock_get_client:
            mock_client = Mock()
            mock_client.upload_file_3stage.return_value = mock_file_info
            mock_get_client.return_value = mock_client

            upload_result = runner.invoke(app, ["files", "upload", "proj-1", str(source_file)])
            assert upload_result.exit_code == 0

        # Download to different location
        download_path = tmp_path / "downloaded.txt"
        download_result = runner.invoke(
            app, ["files", "download", "proj-1", "round-file", str(download_path)]
        )
        assert download_result.exit_code == 0
        assert download_path.read_text() == "original content"


class TestFilesMissingConfig:
    """Tests for files commands without configuration."""

    def test_list_files_no_config(self):
        """Test that list fails without configuration."""
        result = runner.invoke(app, ["files", "list", "proj-1"])
        assert result.exit_code != 0

    def test_upload_file_no_config(self, tmp_path):
        """Test that upload fails without configuration."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        result = runner.invoke(app, ["files", "upload", "proj-1", str(test_file)])
        assert result.exit_code != 0
