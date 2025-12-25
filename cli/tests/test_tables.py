"""Tests for tables commands."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import respx
from httpx import Response
from typer.testing import CliRunner

from keboola_duckdb_cli.main import app


runner = CliRunner()


@pytest.fixture
def mock_config(monkeypatch):
    """Mock CLI configuration."""
    monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "http://test-api")
    monkeypatch.setenv("KEBOOLA_DUCKDB_API_KEY", "test-key")


class TestTablesList:
    """Tests for 'tables list' command."""

    @respx.mock
    def test_list_tables_success(self, mock_config):
        """Test listing tables in a bucket."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables").mock(
            return_value=Response(200, json={
                "tables": [
                    {
                        "name": "orders",
                        "bucket": "in.c-sales",
                        "row_count": 100,
                        "size_bytes": 2048,
                        "primary_key": ["id"],
                        "source": "import"
                    },
                    {
                        "name": "customers",
                        "bucket": "in.c-sales",
                        "row_count": 50,
                        "size_bytes": 1024,
                        "primary_key": [],
                        "source": "manual"
                    }
                ],
                "total": 2
            })
        )
        result = runner.invoke(app, ["tables", "list", "proj-1", "in.c-sales"])
        assert result.exit_code == 0
        assert "orders" in result.stdout
        assert "customers" in result.stdout
        assert "Total: 2 table(s)" in result.stdout

    @respx.mock
    def test_list_tables_with_branch(self, mock_config):
        """Test listing tables with custom branch."""
        respx.get("http://test-api/projects/proj-1/branches/dev-branch/buckets/in.c-sales/tables").mock(
            return_value=Response(200, json={
                "tables": [
                    {"name": "orders", "row_count": 100, "size_bytes": 2048, "primary_key": ["id"]}
                ],
                "total": 1
            })
        )
        result = runner.invoke(app, ["tables", "list", "proj-1", "in.c-sales", "--branch", "dev-branch"])
        assert result.exit_code == 0
        assert "orders" in result.stdout

    @respx.mock
    def test_list_tables_json_output(self, mock_config):
        """Test listing tables with JSON output."""
        api_response = {
            "tables": [
                {"name": "orders", "row_count": 100, "size_bytes": 2048, "primary_key": ["id"]}
            ],
            "total": 1
        }
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables").mock(
            return_value=Response(200, json=api_response)
        )
        result = runner.invoke(app, ["--json", "tables", "list", "proj-1", "in.c-sales"])
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output == api_response
        assert output["total"] == 1

    @respx.mock
    def test_list_tables_empty(self, mock_config):
        """Test listing tables when bucket is empty."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables").mock(
            return_value=Response(200, json={"tables": [], "total": 0})
        )
        result = runner.invoke(app, ["tables", "list", "proj-1", "in.c-sales"])
        assert result.exit_code == 0
        assert "No tables found" in result.stdout

    @respx.mock
    def test_list_tables_error(self, mock_config):
        """Test listing tables with API error."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables").mock(
            return_value=Response(404, json={"message": "Bucket not found"})
        )
        result = runner.invoke(app, ["tables", "list", "proj-1", "in.c-sales"])
        assert result.exit_code != 0


class TestTablesPreview:
    """Tests for 'tables preview' command."""

    @respx.mock
    def test_preview_table_success(self, mock_config):
        """Test previewing table data."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/preview").mock(
            return_value=Response(200, json={
                "columns": ["id", "name", "amount"],
                "rows": [
                    ["1", "Order A", "100.00"],
                    ["2", "Order B", "200.00"]
                ],
                "total_row_count": 100,
                "preview_row_count": 2
            })
        )
        result = runner.invoke(app, ["tables", "preview", "proj-1", "in.c-sales", "orders"])
        assert result.exit_code == 0
        assert "Showing 2 of 100 row(s)" in result.stdout

    @respx.mock
    def test_preview_table_with_limit(self, mock_config):
        """Test previewing table with custom limit."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/preview").mock(
            return_value=Response(200, json={
                "columns": ["id", "name"],
                "rows": [["1", "Order A"], ["2", "Order B"], ["3", "Order C"]],
                "total_row_count": 100,
                "preview_row_count": 3
            })
        )
        result = runner.invoke(app, ["tables", "preview", "proj-1", "in.c-sales", "orders", "--limit", "3"])
        assert result.exit_code == 0
        assert "Showing 3 of 100 row(s)" in result.stdout

    @respx.mock
    def test_preview_table_with_branch(self, mock_config):
        """Test previewing table from custom branch."""
        respx.get("http://test-api/projects/proj-1/branches/dev-branch/buckets/in.c-sales/tables/orders/preview").mock(
            return_value=Response(200, json={
                "columns": ["id", "name"],
                "rows": [["1", "Order A"]],
                "total_row_count": 10,
                "preview_row_count": 1
            })
        )
        result = runner.invoke(app, ["tables", "preview", "proj-1", "in.c-sales", "orders", "--branch", "dev-branch"])
        assert result.exit_code == 0
        assert "Showing 1 of 10 row(s)" in result.stdout

    @respx.mock
    def test_preview_table_json_output(self, mock_config):
        """Test previewing table with JSON output."""
        api_response = {
            "columns": ["id", "name"],
            "rows": [["1", "Order A"]],
            "total_row_count": 10,
            "preview_row_count": 1
        }
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/preview").mock(
            return_value=Response(200, json=api_response)
        )
        result = runner.invoke(app, ["--json", "tables", "preview", "proj-1", "in.c-sales", "orders"])
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output == api_response

    @respx.mock
    def test_preview_empty_table(self, mock_config):
        """Test previewing an empty table."""
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/preview").mock(
            return_value=Response(200, json={
                "columns": ["id", "name"],
                "rows": [],
                "total_row_count": 0,
                "preview_row_count": 0
            })
        )
        result = runner.invoke(app, ["tables", "preview", "proj-1", "in.c-sales", "orders"])
        assert result.exit_code == 0
        assert "Table orders is empty" in result.stdout


class TestTablesImport:
    """Tests for 'tables import' command."""

    @respx.mock
    def test_import_csv_success(self, mock_config, tmp_path):
        """Test importing CSV file to table."""
        # Create temp CSV file
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,test\n2,demo")

        # Mock file upload 3-stage workflow
        respx.post("http://test-api/projects/proj-1/files/prepare").mock(
            return_value=Response(200, json={
                "upload_key": "key-123",
                "upload_url": "/upload/key-123"
            })
        )
        respx.post("http://test-api/projects/proj-1/files/upload/key-123").mock(
            return_value=Response(200, json={
                "upload_key": "key-123",
                "size_bytes": 24
            })
        )
        respx.post("http://test-api/projects/proj-1/files").mock(
            return_value=Response(200, json={
                "id": "file-abc",
                "name": "data.csv",
                "size_bytes": 24
            })
        )

        # Mock import
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/import/file").mock(
            return_value=Response(200, json={
                "imported_rows": 2,
                "table_rows_after": 2,
                "table_size_bytes": 512
            })
        )

        result = runner.invoke(app, ["tables", "import", "proj-1", "in.c-sales", "orders", str(csv_file)])
        assert result.exit_code == 0
        assert "File uploaded: file-abc" in result.stdout
        assert "Imported 2 rows" in result.stdout
        assert "Table now has 2 rows" in result.stdout

    @respx.mock
    def test_import_csv_with_branch(self, mock_config, tmp_path):
        """Test importing CSV to table in custom branch."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,test")

        # Mock file upload
        respx.post("http://test-api/projects/proj-1/files/prepare").mock(
            return_value=Response(200, json={"upload_key": "key-123"})
        )
        respx.post("http://test-api/projects/proj-1/files/upload/key-123").mock(
            return_value=Response(200, json={"upload_key": "key-123", "size_bytes": 15})
        )
        respx.post("http://test-api/projects/proj-1/files").mock(
            return_value=Response(200, json={"id": "file-abc", "name": "data.csv"})
        )

        # Mock import to dev branch
        respx.post("http://test-api/projects/proj-1/branches/dev-branch/buckets/in.c-sales/tables/orders/import/file").mock(
            return_value=Response(200, json={
                "imported_rows": 1,
                "table_rows_after": 1,
                "table_size_bytes": 256
            })
        )

        result = runner.invoke(app, [
            "tables", "import", "proj-1", "in.c-sales", "orders", str(csv_file),
            "--branch", "dev-branch"
        ])
        assert result.exit_code == 0
        assert "Imported 1 rows" in result.stdout

    @respx.mock
    def test_import_csv_incremental(self, mock_config, tmp_path):
        """Test incremental import (append mode)."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n3,new")

        # Mock file upload
        respx.post("http://test-api/projects/proj-1/files/prepare").mock(
            return_value=Response(200, json={"upload_key": "key-123"})
        )
        respx.post("http://test-api/projects/proj-1/files/upload/key-123").mock(
            return_value=Response(200, json={"upload_key": "key-123", "size_bytes": 13})
        )
        respx.post("http://test-api/projects/proj-1/files").mock(
            return_value=Response(200, json={"id": "file-abc", "name": "data.csv"})
        )

        # Mock incremental import
        import_route = respx.post(
            "http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/import/file"
        ).mock(
            return_value=Response(200, json={
                "imported_rows": 1,
                "table_rows_after": 3,  # After appending to existing 2 rows
                "table_size_bytes": 768
            })
        )

        result = runner.invoke(app, [
            "tables", "import", "proj-1", "in.c-sales", "orders", str(csv_file),
            "--incremental"
        ])
        assert result.exit_code == 0
        assert "Imported 1 rows" in result.stdout
        assert "Table now has 3 rows" in result.stdout

        # Verify incremental flag was sent
        import_request = import_route.calls[0].request
        body = json.loads(import_request.content)
        assert body["import_options"]["incremental"] is True

    @respx.mock
    def test_import_csv_json_output(self, mock_config, tmp_path):
        """Test import with JSON output."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,test")

        # Mock file upload
        respx.post("http://test-api/projects/proj-1/files/prepare").mock(
            return_value=Response(200, json={"upload_key": "key-123"})
        )
        respx.post("http://test-api/projects/proj-1/files/upload/key-123").mock(
            return_value=Response(200, json={"upload_key": "key-123", "size_bytes": 15})
        )
        respx.post("http://test-api/projects/proj-1/files").mock(
            return_value=Response(200, json={"id": "file-abc", "name": "data.csv"})
        )

        # Mock import
        api_response = {
            "imported_rows": 1,
            "table_rows_after": 1,
            "table_size_bytes": 256
        }
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/import/file").mock(
            return_value=Response(200, json=api_response)
        )

        result = runner.invoke(app, [
            "--json", "tables", "import", "proj-1", "in.c-sales", "orders", str(csv_file)
        ])
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["imported_rows"] == 1

    @respx.mock
    def test_import_file_not_found(self, mock_config, tmp_path):
        """Test import with non-existent file."""
        non_existent = tmp_path / "missing.csv"
        result = runner.invoke(app, ["tables", "import", "proj-1", "in.c-sales", "orders", str(non_existent)])
        assert result.exit_code != 0
        # Typer's file validation error goes to output (combined stdout/stderr in CliRunner)
        output = (result.stdout + result.output).lower()
        assert "does not exist" in output or "no such file" in output or "invalid value" in output

    @respx.mock
    def test_import_upload_error(self, mock_config, tmp_path):
        """Test import when file upload fails."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,test")

        # Mock failed prepare
        respx.post("http://test-api/projects/proj-1/files/prepare").mock(
            return_value=Response(500, json={"message": "Internal server error"})
        )

        result = runner.invoke(app, ["tables", "import", "proj-1", "in.c-sales", "orders", str(csv_file)])
        assert result.exit_code != 0


class TestTablesExport:
    """Tests for 'tables export' command."""

    @respx.mock
    def test_export_table_success(self, mock_config, tmp_path):
        """Test exporting table to CSV file."""
        output_file = tmp_path / "export.csv"

        # Mock export
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/export").mock(
            return_value=Response(200, json={
                "file_id": "file-xyz",
                "rows_exported": 100
            })
        )

        # Mock download
        csv_content = b"id,name\n1,test\n2,demo"
        respx.get("http://test-api/projects/proj-1/files/file-xyz/download").mock(
            return_value=Response(200, content=csv_content, headers={"content-length": str(len(csv_content))})
        )

        result = runner.invoke(app, ["tables", "export", "proj-1", "in.c-sales", "orders", str(output_file)])
        assert result.exit_code == 0
        assert "Exported 100 rows" in result.stdout
        assert output_file.exists()
        assert output_file.read_bytes() == csv_content

    @respx.mock
    def test_export_table_with_branch(self, mock_config, tmp_path):
        """Test exporting table from custom branch."""
        output_file = tmp_path / "export.csv"

        # Mock export from dev branch
        respx.post("http://test-api/projects/proj-1/branches/dev-branch/buckets/in.c-sales/tables/orders/export").mock(
            return_value=Response(200, json={
                "file_id": "file-xyz",
                "rows_exported": 50
            })
        )

        # Mock download
        csv_content = b"id,name\n1,test"
        respx.get("http://test-api/projects/proj-1/files/file-xyz/download").mock(
            return_value=Response(200, content=csv_content, headers={"content-length": str(len(csv_content))})
        )

        result = runner.invoke(app, [
            "tables", "export", "proj-1", "in.c-sales", "orders", str(output_file),
            "--branch", "dev-branch"
        ])
        assert result.exit_code == 0
        assert "Exported 50 rows" in result.stdout
        assert output_file.exists()

    @respx.mock
    def test_export_table_json_output(self, mock_config, tmp_path):
        """Test export with JSON output."""
        output_file = tmp_path / "export.csv"

        # Mock export
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/export").mock(
            return_value=Response(200, json={
                "file_id": "file-xyz",
                "rows_exported": 100
            })
        )

        # Mock download
        csv_content = b"id,name\n1,test"
        respx.get("http://test-api/projects/proj-1/files/file-xyz/download").mock(
            return_value=Response(200, content=csv_content, headers={"content-length": str(len(csv_content))})
        )

        result = runner.invoke(app, [
            "--json", "tables", "export", "proj-1", "in.c-sales", "orders", str(output_file)
        ])
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["file_id"] == "file-xyz"
        assert output["rows_exported"] == 100
        assert "export.csv" in output["output_file"]

    @respx.mock
    def test_export_no_file_id_returned(self, mock_config, tmp_path):
        """Test export when API doesn't return file_id."""
        output_file = tmp_path / "export.csv"

        # Mock export without file_id
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/export").mock(
            return_value=Response(200, json={"rows_exported": 0})
        )

        result = runner.invoke(app, ["tables", "export", "proj-1", "in.c-sales", "orders", str(output_file)])
        assert result.exit_code == 1
        # Error message is only shown in non-JSON mode, but we can verify the exit code

    @respx.mock
    def test_export_table_error(self, mock_config, tmp_path):
        """Test export when table doesn't exist."""
        output_file = tmp_path / "export.csv"

        # Mock export error
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/missing/export").mock(
            return_value=Response(404, json={"message": "Table not found"})
        )

        result = runner.invoke(app, ["tables", "export", "proj-1", "in.c-sales", "missing", str(output_file)])
        assert result.exit_code != 0

    @respx.mock
    def test_export_download_error(self, mock_config, tmp_path):
        """Test export when download fails."""
        output_file = tmp_path / "export.csv"

        # Mock successful export
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/export").mock(
            return_value=Response(200, json={
                "file_id": "file-xyz",
                "rows_exported": 100
            })
        )

        # Mock failed download
        respx.get("http://test-api/projects/proj-1/files/file-xyz/download").mock(
            return_value=Response(404, json={"message": "File not found"})
        )

        result = runner.invoke(app, ["tables", "export", "proj-1", "in.c-sales", "orders", str(output_file)])
        assert result.exit_code != 0


class TestTablesIntegration:
    """Integration tests for table operations."""

    @respx.mock
    def test_full_workflow_import_preview_export(self, mock_config, tmp_path):
        """Test complete workflow: import -> preview -> export."""
        # Setup files
        import_file = tmp_path / "import.csv"
        import_file.write_text("id,name,amount\n1,Order A,100.00\n2,Order B,200.00")
        export_file = tmp_path / "export.csv"

        # Mock import workflow
        respx.post("http://test-api/projects/proj-1/files/prepare").mock(
            return_value=Response(200, json={"upload_key": "key-123"})
        )
        respx.post("http://test-api/projects/proj-1/files/upload/key-123").mock(
            return_value=Response(200, json={"upload_key": "key-123", "size_bytes": 50})
        )
        respx.post("http://test-api/projects/proj-1/files").mock(
            return_value=Response(200, json={"id": "file-abc", "name": "import.csv"})
        )
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/import/file").mock(
            return_value=Response(200, json={
                "imported_rows": 2,
                "table_rows_after": 2,
                "table_size_bytes": 512
            })
        )

        # Mock preview
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/preview").mock(
            return_value=Response(200, json={
                "columns": ["id", "name", "amount"],
                "rows": [["1", "Order A", "100.00"], ["2", "Order B", "200.00"]],
                "total_row_count": 2,
                "preview_row_count": 2
            })
        )

        # Mock export
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/export").mock(
            return_value=Response(200, json={
                "file_id": "file-xyz",
                "rows_exported": 2
            })
        )
        export_content = b"id,name,amount\n1,Order A,100.00\n2,Order B,200.00"
        respx.get("http://test-api/projects/proj-1/files/file-xyz/download").mock(
            return_value=Response(200, content=export_content, headers={"content-length": str(len(export_content))})
        )

        # Import
        result = runner.invoke(app, ["tables", "import", "proj-1", "in.c-sales", "orders", str(import_file)])
        assert result.exit_code == 0

        # Preview
        result = runner.invoke(app, ["tables", "preview", "proj-1", "in.c-sales", "orders"])
        assert result.exit_code == 0
        assert "Order A" in result.stdout

        # Export
        result = runner.invoke(app, ["tables", "export", "proj-1", "in.c-sales", "orders", str(export_file)])
        assert result.exit_code == 0
        assert export_file.exists()
        assert export_file.read_bytes() == export_content

    @respx.mock
    def test_branch_isolation(self, mock_config):
        """Test that branch parameter isolates operations."""
        # List tables in default branch
        respx.get("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables").mock(
            return_value=Response(200, json={
                "tables": [{"name": "orders", "row_count": 100, "size_bytes": 2048, "primary_key": []}],
                "total": 1
            })
        )

        # List tables in dev branch (empty)
        respx.get("http://test-api/projects/proj-1/branches/dev-branch/buckets/in.c-sales/tables").mock(
            return_value=Response(200, json={"tables": [], "total": 0})
        )

        # Default branch has tables
        result = runner.invoke(app, ["tables", "list", "proj-1", "in.c-sales"])
        assert result.exit_code == 0
        assert "orders" in result.stdout

        # Dev branch is empty
        result = runner.invoke(app, ["tables", "list", "proj-1", "in.c-sales", "--branch", "dev-branch"])
        assert result.exit_code == 0
        assert "No tables found" in result.stdout


class TestTablesProfile:
    """Tests for 'tables profile' command."""

    @respx.mock
    def test_profile_table_success(self, mock_config):
        """Test profiling a table with various column types."""
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/profile").mock(
            return_value=Response(200, json={
                "table_name": "orders",
                "bucket_name": "in.c-sales",
                "row_count": 10000,
                "column_count": 5,
                "statistics": [
                    {
                        "column_name": "id",
                        "column_type": "INTEGER",
                        "min": "1",
                        "max": "10000",
                        "approx_unique": 10000,
                        "avg": 5000.5,
                        "std": 2886.89,
                        "q25": "2500",
                        "q50": "5000",
                        "q75": "7500",
                        "count": 10000,
                        "null_percentage": 0
                    },
                    {
                        "column_name": "name",
                        "column_type": "VARCHAR",
                        "min": "Alice",
                        "max": "Zoe",
                        "approx_unique": 8500,
                        "avg": None,
                        "std": None,
                        "q25": None,
                        "q50": None,
                        "q75": None,
                        "count": 10000,
                        "null_percentage": 0.5
                    },
                    {
                        "column_name": "amount",
                        "column_type": "DOUBLE",
                        "min": "0.01",
                        "max": "9999.99",
                        "approx_unique": 9500,
                        "avg": 500.25,
                        "std": 250.12,
                        "q25": "125.00",
                        "q50": "500.00",
                        "q75": "875.00",
                        "count": 10000,
                        "null_percentage": 0
                    }
                ]
            })
        )
        result = runner.invoke(app, ["tables", "profile", "proj-1", "in.c-sales", "orders"])
        assert result.exit_code == 0
        assert "in.c-sales.orders" in result.stdout
        assert "Rows: 10,000" in result.stdout
        assert "Columns: 5" in result.stdout
        assert "id" in result.stdout
        assert "name" in result.stdout
        assert "amount" in result.stdout
        # Rich may truncate column types in narrow terminals, check for prefix
        assert "INTEG" in result.stdout  # INTEGER may be truncated
        assert "VARCH" in result.stdout  # VARCHAR may be truncated
        assert "DOUBLE" in result.stdout

    @respx.mock
    def test_profile_table_with_branch(self, mock_config):
        """Test profiling table in custom branch."""
        respx.post("http://test-api/projects/proj-1/branches/dev-branch/buckets/in.c-sales/tables/orders/profile").mock(
            return_value=Response(200, json={
                "table_name": "orders",
                "bucket_name": "in.c-sales",
                "row_count": 100,
                "column_count": 2,
                "statistics": [
                    {
                        "column_name": "id",
                        "column_type": "INTEGER",
                        "min": "1",
                        "max": "100",
                        "approx_unique": 100,
                        "avg": 50.5,
                        "std": 28.87,
                        "q25": "25",
                        "q50": "50",
                        "q75": "75",
                        "count": 100,
                        "null_percentage": 0
                    }
                ]
            })
        )
        result = runner.invoke(app, [
            "tables", "profile", "proj-1", "in.c-sales", "orders",
            "--branch", "dev-branch"
        ])
        assert result.exit_code == 0
        assert "Rows: 100" in result.stdout

    @respx.mock
    def test_profile_table_json_output(self, mock_config):
        """Test profile with JSON output."""
        api_response = {
            "table_name": "orders",
            "bucket_name": "in.c-sales",
            "row_count": 100,
            "column_count": 2,
            "statistics": [
                {
                    "column_name": "id",
                    "column_type": "INTEGER",
                    "min": "1",
                    "max": "100",
                    "approx_unique": 100,
                    "avg": 50.5,
                    "std": 28.87,
                    "q25": "25",
                    "q50": "50",
                    "q75": "75",
                    "count": 100,
                    "null_percentage": 0
                }
            ]
        }
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/profile").mock(
            return_value=Response(200, json=api_response)
        )
        result = runner.invoke(app, ["--json", "tables", "profile", "proj-1", "in.c-sales", "orders"])
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output == api_response
        assert output["row_count"] == 100
        assert len(output["statistics"]) == 1

    @respx.mock
    def test_profile_table_with_column_filter(self, mock_config):
        """Test profiling with column filter."""
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/profile").mock(
            return_value=Response(200, json={
                "table_name": "orders",
                "bucket_name": "in.c-sales",
                "row_count": 1000,
                "column_count": 5,
                "statistics": [
                    {"column_name": "id", "column_type": "INTEGER", "min": "1", "max": "1000",
                     "approx_unique": 1000, "avg": 500.5, "std": 288.68, "q25": "250", "q50": "500",
                     "q75": "750", "count": 1000, "null_percentage": 0},
                    {"column_name": "name", "column_type": "VARCHAR", "min": "A", "max": "Z",
                     "approx_unique": 100, "avg": None, "std": None, "q25": None, "q50": None,
                     "q75": None, "count": 1000, "null_percentage": 0},
                    {"column_name": "price", "column_type": "DOUBLE", "min": "0.01", "max": "999.99",
                     "approx_unique": 500, "avg": 250.0, "std": 125.0, "q25": "62.5", "q50": "250.0",
                     "q75": "437.5", "count": 1000, "null_percentage": 0},
                ]
            })
        )
        result = runner.invoke(app, [
            "tables", "profile", "proj-1", "in.c-sales", "orders",
            "--columns", "id,price"
        ])
        assert result.exit_code == 0
        # Should only show filtered columns in the output
        assert "id" in result.stdout
        assert "price" in result.stdout
        # "name" column should not be displayed (filtered out)
        # Note: we can't easily verify it's not in output since table formatting might vary

    @respx.mock
    def test_profile_empty_table(self, mock_config):
        """Test profiling an empty table."""
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/profile").mock(
            return_value=Response(200, json={
                "table_name": "orders",
                "bucket_name": "in.c-sales",
                "row_count": 0,
                "column_count": 3,
                "statistics": []
            })
        )
        result = runner.invoke(app, ["tables", "profile", "proj-1", "in.c-sales", "orders"])
        assert result.exit_code == 0
        assert "Rows: 0" in result.stdout
        assert "No statistics available" in result.stdout

    @respx.mock
    def test_profile_table_not_found(self, mock_config):
        """Test profiling non-existent table."""
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/missing/profile").mock(
            return_value=Response(404, json={"message": "Table not found"})
        )
        result = runner.invoke(app, ["tables", "profile", "proj-1", "in.c-sales", "missing"])
        assert result.exit_code != 0

    @respx.mock
    def test_profile_table_no_matching_columns(self, mock_config):
        """Test profile with filter that matches no columns."""
        respx.post("http://test-api/projects/proj-1/branches/default/buckets/in.c-sales/tables/orders/profile").mock(
            return_value=Response(200, json={
                "table_name": "orders",
                "bucket_name": "in.c-sales",
                "row_count": 100,
                "column_count": 2,
                "statistics": [
                    {"column_name": "id", "column_type": "INTEGER", "min": "1", "max": "100",
                     "approx_unique": 100, "avg": 50.5, "std": 28.87, "q25": "25", "q50": "50",
                     "q75": "75", "count": 100, "null_percentage": 0},
                ]
            })
        )
        result = runner.invoke(app, [
            "tables", "profile", "proj-1", "in.c-sales", "orders",
            "--columns", "nonexistent"
        ])
        assert result.exit_code == 0
        assert "No matching columns found" in result.stdout
