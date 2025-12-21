"""Import/Export command handlers for gRPC service."""

import sys
import time
import tempfile
from pathlib import Path
from typing import Optional, Any

import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import table_pb2, info_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import ProjectDBManager, TABLE_DATA_NAME, table_lock_manager


class TableImportFromFileHandler(BaseCommandHandler):
    """
    Import data from a file into a table.

    This handler:
    1. Parses file path and credentials from command
    2. Sets up DuckDB httpfs extension for S3 access
    3. Imports data using COPY FROM or INSERT INTO
    4. Handles full/incremental load modes

    Supports S3, ABS (Azure), and GCS file providers.
    """

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> table_pb2.TableImportResponse:
        cmd = table_pb2.TableImportFromFileCommand()
        command.Unpack(cmd)

        start_time = time.time()

        # Parse destination table
        dest = cmd.destination
        dest_path = list(dest.path)
        table_name = dest.tableName

        if len(dest_path) < 2:
            raise ValueError(
                "Destination path must contain [project_id, bucket_name]"
            )
        if not table_name:
            raise ValueError("tableName is required")

        project_id = dest_path[0]
        bucket_name = dest_path[-1]

        # Handle branch in path
        branch_id = "default"
        if len(dest_path) > 2:
            branch_id = dest_path[1]

        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Validate table exists
        if not self.project_manager.table_exists(
            effective_project_id, bucket_name, table_name
        ):
            raise KeyError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        # Parse file path
        file_path = cmd.filePath
        file_url = self._build_file_url(cmd.fileProvider, file_path)

        # Get S3 credentials
        s3_creds = self._extract_credentials(cmd.fileProvider, cmd.fileCredentials)

        # Parse import options
        import_opts = cmd.importOptions
        is_incremental = (
            import_opts.importType ==
            table_pb2.ImportExportShared.ImportOptions.ImportType.INCREMENTAL
        )

        # Parse CSV options if present
        csv_opts = self._extract_csv_options(cmd.formatTypeOptions)

        # Get table path
        table_path = self.project_manager.get_table_path(
            effective_project_id, bucket_name, table_name
        )

        # Execute import with table lock
        with table_lock_manager.acquire(project_id, bucket_name, table_name):
            result = self._execute_import(
                table_path=table_path,
                file_url=file_url,
                s3_creds=s3_creds,
                csv_opts=csv_opts,
                is_incremental=is_incremental,
            )

        duration = time.time() - start_time

        # Build response
        response = table_pb2.TableImportResponse()
        response.importedRowsCount = result["imported_rows"]
        response.tableRowsCount = result["total_rows"]
        response.tableSizeBytes = result["size_bytes"]
        response.importedColumns.extend(result.get("columns", []))

        # Add timing information
        timer = response.timers.add()
        timer.name = "total"
        timer.duration = f"{duration:.3f}s"

        self.log_info(
            f"Imported {result['imported_rows']} rows into {table_name} "
            f"(incremental={is_incremental})"
        )

        return response

    def _build_file_url(self, provider, file_path) -> str:
        """Build file URL from provider and path."""
        root = file_path.root
        path = file_path.path
        file_name = file_path.fileName

        # S3 provider
        if provider == table_pb2.ImportExportShared.FileProvider.S3:
            if path:
                return f"s3://{root}/{path}/{file_name}"
            return f"s3://{root}/{file_name}"

        # Azure Blob Storage
        if provider == table_pb2.ImportExportShared.FileProvider.ABS:
            if path:
                return f"azure://{root}/{path}/{file_name}"
            return f"azure://{root}/{file_name}"

        # Google Cloud Storage
        if provider == table_pb2.ImportExportShared.FileProvider.GCS:
            if path:
                return f"gcs://{root}/{path}/{file_name}"
            return f"gcs://{root}/{file_name}"

        raise ValueError(f"Unsupported file provider: {provider}")

    def _extract_credentials(self, provider, creds_any) -> dict:
        """Extract credentials from Any message."""
        creds = {}

        if provider == table_pb2.ImportExportShared.FileProvider.S3:
            s3_creds = table_pb2.ImportExportShared.S3Credentials()
            if creds_any.ByteSize() > 0:
                creds_any.Unpack(s3_creds)
                creds = {
                    "key": s3_creds.key,
                    "secret": s3_creds.secret,
                    "region": s3_creds.region,
                    "token": s3_creds.token if s3_creds.token else None,
                }

        return creds

    def _extract_csv_options(self, opts_any) -> dict:
        """Extract CSV options from Any message."""
        csv_opts = {}

        if opts_any.ByteSize() > 0:
            csv_type_opts = table_pb2.TableImportFromFileCommand.CsvTypeOptions()
            opts_any.Unpack(csv_type_opts)
            csv_opts = {
                "delimiter": csv_type_opts.delimiter or ",",
                "enclosure": csv_type_opts.enclosure or '"',
                "escaped_by": csv_type_opts.escapedBy or '"',
                "columns": list(csv_type_opts.columnsNames) if csv_type_opts.columnsNames else None,
            }

        return csv_opts

    def _execute_import(
        self,
        table_path: Path,
        file_url: str,
        s3_creds: dict,
        csv_opts: dict,
        is_incremental: bool,
    ) -> dict:
        """Execute the actual import operation."""
        conn = duckdb.connect(str(table_path))
        try:
            # Configure DuckDB for S3 access if credentials provided
            if s3_creds.get("key"):
                conn.execute("INSTALL httpfs; LOAD httpfs;")
                conn.execute(f"SET s3_access_key_id='{s3_creds['key']}'")
                conn.execute(f"SET s3_secret_access_key='{s3_creds['secret']}'")
                if s3_creds.get("region"):
                    conn.execute(f"SET s3_region='{s3_creds['region']}'")
                if s3_creds.get("token"):
                    conn.execute(f"SET s3_session_token='{s3_creds['token']}'")

            # Get column info
            columns_result = conn.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{TABLE_DATA_NAME}'
                ORDER BY ordinal_position
            """).fetchall()
            columns = [row[0] for row in columns_result]

            # Get row count before import
            rows_before = conn.execute(
                f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
            ).fetchone()[0]

            # If not incremental, truncate table first
            if not is_incremental:
                conn.execute(f"DELETE FROM main.{TABLE_DATA_NAME}")

            # Build CSV options for COPY
            csv_copy_opts = ["FORMAT CSV", "HEADER true"]
            if csv_opts.get("delimiter"):
                csv_copy_opts.append(f"DELIMITER '{csv_opts['delimiter']}'")

            # Import using COPY or INSERT INTO ... SELECT
            copy_sql = f"""
                INSERT INTO main.{TABLE_DATA_NAME}
                SELECT * FROM read_csv('{file_url}', {', '.join(csv_copy_opts)})
            """

            try:
                conn.execute(copy_sql)
            except Exception as e:
                self.log_error(f"Import failed: {e}")
                raise ValueError(f"Failed to import file: {e}")

            # Get final counts
            rows_after = conn.execute(
                f"SELECT COUNT(*) FROM main.{TABLE_DATA_NAME}"
            ).fetchone()[0]

            conn.commit()

            # Calculate imported rows
            imported_rows = rows_after - (0 if not is_incremental else rows_before)

            # Get file size
            size_bytes = table_path.stat().st_size

            return {
                "imported_rows": imported_rows,
                "total_rows": rows_after,
                "size_bytes": size_bytes,
                "columns": columns,
            }
        finally:
            conn.close()


class TableExportToFileHandler(BaseCommandHandler):
    """
    Export table data to a file.

    This handler:
    1. Parses source table and destination file path
    2. Sets up DuckDB httpfs extension for S3 access
    3. Exports data using COPY TO
    4. Returns table info

    Supports S3, ABS (Azure), and GCS file providers.
    """

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> table_pb2.TableExportToFileResponse:
        cmd = table_pb2.TableExportToFileCommand()
        command.Unpack(cmd)

        # Parse source table
        src = cmd.source
        src_path = list(src.path)
        table_name = src.tableName

        if len(src_path) < 2:
            raise ValueError(
                "Source path must contain [project_id, bucket_name]"
            )
        if not table_name:
            raise ValueError("tableName is required")

        project_id = src_path[0]
        bucket_name = src_path[-1]

        # Handle branch in path
        branch_id = "default"
        if len(src_path) > 2:
            branch_id = src_path[1]

        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Validate table exists
        table_info = self.project_manager.get_table(
            effective_project_id, bucket_name, table_name
        )
        if not table_info:
            raise KeyError(
                f"Table not found: {project_id}/{bucket_name}/{table_name}"
            )

        # Parse destination file path
        file_path = cmd.filePath
        file_url = self._build_file_url(cmd.fileProvider, file_path)

        # Get S3 credentials
        s3_creds = self._extract_credentials(cmd.fileProvider, cmd.fileCredentials)

        # Parse export options
        export_opts = cmd.exportOptions
        columns_to_export = list(export_opts.columnsToExport) if export_opts.columnsToExport else None
        is_compressed = export_opts.isCompressed

        # Get table path
        table_path = self.project_manager.get_table_path(
            effective_project_id, bucket_name, table_name
        )

        # Execute export (read-only, no lock needed)
        rows_exported = self._execute_export(
            table_path=table_path,
            file_url=file_url,
            s3_creds=s3_creds,
            columns=columns_to_export,
            is_compressed=is_compressed,
            file_format=cmd.fileFormat,
        )

        # Build response with TableInfo
        response = table_pb2.TableExportToFileResponse()

        # Fill tableInfo
        ti = response.tableInfo
        ti.path.extend(src_path)
        ti.tableName = table_name
        ti.rowsCount = rows_exported
        ti.sizeBytes = table_info.get("size_bytes", 0)
        ti.tableType = info_pb2.NORMAL

        # Add columns
        for col in table_info.get("columns", []):
            tc = ti.columns.add()
            tc.name = col["name"]
            tc.type = col["type"]
            tc.nullable = col.get("nullable", True)

        # Add primary keys
        ti.primaryKeysNames.extend(table_info.get("primary_key", []))

        self.log_info(f"Exported {rows_exported} rows from {table_name}")

        return response

    def _build_file_url(self, provider, file_path) -> str:
        """Build file URL from provider and path."""
        root = file_path.root
        path = file_path.path
        file_name = file_path.fileName

        # S3 provider
        if provider == table_pb2.ImportExportShared.FileProvider.S3:
            if path:
                return f"s3://{root}/{path}/{file_name}"
            return f"s3://{root}/{file_name}"

        # Azure Blob Storage
        if provider == table_pb2.ImportExportShared.FileProvider.ABS:
            if path:
                return f"azure://{root}/{path}/{file_name}"
            return f"azure://{root}/{file_name}"

        # Google Cloud Storage
        if provider == table_pb2.ImportExportShared.FileProvider.GCS:
            if path:
                return f"gcs://{root}/{path}/{file_name}"
            return f"gcs://{root}/{file_name}"

        raise ValueError(f"Unsupported file provider: {provider}")

    def _extract_credentials(self, provider, creds_any) -> dict:
        """Extract credentials from Any message."""
        creds = {}

        if provider == table_pb2.ImportExportShared.FileProvider.S3:
            s3_creds = table_pb2.ImportExportShared.S3Credentials()
            if creds_any.ByteSize() > 0:
                creds_any.Unpack(s3_creds)
                creds = {
                    "key": s3_creds.key,
                    "secret": s3_creds.secret,
                    "region": s3_creds.region,
                }

        return creds

    def _execute_export(
        self,
        table_path: Path,
        file_url: str,
        s3_creds: dict,
        columns: Optional[list],
        is_compressed: bool,
        file_format,
    ) -> int:
        """Execute the actual export operation."""
        conn = duckdb.connect(str(table_path), read_only=True)
        try:
            # Configure DuckDB for S3 access if credentials provided
            if s3_creds.get("key"):
                conn.execute("INSTALL httpfs; LOAD httpfs;")
                conn.execute(f"SET s3_access_key_id='{s3_creds['key']}'")
                conn.execute(f"SET s3_secret_access_key='{s3_creds['secret']}'")
                if s3_creds.get("region"):
                    conn.execute(f"SET s3_region='{s3_creds['region']}'")

            # Build SELECT query
            columns_sql = ", ".join(columns) if columns else "*"
            select_sql = f"SELECT {columns_sql} FROM main.{TABLE_DATA_NAME}"

            # Count rows
            count_result = conn.execute(
                f"SELECT COUNT(*) FROM ({select_sql})"
            ).fetchone()
            rows_count = count_result[0] if count_result else 0

            # Build COPY TO options
            copy_opts = []
            if file_format == table_pb2.ImportExportShared.FileFormat.CSV:
                copy_opts.append("FORMAT CSV")
                copy_opts.append("HEADER true")
                if is_compressed:
                    copy_opts.append("COMPRESSION GZIP")
            else:
                # Default to CSV
                copy_opts.append("FORMAT CSV")
                copy_opts.append("HEADER true")

            # Execute export
            copy_sql = f"COPY ({select_sql}) TO '{file_url}' ({', '.join(copy_opts)})"

            try:
                conn.execute(copy_sql)
            except Exception as e:
                self.log_error(f"Export failed: {e}")
                raise ValueError(f"Failed to export to file: {e}")

            return rows_count
        finally:
            conn.close()
