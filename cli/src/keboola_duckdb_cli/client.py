"""HTTP client for DuckDB Storage API."""

from typing import Any, BinaryIO
from pathlib import Path
import json

import httpx
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn

from .config import CLIConfig, get_config


class APIError(Exception):
    """API error with status code and details."""

    def __init__(self, status_code: int, message: str, details: dict | None = None):
        self.status_code = status_code
        self.message = message
        self.details = details or {}
        super().__init__(f"[{status_code}] {message}")


class DuckDBClient:
    """HTTP client for DuckDB Storage API."""

    def __init__(self, config: CLIConfig | None = None, verbose: bool = False):
        self.config = config or get_config()
        self.verbose = verbose
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.config.url,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=60.0,
            )
        return self._client

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> "DuckDBClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _handle_response(self, response: httpx.Response) -> dict[str, Any]:
        """Handle API response, raising error if not successful."""
        if self.verbose:
            print(f"  -> {response.status_code} ({response.elapsed.total_seconds():.2f}s)")

        if response.status_code >= 400:
            try:
                error_data = response.json()
                message = error_data.get("message", error_data.get("detail", str(response.text)))
                details = error_data.get("details", {})
            except Exception:
                message = response.text or f"HTTP {response.status_code}"
                details = {}

            raise APIError(response.status_code, message, details)

        if response.status_code == 204:
            return {}

        try:
            return response.json()
        except Exception:
            return {}

    def get(self, path: str, params: dict | None = None) -> dict[str, Any]:
        """Make GET request."""
        if self.verbose:
            print(f"GET {path}")
        response = self.client.get(path, params=params)
        return self._handle_response(response)

    def post(
        self,
        path: str,
        json_data: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        """Make POST request with JSON body and optional query params."""
        if self.verbose:
            print(f"POST {path}")
        response = self.client.post(path, json=json_data, params=params)
        return self._handle_response(response)

    def delete(self, path: str) -> dict[str, Any]:
        """Make DELETE request."""
        if self.verbose:
            print(f"DELETE {path}")
        response = self.client.delete(path)
        return self._handle_response(response)

    def upload_file(
        self,
        path: str,
        file: BinaryIO,
        filename: str,
        show_progress: bool = True
    ) -> dict[str, Any]:
        """Upload a file using multipart form data."""
        if self.verbose:
            print(f"POST {path} (file upload)")

        # Get file size for progress
        file.seek(0, 2)
        file_size = file.tell()
        file.seek(0)

        # Build full URL
        full_url = f"{self.config.url}{path}"

        # For multipart uploads, use a fresh request to avoid header conflicts
        files = {"file": (filename, file)}
        headers = {"Authorization": f"Bearer {self.config.api_key}"}

        if show_progress and file_size > 1024 * 1024:  # Show progress for files > 1MB
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
            ) as progress:
                task = progress.add_task(f"Uploading {filename}", total=file_size)

                # Upload with progress tracking
                response = httpx.post(full_url, files=files, headers=headers, timeout=60.0)
                progress.update(task, completed=file_size)
        else:
            response = httpx.post(full_url, files=files, headers=headers, timeout=60.0)

        return self._handle_response(response)

    def download_file(
        self,
        path: str,
        output_path: Path,
        show_progress: bool = True
    ) -> None:
        """Download a file to local path."""
        if self.verbose:
            print(f"GET {path} (file download)")

        with self.client.stream("GET", path) as response:
            if response.status_code >= 400:
                # Read error body
                error_body = b""
                for chunk in response.iter_bytes():
                    error_body += chunk
                try:
                    error_data = json.loads(error_body.decode())
                    message = error_data.get("message", error_data.get("detail", "Download failed"))
                except Exception:
                    message = error_body.decode() or f"HTTP {response.status_code}"
                raise APIError(response.status_code, message)

            total = int(response.headers.get("content-length", 0))

            if show_progress and total > 1024 * 1024:  # Show progress for files > 1MB
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    DownloadColumn(),
                ) as progress:
                    task = progress.add_task(f"Downloading to {output_path.name}", total=total or None)

                    with open(output_path, "wb") as f:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))
            else:
                with open(output_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)

    # High-level file operations

    def upload_file_3stage(
        self,
        project_id: str,
        file_path: Path,
        show_progress: bool = True
    ) -> dict[str, Any]:
        """Upload a file using the 3-stage workflow.

        Returns the registered file info including file ID.
        """
        filename = file_path.name
        content_type = self._guess_content_type(filename)

        # Stage 1: Prepare
        prepare_response = self.post(
            f"/projects/{project_id}/files/prepare",
            {"filename": filename, "content_type": content_type}
        )
        upload_key = prepare_response["upload_key"]

        # Stage 2: Upload
        with open(file_path, "rb") as f:
            self.upload_file(
                f"/projects/{project_id}/files/upload/{upload_key}",
                f,
                filename,
                show_progress=show_progress
            )

        # Stage 3: Register
        file_info = self.post(
            f"/projects/{project_id}/files",
            {"upload_key": upload_key}
        )

        return file_info

    @staticmethod
    def _guess_content_type(filename: str) -> str:
        """Guess content type from filename."""
        ext = Path(filename).suffix.lower()
        content_types = {
            ".csv": "text/csv",
            ".json": "application/json",
            ".parquet": "application/x-parquet",
            ".txt": "text/plain",
            ".gz": "application/gzip",
        }
        return content_types.get(ext, "application/octet-stream")


def get_client(verbose: bool = False) -> DuckDBClient:
    """Get a configured API client."""
    config = get_config()
    errors = config.validate()
    if errors:
        raise ValueError("\n".join(errors))
    return DuckDBClient(config, verbose=verbose)
