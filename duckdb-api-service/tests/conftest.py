"""Pytest configuration and fixtures."""

import pytest
from pathlib import Path
from fastapi.testclient import TestClient
import tempfile
import shutil

from src.main import app
from src.config import settings


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def temp_data_dir(monkeypatch):
    """Create temporary data directories for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # Create subdirectories
        data_dir = tmp_path / "data"
        duckdb_dir = data_dir / "duckdb"
        files_dir = data_dir / "files"
        snapshots_dir = data_dir / "snapshots"
        metadata_db_path = data_dir / "metadata.duckdb"

        for dir_path in [data_dir, duckdb_dir, files_dir, snapshots_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Patch settings
        monkeypatch.setattr(settings, "data_dir", data_dir)
        monkeypatch.setattr(settings, "duckdb_dir", duckdb_dir)
        monkeypatch.setattr(settings, "files_dir", files_dir)
        monkeypatch.setattr(settings, "snapshots_dir", snapshots_dir)
        monkeypatch.setattr(settings, "metadata_db_path", metadata_db_path)

        yield {
            "data_dir": data_dir,
            "duckdb_dir": duckdb_dir,
            "files_dir": files_dir,
            "snapshots_dir": snapshots_dir,
            "metadata_db_path": metadata_db_path,
        }


@pytest.fixture
def missing_data_dir(monkeypatch):
    """Configure settings with non-existent paths for testing errors."""
    nonexistent = Path("/nonexistent/path/that/does/not/exist")

    monkeypatch.setattr(settings, "data_dir", nonexistent)
    monkeypatch.setattr(settings, "duckdb_dir", nonexistent / "duckdb")
    monkeypatch.setattr(settings, "files_dir", nonexistent / "files")
    monkeypatch.setattr(settings, "snapshots_dir", nonexistent / "snapshots")
    monkeypatch.setattr(settings, "metadata_db_path", nonexistent / "metadata.duckdb")

    yield nonexistent


@pytest.fixture
def initialized_backend(client, temp_data_dir):
    """Initialize backend and metadata DB before tests."""
    from src import database
    database.metadata_db.initialize()

    response = client.post("/backend/init")
    assert response.status_code == 200

    yield temp_data_dir
