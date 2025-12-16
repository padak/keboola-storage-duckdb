"""Application configuration using pydantic-settings."""

from pathlib import Path
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    All settings can be configured via:
    1. Environment variables (e.g., DATA_DIR=/my/path)
    2. .env file in the project root

    Storage paths are derived from DATA_DIR by default but can be overridden.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # API settings
    api_title: str = "DuckDB Storage API"
    api_version: str = "0.1.0"
    api_prefix: str = "/api/v1"
    debug: bool = True  # Default to True for development

    # Authentication
    admin_api_key: str | None = None

    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000

    # Storage paths - all derived from data_dir by default
    data_dir: Path = Path("./data")

    # These can be overridden, but default to subdirs of data_dir
    duckdb_dir: Path | None = None
    files_dir: Path | None = None
    snapshots_dir: Path | None = None
    metadata_db_path: Path | None = None

    # DuckDB settings
    duckdb_threads: int = 4
    duckdb_memory_limit: str = "4GB"

    # Timeouts (seconds)
    operation_timeout: int = 240
    connection_timeout: int = 10

    @model_validator(mode="after")
    def set_default_paths(self) -> "Settings":
        """Set default paths based on data_dir if not explicitly provided."""
        if self.duckdb_dir is None:
            self.duckdb_dir = self.data_dir / "duckdb"
        if self.files_dir is None:
            self.files_dir = self.data_dir / "files"
        if self.snapshots_dir is None:
            self.snapshots_dir = self.data_dir / "snapshots"
        if self.metadata_db_path is None:
            self.metadata_db_path = self.data_dir / "metadata.duckdb"
        return self

    @property
    def storage_paths(self) -> dict[str, Path]:
        """Return all storage paths for health check validation."""
        return {
            "data_dir": self.data_dir,
            "duckdb_dir": self.duckdb_dir,
            "files_dir": self.files_dir,
            "snapshots_dir": self.snapshots_dir,
        }


# Global settings instance
settings = Settings()
