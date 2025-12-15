"""Application configuration using pydantic-settings."""

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # API settings
    api_title: str = "DuckDB Storage API"
    api_version: str = "0.1.0"
    api_prefix: str = "/api/v1"
    debug: bool = False

    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000

    # Storage paths
    data_dir: Path = Path("/data")
    duckdb_dir: Path = Path("/data/duckdb")
    files_dir: Path = Path("/data/files")
    snapshots_dir: Path = Path("/data/snapshots")

    # DuckDB settings
    duckdb_threads: int = 4
    duckdb_memory_limit: str = "4GB"

    # Timeouts (seconds)
    operation_timeout: int = 240
    connection_timeout: int = 10

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
