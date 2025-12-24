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

    # Self-referential URL for S3-compatible API (used for file imports)
    # When running in Docker, this should be the internal service URL
    service_url: str = "http://localhost:8000"

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

    # S3 Pre-signed URL settings
    presign_secret_key: str | None = None  # Secret key for signing URLs (auto-generated if not set)
    presign_default_expiry: int = 3600  # Default URL expiry in seconds (1 hour)
    presign_max_expiry: int = 604800  # Max URL expiry in seconds (7 days)
    base_url: str = "http://localhost:8000"  # Base URL for pre-signed URLs

    # AWS Signature V4 settings (for boto3/aws-cli/rclone compatibility)
    # These are separate from project API keys - used only for S3-compatible API
    s3_access_key_id: str = "duckdb"  # AWS-style access key ID
    s3_secret_access_key: str | None = None  # AWS-style secret key (defaults to admin_api_key)
    s3_region: str = "local"  # Region name (used in signature, can be anything)
    s3_sig_v4_max_age_seconds: int = 900  # Max age of signed request (15 minutes)

    # PG Wire server settings (Phase 11b)
    pgwire_host: str = "localhost"
    pgwire_port: int = 5432
    pgwire_max_connections_total: int = 100
    pgwire_max_connections_per_workspace: int = 5
    pgwire_idle_timeout_seconds: int = 3600  # 1 hour
    pgwire_query_timeout_seconds: int = 300  # 5 minutes
    pgwire_session_memory_limit: str = "4GB"
    pgwire_ssl_mode: str = "prefer"

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
