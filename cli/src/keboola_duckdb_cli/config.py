"""Configuration management for Keboola DuckDB CLI."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import os

import yaml


CONFIG_DIR = Path.home() / ".keboola-duckdb"
CONFIG_FILE = CONFIG_DIR / "config.yaml"


@dataclass
class CLIConfig:
    """CLI configuration."""

    url: str = ""
    api_key: str = ""

    @classmethod
    def load(cls) -> "CLIConfig":
        """Load configuration from file and environment.

        Priority (highest to lowest):
        1. Environment variables (KEBOOLA_DUCKDB_URL, KEBOOLA_DUCKDB_API_KEY)
        2. Config file (~/.keboola-duckdb/config.yaml)
        3. Defaults
        """
        config = cls()

        # Load from file
        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE) as f:
                    data = yaml.safe_load(f) or {}
                config.url = data.get("url", "")
                config.api_key = data.get("api_key", "")
            except Exception:
                pass  # Ignore file errors, use defaults

        # Override with environment variables
        if env_url := os.environ.get("KEBOOLA_DUCKDB_URL"):
            config.url = env_url
        if env_key := os.environ.get("KEBOOLA_DUCKDB_API_KEY"):
            config.api_key = env_key

        return config

    def save(self) -> None:
        """Save configuration to file."""
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)

        data = {
            "url": self.url,
            "api_key": self.api_key,
        }

        with open(CONFIG_FILE, "w") as f:
            yaml.dump(data, f, default_flow_style=False)

    def set_value(self, key: str, value: str) -> None:
        """Set a configuration value."""
        key_normalized = key.lower().replace("-", "_")

        if key_normalized == "url":
            self.url = value
        elif key_normalized in ("api_key", "apikey"):
            self.api_key = value
        else:
            raise ValueError(f"Unknown config key: {key}")

        self.save()

    def get_value(self, key: str) -> str:
        """Get a configuration value."""
        key_normalized = key.lower().replace("-", "_")

        if key_normalized == "url":
            return self.url
        elif key_normalized in ("api_key", "apikey"):
            return self.api_key
        else:
            raise ValueError(f"Unknown config key: {key}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for display."""
        return {
            "url": self.url,
            "api_key": self._mask_key(self.api_key) if self.api_key else "",
        }

    @staticmethod
    def _mask_key(key: str) -> str:
        """Mask API key for display."""
        if len(key) <= 8:
            return "*" * len(key)
        return key[:4] + "*" * (len(key) - 8) + key[-4:]

    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []
        if not self.url:
            errors.append("URL not configured. Use: keboola-duckdb config set url <url>")
        if not self.api_key:
            errors.append("API key not configured. Use: keboola-duckdb config set api-key <key>")
        return errors


def get_config() -> CLIConfig:
    """Get the current configuration."""
    return CLIConfig.load()
