"""Tests for configuration management."""

from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from keboola_duckdb_cli.config import CLIConfig, CONFIG_DIR, CONFIG_FILE
from keboola_duckdb_cli.main import app

runner = CliRunner()


class TestCLIConfig:
    """Tests for CLIConfig class."""

    def test_default_values(self) -> None:
        """Test that default values are empty strings."""
        config = CLIConfig()
        assert config.url == ""
        assert config.api_key == ""

    def test_load_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test loading configuration from environment variables."""
        monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "http://test-env")
        monkeypatch.setenv("KEBOOLA_DUCKDB_API_KEY", "test-key-env")

        config = CLIConfig.load()
        assert config.url == "http://test-env"
        assert config.api_key == "test-key-env"

    def test_load_from_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test loading configuration from file."""
        # Setup temp config file
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        data = {
            "url": "http://test-file",
            "api_key": "test-key-file",
        }
        with open(config_file, "w") as f:
            yaml.dump(data, f)

        # Mock CONFIG_FILE path
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig.load()
        assert config.url == "http://test-file"
        assert config.api_key == "test-key-file"

    def test_load_env_overrides_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that environment variables override file configuration."""
        # Setup temp config file
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        data = {
            "url": "http://test-file",
            "api_key": "test-key-file",
        }
        with open(config_file, "w") as f:
            yaml.dump(data, f)

        # Mock CONFIG_FILE path
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        # Set environment variables
        monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "http://test-env")
        monkeypatch.setenv("KEBOOLA_DUCKDB_API_KEY", "test-key-env")

        config = CLIConfig.load()
        assert config.url == "http://test-env"
        assert config.api_key == "test-key-env"

    def test_load_partial_env_override(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that environment variables can partially override file configuration."""
        # Setup temp config file
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        data = {
            "url": "http://test-file",
            "api_key": "test-key-file",
        }
        with open(config_file, "w") as f:
            yaml.dump(data, f)

        # Mock CONFIG_FILE path
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        # Set only URL in environment
        monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "http://test-env")

        config = CLIConfig.load()
        assert config.url == "http://test-env"
        assert config.api_key == "test-key-file"  # From file

    def test_load_missing_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test loading when config file doesn't exist."""
        config_file = tmp_path / "nonexistent" / "config.yaml"
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig.load()
        assert config.url == ""
        assert config.api_key == ""

    def test_load_invalid_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test loading when config file contains invalid YAML."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        # Write invalid YAML
        with open(config_file, "w") as f:
            f.write("invalid: yaml: content: [")

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        # Should not raise, just use defaults
        config = CLIConfig.load()
        assert config.url == ""
        assert config.api_key == ""

    def test_load_empty_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test loading when config file is empty."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        # Write empty file
        config_file.touch()

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig.load()
        assert config.url == ""
        assert config.api_key == ""

    def test_save(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test saving configuration to file."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig(url="http://test-save", api_key="test-key-save")
        config.save()

        # Verify file was created
        assert config_file.exists()

        # Verify content
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data["url"] == "http://test-save"
        assert data["api_key"] == "test-key-save"

    def test_save_creates_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that save creates config directory if it doesn't exist."""
        config_dir = tmp_path / "new_dir" / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig(url="http://test", api_key="test-key")
        config.save()

        assert config_dir.exists()
        assert config_file.exists()

    def test_set_value_url(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test setting URL value."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig()
        config.set_value("url", "http://new-url")

        assert config.url == "http://new-url"
        assert config_file.exists()

    def test_set_value_api_key(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test setting API key value."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig()
        config.set_value("api-key", "new-key-123")

        assert config.api_key == "new-key-123"
        assert config_file.exists()

    def test_set_value_api_key_variations(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test setting API key with different key name variations."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig()

        # Test variations
        for key_name in ["api-key", "api_key", "apikey"]:
            config.set_value(key_name, "test-key")
            assert config.api_key == "test-key"

    def test_set_value_unknown_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test setting unknown configuration key raises ValueError."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig()

        with pytest.raises(ValueError, match="Unknown config key: unknown"):
            config.set_value("unknown", "value")

    def test_get_value_url(self) -> None:
        """Test getting URL value."""
        config = CLIConfig(url="http://test-url")
        assert config.get_value("url") == "http://test-url"

    def test_get_value_api_key(self) -> None:
        """Test getting API key value."""
        config = CLIConfig(api_key="test-key-123")
        assert config.get_value("api-key") == "test-key-123"
        assert config.get_value("api_key") == "test-key-123"
        assert config.get_value("apikey") == "test-key-123"

    def test_get_value_unknown_key(self) -> None:
        """Test getting unknown configuration key raises ValueError."""
        config = CLIConfig()

        with pytest.raises(ValueError, match="Unknown config key: unknown"):
            config.get_value("unknown")

    def test_mask_key_short(self) -> None:
        """Test masking short API keys (8 chars or less)."""
        assert CLIConfig._mask_key("abc") == "***"
        assert CLIConfig._mask_key("12345678") == "********"
        assert CLIConfig._mask_key("") == ""
        assert CLIConfig._mask_key("x") == "*"

    def test_mask_key_long(self) -> None:
        """Test masking long API keys."""
        key = "proj_123_admin_abc123xyz"
        masked = CLIConfig._mask_key(key)
        assert masked.startswith("proj")
        assert masked.endswith("3xyz")
        assert "***" in masked
        assert len(masked) == len(key)

    def test_mask_key_medium(self) -> None:
        """Test masking medium-length API keys (9-16 chars)."""
        key = "test-key-9"  # 10 chars
        masked = CLIConfig._mask_key(key)
        assert masked.startswith("test")
        assert masked.endswith("ey-9")
        assert "*" in masked
        assert len(masked) == len(key)

    def test_to_dict(self) -> None:
        """Test converting config to dictionary."""
        config = CLIConfig(url="http://test", api_key="proj_123_admin_abc123xyz")
        result = config.to_dict()

        assert result["url"] == "http://test"
        assert result["api_key"].startswith("proj")
        assert "***" in result["api_key"]
        # Ensure actual key is not in output
        assert "abc123xyz" not in result["api_key"]

    def test_to_dict_empty_key(self) -> None:
        """Test converting config with empty API key to dictionary."""
        config = CLIConfig(url="http://test", api_key="")
        result = config.to_dict()

        assert result["url"] == "http://test"
        assert result["api_key"] == ""

    def test_validate_success(self) -> None:
        """Test validation with valid configuration."""
        config = CLIConfig(url="http://test", api_key="test-key")
        errors = config.validate()
        assert errors == []

    def test_validate_missing_url(self) -> None:
        """Test validation with missing URL."""
        config = CLIConfig(url="", api_key="test-key")
        errors = config.validate()
        assert len(errors) == 1
        assert "URL not configured" in errors[0]

    def test_validate_missing_api_key(self) -> None:
        """Test validation with missing API key."""
        config = CLIConfig(url="http://test", api_key="")
        errors = config.validate()
        assert len(errors) == 1
        assert "API key not configured" in errors[0]

    def test_validate_missing_both(self) -> None:
        """Test validation with both URL and API key missing."""
        config = CLIConfig(url="", api_key="")
        errors = config.validate()
        assert len(errors) == 2
        assert any("URL not configured" in e for e in errors)
        assert any("API key not configured" in e for e in errors)


class TestConfigCommands:
    """Tests for config CLI commands."""

    def test_config_show_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test 'config show' with default/empty configuration."""
        # Ensure no config file exists
        config_file = Path("/tmp/nonexistent-config.yaml")
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0
        # Check for key names in the table output (without colon)
        assert "url" in result.stdout
        assert "api_key" in result.stdout

    def test_config_show_with_values(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 'config show' with configured values."""
        # Setup temp config
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        data = {
            "url": "http://test-show",
            "api_key": "proj_123_admin_abc123xyz",
        }
        with open(config_file, "w") as f:
            yaml.dump(data, f)

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0
        assert "http://test-show" in result.stdout
        # API key should be masked
        assert "proj" in result.stdout
        assert "xyz" in result.stdout
        assert "***" in result.stdout
        # Full key should not be visible
        assert "abc123" not in result.stdout

    def test_config_show_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 'config show --json' output."""
        # Setup temp config
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        data = {
            "url": "http://test-json",
            "api_key": "test-key-123",
        }
        with open(config_file, "w") as f:
            yaml.dump(data, f)

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        result = runner.invoke(app, ["--json", "config", "show"])
        assert result.exit_code == 0
        assert '"url": "http://test-json"' in result.stdout
        assert '"api_key"' in result.stdout

    def test_config_set_url(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test 'config set url <value>' command."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        result = runner.invoke(app, ["config", "set", "url", "http://new-url"])
        assert result.exit_code == 0
        assert "Configuration updated" in result.stdout
        assert "url = http://new-url" in result.stdout

        # Verify file was created
        assert config_file.exists()
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data["url"] == "http://new-url"

    def test_config_set_api_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 'config set api-key <value>' command."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        result = runner.invoke(
            app, ["config", "set", "api-key", "proj_123_admin_abc123xyz"]
        )
        assert result.exit_code == 0
        assert "Configuration updated" in result.stdout
        # API key should be masked in output
        assert "proj" in result.stdout
        assert "xyz" in result.stdout
        assert "***" in result.stdout
        # Full key should not be visible
        assert "abc123" not in result.stdout

        # Verify file contains actual key (not masked)
        assert config_file.exists()
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data["api_key"] == "proj_123_admin_abc123xyz"

    def test_config_set_json(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test 'config set --json' output."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        result = runner.invoke(
            app, ["--json", "config", "set", "url", "http://test-json"]
        )
        assert result.exit_code == 0
        assert '"success": true' in result.stdout
        assert '"key": "url"' in result.stdout

    def test_config_set_unknown_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 'config set' with unknown key."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        result = runner.invoke(app, ["config", "set", "unknown-key", "value"])
        assert result.exit_code == 1
        # Error output goes to stderr via error_console (included in output)
        assert "Unknown config key" in result.output

    def test_config_set_updates_existing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that 'config set' updates existing values."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        # Create initial config
        data = {
            "url": "http://old-url",
            "api_key": "old-key",
        }
        with open(config_file, "w") as f:
            yaml.dump(data, f)

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        # Update URL
        result = runner.invoke(app, ["config", "set", "url", "http://new-url"])
        assert result.exit_code == 0

        # Verify URL updated but API key preserved
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data["url"] == "http://new-url"
        assert data["api_key"] == "old-key"

    def test_config_show_with_env_override(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 'config show' displays environment variable overrides."""
        # Setup temp config file
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        data = {
            "url": "http://file-url",
            "api_key": "file-key",
        }
        with open(config_file, "w") as f:
            yaml.dump(data, f)

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)
        monkeypatch.setattr(
            "keboola_duckdb_cli.commands.config_cmd.CONFIG_FILE", config_file
        )

        # Set environment variables
        monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "http://env-url")
        monkeypatch.setenv("KEBOOLA_DUCKDB_API_KEY", "env-key")

        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0
        # Should show env values, not file values
        assert "http://env-url" in result.stdout
        assert "http://file-url" not in result.stdout


class TestGetConfig:
    """Tests for get_config helper function."""

    def test_get_config_returns_loaded_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that get_config returns a properly loaded CLIConfig."""
        from keboola_duckdb_cli.config import get_config

        monkeypatch.setenv("KEBOOLA_DUCKDB_URL", "http://test-helper")
        monkeypatch.setenv("KEBOOLA_DUCKDB_API_KEY", "test-key-helper")

        config = get_config()
        assert isinstance(config, CLIConfig)
        assert config.url == "http://test-helper"
        assert config.api_key == "test-key-helper"


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_yaml_with_special_characters(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handling of special characters in configuration values."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        # URL with special characters
        special_url = "http://test.com:8080/path?query=value&other=123"
        # API key with special characters
        special_key = "proj_123_admin_!@#$%^&*()"

        config = CLIConfig(url=special_url, api_key=special_key)
        config.save()

        # Reload and verify
        loaded = CLIConfig.load()
        assert loaded.url == special_url
        assert loaded.api_key == special_key

    def test_concurrent_config_access(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that multiple config loads don't interfere."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"
        config_dir.mkdir()

        data = {
            "url": "http://concurrent-test",
            "api_key": "concurrent-key",
        }
        with open(config_file, "w") as f:
            yaml.dump(data, f)

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        # Load multiple times
        config1 = CLIConfig.load()
        config2 = CLIConfig.load()

        # Both should have same values
        assert config1.url == config2.url == "http://concurrent-test"
        assert config1.api_key == config2.api_key == "concurrent-key"

        # Modifying one shouldn't affect the other
        config1.url = "http://modified"
        assert config2.url == "http://concurrent-test"

    def test_case_insensitive_key_names(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that key names are case-insensitive."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig()

        # Test various case combinations
        config.set_value("URL", "http://test1")
        assert config.get_value("url") == "http://test1"

        config.set_value("Api-Key", "key1")
        assert config.get_value("API_KEY") == "key1"

        config.set_value("API-KEY", "key2")
        assert config.get_value("api-key") == "key2"

    def test_empty_string_values(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handling of empty string values."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig()
        config.set_value("url", "")
        config.set_value("api-key", "")

        assert config.url == ""
        assert config.api_key == ""

        # Reload from file
        loaded = CLIConfig.load()
        assert loaded.url == ""
        assert loaded.api_key == ""

    def test_whitespace_in_values(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that whitespace in values is preserved."""
        config_dir = tmp_path / ".keboola-duckdb"
        config_file = config_dir / "config.yaml"

        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_DIR", config_dir)
        monkeypatch.setattr("keboola_duckdb_cli.config.CONFIG_FILE", config_file)

        config = CLIConfig()
        config.set_value("url", "  http://test  ")
        config.set_value("api-key", "  key-with-spaces  ")

        assert config.url == "  http://test  "
        assert config.api_key == "  key-with-spaces  "

        # Reload from file
        loaded = CLIConfig.load()
        assert loaded.url == "  http://test  "
        assert loaded.api_key == "  key-with-spaces  "
