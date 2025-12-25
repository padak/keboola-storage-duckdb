"""Configuration management commands."""

import typer

from ..config import get_config, CONFIG_FILE
from ..output import print_dict, print_success, print_error, print_json
from ..main import state


app = typer.Typer(help="Configuration management")


@app.command("set")
def set_config(
    key: str = typer.Argument(..., help="Configuration key (url, api-key)"),
    value: str = typer.Argument(..., help="Configuration value"),
) -> None:
    """Set a configuration value.

    Supported keys:
    - url: DuckDB API service URL
    - api-key: API authentication key

    Configuration is saved to ~/.keboola-duckdb/config.yaml
    """
    try:
        config = get_config()
        config.set_value(key, value)

        if state.json_output:
            print_json({"success": True, "key": key, "file": str(CONFIG_FILE)})
        else:
            # Mask the value if it's an API key
            display_value = value
            if key.lower() in ("api-key", "api_key", "apikey"):
                display_value = config._mask_key(value) if value else ""

            print_success(f"Configuration updated: {key} = {display_value}")
            print_success(f"Saved to: {CONFIG_FILE}")

    except ValueError as e:
        print_error(str(e))
        raise typer.Exit(1)


@app.command("show")
def show_config() -> None:
    """Show current configuration.

    Displays configuration from all sources:
    1. Environment variables (highest priority)
    2. Config file (~/.keboola-duckdb/config.yaml)
    3. Defaults

    API key is masked for security.
    """
    config = get_config()

    if state.json_output:
        print_json(config.to_dict())
    else:
        print_dict(config.to_dict(), title="Current Configuration")

        # Show config file location
        if CONFIG_FILE.exists():
            print_success(f"\nConfig file: {CONFIG_FILE}")
        else:
            print_error(f"\nConfig file not found: {CONFIG_FILE}")
