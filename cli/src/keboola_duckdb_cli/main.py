"""Main CLI entry point for Keboola DuckDB CLI."""

from typing import Optional

import typer

from . import __version__
from .client import APIError
from .output import print_error


# Create main app
app = typer.Typer(
    name="keboola-duckdb",
    help="CLI tool for Keboola DuckDB Storage API",
    no_args_is_help=True,
)

# Global state
class GlobalState:
    json_output: bool = False
    verbose: bool = False

state = GlobalState()


def version_callback(value: bool) -> None:
    """Show version and exit."""
    if value:
        print(f"keboola-duckdb version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    json_output: bool = typer.Option(
        False, "--json", "-j",
        help="Output as JSON instead of tables"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Show debug information"
    ),
    version: Optional[bool] = typer.Option(
        None, "--version", "-V",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit"
    ),
) -> None:
    """Keboola DuckDB CLI - Command line tool for DuckDB Storage API."""
    state.json_output = json_output
    state.verbose = verbose


# Import and register command groups
from .commands import config_cmd, projects, buckets, tables, files

app.add_typer(config_cmd.app, name="config")
app.add_typer(projects.app, name="projects")
app.add_typer(buckets.app, name="buckets")
app.add_typer(tables.app, name="tables")
app.add_typer(files.app, name="files")


if __name__ == "__main__":
    app()
