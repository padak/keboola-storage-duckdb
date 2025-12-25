"""Output formatting for CLI."""

from typing import Any
import json

from rich.console import Console
from rich.table import Table
from rich import box


console = Console()
error_console = Console(stderr=True)


def print_json(data: Any) -> None:
    """Print data as formatted JSON."""
    print(json.dumps(data, indent=2, default=str))


def print_table(
    data: list[dict[str, Any]] | list[str],
    columns: list[str] | list[list[Any]] | None = None,
    title: str | None = None,
) -> None:
    """Print data as a formatted table.

    Supports two calling styles:
    1. print_table(list_of_dicts, columns=["col1", "col2"], title="Title")
    2. print_table(["col1", "col2"], [["val1", "val2"], ["val3", "val4"]])  # Legacy
    """
    # Handle legacy style: print_table(headers, rows)
    if data and isinstance(data, list) and len(data) > 0 and isinstance(data[0], str):
        # This is the legacy call style where data is actually headers
        headers = data
        rows = columns if columns else []

        if not rows:
            console.print("[dim]No data[/dim]")
            return

        table = Table(title=title, box=box.ROUNDED)
        for header in headers:
            table.add_column(str(header), style="cyan" if header in ("id", "name", "Name") else None)

        for row in rows:
            table.add_row(*[str(v) for v in row])

        console.print(table)
        return

    # Modern style: print_table(list_of_dicts, ...)
    if not data:
        console.print("[dim]No data[/dim]")
        return

    # Determine columns from data if not specified
    if columns is None:
        columns = list(data[0].keys())

    table = Table(title=title, box=box.ROUNDED)

    for col in columns:
        table.add_column(col, style="cyan" if col in ("id", "name") else None)

    for row in data:
        values = []
        for col in columns:
            value = row.get(col, "")
            if value is None:
                value = ""
            elif isinstance(value, bool):
                value = "Yes" if value else "No"
            elif isinstance(value, (list, dict)):
                value = json.dumps(value)
            else:
                value = str(value)
            values.append(value)
        table.add_row(*values)

    console.print(table)


def print_dict(
    data: dict[str, Any],
    title: str | None = None,
) -> None:
    """Print a single dict as a key-value table."""
    table = Table(title=title, box=box.ROUNDED, show_header=False)
    table.add_column("Key", style="cyan")
    table.add_column("Value")

    for key, value in data.items():
        if value is None:
            value_str = ""
        elif isinstance(value, bool):
            value_str = "Yes" if value else "No"
        elif isinstance(value, (list, dict)):
            value_str = json.dumps(value)
        else:
            value_str = str(value)
        table.add_row(key, value_str)

    console.print(table)


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[green]{message}[/green]")


def print_error(message: str) -> None:
    """Print an error message to stderr."""
    error_console.print(f"[red]Error: {message}[/red]")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[yellow]Warning: {message}[/yellow]")


def print_info(message: str) -> None:
    """Print an info message."""
    console.print(f"[dim]{message}[/dim]")


def format_bytes(size: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"
