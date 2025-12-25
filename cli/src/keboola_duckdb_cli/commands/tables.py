"""Tables commands for Keboola DuckDB CLI."""

from pathlib import Path
from typing import Optional
import csv
import json
import re
from datetime import datetime
import typer

from ..client import get_client
from ..output import print_table, print_json, print_success, print_dict, format_bytes
from ..main import state

app = typer.Typer(help="Manage tables")


def infer_column_type(values: list[str]) -> str:
    """Infer column type from sample values."""
    # Filter out empty values
    non_empty = [v.strip() for v in values if v and v.strip()]

    if not non_empty:
        return "VARCHAR"

    # Check if all values are integers
    int_pattern = re.compile(r'^-?\d+$')
    if all(int_pattern.match(v) for v in non_empty):
        # Check if values fit in INTEGER vs BIGINT
        try:
            max_val = max(abs(int(v)) for v in non_empty)
            if max_val > 2147483647:
                return "BIGINT"
            return "INTEGER"
        except ValueError:
            pass

    # Check if all values are floats/decimals
    float_pattern = re.compile(r'^-?\d+\.?\d*$|^-?\d*\.?\d+$')
    if all(float_pattern.match(v) for v in non_empty):
        return "DOUBLE"

    # Check if all values are booleans
    bool_values = {'true', 'false', '1', '0', 'yes', 'no', 't', 'f'}
    if all(v.lower() in bool_values for v in non_empty):
        return "BOOLEAN"

    # Check if all values are dates/timestamps
    date_patterns = [
        (r'^\d{4}-\d{2}-\d{2}$', "DATE"),
        (r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', "TIMESTAMP"),
        (r'^\d{2}/\d{2}/\d{4}$', "DATE"),
    ]
    for pattern, dtype in date_patterns:
        if all(re.match(pattern, v) for v in non_empty):
            return dtype

    return "VARCHAR"


def infer_schema_from_csv(file_path: Path, sample_rows: int = 100) -> list[dict]:
    """Infer schema from CSV file by sampling rows."""
    columns = []

    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)

        # Get header
        header = next(reader)

        # Collect sample values for each column
        column_values: list[list[str]] = [[] for _ in header]

        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            for j, value in enumerate(row):
                if j < len(column_values):
                    column_values[j].append(value)

        # Infer type for each column
        for name, values in zip(header, column_values):
            col_type = infer_column_type(values)
            columns.append({"name": name, "type": col_type})

    return columns


@app.command("create")
def create_table(
    project: str = typer.Argument(..., help="Project ID"),
    bucket: str = typer.Argument(..., help="Bucket name"),
    name: str = typer.Argument(..., help="Table name"),
    columns: Optional[str] = typer.Option(
        None, "--columns", "-c",
        help='Columns as JSON: \'[{"name":"id","type":"INTEGER"},{"name":"name","type":"VARCHAR"}]\''
    ),
    from_csv: Optional[Path] = typer.Option(
        None, "--from-csv", "-f",
        help="Infer schema from CSV file (reads header and samples rows)"
    ),
    primary_key: Optional[str] = typer.Option(
        None, "--pk", "-p",
        help="Primary key columns (comma-separated)"
    ),
    branch: str = typer.Option("default", "--branch", "-b", help="Branch ID"),
) -> None:
    """Create a new table with specified schema.

    Schema can be provided via --columns JSON or inferred from --from-csv file.

    Column types: VARCHAR, INTEGER, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP, DATE

    Examples:
        # Manual schema
        keboola-duckdb tables create proj bucket mytable \\
            --columns '[{"name":"id","type":"INTEGER"},{"name":"name","type":"VARCHAR"}]' \\
            --pk id

        # Infer schema from CSV
        keboola-duckdb tables create proj bucket mytable --from-csv data.csv --pk id
    """
    # Validate options
    if not columns and not from_csv:
        typer.echo("Error: Either --columns or --from-csv is required", err=True)
        raise typer.Exit(1)

    if columns and from_csv:
        typer.echo("Error: Use either --columns or --from-csv, not both", err=True)
        raise typer.Exit(1)

    client = get_client(verbose=state.verbose)

    try:
        # Get columns from JSON or infer from CSV
        if from_csv:
            if not from_csv.exists():
                typer.echo(f"Error: File not found: {from_csv}", err=True)
                raise typer.Exit(1)

            if not state.json_output:
                typer.echo(f"Inferring schema from {from_csv.name}...")

            columns_list = infer_schema_from_csv(from_csv)

            if not state.json_output:
                typer.echo("Detected columns:")
                for col in columns_list:
                    typer.echo(f"  - {col['name']}: {col['type']}")
        else:
            try:
                columns_list = json.loads(columns)
            except json.JSONDecodeError as e:
                typer.echo(f"Error: Invalid JSON for columns: {e}", err=True)
                raise typer.Exit(1)

        payload = {
            "name": name,
            "columns": columns_list,
        }

        if primary_key:
            payload["primary_key"] = [col.strip() for col in primary_key.split(",")]

        response = client.post(
            f"/projects/{project}/branches/{branch}/buckets/{bucket}/tables",
            payload
        )

        if state.json_output:
            print_json(response)
        else:
            print_success(f"Table '{name}' created successfully")
            print_dict({
                "Name": response.get("name", name),
                "Bucket": bucket,
                "Columns": len(columns_list),
                "Primary Key": ", ".join(response.get("primary_key", [])) or "-",
            })
    finally:
        client.close()


@app.command("list")
def list_tables(
    project: str = typer.Argument(..., help="Project ID"),
    bucket: str = typer.Argument(..., help="Bucket name (e.g., in.c-mybucket)"),
    branch: str = typer.Option("default", help="Branch ID"),
) -> None:
    """List tables in a bucket."""
    client = get_client()

    result = client.get(f"/projects/{project}/branches/{branch}/buckets/{bucket}/tables")

    if state.json_output:
        print_json(result)
        return

    tables = result.get("tables", [])
    total = result.get("total", 0)

    if not tables:
        typer.echo(f"No tables found in bucket {bucket}")
        return

    # Prepare table data
    rows = []
    for table in tables:
        rows.append([
            table["name"],
            f"{table['row_count']:,}" if table.get("row_count") else "0",
            format_bytes(table.get("size_bytes", 0)),
            ", ".join(table.get("primary_key", [])) or "-",
            table.get("source", "-"),
        ])

    headers = ["Name", "Rows", "Size", "Primary Key", "Source"]
    print_table(headers, rows)
    typer.echo(f"\nTotal: {total} table(s)")


@app.command("preview")
def preview_table(
    project: str = typer.Argument(..., help="Project ID"),
    bucket: str = typer.Argument(..., help="Bucket name"),
    table: str = typer.Argument(..., help="Table name"),
    branch: str = typer.Option("default", help="Branch ID"),
    limit: int = typer.Option(10, help="Number of rows to preview"),
) -> None:
    """Preview table data."""
    client = get_client()

    result = client.get(
        f"/projects/{project}/branches/{branch}/buckets/{bucket}/tables/{table}/preview",
        params={"limit": limit}
    )

    if state.json_output:
        print_json(result)
        return

    columns = result.get("columns", [])
    rows = result.get("rows", [])
    total_row_count = result.get("total_row_count", 0)
    preview_row_count = result.get("preview_row_count", 0)

    if not rows:
        typer.echo(f"Table {table} is empty")
        return

    # Print table data
    print_table(columns, rows)
    typer.echo(f"\nShowing {preview_row_count} of {total_row_count:,} row(s)")


@app.command("import")
def import_table(
    project: str = typer.Argument(..., help="Project ID"),
    bucket: str = typer.Argument(..., help="Bucket name"),
    table: str = typer.Argument(..., help="Table name"),
    file: Path = typer.Argument(..., help="CSV file to import", exists=True, dir_okay=False),
    branch: str = typer.Option("default", help="Branch ID"),
    incremental: bool = typer.Option(False, help="Incremental import (append/update)"),
) -> None:
    """Import data from CSV file into table."""
    client = get_client()

    # Step 1: Upload file
    if not state.json_output:
        typer.echo(f"Uploading {file.name}...")
    file_info = client.upload_file_3stage(project, file)
    file_id = file_info["id"]
    if not state.json_output:
        typer.echo(f"File uploaded: {file_id}")

    # Step 2: Import to table
    if not state.json_output:
        typer.echo(f"Importing to table {table}...")
    result = client.post(
        f"/projects/{project}/branches/{branch}/buckets/{bucket}/tables/{table}/import/file",
        json_data={
            "file_id": file_id,
            "format": "csv",
            "import_options": {
                "incremental": incremental
            }
        }
    )

    if state.json_output:
        print_json(result)
        return

    imported_rows = result.get("imported_rows", 0)
    table_rows_after = result.get("table_rows_after", 0)
    table_size_bytes = result.get("table_size_bytes", 0)

    print_success(f"Imported {imported_rows:,} rows")
    typer.echo(f"Table now has {table_rows_after:,} rows ({format_bytes(table_size_bytes)})")


@app.command("export")
def export_table(
    project: str = typer.Argument(..., help="Project ID"),
    bucket: str = typer.Argument(..., help="Bucket name"),
    table: str = typer.Argument(..., help="Table name"),
    output: Path = typer.Argument(..., help="Output CSV file path"),
    branch: str = typer.Option("default", help="Branch ID"),
) -> None:
    """Export table data to CSV file."""
    client = get_client()

    # Step 1: Export table to file
    if not state.json_output:
        typer.echo(f"Exporting table {table}...")
    result = client.post(
        f"/projects/{project}/branches/{branch}/buckets/{bucket}/tables/{table}/export",
        json_data={"format": "csv"}
    )

    file_id = result.get("file_id")
    rows_exported = result.get("rows_exported", 0)

    if not file_id:
        if not state.json_output:
            typer.echo("Error: No file_id returned from export", err=True)
        raise typer.Exit(1)

    if not state.json_output:
        typer.echo(f"Exported {rows_exported:,} rows to file {file_id}")

    # Step 2: Download file
    if not state.json_output:
        typer.echo(f"Downloading to {output}...")
    client.download_file(f"/projects/{project}/files/{file_id}/download", output)

    if state.json_output:
        print_json({
            "file_id": file_id,
            "rows_exported": rows_exported,
            "output_file": str(output)
        })
        return

    print_success(f"Exported {rows_exported:,} rows to {output}")


def _format_stat_value(value: any, precision: int = 2) -> str:
    """Format a statistic value for display."""
    if value is None:
        return "-"
    if isinstance(value, float):
        # Format floats with reasonable precision
        if abs(value) >= 1000:
            return f"{value:,.{precision}f}"
        elif abs(value) < 0.01 and value != 0:
            return f"{value:.4f}"
        else:
            return f"{value:.{precision}f}"
    return str(value)


def _format_cardinality(stat: dict) -> str:
    """Format cardinality class with ratio."""
    card_class = stat.get("cardinality_class", "")
    ratio = stat.get("cardinality_ratio", 0)
    if card_class == "unique":
        return "UNIQUE"
    elif card_class == "constant":
        return "CONST"
    elif ratio:
        return f"{card_class.upper()[:3]} ({ratio*100:.0f}%)"
    return card_class.upper()[:6] if card_class else "-"


def _get_quality_label(score: float) -> tuple[str, str]:
    """Get quality label and color based on score."""
    if score >= 90:
        return "Excellent", "green"
    elif score >= 75:
        return "Good", "green"
    elif score >= 50:
        return "Fair", "yellow"
    else:
        return "Poor", "red"


def _render_histogram(histogram: dict, width: int = 30, max_buckets: int = 15) -> list[str]:
    """Render a text-based histogram.

    Handles both:
    - Range format from DuckDB: {"[0, 100)": 50, "[100, 200)": 30}
    - Value format: {100: 50, 200: 30}
    """
    if not histogram:
        return []

    lines = []
    total_count = sum(histogram.values())
    max_count = max(histogram.values()) if histogram else 1

    # Sort by key (try numeric first, then string)
    def sort_key(item):
        key = item[0]
        # Try to extract numeric value from range string like "[0, 100)"
        if isinstance(key, str) and ',' in key:
            try:
                return float(key.split(',')[0].strip('[(] '))
            except ValueError:
                return key
        # Try direct numeric conversion
        try:
            return float(key)
        except (ValueError, TypeError):
            return str(key)

    sorted_items = sorted(histogram.items(), key=sort_key)

    # Limit buckets if too many (show top N by count)
    if len(sorted_items) > max_buckets:
        # For high cardinality, show note and top values by count
        sorted_by_count = sorted(histogram.items(), key=lambda x: x[1], reverse=True)
        sorted_items = sorted_by_count[:max_buckets]
        lines.append(f"      [dim](Showing top {max_buckets} of {len(histogram)} values)[/dim]")

    for bucket, count in sorted_items:
        bar_len = int((count / max_count) * width) if max_count > 0 else 0
        bar = "█" * bar_len
        pct = (count / total_count * 100) if total_count > 0 else 0
        # Format bucket label
        bucket_str = str(bucket)
        if len(bucket_str) > 18:
            bucket_str = bucket_str[:15] + "..."
        lines.append(f"      {bucket_str:>18} {bar} {count:,} ({pct:.1f}%)")

    return lines


@app.command("profile")
def profile_table(
    project: str = typer.Argument(..., help="Project ID"),
    bucket: str = typer.Argument(..., help="Bucket name"),
    table: str = typer.Argument(..., help="Table name"),
    branch: str = typer.Option("default", help="Branch ID"),
    mode: str = typer.Option(
        "basic", "--mode", "-m",
        help="Profile mode: basic, full, distribution, quality"
    ),
    columns_filter: Optional[str] = typer.Option(
        None, "--columns", "-c",
        help="Filter columns (comma-separated)"
    ),
    show_quality: bool = typer.Option(
        False, "--quality", "-q",
        help="Show data quality report"
    ),
    show_distribution: bool = typer.Option(
        False, "--distribution", "-d",
        help="Show distribution details for numeric columns"
    ),
    show_correlations: bool = typer.Option(
        False, "--correlations", "-r",
        help="Show column correlations"
    ),
) -> None:
    """Get advanced statistical profile of a table.

    Shows comprehensive statistics for each column including:
    - Cardinality analysis (unique, high, medium, low, constant)
    - Distribution stats (skewness, kurtosis, outliers)
    - Extended percentiles (q01, q05, q25, q50, q75, q95, q99)
    - Data quality score and recommendations
    - String pattern detection (email, UUID, URL, phone)
    - Column correlations

    Examples:
        # Basic profile
        keboola-duckdb tables profile my-project my-bucket my-table

        # Full analysis with all features
        keboola-duckdb tables profile my-project my-bucket my-table -m full

        # Show quality report
        keboola-duckdb tables profile my-project my-bucket my-table -q

        # Show correlations
        keboola-duckdb tables profile my-project my-bucket my-table -r
    """
    client = get_client()

    # Determine API mode based on flags
    api_mode = mode
    if show_quality or show_correlations:
        api_mode = "quality"
    elif show_distribution:
        api_mode = "distribution"

    # When mode is explicitly set, enable corresponding display options
    if mode == "full":
        show_quality = True
        show_correlations = True
        show_distribution = True
    elif mode == "quality":
        show_quality = True
        show_correlations = True
    elif mode == "distribution":
        show_distribution = True

    result = client.post(
        f"/projects/{project}/branches/{branch}/buckets/{bucket}/tables/{table}/profile",
        params={"mode": api_mode}
    )

    if state.json_output:
        print_json(result)
        return

    table_name = result.get("table_name", table)
    bucket_name = result.get("bucket_name", bucket)
    row_count = result.get("row_count", 0)
    column_count = result.get("column_count", 0)
    statistics = result.get("statistics", [])
    quality_score = result.get("quality_score", 100)
    quality_issues = result.get("quality_issues", [])
    correlations = result.get("correlations", [])

    # Print header with quality score
    quality_label, quality_color = _get_quality_label(quality_score)
    typer.echo(f"\n[bold]Table: {bucket_name}.{table_name}[/bold]")
    typer.echo(f"Rows: {row_count:,} | Columns: {column_count} | Quality: [{quality_color}]{quality_score:.0f}% ({quality_label})[/{quality_color}]")
    typer.echo()

    if not statistics:
        typer.echo("No statistics available")
        return

    # Filter columns if specified
    if columns_filter:
        filter_cols = {c.strip().lower() for c in columns_filter.split(",")}
        statistics = [s for s in statistics if s["column_name"].lower() in filter_cols]

    if not statistics:
        typer.echo("No matching columns found")
        return

    # Build main table with advanced stats
    headers = ["Column", "Type", "Cardinality", "Nulls%", "Avg", "Std", "Skew", "Outliers", "Alerts"]
    rows = []

    for stat in statistics:
        col_name = stat.get("column_name", "")
        col_type = stat.get("column_type", "")
        # Shorten common type names
        if col_type.startswith("VARCHAR"):
            col_type = "VARCHAR"

        # Determine alerts for this column
        alerts = []
        for issue in quality_issues:
            if issue.get("column") == col_name:
                issue_type = issue.get("type", "")
                if issue_type == "pk_candidate":
                    alerts.append("PK?")
                elif issue_type == "enum_candidate":
                    alerts.append("ENUM?")
                elif issue_type == "skewed":
                    alerts.append("SKEW")
                elif issue_type == "outliers":
                    alerts.append("OUT")
                elif issue_type == "constant":
                    alerts.append("CONST")

        outlier_count = stat.get("outlier_count")
        outlier_str = f"{outlier_count:,}" if outlier_count is not None else "-"

        rows.append([
            col_name,
            col_type,
            _format_cardinality(stat),
            f"{stat.get('null_percentage', 0):.1f}%",
            _format_stat_value(stat.get("avg")),
            _format_stat_value(stat.get("std")),
            _format_stat_value(stat.get("skewness")),
            outlier_str,
            " ".join(alerts) if alerts else "",
        ])

    print_table(headers, rows)

    # Show quality issues if requested or if there are warnings
    warnings = [i for i in quality_issues if i.get("severity") == "warning"]
    infos = [i for i in quality_issues if i.get("severity") == "info"]

    if show_quality or warnings:
        if warnings:
            typer.echo("\n[yellow]Warnings:[/yellow]")
            for issue in warnings:
                typer.echo(f"  ! {issue['column']}: {issue['message']}")

        if show_quality and infos:
            typer.echo("\n[dim]Recommendations:[/dim]")
            for issue in infos:
                typer.echo(f"  ? {issue['column']}: {issue['message']}")

    # Show correlations if requested
    if show_correlations and correlations:
        typer.echo("\n[bold]Column Correlations:[/bold]")
        strong = [c for c in correlations if c.get("strength") == "strong"]
        moderate = [c for c in correlations if c.get("strength") == "moderate"]

        if strong:
            typer.echo("\n[cyan]Strong (|r| > 0.7):[/cyan]")
            for corr in strong:
                bar_len = int(abs(corr["correlation"]) * 20)
                bar = "█" * bar_len
                sign = "+" if corr["correlation"] > 0 else "-"
                typer.echo(f"  {corr['column1']} <-> {corr['column2']}: {sign}{abs(corr['correlation']):.2f} {bar}")

        if moderate:
            typer.echo("\n[dim]Moderate (0.3 < |r| < 0.7):[/dim]")
            for corr in moderate[:5]:  # Show top 5
                sign = "+" if corr["correlation"] > 0 else "-"
                typer.echo(f"  {corr['column1']} <-> {corr['column2']}: {sign}{abs(corr['correlation']):.2f}")

    # Show distribution details if requested
    if show_distribution:
        typer.echo("\n[bold]Distribution Details:[/bold]")
        for stat in statistics:
            if stat.get("q01") is not None:  # Numeric column
                typer.echo(f"\n  [cyan]{stat['column_name']}[/cyan] ({stat['column_type']})")
                typer.echo(f"    Range: {_format_stat_value(stat.get('min'))} - {_format_stat_value(stat.get('max'))}")
                typer.echo(f"    Mean: {_format_stat_value(stat.get('avg'))} | Std: {_format_stat_value(stat.get('std'))}")
                typer.echo(f"    Skewness: {_format_stat_value(stat.get('skewness'))} | Kurtosis: {_format_stat_value(stat.get('kurtosis'))}")
                typer.echo(f"    Percentiles:")
                typer.echo(f"      Q01: {_format_stat_value(stat.get('q01'))} | Q05: {_format_stat_value(stat.get('q05'))}")
                typer.echo(f"      Q25: {_format_stat_value(stat.get('q25'))} | Q50: {_format_stat_value(stat.get('q50'))} | Q75: {_format_stat_value(stat.get('q75'))}")
                typer.echo(f"      Q95: {_format_stat_value(stat.get('q95'))} | Q99: {_format_stat_value(stat.get('q99'))}")

                if stat.get("outlier_count"):
                    typer.echo(f"    Outliers: {stat['outlier_count']} (bounds: {_format_stat_value(stat.get('outlier_lower_bound'))} - {_format_stat_value(stat.get('outlier_upper_bound'))})")

                # Show histogram if available
                histogram = stat.get("histogram")
                if histogram:
                    typer.echo(f"    Histogram:")
                    for line in _render_histogram(histogram, width=30):
                        typer.echo(line)

    # Show pattern detection for strings
    patterns_found = False
    for stat in statistics:
        if stat.get("detected_patterns"):
            if not patterns_found:
                typer.echo("\n[bold]Detected Patterns:[/bold]")
                patterns_found = True
            for pattern in stat["detected_patterns"]:
                typer.echo(f"  {stat['column_name']}: {pattern['pattern']} ({pattern['match_percentage']:.1f}%)")

    typer.echo("\n[dim]Tip: Use --json for complete data, -q for quality report, -r for correlations, -d for distributions[/dim]")
