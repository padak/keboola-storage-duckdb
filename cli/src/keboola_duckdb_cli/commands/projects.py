"""Project management commands."""

import typer

from ..client import get_client
from ..output import print_table, print_json, format_bytes
from ..main import state


app = typer.Typer(
    name="projects",
    help="Manage DuckDB projects",
    no_args_is_help=True,
)


@app.command("list")
def list_projects() -> None:
    """List all projects (requires admin API key)."""
    client = get_client(verbose=state.verbose)

    try:
        response = client.get("/projects")
        projects = response.get("projects", [])
        total = response.get("total", len(projects))

        if state.json_output:
            print_json({"projects": projects, "total": total})
        else:
            if not projects:
                print("No projects found")
                return

            # Format data for table display
            table_data = []
            for project in projects:
                table_data.append({
                    "ID": project.get("id", ""),
                    "Name": project.get("name", ""),
                    "Status": project.get("status", ""),
                    "Size": format_bytes(project.get("size_bytes", 0)),
                    "Tables": project.get("table_count", 0),
                    "Buckets": project.get("bucket_count", 0),
                    "Created": project.get("created_at", "")[:19] if project.get("created_at") else "",
                })

            print_table(
                table_data,
                columns=["ID", "Name", "Status", "Size", "Tables", "Buckets", "Created"],
                title=f"Projects (Total: {total})"
            )
    finally:
        client.close()
