"""Bucket management commands."""

import typer

from ..client import get_client
from ..output import print_table, print_json, print_dict, print_success
from ..main import state


app = typer.Typer(
    name="buckets",
    help="Manage buckets in a project",
    no_args_is_help=True,
)


@app.command("list")
def list_buckets(
    project: str = typer.Argument(..., help="Project ID"),
    branch: str = typer.Option("default", "--branch", "-b", help="Branch ID"),
) -> None:
    """List buckets in a project."""
    client = get_client(verbose=state.verbose)

    try:
        response = client.get(f"/projects/{project}/branches/{branch}/buckets")
        buckets = response.get("buckets", [])
        total = response.get("total", len(buckets))

        if state.json_output:
            print_json({"buckets": buckets, "total": total})
        else:
            if not buckets:
                print(f"No buckets found in project '{project}' (branch: {branch})")
                return

            # Format data for table display
            table_data = []
            for bucket in buckets:
                # Handle linked buckets
                linked_info = ""
                if bucket.get("is_linked"):
                    source_proj = bucket.get("source_project_id", "?")
                    source_bucket = bucket.get("source_bucket_name", "?")
                    linked_info = f"{source_proj}/{source_bucket}"

                description = bucket.get("description") or ""
                table_data.append({
                    "Name": bucket.get("name", ""),
                    "Tables": bucket.get("table_count", 0),
                    "Description": description[:40],  # Truncate long descriptions
                    "Linked": "Yes" if bucket.get("is_linked") else "No",
                    "Source": linked_info,
                })

            print_table(
                table_data,
                columns=["Name", "Tables", "Description", "Linked", "Source"],
                title=f"Buckets in {project}/{branch} (Total: {total})"
            )
    finally:
        client.close()


@app.command("create")
def create_bucket(
    project: str = typer.Argument(..., help="Project ID"),
    name: str = typer.Argument(..., help="Bucket name (e.g., in.c-sales)"),
    description: str = typer.Option("", "--description", "-d", help="Bucket description"),
    branch: str = typer.Option("default", "--branch", "-b", help="Branch ID"),
) -> None:
    """Create a new bucket."""
    client = get_client(verbose=state.verbose)

    try:
        payload = {"name": name}
        if description:
            payload["description"] = description

        response = client.post(
            f"/projects/{project}/branches/{branch}/buckets",
            payload
        )

        if state.json_output:
            print_json(response)
        else:
            print_success(f"Bucket '{name}' created successfully")
            print_dict({
                "Name": response.get("name", name),
                "Description": response.get("description") or "",
                "Project": project,
                "Branch": branch,
            })
    finally:
        client.close()
