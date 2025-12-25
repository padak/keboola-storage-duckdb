"""File management commands."""

from pathlib import Path

import typer

from ..client import get_client
from ..output import print_table, print_json, print_success, print_error, format_bytes
from ..main import state


app = typer.Typer(
    name="files",
    help="Manage files in a project",
    no_args_is_help=True,
)


@app.command("list")
def list_files(
    project: str = typer.Argument(..., help="Project ID"),
    limit: int = typer.Option(100, "--limit", "-l", help="Maximum number of files to return"),
) -> None:
    """List files in a project."""
    client = get_client(verbose=state.verbose)

    try:
        response = client.get(f"/projects/{project}/files", params={"limit": limit})
        files = response.get("files", [])
        total = response.get("total", len(files))

        if state.json_output:
            print_json({"files": files, "total": total})
        else:
            if not files:
                print(f"No files found in project '{project}'")
                return

            # Format data for table display
            table_data = []
            for file in files:
                table_data.append({
                    "ID": file.get("id", ""),
                    "Name": file.get("name", ""),
                    "Size": format_bytes(file.get("size_bytes", 0)),
                    "Type": file.get("content_type", ""),
                    "Staged": "Yes" if file.get("is_staged") else "No",
                    "Created": file.get("created_at", "")[:19] if file.get("created_at") else "",  # Truncate timestamp
                })

            print_table(
                table_data,
                columns=["ID", "Name", "Size", "Type", "Staged", "Created"],
                title=f"Files in {project} (Total: {total}, Showing: {len(files)})"
            )
    finally:
        client.close()


@app.command("upload")
def upload_file(
    project: str = typer.Argument(..., help="Project ID"),
    file: Path = typer.Argument(..., help="Path to file to upload", exists=True, file_okay=True, dir_okay=False),
) -> None:
    """Upload a file to a project.

    Uses the 3-stage upload workflow:
    1. Prepare upload (get upload key)
    2. Upload file content
    3. Register file in project
    """
    client = get_client(verbose=state.verbose)

    try:
        # Validate file exists and is readable
        if not file.exists():
            print_error(f"File not found: {file}")
            raise typer.Exit(1)

        if not file.is_file():
            print_error(f"Not a file: {file}")
            raise typer.Exit(1)

        # Use the 3-stage upload method from client
        file_info = client.upload_file_3stage(
            project_id=project,
            file_path=file,
            show_progress=not state.json_output
        )

        if state.json_output:
            print_json(file_info)
        else:
            file_id = file_info.get("id", "")
            file_name = file_info.get("name", "")
            file_size = file_info.get("size_bytes", 0)
            print_success(
                f"File uploaded successfully: {file_name} ({format_bytes(file_size)})\n"
                f"File ID: {file_id}"
            )
    finally:
        client.close()


@app.command("download")
def download_file(
    project: str = typer.Argument(..., help="Project ID"),
    file_id: str = typer.Argument(..., help="File ID to download"),
    output: Path = typer.Argument(..., help="Output path for downloaded file"),
) -> None:
    """Download a file from a project."""
    client = get_client(verbose=state.verbose)

    try:
        # Check if output path exists and is a directory
        if output.exists() and output.is_dir():
            print_error(f"Output path is a directory: {output}")
            raise typer.Exit(1)

        # Check if output file already exists
        if output.exists():
            if not typer.confirm(f"File {output} already exists. Overwrite?"):
                print("Download cancelled")
                raise typer.Exit(0)

        # Download the file
        client.download_file(
            path=f"/projects/{project}/files/{file_id}/download",
            output_path=output,
            show_progress=not state.json_output
        )

        # Get file size for success message
        file_size = output.stat().st_size

        if state.json_output:
            print_json({"path": str(output), "size_bytes": file_size})
        else:
            print_success(
                f"File downloaded successfully to: {output}\n"
                f"Size: {format_bytes(file_size)}"
            )
    finally:
        client.close()


@app.command("delete")
def delete_file(
    project: str = typer.Argument(..., help="Project ID"),
    file_id: str = typer.Argument(..., help="File ID to delete"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Delete a file from a project."""
    client = get_client(verbose=state.verbose)

    try:
        # Confirm deletion unless --yes flag is provided
        if not yes and not state.json_output:
            if not typer.confirm(f"Are you sure you want to delete file '{file_id}'?"):
                print("Deletion cancelled")
                raise typer.Exit(0)

        # Delete the file
        client.delete(f"/projects/{project}/files/{file_id}")

        if state.json_output:
            print_json({"file_id": file_id, "deleted": True})
        else:
            print_success(f"File '{file_id}' deleted successfully")
    finally:
        client.close()
