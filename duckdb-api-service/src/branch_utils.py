"""Branch utilities for ADR-012: Branch-First API.

Shared functions for resolving branch context in all routers.

Branch Resolution:
- branch_id = "default" -> operations on main (production) project
- branch_id = {uuid} -> operations on dev branch with CoW semantics
"""

from typing import Literal

from fastapi import HTTPException, status

from src.database import metadata_db, project_db_manager


def resolve_branch(project_id: str, branch_id: str) -> tuple[str, str | None]:
    """
    Resolve branch_id to actual project/branch identifiers.

    Args:
        project_id: The project ID from URL path
        branch_id: The branch ID from URL path ("default" or UUID)

    Returns:
        Tuple of (resolved_project_id, resolved_branch_id)
        - For "default": (project_id, None) - operations on main
        - For branch UUID: (project_id, branch_id) - operations on branch

    Raises:
        HTTPException 404 if project or branch not found
    """
    # Validate project exists
    project = metadata_db.get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )

    if branch_id == "default":
        # Main branch - operate on project directly
        return project_id, None

    # Dev branch - validate it exists
    branch = metadata_db.get_branch_by_project(project_id, branch_id)
    if not branch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "branch_not_found",
                "message": f"Branch {branch_id} not found in project {project_id}",
                "details": {"project_id": project_id, "branch_id": branch_id},
            },
        )

    return project_id, branch_id


def validate_project_db_exists(project_id: str) -> None:
    """
    Validate that project database directory exists.

    Args:
        project_id: The project ID

    Raises:
        HTTPException 404 if project DB not found
    """
    if not project_db_manager.project_exists(project_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_db_not_found",
                "message": f"Database file for project {project_id} not found",
                "details": {"project_id": project_id},
            },
        )


def validate_bucket_exists(project_id: str, bucket_name: str) -> None:
    """
    Validate that bucket exists in project.

    Note: Buckets are always defined in main project (branches share buckets).

    Args:
        project_id: The project ID
        bucket_name: The bucket name

    Raises:
        HTTPException 404 if bucket not found
    """
    if not project_db_manager.bucket_exists(project_id, bucket_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "bucket_not_found",
                "message": f"Bucket {bucket_name} not found in project {project_id}",
                "details": {"project_id": project_id, "bucket_name": bucket_name},
            },
        )


def validate_project_and_bucket(
    project_id: str, branch_id: str | None, bucket_name: str
) -> None:
    """
    Validate that project DB exists and bucket exists.

    For both main and branches, buckets are defined in main project
    (branches don't create new buckets, they only copy tables).

    Args:
        project_id: The project ID
        branch_id: The resolved branch ID (None for main)
        bucket_name: The bucket name

    Raises:
        HTTPException 404 if project DB or bucket not found
    """
    validate_project_db_exists(project_id)
    validate_bucket_exists(project_id, bucket_name)


def get_table_source(
    project_id: str, branch_id: str | None, bucket_name: str, table_name: str
) -> Literal["main", "branch"]:
    """
    Determine the source of a table in branch context.

    Args:
        project_id: The project ID
        branch_id: The resolved branch ID (None for main)
        bucket_name: The bucket name
        table_name: The table name

    Returns:
        "main" - table is from main (not copied to branch or is default branch)
        "branch" - table has been copied to branch (CoW)
    """
    if branch_id is None:
        # Default branch - always main
        return "main"

    # Check if table is in branch
    is_in_branch = metadata_db.is_table_in_branch(branch_id, bucket_name, table_name)
    return "branch" if is_in_branch else "main"


def require_default_branch(branch_id: str | None, operation: str) -> None:
    """
    Require that operation is performed on default branch.

    Some operations (like bucket deletion) can only be done on main.

    Args:
        branch_id: The resolved branch ID (None for main)
        operation: Description of the operation (for error message)

    Raises:
        HTTPException 400 if not on default branch
    """
    if branch_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "operation_not_allowed",
                "message": f"Cannot {operation} from dev branches. Use default branch.",
                "details": {"branch_id": branch_id},
            },
        )
