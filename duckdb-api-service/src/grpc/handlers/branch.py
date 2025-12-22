"""Dev branch command handlers for gRPC service.

Phase 12g: Dev branch gRPC handlers
- CreateDevBranchCommand
- DropDevBranchCommand
"""

import sys
import uuid
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import project_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import MetadataDB, ProjectDBManager
from src import metrics


class CreateDevBranchHandler(BaseCommandHandler):
    """
    Create a new dev branch in a project.

    ADR-007 behavior:
    - Branch starts empty (no data copied)
    - Reads from branch return main data (live view)
    - First write to a table triggers Copy-on-Write
    - Branch tables are isolated after CoW
    """

    def __init__(self, metadata_db: MetadataDB, project_manager: ProjectDBManager):
        super().__init__()
        self.metadata_db = metadata_db
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> project_pb2.CreateDevBranchResponse:
        cmd = project_pb2.CreateDevBranchCommand()
        command.Unpack(cmd)

        project_id = cmd.projectId
        branch_id = cmd.branchId
        project_role_name = cmd.projectRoleName
        project_readonly_role = cmd.projectReadOnlyRoleName

        if not project_id:
            raise ValueError("projectId is required")
        if not branch_id:
            # Generate branch ID if not provided
            branch_id = str(uuid.uuid4())[:8]

        # Verify project exists
        project = self.metadata_db.get_project(project_id)
        if not project:
            raise KeyError(f"Project {project_id} not found")

        # Create branch directory
        self.project_manager.create_branch_db(project_id, branch_id)

        # Create branch metadata record
        branch = self.metadata_db.create_branch(
            branch_id=branch_id,
            project_id=project_id,
            name=f"Branch {branch_id}",  # Default name
            description=None,
        )

        # Update metrics
        metrics.BRANCHES_TOTAL.set(self.metadata_db.count_branches())

        # Generate a readonly role name for the branch (for proto compatibility)
        dev_branch_readonly_role = f"branch_{project_id}_{branch_id}_readonly"

        self.log_info(f"Dev branch {branch_id} created in project {project_id}")

        response = project_pb2.CreateDevBranchResponse()
        response.devBranchReadOnlyRoleName = dev_branch_readonly_role

        return response


class DropDevBranchHandler(BaseCommandHandler):
    """
    Drop (delete) a dev branch from a project.

    This:
    1. Deletes all branch tables
    2. Removes branch directory
    3. Removes branch metadata
    """

    def __init__(self, metadata_db: MetadataDB, project_manager: ProjectDBManager):
        super().__init__()
        self.metadata_db = metadata_db
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = project_pb2.DropDevBranchCommand()
        command.Unpack(cmd)

        dev_branch_readonly_role = cmd.devBranchReadOnlyRoleName

        if not dev_branch_readonly_role:
            raise ValueError("devBranchReadOnlyRoleName is required")

        # Parse role name to extract project_id and branch_id
        # Format: branch_{project_id}_{branch_id}_readonly
        parts = dev_branch_readonly_role.split("_")
        if len(parts) < 4 or parts[0] != "branch":
            raise ValueError(f"Invalid devBranchReadOnlyRoleName format: {dev_branch_readonly_role}")

        # Extract project_id and branch_id
        # Handle case where project_id might contain underscores
        # Format: branch_<project_id>_<branch_id>_readonly
        # We know the last part is "readonly" and before it is branch_id (8 chars)
        if parts[-1] != "readonly":
            raise ValueError(f"Invalid devBranchReadOnlyRoleName format: {dev_branch_readonly_role}")

        branch_id = parts[-2]
        project_id = "_".join(parts[1:-2])

        # Verify branch exists
        branch = self.metadata_db.get_branch_by_project(project_id, branch_id)
        if not branch:
            self.log_warning(f"Branch {branch_id} not found in project {project_id}")
            # Idempotent - return success even if not found
            return None

        # Delete branch directory and all tables
        self.project_manager.delete_branch_db(project_id, branch_id)

        # Delete branch metadata
        self.metadata_db.delete_branch(branch_id)

        # Update metrics
        metrics.BRANCHES_TOTAL.set(self.metadata_db.count_branches())

        self.log_info(f"Dev branch {branch_id} dropped from project {project_id}")

        return None
