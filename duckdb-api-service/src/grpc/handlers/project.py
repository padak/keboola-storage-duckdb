"""Project command handlers."""

import uuid
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import project_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import MetadataDB, ProjectDBManager
from src.auth import generate_api_key, hash_key, get_key_prefix


class CreateProjectHandler(BaseCommandHandler):
    """
    Create a new project with DuckDB storage.

    This handler:
    1. Registers the project in metadata database
    2. Creates the project directory structure
    3. Generates a project admin API key
    4. Returns credentials for accessing the project
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
    ) -> project_pb2.CreateProjectResponse:
        cmd = project_pb2.CreateProjectCommand()
        command.Unpack(cmd)

        project_id = cmd.projectId

        # Check if project already exists
        existing = self.metadata_db.get_project(project_id)
        if existing:
            raise ValueError(f"Project {project_id} already exists")

        # 1. Register project in metadata DB
        self.metadata_db.create_project(
            project_id=project_id,
            name=f"Project {project_id}",  # Default name
            settings_json=None,
        )
        self.log_info(f"Project {project_id} registered in metadata")

        # 2. Create project directory structure (ADR-009)
        self.project_manager.create_project_db(project_id)
        self.log_info(f"Project {project_id} directory created")

        # 3. Generate API key for this project
        api_key = generate_api_key(project_id)
        key_id = str(uuid.uuid4())
        key_hash = hash_key(api_key)
        key_prefix = get_key_prefix(api_key)

        # Store key in database
        self.metadata_db.create_api_key(
            key_id=key_id,
            project_id=project_id,
            key_hash=key_hash,
            key_prefix=key_prefix,
            description="Project admin key (created via gRPC)",
        )
        self.log_info(f"API key created for project {project_id}")

        # 4. Build response
        response = project_pb2.CreateProjectResponse()
        response.projectPassword = api_key  # API key is returned as "password"
        response.projectDatabaseName = project_id
        response.projectUserName = f"project_{project_id}"
        # projectRoleName and projectReadOnlyRoleName are not used for DuckDB

        return response


class DropProjectHandler(BaseCommandHandler):
    """
    Drop (delete) a project.

    This handler:
    1. Marks the project as deleted in metadata (soft delete)
    2. Optionally deletes the project directory

    Note: By default, we do a soft delete to allow recovery.
    The actual data directory can be cleaned up separately.
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
    ):
        cmd = project_pb2.DropProjectCommand()
        command.Unpack(cmd)

        # projectDatabaseName contains the project_id
        project_id = cmd.projectDatabaseName

        if not project_id:
            raise ValueError("projectDatabaseName (project_id) is required")

        # Check if project exists
        existing = self.metadata_db.get_project(project_id)
        if not existing:
            raise KeyError(f"Project {project_id} not found")

        # 1. Clean up API keys first (foreign key constraint)
        deleted_keys = self.metadata_db.delete_project_api_keys(project_id)
        if deleted_keys > 0:
            self.log_info(f"Deleted {deleted_keys} API keys for project {project_id}")

        # 2. Delete project data (ADR-009: deletes entire project directory)
        if self.project_manager.project_exists(project_id):
            self.project_manager.delete_project_db(project_id)
            self.log_info(f"Project {project_id} data directory deleted")

        # 3. Soft delete in metadata (marks as deleted, preserves for audit)
        self.metadata_db.delete_project(project_id)
        self.log_info(f"Project {project_id} marked as deleted")

        # DropProjectCommand has no response message defined
        return None
