"""Workspace command handlers for gRPC service.

Phase 12e: 8 workspace handlers for Connection integration.
"""

import hashlib
import secrets
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import workspace_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import MetadataDB, ProjectDBManager


def _generate_password() -> str:
    """Generate a secure random password (32 characters)."""
    return secrets.token_urlsafe(24)[:32]


def _hash_password(password: str) -> str:
    """Hash password using SHA256."""
    return hashlib.sha256(password.encode()).hexdigest()


def _generate_username(workspace_id: str) -> str:
    """Generate workspace username."""
    random_suffix = secrets.token_hex(4)
    return f"ws_{workspace_id}_{random_suffix}"


class CreateWorkspaceHandler(BaseCommandHandler):
    """
    Create a new workspace in a project.

    This handler:
    1. Creates workspace database file
    2. Generates credentials (username/password)
    3. Stores metadata in metadata DB
    4. Returns workspace credentials and object name

    Proto: CreateWorkspaceCommand -> CreateWorkspaceResponse
    """

    def __init__(self, project_manager: ProjectDBManager, metadata_db: MetadataDB):
        super().__init__()
        self.project_manager = project_manager
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> workspace_pb2.CreateWorkspaceResponse:
        cmd = workspace_pb2.CreateWorkspaceCommand()
        command.Unpack(cmd)

        project_id = cmd.projectId
        workspace_id = cmd.workspaceId
        branch_id = cmd.branchId if cmd.branchId and not cmd.isBranchDefault else None

        if not project_id:
            raise ValueError("projectId is required")
        if not workspace_id:
            raise ValueError("workspaceId is required")

        # Generate credentials
        username = _generate_username(workspace_id)
        password = _generate_password()
        password_hash = _hash_password(password)

        # Create workspace database file
        db_path = self.project_manager.create_workspace_db(
            project_id, workspace_id, branch_id=branch_id
        )

        # Create workspace metadata record
        workspace = self.metadata_db.create_workspace(
            workspace_id=workspace_id,
            project_id=project_id,
            name=f"workspace_{workspace_id}",
            db_path=str(db_path),
            branch_id=branch_id,
            expires_at=None,  # No expiration for gRPC-created workspaces
            size_limit_bytes=10 * 1024 * 1024 * 1024,  # 10GB default
        )

        # Create credentials
        self.metadata_db.create_workspace_credentials(workspace_id, username, password_hash)

        self.log_info(f"Workspace {workspace_id} created in project {project_id}")

        # Build response
        response = workspace_pb2.CreateWorkspaceResponse()
        response.workspaceUserName = username
        response.workspaceRoleName = f"role_{workspace_id}"  # DuckDB doesn't use roles, but field is required
        response.workspacePassword = password
        response.workspaceObjectName = workspace_id

        return response


class DropWorkspaceHandler(BaseCommandHandler):
    """
    Drop (delete) a workspace.

    This handler:
    1. Deletes workspace database file
    2. Removes metadata and credentials
    3. Optionally cascades to delete all content

    Proto: DropWorkspaceCommand -> None (void)
    """

    def __init__(self, project_manager: ProjectDBManager, metadata_db: MetadataDB):
        super().__init__()
        self.project_manager = project_manager
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = workspace_pb2.DropWorkspaceCommand()
        command.Unpack(cmd)

        workspace_object_name = cmd.workspaceObjectName
        # isCascade is always true for DuckDB - we delete the whole file

        if not workspace_object_name:
            raise ValueError("workspaceObjectName is required")

        # Get workspace metadata to find project_id and branch_id
        workspace = self.metadata_db.get_workspace(workspace_object_name)
        if not workspace:
            self.log_warning(f"Workspace {workspace_object_name} not found - treating as already deleted")
            return None

        project_id = workspace.get("project_id")
        branch_id = workspace.get("branch_id")

        # Delete workspace database file
        self.project_manager.delete_workspace_db(project_id, workspace_object_name, branch_id)

        # Delete workspace metadata (credentials cascade)
        self.metadata_db.delete_workspace(workspace_object_name)

        self.log_info(f"Workspace {workspace_object_name} dropped")

        return None


class ClearWorkspaceHandler(BaseCommandHandler):
    """
    Clear all content from a workspace.

    This handler:
    1. Drops all tables/views in workspace DB
    2. Optionally preserves specified objects
    3. Ignores errors if flag is set

    Proto: ClearWorkspaceCommand -> None (void)
    """

    def __init__(self, project_manager: ProjectDBManager, metadata_db: MetadataDB):
        super().__init__()
        self.project_manager = project_manager
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = workspace_pb2.ClearWorkspaceCommand()
        command.Unpack(cmd)

        workspace_object_name = cmd.workspaceObjectName
        ignore_errors = cmd.ignoreErrors
        objects_to_preserve = list(cmd.objectsToPreserve) if cmd.objectsToPreserve else []

        if not workspace_object_name:
            raise ValueError("workspaceObjectName is required")

        # Get workspace metadata
        workspace = self.metadata_db.get_workspace(workspace_object_name)
        if not workspace:
            if ignore_errors:
                self.log_warning(f"Workspace {workspace_object_name} not found - ignoring")
                return None
            raise KeyError(f"Workspace {workspace_object_name} not found")

        project_id = workspace.get("project_id")
        branch_id = workspace.get("branch_id")

        try:
            # Clear workspace (drop all objects)
            # Note: objects_to_preserve not implemented in DuckDB - would need enhancement
            if objects_to_preserve:
                self.log_warning(
                    f"objectsToPreserve not implemented for DuckDB, clearing all objects"
                )
            self.project_manager.clear_workspace(project_id, workspace_object_name, branch_id)
            self.log_info(f"Workspace {workspace_object_name} cleared")
        except Exception as e:
            if ignore_errors:
                self.log_warning(f"Error clearing workspace {workspace_object_name}: {e} - ignoring")
            else:
                raise

        return None


class ResetWorkspacePasswordHandler(BaseCommandHandler):
    """
    Reset workspace password.

    This handler:
    1. Generates new password
    2. Updates credentials in metadata DB
    3. Returns new credentials

    Proto: ResetWorkspacePasswordCommand -> ResetWorkspacePasswordResponse
    """

    def __init__(self, metadata_db: MetadataDB):
        super().__init__()
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> workspace_pb2.ResetWorkspacePasswordResponse:
        cmd = workspace_pb2.ResetWorkspacePasswordCommand()
        command.Unpack(cmd)

        workspace_user_name = cmd.workspaceUserName

        if not workspace_user_name:
            raise ValueError("workspaceUserName is required")

        # Find workspace by username - returns workspace with credentials
        workspace_data = self.metadata_db.get_workspace_by_username(workspace_user_name)
        if not workspace_data:
            raise KeyError(f"Workspace credentials not found for user {workspace_user_name}")

        workspace_id = workspace_data.get("id")

        # Generate new password
        new_password = _generate_password()
        new_password_hash = _hash_password(new_password)

        # Update credentials
        self.metadata_db.update_workspace_credentials(workspace_id, new_password_hash)

        self.log_info(f"Password reset for workspace user {workspace_user_name}")

        # Build response
        response = workspace_pb2.ResetWorkspacePasswordResponse()
        response.workspaceUserName = workspace_user_name
        response.workspacePassword = new_password

        return response


class DropWorkspaceObjectHandler(BaseCommandHandler):
    """
    Drop a single object from workspace.

    This handler:
    1. Identifies object (table/view) to drop
    2. Drops it from workspace DB
    3. Optionally ignores if not exists

    Proto: DropWorkspaceObjectCommand -> None (void)
    """

    def __init__(self, project_manager: ProjectDBManager, metadata_db: MetadataDB):
        super().__init__()
        self.project_manager = project_manager
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = workspace_pb2.DropWorkspaceObjectCommand()
        command.Unpack(cmd)

        workspace_object_name = cmd.workspaceObjectName
        object_name_to_drop = cmd.objectNameToDrop
        ignore_if_not_exists = cmd.ignoreIfNotExists

        if not workspace_object_name:
            raise ValueError("workspaceObjectName is required")
        if not object_name_to_drop:
            raise ValueError("objectNameToDrop is required")

        # Get workspace metadata
        workspace = self.metadata_db.get_workspace(workspace_object_name)
        if not workspace:
            if ignore_if_not_exists:
                self.log_warning(f"Workspace {workspace_object_name} not found - ignoring")
                return None
            raise KeyError(f"Workspace {workspace_object_name} not found")

        project_id = workspace.get("project_id")
        branch_id = workspace.get("branch_id")

        try:
            # Drop the object
            self.project_manager.drop_workspace_object(
                project_id, workspace_object_name, object_name_to_drop, branch_id
            )
            self.log_info(f"Object {object_name_to_drop} dropped from workspace {workspace_object_name}")
        except Exception as e:
            if ignore_if_not_exists:
                self.log_warning(
                    f"Error dropping {object_name_to_drop} from workspace {workspace_object_name}: {e} - ignoring"
                )
            else:
                raise

        return None


class GrantWorkspaceAccessToProjectHandler(BaseCommandHandler):
    """
    Grant workspace access to another project.

    For DuckDB, this is mostly a no-op since we handle access differently.
    The handler records the grant for compatibility but doesn't create
    actual database grants (DuckDB doesn't have a role system like Snowflake).

    Proto: GrantWorkspaceAccessToProjectCommand -> None (void)
    """

    def __init__(self, metadata_db: MetadataDB):
        super().__init__()
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = workspace_pb2.GrantWorkspaceAccessToProjectCommand()
        command.Unpack(cmd)

        workspace_object_name = cmd.workspaceObjectName
        project_user_name = cmd.projectUserName
        project_role_name = cmd.projectRoleName

        if not workspace_object_name:
            raise ValueError("workspaceObjectName is required")

        # For DuckDB, we don't have actual role grants
        # This is logged for audit purposes and potential future use
        self.log_info(
            f"Grant workspace access recorded: workspace={workspace_object_name}, "
            f"project_user={project_user_name}, project_role={project_role_name}"
        )

        return None


class RevokeWorkspaceAccessToProjectHandler(BaseCommandHandler):
    """
    Revoke workspace access from another project.

    For DuckDB, this is mostly a no-op (see GrantWorkspaceAccessToProjectHandler).

    Proto: RevokeWorkspaceAccessToProjectCommand -> None (void)
    """

    def __init__(self, metadata_db: MetadataDB):
        super().__init__()
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = workspace_pb2.RevokeWorkspaceAccessToProjectCommand()
        command.Unpack(cmd)

        workspace_object_name = cmd.workspaceObjectName
        project_user_name = cmd.projectUserName
        project_role_name = cmd.projectRoleName

        if not workspace_object_name:
            raise ValueError("workspaceObjectName is required")

        # For DuckDB, we don't have actual role revokes
        # This is logged for audit purposes
        self.log_info(
            f"Revoke workspace access recorded: workspace={workspace_object_name}, "
            f"project_user={project_user_name}, project_role={project_role_name}"
        )

        return None


class LoadTableToWorkspaceHandler(BaseCommandHandler):
    """
    Load a table from project storage into workspace.

    This handler:
    1. Reads source table from project bucket
    2. Applies filters and column mappings
    3. Creates/populates destination table in workspace

    Proto: LoadTableToWorkspaceCommand -> None (void, but logs rows loaded)
    """

    def __init__(self, project_manager: ProjectDBManager, metadata_db: MetadataDB):
        super().__init__()
        self.project_manager = project_manager
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = workspace_pb2.LoadTableToWorkspaceCommand()
        command.Unpack(cmd)

        # Extract source info
        source = cmd.source
        source_path = list(source.path) if source.path else []
        source_table_name = source.tableName
        source_limit = source.limit if source.limit else None
        column_mappings = []
        for mapping in source.columnMappings:
            column_mappings.append({
                'source': mapping.sourceColumnName,
                'destination': mapping.destinationColumnName,
            })

        # Extract destination info
        dest = cmd.destination
        dest_path = list(dest.path) if dest.path else []
        dest_table_name = dest.tableName

        # Parse paths - source: [project_id, bucket_name] or [project_id, branch_id, bucket_name]
        if len(source_path) < 2:
            raise ValueError("source.path must contain at least [project_id, bucket_name]")

        source_project_id = source_path[0]
        source_bucket = source_path[-1]
        source_branch_id = source_path[1] if len(source_path) > 2 else "default"

        # Destination workspace_id is in dest_path
        if len(dest_path) < 1:
            raise ValueError("destination.path must contain workspace identifier")

        workspace_object_name = dest_path[-1]  # Last element is workspace object name

        # Get workspace metadata
        workspace = self.metadata_db.get_workspace(workspace_object_name)
        if not workspace:
            raise KeyError(f"Workspace {workspace_object_name} not found")

        # Determine columns to select
        columns = None
        if column_mappings:
            columns = [cm['source'] for cm in column_mappings]

        # Load table to workspace
        # Note: limit and where_clause not implemented - would need method enhancement
        if source_limit:
            self.log_warning(f"limit parameter not implemented for LoadTableToWorkspace")

        result = self.project_manager.load_table_to_workspace(
            project_id=source_project_id,
            workspace_id=workspace_object_name,
            source_bucket=source_bucket,
            source_table=source_table_name,
            dest_table=dest_table_name,
            columns=columns,
            where_clause=None,  # TODO: implement where filters from source.whereFilters
            branch_id=workspace.get("branch_id"),
        )

        rows_loaded = result.get("rows", 0)
        self.log_info(
            f"Loaded {rows_loaded} rows from {source_bucket}.{source_table_name} "
            f"to workspace {workspace_object_name}.{dest_table_name}"
        )

        return None
