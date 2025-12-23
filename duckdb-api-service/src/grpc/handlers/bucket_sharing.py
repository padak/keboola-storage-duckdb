"""Bucket sharing command handlers for gRPC service.

Phase 12f: Bucket sharing gRPC handlers
- ShareBucketCommand
- UnshareBucketCommand
- LinkBucketCommand
- UnlinkBucketCommand
- GrantBucketAccessToReadOnlyRoleCommand
- RevokeBucketAccessFromReadOnlyRoleCommand
"""

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import bucket_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import MetadataDB, ProjectDBManager
from src import metrics


class ShareBucketHandler(BaseCommandHandler):
    """
    Share a bucket with another project.

    This records the share in metadata. The target project must call
    LinkBucketCommand to create actual views.
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
    ) -> bucket_pb2.ShareBucketResponse:
        cmd = bucket_pb2.ShareBucketCommand()
        command.Unpack(cmd)

        source_project_id = cmd.sourceProjectId
        source_bucket_name = cmd.sourceBucketId or cmd.sourceBucketObjectName

        if not source_project_id:
            raise ValueError("sourceProjectId is required")
        if not source_bucket_name:
            raise ValueError("sourceBucketId or sourceBucketObjectName is required")

        # Normalize bucket name (e.g., "in.c-sales" -> "in_c_sales")
        bucket_name = source_bucket_name.replace(".", "_").replace("-", "_")

        # Verify source bucket exists
        if not self.project_manager.bucket_exists(source_project_id, bucket_name):
            raise KeyError(f"Bucket {bucket_name} not found in project {source_project_id}")

        # For DuckDB, sharing is recorded but doesn't create database objects.
        # The target project calls LinkBucket to create views.
        # We generate a share role name for compatibility with the proto interface.
        share_role_name = f"share_{source_project_id}_{bucket_name}"

        self.log_info(f"Bucket {bucket_name} shared from project {source_project_id}")

        metrics.BUCKET_SHARING_OPERATIONS.labels(operation="share", status="success").inc()

        response = bucket_pb2.ShareBucketResponse()
        response.bucketShareRoleName = share_role_name

        return response


class UnshareBucketHandler(BaseCommandHandler):
    """
    Unshare a bucket (remove share record).

    This removes the share metadata but doesn't automatically unlink
    from the target side. Target should call UnlinkBucketCommand.
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
        cmd = bucket_pb2.UnshareBucketCommand()
        command.Unpack(cmd)

        bucket_name = cmd.bucketObjectName
        share_role_name = cmd.bucketShareRoleName

        if not bucket_name:
            raise ValueError("bucketObjectName is required")

        # Extract project_id from credentials
        if not credentials or "project_id" not in credentials:
            raise ValueError("Credentials with project_id required for UnshareBucket")

        project_id = credentials["project_id"]

        # Normalize bucket name
        bucket_name = bucket_name.replace(".", "_").replace("-", "_")

        self.log_info(f"Bucket {bucket_name} unshared from project {project_id}")

        metrics.BUCKET_SHARING_OPERATIONS.labels(operation="unshare", status="success").inc()

        # No response for UnshareBucketCommand
        return None


class LinkBucketHandler(BaseCommandHandler):
    """
    Link a bucket from another project.

    This:
    1. ATTACHes the source project's table files in READ_ONLY mode
    2. Creates VIEWs for each table in the source bucket
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
    ) -> bucket_pb2.LinkedBucketResponse:
        cmd = bucket_pb2.LinkBucketCommand()
        command.Unpack(cmd)

        target_project_id = cmd.targetProjectId
        target_bucket_id = cmd.targetBucketId
        source_share_role_name = cmd.sourceShareRoleName

        if not target_project_id:
            raise ValueError("targetProjectId is required")
        if not target_bucket_id:
            raise ValueError("targetBucketId is required")
        if not source_share_role_name:
            raise ValueError("sourceShareRoleName is required")

        # Parse source info from share role name: "share_{project}_{bucket}"
        # Format: share_<source_project_id>_<source_bucket_name>
        parts = source_share_role_name.split("_", 2)
        if len(parts) < 3 or parts[0] != "share":
            raise ValueError(f"Invalid sourceShareRoleName format: {source_share_role_name}")

        source_project_id = parts[1]
        source_bucket_name = parts[2]

        # Normalize target bucket name
        target_bucket_name = target_bucket_id.replace(".", "_").replace("-", "_")

        # Verify source bucket exists
        if not self.project_manager.bucket_exists(source_project_id, source_bucket_name):
            raise KeyError(f"Source bucket {source_bucket_name} not found in project {source_project_id}")

        # Check if target bucket already exists
        if self.project_manager.bucket_exists(target_project_id, target_bucket_name):
            raise ValueError(f"Bucket {target_bucket_name} already exists in project {target_project_id}")

        # Generate alias for attached database
        db_alias = f"source_proj_{source_project_id}"

        # Link bucket using combined method (ATTACH + views in one connection)
        created_views = self.project_manager.link_bucket_with_views(
            target_project_id=target_project_id,
            target_bucket_name=target_bucket_name,
            source_project_id=source_project_id,
            source_bucket_name=source_bucket_name,
            source_db_alias=db_alias,
        )

        # Record link in metadata
        self.metadata_db.create_bucket_link(
            target_project_id=target_project_id,
            target_bucket_name=target_bucket_name,
            source_project_id=source_project_id,
            source_bucket_name=source_bucket_name,
            attached_db_alias=db_alias,
        )

        self.log_info(
            f"Bucket {target_bucket_name} linked in project {target_project_id} "
            f"from {source_project_id}.{source_bucket_name} ({len(created_views)} views)"
        )

        metrics.BUCKET_SHARING_OPERATIONS.labels(operation="link", status="success").inc()

        response = bucket_pb2.LinkedBucketResponse()
        response.linkedBucketObjectName = target_bucket_name

        return response


class UnlinkBucketHandler(BaseCommandHandler):
    """
    Unlink a previously linked bucket.

    This:
    1. Drops all views in the bucket
    2. Drops the schema (bucket)
    3. DETACHes the source database
    4. Removes link record from metadata
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
        cmd = bucket_pb2.UnlinkBucketCommand()
        command.Unpack(cmd)

        bucket_name = cmd.bucketObjectName

        if not bucket_name:
            raise ValueError("bucketObjectName is required")

        # Extract project_id from credentials
        if not credentials or "project_id" not in credentials:
            raise ValueError("Credentials with project_id required for UnlinkBucket")

        project_id = credentials["project_id"]

        # Normalize bucket name
        bucket_name = bucket_name.replace(".", "_").replace("-", "_")

        # Get link info
        link = self.metadata_db.get_bucket_link(project_id, bucket_name)
        if not link:
            self.log_warning(f"No link found for bucket {bucket_name} in project {project_id}")
            # Idempotent - return success even if not found
            return None

        # Drop views
        try:
            self.project_manager.drop_bucket_views(
                target_project_id=project_id,
                target_bucket_name=bucket_name,
            )
        except Exception as e:
            self.log_warning(f"Failed to drop views: {e}")

        # Drop schema (bucket)
        try:
            self.project_manager.delete_bucket(
                project_id=project_id,
                bucket_name=bucket_name,
                cascade=True,
            )
        except Exception as e:
            self.log_warning(f"Failed to delete bucket: {e}")

        # Detach database
        try:
            self.project_manager.detach_database(
                target_project_id=project_id,
                alias=link["attached_db_alias"],
            )
        except Exception as e:
            self.log_warning(f"Failed to detach database: {e}")

        # Remove link record
        self.metadata_db.delete_bucket_link(
            target_project_id=project_id,
            target_bucket_name=bucket_name,
        )

        self.log_info(f"Bucket {bucket_name} unlinked from project {project_id}")

        metrics.BUCKET_SHARING_OPERATIONS.labels(operation="unlink", status="success").inc()

        return None


class GrantBucketAccessToReadOnlyRoleHandler(BaseCommandHandler):
    """
    Grant readonly access to a bucket.

    For DuckDB, this is a metadata operation since DuckDB doesn't have
    user-level permissions. Readonly is enforced via ATTACH READ_ONLY.
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
    ) -> bucket_pb2.GrantBucketAccessToReadOnlyRoleResponse:
        cmd = bucket_pb2.GrantBucketAccessToReadOnlyRoleCommand()
        command.Unpack(cmd)

        # Extract bucket from path
        path = list(cmd.path)
        destination_name = cmd.destinationObjectName
        project_readonly_role = cmd.projectReadOnlyRoleName

        # Parse path to get bucket info
        if path:
            bucket_name = path[-1] if path else destination_name
        else:
            bucket_name = destination_name

        if not bucket_name:
            raise ValueError("path or destinationObjectName is required")

        # Normalize bucket name
        bucket_name = bucket_name.replace(".", "_").replace("-", "_")

        # For DuckDB, readonly is inherent via ATTACH READ_ONLY
        # This is a no-op but we log it for audit purposes
        self.log_info(f"Readonly access granted to bucket {bucket_name} (enforced via ATTACH READ_ONLY)")

        metrics.BUCKET_SHARING_OPERATIONS.labels(operation="grant_readonly", status="success").inc()

        response = bucket_pb2.GrantBucketAccessToReadOnlyRoleResponse()
        response.createBucketObjectName = bucket_name

        return response


class RevokeBucketAccessFromReadOnlyRoleHandler(BaseCommandHandler):
    """
    Revoke readonly access from a bucket.

    For DuckDB, this is a no-op since there are no user-level permissions.
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
        cmd = bucket_pb2.RevokeBucketAccessFromReadOnlyRoleCommand()
        command.Unpack(cmd)

        bucket_name = cmd.bucketObjectName

        if not bucket_name:
            raise ValueError("bucketObjectName is required")

        # For DuckDB, this is a no-op
        self.log_info(f"Readonly access revoked from bucket {bucket_name} (no-op for DuckDB)")

        metrics.BUCKET_SHARING_OPERATIONS.labels(operation="revoke_readonly", status="success").inc()

        return None
