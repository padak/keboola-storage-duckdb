"""Bucket command handlers for gRPC service."""

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import bucket_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import ProjectDBManager


class CreateBucketHandler(BaseCommandHandler):
    """
    Create a new bucket in a project.

    This handler:
    1. Parses bucketId to extract stage and bucket name
    2. Creates the bucket directory using ProjectDBManager
    3. Returns bucket identifier for subsequent operations

    Note: bucketId format is "stage.c-name" (e.g., "in.c-sales", "out.c-reports")
    """

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> bucket_pb2.CreateBucketResponse:
        cmd = bucket_pb2.CreateBucketCommand()
        command.Unpack(cmd)

        project_id = cmd.projectId
        bucket_id = cmd.bucketId  # Format: "in.c-sales" or "out.c-reports"
        branch_id = cmd.branchId if cmd.branchId else "default"

        if not project_id:
            raise ValueError("projectId is required")
        if not bucket_id:
            raise ValueError("bucketId is required")

        # Parse bucket_id: "in.c-sales" -> bucket_name = "in_c_sales"
        # Connection sends bucket_id in format "stage.c-name"
        # We normalize it to use underscores for filesystem compatibility
        bucket_name = bucket_id.replace(".", "_").replace("-", "_")

        # For branch operations, adjust project_id
        effective_project_id = project_id
        if branch_id and branch_id != "default":
            effective_project_id = f"{project_id}_branch_{branch_id}"

        # Create bucket using existing logic
        result = self.project_manager.create_bucket(
            project_id=effective_project_id,
            bucket_name=bucket_name,
            description=None,
        )

        self.log_info(f"Bucket {bucket_id} created in project {project_id}")

        # Build response
        response = bucket_pb2.CreateBucketResponse()
        response.createBucketObjectName = bucket_name
        response.path.extend([project_id, bucket_name])

        return response


class DropBucketHandler(BaseCommandHandler):
    """
    Drop (delete) a bucket from a project.

    This handler:
    1. Validates bucket exists
    2. Deletes bucket directory (with cascade if specified)
    3. Cleans up any table locks

    Note: bucketObjectName is the normalized bucket name from CreateBucketResponse
    """

    def __init__(self, project_manager: ProjectDBManager):
        super().__init__()
        self.project_manager = project_manager

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        cmd = bucket_pb2.DropBucketCommand()
        command.Unpack(cmd)

        bucket_name = cmd.bucketObjectName
        cascade = cmd.isCascade

        if not bucket_name:
            raise ValueError("bucketObjectName is required")

        # Extract project_id from credentials
        if not credentials or "project_id" not in credentials:
            raise ValueError("Credentials with project_id required for DropBucket")

        project_id = credentials["project_id"]

        # Check if bucket exists
        if not self.project_manager.bucket_exists(project_id, bucket_name):
            self.log_warning(f"Bucket {bucket_name} not found in project {project_id}")
            # Idempotent - return success even if not found
            return None

        # Delete bucket
        self.project_manager.delete_bucket(
            project_id=project_id,
            bucket_name=bucket_name,
            cascade=cascade,
        )

        self.log_info(
            f"Bucket {bucket_name} dropped from project {project_id} (cascade={cascade})"
        )

        # DropBucketCommand has no response message defined
        return None
