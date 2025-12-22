"""Command handlers for gRPC service."""

from .backend import InitBackendHandler, RemoveBackendHandler
from .project import CreateProjectHandler, DropProjectHandler
from .bucket import CreateBucketHandler, DropBucketHandler
from .table import CreateTableHandler, DropTableHandler, PreviewTableHandler
from .info import ObjectInfoHandler
from .import_export import TableImportFromFileHandler, TableExportToFileHandler
from .schema import (
    AddColumnHandler,
    DropColumnHandler,
    AlterColumnHandler,
    AddPrimaryKeyHandler,
    DropPrimaryKeyHandler,
    DeleteTableRowsHandler,
)
from .workspace import (
    CreateWorkspaceHandler,
    DropWorkspaceHandler,
    ClearWorkspaceHandler,
    ResetWorkspacePasswordHandler,
    DropWorkspaceObjectHandler,
    GrantWorkspaceAccessToProjectHandler,
    RevokeWorkspaceAccessToProjectHandler,
    LoadTableToWorkspaceHandler,
)
# Bucket sharing handlers (Phase 12f)
from .bucket_sharing import (
    ShareBucketHandler,
    UnshareBucketHandler,
    LinkBucketHandler,
    UnlinkBucketHandler,
    GrantBucketAccessToReadOnlyRoleHandler,
    RevokeBucketAccessFromReadOnlyRoleHandler,
)
# Branch handlers (Phase 12g)
from .branch import CreateDevBranchHandler, DropDevBranchHandler
# Query handler (Phase 12g)
from .query import ExecuteQueryHandler

__all__ = [
    # Backend handlers
    'InitBackendHandler',
    'RemoveBackendHandler',
    # Project handlers
    'CreateProjectHandler',
    'DropProjectHandler',
    # Bucket handlers (Phase 12c)
    'CreateBucketHandler',
    'DropBucketHandler',
    # Table handlers (Phase 12c)
    'CreateTableHandler',
    'DropTableHandler',
    'PreviewTableHandler',
    # Info handlers (Phase 12c)
    'ObjectInfoHandler',
    # Import/Export handlers (Phase 12c)
    'TableImportFromFileHandler',
    'TableExportToFileHandler',
    # Schema handlers (Phase 12d)
    'AddColumnHandler',
    'DropColumnHandler',
    'AlterColumnHandler',
    'AddPrimaryKeyHandler',
    'DropPrimaryKeyHandler',
    'DeleteTableRowsHandler',
    # Workspace handlers (Phase 12e)
    'CreateWorkspaceHandler',
    'DropWorkspaceHandler',
    'ClearWorkspaceHandler',
    'ResetWorkspacePasswordHandler',
    'DropWorkspaceObjectHandler',
    'GrantWorkspaceAccessToProjectHandler',
    'RevokeWorkspaceAccessToProjectHandler',
    'LoadTableToWorkspaceHandler',
    # Bucket sharing handlers (Phase 12f)
    'ShareBucketHandler',
    'UnshareBucketHandler',
    'LinkBucketHandler',
    'UnlinkBucketHandler',
    'GrantBucketAccessToReadOnlyRoleHandler',
    'RevokeBucketAccessFromReadOnlyRoleHandler',
    # Branch handlers (Phase 12g)
    'CreateDevBranchHandler',
    'DropDevBranchHandler',
    # Query handler (Phase 12g)
    'ExecuteQueryHandler',
]
