"""Backend command handlers."""

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import backend_pb2, common_pb2
from src.grpc.handlers.base import BaseCommandHandler
from src.database import MetadataDB


class InitBackendHandler(BaseCommandHandler):
    """
    Initialize storage backend - validates connection works.

    This handler is called when the DuckDB backend is registered
    in Keboola Connection. It ensures the metadata database is
    initialized and ready to accept commands.
    """

    def __init__(self, metadata_db: MetadataDB):
        super().__init__()
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> backend_pb2.InitBackendResponse:
        # Unpack command (even if we don't use all fields)
        cmd = backend_pb2.InitBackendCommand()
        command.Unpack(cmd)

        # Initialize metadata DB - creates schema if needed
        self.metadata_db.initialize()

        self.log_info("DuckDB backend initialized successfully")

        return backend_pb2.InitBackendResponse()


class RemoveBackendHandler(BaseCommandHandler):
    """
    Remove storage backend.

    This handler is called when the DuckDB backend is unregistered
    from Keboola Connection. For DuckDB, we don't actually remove
    data - the files stay on disk. This just disconnects Connection.
    """

    def __init__(self, metadata_db: MetadataDB):
        super().__init__()
        self.metadata_db = metadata_db

    def handle(
        self,
        command,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ):
        cmd = backend_pb2.RemoveBackendCommand()
        command.Unpack(cmd)

        # For DuckDB, we don't remove data on backend removal
        # The data stays on disk, just Connection won't use it
        self.log_info("DuckDB backend removed from Connection (data preserved)")

        # RemoveBackendCommand has no response message defined
        return None
