"""
Unified server running both REST (FastAPI) and gRPC.

This is the recommended way to run the DuckDB API Service in production,
as it provides a single process with:
- REST API on port 8000 (for dashboard, metrics, debugging)
- gRPC on port 50051 (for Storage Driver protocol)

Usage:
    python -m src.unified_server
"""

import asyncio
import logging
import signal
import sys
import threading
from concurrent import futures
from contextlib import asynccontextmanager

import grpc
import uvicorn

# Add generated proto to path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "generated"))

from proto import service_pb2_grpc
from src.main import app
from src.grpc.servicer import StorageDriverServicer
from src.database import MetadataDB, ProjectDBManager
from src.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GRPCServerManager:
    """Manages the gRPC server lifecycle."""

    def __init__(
        self,
        metadata_db: MetadataDB,
        project_manager: ProjectDBManager,
        host: str = "0.0.0.0",
        port: int = 50051,
        max_workers: int = 10
    ):
        self.metadata_db = metadata_db
        self.project_manager = project_manager
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.server: grpc.Server | None = None

    def start(self) -> grpc.Server:
        """Start gRPC server in a background thread."""
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=self.max_workers)
        )

        servicer = StorageDriverServicer(self.metadata_db, self.project_manager)
        service_pb2_grpc.add_StorageDriverServiceServicer_to_server(servicer, self.server)

        address = f"{self.host}:{self.port}"
        self.server.add_insecure_port(address)
        self.server.start()

        logger.info("gRPC server started on %s", address)
        return self.server

    def stop(self, grace: int = 5):
        """Stop gRPC server gracefully."""
        if self.server:
            logger.info("Stopping gRPC server (grace=%ds)...", grace)
            self.server.stop(grace=grace)
            logger.info("gRPC server stopped")


# Global server manager instance
_grpc_manager: GRPCServerManager | None = None


def run_unified_server(
    rest_host: str = "0.0.0.0",
    rest_port: int = 8000,
    grpc_host: str = "0.0.0.0",
    grpc_port: int = 50051,
    grpc_workers: int = 10
):
    """
    Run unified REST + gRPC server.

    This starts:
    1. gRPC server on grpc_port (background thread)
    2. FastAPI/uvicorn on rest_port (main thread)

    Both servers share the same MetadataDB and ProjectDBManager instances.
    """
    global _grpc_manager

    # Create shared instances
    metadata_db = MetadataDB()
    project_manager = ProjectDBManager()

    # Initialize metadata DB
    logger.info("Initializing metadata database...")
    metadata_db.initialize()

    # Start gRPC server in background
    _grpc_manager = GRPCServerManager(
        metadata_db, project_manager, grpc_host, grpc_port, grpc_workers
    )
    _grpc_manager.start()

    # Graceful shutdown handler
    def shutdown():
        if _grpc_manager:
            _grpc_manager.stop()

    # Run FastAPI in main thread
    logger.info("Starting FastAPI on %s:%d...", rest_host, rest_port)
    try:
        uvicorn.run(
            app,
            host=rest_host,
            port=rest_port,
            log_level="info"
        )
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        shutdown()


def main():
    """Entry point for unified server."""
    import argparse

    parser = argparse.ArgumentParser(description="DuckDB API Service (REST + gRPC)")
    parser.add_argument("--rest-host", default="0.0.0.0", help="REST API host")
    parser.add_argument("--rest-port", type=int, default=8000, help="REST API port")
    parser.add_argument("--grpc-host", default="0.0.0.0", help="gRPC host")
    parser.add_argument("--grpc-port", type=int, default=50051, help="gRPC port")
    parser.add_argument("--grpc-workers", type=int, default=10, help="gRPC worker threads")

    args = parser.parse_args()

    run_unified_server(
        rest_host=args.rest_host,
        rest_port=args.rest_port,
        grpc_host=args.grpc_host,
        grpc_port=args.grpc_port,
        grpc_workers=args.grpc_workers
    )


if __name__ == "__main__":
    main()
