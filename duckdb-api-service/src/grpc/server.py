"""gRPC server for StorageDriverService."""

import logging
import signal
import sys
from concurrent import futures

import grpc

# Add generated proto to path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "generated"))

from proto import service_pb2_grpc
from src.grpc.servicer import StorageDriverServicer
from src.database import MetadataDB, ProjectDBManager

logger = logging.getLogger(__name__)


def create_server(
    metadata_db: MetadataDB,
    project_manager: ProjectDBManager,
    host: str = "0.0.0.0",
    port: int = 50051,
    max_workers: int = 10
) -> grpc.Server:
    """
    Create and configure gRPC server.

    Args:
        metadata_db: Shared MetadataDB instance
        project_manager: Shared ProjectDBManager instance
        host: Server host (default: 0.0.0.0 for all interfaces)
        port: Server port (default: 50051, standard gRPC port)
        max_workers: Maximum number of worker threads

    Returns:
        Configured but not started gRPC server
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

    servicer = StorageDriverServicer(metadata_db, project_manager)
    service_pb2_grpc.add_StorageDriverServiceServicer_to_server(servicer, server)

    address = f"{host}:{port}"
    server.add_insecure_port(address)

    logger.info("gRPC server configured on %s", address)
    return server


def serve(
    metadata_db: MetadataDB,
    project_manager: ProjectDBManager,
    host: str = "0.0.0.0",
    port: int = 50051,
    max_workers: int = 10
) -> grpc.Server:
    """
    Start gRPC server (blocking).

    This is for standalone gRPC server usage.
    For unified REST+gRPC, use unified_server.py instead.

    Args:
        metadata_db: Shared MetadataDB instance
        project_manager: Shared ProjectDBManager instance
        host: Server host
        port: Server port
        max_workers: Maximum worker threads

    Returns:
        Running gRPC server (after termination)
    """
    server = create_server(metadata_db, project_manager, host, port, max_workers)
    server.start()

    logger.info("gRPC server started on %s:%d", host, port)

    # Graceful shutdown handler
    def shutdown(signum, frame):
        logger.info("Shutting down gRPC server...")
        server.stop(grace=5)
        logger.info("gRPC server stopped")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    server.wait_for_termination()
    return server


if __name__ == "__main__":
    """
    Run standalone gRPC server.

    Usage:
        python -m src.grpc.server
        python -m src.grpc.server --port 8080
    """
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="StorageDriver gRPC Server")
    parser.add_argument("--host", default="0.0.0.0", help="Server host")
    parser.add_argument("--port", type=int, default=50051, help="Server port")
    parser.add_argument("--workers", type=int, default=10, help="Max workers")

    args = parser.parse_args()

    # Create shared instances
    metadata_db = MetadataDB()
    project_manager = ProjectDBManager()

    # Initialize metadata DB
    metadata_db.initialize()

    # Start server
    serve(metadata_db, project_manager, args.host, args.port, args.workers)
