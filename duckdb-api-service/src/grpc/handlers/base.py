"""Base handler for command processing."""

import logging
from abc import ABC, abstractmethod
from typing import Optional
from google.protobuf.message import Message

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "generated"))

from proto import common_pb2, credentials_pb2
from src.grpc.utils import LogMessageCollector


class BaseCommandHandler(ABC):
    """
    Abstract base class for command handlers.

    Each command type (InitBackend, CreateProject, etc.) has its own
    handler class that inherits from this base. Handlers process the
    command and return a response message.
    """

    def __init__(self):
        self.log_collector = LogMessageCollector()
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def handle(
        self,
        command: Message,
        credentials: Optional[dict],
        runtime_options: common_pb2.RuntimeOptions
    ) -> Optional[Message]:
        """
        Handle the command and return a response.

        Args:
            command: The protobuf Any-packed command message
            credentials: Extracted credentials dict (project_id, api_key) or None
            runtime_options: Runtime options (runId, queryTags)

        Returns:
            Response message or None if no response is expected

        Raises:
            ValueError: For invalid parameters
            KeyError: For missing resources
        """
        pass

    def get_log_messages(self) -> list:
        """Get all collected log messages."""
        return self.log_collector.get_messages()

    def log_info(self, message: str) -> None:
        """Log an informational message (both Python logger and gRPC response)."""
        self.logger.info(message)
        self.log_collector.info(message)

    def log_warning(self, message: str) -> None:
        """Log a warning message."""
        self.logger.warning(message)
        self.log_collector.warning(message)

    def log_error(self, message: str) -> None:
        """Log an error message."""
        self.logger.error(message)
        self.log_collector.error(message)

    def log_debug(self, message: str) -> None:
        """Log a debug message."""
        self.logger.debug(message)
        self.log_collector.debug(message)

    @staticmethod
    def extract_credentials(credentials_any) -> dict:
        """
        Extract project_id and api_key from GenericBackendCredentials.

        Per ADR-014, credentials are mapped as:
        - host = project_id
        - principal = api_key
        """
        creds = credentials_pb2.GenericBackendCredentials()
        credentials_any.Unpack(creds)
        return {
            'project_id': creds.host,
            'api_key': creds.principal,
        }
