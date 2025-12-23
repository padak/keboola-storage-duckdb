"""
Utility functions for gRPC handlers.

Adapted from storage-backend/generated-py/examples/utils.py
"""

import logging
from typing import Optional, List
from google.protobuf.message import Message
from google.protobuf import any_pb2

# Add generated proto to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "generated"))

from proto import common_pb2

logger = logging.getLogger(__name__)


def get_type_name(any_msg: any_pb2.Any) -> str:
    """
    Extract type name from Any message (e.g., 'CreateProjectCommand').

    The type_url format is 'type.googleapis.com/keboola.storageDriver.command.backend.InitBackendCommand'.
    We extract just the class name by:
    1. Split by '/' to get 'keboola.storageDriver.command.backend.InitBackendCommand'
    2. Split by '.' to get the last part: 'InitBackendCommand'
    """
    full_name = any_msg.type_url.split('/')[-1]
    # Extract just the class name (last part after the final dot)
    return full_name.split('.')[-1]


def create_log_message(
    level: common_pb2.LogMessage.Level,
    message: str,
    context: Optional[Message] = None
) -> common_pb2.LogMessage:
    """Create a LogMessage for DriverResponse."""
    log_msg = common_pb2.LogMessage()
    log_msg.level = level
    log_msg.message = message
    if context:
        log_msg.context.Pack(context)
    return log_msg


class LogMessageCollector:
    """
    Collector for LogMessage instances during command handling.

    These log messages are included in the DriverResponse and can be
    displayed to users in the Keboola Connection UI.
    """

    def __init__(self):
        self._messages: List[common_pb2.LogMessage] = []

    def add(
        self,
        level: common_pb2.LogMessage.Level,
        message: str,
        context: Optional[Message] = None
    ) -> None:
        """Add a log message."""
        self._messages.append(create_log_message(level, message, context))

    def info(self, message: str, context: Optional[Message] = None) -> None:
        """Add an informational level message."""
        self.add(common_pb2.LogMessage.Level.Informational, message, context)

    def warning(self, message: str, context: Optional[Message] = None) -> None:
        """Add a warning level message."""
        self.add(common_pb2.LogMessage.Level.Warning, message, context)

    def error(self, message: str, context: Optional[Message] = None) -> None:
        """Add an error level message."""
        self.add(common_pb2.LogMessage.Level.Error, message, context)

    def debug(self, message: str, context: Optional[Message] = None) -> None:
        """Add a debug level message."""
        self.add(common_pb2.LogMessage.Level.Debug, message, context)

    def get_messages(self) -> List[common_pb2.LogMessage]:
        """Get all collected log messages."""
        return self._messages

    def clear(self) -> None:
        """Clear all collected messages."""
        self._messages.clear()

    def __len__(self) -> int:
        """Return the number of collected messages."""
        return len(self._messages)
