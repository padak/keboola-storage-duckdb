"""Custom PostgreSQL Wire Protocol server for Keboola Workspaces.

This module provides a PG Wire server that:
1. Authenticates using workspace credentials from metadata_db
2. Opens workspace-specific DuckDB files
3. ATTACHes all project tables as READ_ONLY
4. Tracks sessions for monitoring
5. Collects Prometheus metrics for observability

Usage:
    python -m src.pgwire_server --host 0.0.0.0 --port 5432

Or programmatically:
    server = WorkspacePGServer(("0.0.0.0", 5432))
    server.serve_forever()
"""

import argparse
import hashlib
import signal
import socket
import ssl
import socketserver
import struct
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional
import uuid

import duckdb
import structlog

from buenavista.core import Connection, Session, Extension, QueryResult
from buenavista.backends.duckdb import DuckDBConnection, DuckDBSession
from buenavista.postgres import (
    BuenaVistaServer,
    BuenaVistaHandler,
    BVContext,
    TransactionStatus,
)

from src.config import settings
from src.database import metadata_db, project_db_manager
from src.metrics import (
    PGWIRE_CONNECTIONS_TOTAL,
    PGWIRE_CONNECTIONS_ACTIVE,
    PGWIRE_QUERIES_TOTAL,
    PGWIRE_QUERY_DURATION,
    PGWIRE_SESSIONS_TOTAL,
    PGWIRE_AUTH_DURATION,
)

logger = structlog.get_logger()


class QueryTimeoutError(Exception):
    """Raised when a query exceeds the configured timeout."""

    pass


class WorkspaceSession(DuckDBSession):
    """
    Extended DuckDB session for workspace connections.

    Adds:
    - Session tracking in metadata_db
    - Automatic ATTACH of project tables
    - Resource limit enforcement
    - Query timeout enforcement
    - Prometheus metrics collection
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        workspace_id: str,
        project_id: str,
        branch_id: Optional[str],
        session_id: str,
        client_ip: Optional[str] = None,
        query_timeout: int = 300,
    ):
        # DuckDBSession expects a cursor, not connection
        super().__init__(conn.cursor())
        self._conn = conn  # Keep reference to connection for ATTACH operations
        self.workspace_id = workspace_id
        self.project_id = project_id
        self.branch_id = branch_id
        self.session_id = session_id
        self.client_ip = client_ip
        self.query_timeout = query_timeout
        self._attached_tables = []
        self._query_count = 0
        self._log = logger.bind(
            session_id=session_id,
            workspace_id=workspace_id,
            project_id=project_id,
        )

        # Increment active connections metric
        PGWIRE_CONNECTIONS_ACTIVE.labels(workspace_id=workspace_id).inc()
        PGWIRE_SESSIONS_TOTAL.inc()

    def attach_project_tables(self) -> int:
        """ATTACH all project tables as READ_ONLY. Returns count."""
        attached = 0
        buckets = project_db_manager.list_buckets(self.project_id)

        for bucket in buckets:
            bucket_name = bucket["name"]
            tables = project_db_manager.list_tables(self.project_id, bucket_name)

            for table in tables:
                table_name = table["name"]

                # Get path to table file
                if self.branch_id:
                    branch_path = project_db_manager.get_branch_table_path(
                        self.project_id, self.branch_id, bucket_name, table_name
                    )
                    if branch_path.exists():
                        table_path = branch_path
                    else:
                        table_path = project_db_manager.get_table_path(
                            self.project_id, bucket_name, table_name
                        )
                else:
                    table_path = project_db_manager.get_table_path(
                        self.project_id, bucket_name, table_name
                    )

                if not table_path.exists():
                    self._log.warning("table_file_not_found", table_path=str(table_path))
                    continue

                # Create unique alias for attached database
                alias = f"{bucket_name}_{table_name}"

                try:
                    self._conn.execute(
                        f"ATTACH '{table_path}' AS \"{alias}\" (READ_ONLY)"
                    )
                    self._attached_tables.append(alias)
                    attached += 1
                    self._log.debug("table_attached", alias=alias, path=str(table_path))
                except Exception as e:
                    self._log.error("table_attach_failed", alias=alias, error=str(e))

        return attached

    def execute(self, query: str):
        """Execute query with timeout enforcement and metrics."""
        start_time = time.time()
        self._query_count += 1

        self._log.info("query_started", query_preview=query[:100] if len(query) > 100 else query)

        try:
            # Set DuckDB query timeout
            timeout_ms = self.query_timeout * 1000
            self._conn.execute(f"SET statement_timeout = {timeout_ms}")

            result = super().execute(query)

            duration = time.time() - start_time
            PGWIRE_QUERIES_TOTAL.labels(
                workspace_id=self.workspace_id, status="success"
            ).inc()
            PGWIRE_QUERY_DURATION.labels(workspace_id=self.workspace_id).observe(duration)

            self._log.info("query_completed", duration_ms=round(duration * 1000, 2))
            return result

        except Exception as e:
            duration = time.time() - start_time
            error_type = type(e).__name__

            # Check if it's a timeout
            if "timeout" in str(e).lower() or "interrupt" in str(e).lower():
                PGWIRE_QUERIES_TOTAL.labels(
                    workspace_id=self.workspace_id, status="timeout"
                ).inc()
                self._log.warning(
                    "query_timeout",
                    duration_ms=round(duration * 1000, 2),
                    timeout_seconds=self.query_timeout,
                )
            else:
                PGWIRE_QUERIES_TOTAL.labels(
                    workspace_id=self.workspace_id, status="error"
                ).inc()
                self._log.error(
                    "query_failed",
                    error=str(e),
                    error_type=error_type,
                    duration_ms=round(duration * 1000, 2),
                )

            PGWIRE_QUERY_DURATION.labels(workspace_id=self.workspace_id).observe(duration)
            raise

    def close(self):
        """Close session and cleanup."""
        self._log.info("session_closing", query_count=self._query_count)

        # Decrement active connections metric
        PGWIRE_CONNECTIONS_ACTIVE.labels(workspace_id=self.workspace_id).dec()
        PGWIRE_SESSIONS_TOTAL.dec()

        # Update session status in metadata
        try:
            metadata_db.close_pgwire_session(self.session_id, status="disconnected")
        except Exception as e:
            self._log.error("session_close_failed", error=str(e))

        # Detach all attached databases
        for alias in self._attached_tables:
            try:
                self._conn.execute(f'DETACH "{alias}"')
            except Exception:
                pass

        super().close()
        self._log.info("session_closed")


class WorkspaceConnection(Connection):
    """
    Dynamic connection factory for workspace sessions.

    Creates a new DuckDB connection for each authenticated workspace user.
    Manages session lifecycle and graceful shutdown.
    """

    def __init__(self):
        self._sessions: Dict[str, WorkspaceSession] = {}
        self._lock = threading.Lock()
        self._shutdown_event = threading.Event()

    def parameters(self) -> Dict[str, str]:
        """Return server parameters required by PostgreSQL clients."""
        return {
            "server_version": "9.3.duckdb",
            "server_encoding": "UTF8",
            "client_encoding": "UTF8",
            "DateStyle": "ISO, MDY",
            "integer_datetimes": "on",
            "TimeZone": "UTC",
        }

    def new_session(self) -> Session:
        """Not used - sessions created via create_workspace_session."""
        raise NotImplementedError("Use create_workspace_session instead")

    def create_workspace_session(
        self,
        workspace_id: str,
        project_id: str,
        branch_id: Optional[str],
        db_path: str,
        client_ip: Optional[str] = None,
    ) -> WorkspaceSession:
        """Create a new session for a workspace."""
        session_id = f"pgw_{uuid.uuid4().hex[:16]}"
        log = logger.bind(
            session_id=session_id,
            workspace_id=workspace_id,
            project_id=project_id,
        )

        # Check if we're shutting down
        if self._shutdown_event.is_set():
            log.warning("session_rejected_shutdown")
            raise RuntimeError("Server is shutting down")

        # Open workspace DuckDB
        conn = duckdb.connect(db_path)

        # Set resource limits
        conn.execute(f"SET memory_limit='{settings.pgwire_session_memory_limit}'")
        conn.execute(f"SET threads={settings.duckdb_threads}")

        # Create session with query timeout
        session = WorkspaceSession(
            conn=conn,
            workspace_id=workspace_id,
            project_id=project_id,
            branch_id=branch_id,
            session_id=session_id,
            client_ip=client_ip,
            query_timeout=settings.pgwire_query_timeout_seconds,
        )

        # Attach project tables
        attached = session.attach_project_tables()
        log.info("session_created", attached_tables=attached, client_ip=client_ip)

        # Register session in metadata
        try:
            metadata_db.create_pgwire_session(
                session_id=session_id,
                workspace_id=workspace_id,
                client_ip=client_ip,
            )
        except Exception as e:
            log.error("session_register_failed", error=str(e))

        with self._lock:
            self._sessions[session_id] = session

        return session

    def remove_session(self, session_id: str):
        """Remove a session from tracking."""
        with self._lock:
            if session_id in self._sessions:
                del self._sessions[session_id]
                logger.debug("session_removed", session_id=session_id)

    def get_active_sessions(self) -> Dict[str, WorkspaceSession]:
        """Get all active sessions."""
        with self._lock:
            return dict(self._sessions)

    def initiate_shutdown(self, timeout: float = 30.0) -> int:
        """
        Initiate graceful shutdown.

        Sets shutdown flag and waits for active sessions to complete.
        Returns number of sessions that were forcefully closed.
        """
        self._shutdown_event.set()
        logger.info("shutdown_initiated", active_sessions=len(self._sessions))

        # Wait for sessions to complete
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._lock:
                if not self._sessions:
                    logger.info("shutdown_complete", forced_closures=0)
                    return 0
            time.sleep(0.5)

        # Force close remaining sessions
        forced = 0
        with self._lock:
            for session_id, session in list(self._sessions.items()):
                try:
                    session.close()
                    forced += 1
                    logger.warning("session_force_closed", session_id=session_id)
                except Exception as e:
                    logger.error("session_force_close_failed", session_id=session_id, error=str(e))
            self._sessions.clear()

        logger.info("shutdown_complete", forced_closures=forced)
        return forced


class WorkspacePGHandler(BuenaVistaHandler):
    """
    Custom handler with workspace credential authentication.

    Uses cleartext password auth over TLS:
    1. Look up workspace by username in metadata_db
    2. Verify password (SHA256 hash comparison)
    3. Create workspace-specific DuckDB session
    4. ATTACH project tables
    """

    def send_auth_request(self, ctx: BVContext):
        """Send cleartext password authentication request."""
        # Use AuthenticationCleartextPassword (code 3)
        self.send_authentication_cleartext()

    def send_authentication_cleartext(self):
        """Send cleartext password request (AuthenticationCleartextPassword)."""
        import struct
        # 'R' = AUTHENTICATION_REQUEST, length=8, code=3 (cleartext)
        msg = struct.pack(">cii", b"R", 8, 3)
        self.wfile.write(msg)
        self.wfile.flush()

    def handle(self):
        """Override handle to support SSL and cleartext password."""
        try:
            # Capture client IP for logging
            self.client_ip = self.client_address[0] if self.client_address else None

            # Check for SSL request first
            if not self._handle_ssl_if_requested():
                return

            conn = self.server.conn
            ctx = self.handle_startup(conn)
            if ctx is None:
                return

            # Read password message
            msg_type = self.rfile.read(1)
            if msg_type != b"p":
                self.send_error("Expected password message")
                return

            # Read message length and payload
            length = struct.unpack(">i", self.rfile.read(4))[0]
            payload = self.rfile.read(length - 4)

            # Handle cleartext password
            self.handle_cleartext_password(ctx, payload)

            if not ctx.authenticated:
                return

            # Continue with normal query loop
            self._handle_queries(ctx)

        except Exception as e:
            logger.error(f"Connection error: {e}")

    def _handle_ssl_if_requested(self) -> bool:
        """Check for SSL request and upgrade connection if needed. Returns True to continue."""
        # Read the startup message length
        length_bytes = self.rfile.read(4)
        if len(length_bytes) < 4:
            return False

        length = struct.unpack(">i", length_bytes)[0]
        payload = self.rfile.read(length - 4)

        if len(payload) < 4:
            return False

        code = struct.unpack(">i", payload[:4])[0]

        # SSL request code
        if code == 80877103:
            if self.server.ssl_context:
                # Send 'S' to indicate SSL is supported
                self.wfile.write(b"S")
                self.wfile.flush()

                # Upgrade the socket to SSL
                try:
                    ssl_socket = self.server.ssl_context.wrap_socket(
                        self.request,
                        server_side=True,
                    )
                    # Replace the request socket
                    self.request = ssl_socket
                    self.rfile = ssl_socket.makefile("rb")
                    self.wfile = ssl_socket.makefile("wb")
                    logger.info("SSL connection established")
                except ssl.SSLError as e:
                    logger.error(f"SSL handshake failed: {e}")
                    return False
            else:
                # Send 'N' to indicate SSL is not supported
                self.wfile.write(b"N")
                self.wfile.flush()

            return True

        # Not an SSL request - put the bytes back by handling them normally
        # We need to reprocess this as the startup message
        self._pending_startup = length_bytes + payload
        return True

    def handle_startup(self, conn):
        """Handle the startup message."""
        # Check if we have a pending startup from SSL check
        if hasattr(self, '_pending_startup') and self._pending_startup:
            data = self._pending_startup
            self._pending_startup = None
            length = struct.unpack(">i", data[:4])[0]
            payload = data[4:length]
        else:
            length_bytes = self.rfile.read(4)
            if len(length_bytes) < 4:
                return None
            length = struct.unpack(">i", length_bytes)[0]
            payload = self.rfile.read(length - 4)

        if len(payload) < 4:
            return None

        code = struct.unpack(">i", payload[:4])[0]

        # Check for SSL request (in case we missed it earlier)
        if code == 80877103:
            self.wfile.write(b"N")
            self.wfile.flush()
            return self.handle_startup(conn)

        # Check for cancel request
        if code == 80877102:
            logger.info("Cancel request received")
            return None

        # Protocol version 3.0 (196608)
        if code != 196608:
            self.send_error(f"Unsupported protocol version: {code}")
            return None

        # Parse parameters
        params = {}
        param_data = payload[4:]
        parts = param_data.split(b"\x00")
        for i in range(0, len(parts) - 1, 2):
            if i + 1 < len(parts):
                key = parts[i].decode("utf-8")
                value = parts[i + 1].decode("utf-8")
                if key:
                    params[key] = value

        # Create context
        from buenavista.postgres import BVContext
        ctx = BVContext(session=None, rewriter=self.server.rewriter, params=params)

        # Send auth request
        self.send_auth_request(ctx)

        return ctx

    def _handle_queries(self, ctx: BVContext):
        """Handle query loop after authentication."""
        while True:
            try:
                msg_type = self.rfile.read(1)
                if not msg_type:
                    break

                length = struct.unpack(">i", self.rfile.read(4))[0]
                payload = self.rfile.read(length - 4)

                if msg_type == b"Q":
                    self.handle_query(ctx, payload)
                elif msg_type == b"X":
                    # Terminate
                    break
                elif msg_type == b"P":
                    self.handle_parse(ctx, payload)
                elif msg_type == b"B":
                    self.handle_bind(ctx, payload)
                elif msg_type == b"D":
                    self.handle_describe(ctx, payload)
                elif msg_type == b"E":
                    self.handle_execute(ctx, payload)
                elif msg_type == b"S":
                    # Sync - reset error state and send ReadyForQuery
                    ctx.sync()
                    self.send_ready_for_query(ctx)
                elif msg_type == b"C":
                    self.handle_close(ctx, payload)
                elif msg_type == b"H":
                    # Flush - just flush the output
                    self.wfile.flush()
                else:
                    logger.warning(f"Unknown message type: {msg_type}")

            except Exception as e:
                logger.error(f"Query error: {e}")
                self.send_error(str(e))

    def handle_cleartext_password(self, ctx: BVContext, payload: bytes):
        """Verify workspace credentials using cleartext password."""
        start_time = time.time()
        password = payload.decode("utf-8").rstrip("\x00")
        username = ctx.params.get("user", "")
        log = logger.bind(username=username, client_ip=self.client_ip)

        log.info("auth_attempt")

        # Look up workspace by username
        workspace = metadata_db.get_workspace_by_username(username)

        if not workspace:
            PGWIRE_CONNECTIONS_TOTAL.labels(status="auth_failed").inc()
            PGWIRE_AUTH_DURATION.observe(time.time() - start_time)
            log.warning("auth_failed_unknown_user")
            self.send_error("Invalid credentials")
            return

        workspace_id = workspace["id"]
        log = log.bind(workspace_id=workspace_id)

        # Check workspace status
        if workspace.get("status") != "active":
            PGWIRE_CONNECTIONS_TOTAL.labels(status="auth_failed").inc()
            PGWIRE_AUTH_DURATION.observe(time.time() - start_time)
            log.warning("auth_failed_inactive", status=workspace.get("status"))
            self.send_error("Workspace not active")
            return

        # Check expiration
        if workspace.get("expires_at"):
            expires_at = datetime.fromisoformat(workspace["expires_at"])
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at <= datetime.now(timezone.utc):
                PGWIRE_CONNECTIONS_TOTAL.labels(status="expired").inc()
                PGWIRE_AUTH_DURATION.observe(time.time() - start_time)
                log.warning("auth_failed_expired", expires_at=workspace["expires_at"])
                self.send_error("Workspace expired")
                return

        # Check connection limit
        active_sessions = metadata_db.count_active_pgwire_sessions(workspace_id)
        if active_sessions >= settings.pgwire_max_connections_per_workspace:
            PGWIRE_CONNECTIONS_TOTAL.labels(status="limit_reached").inc()
            PGWIRE_AUTH_DURATION.observe(time.time() - start_time)
            log.warning(
                "auth_failed_limit",
                active_sessions=active_sessions,
                max_connections=settings.pgwire_max_connections_per_workspace,
            )
            self.send_error("Too many connections")
            return

        # Verify password (SHA256 hash comparison)
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        if password_hash != workspace.get("password_hash"):
            PGWIRE_CONNECTIONS_TOTAL.labels(status="auth_failed").inc()
            PGWIRE_AUTH_DURATION.observe(time.time() - start_time)
            log.warning("auth_failed_password")
            self.send_error("Invalid credentials")
            return

        # Create workspace session
        workspace_conn: WorkspaceConnection = self.server.conn
        try:
            session = workspace_conn.create_workspace_session(
                workspace_id=workspace_id,
                project_id=workspace["project_id"],
                branch_id=workspace.get("branch_id"),
                db_path=workspace["db_path"],
                client_ip=self.client_ip,
            )
            ctx.session = session
            ctx.authenticated = True

            PGWIRE_CONNECTIONS_TOTAL.labels(status="success").inc()
            PGWIRE_AUTH_DURATION.observe(time.time() - start_time)
            log.info("auth_success", session_id=session.session_id)

            self.send_authentication_ok()
            self.handle_post_auth(ctx)

        except Exception as e:
            PGWIRE_CONNECTIONS_TOTAL.labels(status="auth_failed").inc()
            PGWIRE_AUTH_DURATION.observe(time.time() - start_time)
            log.error("session_creation_failed", error=str(e))
            self.send_error(f"Session creation failed: {e}")


class WorkspacePGServer(socketserver.ThreadingTCPServer):
    """
    PostgreSQL Wire Protocol server for Keboola Workspaces.

    Uses custom handler for workspace authentication.
    Supports optional TLS encryption and graceful shutdown.
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address,
        ssl_context: Optional[ssl.SSLContext] = None,
        shutdown_timeout: float = 30.0,
    ):
        self.conn = WorkspaceConnection()
        self.ssl_context = ssl_context
        self.auth = None  # We handle auth ourselves
        self.rewriter = None
        self.extensions = []
        self.shutdown_timeout = shutdown_timeout
        self._is_shutting_down = False

        super().__init__(server_address, WorkspacePGHandler)

        logger.info(
            "pgwire_server_created",
            host=server_address[0],
            port=server_address[1],
            ssl_enabled=ssl_context is not None,
        )

    def shutdown_request(self, request):
        """Called to shutdown and close an individual request."""
        try:
            request.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        self.close_request(request)

    def graceful_shutdown(self):
        """
        Perform graceful shutdown.

        1. Stop accepting new connections
        2. Wait for active queries to complete
        3. Force close remaining sessions after timeout
        """
        if self._is_shutting_down:
            return

        self._is_shutting_down = True
        logger.info("graceful_shutdown_started")

        # Initiate connection shutdown
        forced = self.conn.initiate_shutdown(timeout=self.shutdown_timeout)

        # Stop the server
        self.shutdown()

        logger.info("graceful_shutdown_complete", forced_sessions=forced)


def create_ssl_context(
    cert_path: Optional[Path] = None,
    key_path: Optional[Path] = None,
) -> Optional[ssl.SSLContext]:
    """Create SSL context for TLS connections."""
    if not cert_path or not key_path:
        return None

    if not cert_path.exists() or not key_path.exists():
        logger.warning("ssl_cert_not_found", cert_path=str(cert_path), key_path=str(key_path))
        return None

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(cert_path), str(key_path))
    logger.info("ssl_context_created", cert_path=str(cert_path))
    return context


def run_server(
    host: str = "0.0.0.0",
    port: int = 5432,
    ssl_cert: Optional[Path] = None,
    ssl_key: Optional[Path] = None,
    shutdown_timeout: float = 30.0,
):
    """Run the PG Wire server with graceful shutdown support."""
    # Initialize metadata database
    metadata_db.initialize()

    ssl_context = create_ssl_context(ssl_cert, ssl_key)

    server_address = (host, port)
    server = WorkspacePGServer(
        server_address,
        ssl_context=ssl_context,
        shutdown_timeout=shutdown_timeout,
    )

    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info("shutdown_signal_received", signal=sig_name)
        # Run shutdown in a thread to avoid blocking signal handler
        threading.Thread(target=server.graceful_shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info(
        "pgwire_server_starting",
        host=host,
        port=port,
        ssl_enabled=ssl_context is not None,
        shutdown_timeout=shutdown_timeout,
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
    finally:
        server.server_close()
        logger.info("server_closed")


def setup_structlog(debug: bool = False):
    """Configure structured logging for PG Wire server."""
    import logging

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer() if not debug else structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.DEBUG if debug else logging.INFO
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Keboola Workspace PostgreSQL Wire Protocol Server"
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5432, help="Port to listen on")
    parser.add_argument("--ssl-cert", type=Path, help="Path to SSL certificate")
    parser.add_argument("--ssl-key", type=Path, help="Path to SSL key")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--shutdown-timeout",
        type=float,
        default=30.0,
        help="Graceful shutdown timeout in seconds",
    )

    args = parser.parse_args()

    # Setup structured logging
    setup_structlog(debug=args.debug)

    run_server(
        host=args.host,
        port=args.port,
        ssl_cert=args.ssl_cert,
        ssl_key=args.ssl_key,
        shutdown_timeout=args.shutdown_timeout,
    )


if __name__ == "__main__":
    main()
