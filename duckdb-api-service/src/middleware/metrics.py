"""Prometheus metrics middleware for HTTP request instrumentation.

Collects HTTP request metrics:
- Request count by method, endpoint, status code
- Request duration histogram
- In-flight requests gauge
"""

import time
import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from src.metrics import (
    REQUEST_COUNT,
    REQUEST_DURATION,
    REQUEST_IN_FLIGHT,
)

logger = structlog.get_logger()


def normalize_path(path: str) -> str:
    """
    Normalize path for metrics labels to avoid high cardinality.

    Replaces dynamic path segments (UUIDs, IDs) with placeholders.

    Examples:
        /projects/abc123 -> /projects/{project_id}
        /projects/abc123/buckets/my_bucket -> /projects/{project_id}/buckets/{bucket_name}
        /projects/abc123/buckets/my_bucket/tables/orders ->
            /projects/{project_id}/buckets/{bucket_name}/tables/{table_name}
    """
    parts = path.strip("/").split("/")
    normalized = []

    i = 0
    while i < len(parts):
        part = parts[i]

        if part == "projects" and i + 1 < len(parts):
            normalized.append("projects")
            normalized.append("{project_id}")
            i += 2
            continue

        if part == "buckets" and i + 1 < len(parts):
            normalized.append("buckets")
            normalized.append("{bucket_name}")
            i += 2
            continue

        if part == "tables" and i + 1 < len(parts):
            normalized.append("tables")
            normalized.append("{table_name}")
            i += 2
            continue

        if part == "snapshots" and i + 1 < len(parts):
            normalized.append("snapshots")
            normalized.append("{snapshot_id}")
            i += 2
            continue

        if part == "branches" and i + 1 < len(parts):
            normalized.append("branches")
            normalized.append("{branch_id}")
            i += 2
            continue

        if part == "workspaces" and i + 1 < len(parts):
            normalized.append("workspaces")
            normalized.append("{workspace_id}")
            i += 2
            continue

        if part == "files" and i + 1 < len(parts):
            next_part = parts[i + 1]
            # Keep special endpoints like "prepare", "upload"
            if next_part in ("prepare", "upload"):
                normalized.append("files")
                normalized.append(next_part)
                i += 2
                continue
            else:
                normalized.append("files")
                normalized.append("{file_id}")
                i += 2
                continue

        normalized.append(part)
        i += 1

    return "/" + "/".join(normalized) if normalized else "/"


class MetricsMiddleware(BaseHTTPMiddleware):
    """
    Middleware that collects Prometheus metrics for HTTP requests.

    Metrics collected:
    - duckdb_api_requests_total: Counter by method, endpoint, status_code
    - duckdb_api_request_duration_seconds: Histogram by method, endpoint
    - duckdb_api_requests_in_flight: Gauge by method
    """

    # Endpoints to skip (internal/debug endpoints)
    SKIP_PATHS = {"/metrics", "/docs", "/redoc", "/openapi.json"}

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        method = request.method

        # Skip metrics for internal/debug endpoints
        if request.url.path in self.SKIP_PATHS:
            return await call_next(request)

        # Normalize path for consistent labels
        endpoint = normalize_path(request.url.path)

        # Track in-flight requests
        REQUEST_IN_FLIGHT.labels(method=method).inc()

        # Measure request duration
        start_time = time.perf_counter()

        try:
            response = await call_next(request)
            status_code = str(response.status_code)
        except Exception:
            status_code = "500"
            raise
        finally:
            # Record duration
            duration = time.perf_counter() - start_time
            REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)

            # Record request count
            REQUEST_COUNT.labels(
                method=method,
                endpoint=endpoint,
                status_code=status_code
            ).inc()

            # Decrement in-flight
            REQUEST_IN_FLIGHT.labels(method=method).dec()

        return response
