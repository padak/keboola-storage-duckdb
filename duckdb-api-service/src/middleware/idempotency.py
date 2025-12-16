"""Idempotency middleware for HTTP request deduplication.

Ensures that requests with the same X-Idempotency-Key header
return the same response without re-executing the operation.

TTL: 10 minutes (600 seconds)
Scope: POST, PUT, DELETE requests only
"""

import hashlib
import json
import structlog
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from src.database import metadata_db
from src.metrics import (
    IDEMPOTENCY_CACHE_HITS,
    IDEMPOTENCY_CACHE_MISSES,
    IDEMPOTENCY_CACHE_CONFLICTS,
)

logger = structlog.get_logger()

# Methods that support idempotency
IDEMPOTENT_METHODS = {"POST", "PUT", "DELETE"}

# TTL in seconds (10 minutes)
IDEMPOTENCY_TTL_SECONDS = 600

# Header name
IDEMPOTENCY_HEADER = "X-Idempotency-Key"


def compute_request_hash(body: bytes) -> str:
    """Compute SHA-256 hash of request body."""
    return hashlib.sha256(body).hexdigest()


class IdempotencyMiddleware(BaseHTTPMiddleware):
    """
    Middleware that implements idempotent request handling.

    When a request includes an X-Idempotency-Key header:
    1. Check if we've seen this key before (and it hasn't expired)
    2. If yes, return the cached response
    3. If no, execute the request and cache the response

    This prevents duplicate operations from network retries.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Only apply to mutating methods
        if request.method not in IDEMPOTENT_METHODS:
            return await call_next(request)

        # Check for idempotency key header
        idempotency_key = request.headers.get(IDEMPOTENCY_HEADER)

        if not idempotency_key:
            # No idempotency key - execute normally
            return await call_next(request)

        # Get request details
        endpoint = request.url.path
        method = request.method

        # Read request body for hashing
        body = await request.body()
        request_hash = compute_request_hash(body) if body else None

        # Check if we have a cached response for this key
        cached = metadata_db.get_idempotency_key(idempotency_key)

        if cached:
            # Validate that this is the same request (optional safety check)
            if cached["method"] != method:
                logger.warning(
                    "idempotency_method_mismatch",
                    key=idempotency_key[:20] + "...",
                    cached_method=cached["method"],
                    request_method=method,
                )
                IDEMPOTENCY_CACHE_CONFLICTS.inc()
                return JSONResponse(
                    status_code=409,
                    content={
                        "error": "idempotency_conflict",
                        "message": f"Idempotency key was used with {cached['method']}, not {method}",
                    },
                )

            if cached["endpoint"] != endpoint:
                logger.warning(
                    "idempotency_endpoint_mismatch",
                    key=idempotency_key[:20] + "...",
                    cached_endpoint=cached["endpoint"],
                    request_endpoint=endpoint,
                )
                IDEMPOTENCY_CACHE_CONFLICTS.inc()
                return JSONResponse(
                    status_code=409,
                    content={
                        "error": "idempotency_conflict",
                        "message": f"Idempotency key was used with different endpoint",
                    },
                )

            # Optional: validate request body hash
            if request_hash and cached["request_hash"] and cached["request_hash"] != request_hash:
                logger.warning(
                    "idempotency_body_mismatch",
                    key=idempotency_key[:20] + "...",
                )
                IDEMPOTENCY_CACHE_CONFLICTS.inc()
                return JSONResponse(
                    status_code=409,
                    content={
                        "error": "idempotency_conflict",
                        "message": "Idempotency key was used with different request body",
                    },
                )

            # Return cached response
            logger.info(
                "idempotency_cache_hit",
                key=idempotency_key[:20] + "...",
                endpoint=endpoint,
                method=method,
            )
            IDEMPOTENCY_CACHE_HITS.inc()

            return JSONResponse(
                status_code=cached["response_status"],
                content=json.loads(cached["response_body"]) if cached["response_body"] else None,
                headers={IDEMPOTENCY_HEADER: idempotency_key, "X-Idempotency-Replay": "true"},
            )

        # No cached response - execute the request
        # Record cache miss
        IDEMPOTENCY_CACHE_MISSES.inc()

        # We need to restore the body for the actual handler
        # Create a new request with the body we already read

        async def receive():
            return {"type": "http.request", "body": body}

        request._receive = receive

        # Execute the request
        response = await call_next(request)

        # Read the response body to cache it
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        # Store in cache
        try:
            metadata_db.store_idempotency_key(
                key=idempotency_key,
                method=method,
                endpoint=endpoint,
                request_hash=request_hash,
                response_status=response.status_code,
                response_body=response_body.decode("utf-8"),
                ttl_seconds=IDEMPOTENCY_TTL_SECONDS,
            )

            logger.info(
                "idempotency_key_stored",
                key=idempotency_key[:20] + "...",
                endpoint=endpoint,
                method=method,
                status=response.status_code,
            )
        except Exception as e:
            # Don't fail the request if caching fails
            logger.error(
                "idempotency_store_failed",
                key=idempotency_key[:20] + "...",
                error=str(e),
            )

        # Return the response with idempotency header
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )
