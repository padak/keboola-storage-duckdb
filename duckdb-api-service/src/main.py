"""DuckDB Storage API - FastAPI application."""

import asyncio
import logging
import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import time
import uuid

from src.config import settings
from src.routers import backend, buckets, bucket_sharing, projects, tables, metrics
from src.database import metadata_db
from src.middleware.idempotency import IdempotencyMiddleware
from src.middleware.metrics import MetricsMiddleware, normalize_path
from src.metrics import ERROR_COUNT


def setup_logging() -> None:
    """Configure structured logging."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer() if not settings.debug else structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.INFO if not settings.debug else logging.DEBUG
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def cleanup_idempotency_keys_task():
    """Background task to periodically clean up expired idempotency keys."""
    logger = structlog.get_logger()
    # Run cleanup every 5 minutes
    cleanup_interval = 300

    while True:
        try:
            await asyncio.sleep(cleanup_interval)
            count = metadata_db.cleanup_expired_idempotency_keys()
            if count > 0:
                logger.info("idempotency_cleanup_completed", deleted_count=count)
        except asyncio.CancelledError:
            logger.info("idempotency_cleanup_task_cancelled")
            break
        except Exception as e:
            logger.error("idempotency_cleanup_failed", error=str(e))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger = structlog.get_logger()
    logger.info(
        "application_startup",
        version=settings.api_version,
        debug=settings.debug,
        data_dir=str(settings.data_dir),
    )

    # Initialize metadata database
    try:
        metadata_db.initialize()
        logger.info("metadata_db_initialized", path=str(settings.metadata_db_path))
    except Exception as e:
        logger.error("metadata_db_init_failed", error=str(e), exc_info=True)
        raise

    # Start background cleanup task
    cleanup_task = asyncio.create_task(cleanup_idempotency_keys_task())
    logger.info("idempotency_cleanup_task_started")

    yield

    # Cancel cleanup task on shutdown
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

    logger.info("application_shutdown")


# Setup logging before creating app
setup_logging()
logger = structlog.get_logger()

# Create FastAPI application
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description="""
DuckDB Storage API for Keboola Connection.

This service provides a REST API for managing DuckDB-based storage:
- Backend initialization and health checks
- Project management (create/drop DuckDB files)
- Bucket management (schemas)
- Table operations (CRUD, import/export)
- Workspace management

Part of the on-premise Keboola setup without cloud dependencies.
    """,
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add idempotency middleware (for POST/PUT/DELETE deduplication)
app.add_middleware(IdempotencyMiddleware)

# Add metrics middleware (for Prometheus request instrumentation)
app.add_middleware(MetricsMiddleware)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """Log all requests with timing and request ID."""
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)

    start_time = time.perf_counter()

    logger.info(
        "request_started",
        method=request.method,
        path=request.url.path,
    )

    response = await call_next(request)

    duration_ms = (time.perf_counter() - start_time) * 1000

    logger.info(
        "request_completed",
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=round(duration_ms, 2),
    )

    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions."""
    # Normalize path for metrics
    endpoint = normalize_path(request.url.path)

    # Determine error type
    error_type = type(exc).__name__

    # Record error metric
    ERROR_COUNT.labels(type=error_type, endpoint=endpoint).inc()

    logger.error(
        "unhandled_exception",
        method=request.method,
        path=request.url.path,
        error=str(exc),
        error_type=error_type,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_server_error",
            "message": str(exc) if settings.debug else "An internal error occurred",
        },
    )


# Include routers
app.include_router(backend.router)
app.include_router(projects.router)
app.include_router(buckets.router)
app.include_router(bucket_sharing.router)
app.include_router(tables.router)
app.include_router(metrics.router)

# Root endpoint
@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint - redirects to health check."""
    return {
        "service": settings.api_title,
        "version": settings.api_version,
        "health": "/health",
        "docs": "/docs" if settings.debug else None,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )
