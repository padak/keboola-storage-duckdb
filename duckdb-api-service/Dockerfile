# DuckDB Storage API Service
FROM python:3.12-slim

LABEL maintainer="Keboola"
LABEL description="DuckDB Storage API for Keboola Connection"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user
RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/

# Copy proto definitions and generated code
COPY proto/ ./proto/
COPY generated/ ./generated/

# Create data directories
RUN mkdir -p /data/duckdb /data/files /data/snapshots /data/metadata && \
    chown -R appuser:appgroup /data

# Switch to non-root user
USER appuser

# Expose ports (REST API + PG Wire + gRPC)
EXPOSE 8000 5432 50051

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/health', timeout=5).raise_for_status()"

# Run the unified server (REST + gRPC)
CMD ["python", "-m", "src.unified_server"]
