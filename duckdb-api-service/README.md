# DuckDB Storage API Service

FastAPI microservice providing REST API for DuckDB-based Keboola Storage backend.

## Features

- **Project Management** - Create, list, update, delete projects
- **Bucket Operations** - CRUD for buckets (DuckDB schemas)
- **Table Operations** - CRUD with preview, primary key support
- **Bucket Sharing** - Share, link, grant readonly access between projects
- **Authentication** - Hierarchical API key model (admin + project keys)
- **Per-table Files** - ADR-009 architecture (1 DuckDB file per table)

## Quick Start

### Prerequisites

- Python 3.11+
- Virtual environment

### Installation

```bash
cd duckdb-api-service

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create `.env` file or set environment variables:

```bash
# Required for production
ADMIN_API_KEY=your_secure_admin_key_here

# Optional (defaults shown)
DATA_DIR=./data
DEBUG=true
HOST=0.0.0.0
PORT=8000
DUCKDB_THREADS=4
DUCKDB_MEMORY_LIMIT=4GB
```

### Running

```bash
# Development (with hot reload)
python -m src.main

# Or with uvicorn directly
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

### Docker

```bash
docker compose up --build
```

## Authentication

The API uses a hierarchical API key model:

### Admin API Key

Set via `ADMIN_API_KEY` environment variable. Required for:
- `POST /projects` - Create new projects
- `GET /projects` - List all projects
- `POST /backend/init` - Initialize backend
- `POST /backend/remove` - Remove backend

### Project API Key

Returned when creating a project. Use for all project-scoped operations:
- All `/projects/{id}/*` endpoints
- Bucket and table operations within the project

**Important:** The project API key is shown only once at creation time. Store it securely!

### Usage Examples

```bash
# Set admin key
export ADMIN_API_KEY="your_admin_key"

# Create a project (returns project API key)
curl -X POST http://localhost:8000/projects \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"id": "123", "name": "My Project"}'

# Response includes api_key - SAVE THIS!
# {
#   "id": "123",
#   "name": "My Project",
#   "api_key": "proj_123_admin_a1b2c3d4e5f6..."
# }

# Use project key for project operations
export PROJECT_KEY="proj_123_admin_a1b2c3d4e5f6..."

# Create a bucket
curl -X POST http://localhost:8000/projects/123/buckets \
  -H "Authorization: Bearer $PROJECT_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "in_c_sales"}'

# Create a table
curl -X POST http://localhost:8000/projects/123/buckets/in_c_sales/tables \
  -H "Authorization: Bearer $PROJECT_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "columns": [
      {"name": "id", "type": "INTEGER", "nullable": false},
      {"name": "customer", "type": "VARCHAR"},
      {"name": "amount", "type": "DECIMAL(10,2)"}
    ],
    "primary_key": ["id"]
  }'

# Preview table data
curl http://localhost:8000/projects/123/buckets/in_c_sales/tables/orders/preview \
  -H "Authorization: Bearer $PROJECT_KEY"

# Admin can also access any project
curl http://localhost:8000/projects/123/buckets \
  -H "Authorization: Bearer $ADMIN_API_KEY"
```

## API Endpoints

### Public

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |

### Admin Only

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/backend/init` | Initialize storage directories |
| POST | `/backend/remove` | Remove backend (no-op) |
| POST | `/projects` | Create project (returns API key) |
| GET | `/projects` | List all projects |

### Project Access (Admin or Project Key)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/projects/{id}` | Get project info |
| PUT | `/projects/{id}` | Update project |
| DELETE | `/projects/{id}` | Delete project |
| GET | `/projects/{id}/stats` | Get live statistics |
| GET | `/projects/{id}/buckets` | List buckets |
| POST | `/projects/{id}/buckets` | Create bucket |
| GET | `/projects/{id}/buckets/{name}` | Get bucket |
| DELETE | `/projects/{id}/buckets/{name}` | Delete bucket |
| POST | `/projects/{id}/buckets/{name}/share` | Share bucket |
| DELETE | `/projects/{id}/buckets/{name}/share` | Unshare bucket |
| POST | `/projects/{id}/buckets/{name}/link` | Link bucket |
| DELETE | `/projects/{id}/buckets/{name}/link` | Unlink bucket |
| POST | `/projects/{id}/buckets/{name}/grant-readonly` | Grant readonly |
| DELETE | `/projects/{id}/buckets/{name}/grant-readonly` | Revoke readonly |
| GET | `/projects/{id}/buckets/{bucket}/tables` | List tables |
| POST | `/projects/{id}/buckets/{bucket}/tables` | Create table |
| GET | `/projects/{id}/buckets/{bucket}/tables/{table}` | Get table info |
| DELETE | `/projects/{id}/buckets/{bucket}/tables/{table}` | Delete table |
| GET | `/projects/{id}/buckets/{bucket}/tables/{table}/preview` | Preview data |

## Testing

```bash
# Activate virtual environment
source .venv/bin/activate

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_projects.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Architecture (ADR-009)

Each table is stored as a separate DuckDB file:

```
/data/
├── metadata.duckdb              # Central metadata database
├── duckdb/
│   └── project_123/             # Project = directory
│       ├── in_c_sales/          # Bucket = directory
│       │   ├── orders.duckdb    # Table = file
│       │   └── customers.duckdb
│       └── out_c_reports/
│           └── summary.duckdb
├── files/                       # Storage files (future)
└── snapshots/                   # Table snapshots (future)
```

**Benefits:**
- Parallel writes to different tables
- Simple per-table locking (no project-level queue)
- Natural copy-on-write for dev branches
- Industry standard (Delta Lake, Iceberg pattern)

## Development

### Project Structure

```
duckdb-api-service/
├── src/
│   ├── main.py           # FastAPI app, middleware
│   ├── config.py         # Settings (pydantic-settings)
│   ├── database.py       # MetadataDB + ProjectDBManager
│   ├── auth.py           # API key generation/verification
│   ├── dependencies.py   # FastAPI auth dependencies
│   ├── models/
│   │   └── responses.py  # Pydantic response models
│   └── routers/
│       ├── backend.py    # /health, /backend/*
│       ├── projects.py   # /projects
│       ├── buckets.py    # /projects/{id}/buckets
│       ├── bucket_sharing.py  # share, link, readonly
│       └── tables.py     # /projects/{id}/buckets/{b}/tables
├── tests/
│   ├── conftest.py       # Pytest fixtures
│   ├── test_auth.py      # Auth unit tests
│   ├── test_api_keys.py  # API key storage tests
│   ├── test_backend.py
│   ├── test_projects.py
│   ├── test_buckets.py
│   ├── test_bucket_sharing.py
│   ├── test_tables.py
│   └── test_table_lock.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── pytest.ini
```

### Adding New Endpoints

1. Create or update router in `src/routers/`
2. Add Pydantic models in `src/models/responses.py`
3. Include router in `src/main.py`
4. Add auth dependency: `dependencies=[Depends(require_project_access)]`
5. Write tests in `tests/`

## License

Proprietary - Keboola
