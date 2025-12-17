# Decisions

All architectural and implementation decisions for DuckDB Storage Backend.

## Architecture (ADR)

| ADR | Decision | Status |
|-----|----------|--------|
| 001 | Python microservice instead of PHP FFI driver | **Accepted** |
| 002 | ~~1 project = 1 DuckDB file, bucket = schema~~ | **Superseded by 009** |
| 003 | Dev branches = separate DuckDB files | Superseded by 007 |
| 004 | Snapshots = Parquet export | **Accepted** |
| 005 | ~~Write serialization = async queue per project~~ | **Simplified by 009** |
| 006 | Storage Files = local filesystem + metadata in DuckDB | **Accepted** |
| 007 | Copy-on-Write branching = lazy table-level copy | **Accepted** |
| 008 | Central metadata database (`metadata.duckdb`) | **Accepted** |
| **009** | **1 DuckDB file per table** | **Accepted (2024-12-16)** |

## Critical Decisions (APPROVED)

| Area | Decision | Value |
|------|----------|-------|
| **Write Queue** | Queue durability | In-memory |
| **Write Queue** | Max queue size | 1000 (configurable) |
| **Write Queue** | Priority levels | Normal + High |
| **Import/Export** | Staging location | `_staging/{uuid}.duckdb` |
| **Import/Export** | Deduplication | INSERT ON CONFLICT |
| **Import/Export** | Incremental mode | Full MERGE (INSERT/UPDATE/DELETE) |
| **Files API** | Upload mechanism | Multipart POST |
| **Files API** | Staging TTL | 24 hours |
| **Files API** | Max file size | 10GB (configurable) |
| **Files API** | File quotas | 10000 files, 1TB per project |
| **Snapshots** | Manual retention | 90 days |
| **Snapshots** | Auto retention | 7 days |
| **Snapshots** | Auto triggers | Per-project configurable, default DROP TABLE only |

## Security Decisions (APPROVED)

| Decision | Value |
|----------|-------|
| Auth model | Hierarchical API keys (ADMIN > PROJECT) |
| Key storage | SHA256 hash in metadata.duckdb |
| API key format | `admin_*` for admin, `proj_{id}_admin_*` for project |

## Deferred Decisions

| Area | Decision | Reason |
|------|----------|--------|
| Rate limiting | Per project | Post-MVP |
| Encryption at rest | Optional AES-256 | Post-MVP, use LUKS |
| mTLS | Client certificates | Post-MVP |
| Key rotation | Automatic | Post-MVP |

## Auth Model

```
ADMIN_API_KEY (ENV variable)
├── Endpoints: POST /projects, GET /projects, /backend/*
└── Access: ALL projects

PROJECT_ADMIN_API_KEY (returned on POST /projects)
├── Format: proj_{project_id}_admin_{random}
├── Storage: SHA256 hash in metadata.duckdb
└── Endpoints: Everything in /projects/{id}/*
```
