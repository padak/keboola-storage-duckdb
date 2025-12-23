# DuckDB Storage Backend - Dokumentace

> On-premise Keboola bez Snowflake a bez S3

## Aktualni stav: MVP COMPLETE

**590 testu** | **93 API endpointu** | **35 gRPC handleru**

---

## Hlavni dokumentace

### 1. Implementacni plan (START HERE)

**[plan/README.md](plan/README.md)** - Hlavni implementacni plan s fazemi 1-15

| Faze | Stav | Popis |
|------|------|-------|
| 1-11 | DONE | Core API (projekty, buckety, tabulky, snapshoty, branche, workspaces) |
| 12 | DONE | Connection Integration (gRPC, S3 API) |
| 13 | DONE | Observability (Prometheus metriky) |
| 14 | PROPOSAL | Backend Plugin Architecture |
| 15 | TODO | Comprehensive E2E Test Suite |

### 2. API Reference

| Soubor | Popis |
|--------|-------|
| **[api/duckapi.json](api/duckapi.json)** | OpenAPI specifikace (93 endpointu) |
| [api/keboola.apib](api/keboola.apib) | Keboola Storage API Blueprint (reference) |
| [api/keboolamanagementapi.apib](api/keboolamanagementapi.apib) | Keboola Management API Blueprint (reference) |

### 3. Architecture Decision Records (ADR)

**[adr/](adr/)** - 15 architektonickych rozhodnuti

| ADR | Rozhodnuti | Stav |
|-----|------------|------|
| [001](adr/001-duckdb-microservice-architecture.md) | Python microservice (ne PHP FFI) | IMPLEMENTED |
| [002](adr/002-duckdb-file-organization.md) | 1 projekt = 1 DuckDB soubor | SUPERSEDED by 009 |
| [003](adr/003-duckdb-branch-strategy.md) | Dev branches = separate soubory | SUPERSEDED by 007 |
| [004](adr/004-duckdb-snapshots.md) | Snapshoty = Parquet export | IMPLEMENTED |
| [005](adr/005-duckdb-write-serialization.md) | Write queue pro serializaci | IMPLEMENTED |
| [006](adr/006-duckdb-on-prem-storage.md) | Storage Files = lokalni FS | IMPLEMENTED |
| [007](adr/007-duckdb-cow-branching.md) | Copy-on-Write branching | APPROVED (post-MVP) |
| [008](adr/008-central-metadata-database.md) | Centralni metadata.duckdb | IMPLEMENTED |
| **[009](adr/009-duckdb-file-per-table.md)** | **1 tabulka = 1 DuckDB soubor** | **IMPLEMENTED** |
| [010](adr/010-duckdb-sql-interface.md) | PG Wire SQL interface | IMPLEMENTED |
| [011](adr/011-apache-arrow-integration.md) | Apache Arrow integrace | DEFERRED |
| **[012](adr/012-branch-first-api-design.md)** | **Branch-First API design** | **IMPLEMENTED** |
| [013](adr/013-sql-object-naming-convention.md) | SQL naming conventions | IMPLEMENTED |
| **[014](adr/014-grpc-driver-interface.md)** | **gRPC driver interface** | **IMPLEMENTED** |
| [015](adr/015-observability-and-operation-tracking.md) | Observability & metriky | IMPLEMENTED |

**Klicove ADR:** 009 (per-table files), 012 (branch-first API), 014 (gRPC)

### 4. Dalsi dokumenty

| Soubor | Popis |
|--------|-------|
| [plan/decisions.md](plan/decisions.md) | Vsechna schvalena rozhodnuti |
| [plan/risks.md](plan/risks.md) | Akceptovana rizika pro MVP |
| [connection-duckdb-patch.md](connection-duckdb-patch.md) | Zmeny v Connection pro DuckDB |
| [local-connection.md](local-connection.md) | Navod na lokalni Connection setup |

### 5. Puvodni vyzkum (historicke)

| Soubor | Popis |
|--------|-------|
| [zajca.md](zajca.md) | Puvodni zadani od Zajcy |
| [duckdb-technical-research.md](duckdb-technical-research.md) | Technicky vyzkum DuckDB |
| [duckdb-keboola-features.md](duckdb-keboola-features.md) | Mapovani Keboola features |
| [duckdb-api-endpoints.md](duckdb-api-endpoints.md) | Puvodni seznam API endpointu |
| [bigquery-driver-research.md](bigquery-driver-research.md) | Analyza BigQuery driveru |

---

## Architektura

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           ON-PREMISE KEBOOLA                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Keboola Connection (PHP)                                                    │
│  │                                                                           │
│  ├── Storage API (/v2/storage/*)                                             │
│  │        │                                                                  │
│  │        │  gRPC (Protocol Buffers)                                         │
│  │        ▼                                                                  │
│  └── DuckDB Driver ──────────────────────────────┐                           │
│       └── DuckdbDriverClient                     │                           │
│                                                  │ HTTP REST/JSON            │
│                                                  ▼                           │
│                                       ┌─────────────────────────┐            │
│                                       │  DuckDB API Service     │            │
│                                       │  (Python FastAPI)       │            │
│                                       ├─────────────────────────┤            │
│                                       │  - REST API (port 8000) │            │
│                                       │  - gRPC (port 50051)    │            │
│                                       │  - PG Wire (port 5432)  │            │
│                                       │  - S3 API (/s3/*)       │            │
│                                       │  - Prometheus /metrics  │            │
│                                       └───────────┬─────────────┘            │
│                                                   │                          │
│                                                   ▼                          │
│                                       ┌─────────────────────────┐            │
│                                       │   LOCAL FILESYSTEM      │            │
│                                       ├─────────────────────────┤            │
│                                       │  /data/duckdb/          │            │
│                                       │  ├── project_X/         │            │
│                                       │  │   └── bucket/        │            │
│                                       │  │       └── table.duckdb│           │
│                                       │  └── metadata.duckdb    │            │
│                                       │                         │            │
│                                       │  /data/files/           │            │
│                                       │  └── project_X/...      │            │
│                                       └─────────────────────────┘            │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Klicova rozhodnuti:**
- **1 tabulka = 1 DuckDB soubor** (ADR-009) - parallel writes, easy branching
- **Branch-First API** (ADR-012) - `/branches/{branch_id}/buckets/...`
- **gRPC + HTTP bridge** (ADR-014) - flexibilni integrace s Connection
- **PG Wire** - SQL pristup pres PostgreSQL klienty (DBeaver, psql, psycopg2)
- **S3-Compatible API** - pre-signed URLs pro file upload/download

---

## Quick Start

```bash
# Spustit DuckDB API Service
cd duckdb-api-service
source .venv/bin/activate
python -m src.unified_server  # REST + gRPC + PG Wire

# Spustit testy
pytest tests/ -v

# Otevrit metriky dashboard
open dashboard2.html
```

---

## Post-MVP TODO

Viz [CLAUDE.md](../CLAUDE.md) sekce "Post-MVP TODO & Technical Debt":
- Phase 15: E2E Test Suite
- CoW branching (ADR-007)
- HA / Multi-instance
- Key rotation, RBAC
