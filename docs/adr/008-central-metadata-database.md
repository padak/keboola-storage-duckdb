# ADR-008: Centralni metadata databaze

## Status

Accepted

## Datum

2024-12-15

## Kontext

DuckDB API Service potrebuje sledovat:

1. **Ktere projekty existuji** - bez skenovani filesystemu
2. **Metadata projektu** - created_at, size, table_count, status
3. **File storage metadata** - nahrada S3 metadata (pro on-prem)
4. **Audit log** - kdo/kdy/co delal (debugging, compliance)
5. **Statistiky** - agregovane metriky bez dotazovani vsech DB

### Problem s filesystem-only pristupem

Puvodni plan predpokladal:
- List projektu = `ls /data/duckdb/*.duckdb`
- Metadata = ???? (nikde)
- File registry = `/data/metadata/files.duckdb` (oddelene)

Problemy:
- Pomalé pro vetsi pocet projektu
- Zadne misto pro metadata (created_at, settings)
- Zadny audit trail
- Nekonzistentni pristup (projekty na FS, files v DB)

### Inspirace z BigQuery driveru

BigQuery driver nema lokalni metadata - GCP je source of truth:
- Projekty jsou GCP projekty (spravovane GCP)
- Metadata jsou v GCP APIs
- Billing, IAM, audit - vse v GCP

Pro on-prem reseni potrebujeme vlastni "control plane".

## Rozhodnuti

**Zavedeme centralni metadata databazi `/data/metadata.duckdb`** jako single source of truth pro:

1. Registry projektu
2. File storage metadata
3. Operations audit log
4. Agregovane statistiky

### Schema

```sql
-- ============================================
-- PROJECTS - registry vsech projektu
-- ============================================
CREATE TABLE projects (
    id VARCHAR PRIMARY KEY,              -- Keboola project ID
    name VARCHAR,                        -- Human-readable name
    db_path VARCHAR NOT NULL,            -- Relativni cesta k .duckdb souboru
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ,
    size_bytes BIGINT DEFAULT 0,         -- Velikost DB souboru
    table_count INTEGER DEFAULT 0,       -- Pocet tabulek
    bucket_count INTEGER DEFAULT 0,      -- Pocet schemat/bucketu
    status VARCHAR DEFAULT 'active',     -- active, deleted, locked
    settings JSON                        -- Project-specific settings
);

-- Index pro rychle vyhledavani
CREATE INDEX idx_projects_status ON projects(status);
CREATE INDEX idx_projects_created ON projects(created_at);

-- ============================================
-- FILES - storage files metadata (nahrada S3)
-- ============================================
CREATE TABLE files (
    id VARCHAR PRIMARY KEY,              -- Unique file ID
    project_id VARCHAR NOT NULL,         -- Reference na projekt
    name VARCHAR NOT NULL,               -- Original filename
    path VARCHAR NOT NULL,               -- Relativni cesta v /data/files/
    size_bytes BIGINT NOT NULL,
    content_type VARCHAR,                -- MIME type
    checksum_md5 VARCHAR,                -- MD5 pro integritu
    checksum_sha256 VARCHAR,             -- SHA256 pro bezpecnost
    is_staged BOOLEAN DEFAULT true,      -- Staging vs permanent
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,              -- Pro staging files
    tags JSON,                           -- Custom metadata

    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE INDEX idx_files_project ON files(project_id);
CREATE INDEX idx_files_staged ON files(is_staged, expires_at);

-- ============================================
-- OPERATIONS_LOG - audit trail
-- ============================================
CREATE TABLE operations_log (
    id BIGINT PRIMARY KEY,               -- Auto-increment v aplikaci
    timestamp TIMESTAMPTZ DEFAULT now(),
    request_id VARCHAR,                  -- X-Request-ID pro tracing
    project_id VARCHAR,
    operation VARCHAR NOT NULL,          -- create_project, import_table, etc.
    resource_type VARCHAR,               -- project, bucket, table, file
    resource_id VARCHAR,
    details JSON,                        -- Operation-specific data
    duration_ms INTEGER,
    status VARCHAR NOT NULL,             -- success, failed, in_progress
    error_message VARCHAR
);

CREATE INDEX idx_ops_project ON operations_log(project_id, timestamp);
CREATE INDEX idx_ops_timestamp ON operations_log(timestamp);
CREATE INDEX idx_ops_request ON operations_log(request_id);

-- ============================================
-- STATS - agregovane statistiky (cache)
-- ============================================
CREATE TABLE stats (
    key VARCHAR PRIMARY KEY,             -- Stat identifier
    value JSON NOT NULL,                 -- Stat value
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Priklad stats:
-- key: 'global', value: {"total_projects": 150, "total_size_bytes": 1234567890}
-- key: 'project_123', value: {"queries_today": 500, "imports_today": 10}
```

### Architektura

```
┌─────────────────────────────────────────────────────────┐
│                    DuckDB API Service                    │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────────┐    ┌───────────────────────────┐  │
│  │  MetadataDB      │    │  ProjectDB Manager        │  │
│  │  (Singleton)     │    │  (Connection per project) │  │
│  │                  │    │                           │  │
│  │  - projects      │    │  project_123.duckdb       │  │
│  │  - files         │    │  project_124.duckdb       │  │
│  │  - ops_log       │    │  ...                      │  │
│  │  - stats         │    │                           │  │
│  └────────┬─────────┘    └─────────────┬─────────────┘  │
│           │                            │                 │
│           ▼                            ▼                 │
│  /data/metadata.duckdb      /data/duckdb/*.duckdb       │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### API Flow priklady

**Create Project:**
```
1. POST /projects {id: "123", name: "My Project"}
2. MetadataDB: INSERT INTO projects (id, name, db_path) VALUES (...)
3. Create file: /data/duckdb/project_123.duckdb
4. MetadataDB: UPDATE projects SET status='active' WHERE id='123'
5. Operations log: {operation: 'create_project', status: 'success'}
```

**List Projects:**
```
1. GET /projects?status=active
2. MetadataDB: SELECT * FROM projects WHERE status='active'
3. Return list (no filesystem scan needed)
```

**Delete Project:**
```
1. DELETE /projects/123
2. MetadataDB: UPDATE projects SET status='deleted'
3. Delete file: /data/duckdb/project_123.duckdb
4. Operations log: {operation: 'drop_project', status: 'success'}
```

## Duvody

### Proc centralni metadata DB?

1. **Rychlost** - dotazy na metadata bez FS skenovani
2. **Konzistence** - vsechna metadata na jednom miste
3. **Audit** - kompletni log operaci pro debugging
4. **Statistiky** - agregovane metriky bez zatizeni projektu
5. **Recovery** - lze rekonstruovat z DB souboru pokud metadata ztracena

### Proc DuckDB pro metadata?

1. **Konzistence** - pouzivame stejnou technologii
2. **Jednoduchost** - zadna dalsi zavislost (Postgres, SQLite)
3. **Vykon** - DuckDB je rychle pro analyticke dotazy
4. **Persistence** - jeden soubor, snadny backup

### Proc ne jen filesystem?

1. Pomalé pro `ls` na tisice souboru
2. Zadna metadata (created_at, size_bytes je na souboru ale table_count ne)
3. Zadny audit trail
4. Atomicita operaci (create project + log = jedna transakce)

## Dusledky

### Pozitivni

- Rychle listovani a vyhledavani projektu
- Kompletni audit trail
- Metadata projektu na jednom miste
- Jednotny pristup k file storage (files tabulka)
- Moznost budoucich rozsireni (quotas, rate limiting)

### Negativni

- Dalsi komponenta k udrzbe
- Potencialni SPOF (ale lze rekonstruovat)
- Synchronizace mezi metadata DB a skutecnymi soubory

### Mitigace rizik

1. **SPOF** - metadata.duckdb lze znovuvytvorit skenovanim /data/duckdb/
2. **Sync** - health check validuje konzistenci
3. **Corruption** - DuckDB ma ACID, pravidelne backupy

## Implementace

### Faze 1: Zaklad
- [ ] MetadataDB class (singleton)
- [ ] Schema inicializace pri startu
- [ ] Projects CRUD operace
- [ ] Integrace do /projects endpointu

### Faze 2: Files
- [ ] Files CRUD operace
- [ ] Staging file management
- [ ] Cleanup expired staging files

### Faze 3: Observability
- [ ] Operations logging
- [ ] Stats agregace
- [ ] Health check consistency validation

## Reference

- ADR-002: Organizace DuckDB souboru
- ADR-006: On-prem storage files
- docs/duckdb-driver-plan.md
