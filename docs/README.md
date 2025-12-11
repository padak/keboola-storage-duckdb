# DuckDB Storage Backend - Dokumentace

> On-premise Keboola bez Snowflake a bez S3

## Jak cist tuto dokumentaci

### 1. Zacni zde: Puvodni zadani

**[zajca.md](zajca.md)** - Puvodni pozadavky od Zajcy. Kratky soubor, ktery vysvetluje PROC to delame a na co navazujeme (BigQuery driver jako reference).

### 2. Pochop DuckDB

**[duckdb-technical-research.md](duckdb-technical-research.md)** - Technicky vyzkum DuckDB. Obsahuje:
- Jak funguje import souboru v Keboola
- Kompletni seznam 33 povinnnych + 2 volitelnych driver operaci
- DuckDB limity a concurrency model (single-writer!)
- ATTACH pro cross-database JOINy
- Doporuceni pro organizaci souboru

Cti toto, pokud chces pochopit TECHNICKE OMEZENI DuckDB.

### 3. Pochop Keboola features

**[duckdb-keboola-features.md](duckdb-keboola-features.md)** - Mapovani Keboola features na DuckDB. Obsahuje:
- Bucket sharing a linked buckets
- Dev branches
- Snapshoty a time-travel
- Query Service a write serialization
- Python integrace a AI agenti

Cti toto, pokud chces pochopit JAK KEBOOLA FUNGUJE a jak to namapovat na DuckDB.

### 4. Pochop Storage API

**[duckdb-api-endpoints.md](duckdb-api-endpoints.md)** - Seznam vsech Storage API endpointu, ktere driver musi podporovat. Rozdeleno na:
- Endpointy ktere resi DRIVER (buckety, tabulky, workspaces...)
- Endpointy ktere resi CONNECTION (tokeny, eventy, joby...)
- Mapovani na BigQuery driver commands

Cti toto, pokud chces vedet CO PRESNE MUSI DRIVER IMPLEMENTOVAT.

### 5. Architektonicka rozhodnuti (ADR)

Adresar **[adr/](adr/)** obsahuje zaznam architektonickych rozhodnuti:

| ADR | Soubor | Rozhodnuti |
|-----|--------|------------|
| 001 | [001-duckdb-microservice-architecture.md](adr/001-duckdb-microservice-architecture.md) | Python microservice misto PHP FFI |
| 002 | [002-duckdb-file-organization.md](adr/002-duckdb-file-organization.md) | 1 projekt = 1 DuckDB soubor |
| 003 | [003-duckdb-branch-strategy.md](adr/003-duckdb-branch-strategy.md) | Dev branches = separate soubory |
| 004 | [004-duckdb-snapshots.md](adr/004-duckdb-snapshots.md) | Snapshoty = Parquet export |
| 005 | [005-duckdb-write-serialization.md](adr/005-duckdb-write-serialization.md) | Write queue pro serializaci |
| 006 | [006-duckdb-on-prem-storage.md](adr/006-duckdb-on-prem-storage.md) | Storage Files = lokalni FS |

Cti ADR, pokud chces pochopit PROC jsme se rozhodli tak, jak jsme se rozhodli.

### 6. Implementacni plan

**[duckdb-driver-plan.md](duckdb-driver-plan.md)** - Hlavni implementacni plan. Obsahuje:
- Architektura (ASCII diagram)
- Vsech 35 driver commands s prirazenim k fazim
- Struktura Python API Service
- Struktura PHP driveru v Connection
- 12 implementacnich fazi
- Kompletni API endpointy
- Technologie a zavislosti

**TOTO JE HLAVNI DOKUMENT** - pouzivej ho jako checklist pri implementaci.

---

## Doporucene poradi cteni

```
Pro pochopeni projektu:
zajca.md -> duckdb-technical-research.md -> duckdb-keboola-features.md

Pro pochopeni rozhodnuti:
adr/001-*.md -> adr/002-*.md -> ... -> adr/006-*.md

Pro implementaci:
duckdb-driver-plan.md (hlavni reference)
duckdb-api-endpoints.md (co implementovat)
```

---

## Ostatni soubory

| Soubor | Popis |
|--------|-------|
| [time-tracker.md](time-tracker.md) | Sledovani casu straveneho na projektu |

---

## Architektura (TL;DR)

```
┌─────────────────────────────────────────────────────────────────┐
│                      ON-PREMISE KEBOOLA                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Keboola Connection (PHP)  ◄──REST──►  DuckDB API Service (Py)  │
│  - Thin HTTP client                    - FastAPI                │
│  - Credentials                         - Write Queue            │
│                                        - All handlers           │
│                                                 │               │
│                                                 ▼               │
│                          LOCAL FILESYSTEM                       │
│                          /data/duckdb/*.duckdb                  │
│                          /data/files/*                          │
│                          /data/snapshots/*                      │
└─────────────────────────────────────────────────────────────────┘
```

**Klicove rozhodnuti:**
- DuckDB bezi v Python microservice (ne PHP FFI)
- 1 Keboola projekt = 1 DuckDB soubor
- Dev branches = separate DuckDB soubory
- Snapshoty = Parquet export
- Write operace serializovany pres async frontu
- Storage Files na lokalnim filesystem (ne S3)
