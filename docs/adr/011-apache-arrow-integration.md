# ADR-011: Apache Arrow Integration pro Agent Interface

## Status

**Proposed** - RFC pro budouci implementaci

Inspirovano: [TextQL Sandcastles](https://textql.com/blog/sandcastles) - purpose-built analytical sandbox s Arrow streaming

## Datum

2024-12-18 (proposed)

## Kontext

### Problem

Soucasny import/export v Keboola Storage v3 pouziva:
- **CSV** pro file-based import/export
- **JSON** pro API responses (preview, query results)

Pro AI/agent workloads je to neefektivni:

| Metrika (100k rows) | JSON | Arrow IPC | Rozdil |
|---------------------|------|-----------|--------|
| Velikost | 8.5 MB | 2.7 MB | **3.2x mensi** |
| Serializace | 151 ms | 0.7 ms | **201x rychlejsi** |
| Type preservation | Ztraceno | Zachovano | - |
| Zero-copy deserializace | Ne | Ano | - |

### Proc je to dulezite pro AI agenty?

TextQL ve svem blogu identifikoval klicove problemy:

1. **LLM context windows** - data musi opustit kontext a byt zpracovana externe
2. **Iterativni analyza** - agent potrebuje cachovat mezivysledky
3. **Heterogenni zdroje** - ruzne formaty a dialekty
4. **Enterprise volumes** - GB-TB datasety

> "LLMs are very fluent at SQL when the schema fits in context—say, under 25 tables. So our architecture leans into that strength: we can offload heavy computation to DuckDB or Iceberg environments on-demand."
> — TextQL Sandcastles blog

### DuckDB + Arrow kompatibilita

DuckDB ma **nativni zero-copy Arrow support**:

```python
import duckdb
import pyarrow as pa

conn = duckdb.connect("table.duckdb")

# Export: DuckDB -> Arrow (zero-copy)
arrow_table = conn.execute("SELECT * FROM data").fetch_arrow_table()

# Import: Arrow -> DuckDB query (zero-copy scan)
external_arrow = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
result = conn.execute("SELECT * FROM external_arrow WHERE value > 15").fetchall()
```

**Overeno benchmarkem:**
- DuckDB 1.4.3 + PyArrow 22.0.0
- 100k rows export: 2.0 ms
- Arrow IPC serialization: 0.7 ms (2.7 MB)
- JSON serialization: 151 ms (8.5 MB)

## Rozhodnuti

**Pridat Apache Arrow jako alternativni wire format pro API responses.**

### 1. Arrow Export Endpoint

```
GET /projects/{id}/buckets/{bucket}/tables/{table}/export?format=arrow
Accept: application/vnd.apache.arrow.stream

Response: Binary Arrow IPC stream
```

### 2. Arrow Query Endpoint (pro workspaces)

```
POST /projects/{id}/workspaces/{ws_id}/query
Content-Type: application/json
Accept: application/vnd.apache.arrow.stream

{
  "sql": "SELECT * FROM orders WHERE amount > 100",
  "format": "arrow"
}

Response: Binary Arrow IPC stream
```

### 3. Arrow Import Endpoint

```
POST /projects/{id}/buckets/{bucket}/tables/{table}/import
Content-Type: application/vnd.apache.arrow.stream

Body: Binary Arrow IPC stream
```

## Implementace

### Server-side (FastAPI)

```python
from fastapi import Response
from fastapi.responses import StreamingResponse
import pyarrow as pa
import pyarrow.ipc as ipc

ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"

@router.get("/projects/{project_id}/buckets/{bucket}/tables/{table}/export")
async def export_table(
    project_id: int,
    bucket: str,
    table: str,
    format: str = "csv",  # csv | json | arrow
):
    conn = get_table_connection(project_id, bucket, table)

    if format == "arrow":
        arrow_table = conn.execute("SELECT * FROM data").fetch_arrow_table()

        sink = pa.BufferOutputStream()
        with ipc.RecordBatchStreamWriter(sink, arrow_table.schema) as writer:
            writer.write_table(arrow_table)

        return Response(
            content=sink.getvalue().to_pybytes(),
            media_type=ARROW_MEDIA_TYPE,
            headers={
                "Content-Disposition": f"attachment; filename={table}.arrow",
                "X-Arrow-Schema": arrow_table.schema.to_string(),
            }
        )

    # ... existing CSV/JSON handling
```

### Streaming pro velke datasety

```python
@router.get("/projects/{project_id}/buckets/{bucket}/tables/{table}/export/stream")
async def export_table_stream(
    project_id: int,
    bucket: str,
    table: str,
    chunk_size: int = 100_000,
):
    """Stream large tables as Arrow RecordBatches."""

    async def generate():
        conn = get_table_connection(project_id, bucket, table)
        rel = conn.sql("SELECT * FROM data")
        reader = rel.arrow()  # RecordBatchReader

        # First batch includes schema
        for batch in reader:
            sink = pa.BufferOutputStream()
            with ipc.RecordBatchStreamWriter(sink, batch.schema) as writer:
                writer.write_batch(batch)
            yield sink.getvalue().to_pybytes()

    return StreamingResponse(generate(), media_type=ARROW_MEDIA_TYPE)
```

### Client-side (Python Agent)

```python
import httpx
import pyarrow.ipc as ipc
import duckdb

class KeboolaArrowClient:
    """High-performance client for AI/agent workloads."""

    def __init__(self, base_url: str, api_key: str):
        self.client = httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"}
        )

    def export_arrow(self, project_id: int, bucket: str, table: str) -> pa.Table:
        """Export table as Arrow - 200x faster than JSON."""
        response = self.client.get(
            f"/projects/{project_id}/buckets/{bucket}/tables/{table}/export",
            params={"format": "arrow"},
            headers={"Accept": "application/vnd.apache.arrow.stream"}
        )
        response.raise_for_status()
        reader = ipc.open_stream(response.content)
        return reader.read_all()

    def query_arrow(self, project_id: int, workspace_id: str, sql: str) -> pa.Table:
        """Execute SQL in workspace, return Arrow."""
        response = self.client.post(
            f"/projects/{project_id}/workspaces/{workspace_id}/query",
            json={"sql": sql, "format": "arrow"},
            headers={"Accept": "application/vnd.apache.arrow.stream"}
        )
        response.raise_for_status()
        return ipc.open_stream(response.content).read_all()

    def to_local_duckdb(self, arrow_table: pa.Table, conn: duckdb.DuckDBPyConnection):
        """Load Arrow table into local DuckDB (zero-copy)."""
        # DuckDB can query Arrow directly without copying
        return conn.execute("SELECT * FROM arrow_table").fetch_arrow_table()
```

## Agent Workflow Pattern

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Keboola Storage API                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │ orders.duckdb│  │customers.duckdb│ │products.duckdb│              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                 │
│         │                │                │                         │
│         └────────────────┼────────────────┘                         │
│                          │                                          │
│                    Arrow IPC Stream                                 │
│                    (3x smaller, 200x faster)                        │
└──────────────────────────┼──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Agent Sandbox                                  │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ Local DuckDB (in-memory)                                      │   │
│  │                                                               │   │
│  │ -- Zero-copy query on Arrow data                              │   │
│  │ SELECT o.*, c.name                                            │   │
│  │ FROM orders_arrow o                                           │   │
│  │ JOIN customers_arrow c ON o.customer_id = c.id                │   │
│  │                                                               │   │
│  │ -- Cache results locally for iteration                        │   │
│  │ CREATE TABLE analysis AS SELECT ...                           │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  Agent can:                                                          │
│  - Run complex SQL on cached Arrow data                              │
│  - Join multiple tables locally (no more API calls)                  │
│  - Build derived datasets iteratively                                │
│  - Work with GB-scale data outside LLM context                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Duvody

### Proc Arrow?

1. **Nativni DuckDB podpora** - zero-copy v obou smerech
2. **Industry standard** - Spark, Pandas, Polars, DataFusion vse podporuje Arrow
3. **Type preservation** - timestamps, decimals, nested types zachovany
4. **Columnar format** - efektivni pro analyticke workloads
5. **Zero-copy IPC** - data se nekopiruji pri deserializaci

### Proc ne Parquet?

- Parquet je storage format (kompresi, metadata)
- Arrow je memory/wire format (rychlost, zero-copy)
- Pro API responses je Arrow lepsi (nizsi latence)
- Pro file export lze nabidnout obe

### Proc ne gRPC/Flight?

- Arrow Flight je komplexnejsi setup (vlastni protocol)
- Pro MVP staci Arrow IPC pres HTTP
- Flight lze pridat pozdeji pro streaming use cases

## Dusledky

### Pozitivni

- **200x rychlejsi serializace** (vs JSON)
- **3x mensi payload** (vs JSON)
- **Type-safe** - zadne "timestamp se zmenil na string"
- **Zero-copy na klientu** - primo do DuckDB/Pandas/Polars
- **Streaming support** - velke datasety bez OOM

### Negativni

- **Novy dependency** - `pyarrow` (~30 MB)
- **Binary format** - nelze debugovat curl-em
- **Client support** - ne vsechny jazyky maji Arrow knihovny

### Mitigace

- JSON zustava jako default (backward compatible)
- Arrow je opt-in (`format=arrow` nebo `Accept` header)
- Dokumentace s priklady pro Python/JS/Go klienty

## Alternativy

### 1. Arrow Flight (rejected for MVP)

```
+ Nativni streaming protocol
+ Bidirectional
- Komplexnejsi setup (gRPC)
- Overkill pro jednoduche export
```

### 2. Parquet export (complementary)

```
+ Lepsi komprese pro storage
+ Siroke tooling support
- Vyssi latence (komprese)
- Ne zero-copy
```

**Rozhodnuti:** Pridat Parquet jako dalsi `format` option, ale Arrow jako primarni pro real-time.

### 3. MessagePack / CBOR (rejected)

```
+ Menssi nez JSON
- Neni columnar
- Neni zero-copy
- Nema schema
```

## Performance Expectations

| Operace | JSON | Arrow | Zlepseni |
|---------|------|-------|----------|
| Export 100k rows | 200ms | 3ms | 66x |
| Export 1M rows | 2s | 30ms | 66x |
| Wire size 100k rows | 8.5 MB | 2.7 MB | 3.2x |
| Wire size 1M rows | 85 MB | 27 MB | 3.2x |
| Client deserialize | 150ms | 1ms | 150x |

## Implementacni faze

### Faze 1: Export endpoint (nizka narocnost)

```
GET /projects/{id}/buckets/{bucket}/tables/{table}/export?format=arrow
```

- Pouze cte existujici tabulku
- Jednoducha implementace
- Zadne zmeny v ukladani dat

### Faze 2: Query endpoint (stredni narocnost)

```
POST /projects/{id}/workspaces/{ws_id}/query
{"sql": "...", "format": "arrow"}
```

- Vyzaduje workspace session
- Arbitrary SQL s Arrow response

### Faze 3: Import endpoint (vyssi narocnost)

```
POST /projects/{id}/buckets/{bucket}/tables/{table}/import
Content-Type: application/vnd.apache.arrow.stream
```

- Schema inference z Arrow
- Type mapping Arrow -> DuckDB
- Streaming import pro velke soubory

### Faze 4: Arrow Flight (optional, post-MVP)

- Bidirectional streaming
- Nativni DuckDB Flight extension
- Pro heavy-duty AI pipelines

## Dependencies

```
# requirements.txt
pyarrow>=14.0.0  # Arrow IPC, Schema handling
```

**Poznamka:** `pyarrow` je jiz casto dependency pro `pandas`, `duckdb` atd.

## Reference

- [TextQL Sandcastles Blog](https://textql.com/blog/sandcastles) - inspirace pro tuto ADR
- [DuckDB Arrow Export](https://duckdb.org/docs/stable/guides/python/export_arrow.html)
- [DuckDB Arrow Import](https://duckdb.org/docs/stable/guides/python/import_arrow.html)
- [Apache Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
- ADR-009: 1 DuckDB file per table
- ADR-010: DuckDB SQL Interface (workspaces)
