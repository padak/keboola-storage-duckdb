# ADR-005: Serializace zapisu pres API frontu

## Status

Accepted

## Datum

2024-12-11

## Kontext

DuckDB ma **single-writer** omezeni - v jednom okamziku muze do databazoveho souboru zapisovat pouze jeden proces. Keboola ma vsak vice komponent, ktere potrebuji zapisovat:

- Storage API (CRUD operace)
- Query Service (SQL transformace)
- Python transformace
- Sync jobs

Potrebujeme zajistit serializaci zapisu bez deadlocku a s rozumnou latenci.

## Rozhodnuti

**Vsechny write operace budou serializovany pres async frontu v DuckDB API Service.**

### Architektura

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Storage API     │     │  Query Service   │     │  Python Jobs     │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                        │                        │
         │ HTTP POST /query       │ HTTP POST /query       │
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │   DuckDB API Service    │
                    │                         │
                    │  ┌───────────────────┐  │
                    │  │ Request Router    │  │
                    │  │ - is_write_query? │  │
                    │  └─────────┬─────────┘  │
                    │            │            │
                    │     ┌──────┴──────┐     │
                    │     │             │     │
                    │     ▼             ▼     │
                    │  ┌──────┐    ┌───────┐  │
                    │  │WRITE │    │ READ  │  │
                    │  │Queue │    │ Pool  │  │
                    │  │(async)│   │(parallel)│
                    │  └──┬───┘    └───┬───┘  │
                    │     │            │      │
                    │     ▼            ▼      │
                    │  ┌────────────────────┐ │
                    │  │   DuckDB File      │ │
                    │  │ (single writer,    │ │
                    │  │  multi reader)     │ │
                    │  └────────────────────┘ │
                    └─────────────────────────┘
```

## Duvody

### Proc ne zamky na urovni aplikace?

1. **Distribuovane zamky**: Slozite (Redis, etcd), dalsi zavislost
2. **File locking**: DuckDB si resi sam, ale timeout handling je slozity
3. **Deadlock riziko**: Vice procesu cekajicich na zamek

### Proc async fronta?

1. **Jednoduchost**: Jeden writer = jedna fronta
2. **Fairness**: FIFO zpracovani
3. **Backpressure**: Moznost limitovat frontu
4. **Monitoring**: Snadne sledovani queue depth
5. **Timeout handling**: Jasne definovane chovani

## Dusledky

### Pozitivni

- Zadne deadlocky
- Predikovatelne chovani
- Jednoducha implementace
- Snadny monitoring a debugging

### Negativni

- Latence pro write operace (cekani ve fronte)
- Bottleneck pro write-heavy workloads
- Single point of failure (API service)

### Mitigace

- **Latence**: Priority queue pro kriticke operace
- **Bottleneck**: Fronta per projekt (paralelismus mezi projekty)
- **SPOF**: Horizontal scaling API service (kazda instance = jine projekty)

## Implementace

### Write Queue Manager

```python
import asyncio
from dataclasses import dataclass
from typing import Any
from collections import defaultdict

@dataclass
class WriteOperation:
    sql: str
    future: asyncio.Future
    priority: int = 0  # 0 = normal, 1 = high
    timeout: float = 300.0

class WriteQueueManager:
    def __init__(self):
        self.queues: dict[str, asyncio.PriorityQueue] = defaultdict(
            lambda: asyncio.PriorityQueue(maxsize=1000)
        )
        self.workers: dict[str, asyncio.Task] = {}

    async def enqueue_write(self, project_id: str, sql: str,
                           priority: int = 0, timeout: float = 300.0) -> Any:
        """Enqueue write operation and wait for result."""
        future = asyncio.Future()
        operation = WriteOperation(sql=sql, future=future,
                                  priority=priority, timeout=timeout)

        # Start worker if not running
        if project_id not in self.workers:
            self.workers[project_id] = asyncio.create_task(
                self._process_queue(project_id)
            )

        # Priority queue uses (priority, operation) - lower = higher priority
        await self.queues[project_id].put((-priority, operation))

        # Wait for result with timeout
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Write operation timed out after {timeout}s")

    async def _process_queue(self, project_id: str):
        """Worker that processes write operations sequentially."""
        queue = self.queues[project_id]
        db_path = f'/data/duckdb/project_{project_id}_main.duckdb'

        while True:
            _, operation = await queue.get()

            try:
                # Single connection for write
                conn = duckdb.connect(db_path)
                result = conn.execute(operation.sql).fetchall()
                conn.close()

                operation.future.set_result(result)
            except Exception as e:
                operation.future.set_exception(e)
            finally:
                queue.task_done()
```

### API Endpoint

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()
write_manager = WriteQueueManager()

class QueryRequest(BaseModel):
    project_id: str
    sql: str
    priority: int = 0
    timeout: float = 300.0

def is_write_query(sql: str) -> bool:
    """Detect if query modifies data."""
    write_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
        'ALTER', 'TRUNCATE', 'COPY'
    ]
    sql_normalized = ' '.join(sql.upper().split())
    return any(sql_normalized.startswith(kw) for kw in write_keywords)

@app.post("/query")
async def execute_query(request: QueryRequest):
    if is_write_query(request.sql):
        # Write -> queue
        try:
            result = await write_manager.enqueue_write(
                request.project_id,
                request.sql,
                request.priority,
                request.timeout
            )
            return {"status": "ok", "result": result}
        except TimeoutError as e:
            raise HTTPException(status_code=408, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        # Read -> parallel
        try:
            db_path = f'/data/duckdb/project_{request.project_id}_main.duckdb'
            conn = duckdb.connect(db_path, read_only=True)
            result = conn.execute(request.sql).fetchall()
            conn.close()
            return {"status": "ok", "result": result}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
```

### Query Service Integration

```python
class KeboolaQueryServiceClient:
    """Client for Keboola components to execute queries."""

    def __init__(self, api_url: str):
        self.api_url = api_url

    async def execute(self, project_id: str, sql: str,
                     priority: int = 0) -> list:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.api_url}/query",
                json={
                    "project_id": project_id,
                    "sql": sql,
                    "priority": priority
                },
                timeout=600.0
            )
            response.raise_for_status()
            return response.json()["result"]
```

## Reference

- [DuckDB Concurrency](https://duckdb.org/docs/stable/connect/concurrency.html)
- [Python asyncio Queue](https://docs.python.org/3/library/asyncio-queue.html)
