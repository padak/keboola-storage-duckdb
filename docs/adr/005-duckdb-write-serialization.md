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

## Edge Cases a Failure Handling

### 1. Timeouts

```python
@dataclass
class WriteOperation:
    sql: str
    future: asyncio.Future
    priority: int = 0
    timeout: float = 300.0  # Default 5 minut
    enqueued_at: float = field(default_factory=time.time)
    max_execution_time: float = 600.0  # Max 10 minut pro jednu operaci

# Timeout handling v queue workeru:
async def _process_queue(self, project_id: str):
    while True:
        _, operation = await queue.get()

        # Check if already expired in queue
        wait_time = time.time() - operation.enqueued_at
        if wait_time > operation.timeout:
            operation.future.set_exception(
                TimeoutError(f"Operation expired in queue after {wait_time:.1f}s")
            )
            queue.task_done()
            continue

        # Execute with remaining timeout
        remaining_timeout = operation.timeout - wait_time
        try:
            result = await asyncio.wait_for(
                self._execute_sql(project_id, operation.sql),
                timeout=min(remaining_timeout, operation.max_execution_time)
            )
            operation.future.set_result(result)
        except asyncio.TimeoutError:
            operation.future.set_exception(
                TimeoutError(f"Query execution timed out")
            )
```

**Doporucene timeouty:**
| Operace | Timeout |
|---------|---------|
| CREATE TABLE | 30s |
| DROP TABLE | 30s |
| ALTER TABLE | 60s |
| INSERT (male) | 60s |
| INSERT (velke, >1M rows) | 300s |
| COPY FROM FILE | 600s |
| DELETE WHERE | 300s |

### 2. Retry Policy

```python
@dataclass
class RetryPolicy:
    max_retries: int = 3
    base_delay: float = 0.1  # 100ms
    max_delay: float = 10.0
    exponential_base: float = 2.0
    retryable_errors: tuple = (
        "database is locked",
        "connection lost",
        "disk I/O error",
    )

async def execute_with_retry(
    self,
    project_id: str,
    sql: str,
    retry_policy: RetryPolicy = RetryPolicy()
) -> Any:
    last_error = None
    for attempt in range(retry_policy.max_retries + 1):
        try:
            return await self._execute_sql(project_id, sql)
        except Exception as e:
            error_msg = str(e).lower()
            is_retryable = any(
                err in error_msg for err in retry_policy.retryable_errors
            )

            if not is_retryable or attempt == retry_policy.max_retries:
                raise

            last_error = e
            delay = min(
                retry_policy.base_delay * (retry_policy.exponential_base ** attempt),
                retry_policy.max_delay
            )
            logger.warning(
                "retrying_operation",
                attempt=attempt + 1,
                delay=delay,
                error=str(e)
            )
            await asyncio.sleep(delay)

    raise last_error
```

**Co se NERETRYUJE:**
- Syntax errors
- Constraint violations (UNIQUE, FK)
- Permission errors
- Invalid table/column names

### 3. Worker Crash Recovery

```python
class WriteQueueManager:
    def __init__(self):
        self.queues: dict[str, asyncio.PriorityQueue] = {}
        self.workers: dict[str, asyncio.Task] = {}
        self.operation_log: dict[str, list[WriteOperation]] = defaultdict(list)

    async def _process_queue(self, project_id: str):
        """Worker s crash recovery."""
        while True:
            try:
                _, operation = await self.queues[project_id].get()

                # Log operation start (pro crash recovery)
                operation_id = str(uuid.uuid4())
                self._log_operation_start(project_id, operation_id, operation)

                try:
                    result = await self._execute_sql(project_id, operation.sql)
                    operation.future.set_result(result)
                    self._log_operation_complete(project_id, operation_id)
                except Exception as e:
                    operation.future.set_exception(e)
                    self._log_operation_failed(project_id, operation_id, e)

            except asyncio.CancelledError:
                # Graceful shutdown
                logger.info("worker_shutdown", project_id=project_id)
                break
            except Exception as e:
                # Worker crash - restart
                logger.error(
                    "worker_crashed",
                    project_id=project_id,
                    error=str(e),
                    exc_info=True
                )
                # Kratka pauza pred restartem
                await asyncio.sleep(1.0)

    def _ensure_worker_running(self, project_id: str):
        """Restart worker pokud spadl."""
        if project_id in self.workers:
            task = self.workers[project_id]
            if task.done():
                # Worker spadl nebo byl zrusen
                exception = task.exception() if not task.cancelled() else None
                if exception:
                    logger.warning(
                        "restarting_crashed_worker",
                        project_id=project_id,
                        previous_error=str(exception)
                    )
                self.workers[project_id] = asyncio.create_task(
                    self._process_queue(project_id)
                )
        else:
            self.workers[project_id] = asyncio.create_task(
                self._process_queue(project_id)
            )
```

### 4. Long Reads vs Write Lock

**Problem:** Dlouhy SELECT blokuje write operace (DuckDB pouziva MVCC, ale checkpoint potrebuje exclusive lock).

```python
class ConnectionManager:
    """Sprava read/write connections s fair scheduling."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.write_lock = asyncio.Lock()
        self.active_reads: int = 0
        self.reads_condition = asyncio.Condition()
        self.pending_writes: int = 0

    async def acquire_read(self) -> duckdb.DuckDBPyConnection:
        """Ziskej read connection - muze byt paralelni."""
        async with self.reads_condition:
            # Cekej pokud jsou pending writes (write priority)
            while self.pending_writes > 0:
                await self.reads_condition.wait()
            self.active_reads += 1

        return duckdb.connect(self.db_path, read_only=True)

    async def release_read(self, conn: duckdb.DuckDBPyConnection):
        """Uvolni read connection."""
        conn.close()
        async with self.reads_condition:
            self.active_reads -= 1
            self.reads_condition.notify_all()

    async def acquire_write(self) -> duckdb.DuckDBPyConnection:
        """Ziskej write connection - exkluzivni."""
        async with self.reads_condition:
            self.pending_writes += 1

        # Cekej az vsechny reads dokonci
        async with self.reads_condition:
            while self.active_reads > 0:
                await self.reads_condition.wait()

        await self.write_lock.acquire()
        return duckdb.connect(self.db_path)

    async def release_write(self, conn: duckdb.DuckDBPyConnection):
        """Uvolni write connection."""
        conn.close()
        self.write_lock.release()
        async with self.reads_condition:
            self.pending_writes -= 1
            self.reads_condition.notify_all()

    @asynccontextmanager
    async def read_connection(self):
        conn = await self.acquire_read()
        try:
            yield conn
        finally:
            await self.release_read(conn)

    @asynccontextmanager
    async def write_connection(self):
        conn = await self.acquire_write()
        try:
            yield conn
        finally:
            await self.release_write(conn)
```

**Strategie:**
1. **Write priority**: Pending writes blokuji nove reads
2. **Read timeout**: Dlouhe reads maji soft timeout (warn log po 60s)
3. **Connection pooling**: Read connections jsou recyklovane

### 5. Queue Overflow

```python
class WriteQueueManager:
    MAX_QUEUE_SIZE = 1000
    MAX_QUEUE_WAIT = 60.0  # Max cekani na misto ve fronte

    async def enqueue_write(self, project_id: str, sql: str, ...) -> Any:
        queue = self._get_queue(project_id)

        # Check queue size
        if queue.qsize() >= self.MAX_QUEUE_SIZE:
            logger.warning(
                "queue_full",
                project_id=project_id,
                queue_size=queue.qsize()
            )
            raise QueueFullError(
                f"Write queue full for project {project_id}. "
                f"Try again later or increase capacity."
            )

        # Enqueue with backpressure
        try:
            await asyncio.wait_for(
                queue.put((-priority, operation)),
                timeout=self.MAX_QUEUE_WAIT
            )
        except asyncio.TimeoutError:
            raise QueueFullError(
                f"Could not enqueue operation within {self.MAX_QUEUE_WAIT}s"
            )
```

### 6. Graceful Shutdown

```python
class WriteQueueManager:
    async def shutdown(self, timeout: float = 30.0):
        """Graceful shutdown - dokoncit rozpracovane operace."""
        logger.info("shutdown_started", pending_workers=len(self.workers))

        # Stop accepting new operations
        self._accepting_new = False

        # Wait for queues to drain
        try:
            await asyncio.wait_for(
                self._drain_all_queues(),
                timeout=timeout
            )
            logger.info("queues_drained")
        except asyncio.TimeoutError:
            logger.warning(
                "shutdown_timeout",
                remaining_operations=sum(q.qsize() for q in self.queues.values())
            )

        # Cancel workers
        for project_id, task in self.workers.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        logger.info("shutdown_complete")
```

## Monitoring a Alerting

### Doporucene alerty

| Metrika | Threshold | Severity |
|---------|-----------|----------|
| Queue depth | > 100 | Warning |
| Queue depth | > 500 | Critical |
| Queue wait time p99 | > 30s | Warning |
| Queue wait time p99 | > 120s | Critical |
| Worker crash rate | > 1/min | Critical |
| Write timeout rate | > 5% | Warning |

## Reference

- [DuckDB Concurrency](https://duckdb.org/docs/stable/connect/concurrency.html)
- [Python asyncio Queue](https://docs.python.org/3/library/asyncio-queue.html)
