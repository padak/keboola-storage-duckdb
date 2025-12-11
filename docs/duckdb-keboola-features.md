# DuckDB: Pokryti Keboola Features a Prilezitosti

## Obsah

1. [Sdileni tabulek mezi projekty](#1-sdileni-tabulek-mezi-projekty)
2. [Branchovana storage](#2-branchovana-storage)
3. [Backup a rollback dat](#3-backup-a-rollback-dat)
4. [SQL transformace a Query Service](#4-sql-transformace-a-query-service)
5. [Prilezitosti DuckDB vs Snowflake/BigQuery](#5-prilezitosti-duckdb)
6. [Limity a skala (5000 tabulek, 500GB)](#6-limity-a-skala)
7. [Python integrace](#7-python-integrace)
8. [AI agenti a prace s daty](#8-ai-agenti)

---

## 1. Sdileni tabulek mezi projekty

### Jak to funguje v Keboola

Keboola ma sofistikovany system sdileni bucketu:

| Typ sdileni | Popis |
|-------------|-------|
| SHARING_ORGANIZATION | Sdileno s celou organizaci |
| SHARING_ORGANIZATION_PROJECT | Sdileno s projekty v organizaci |
| SHARING_SPECIFIC_PROJECTS | Sdileno s konkretnimi projekty |
| SHARING_SPECIFIC_USERS | Sdileno s konkretnimi uzivateli |

**Linked Buckets**: Cilovy projekt vytvori "linked bucket" - **read-only referenci** na zdrojovy bucket.

### Implementace v DuckDB

**Reseni: ATTACH + Views**

```
project_123.duckdb (source)
├── in_c_customers/
│   └── customers          # zdrojova tabulka

project_456.duckdb (target)
├── linked_in_c_customers/     # linked bucket schema
│   └── customers              # VIEW na zdrojovou tabulku
```

**Jak to bude fungovat:**

```python
# Pri vytvoreni linked bucket
def create_linked_bucket(source_project, source_bucket, target_project, linked_bucket_name):
    target_conn = duckdb.connect(f'project_{target_project}.duckdb')

    # Pripojit zdrojovy projekt jako read-only
    target_conn.execute(f"""
        ATTACH 'project_{source_project}.duckdb' AS source_proj (READ_ONLY)
    """)

    # Vytvorit schema pro linked bucket
    target_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {linked_bucket_name}")

    # Pro kazdou tabulku ve zdrojovem bucketu vytvorit VIEW
    for table in get_tables(source_project, source_bucket):
        target_conn.execute(f"""
            CREATE VIEW {linked_bucket_name}.{table} AS
            SELECT * FROM source_proj.{source_bucket}.{table}
        """)
```

**Vyhody:**
- Read-only je vynuceno na urovni ATTACH
- Zmeny ve zdroji jsou okamzite viditelne
- Zadna duplikace dat

**Omezeni:**
- Vyzaduje ATTACH pri otevreni databaze
- Cross-project JOINy funguji, ale pres VIEW

---

## 2. Branchovana storage

### Jak to funguje v Keboola

- **Dev Branches**: Izolovane prostredi pro vyvoj
- Kazda branch ma vlastni namespace pro buckety a tabulky
- Zmeny v branch neovlivnuji default branch
- **Merge Requests**: Workflow pro merge do default branch

### Implementace v DuckDB

**Reseni A: Schema-based branching** (doporuceno pro jednoduchost)

```
project_123.duckdb
├── main/                         # default branch
│   └── in_c_customers/
│       └── customers
├── dev_branch_456/               # dev branch jako schema prefix
│   └── in_c_customers/
│       └── customers             # kopie nebo COW reference
```

**Reseni B: Separate files per branch**

```
project_123_main.duckdb           # default branch
project_123_branch_456.duckdb     # dev branch
```

### Doporuceni: Reseni B (separate files)

**Proc:**
1. **Izolace**: Zmeny v branch nemohou poskozit main
2. **Snadny merge**: ATTACH + INSERT INTO ... SELECT
3. **Snadne smazani**: Smazat soubor
4. **Paralelni prace**: Ruzne branche = ruzne soubory = paralelni zapis

**Implementace:**

```python
# Vytvoreni dev branch
def create_dev_branch(project_id, branch_id):
    main_path = f'project_{project_id}_main.duckdb'
    branch_path = f'project_{project_id}_branch_{branch_id}.duckdb'

    # Zkopirovat strukturu (bez dat nebo s daty)
    shutil.copy(main_path, branch_path)
    # Nebo: vytvorit prazdnou DB a kopirovat jen schema

# Merge branch do main
def merge_branch(project_id, branch_id):
    main_conn = duckdb.connect(f'project_{project_id}_main.duckdb')
    main_conn.execute(f"""
        ATTACH 'project_{project_id}_branch_{branch_id}.duckdb' AS branch (READ_ONLY)
    """)

    # Pro kazdy bucket/tabulku v branch
    for schema, table in get_branch_tables(branch_id):
        # Merge strategie: replace nebo upsert
        main_conn.execute(f"""
            CREATE OR REPLACE TABLE {schema}.{table} AS
            SELECT * FROM branch.{schema}.{table}
        """)

    main_conn.execute("DETACH branch")
```

### Struktura souboru s branchemi

```
/data/duckdb/
├── project_123_main.duckdb           # default branch
├── project_123_branch_456.duckdb     # dev branch "feature-x"
├── project_123_branch_789.duckdb     # dev branch "experiment"
├── project_124_main.duckdb
└── ...
```

---

## 3. Backup a rollback dat

### Jak to funguje v Keboola

**Snapshots**: Point-in-time zachyceni stavu tabulky
- Ulozi schema, data, metadata
- Moznost obnovit tabulku ze snapshotu
- Filtrovane snapshoty (subset dat)

### DuckDB omezeni

**DuckDB NEMA nativni time-travel** jako Snowflake (UNDROP, AT TIMESTAMP).

### Implementace v DuckDB

**Reseni: Snapshot soubory**

```python
# Vytvoreni snapshotu
def create_snapshot(project_id, bucket, table, snapshot_id, description=None):
    conn = duckdb.connect(f'project_{project_id}_main.duckdb')

    snapshot_dir = f'/data/snapshots/{project_id}/{snapshot_id}'
    os.makedirs(snapshot_dir, exist_ok=True)

    # Export tabulky do Parquet (efektivni, komprimovany)
    conn.execute(f"""
        COPY {bucket}.{table} TO '{snapshot_dir}/{table}.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Ulozit metadata
    metadata = {
        'snapshot_id': snapshot_id,
        'project_id': project_id,
        'bucket': bucket,
        'table': table,
        'created_at': datetime.now().isoformat(),
        'description': description,
        'schema': get_table_schema(conn, bucket, table),
        'row_count': get_row_count(conn, bucket, table)
    }
    with open(f'{snapshot_dir}/metadata.json', 'w') as f:
        json.dump(metadata, f)

# Obnoveni ze snapshotu
def restore_from_snapshot(project_id, snapshot_id, target_bucket, target_table):
    conn = duckdb.connect(f'project_{project_id}_main.duckdb')
    snapshot_dir = f'/data/snapshots/{project_id}/{snapshot_id}'

    with open(f'{snapshot_dir}/metadata.json') as f:
        metadata = json.load(f)

    # Recreate table from Parquet
    conn.execute(f"""
        CREATE OR REPLACE TABLE {target_bucket}.{target_table} AS
        SELECT * FROM read_parquet('{snapshot_dir}/{metadata['table']}.parquet')
    """)
```

### Automaticke snapshoty

```python
# Pre-operation snapshot (pred destruktivni operaci)
def safe_drop_table(project_id, bucket, table):
    # Automaticky snapshot pred smazanim
    snapshot_id = f"auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    create_snapshot(project_id, bucket, table, snapshot_id,
                   description=f"Auto-backup before DROP TABLE {bucket}.{table}")

    # Ted smazat
    conn = duckdb.connect(f'project_{project_id}_main.duckdb')
    conn.execute(f"DROP TABLE {bucket}.{table}")
```

### Struktura snapshotu

```
/data/snapshots/
├── project_123/
│   ├── snap_001/
│   │   ├── metadata.json
│   │   └── customers.parquet
│   ├── snap_002/
│   │   ├── metadata.json
│   │   └── orders.parquet
│   └── auto_20241211_143022/      # automaticky snapshot
│       ├── metadata.json
│       └── deleted_table.parquet
```

---

## 4. SQL transformace a Query Service

### Problem

- **Keboola Query Service** spousti SQL transformace
- Vice transformaci muze bezet soucasne
- DuckDB je **single-writer** - jak serializovat zapisy?

### Reseni: API Service jako Write Serializer

```
┌──────────────────────┐     ┌──────────────────────┐
│  Query Service 1     │────►│                      │
├──────────────────────┤     │  DuckDB API Service  │
│  Query Service 2     │────►│  (Python + FastAPI)  │
├──────────────────────┤     │                      │
│  Query Service N     │────►│  - Write Queue       │
└──────────────────────┘     │  - Connection Pool   │
                             │  - Serialization     │
                             └──────────┬───────────┘
                                        │
                              ┌─────────▼─────────┐
                              │   DuckDB File     │
                              │   (single writer) │
                              └───────────────────┘
```

### Implementace: Async Write Queue

```python
import asyncio
from fastapi import FastAPI
from collections import defaultdict

app = FastAPI()

# Write queue per projekt
write_queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
write_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

async def process_write_queue(project_id: str):
    """Background worker pro zpracovani write operaci"""
    queue = write_queues[project_id]
    lock = write_locks[project_id]

    while True:
        operation = await queue.get()
        async with lock:
            try:
                conn = duckdb.connect(f'project_{project_id}_main.duckdb')
                result = conn.execute(operation['sql']).fetchall()
                operation['future'].set_result(result)
            except Exception as e:
                operation['future'].set_exception(e)
            finally:
                conn.close()
                queue.task_done()

@app.post("/query/execute")
async def execute_query(project_id: str, sql: str, is_write: bool = False):
    if is_write:
        # Write operace jdou do fronty
        future = asyncio.Future()
        await write_queues[project_id].put({
            'sql': sql,
            'future': future
        })
        result = await future
        return {"result": result}
    else:
        # Read operace jsou paralelni
        conn = duckdb.connect(f'project_{project_id}_main.duckdb', read_only=True)
        result = conn.execute(sql).fetchall()
        conn.close()
        return {"result": result}
```

### Query Service integrace

```python
# Keboola Query Service vola nase API
class DuckDBQueryServiceAdapter:
    def __init__(self, api_url: str):
        self.api_url = api_url

    async def execute_transformation(self, project_id: str, sql: str):
        # Detekce write operaci
        is_write = self._is_write_query(sql)

        response = await httpx.post(
            f"{self.api_url}/query/execute",
            json={
                "project_id": project_id,
                "sql": sql,
                "is_write": is_write
            }
        )
        return response.json()

    def _is_write_query(self, sql: str) -> bool:
        write_keywords = ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE']
        sql_upper = sql.upper().strip()
        return any(sql_upper.startswith(kw) for kw in write_keywords)
```

---

## 5. Prilezitosti DuckDB

### DuckDB vs Snowflake/BigQuery

| Aspekt | DuckDB | Snowflake/BigQuery |
|--------|--------|-------------------|
| **Latence** | Milisekundy | Sekundy |
| **Naklady** | $0 (self-hosted) | $$$ (compute + storage + egress) |
| **Infrastruktura** | Zadna | Komplexni |
| **Vendor lock-in** | Zadny | Vysoky |
| **Offline prace** | Ano | Ne |
| **Embedded analytics** | Ano | Ne |

### Unikatni prilezitosti DuckDB

#### 1. Local-First Analytics
```python
# Uzivatel muze stahnout data lokalne a analyzovat bez cloudu
duckdb.connect('my_data.duckdb').execute("SELECT * FROM sales")
```

#### 2. Zero-Cost Development
- Zadne naklady na dev/test prostredi
- Vyvojari mohou mit lokalni kopii dat

#### 3. Instant Query Response
- Zadny cold start jako u Snowflake
- Zadna kompilace query jako u BigQuery
- Idealni pro interaktivni BI

#### 4. Embedded v aplikacich
- DuckDB muze bezet primo v aplikaci
- Zadna externi zavislost

#### 5. Nativni podpora modernich formatu
```sql
-- Primo cist Parquet, CSV, JSON bez importu
SELECT * FROM read_parquet('s3://bucket/*.parquet');
SELECT * FROM read_csv('data.csv');
SELECT * FROM read_json('data.json');
```

#### 6. Kompatibilita s data lake
```sql
-- Primo dotazovat Delta Lake, Iceberg
SELECT * FROM delta_scan('s3://bucket/delta_table');
SELECT * FROM iceberg_scan('s3://bucket/iceberg_table');
```

### Nove use-cases pro Keboola

| Use case | Popis |
|----------|-------|
| **Edge Analytics** | DuckDB na edge zarizeni, sync s centralou |
| **Offline Mode** | Prace s daty bez internetu |
| **Cost Optimization** | Mene zateze na Snowflake = nizsi naklady |
| **Dev Environments** | Kazdy vyvojar ma lokalni "Snowflake" |
| **Faster Prototyping** | Okamzita iterace bez cekani na cloud |

---

## 6. Limity a skala

### Pozadavky

- 5000 tabulek per projekt
- 500 GB per projekt

### DuckDB schopnosti

| Metrika | DuckDB Limit | Nas pozadavek |
|---------|--------------|---------------|
| Pocet tabulek | **Prakticky neomezeno** | 5000 |
| Velikost DB | **Testovano az 10 TB** | 500 GB |
| Komprese | **75-95%** | - |

### Komprese dat

DuckDB pouziva pokrocilou kompresi:

| Algoritmus | Pouziti |
|------------|---------|
| RLE (Run-Length) | Opakovane hodnoty |
| Dictionary | Nizka kardinalita |
| Bit-packing | Cisla |
| Chimp/Patas | Floating point |
| FSST | Stringy |
| Constant | Konstantni sloupce |

**Vysledek**: 500 GB raw dat = cca **50-125 GB** na disku

### Performance s 5000 tabulkami

```sql
-- DuckDB metadata queries jsou rychle
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'my_bucket';
-- Odpoved: < 10ms
```

**Doporuceni pro velke projekty:**
1. Pouzit schemas (buckety) pro organizaci
2. Pravidelna VACUUM pro optimalizaci
3. Monitoring velikosti souboru

---

## 7. Python integrace

### Zero-Copy DataFrame Access

```python
import duckdb
import pandas as pd

# DataFrame v pameti
df = pd.read_csv('huge_file.csv')

# DuckDB query primo na DataFrame - ZERO COPY!
result = duckdb.query("""
    SELECT category, SUM(amount) as total
    FROM df
    GROUP BY category
    ORDER BY total DESC
""").df()
```

### Prilezitosti pro Keboola

#### 1. Python Transformace primo v DuckDB
```python
# Misto Snowflake, pouzit DuckDB pro Python transformace
def keboola_python_transformation(input_tables, output_table):
    conn = duckdb.connect()

    # Load input tables
    for name, df in input_tables.items():
        conn.register(name, df)

    # Run transformation
    result = conn.execute("""
        SELECT a.*, b.category_name
        FROM orders a
        JOIN categories b ON a.category_id = b.id
        WHERE a.amount > 100
    """).df()

    return result
```

#### 2. Jupyter Notebook integrace
```python
# Data scientist muze primo dotazovat Keboola data
%load_ext duckdb_magic

%%duckdb
SELECT * FROM 's3://keboola-bucket/project_123/in_c_sales/orders.parquet'
LIMIT 100
```

#### 3. Streamlit/Dash aplikace
```python
import streamlit as st
import duckdb

@st.cache_resource
def get_connection():
    return duckdb.connect('project_data.duckdb', read_only=True)

conn = get_connection()
df = conn.execute("SELECT * FROM sales WHERE date > '2024-01-01'").df()
st.dataframe(df)
```

---

## 8. AI agenti a prace s daty

### Prilezitosti pro AI

#### 1. Natural Language to SQL
```python
# AI agent generuje DuckDB SQL
def ai_query(natural_language: str, schema: dict) -> str:
    prompt = f"""
    Given this schema: {schema}
    Generate DuckDB SQL for: {natural_language}
    """
    sql = llm.generate(prompt)
    return sql

# Uzivatel: "Show me top 10 customers by revenue"
sql = ai_query("Show me top 10 customers by revenue", get_schema())
result = conn.execute(sql).df()
```

#### 2. Autonomous Data Analysis
```python
# AI agent analyzuje data autonomne
class DataAnalysisAgent:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def analyze(self, table: str) -> dict:
        # Schema discovery
        schema = self.conn.execute(f"DESCRIBE {table}").df()

        # Statistical analysis
        stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as row_count,
                COUNT(DISTINCT *) as unique_rows
            FROM {table}
        """).fetchone()

        # AI-powered insights
        insights = self.generate_insights(schema, stats)
        return insights
```

#### 3. RAG nad firemnimi daty
```python
# Embeddings ulozene v DuckDB + vector search
# (DuckDB ma VSS extension pro vector similarity search)

conn.execute("INSTALL vss; LOAD vss;")

conn.execute("""
    CREATE TABLE documents (
        id INTEGER,
        content TEXT,
        embedding FLOAT[1536]  -- OpenAI embedding dimension
    )
""")

# Vector search
similar = conn.execute("""
    SELECT content, array_cosine_similarity(embedding, $1) as similarity
    FROM documents
    ORDER BY similarity DESC
    LIMIT 5
""", [query_embedding]).df()
```

#### 4. MCP (Model Context Protocol) Server
```python
# DuckDB jako MCP server pro Claude/GPT
from mcp import Server

class DuckDBMCPServer(Server):
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path, read_only=True)

    async def query(self, sql: str) -> dict:
        """AI muze primo dotazovat DuckDB"""
        result = self.conn.execute(sql).df()
        return result.to_dict()

    async def get_schema(self) -> dict:
        """AI vidi schema"""
        tables = self.conn.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
        """).df()
        return tables.to_dict()
```

### Architektura pro AI agenty

```
┌─────────────────────┐
│   AI Agent (LLM)    │
│   - Claude/GPT      │
│   - Generates SQL   │
└──────────┬──────────┘
           │ MCP / API
           ▼
┌─────────────────────┐
│  DuckDB API Service │
│  - Schema discovery │
│  - Query execution  │
│  - Result caching   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│     DuckDB File     │
│  - Company data     │
│  - Vector embeddings│
└─────────────────────┘
```

---

## Shrnuti: Feature Matrix

| Keboola Feature | DuckDB Implementace | Slozitost |
|-----------------|---------------------|-----------|
| Bucket Sharing | ATTACH + Views | Stredni |
| Linked Buckets | READ_ONLY Views | Stredni |
| Dev Branches | Separate files per branch | Nizka |
| Merge Requests | ATTACH + INSERT SELECT | Nizka |
| Snapshots | Parquet export | Nizka |
| Time Travel | Snapshot-based | Stredni |
| Query Service | Write serialization queue | Vysoka |
| Python Transformace | Native integration | Nizka |
| AI Agents | MCP Server + Vector Search | Stredni |

## Dalsi kroky

1. [ ] Rozhodnout o branch strategii (schema vs files)
2. [ ] Navrhnout snapshot retention politiku
3. [ ] Definovat API pro Query Service integraci
4. [ ] Prototype MCP serveru pro AI agenty
5. [ ] Benchmark 5000 tabulek / 500GB
