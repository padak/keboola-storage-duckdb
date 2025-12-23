# ADR-014: gRPC Driver Interface

## Status

PROPOSED (2024-12-20)

## Klicova rozhodnuti

Nasledujici rozhodnuti byla prijata behem navrhu:

| # | Oblast | Rozhodnuti | Zduvodneni |
|---|--------|------------|------------|
| 1 | **Proto source** | Zajcuv PR #259 | Ma `service.proto` s RPC, Python example |
| 2 | **Deployment** | Unified proces | Sdilena pamet, jeden healthcheck, atomicita |
| 3 | **gRPC autentizace** | Credentials v protobuf | Jako BigQuery, standardni pattern |
| 4 | **DuckDB credentials** | `host`=project_id, `principal`=api_key | Mapovani na GenericBackendCredentials |
| 5 | **File handling** | S3 path → local path | Driver prelozi S3 cestu na lokalni |
| 6 | **Dev branches** | Implementovat handlery | Mame plnou podporu, ne EmptyHandler |
| 7 | **Extra features** | Jen standardni commands | Snapshots/files zustavaji v REST |
| 8 | **File storage** | Vlastni S3-compatible API | Bez MinIO, plna kontrola |

## Kontext

### Aktualni stav

DuckDB API Service poskytuje REST API (FastAPI) pro vsechny storage operace:
- Project/Bucket/Table CRUD
- Import/Export
- Snapshots
- Dev Branches
- Workspaces
- PG Wire

### Problem

Keboola Connection ocekava **gRPC rozhrani** pro komunikaci s drivery, ne REST API.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Jak to funguje dnes (BigQuery)                       │
│                                                                          │
│   Connection ──► BigQueryDriverClient ──► BigQuery SDK ──► BigQuery API │
│                  (PHP, in-process)        (PHP)            (Google)      │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                     Jak to melo fungovat (gRPC)                          │
│                                                                          │
│   Connection ──► gRPC Client ──────────► gRPC Server ──► DuckDB         │
│                  (PHP)                    (Python)                       │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                     Co jsme udelali (REST)                               │
│                                                                          │
│   Connection ──► PHP Driver ──► DuckDB SDK ──► REST API ──► DuckDB     │
│                  (chybi!)       (chybi!)       (existuje)               │
└─────────────────────────────────────────────────────────────────────────┘
```

### Diskuse s Keboola tymem

Ref: [Slack diskuse](../zajca2.md)

**Klicove body od @zajca a @Vojta Biberle:**

1. Driver interface je definovan v protobuf (`php-storage-driver-common/proto/`)
2. REST API vypada jako kopie Storage API, ne jako driver
3. Driver nema: list projektu, list bucketu, files API, snapshots management
4. Pro integraci s Connection je potreba gRPC rozhrani

**Zajcuv PR:** https://github.com/keboola/storage-backend/pull/259
- Pridal `service.proto` s RPC rozhranim
- Vygeneroval Python gRPC kod
- Ukazka gRPC serveru

### Pozadavky

1. **Integrace s Connection** - driver musi implementovat ocekavane rozhrani
2. **Zachovat REST API** - pro debugging, dashboard, manualni testovani
3. **Minimalni duplikace** - sdilena business logika

## Rozhodnuti

**Pridat gRPC rozhrani vedle existujiciho REST API.**

Obe rozhrani sdili stejnou business logiku - jen ruzna serializace a transport.

### Architektura

```
                                    ┌─────────────────────────────────────┐
                                    │      DuckDB API Service             │
                                    │         (Python)                    │
                                    │                                     │
┌──────────────┐   gRPC :50051      │  ┌─────────────┐  ┌──────────────┐ │
│  Connection  │ ──────────────────►│  │ gRPC Server │  │ REST API     │ │
│  (PHP)       │                    │  │ (grpcio)    │  │ (FastAPI)    │ │
│              │                    │  │             │  │ :8000        │ │
└──────────────┘                    │  └──────┬──────┘  └──────┬───────┘ │
                                    │         │                 │         │
                                    │         ▼                 ▼         │
┌──────────────┐   HTTP :8000       │  ┌─────────────────────────────────┐│
│  Dashboard   │ ──────────────────►│  │    Services Layer               ││
│  Debug CLI   │                    │  │    (shared business logic)      ││
│  Tests       │                    │  │                                 ││
└──────────────┘                    │  │  TableService, BucketService,   ││
                                    │  │  ImportService, WorkspaceService││
                                    │  └──────────────┬──────────────────┘│
                                    │                 │                   │
                                    │                 ▼                   │
                                    │  ┌─────────────────────────────────┐│
                                    │  │  Storage Layer                  ││
                                    │  │  ProjectDBManager, MetadataDB   ││
                                    │  └──────────────┬──────────────────┘│
                                    │                 │                   │
                                    │                 ▼                   │
                                    │         ┌───────────────┐          │
                                    │         │    DuckDB     │          │
                                    │         │  (.duckdb)    │          │
                                    │         └───────────────┘          │
                                    └─────────────────────────────────────┘
```

### Dual Interface Pattern

| Vrstva | REST (FastAPI) | gRPC |
|--------|----------------|------|
| **Transport** | HTTP/1.1, JSON | HTTP/2, Protobuf |
| **Port** | 8000 | 50051 |
| **Handlers** | `routers/*.py` | `grpc_handlers/*.py` |
| **Business Logic** | `services/*.py` (sdilena) | `services/*.py` (sdilena) |
| **Pouziti** | Debug, dashboard, testy | Connection (produkce) |

### gRPC Commands (Driver Interface)

Podle `php-storage-driver-common/proto/`:

| Command | Handler | Popis |
|---------|---------|-------|
| `InitBackendCommand` | InitBackendHandler | Inicializace storage |
| `RemoveBackendCommand` | RemoveBackendHandler | Odstraneni storage |
| `CreateProjectCommand` | CreateProjectHandler | Vytvoreni projektu |
| `DropProjectCommand` | DropProjectHandler | Smazani projektu |
| `CreateBucketCommand` | CreateBucketHandler | Vytvoreni bucketu |
| `DropBucketCommand` | DropBucketHandler | Smazani bucketu |
| `ShareBucketCommand` | ShareBucketHandler | Sdileni bucketu |
| `LinkBucketCommand` | LinkBucketHandler | Linkovani bucketu |
| `CreateTableCommand` | CreateTableHandler | Vytvoreni tabulky |
| `DropTableCommand` | DropTableHandler | Smazani tabulky |
| `AddColumnCommand` | AddColumnHandler | Pridani sloupce |
| `DropColumnCommand` | DropColumnHandler | Smazani sloupce |
| `TableImportFromFileCommand` | ImportFromFileHandler | Import dat |
| `TableExportToFileCommand` | ExportToFileHandler | Export dat |
| `PreviewTableCommand` | PreviewTableHandler | Nahled dat |
| `ObjectInfoCommand` | ObjectInfoHandler | Discovery objektu |
| `CreateWorkspaceCommand` | CreateWorkspaceHandler | Vytvoreni workspace |
| `DropWorkspaceCommand` | DropWorkspaceHandler | Smazani workspace |
| `ExecuteQueryCommand` | ExecuteQueryHandler | SQL dotaz |

**Pozn:** Driver NEMA tyto operace (patri do Storage API):
- List projects/buckets/tables
- Project/bucket/table stats
- Files management (upload, download, list)
- Snapshots management

**Dev branches:** `CreateDevBranchCommand` a `DropDevBranchCommand` JSOU v proto a budeme je implementovat (ne EmptyHandler jako BigQuery)

### Co zustane pouze v REST API

| Endpoint | Duvod |
|----------|-------|
| `GET /projects` | Storage API operace |
| `GET /projects/{id}/stats` | Storage API operace |
| `GET /buckets` | Storage API operace |
| `POST /files/*` | Files management |
| `POST /snapshots/*` | Snapshots management |
| `GET/POST /branches` | Branch management |
| `GET /metrics` | Observability |
| `GET /health` | Health check |

## Implementace

### Faze 1: Refaktoring do Services vrstvy

Presunout business logiku z routeru do services:

```
src/
├── services/           # NOVE - sdilena business logika
│   ├── __init__.py
│   ├── backend_service.py
│   ├── project_service.py
│   ├── bucket_service.py
│   ├── table_service.py
│   ├── import_service.py
│   ├── export_service.py
│   ├── workspace_service.py
│   └── query_service.py
├── routers/            # Existujici - vola services
└── grpc_handlers/      # NOVE - vola services
```

### Faze 2: Proto soubory a generovani

```
proto/                  # Zkopirovat z php-storage-driver-common
├── service.proto       # RPC definice
├── common.proto        # Spolecne typy
├── backend.proto       # Backend commands
├── project.proto       # Project commands
├── bucket.proto        # Bucket commands
├── table.proto         # Table commands
├── workspace.proto     # Workspace commands
└── info.proto          # Discovery

generated/              # Vygenerovany Python kod
├── service_pb2.py
├── service_pb2_grpc.py
└── ...
```

Generovani:
```bash
python -m grpc_tools.protoc \
  -I./proto \
  --python_out=./generated \
  --grpc_python_out=./generated \
  proto/*.proto
```

### Faze 3: gRPC Server implementace

```python
# src/grpc_server.py
import grpc
from concurrent import futures
from generated import service_pb2_grpc, common_pb2

class StorageDriverServicer(service_pb2_grpc.StorageDriverServiceServicer):
    def __init__(self):
        self.table_service = TableService()
        self.bucket_service = BucketService()
        # ...

    def Execute(self, request: common_pb2.DriverRequest, context) -> common_pb2.DriverResponse:
        """Hlavni RPC metoda - routuje command na handler."""
        command_type = request.command.type_url.split('.')[-1]

        handler = self._get_handler(command_type)
        if not handler:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(f'Command {command_type} not supported')
            return common_pb2.DriverResponse()

        try:
            return handler(request)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return common_pb2.DriverResponse()

def serve(host='0.0.0.0', port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_StorageDriverServiceServicer_to_server(
        StorageDriverServicer(), server
    )
    server.add_insecure_port(f'{host}:{port}')
    server.start()
    server.wait_for_termination()
```

### Faze 4: Docker Compose

```yaml
# docker-compose.yml
services:
  duckdb-rest:
    build: .
    command: python -m src.main
    ports:
      - "8000:8000"
    volumes:
      - ./data:/data

  duckdb-grpc:
    build: .
    command: python -m src.grpc_server
    ports:
      - "50051:50051"
    volumes:
      - ./data:/data

  # Nebo jeden proces s obema servery
  duckdb-unified:
    build: .
    command: python -m src.unified_server
    ports:
      - "8000:8000"   # REST
      - "50051:50051" # gRPC
    volumes:
      - ./data:/data
```

### Faze 5: Integrace s Connection

V Connection pridat DuckDB driver client:

```php
// V connection repository
class DuckDBDriverClient implements ClientInterface
{
    private GrpcClient $grpcClient;

    public function __construct(string $serviceUrl)
    {
        $this->grpcClient = new GrpcClient($serviceUrl);
    }

    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        $request = new DriverRequest();
        $request->setCredentials($credentials);
        $request->setCommand(Any::pack($command));
        $request->setFeatures($features);
        $request->setRuntimeOptions($runtimeOptions);

        return $this->grpcClient->Execute($request);
    }
}
```

## Detaily rozhodnuti

### R4: Mapovani GenericBackendCredentials

BigQuery pouziva `GenericBackendCredentials` takto:
```protobuf
message GenericBackendCredentials {
    string host = 1;       // BQ: project ID
    string principal = 2;  // BQ: service account JSON
    string secret = 3;     // BQ: private key
}
```

**DuckDB mapovani:**
```
host      = "123"                      # Project ID
principal = "proj_123_admin_abc..."    # Project API key
secret    = ""                         # Nepouzito
```

**gRPC handler:**
```python
def Execute(self, request, context):
    creds = request.credentials
    project_id = creds.host           # "123"
    api_key = creds.principal         # "proj_123_admin_abc..."

    if not self.auth.verify_project_key(project_id, api_key):
        context.set_code(grpc.StatusCode.UNAUTHENTICATED)
        return DriverResponse()
```

### R5: File Handling - S3 Path Translation

`TableImportFromFileCommand` prijde s S3 path:
```protobuf
message FilePath {
    string root = 1;   // "keboola-files" (bucket)
    string path = 2;   // "project_123/data.csv"
}
```

**Driver prelozi na lokalni cestu:**
```python
def handle_import_from_file(self, command):
    bucket = command.file_path.root   # "keboola-files"
    key = command.file_path.path      # "project_123/data.csv"

    # S3 path → local path
    local_path = Path(f"/data/s3/{bucket}/{key}")

    # Pouzije existujici import logiku
    self.import_service.import_from_file(table, local_path, options)
```

### R8: S3-Compatible API

Misto MinIO implementujeme vlastni S3-compatible vrstvu v FastAPI.

**Architektura:**
```
Connection                          DuckDB Service
    │                                    │
    │ S3 PutObject                       │
    ├───────────────────────────────────►│ /s3/{bucket}/{key}
    │                                    │      │
    │                                    │      ▼
    │                                    │ /data/s3/bucket/key
    │                                    │
    │ gRPC TableImportFromFileCommand    │
    ├───────────────────────────────────►│
    │   file_path.root = "bucket"        │      │
    │   file_path.path = "key"           │      ▼
    │                                    │ Prelozi na /data/s3/bucket/key
    │                                    │ Importuje do DuckDB
```

**Implementovane S3 endpointy:**

| Operace | Endpoint | Popis |
|---------|----------|-------|
| PutObject | `PUT /s3/{bucket}/{key}` | Upload souboru |
| GetObject | `GET /s3/{bucket}/{key}` | Download souboru |
| HeadObject | `HEAD /s3/{bucket}/{key}` | Kontrola existence |
| DeleteObject | `DELETE /s3/{bucket}/{key}` | Smazani souboru |
| CreateMultipartUpload | `POST /s3/{bucket}/{key}?uploads` | Zahajeni multipart |
| UploadPart | `PUT /s3/{bucket}/{key}?uploadId=X&partNumber=N` | Upload casti |
| CompleteMultipartUpload | `POST /s3/{bucket}/{key}?uploadId=X` | Dokonceni multipart |
| CreateBucket | `PUT /s3/{bucket}` | Vytvoreni bucketu |
| HeadBucket | `HEAD /s3/{bucket}` | Kontrola bucketu |

**Connection konfigurace:**
```bash
S3_ENDPOINT=http://duckdb-service:8000/s3
S3_ACCESS_KEY=dummy     # Auth pres project credentials
S3_SECRET_KEY=dummy
S3_BUCKET=keboola-files
```

**Struktura na disku:**
```
/data/
├── duckdb/           # DuckDB tabulky
├── files/            # Nase Files API (REST)
├── s3/               # S3-compatible storage
│   └── keboola-files/
│       └── project_123/
│           └── upload_abc.csv
└── metadata.duckdb   # Metadata DB
```

**Vyhody vlastni implementace:**
- Zadny externi kontejner (MinIO)
- Plna kontrola nad ukladanim
- Integrace s existujici auth
- Jednodussi debugging

## Dusledky

### Pozitivni

- **Standardni integrace** - Connection pouziva ocekavane gRPC rozhrani
- **Zachovane REST API** - debugging, dashboard, manualni testovani
- **Sdilena logika** - zadna duplikace business kodu
- **Performance** - gRPC je efektivnejsi nez REST pro driver operace
- **Type safety** - Protobuf zajistuje typovou kontrolu

### Negativni

- **Komplexita** - dva servery/rozhrani misto jednoho
- **Zavislosti** - grpcio, protobuf dependencies
- **Testovani** - potreba testovat obe rozhrani
- **Deployment** - dva porty (8000, 50051)

### Neutralni

- **Proto soubory** - potreba synchronizovat s upstream (php-storage-driver-common)
- **Dokumentace** - REST pro API docs, gRPC pro driver docs

## Alternativy

### A1: Pouze REST + PHP SDK

```
Connection → PHP Driver → PHP SDK → REST API → DuckDB
```

- **Pro:** Jednodussi Python strana
- **Proti:** Nestandardni, nutnost udrzovat PHP SDK

### A2: Pouze gRPC (bez REST)

- **Pro:** Jednoduche, jeden server
- **Proti:** Ztrata debug moznosti, dashboard

### A3: REST-to-gRPC proxy

- **Pro:** Zadne zmeny v Python kodu
- **Proti:** Extra komponenta, latence

**Rozhodnuti:** Dual interface (REST + gRPC) je nejflexibilnejsi reseni.

## Reference

- [Slack diskuse](../zajca2.md) - kontext od Keboola tymu
- [Zajcuv PR](https://github.com/keboola/storage-backend/pull/259) - service.proto a Python example
- [php-storage-driver-common](https://github.com/keboola/php-storage-driver-common) - proto definice
- [php-storage-driver-bigquery](https://github.com/keboola/php-storage-driver-bigquery) - reference implementace
- ADR-001: DuckDB Microservice Architecture
- ADR-009: DuckDB File Per Table

## Changelog

- 2024-12-20: Pridana rozhodnuti R1-R8 (proto source, deployment, auth, credentials, files, branches, features, S3 API)
- 2024-12-19: Initial proposal
