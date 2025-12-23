# Phase 12c: Core gRPC Handlers

**Status:** DONE (2024-12-21)
**Goal:** Implementovat 8 zakladnich gRPC handleru pro funkcni tabulky v Connection
**Prerekvizity:** Phase 12a (gRPC server), Phase 12b (Connection registration)

## Implementation Summary

All 8 handlers implemented with 23 tests passing:

| Handler | File | Status |
|---------|------|--------|
| CreateBucketHandler | `src/grpc/handlers/bucket.py` | DONE |
| DropBucketHandler | `src/grpc/handlers/bucket.py` | DONE |
| CreateTableHandler | `src/grpc/handlers/table.py` | DONE |
| DropTableHandler | `src/grpc/handlers/table.py` | DONE |
| PreviewTableHandler | `src/grpc/handlers/table.py` | DONE |
| ObjectInfoHandler | `src/grpc/handlers/info.py` | DONE |
| TableImportFromFileHandler | `src/grpc/handlers/import_export.py` | DONE |
| TableExportToFileHandler | `src/grpc/handlers/import_export.py` | DONE |

Tests: `tests/test_grpc_handlers_phase12c.py` (23 tests)

---

## Prehled

Mame 4 handlery (InitBackend, RemoveBackend, CreateProject, DropProject).
Potrebujeme pridat 8 core handleru pro zakladni CRUD operace.

## Architektura

```
Connection (PHP)
      │
      │ gRPC / HTTP Bridge
      ▼
┌─────────────────────────────────────────────────────────────┐
│  DuckDB API Service                                          │
│                                                              │
│  gRPC Servicer                                               │
│       │                                                      │
│       ▼                                                      │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Handler                     Existujici logika       │    │
│  │  ─────────────────────────────────────────────────  │    │
│  │  CreateBucketHandler    →    database.py             │    │
│  │  DropBucketHandler      →    database.py             │    │
│  │  CreateTableHandler     →    database.py             │    │
│  │  DropTableHandler       →    database.py             │    │
│  │  ObjectInfoHandler      →    database.py             │    │
│  │  PreviewTableHandler    →    routers/tables.py       │    │
│  │  ImportFromFileHandler  →    routers/table_import.py │    │
│  │  ExportToFileHandler    →    routers/table_import.py │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  REST API (pro standalone pouziti - zachovano)              │
└─────────────────────────────────────────────────────────────┘
```

## Handlery k implementaci

### 1. CreateBucketHandler

**Proto command:** `CreateBucketCommand`
```protobuf
message CreateBucketCommand {
  string stackPrefix = 1;
  string projectId = 2;
  string bucketId = 3;       // "out.c-my-bucket"
  string branchId = 7;
  string projectRoleName = 4;
  string projectReadOnlyRoleName = 5;
  bool isBranchDefault = 9;
}
```

**Proto response:** `CreateBucketResponse`
```protobuf
message CreateBucketResponse {
  repeated string path = 1;
  string createBucketObjectName = 2;  // "out_c_my_bucket"
}
```

**Mapovani na REST:**
- REST endpoint: `POST /projects/{id}/branches/{branch}/buckets`
- Existujici logika: `ProjectDBManager.create_bucket()`

**Implementace:**
```python
# src/grpc/handlers/bucket.py
class CreateBucketHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        cmd = bucket_pb2.CreateBucketCommand()
        command.Unpack(cmd)

        project_id = cmd.projectId
        bucket_id = cmd.bucketId  # "out.c-my-bucket"
        branch_id = cmd.branchId if cmd.branchId else "default"

        # Parse bucket_id -> stage, name
        # "out.c-my-bucket" -> stage="out", name="c-my-bucket"
        parts = bucket_id.split(".", 1)
        stage = parts[0] if len(parts) > 1 else "in"
        name = parts[1] if len(parts) > 1 else bucket_id

        # Pouzij existujici logiku
        from src.database import ProjectDBManager
        manager = ProjectDBManager()
        result = manager.create_bucket(project_id, name, stage, branch_id)

        response = bucket_pb2.CreateBucketResponse()
        response.createBucketObjectName = result['name']  # "out_c_my_bucket"
        response.path.extend([project_id, result['name']])
        return response
```

---

### 2. DropBucketHandler

**Proto command:** `DropBucketCommand`
```protobuf
message DropBucketCommand {
  string bucketObjectName = 1;  // "out_c_my_bucket"
  string projectReadOnlyRoleName = 3;
  bool isCascade = 5;
}
```

**Proto response:** None (void)

**Mapovani na REST:**
- REST endpoint: `DELETE /projects/{id}/branches/{branch}/buckets/{name}`
- Existujici logika: `ProjectDBManager.delete_bucket()`

**Implementace:**
```python
class DropBucketHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        cmd = bucket_pb2.DropBucketCommand()
        command.Unpack(cmd)

        project_id = credentials['project_id']
        bucket_name = cmd.bucketObjectName
        cascade = cmd.isCascade

        from src.database import ProjectDBManager
        manager = ProjectDBManager()
        manager.delete_bucket(project_id, bucket_name, cascade=cascade)

        self.log_info(f"Bucket {bucket_name} dropped")
        return None
```

---

### 3. CreateTableHandler

**Proto command:** `CreateTableCommand`
```protobuf
message CreateTableCommand {
  repeated string path = 1;       // ["project_id", "bucket_name"]
  string tableName = 2;           // "my_table"
  repeated TableColumnShared columns = 3;
  repeated string primaryKeysNames = 4;
}

message TableColumnShared {
  string name = 1;
  string type = 2;
  string length = 3;
  bool nullable = 4;
  string default = 5;
}
```

**Proto response:** None (void) - ale v DuckDB vratime TableInfo

**Mapovani na REST:**
- REST endpoint: `POST /projects/{id}/branches/{branch}/buckets/{bucket}/tables`
- Existujici logika: `ProjectDBManager.create_table()`

**Implementace:**
```python
class CreateTableHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        cmd = table_pb2.CreateTableCommand()
        command.Unpack(cmd)

        # Path: ["project_id", "bucket_name"] nebo ["project_id", "branch_id", "bucket_name"]
        path = list(cmd.path)
        table_name = cmd.tableName

        # Parse path
        if len(path) >= 2:
            project_id = path[0]
            bucket_name = path[-1]  # posledni element
            branch_id = path[1] if len(path) > 2 else "default"
        else:
            raise ValueError("Invalid path")

        # Convert columns
        columns = []
        for col in cmd.columns:
            columns.append({
                'name': col.name,
                'type': col.type or 'VARCHAR',
                'nullable': col.nullable,
                'default': col.default if col.default else None,
            })

        primary_keys = list(cmd.primaryKeysNames)

        from src.database import ProjectDBManager
        manager = ProjectDBManager()
        result = manager.create_table(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            branch_id=branch_id
        )

        self.log_info(f"Table {table_name} created in {bucket_name}")
        return None  # CreateTableCommand nema response v proto
```

---

### 4. DropTableHandler

**Proto command:** `DropTableCommand`
```protobuf
message DropTableCommand {
  repeated string path = 1;
  string tableName = 2;
}
```

**Proto response:** None

**Implementace:**
```python
class DropTableHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        cmd = table_pb2.DropTableCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName

        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        from src.database import ProjectDBManager
        manager = ProjectDBManager()
        manager.delete_table(project_id, bucket_name, table_name, branch_id)

        self.log_info(f"Table {table_name} dropped")
        return None
```

---

### 5. ObjectInfoHandler

**Proto command:** `ObjectInfoCommand`
```protobuf
message ObjectInfoCommand {
  repeated string path = 1;
  ObjectType expectedObjectType = 2;  // DATABASE, SCHEMA, TABLE, VIEW
}
```

**Proto response:** `ObjectInfoResponse`
```protobuf
message ObjectInfoResponse {
  repeated string path = 1;
  ObjectType objectType = 2;
  oneof objectInfo {
    DatabaseInfo databaseInfo = 3;
    SchemaInfo schemaInfo = 4;
    ViewInfo viewInfo = 5;
    TableInfo tableInfo = 6;
  }
}

message TableInfo {
  repeated string path = 1;
  string tableName = 2;
  repeated TableColumn columns = 3;
  repeated string primaryKeysNames = 4;
  int64 rowsCount = 5;
  int64 sizeBytes = 6;
}
```

**Mapovani na REST:**
- REST endpoint: `GET /projects/{id}/branches/{branch}/buckets/{bucket}/tables/{table}`
- Existujici logika: `ProjectDBManager.get_table_info()`

**Implementace:**
```python
class ObjectInfoHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        cmd = info_pb2.ObjectInfoCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        expected_type = cmd.expectedObjectType

        response = info_pb2.ObjectInfoResponse()
        response.path.extend(path)

        if expected_type == info_pb2.TABLE:
            # path: ["project_id", "bucket_name", "table_name"]
            project_id = path[0]
            bucket_name = path[1] if len(path) > 1 else None
            table_name = path[2] if len(path) > 2 else None

            from src.database import ProjectDBManager
            manager = ProjectDBManager()
            table_data = manager.get_table_info(project_id, bucket_name, table_name)

            response.objectType = info_pb2.TABLE
            table_info = response.tableInfo
            table_info.path.extend(path[:-1])
            table_info.tableName = table_name
            table_info.rowsCount = table_data.get('row_count', 0)
            table_info.sizeBytes = table_data.get('size_bytes', 0)

            # Columns
            for col in table_data.get('columns', []):
                tc = table_info.columns.add()
                tc.name = col['name']
                tc.type = col['type']
                tc.nullable = col.get('nullable', True)

            # Primary keys
            table_info.primaryKeysNames.extend(table_data.get('primary_keys', []))

        elif expected_type == info_pb2.SCHEMA:
            # Bucket info - list tables
            project_id = path[0]
            bucket_name = path[1] if len(path) > 1 else None

            from src.database import ProjectDBManager
            manager = ProjectDBManager()
            tables = manager.list_tables(project_id, bucket_name)

            response.objectType = info_pb2.SCHEMA
            schema_info = response.schemaInfo
            for t in tables:
                obj = schema_info.objects.add()
                obj.objectName = t['name']
                obj.objectType = info_pb2.TABLE

        return response
```

---

### 6. PreviewTableHandler

**Proto command:** `PreviewTableCommand`
```protobuf
message PreviewTableCommand {
  repeated string path = 1;
  string tableName = 2;
  repeated string columns = 3;
  repeated ExportOrderBy orderBy = 4;
  ExportFilters filters = 5;
}
```

**Proto response:** `PreviewTableResponse`
```protobuf
message PreviewTableResponse {
  repeated string columns = 1;
  repeated Row rows = 2;
  message Row {
    repeated Column columns = 1;
    message Column {
      string columnName = 1;
      google.protobuf.Value value = 2;
      bool isTruncated = 3;
    }
  }
}
```

**Mapovani na REST:**
- REST endpoint: `GET /projects/{id}/branches/{branch}/buckets/{bucket}/tables/{table}/preview`
- Existujici logika: `routers/tables.py::preview_table()`

**Implementace:**
```python
class PreviewTableHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        cmd = table_pb2.PreviewTableCommand()
        command.Unpack(cmd)

        path = list(cmd.path)
        table_name = cmd.tableName
        columns_filter = list(cmd.columns) if cmd.columns else None
        limit = cmd.filters.limit if cmd.filters and cmd.filters.limit else 100

        project_id = path[0]
        bucket_name = path[-1]
        branch_id = path[1] if len(path) > 2 else "default"

        from src.database import ProjectDBManager
        manager = ProjectDBManager()
        preview_data = manager.preview_table(
            project_id, bucket_name, table_name,
            branch_id=branch_id,
            columns=columns_filter,
            limit=limit
        )

        response = table_pb2.PreviewTableResponse()
        response.columns.extend(preview_data['columns'])

        for row_data in preview_data['rows']:
            row = response.rows.add()
            for col_name, value in row_data.items():
                col = row.columns.add()
                col.columnName = col_name
                # Convert to protobuf Value
                from google.protobuf import struct_pb2
                if value is None:
                    col.value.null_value = struct_pb2.NullValue.NULL_VALUE
                elif isinstance(value, bool):
                    col.value.bool_value = value
                elif isinstance(value, (int, float)):
                    col.value.number_value = float(value)
                else:
                    col.value.string_value = str(value)
                col.isTruncated = False

        return response
```

---

### 7. TableImportFromFileHandler

**Proto command:** `TableImportFromFileCommand`
```protobuf
message TableImportFromFileCommand {
  FileProvider fileProvider = 1;      // S3, ABS, GCS
  FileFormat fileFormat = 2;          // CSV
  Any formatTypeOptions = 3;          // CsvTypeOptions
  FilePath filePath = 4;              // root, path, fileName
  Any fileCredentials = 5;            // S3Credentials
  Table destination = 6;              // path, tableName
  ImportOptions importOptions = 7;    // timestampColumn, importType, dedupType
}
```

**Proto response:** `TableImportResponse`
```protobuf
message TableImportResponse {
  int64 importedRowsCount = 1;
  int64 tableRowsCount = 2;
  int64 tableSizeBytes = 3;
  repeated Timer timers = 4;
  repeated string importedColumns = 5;
}
```

**Mapovani na REST:**
- REST endpoint: `POST /projects/{id}/branches/{branch}/buckets/{bucket}/tables/{table}/import/file`
- Existujici logika: `routers/table_import.py::import_from_file()`

**Implementace:**
```python
class TableImportFromFileHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        cmd = table_pb2.TableImportFromFileCommand()
        command.Unpack(cmd)

        # Destination table
        dest = cmd.destination
        dest_path = list(dest.path)
        table_name = dest.tableName
        project_id = dest_path[0]
        bucket_name = dest_path[-1]

        # File path (S3 compatible)
        file_path = cmd.filePath
        file_url = f"s3://{file_path.root}/{file_path.path}/{file_path.fileName}"

        # S3 Credentials
        s3_creds = table_pb2.ImportExportShared.S3Credentials()
        cmd.fileCredentials.Unpack(s3_creds)

        # Import options
        opts = cmd.importOptions
        import_type = "full" if opts.importType == 0 else "incremental"

        # CSV options
        csv_opts = table_pb2.TableImportFromFileCommand.CsvTypeOptions()
        cmd.formatTypeOptions.Unpack(csv_opts)

        from src.database import ProjectDBManager
        manager = ProjectDBManager()

        # Pro DuckDB - stahneme soubor lokalne nebo pouzijeme httpfs
        result = manager.import_from_file(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            file_url=file_url,
            s3_credentials={
                'key': s3_creds.key,
                'secret': s3_creds.secret,
                'region': s3_creds.region,
                'token': s3_creds.token,
            },
            import_type=import_type,
            delimiter=csv_opts.delimiter or ',',
            columns=list(csv_opts.columnsNames),
        )

        response = table_pb2.TableImportResponse()
        response.importedRowsCount = result['imported_rows']
        response.tableRowsCount = result['total_rows']
        response.tableSizeBytes = result['size_bytes']
        response.importedColumns.extend(result['columns'])

        return response
```

---

### 8. TableExportToFileHandler

**Proto command:** `TableExportToFileCommand`
```protobuf
message TableExportToFileCommand {
  Table source = 1;
  FileProvider fileProvider = 2;
  FileFormat fileFormat = 3;
  FilePath filePath = 4;
  Any fileCredentials = 5;
  ExportOptions exportOptions = 6;
}
```

**Proto response:** `TableExportToFileResponse`
```protobuf
message TableExportToFileResponse {
  TableInfo tableInfo = 1;
}
```

**Mapovani na REST:**
- REST endpoint: `POST /projects/{id}/branches/{branch}/buckets/{bucket}/tables/{table}/export`
- Existujici logika: `routers/table_import.py::export_table()`

**Implementace:**
```python
class TableExportToFileHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        cmd = table_pb2.TableExportToFileCommand()
        command.Unpack(cmd)

        # Source table
        src = cmd.source
        src_path = list(src.path)
        table_name = src.tableName
        project_id = src_path[0]
        bucket_name = src_path[-1]

        # Destination file
        file_path = cmd.filePath
        dest_url = f"s3://{file_path.root}/{file_path.path}/{file_path.fileName}"

        # S3 Credentials
        s3_creds = table_pb2.ImportExportShared.S3Credentials()
        cmd.fileCredentials.Unpack(s3_creds)

        # Export options
        opts = cmd.exportOptions
        columns = list(opts.columnsToExport) if opts.columnsToExport else None

        from src.database import ProjectDBManager
        manager = ProjectDBManager()

        result = manager.export_to_file(
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
            dest_url=dest_url,
            s3_credentials={
                'key': s3_creds.key,
                'secret': s3_creds.secret,
                'region': s3_creds.region,
            },
            columns=columns,
            compressed=opts.isCompressed,
        )

        response = table_pb2.TableExportToFileResponse()
        # Fill tableInfo
        ti = response.tableInfo
        ti.path.extend(src_path)
        ti.tableName = table_name
        ti.rowsCount = result['rows_exported']

        return response
```

---

## Implementacni kroky

### Krok 1: Vytvorit soubory handleru

```bash
# Vytvorit nove handler soubory
touch duckdb-api-service/src/grpc/handlers/bucket.py
touch duckdb-api-service/src/grpc/handlers/table.py
touch duckdb-api-service/src/grpc/handlers/info.py
touch duckdb-api-service/src/grpc/handlers/import_export.py
```

### Krok 2: Implementovat handlery

1. `bucket.py` - CreateBucketHandler, DropBucketHandler
2. `table.py` - CreateTableHandler, DropTableHandler
3. `info.py` - ObjectInfoHandler, PreviewTableHandler
4. `import_export.py` - TableImportFromFileHandler, TableExportToFileHandler

### Krok 3: Registrovat v ServiceR

Update `src/grpc/servicer.py`:
```python
def _register_handlers(self) -> dict:
    return {
        # Existing
        'InitBackendCommand': (InitBackendHandler(...), backend_pb2.InitBackendCommand),
        'RemoveBackendCommand': (RemoveBackendHandler(...), backend_pb2.RemoveBackendCommand),
        'CreateProjectCommand': (CreateProjectHandler(...), project_pb2.CreateProjectCommand),
        'DropProjectCommand': (DropProjectHandler(...), project_pb2.DropProjectCommand),

        # NEW - Phase 12c
        'CreateBucketCommand': (CreateBucketHandler(...), bucket_pb2.CreateBucketCommand),
        'DropBucketCommand': (DropBucketHandler(...), bucket_pb2.DropBucketCommand),
        'CreateTableCommand': (CreateTableHandler(...), table_pb2.CreateTableCommand),
        'DropTableCommand': (DropTableHandler(...), table_pb2.DropTableCommand),
        'ObjectInfoCommand': (ObjectInfoHandler(...), info_pb2.ObjectInfoCommand),
        'PreviewTableCommand': (PreviewTableHandler(...), table_pb2.PreviewTableCommand),
        'TableImportFromFileCommand': (TableImportFromFileHandler(...), table_pb2.TableImportFromFileCommand),
        'TableExportToFileCommand': (TableExportToFileHandler(...), table_pb2.TableExportToFileCommand),
    }
```

### Krok 4: Update HTTP Bridge

Update `src/routers/driver.py` - pridat nove commands do mapovani.

### Krok 5: Napsat testy

```python
# tests/test_grpc_handlers_phase12c.py

def test_create_bucket_via_grpc():
    """Test CreateBucketCommand."""

def test_drop_bucket_via_grpc():
    """Test DropBucketCommand."""

def test_create_table_via_grpc():
    """Test CreateTableCommand."""

def test_drop_table_via_grpc():
    """Test DropTableCommand."""

def test_object_info_table():
    """Test ObjectInfoCommand for TABLE."""

def test_preview_table():
    """Test PreviewTableCommand."""

def test_import_from_file():
    """Test TableImportFromFileCommand."""

def test_export_to_file():
    """Test TableExportToFileCommand."""
```

### Krok 6: Integracni test s Connection

Po implementaci otestovat z Connection:
1. Vytvorit projekt pres Manage API
2. Vytvorit bucket pres Storage API
3. Vytvorit tabulku pres Storage API
4. Nahrat data
5. Zobrazit preview

---

## Pozadavky na existujici kod

### database.py - potrebne metody

Overit ze existuji:
- `create_bucket(project_id, name, stage, branch_id)` ✓
- `delete_bucket(project_id, bucket_name, cascade)` ✓
- `create_table(project_id, bucket_name, table_name, columns, primary_keys, branch_id)` ✓
- `delete_table(project_id, bucket_name, table_name, branch_id)` ✓
- `get_table_info(project_id, bucket_name, table_name)` ✓
- `list_tables(project_id, bucket_name)` ✓
- `preview_table(project_id, bucket_name, table_name, ...)` ✓

### Import/Export - potrebne metody

Mozna pridat/upravit:
- `import_from_file(project_id, bucket_name, table_name, file_url, s3_credentials, ...)`
- `export_to_file(project_id, bucket_name, table_name, dest_url, s3_credentials, ...)`

Aktualne REST API pouziva staged files - pro gRPC budeme potrebovat primo S3 URL.

---

## Casovy odhad

| Krok | Odhad |
|------|-------|
| Bucket handlers | 1h |
| Table handlers | 1h |
| Info/Preview handlers | 1.5h |
| Import/Export handlers | 2h |
| Servicer update | 30min |
| Tests | 2h |
| Integration testing | 1h |
| **Celkem** | **~9h** |

---

## Verifikace

Po dokonceni Phase 12c:

1. [x] 8 novych handleru implementovano
2. [x] Vsechny testy prochazi (75 gRPC tests total: 17 + 23 + 18 + 17)
3. [x] grpcurl commands fungujici:
   - CreateBucketCommand
   - DropBucketCommand
   - CreateTableCommand
   - DropTableCommand
   - ObjectInfoCommand
   - PreviewTableCommand
   - TableImportFromFileCommand
   - TableExportToFileCommand
4. [x] HTTP Bridge funguje pro vsechny nove commands (26 total)
5. [x] Z Connection lze vytvorit bucket a tabulku (Phase 12b.1 + 12b.2 DONE)

## Current Status (2024-12-21)

- **Phase 12c:** DONE - 8 core handlers
- **Phase 12d:** DONE - 6 schema handlers
- **Phase 12e:** DONE - 8 workspace handlers
- **Phase 12b.2:** DONE - Secure project API keys
- **HTTP Bridge:** 26 commands supported (with project key auth)
- **Total gRPC tests:** 75

---

## Navazujici faze

- **Phase 12f:** Sharing handlers (ShareBucket, UnshareBucket, LinkBucket, UnlinkBucket)
- **Phase 12g:** Advanced handlers (DevBranch, ExecuteQuery)
