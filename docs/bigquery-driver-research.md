# BigQuery Driver Research

> **Cil:** Prostudovat BigQuery driver jako referencni implementaci pro DuckDB driver.
> **Zdroj:** `php-storage-driver-bigquery/src/`
> **Generovano:** 2024-12-15 pomoci 9 paralelnich research agentu

---

## Prehled struktury

```
php-storage-driver-bigquery/src/
├── BigQueryDriverClient.php      # Entry point - implements ClientInterface
├── Handler/
│   ├── HandlerFactory.php        # Command -> Handler dispatch (match expression)
│   ├── Backend/                  # InitBackend, RemoveBackend
│   ├── Project/                  # Create, Update, Drop
│   ├── Bucket/                   # CRUD + Share/Link
│   ├── Table/                    # CRUD + Import/Export + Alter
│   ├── Workspace/                # CRUD + Clear
│   ├── Info/                     # ObjectInfo
│   └── ExecuteQuery/             # SQL execution
├── GCPClientManager.php          # GCP API client factory
├── CredentialsHelper.php         # Credentials extraction
├── NameGenerator.php             # Naming conventions
└── ExceptionHandler.php          # Error mapping
```

**Pocet handleru:** 27 (z toho 2 jsou EmptyHandler pro dev branches)

---

## 1. Komunikacni vrstva

### ClientInterface kontrakt

```php
public function runCommand(
    Message $credentials,      // GenericBackendCredentials
    Message $command,          // Konkretni command (CreateTableCommand, etc.)
    array $features,           // String[] - capability flags
    Message $runtimeOptions,   // RuntimeOptions
): ?Message                    // DriverResponse
```

### GenericBackendCredentials struktura

```
GenericBackendCredentials:
├── host (string) - hostname
├── principal (string) - JSON encoded service account info
├── secret (string) - private key
├── port (uint32) - connection port
└── meta (Any) - backend-specific metadata
    └── BigQueryCredentialsMeta:
        ├── folder_id - GCP folder ID
        └── region - resource location
```

### RuntimeOptions struktura

```
RuntimeOptions:
├── runId (string) - execution identifier
├── queryTags (map<string,string>) - labels for queries (branchId, projectId)
└── meta (Any) - backend-specific runtime config
```

### DriverResponse wrapper

```php
$response = new DriverResponse();
$any = new Any();
$any->pack($handledResponse);  // Pack handler response
$response->setCommandResponse($any);
$response->setMessages($handler->getMessages());  // Log messages
return $response;
```

**Pro DuckDB:** Stejny pattern, jen jednodussi credentials (API URL + auth token).

---

## 2. Backend Handlery

### InitBackendHandler - co validuje

BigQuery validuje 4 veci:
1. **Folder access** - `folders.get`, `folders.list`
2. **Folder permissions** - 6 permissions (projects.create, get, getIamPolicy, list)
3. **Root project IAM roles** - `roles/owner` nebo `roles/storage.objectAdmin`
4. **Billing account** - `roles/billing.user`

**Error handling:**
- Sbira vsechny chyby do `InitExceptionDTO[]`
- Hazi `InitBackendFailedException` s agregovanou zpravou
- Non-retryable exception

**Pro DuckDB:**
```python
# Jednoducha validace:
1. GET /health na Python API
2. Overit /data/duckdb/ existuje a je zapisovatelny
3. Return InitBackendResponse()
```

### RemoveBackendHandler

- **NO-OP** - vraci `null`, nic necisti
- Cleanup je na vyssi urovni (service lifecycle)
- Idempotentni

---

## 3. Project Handlery

### CreateProjectHandler - 11 kroku

1. Generate project ID: `{stackPrefix}-{projectId}-{4-char-hash}`
2. Get billing account from parent
3. Create GCP project in folder
4. Enable 6 GCP services (BigQuery, IAM, Billing, etc.)
5. Attach billing account
6. Create service account: `{stackPrefix}-{projectId}`
7. Grant GCS bucket access (roles/storage.objectAdmin)
8. Wait for IAM propagation (retry loop)
9. Set IAM on project (dataOwner, jobUser, owner)
10. Create service account credentials (key pair)
11. Create Analytics Hub Data Exchange

**Returns:**
```
CreateProjectResponse:
├── projectUserName - public key JSON
├── projectPassword - private key
└── projectReadOnlyRoleName - data exchange ID
```

**Pro DuckDB:**
```python
def create_project(project_id, stack_prefix):
    db_path = f"/data/duckdb/project_{stack_prefix}_{project_id}_main.duckdb"
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS main")
    return CreateProjectResponse(projectUserName=db_path)
```

### DropProjectHandler - HARD DELETE

1. Remove service account from GCS bucket policy
2. Delete all service accounts in project
3. Disable billing
4. Delete Analytics Hub Data Exchange
5. Delete GCP project (async, polling)

**PolicyFilter:** Kriticky helper - resetuje array indexy v IAM policy (GCP requirement).

---

## 4. Bucket Handlery

### CreateBucketHandler

- Vytvori BigQuery dataset
- Naming: `NameGenerator::createObjectNameForBucketInProject(bucketId, branchId)`
- Location z credentials metadata

**Pro DuckDB:** `CREATE SCHEMA {bucket_name}`

### Share vs Link

| Aspekt | ShareBucket | LinkBucket |
|--------|-------------|------------|
| Co dela | Vytvori Analytics Hub Listing | Subscribuje listing, vytvori dataset |
| Kdo | Source project owner | Target project owner |
| Vysledek | Bucket je shareable | Lokalni views na source |

**Pro DuckDB:**
- Share = metadata (zaregistrovat bucket jako shareable)
- Link = `ATTACH source.duckdb AS source_proj (READ_ONLY)` + `CREATE VIEW`

---

## 5. Table Handlery

### Column definition

```
TableColumnShared:
├── name (string)
├── type (Keboola datatype: STRING, INTEGER, DECIMAL, DATE, etc.)
├── length (optional)
├── nullable (bool)
├── default (optional)
└── meta (Any) - backend-specific
```

### Primary Key

- BigQuery: **Metadata only** (logical, not enforced)
- `tableConstraints.primaryKey.columns` v REST API
- AddPrimaryKeyHandler validuje:
  - Zadne duplicity (ROW_NUMBER OVER PARTITION BY)
  - Vsechny PK columns NOT NULL
  - Tabulka jeste nema PK

**Pro DuckDB:** Nativni PRIMARY KEY constraint (enforced).

### ObjectInfoHandler returns

```
ObjectInfoResponse:
├── path (repeated string)
├── objectType (DATABASE|SCHEMA|TABLE|VIEW)
└── objectInfo (oneof):
    ├── databaseInfo - list of schemas
    ├── schemaInfo - list of tables/views
    ├── tableInfo - columns, PK, rowCount, sizeBytes
    └── viewInfo - columns, definition
```

---

## 6. Import/Export Handlery (KRITICKE)

### Import from File - 3-stage pipeline

```
1. STAGE: Load to staging table (raw import)
   ↓
2. TRANSFORM: Apply column mapping + deduplication
   - FullImporter (new tables)
   - IncrementalImporter (existing + dedup)
   ↓
3. CLEANUP: Drop staging table
```

### File formats

- **Import:** CSV only (assertion in code)
- **Export:** CSV only
- Compression: GZIP supported
- Sliced files: Wildcards supported (`*.csv`)

### Deduplication types

| DedupType | Behavior |
|-----------|----------|
| UPDATE_DUPLICATES | MERGE na PK (update existing) |
| INSERT_DUPLICATES | Allow duplicates |
| FAIL_ON_DUPLICATES | Error on duplicate |

### ImportContext / SourceContext

```
ImportContext (7-8 params consolidated):
├── bqClient
├── destination
├── destinationDefinition
├── importOptions
├── source (Table or SelectSource)
├── sourceTableDefinition
├── bigqueryImportOptions
└── sourceMapping

SourceContext:
├── source (SqlSourceInterface)
├── effectiveDefinition (filtered columns)
├── fullDefinition (complete schema)
└── selectedColumns
```

### TableImportResponse

```
TableImportResponse:
├── importedRowsCount (int64)
├── tableRowsCount (int64) - total after import
├── tableSizeBytes (int64)
├── timers[] - performance metrics
└── importedColumns[]
```

### Export - ExportQueryBuilder

Podporuje:
- WHERE filters (changeSince, changeUntil, whereFilters)
- ORDER BY
- LIMIT
- Fulltext search
- TABLESAMPLE pro preview (10% sample pro velke tabulky)

**Pro DuckDB:**
```sql
-- Import
COPY table FROM 's3://bucket/*.csv' (HEADER);
-- nebo
INSERT INTO dest SELECT * FROM staging WHERE ...;

-- Export
COPY (SELECT * FROM table WHERE ...) TO 's3://bucket/file.csv';
```

---

## 7. Workspace Handlery

### CreateWorkspaceHandler

1. Create service account: `{stackPrefix}-ws-{workspaceId}`
2. Create dataset: `WORKSPACE_{workspaceId}` (uppercase)
3. Configure IAM:
   - `dataViewer` with condition: `!resource.name.startsWith('WORKSPACE_')`
   - `jobUser`
   - `readSessionUser`
4. Generate credentials (key pair)

**Isolation mechanism:** IAM condition prevents reading other workspaces.

### Workspace vs Bucket

| Aspekt | Bucket | Workspace |
|--------|--------|-----------|
| Ownership | Project service account | Dedicated workspace SA |
| Isolation | None (all readable) | Strong (IAM condition) |
| Purpose | Permanent storage | Temporary working space |
| Lifetime | Long-lived | Ephemeral |

### Clear vs Drop

| Operation | Dataset | Service Account | Tables | IAM |
|-----------|---------|-----------------|--------|-----|
| CLEAR | Kept | Kept | Deleted | Unchanged |
| DROP | Deleted | Deleted | Deleted | Removed |

**Pro DuckDB:**
```sql
CREATE SCHEMA WORKSPACE_{id};
-- Clear:
DROP TABLE WORKSPACE_{id}.* WHERE NOT IN preserve_list;
-- Drop:
DROP SCHEMA WORKSPACE_{id} CASCADE;
```

---

## 8. Helpers

### GCPClientManager

Spravuje 9 typu GCP klientu:
- FoldersClient, ProjectsClient, ServiceUsageClient
- IAMServiceWrapper, BigQueryClient, CloudBillingClient
- StorageClient, AnalyticsHubServiceClient, ResourceManager

**Timeouts:**
- Connection: 10s
- Operation: 240s
- Default retries: 20

**Pro DuckDB:** Jeden HTTP client (httpx) se session poolingem.

### NameGenerator

```
Project ID: {stackPrefix}-{projectId}-{4-char-hash}
Service Account: {stackPrefix}-{projectId}
Dataset: {bucketId} nebo {branchId}_{bucketId}
Workspace: WORKSPACE_{workspaceId} (uppercase)
Data Exchange: {STACK_PREFIX}_{projectId}_RO
```

**Primo pouzitelne pro DuckDB.**

### Exception types

Non-retryable:
- CredentialsMetaRequiredException
- ProjectIdTooLongException
- ProjectWithProjectIdAlreadyExists
- QueryBuilderException
- ColumnNotFoundException
- InitBackendFailedException

Retryable (with backoff):
- Network errors, rate limiting, temp unavailability

---

## 9. Protobuf Commands - klicove struktury

### Import command

```
TableImportFromFileCommand:
├── fileProvider (S3|ABS|GCS)
├── fileFormat (CSV)
├── formatTypeOptions (CsvTypeOptions)
├── filePath (root, path, fileName)
├── fileCredentials (S3/ABS/GCS credentials)
├── destination (Table)
├── importOptions:
│   ├── timestampColumn
│   ├── importType (FULL|INCREMENTAL|VIEW|CLONE)
│   ├── dedupType (UPDATE|INSERT|FAIL)
│   ├── dedupColumnsNames[]
│   ├── importStrategy (STRING_TABLE|USER_DEFINED_TABLE)
│   └── createMode (CREATE|REPLACE)
└── meta (backend-specific)
```

### TableInfo (returned by many handlers)

```
TableInfo:
├── path[]
├── tableName
├── columns[] (name, type, length, nullable, default)
├── primaryKeysNames[]
├── rowsCount
├── sizeBytes
├── tableType (NORMAL|EXTERNAL)
└── meta (partitioning, clustering for BQ)
```

---

## 10. Doporuceni pro DuckDB implementaci

### Co prevzit

1. **ClientInterface pattern** - stejny kontrakt
2. **HandlerFactory match()** - dispatch pattern
3. **DriverResponse wrapper** - Any packing
4. **NameGenerator** - naming conventions
5. **3-stage import pipeline** - staging -> transform -> cleanup
6. **ImportContext/SourceContext** - parameter consolidation
7. **BaseHandler** - message management

### Co zjednodusit

1. **InitBackend** - jen ping API + check storage path
2. **CreateProject** - vytvorit soubor misto GCP projektu
3. **Credentials** - API URL + auth token misto service account JSON
4. **Workspace isolation** - schema-level misto IAM conditions
5. **Sharing** - ATTACH + views misto Analytics Hub

### Kriticke rozdily

| Aspekt | BigQuery | DuckDB |
|--------|----------|--------|
| Project | GCP project | .duckdb file |
| Bucket | Dataset | Schema |
| Primary Key | Metadata only | Enforced constraint |
| Sharing | Analytics Hub | ATTACH (READ_ONLY) |
| Workspace isolation | IAM conditions | Schema separation |
| Connection | Stateless | Stateful (pooling) |
| Billing | GCP billing | N/A |
| Services | Enable GCP APIs | N/A |

---

## Stav pruzkumu

| Sekce | Status |
|-------|--------|
| 1. Komunikacni vrstva | DONE |
| 2. Backend Handlery | DONE |
| 3. Project Handlery | DONE |
| 4. Bucket Handlery | DONE |
| 5. Table Handlery | DONE |
| 6. Import/Export | DONE |
| 7. Workspace Handlery | DONE |
| 8. Helpers | DONE |
| 9. Protobuf Commands | DONE |

**Pruzkum dokoncen:** 2024-12-15
