# Phase 14: Backend Plugin Architecture for Connection

**Status:** PROPOSAL
**Author:** Claude + Padak
**Date:** 2024-12-22

## Problem Statement

Adding a new backend (DeltaLake, DuckDB, etc.) to Connection requires changes in 15+ files with hardcoded switch/match statements. This makes the codebase hard to extend.

## Current State Analysis

### Three Waves of Backends

| Wave | Backends | Pattern | Communication |
|------|----------|---------|---------------|
| 1 (Legacy) | Redshift, Synapse, Exasol | Direct DBAL | Native database drivers |
| 2a (Hybrid) | Snowflake | Partial driver (4 handlers) | Most operations via DBAL, few via driver |
| 2b (Modern) | BigQuery | **Full driver (33+ handlers)** | ALL operations via driver protocol |
| 3 (On-prem) | DuckDB | HTTP bridge to external service | Protobuf over HTTP to Python API |

**Key insight:** BigQuery is the **reference implementation** for modern backends:
- Snowflake driver has only 4 handlers (CreateBucket, DevBranch, ProfileTable)
- BigQuery driver has 33+ handlers covering ALL operations (Project, Bucket, Table, Workspace, Import/Export, etc.)
- DuckDB follows BigQuery pattern - all operations via driver handlers

**Note:** All drivers use Protobuf messages for command/response structure, but:
- Snowflake is a **hybrid** - most operations still go through legacy DBAL code in Connection
- BigQuery is **fully driver-based** - Connection delegates everything to driver
- DuckDB is the only driver that calls an **external service** via HTTP

### Driver Architecture Details

All "driver-based" backends implement `ClientInterface` with `runCommand()` method:

```php
interface ClientInterface {
    public function runCommand(
        Message $credentials,      // GenericBackendCredentials (Protobuf)
        Message $command,          // Command like CreateTableCommand (Protobuf)
        array $features,           // Feature flags
        Message $runtimeOptions,   // Runtime config
    ): ?Message;                   // Response (Protobuf)
}
```

**How each driver implements it:**

| Driver | Handlers | Implementation | Location |
|--------|----------|---------------|----------|
| **Snowflake** | 4 (hybrid) | `HandlerFactory` → PHP handlers → DBAL | `storage-backend/packages/php-storage-driver-snowflake/` |
| **BigQuery** | 33+ (full) | `HandlerFactory` → PHP handlers → Google Cloud SDK | `php-storage-driver-bigquery/` (standalone repo, reference impl) |
| **DuckDB** | 26+ (full) | HTTP POST → Python API → handlers | `duckdb-api-service/src/grpc/handlers/` |

**DuckDB follows BigQuery pattern** - all operations via driver handlers. Has gRPC server ready (`duckdb-api-service/src/grpc/`) but PHP driver currently uses HTTP bridge.

### gRPC vs HTTP Bridge (Team Discussion Dec 2024)

**Context:** Zajca added gRPC interface to storage-backend in [PR #259](https://github.com/keboola/storage-backend/pull/259):
- Added `service.proto` with RPC interface
- Generated Python code for gRPC server
- Created example gRPC server implementation

**Key insight from Zajca:**
> "Driver API teď definuje datový model toho rozhraní ale ne rozhraní samotné. Původní záměr byl aby to rozhraní byla jedna metoda něco jako `execute` která dostane Request a vrátí Response"

**Why gRPC is preferred (Vojta Biberle):**
> "gRPC ti vygeneruje klienta i server a celou komunikaci mezi nimi vyřeší. Kdybys vygeneroval server a jen implementoval handlery, budeš to mít hned a nebudeš muset psát samotný PHP klient pro tvou service."

**What's missing for gRPC (Tomas Fejfar):**
> "do connection přidat generický grpc (to je ten http interface), abysme si mohli přes ty protocol buffery povídat i s něčím 'venku' mimo aplikaci"

**Current DuckDB situation:**
- We have gRPC server ready: `duckdb-api-service/src/grpc/server.py`
- We have all handlers implemented: `duckdb-api-service/src/grpc/handlers/`
- PHP driver uses HTTP bridge: `connection/Package/StorageDriverDuckdb/src/DuckDBDriverClient.php`
- **TODO:** Switch from HTTP to gRPC for better integration

**Recommended architecture:**
```
Connection → gRPC Client → network → gRPC Server → Python handlers → DuckDB
```

Instead of current:
```
Connection → HTTP Client → network → REST API → Python handlers → DuckDB
```

### Critical Hardcoding Points (15+ files)

**CRITICAL (must change for any backend):**
1. `CredentialsResolver.php` - switch on 7 backends (lines 71-106)
2. `DriverClientFactory.php` - 3 switch statements (lines 32-78)
3. `BackendAssign.php` - 5 different code paths (lines 79-356)
4. `CommonBackendConfigurationFactory.php` - match on 6 backends

**HIGH (workspace/table operations):**
5. `WorkspaceConfigurationFactory.php` - switch on 6 backends
6. `ColumnDefinitionFactory.php` - instanceof checks
7. `ImportTableCommandFactory.php` - backend-specific handling
8. `TestConnectionRequestFactory.php` - incomplete switch
9. `HealthCheckRequestFactory.php` - incomplete switch

**MEDIUM (legacy factories):**
10. `BucketBackend/Factory.php` - DI registered backends
11. `BackendNameGeneratorFactory.php` - if/if/if pattern
12. `FileBackupFactory.php` - file storage adapters

## Proposed Solution: BackendRegistry + Capabilities

### New Interface: `BackendRegistryInterface`

```php
interface BackendRegistryInterface
{
    // Core identification
    public function getBackendName(): string;  // 'duckdb', 'snowflake', etc.

    // Capabilities (replaces scattered if/switch statements)
    public function getCapabilities(): BackendCapabilities;

    // Factory methods (replaces factory switches)
    public function createCredentialsResolver(): CredentialsResolverInterface;
    public function createDriverClient(): ?ClientInterface;  // null for legacy
    public function createConfig(): DriverConfigInterface;

    // Optional specialized factories
    public function createWorkspaceConfiguration(mixed $context): ?WorkspaceConfiguration;
    public function createNameGenerator(): ?BackendNameGeneratorInterface;
}
```

### BackendCapabilities Value Object

```php
final class BackendCapabilities
{
    public function __construct(
        public readonly bool $isDriverBased,     // true = uses driver protocol (Protobuf handlers), false = legacy DBAL
        public readonly bool $supportsWorkspace,
        public readonly bool $supportsSharing,
        public readonly bool $supportsBranches,
        public readonly bool $isCaseSensitive,
        public readonly array $supportedFileStorages,  // ['s3', 'gcs', 'abs', 'local']
        public readonly array $unsupportedFilterTypes,
        public readonly ?string $createTableMetaClass,
    ) {}
}
```

### Central Registry Service

```php
final class BackendRegistry
{
    /** @var array<string, BackendRegistryInterface> */
    private array $backends = [];

    public function register(BackendRegistryInterface $backend): void
    {
        $this->backends[$backend->getBackendName()] = $backend;
    }

    public function get(string $name): BackendRegistryInterface
    {
        return $this->backends[$name] ?? throw new UnsupportedBackendException($name);
    }

    public function getCredentialsResolver(string $backend): CredentialsResolverInterface
    {
        return $this->get($backend)->createCredentialsResolver();
    }

    public function getDriverClient(string $backend): ?ClientInterface
    {
        return $this->get($backend)->createDriverClient();
    }

    public function isDriverBased(string $backend): bool
    {
        return $this->get($backend)->getCapabilities()->isDriverBased;
    }
}
```

## Implementation Phases

### Phase 1: Create Infrastructure (RECOMMENDED - do now)

**Goal:** Create BackendRegistry + DuckDB as reference implementation

1. Create interfaces:
   - `BackendRegistryInterface` - what each backend must provide
   - `BackendCapabilities` - feature flags per backend
   - `BackendRegistry` - central registry service

2. Create DuckDB backend as reference:
   - `DuckDBBackend implements BackendRegistryInterface`
   - Register in Symfony DI

3. Benefits:
   - New backends (DeltaLake, etc.) can use the new pattern
   - Existing code continues to work (no breaking changes)
   - Serves as documentation for "how to add a backend"

### Phase 2-4: Migrate Factories (FUTURE - post-MVP)

**Goal:** Gradually migrate existing switch statements to use registry

- Phase 2: Critical factories (CredentialsResolver, DriverClientFactory)
- Phase 3: Secondary factories (WorkspaceConfigurationFactory, ColumnDefinitionFactory)
- Phase 4: Legacy factories (BucketBackend/Factory)

**Approach:** Facade pattern - old factories will call BackendRegistry internally
- No breaking changes for existing code
- Gradual migration without big bang refactor

## Example: DuckDB Backend Registration

```php
final class DuckDBBackend implements BackendRegistryInterface
{
    public function __construct(
        private readonly string $serviceUrl,
        private readonly string $adminApiKey,
    ) {}

    public function getBackendName(): string
    {
        return BackendSupportsInterface::BACKEND_DUCKDB;
    }

    public function getCapabilities(): BackendCapabilities
    {
        return new BackendCapabilities(
            isDriverBased: true,
            supportsWorkspace: true,
            supportsSharing: true,
            supportsBranches: true,
            isCaseSensitive: true,
            supportedFileStorages: ['local', 's3'],
            unsupportedFilterTypes: ['BLOB', 'JSON', 'ARRAY', 'STRUCT', 'MAP'],
            createTableMetaClass: null,
        );
    }

    public function createCredentialsResolver(): CredentialsResolverInterface
    {
        return new DuckDBCredentialsResolver();
    }

    public function createDriverClient(): ClientInterface
    {
        return new DuckDBDriverClient($this->serviceUrl, $this->adminApiKey);
    }

    public function createConfig(): DriverConfigInterface
    {
        return new DuckDBConfig();
    }

    public function createWorkspaceConfiguration(mixed $context): ?WorkspaceConfiguration
    {
        return new DriverWorkspaceConfiguration($context);
    }

    public function createNameGenerator(): ?BackendNameGeneratorInterface
    {
        return null; // DuckDB doesn't need name generator
    }
}
```

## Symfony DI Configuration

```yaml
# services.yaml
services:
    # Auto-register all backends tagged with 'keboola.backend'
    Keboola\Package\StorageBackend\Registry\BackendRegistry:
        arguments:
            $backends: !tagged_iterator keboola.backend

    # Register backends
    Keboola\Package\StorageBackend\Backend\DuckDBBackend:
        arguments:
            $serviceUrl: '%env(DUCKDB_SERVICE_URL)%'
            $adminApiKey: '%env(DUCKDB_ADMIN_API_KEY)%'
        tags: ['keboola.backend']

    Keboola\Package\StorageBackend\Backend\SnowflakeBackend:
        tags: ['keboola.backend']

    # ... etc for each backend
```

## Benefits

1. **Single registration point** - New backend = 1 class + DI config
2. **Explicit capabilities** - No more scattered if/switch for features
3. **Type safety** - Interfaces enforce what backends must provide
4. **Testability** - Easy to mock/stub backends in tests
5. **Documentation** - Registry serves as living documentation of backends

## Decisions Made

- **Scope:** Phase 1 only (infrastructure + DuckDB reference)
- **Backward compatibility:** Facade pattern (old code works, no breaking changes)
- **Location:** `Package/StorageBackend` (clean, reusable)

## Phase 1 Implementation Steps

### Step 1: Create BackendCapabilities Value Object
```
File: Package/StorageBackend/src/Registry/BackendCapabilities.php
```
- Immutable object with feature flags
- Replace scattered if/switch checks with capability queries

### Step 2: Create BackendRegistryInterface
```
File: Package/StorageBackend/src/Registry/BackendRegistryInterface.php
```
- Define contract for backend implementations
- Include factory methods for credentials, clients, configs

### Step 3: Create BackendRegistry Service
```
File: Package/StorageBackend/src/Registry/BackendRegistry.php
```
- Central registry using Symfony tagged services
- Lookup by backend name

### Step 4: Create DuckDBBackend Implementation
```
File: Package/StorageBackend/src/Backend/DuckDBBackend.php
```
- Reference implementation
- Uses existing DuckDBConfig, DuckDBCredentialsResolver, DuckDBDriverClient

### Step 5: Register in Symfony DI
```
File: Package/StorageBackend/services.yaml
```
- Tag DuckDBBackend with 'keboola.backend'
- Configure BackendRegistry to collect tagged services

### Step 6: Create Documentation
```
File: Package/StorageBackend/docs/adding-new-backend.md
```
- Step-by-step guide for adding new backends
- Use DuckDB as example

## Files to Create (Phase 1)

| File | Description |
|------|-------------|
| `Package/StorageBackend/src/Registry/BackendCapabilities.php` | Feature flags value object |
| `Package/StorageBackend/src/Registry/BackendRegistryInterface.php` | Backend contract |
| `Package/StorageBackend/src/Registry/BackendRegistry.php` | Central registry |
| `Package/StorageBackend/src/Backend/DuckDBBackend.php` | DuckDB implementation |
| `Package/StorageBackend/docs/adding-new-backend.md` | Documentation |

## Files to Modify (Phase 1)

| File | Change |
|------|--------|
| `Package/StorageBackend/services.yaml` | Add DI configuration |
| `Package/StorageBackend/composer.json` | Add autoload for new namespace |

## Success Criteria

1. BackendRegistry can return DuckDB backend by name
2. DuckDBBackend provides all required factory methods
3. Existing code continues to work (no changes to factories yet)
4. Documentation explains how to add a new backend (e.g., DeltaLake)

## Open Questions for Discussion

1. **Should we include ALL existing backends in Phase 1?** (Snowflake, BigQuery, etc.) or just DuckDB as reference?
2. **How to handle legacy backends** (Redshift, Synapse, Exasol) that don't use driver protocol?
3. **Should capabilities be extensible?** (Allow backends to add custom capabilities)
4. **Integration with existing `DriverConfigInterface`** - merge or keep separate?

## Related Documents

- `docs/adr/014-grpc-driver-interface.md` - gRPC protocol design
- `docs/plan/phase-12-php-driver.md` - DuckDB driver integration
- `docs/bigquery-driver-research.md` - BigQuery driver analysis
- [Zajca's PR #259](https://github.com/keboola/storage-backend/pull/259) - Added gRPC service.proto and Python example
- `storage-backend/packages/php-storage-driver-common/proto/service.proto` - gRPC service definition
- `storage-backend/packages/php-storage-driver-common/generated-py/examples/grpc_server.py` - Example gRPC server
