# Connection DuckDB Patch

Apply these changes to make DuckDB work in Connection:

## 1. CredentialsResolver.php

Add DuckDB case to `legacy-app/application/src/Storage/Service/Backend/CredentialsResolver/CredentialsResolver.php`:

```php
case BackendSupportsInterface::BACKEND_BIGQUERY:
    $resolver = $this->resolvers->get(BigqueryCredentialsResolver::class);
    Assert::isInstanceOf($resolver, BigqueryCredentialsResolver::class);
    return $resolver;
case BackendSupportsInterface::BACKEND_DUCKDB:
    $resolver = $this->resolvers->get(DuckDBCredentialsResolver::class);
    Assert::isInstanceOf($resolver, DuckDBCredentialsResolver::class);
    return $resolver;
```

## 2. DuckDBCredentialsResolver.php (NEW FILE)

Create `legacy-app/application/src/Storage/Service/Backend/CredentialsResolver/DuckDBCredentialsResolver.php`:

```php
<?php

declare(strict_types=1);

namespace Keboola\Connection\Storage\Service\Backend\CredentialsResolver;

use Keboola\Core\Doctrine\DoctrineZendModelExchange;
use Keboola\Manage\Projects\Entity\Project;
use Keboola\Manage\StorageBackend\Entity\StorageCredentials;
use Keboola\Manage\StorageBackend\Entity\StorageRootCredentials;
use Keboola\Package\StorageBackend\BackendSupportsInterface;
use Keboola\Storage\Buckets\Entity\StorageBucket;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Model_Row_Bucket;
use Model_Row_ConnectionMysql;
use Model_Row_Project;

class DuckDBCredentialsResolver implements CredentialsResolverInterface
{
    public function __construct(
        private readonly DoctrineZendModelExchange $doctrineZendModelExchange,
    ) {
    }

    public function getProjectCredentials(
        Project|Model_Row_Project $project,
    ): GenericBackendCredentials {
        $projectEntity = $project;
        if ($project instanceof Model_Row_Project) {
            $projectEntity = $this->doctrineZendModelExchange->rowToEntity($project, Project::class);
        }
        assert($projectEntity instanceof Project);
        Assert::assertProjectSupportsBackend($projectEntity, BackendSupportsInterface::BACKEND_DUCKDB);

        return (new GenericBackendCredentials())
            ->setHost((string) $projectEntity->getId())
            ->setPrincipal('duckdb-project')
            ->setSecret('');
    }

    public function getRootCredentials(
        StorageRootCredentials|Model_Row_ConnectionMysql $connection,
    ): GenericBackendCredentials {
        $rootCredentialsEntity = $connection;
        if ($connection instanceof Model_Row_ConnectionMysql) {
            $rootCredentialsEntity = $this->doctrineZendModelExchange->rowToEntity($connection, StorageRootCredentials::class);
        }
        assert($rootCredentialsEntity instanceof StorageRootCredentials);

        return (new GenericBackendCredentials())
            ->setHost($rootCredentialsEntity->getHost())
            ->setPrincipal('duckdb-root')
            ->setSecret('');
    }

    public function getBucketCredentials(Model_Row_Bucket|StorageBucket $sourceObject): GenericBackendCredentials
    {
        return $this->getProjectCredentials($sourceObject->getProject());
    }

    public function getStorageCredentials(StorageCredentials $credentials): GenericBackendCredentials
    {
        return (new GenericBackendCredentials())
            ->setHost((string) $credentials->getProject()->getId())
            ->setPrincipal('duckdb-storage')
            ->setSecret('');
    }
}
```

## 3. Register DuckDBCredentialsResolver in services

Add to `legacy-app/application/config/services.yaml` or relevant service config:

```yaml
Keboola\Connection\Storage\Service\Backend\CredentialsResolver\DuckDBCredentialsResolver:
    arguments:
        $doctrineZendModelExchange: '@Keboola\Core\Doctrine\DoctrineZendModelExchange'
```

## 4. Add to ServiceLocator

Find where the credentials resolvers ServiceLocator is configured and add DuckDBCredentialsResolver.

## Already Applied Changes

These changes were already made:

1. **Projects.php** - Added `DefaultConnectionDuckdb` reference rule
2. **TableCreate.php** - Added `getDefaultColumnForBackend` and `getTimestampColumnForBackend` DuckDB cases
3. **vendor BackendSupportsInterface.php** - Added `BACKEND_DUCKDB` constant (temporary, needs proper package update)
