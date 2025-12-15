# Local Connection (Storage API) Development Setup

## AKTUALNI STAV (2024-12-15)

**Kde jsme:** Connection bezi lokalne a je funkcni pro zakladni operace s OBEMA backendy (Snowflake i BigQuery).

**Co funguje:**
- Vytvareni projektu (po uprave retention time - viz nize)
- Vytvareni tabulek v Snowflake
- BigQuery backend - pripraveny pro vytvareni BigQuery projektu
- Upload souboru pres API (nikoliv pres UI - viz "Znama omezeni")
- Storage API endpointy

**Co je hotovo:**
- Platform setup (case-sensitive volume pro MySQL)
- AWS CLI nakonfigurovan (profil `Keboola-Dev-Connection-Team-AWSAdministratorAccess`)
- GCP CLI nakonfigurovan, projekt `padak-storage-v3-dev` vytvoren
- Terraform provisioning dokoncen - vsechny AWS a GCP resources vytvoreny
- `.env.local` vytvoren s hodnotami z Terraform
- Docker images built (apache, supervisor, mysql, elasticsearch, node)
- PHP dependencies (composer install)
- Frontend dependencies (npm install, grunt)
- Config JSON vygenerovan
- Database migrace provedeny
- Dev user vytvoren (dev@keboola.com / devdevdev)
- Components a UI apps inicializovany
- Elasticsearch indexy vytvoreny
- OAuth klice vygenerovany
- S3 File Storage zaregistrovan (bucket: `padak-kbc-services-s3-files-storage-bucket`)
- GCS File Storage zaregistrovan (bucket: `kbc-padak-files-storage`)
- Snowflake backend zaregistrovan (host: `vceecnu-bz34672.snowflakecomputing.com`)
- BigQuery backend zaregistrovan (folder: `393339196668`)
- Snowflake retention time opraven na 1 den (pro Standard edition)
- GCP IAM permissions nastaveny pro BigQuery driver

**Jak pristoupit:**
- URL: https://localhost:8700/admin
- Login: dev@keboola.com / devdevdev

**Dulezite soubory:**
- `.env.local` - environment variables (obsahuje AWS/GCP credentials)
- `provisioning/local/terraform.tfstate` - Terraform state
- `provisioning/local/gcp.tf` - zmenen projekt na `padak-storage-v3-dev`
- `etc/oauth-server/devel_*.key` - OAuth RSA klice (vygenerovany manualne)

### Znama omezeni lokalniho setupu

| Co nefunguje | Proc | Workaround |
|--------------|------|------------|
| Upload souboru pres UI (`/upload-file`) | Endpoint neni soucasti open-source repo, je v separatni UI komponente | Pouzij API - viz sekce "Upload souboru pres API" |
| Job Queue (`/search/jobs`) | Queue API je separatni microservice | Ignorovat - neni kriticke |
| Refresh API tokenu v UI | Vola `/search/jobs` | Pouzij token z Network tabu v DevTools |

### Upload souboru pres API

```bash
# 1. Ziskej Storage API token (format: {projectId}-{tokenId}-{secret})
#    Najdes v Network tabu prohlizece pri praci s projektem
TOKEN="3-3-xxxxx"

# 2. Priprav upload
PREPARE=$(curl -sk -X POST "https://localhost:8700/v2/storage/files/prepare" \
  -H "X-StorageApi-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "test.csv", "sizeBytes": 100, "federationToken": true}')

echo "$PREPARE" | jq .

# 3. Extrahuj credentials a uploaduj na S3
# (credentials jsou v response.uploadParams.credentials)
```

---

## Proč to děláme

Tento projekt má za cíl vytvořit **on-premise Keboola s DuckDB backendem** (bez Snowflake, bez S3).

Abychom mohli implementovat nový DuckDB driver, potřebujeme:

1. **Rozjet lokální Connection** - vidět jak funguje existující systém
2. **Studovat Snowflake/BigQuery drivery** - jako referenci pro implementaci
3. **Pochopit Protocol Buffers** - jak Connection komunikuje s drivery

```
Tento dokument = Krok 1: Rozjet lokální Connection
                         ↓
              Pak: Studovat BigQuery driver (viz duckdb-driver-plan.md)
                         ↓
              Pak: Implementovat DuckDB driver
```

---

## O čem je tento dokument

Průvodce krok za krokem pro rozjetí Keboola Connection lokálně v Dockeru.

**Zdroj:** [connection/docs/DOCKER.md](../connection/docs/DOCKER.md) a [connection/docs/Terraform.md](../connection/docs/Terraform.md)

---

## Naše konfigurace

Pro tento projekt používáme:
- **AWS** - S3 file storage + SQS fronty (hlavní backend)
- **GCP** - pro BigQuery driver (referenční implementace)
- ~~Azure~~ - nepoužíváme

**AWS Account:** Dev-Product-Team (`355388614113`) - ne Connection-Team!
**GCP Project:** `padak-storage-v3-dev` (vytvořen pro tento experiment)

---

## Přehled kroků (TL;DR)

| Krok | Co delame | Status |
|------|-----------|--------|
| 1 | Platform setup (case-sensitive volume) | DONE |
| 2 | AWS CLI konfigurace | DONE |
| 3 | GCP CLI konfigurace | DONE |
| 4 | Terraform provisioning | DONE |
| 5 | Docker build | DONE |
| 6 | Dependencies (composer, npm) | DONE |
| 7 | Config JSON | DONE |
| 8 | Database init | DONE |
| 9 | Start aplikace | DONE |
| 10 | Register File Storage (S3) | DONE |
| 11 | Register Snowflake backend | DONE |

**Výsledek:** Connection běží na https://localhost:8700, login `dev@keboola.com` / `devdevdev`

---

## Prerequisites

### Required Software

```bash
# Na macOS nainstaluj přes Homebrew:
brew install terraform awscli jq
```

- Docker Engine `^17.12` with buildx
- Docker Compose `^1.18`
- Docker with minimum 4GB memory (required for Elasticsearch)
- Terraform
- jq
- AWS CLI
- GCP CLI (gcloud) - pro BigQuery

### Required Access

- **AWS:** Dev-Product-Team account (`355388614113`) s rolí `AWSAdministratorAccess`
- **GCP:** Keboola Google account
- **Snowflake:** keboolaconnectiondev.us-east-1.snowflakecomputing.com

## Step 1: Platform-Specific Setup

### macOS

Install Docker for Mac or [OrbStack](https://orbstack.dev/) (faster file access).

**Create case-sensitive volume for MySQL:**

MySQL requires a case-sensitive filesystem. Run from the connection repository root:

```bash
cd connection

# Create sparse image with case-sensitive filesystem
hdiutil create -type SPARSE -fs 'Case-sensitive Journaled HFS+' -size 1g -volname mysql-accounts ~/docker-image-mysql-accounts

# Mount the volume
hdiutil attach -mountpoint ./docker/.mysql-accounts-datadir ~/docker-image-mysql-accounts.sparseimage
```

> **Warning:** Volume is not mounted after Mac reboot. Re-mount with:
> ```bash
> hdiutil attach -mountpoint ./docker/.mysql-accounts-datadir ~/docker-image-mysql-accounts.sparseimage
> ```

### Windows

Install Docker for Windows.

**Enable case-sensitive filesystem for MySQL:**

```powershell
# Create directory and enable case-sensitivity
mkdir -p ./docker/.mysql-accounts-datadir/data
fsutil.exe file setCaseSensitiveInfo ./docker/.mysql-accounts-datadir/data enable

# Verify
fsutil.exe file setCaseSensitiveInfo ./docker/.mysql-accounts-datadir/data
```

### Linux

Standard Docker installation. No special filesystem setup needed.

## Step 2: Configure Cloud CLI Tools

### AWS CLI

Add to `~/.aws/config`:

```ini
[profile Keboola-Dev-Connection-Team-AWSAdministratorAccess]
sso_start_url = https://keboola.awsapps.com/start#/
sso_region = us-east-1
sso_account_id = 355388614113
sso_role_name = AWSAdministratorAccess
region = eu-central-1
output = json
```

> **Pozor:** Používáme `355388614113` (Dev-Product-Team), ne původní Connection-Team account!

Login:

```bash
aws sso login --profile=Keboola-Dev-Connection-Team-AWSAdministratorAccess
export AWS_PROFILE=Keboola-Dev-Connection-Team-AWSAdministratorAccess
```

Ověření:

```bash
aws sts get-caller-identity --profile=Keboola-Dev-Connection-Team-AWSAdministratorAccess
# Mělo by vrátit Account: 355388614113
```

### GCP CLI

```bash
# Instalace (pokud chybí)
brew install google-cloud-sdk

# Přihlášení
gcloud auth login
```

#### Vytvoření GCP projektu pro experiment

Potřebujeme vlastní GCP projekt, protože původní Terraform config používá hardcoded `gcp-dev-353411`.

```bash
# Zjisti dostupné organizace
gcloud organizations list
# Vrátí např.: keboola.com   939837775440

# Vytvoř projekt v Keboola organizaci (pokud máš oprávnění)
gcloud projects create padak-storage-v3-dev \
  --name="Padak Storage v3 Dev" \
  --organization=939837775440

# Nebo vytvoř osobní projekt (bez organizace)
gcloud projects create padak-storage-v3-dev \
  --name="Padak Storage v3 Dev"
```

> **Tip:** Pokud nemáš oprávnění vytvářet projekty v organizaci, vytvoř projekt ručně v GCP Console.

#### Nastavení projektu

```bash
# Nastav jako aktivní
gcloud config set project padak-storage-v3-dev

# Připoj billing account
gcloud billing accounts list
gcloud billing projects link padak-storage-v3-dev \
  --billing-account=BILLING_ACCOUNT_ID

# Povol potřebná API
gcloud services enable \
  pubsub.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  --project=padak-storage-v3-dev

# Application Default Credentials (pro Terraform)
gcloud auth application-default login
gcloud auth application-default set-quota-project padak-storage-v3-dev
```

> Azure CLI nepotřebujeme - nepoužíváme Azure.

## Step 3: Terraform Provisioning

Terraform vytváří cloud resources (S3 bucket, SQS fronty, GCS bucket, PubSub).

### Úpravy Terraform souborů (už hotovo)

Pro náš projekt jsme upravili Terraform konfiguraci:

1. **Vypnuli Azure** - přesunuto do `provisioning/local/disabled/`
2. **Přidali Dev-Product-Team** do `aws.tf` whitelist:
   ```hcl
   allowed_account_ids = ["532553470754", "355388614113"]
   ```
3. **Upravili `main.tf`** - odebrali Azure providery
4. **Změnili GCP projekt** v `gcp.tf`:
   ```hcl
   provider "google" {
     project = "padak-storage-v3-dev"  # původně bylo "gcp-dev-353411"
     region  = "us-central1"
   }
   ```

> **Důležité:** Původní `gcp.tf` měl hardcoded projekt `gcp-dev-353411`. Pokud bys to nezměnil, resources by se vytvořily v tom sdíleném projektu!

### Spuštění Terraformu

```bash
cd connection

# Vytvoř terraform.tfvars (hodnoty přímo, ne přes shell proměnné!)
cat > ./provisioning/local/terraform.tfvars << 'EOF'
name_prefix = "padak"
gcs_fileStorage_location = "us-central1"
EOF

# DŮLEŽITÉ: Vyčisti starou Terraform cache (kvůli Azure providerům)
rm -rf ./provisioning/local/.terraform
rm -f ./provisioning/local/.terraform.lock.hcl

# Inicializace (stáhne pouze AWS a GCP providery)
terraform -chdir=./provisioning/local init -upgrade

# Vytvoř resources (potvrdíš "yes")
terraform -chdir=./provisioning/local apply

# Vygeneruj .env.local z Terraform outputs
./provisioning/local/update-env.sh aws
```

> **Note:** Zkontroluj `.env.local` na duplicitní proměnné a odstraň prázdné.

#### Workaround: Cesta s mezerami

Pokud cesta k repozitáři obsahuje mezery (např. `Keboola Storage v3`), skript `update-env.sh` selže. Řešení:

```bash
# Manuálně získej Terraform outputs
cd connection
terraform -chdir=./provisioning/local output -json > /tmp/tf-outputs.json

# Zkopíruj .env.local.dist
cp .env.local.dist .env.local

# Doplň hodnoty z outputs do .env.local ručně nebo použij jq:
cat /tmp/tf-outputs.json | jq -r 'to_entries | .[] | "\(.key)=\(.value.value)"'
```

### Terraform Troubleshooting

#### Chyba: Azure provider not found / az CLI not found

Terraform cache obsahuje staré Azure providery. Řešení:

```bash
rm -rf ./provisioning/local/.terraform
rm -f ./provisioning/local/.terraform.lock.hcl
terraform -chdir=./provisioning/local init -upgrade
```

#### Chyba: Service account already exists

Pokud se service account vytvořil v předchozím pokusu, importuj ho:

```bash
terraform -chdir=./provisioning/local import \
  google_service_account.gcp_dev_service_account \
  projects/padak-storage-v3-dev/serviceAccounts/padak-dev@padak-storage-v3-dev.iam.gserviceaccount.com
```

#### Chyba: GCS bucket name not available

GCS bucket jména jsou globálně unikátní. Pokud bucket existuje v jiném projektu, smaž ho:

```bash
gcloud storage buckets delete gs://kbc-padak-files-storage --project=STARY_PROJEKT
gcloud storage buckets delete gs://kbc-padak-logs --project=STARY_PROJEKT
```

#### Chyba: AWS Account ID not allowed

Zkontroluj `aws.tf` - tvůj account ID musí být ve whitelist:

```hcl
allowed_account_ids = ["532553470754", "355388614113"]
```

### Co Terraform vytvoří (pro pozdější cleanup)

#### AWS Resources (region: `eu-central-1`, account: `355388614113`)

| Typ | Název | Účel |
|-----|-------|------|
| **S3 Buckets** | | |
| | `padak-kbc-services-s3-files-storage-bucket` | File storage pro Connection |
| | `padak-kbc-services-s3-logs-bucket` | Debug logy |
| | `padak-kbc-services-s3-elastic-snapshot-bucket` | Elasticsearch snapshots |
| **SQS Queues** | | |
| | `padak-kbc-services-QueueMain` | Hlavní fronta |
| | `padak-kbc-services-QueueEventsElastic` | Elasticsearch eventy |
| | `padak-kbc-services-QueueCommands` | Příkazy |
| | `padak-kbc-services-QueueJobStats` | Job statistiky |
| | `padak-kbc-services-QueueTableTriggers` | Table triggery |
| | `padak-kbc-services-QueueAuditLogEvents` | Audit log |
| | `padak-kbc-services-SearchIndexQueue` | Search index |
| **SNS Topics** | | |
| | `padak-kbc-services-EventsTopic` | Events pub/sub |
| | `padak-kbc-services-AuditLogTopic` | Audit log pub/sub |
| | `padak-kbc-services-SearchIndexTopic` | Search index pub/sub |
| **IAM** | | |
| | `padak-kbc-services` | Hlavní service user + access key |
| | `padak-kbc-services-s3-file-storage-user` | S3 file storage user + access key |

#### GCP Resources (projekt: `padak-storage-v3-dev`, region: `us-central1`)

| Typ | Název | Účel |
|-----|-------|------|
| **GCS Buckets** | | |
| | `kbc-padak-files-storage` | GCS file storage |
| | `kbc-padak-logs` | GCS logy |
| **PubSub Topics** | | |
| | `kbc-padak-main` | Hlavní topic |
| | `kbc-padak-events` | Events |
| | `kbc-padak-commands` | Commands |
| | `kbc-padak-audit-log-events` | Audit log |
| | `kbc-padak-search-index` | Search index |
| **PubSub Subscriptions** | | |
| | `kbc-padak-*-subscription` | Subscription pro každý topic |
| **Service Account** | | |
| | `padak-dev@padak-storage-v3-dev.iam.gserviceaccount.com` | Dev service account |

#### Cleanup příkazy

```bash
# Smazat všechny Terraform resources
cd connection
terraform -chdir=./provisioning/local destroy

# Nebo smazat celý GCP projekt (pokud byl vytvořen jen pro tento experiment)
gcloud projects delete padak-storage-v3-dev
```

#### Cleanup starých resources v gcp-dev-353411

Pokud se resources omylem vytvořily v `gcp-dev-353411` (před změnou `gcp.tf`), smaž je ručně:

```bash
# Smaž PubSub subscriptions
gcloud pubsub subscriptions delete kbc-padak-main-subscription --project=gcp-dev-353411 --quiet
gcloud pubsub subscriptions delete kbc-padak-events-subscription --project=gcp-dev-353411 --quiet
gcloud pubsub subscriptions delete kbc-padak-commands-subscription --project=gcp-dev-353411 --quiet
gcloud pubsub subscriptions delete kbc-padak-audit-log-events-subscription --project=gcp-dev-353411 --quiet
gcloud pubsub subscriptions delete kbc-padak-search-index-subscription --project=gcp-dev-353411 --quiet
gcloud pubsub subscriptions delete kbc-padak-table-triggers-subscription --project=gcp-dev-353411 --quiet

# Smaž PubSub topics
gcloud pubsub topics delete kbc-padak-main --project=gcp-dev-353411 --quiet
gcloud pubsub topics delete kbc-padak-events --project=gcp-dev-353411 --quiet
gcloud pubsub topics delete kbc-padak-commands --project=gcp-dev-353411 --quiet
gcloud pubsub topics delete kbc-padak-audit-log-events --project=gcp-dev-353411 --quiet
gcloud pubsub topics delete kbc-padak-search-index --project=gcp-dev-353411 --quiet

# Smaž GCS buckets
gcloud storage buckets delete gs://kbc-padak-files-storage --project=gcp-dev-353411
gcloud storage buckets delete gs://kbc-padak-logs --project=gcp-dev-353411

# Smaž service account
gcloud iam service-accounts delete padak-dev@gcp-dev-353411.iam.gserviceaccount.com --project=gcp-dev-353411 --quiet
```

> **Tip:** Všechny resources mají prefix `padak-` pro snadnou identifikaci v AWS/GCP konzoli.

## Step 4: Environment Configuration

Create `.env.local` from template and configure:

```bash
cp .env.local.dist .env.local
```

Ensure these are set in `.env.local`:

```env
APPLICATION_ENV=development
APP_ENV=dev
APP_DEBUG=true
APP_SECRET=your_random_secret_here
```

## Step 5: Build Docker Services

```bash
# Build init image and generate docker-compose.yml
docker build --tag 'keboola_connection_init' -f ./docker/Dockerfile ./docker/

# Generate docker-compose.yml (use -e 7 for Elasticsearch v7, required for ARM Macs)
# POZOR: Pokud cesta obsahuje mezery, pouzij uvozovky kolem $(pwd)!
# POZOR: Pouzij --elasticVersion=7 misto -e 7 (Symfony Console problem)

# For Linux:
docker run --user $(id -u):$(id -g) -v "$(pwd)":/var/connection -t keboola_connection_init php /code/init-docker.php --elasticVersion=7 --setUpUserGroups -u $(id -u) -g $(id -g) --ignoreDatadog

# For macOS/Windows:
docker run -v "$(pwd)":/var/connection -t keboola_connection_init php /code/init-docker.php --elasticVersion=7 --ignoreDatadog

# Copy projects env file
cp provisioning/projects/.env.dist provisioning/projects/.env

# Build Docker images (builduj jednotlive services, ne vsechno najednou - monorepo nema Dockerfile)
docker compose --env-file=.env.local build apache --build-arg IS_LOCAL_DEV=true
docker compose --env-file=.env.local build supervisor --build-arg IS_LOCAL_DEV=true
docker compose --env-file=.env.local build mysql-accounts elasticsearch-v7 node
```

> **Workaround - mezery v ceste:** Pokud mas cestu jako `Keboola Storage v3`, MUSIS pouzit `"$(pwd)"` s uvozovkami, jinak Docker selze s chybou `invalid reference format`.

> **Workaround - monorepo build:** Prikaz `docker compose build --build-arg IS_LOCAL_DEV=true` selze na `monorepo` service (chybi Dockerfile). Builduj services jednotlive.

## Step 6: Install Dependencies

```bash
# PHP dependencies
docker compose run --rm -w /var/www/html --entrypoint="bash -c" cli "composer install --no-scripts"

# Frontend dependencies
docker compose run --rm node npm install -q
docker compose run --rm node grunt
```

## Step 7: Create Configuration

```bash
docker compose run --rm cli ./docker/create-json.php development
```

## Step 8: Initialize Database

Wait a few seconds for MySQL to start, then:

```bash
# Create testing database
# POZOR: Pouzij --skip-ssl kvuli self-signed certificate error!
docker compose run --rm cli mysql -h mysql-accounts -u root -proot --skip-ssl accounts --execute "create database accounts_testing"
docker compose run --rm cli mysql -h mysql-accounts -u root -proot --skip-ssl accounts --execute "grant all on accounts_testing.* to user"

# Run migrations for testing environment
docker compose run -e APPLICATION_ENV=testing --rm cli ./migrations.sh migrations:migrate --no-interaction

# Run migrations for development environment
docker compose run --rm cli ./migrations.sh migrations:migrate --no-interaction

# Initialize user - POZOR: init-user.sh nefunguje kvuli SSL, vloz manualne:
docker compose run --rm cli mysql -h mysql-accounts -u user -ppassword --skip-ssl accounts --execute "
INSERT INTO bi_admins (id, name, email, password, salt, changePasswordUntil, mfaSecret, isActivated, isSuperAdmin, passwordResetToken, passwordResetRequestedAt, features, created, canAccessLogs)
VALUES (1, 'dev user', 'dev@keboola.com', '\$2y\$10\$KtF03RpHEQZSEi8/V944Cu2egTrJYRDJxH2hzaWTX3VjBh2eIXc9C', '', NULL, NULL, 1, 1, NULL, NULL, '', NOW(), 1);
INSERT INTO bi_rAdminsMaintainers (idAdmin, idMaintainer) VALUES (1, 1);
"

# Initialize components
docker compose run --rm cli ./docker/init-components.sh
docker compose run --rm cli php ./scripts/cli.php ui-apps:sync

# Create Elasticsearch indexes
docker compose run --rm cli php ./scripts/cli.php storage:elastic-roll-index files v7
docker compose run --rm cli php ./scripts/cli.php storage:elastic-events-roll-index v7
```

> **Workaround - MySQL SSL:** MySQL pouziva self-signed certifikat, coz zpusobi `TLS/SSL error: self-signed certificate in certificate chain`. Reseni: pridej `--skip-ssl` ke vsem mysql prikazum.

> **Workaround - init-user.sh:** Skript `./docker/init-user.sh` nefunguje kvuli SSL problemu. Vloz dev usera primo SQL prikazem vyse.

## Step 8b: Generate OAuth Keys

OAuth2 server potrebuje RSA klice. Pokud neexistuji, vygeneruj je:

```bash
# Zkontroluj jestli klice existuji
ls -la etc/oauth-server/

# Pokud chybi devel_private.key a devel_public.key, vygeneruj je:
openssl genrsa -out etc/oauth-server/devel_private.key 2048
openssl rsa -in etc/oauth-server/devel_private.key -pubout -out etc/oauth-server/devel_public.key
chmod 600 etc/oauth-server/devel_private.key
```

> **Poznamka:** Bez techto klicu aplikace selze s chybou `Invalid key supplied` pri kazdem requestu.

## Step 9: Start the Application

```bash
docker compose up apache supervisor
```

Application is now available at:

| URL | Description |
|-----|-------------|
| https://localhost:8700 | HTTPS (main) |
| http://localhost:8800 | HTTP |
| http://localhost:8704 | Supervisor UI |
| http://localhost:8707 | Cerebro (Elasticsearch UI) |
| http://localhost:8710 | Mailhog (email testing) |

**Default credentials:**
- **Email:** `dev@keboola.com`
- **Password:** `devdevdev`

## Step 10: Register File Storage

1. Login at https://localhost:8700
2. Get Manage API token from https://localhost:8700/admin/account/access-tokens
3. Register file storage using credentials from Terraform output (`provisioning/local/terraform.tfstate`):

### AWS S3

```bash
curl 'http://localhost:8800/manage/file-storage-s3/' \
  -H 'X-KBC-ManageApiToken: YOUR_TOKEN_HERE' \
  -X POST \
  -d '{
    "awsKey": "YOUR_AWS_KEY",
    "awsSecret": "YOUR_AWS_SECRET",
    "filesBucket": "YOUR_BUCKET_NAME",
    "owner": "keboola",
    "region": "eu-central-1"
}'
```

### Set as default (optional)

```bash
curl 'http://localhost:8800/manage/file-storage-s3/1/default' \
  -H 'X-KBC-ManageApiToken: YOUR_TOKEN_HERE' \
  -X POST
```

## Step 11: Register Snowflake Backend

### Create Snowflake User and Role

Login to https://keboolaconnectiondev.us-east-1.snowflakecomputing.com and run:

```sql
USE ROLE ACCOUNTADMIN;

-- Create role (use CAPITALS for names)
CREATE ROLE "YOURNAME_KBC_STORAGE";
GRANT CREATE DATABASE ON ACCOUNT TO ROLE "YOURNAME_KBC_STORAGE";
GRANT CREATE ROLE ON ACCOUNT TO ROLE "YOURNAME_KBC_STORAGE" WITH GRANT OPTION;
GRANT CREATE USER ON ACCOUNT TO ROLE "YOURNAME_KBC_STORAGE" WITH GRANT OPTION;
GRANT USAGE ON WAREHOUSE "DEV" TO ROLE "YOURNAME_KBC_STORAGE" WITH GRANT OPTION;
GRANT USAGE ON WAREHOUSE "DEV_SMALL" TO ROLE "YOURNAME_KBC_STORAGE" WITH GRANT OPTION;
GRANT USAGE ON WAREHOUSE "DEV_TESTSIZE" TO ROLE "YOURNAME_KBC_STORAGE" WITH GRANT OPTION;

-- Create service user
CREATE USER "YOURNAME_KBC_STORAGE"
  PASSWORD = "YOUR_SECURE_PASSWORD"
  DEFAULT_ROLE = "YOURNAME_KBC_STORAGE"
  TYPE = LEGACY_SERVICE;

GRANT ROLE "YOURNAME_KBC_STORAGE" TO USER "YOURNAME_KBC_STORAGE";
GRANT ROLE "YOURNAME_KBC_STORAGE" TO ROLE SYSADMIN;
GRANT OWNERSHIP ON USER "YOURNAME_KBC_STORAGE" TO ROLE "YOURNAME_KBC_STORAGE";
```

### Register Backend

```bash
curl 'http://localhost:8800/manage/storage-backend' \
  -H 'X-KBC-ManageApiToken: YOUR_TOKEN_HERE' \
  -H 'Content-type: application/json' \
  -X POST \
  -d '{
    "backend": "snowflake",
    "host": "keboolaconnectiondev.us-east-1.snowflakecomputing.com",
    "username": "YOURNAME_KBC_STORAGE",
    "password": "YOUR_SECURE_PASSWORD",
    "warehouse": "DEV",
    "owner": "keboola",
    "region": "eu-central-1"
}'
```

### Set as Default for Maintainer

```bash
curl 'http://localhost:8800/manage/maintainers/1' \
  -H 'X-KBC-ManageApiToken: YOUR_TOKEN_HERE' \
  -H 'Content-type: application/json' \
  -X PATCH \
  -d '{"defaultConnectionSnowflakeId": "1"}'
```

### Fix Data Retention Time (Standard Edition)

Snowflake **Standard edition** podporuje max 1 den Time Travel retention. Vychozi project templates maji 7 dni, coz zpusobi chybu `Exceeds maximum allowable retention time (1 day(s))` pri vytvareni projektu.

```bash
# Uprav retention na 1 den pro vsechny templates
docker compose run --rm cli mysql -h mysql-accounts -u user -ppassword --skip-ssl accounts -e \
  "UPDATE bi_projectLimits_templates SET dataRetentionTimeInDays = 1 WHERE dataRetentionTimeInDays > 1;"
```

> **Poznamka:** Enterprise edition podporuje az 90 dni. Pokud mas Enterprise, tento krok preskoc.

## Useful Commands

### Development

```bash
# Enter CLI container
docker compose run --rm cli bash

# Run PHP commands
docker compose run --rm cli composer phpcs
docker compose run --rm cli composer phpstan
docker compose run --rm cli composer tests

# Clear cache after .env.local changes
docker compose run --rm cli bin/console cache:clear

# View all logs
docker compose logs -f
```

### Database

```bash
# MySQL console
docker compose run --rm cli mysql -h mysql-accounts -u user -ppassword accounts

# Backup database
docker compose run --rm cli mysqldump -h mysql-accounts -proot accounts > accounts.sql
```

### Cleanup

```bash
# Stop all services
docker compose down

# Full reset (removes all data)
docker compose down
rm -rf ./docker/.*-datadir
```

## Troubleshooting

### 404 na vsech URL (vcetne /admin/login)

Aplikace bezi ale vraci 404 nebo "Invalid controller specified". Mozne priciny:
1. Symfony cache je stara - vycisti ji:
   ```bash
   docker compose run --rm --entrypoint="bash -c" cli "rm -rf /var/www/html/var/cache/*"
   ```
2. OAuth klice chybi - viz Step 8b
3. UI apps nejsou nainstalovany - spust `docker compose run --rm cli php ./scripts/cli.php ui-apps:sync`

Zkus alternativni URL:
- https://localhost:8700/v2/storage (Storage API)
- https://localhost:8700/admin
- http://localhost:8800/v2/storage (HTTP bez SSL)

### Invalid key supplied (OAuth error)

Chybi RSA klice pro OAuth2 server. Viz Step 8b pro generovani.

### MySQL SSL error (self-signed certificate)

Prikazy mysql selhavaji s `TLS/SSL error: self-signed certificate in certificate chain`.

Reseni: Pridej `--skip-ssl` ke vsem mysql prikazum:
```bash
docker compose run --rm cli mysql -h mysql-accounts -u user -ppassword --skip-ssl accounts
```

### Snowflake retention time error

Pri vytvareni projektu chyba `Exceeds maximum allowable retention time (1 day(s))`.

Pricina: Snowflake Standard edition podporuje max 1 den Time Travel, ale project templates maji 7 dni.

Reseni: Viz sekce "Fix Data Retention Time" v Step 11.

### Docker time out of sync

If you see errors like `Signature expired`, run:

```bash
docker run -it --rm --privileged --pid=host debian nsenter -t 1 -m -u -n -i date -u $(date -u +%m%d%H%M%Y.%S)
```

### Changes in .env.local not applied

Recreate containers:

```bash
docker compose down
docker compose up apache supervisor
```

### Buildx error: fail resolve image

```bash
docker buildx rm --all-inactive
# Then rebuild
```

### MySQL initialization fails

Check logs for `Killed` messages. May need to set ulimits in docker-compose.override.yml.

### monorepo build fails

Prikaz `docker compose build` selze na `monorepo` service. Builduj services jednotlive:
```bash
docker compose --env-file=.env.local build apache supervisor mysql-accounts elasticsearch-v7 node
```

## API Documentation

- **Storage API docs:** https://localhost:8700/api/doc/storage
- **Manage API docs:** https://localhost:8700/api/doc/manage
- **Symfony Profiler:** https://localhost:8700/_profiler

## Additional Resources

- [DOCKER.md](../connection/docs/DOCKER.md) - Full Docker setup documentation
- [Terraform.md](../connection/docs/Terraform.md) - Cloud provisioning details
- [SAML.md](../connection/docs/SAML.md) - SAML configuration
- [PAYGO.md](../connection/docs/PAYGO.md) - Pay As You Go setup
- [ENVIRONMENT-VARIABLES.md](../connection/docs/ENVIRONMENT-VARIABLES.md) - All environment variables

---

## Co jsme se naucili (Session 2024-12-14)

### Architektura Keboola Connection

1. **Lokalni setup != Produkcni Keboola**
   - Lokalne bezi jen Connection (PHP aplikace)
   - V produkci je pred Connection jeste UI Gateway/Proxy a dalsi microservices
   - Nektere endpointy (`/upload-file`, `/search/jobs`) jsou v techto separatnich sluzbach

2. **File Upload architektura**
   ```
   Produkce:  Browser → UI Gateway → /files/prepare → Browser → S3
   Lokalne:   Browser → /files/prepare → Browser → S3 (funguje)
              Browser → /upload-file → 404 (UI Gateway chybi)
   ```

3. **Snowflake edice a Time Travel**
   - Standard edition: max 1 den retention
   - Enterprise edition: max 90 dni retention
   - Keboola templates maji defaultne 7 dni → nutno zmenit na 1 pro Standard

### Dulezite poznatky

| Tema | Poznatek |
|------|----------|
| Storage API token format | `{projectId}-{tokenId}-{secret}` (napr. `3-3-cvNUj...`) |
| Manage API token | Odlisny od Storage API tokenu, pro spravu backendu |
| awsSecret v DB | Je sifrovany, nelze primo pouzit pro server-side upload |
| Federation token | Pouze pro cteni, ne pro zapis na S3 |
| MySQL SSL | Self-signed cert → pouzivat `--skip-ssl` |

### Resene problemy

1. **"Exceeds maximum allowable retention time (1 day)"**
   - Pricina: Snowflake Standard edition
   - Reseni: `UPDATE bi_projectLimits_templates SET dataRetentionTimeInDays = 1`

2. **404 na /upload-file**
   - Pricina: Endpoint neni v open-source Connection
   - Reseni: Pouzit API (`/v2/storage/files/prepare` + S3 upload)

3. **Cannot GET /search/jobs**
   - Pricina: Queue API je separatni microservice
   - Reseni: Ignorovat, neni kriticke pro zakladni funkcnost

### Dalsi kroky

- [x] Lokalni Connection bezi
- [x] Snowflake backend funguje
- [x] S3 file storage funguje (pres API)
- [x] GCS file storage funguje
- [x] BigQuery backend funguje
- [ ] Studovat BigQuery driver jako referenci pro DuckDB
- [ ] Implementovat DuckDB driver

---

## Co jsme se naucili (Session 2024-12-15) - BigQuery Backend

### BigQuery Driver architektura

1. **Driver bezi jako knihovna v Connection (ne externi service)**
   - Kod: `vendor/keboola/storage-driver-bigquery/`
   - Komunikuje pres Protocol Buffers (ne REST)
   - Implementuje `ClientInterface` z `storage-driver-common`

2. **InitBackendCommand validace**
   BigQuery driver pri inicializaci kontroluje VSECHNY tyto veci:
   ```
   - Folder access (folders.get, folders.list)
   - Project creation permissions (projects.create)
   - IAM policy access (projects.getIamPolicy)
   - Required roles (roles/owner, roles/storage.objectAdmin)
   - Billing account access (billing.user)
   ```

3. **Proc BigQuery potrebuje tolik permissions?**
   - Pri vytvoreni Keboola projektu vytvari NOVY GCP projekt v GCP Folderu
   - Tento GCP projekt musi byt pripojen k billing accountu
   - Data jsou izolovana per-projekt (kazdy ma vlastni GCP projekt)

### API Endpointy - spravny format (kompletni prehled)

#### File Storage

| Operace | Endpoint | Metoda | Poznamka |
|---------|----------|--------|----------|
| List S3 | `/manage/file-storage-s3` | GET | S pomlckou! |
| Create S3 | `/manage/file-storage-s3` | POST | |
| Set S3 default | `/manage/file-storage-s3/{id}/default` | POST | |
| List GCS | `/manage/file-storage-gcs` | GET | S pomlckou! |
| Create GCS | `/manage/file-storage-gcs` | POST | |

#### Storage Backend

| Operace | Endpoint | Metoda | Poznamka |
|---------|----------|--------|----------|
| List all | `/manage/storage-backend` | GET | |
| Create Snowflake | `/manage/storage-backend` | POST | Legacy generic |
| Create BigQuery | `/manage/storage-backend/bigquery` | POST | Dedicated endpoint |

#### Maintainer

| Operace | Endpoint | Metoda | Poznamka |
|---------|----------|--------|----------|
| List | `/manage/maintainers` | GET | |
| Create | `/manage/maintainers` | POST | |
| Update | `/manage/maintainers/{id}` | PATCH | Pro nastaveni defaultu |
| Create org | `/manage/maintainers/{id}/organizations` | POST | |

#### Organization

| Operace | Endpoint | Metoda | Poznamka |
|---------|----------|--------|----------|
| Get | `/manage/organizations/{id}` | GET | |
| Update | `/manage/organizations/{id}` | PATCH | Pro zmenu maintainera |
| Create project | `/manage/organizations/{id}/projects` | POST | |

#### Project

| Operace | Endpoint | Metoda | Poznamka |
|---------|----------|--------|----------|
| Get | `/manage/projects/{id}` | GET | |
| **Assign backend** | `/manage/projects/{id}/storage-backend` | POST | **KRITICKE!** |

> **DULEZITE:** Endpoint `/manage/projects/{id}/storage-backend` je NUTNY pro prirazeni backendu k projektu. Bez nej projekt nepouzije BigQuery!

### Potrebne GCP IAM role pro BigQuery

**Na GCP Folder:**
- `roles/resourcemanager.folderAdmin`
- `roles/resourcemanager.projectCreator`

**Na GCP Project:**
- `roles/owner`
- `roles/storage.objectAdmin`
- `roles/bigquery.admin`

**Na Billing Account:**
- `roles/billing.user`

**Potrebna GCP API:**
- `cloudbilling.googleapis.com`
- `cloudresourcemanager.googleapis.com`
- `bigquery.googleapis.com`

### Implikace pro DuckDB driver

| BigQuery | DuckDB (plan) |
|----------|---------------|
| GCP projekt per Keboola projekt | DuckDB soubor per Keboola projekt |
| Billing account | Neni potreba |
| GCS pro files | Lokalni filesystem |
| Externi GCP sluzba | Lokalni Python microservice |
| Rozsahle IAM permissions | Zadne cloud permissions |

> **Klicovy poznatek:** DuckDB driver bude JEDNODUSSI nez BigQuery, protoze nebude potrebovat zadne cloud permissions. Vsechno bezi lokalne.

### Vytvareni BigQuery projektu - Postup

Pri vytvareni BigQuery projektu jsme zjistili dulezity poznatek:

| Krok | API Endpoint | Poznamka |
|------|--------------|----------|
| 1. Vytvorit maintainera | `POST /manage/maintainers` | S `defaultConnectionBigqueryId` |
| 2. Priradit organizaci | `PATCH /manage/organizations/{id}` | `maintainerId` = BigQuery maintainer |
| 3. Vytvorit projekt | `POST /manage/organizations/{id}/projects` | Vytvori se s file storage |
| 4. **Priradit backend** | `POST /manage/projects/{id}/storage-backend` | **KRITICKE!** |

> **POZOR:** Krok 4 je NUTNY! I kdyz je na maintainerovi nastaven `defaultConnectionBigqueryId`, projekt se vytvori s `defaultBackend: "snowflake"`. Teprve po explicitnim prirazeni backendu pres API se projekt prepne na BigQuery.

### Keboola hierarchie (upreseno)

```
Maintainer (vlastni infrastrukturu)
    │
    ├── defaultConnectionSnowflakeId: 1
    ├── defaultConnectionBigqueryId: 2
    └── defaultFileStorageId: 2
         │
         ▼
    Organization (skupina projektu)
         │
         ▼
    Project
         │
         ├── fileStorage: automaticky z maintainera
         └── backends: MUSI se priradit explicitne!
```

---

## Step 12: Register GCS File Storage (Optional)

Pokud chces mit vedle S3 i GCS file storage pro BigQuery projekty.

### Predpoklady

GCS bucket `kbc-padak-files-storage` uz existuje z Terraform provisioningu.

### Registrace GCS File Storage

```bash
# Extrahuj credentials z Terraform output
cd connection/provisioning/local
GCS_CREDS=$(jq -c '.gcsFileStorageKeyJson.value | fromjson' tfoutput.json)
GCS_BUCKET=$(jq -r '.gcsFileStorageBackendBucket.value' tfoutput.json)

# Registruj GCS file storage
curl -X POST "http://localhost:8800/manage/file-storage-gcs" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"owner\": \"keboola\",
    \"region\": \"us-central1\",
    \"filesBucket\": \"$GCS_BUCKET\",
    \"gcsCredentials\": $GCS_CREDS
  }"
```

---

## Step 13: Register BigQuery Backend (Optional)

Pro vytvareni BigQuery Keboola projektu vedle Snowflake projektu.

### Predpoklady

1. **GCP Folder** - vytvor v GCP Console nebo pres gcloud:
   ```bash
   gcloud resource-manager folders create \
     --display-name="keboola-bigquery-dev" \
     --organization=YOUR_ORG_ID
   ```

2. **IAM Permissions** - service account potrebuje tyto role:

   **Na Folder:**
   ```bash
   FOLDER_ID=393339196668  # tvoje folder ID
   SA=padak-dev@padak-storage-v3-dev.iam.gserviceaccount.com

   gcloud resource-manager folders add-iam-policy-binding $FOLDER_ID \
     --member="serviceAccount:$SA" --role="roles/resourcemanager.folderAdmin"

   gcloud resource-manager folders add-iam-policy-binding $FOLDER_ID \
     --member="serviceAccount:$SA" --role="roles/resourcemanager.projectCreator"
   ```

   **Na Project:**
   ```bash
   PROJECT=padak-storage-v3-dev

   gcloud projects add-iam-policy-binding $PROJECT \
     --member="serviceAccount:$SA" --role="roles/owner"

   gcloud projects add-iam-policy-binding $PROJECT \
     --member="serviceAccount:$SA" --role="roles/storage.objectAdmin"

   gcloud projects add-iam-policy-binding $PROJECT \
     --member="serviceAccount:$SA" --role="roles/bigquery.admin"
   ```

   **Na Billing Account:**
   ```bash
   # Zjisti billing account ID
   gcloud billing accounts list

   # Pridej billing.user roli
   gcloud billing accounts add-iam-policy-binding BILLING_ACCOUNT_ID \
     --member="serviceAccount:$SA" --role="roles/billing.user"
   ```

3. **Povol potrebna API:**
   ```bash
   gcloud services enable cloudbilling.googleapis.com --project=$PROJECT
   gcloud services enable cloudresourcemanager.googleapis.com --project=$PROJECT
   gcloud services enable bigquery.googleapis.com --project=$PROJECT
   ```

### Registrace BigQuery Backend

```bash
cd connection/provisioning/local
GCS_CREDS=$(jq -c '.gcsFileStorageKeyJson.value | fromjson' tfoutput.json)
FOLDER_ID=393339196668  # tvoje folder ID

curl -X POST "http://localhost:8800/manage/storage-backend/bigquery" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"owner\": \"keboola\",
    \"technicalOwner\": \"keboola\",
    \"region\": \"us-central1\",
    \"folderId\": \"$FOLDER_ID\",
    \"credentials\": $GCS_CREDS
  }"
```

### Overeni

```bash
# Seznam file storages
curl -s "http://localhost:8800/manage/file-storage-s3" -H "X-KBC-ManageApiToken: TOKEN"
curl -s "http://localhost:8800/manage/file-storage-gcs" -H "X-KBC-ManageApiToken: TOKEN"

# Seznam storage backends
curl -s "http://localhost:8800/manage/storage-backend" -H "X-KBC-ManageApiToken: TOKEN"
```

### Vytvoreni BigQuery projektu

> **DULEZITE:** Nestaci jen zaregistrovat BigQuery backend. Pro vytvoreni BigQuery projektu je potreba:
> 1. Vytvorit maintainera s BigQuery jako default
> 2. Vytvorit organizaci pod timto maintainerem
> 3. Vytvorit projekt
> 4. **Explicitne priradit BigQuery backend k projektu**

#### Krok 1: Vytvorit BigQuery maintainera

```bash
# Vytvor maintainera
curl -X POST "http://localhost:8800/manage/maintainers" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "BigQuery Services"}'

# Nastav BigQuery a GCS jako defaulty (pouzij ID z predchozich kroku)
# BigQuery backend ID: 2, GCS file storage ID: 2
curl -X PATCH "http://localhost:8800/manage/maintainers/2" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"defaultConnectionBigqueryId": 2, "defaultFileStorageId": 2}'
```

#### Krok 2: Vytvorit nebo priradit organizaci

```bash
# Vytvor novou organizaci
curl -X POST "http://localhost:8800/manage/maintainers/2/organizations" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "BigQuery Organization"}'

# Nebo priradit existujici organizaci k BigQuery maintainerovi
curl -X PATCH "http://localhost:8800/manage/organizations/ORGANIZATION_ID" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"maintainerId": 2}'
```

#### Krok 3: Vytvorit projekt v organizaci

```bash
curl -X POST "http://localhost:8800/manage/organizations/ORGANIZATION_ID/projects" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "BigQuery Test Project"}'
```

#### Krok 4: Priradit BigQuery backend k projektu (KRITICKE!)

> **Bez tohoto kroku projekt nepouzije BigQuery!** Projekt se vytvori s `defaultBackend: "snowflake"` i kdyz je na maintainerovi nastaven BigQuery default.

```bash
# PROJECT_ID je ID projektu z predchoziho kroku
curl -X POST "http://localhost:8800/manage/projects/PROJECT_ID/storage-backend" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"storageBackendId": 2}'
```

Po tomto kroku bude projekt mit:
- `defaultBackend`: "bigquery"
- `hasBigquery`: true
- `assignedBackends`: ["bigquery"]
- `fileStorageProvider`: "gcp"

#### Overeni projektu

```bash
curl -s "http://localhost:8800/manage/projects/PROJECT_ID" \
  -H "X-KBC-ManageApiToken: YOUR_MANAGE_TOKEN" | jq '{
    id,
    name,
    defaultBackend,
    hasBigquery,
    assignedBackends,
    fileStorageProvider
  }'
```

Ocekavany vystup:
```json
{
  "id": 4,
  "name": "BigQuery Test Project",
  "defaultBackend": "bigquery",
  "hasBigquery": true,
  "assignedBackends": ["bigquery"],
  "fileStorageProvider": "gcp"
}
```
