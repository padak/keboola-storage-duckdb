# ADR-006: On-premise Storage bez S3 - lokalni filesystem

## Status

Accepted

## Datum

2024-12-11

## Kontext

Cilem je umoznit **plne on-premise nasazeni Keboola** bez zavislosti na:
- Cloud data warehouse (Snowflake, BigQuery)
- Cloud object storage (S3, GCS, Azure Blob)

Keboola Storage ma dve hlavni casti:
1. **Storage Tables** - strukturovana data (tabulky)
2. **Storage Files** - nestrukturovana data (CSV, JSON, obrazky, ...)

### Otazka

Muze DuckDB nahradit i S3 pro Storage Files?

### Zjisteni z vyzkumu

- DuckDB ma `BLOB` typ, ale **neni optimalizovan pro velke soubory**
- Limit 4GB per BLOB objekt
- DuckDB doporucuje: soubory na filesystem, metadata v DuckDB
- DuckDB umi cist primo z lokalniho FS: `SELECT * FROM '/path/*.parquet'`

## Rozhodnuti

**Pro on-premise nasazeni pouzijeme:**

1. **Storage Tables**: DuckDB (dle predchozich ADR)
2. **Storage Files**: Lokalni filesystem + metadata v DuckDB

### Architektura

```
┌─────────────────────────────────────────────────────────────┐
│                    On-Premise Keboola                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────┐    ┌─────────────────────────────┐ │
│  │   Storage Tables    │    │      Storage Files          │ │
│  │                     │    │                             │ │
│  │  ┌───────────────┐  │    │  ┌───────────────────────┐  │ │
│  │  │ DuckDB Files  │  │    │  │ Local Filesystem      │  │ │
│  │  │               │  │    │  │                       │  │ │
│  │  │ project_1.db  │  │    │  │ /data/files/          │  │ │
│  │  │ project_2.db  │  │    │  │ ├── project_1/        │  │ │
│  │  │ ...           │  │    │  │ │   ├── file_123.csv  │  │ │
│  │  └───────────────┘  │    │  │ │   └── file_456.json │  │ │
│  │                     │    │  │ └── project_2/        │  │ │
│  │                     │    │  │     └── ...           │  │ │
│  │                     │    │  └───────────────────────┘  │ │
│  │                     │    │                             │ │
│  │                     │    │  ┌───────────────────────┐  │ │
│  │                     │    │  │ File Metadata (DuckDB)│  │ │
│  │                     │    │  │ - file_id             │  │ │
│  │                     │    │  │ - path                │  │ │
│  │                     │    │  │ - size, checksum      │  │ │
│  │                     │    │  │ - created_at          │  │ │
│  │                     │    │  └───────────────────────┘  │ │
│  └─────────────────────┘    └─────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Struktura filesystem

```
/data/
├── duckdb/                          # Storage Tables
│   ├── project_123_main.duckdb
│   ├── project_123_branch_456.duckdb
│   └── project_124_main.duckdb
│
├── files/                           # Storage Files
│   ├── project_123/
│   │   ├── 2024/12/11/              # Date-based organization
│   │   │   ├── file_001.csv
│   │   │   ├── file_002.json
│   │   │   └── file_003.parquet
│   │   └── staging/                 # Import staging area
│   │       └── upload_xyz.csv
│   └── project_124/
│       └── ...
│
├── snapshots/                       # Table snapshots
│   └── project_123/
│       └── snap_001/
│           └── table.parquet
│
└── metadata/                        # System metadata
    └── files.duckdb                 # File registry
```

## Duvody

### Proc ne BLOB v DuckDB?

1. **Limit velikosti**: 4GB per objekt
2. **Neni optimalizovano**: DuckDB je OLAP, ne object store
3. **Komplikovane dotazy**: Binary data nelze snadno filtrovat
4. **Backup slozitost**: Velke DB soubory = dlouhe backupy

### Proc lokalni filesystem?

1. **Zadne limity**: Filesystem zvladne libovolne velke soubory
2. **Nativni podpora**: DuckDB cte primo z FS (`read_csv`, `read_parquet`)
3. **Jednoduchost**: Standardni nastroje pro spravu (cp, mv, rsync)
4. **Flexibilita**: Lze pripojit NFS, CIFS, nebo local SSD

### Proc metadata v DuckDB?

1. **Konzistence**: Jednotny dotazovaci jazyk (SQL)
2. **Vyhledavani**: Efektivni indexy na metadata
3. **Integrace**: Moznost JOIN files metadata s tables

## Dusledky

### Pozitivni

- **Plne on-premise**: Zadna cloud zavislost
- **Jednoduchost**: Standardni filesystem operace
- **Flexibilita**: Lze pouzit jakoukoliv storage (local, NAS, SAN)
- **Nizke naklady**: Zadne S3 fees

### Negativni

- **Zadna CDN**: Pro distribuovane nasazeni nutno resit zvlast
- **Backup**: Nutno nastavit vlastni backup strategii
- **HA**: Nutno resit replikaci (DRBD, GlusterFS, ...)

## Implementace

### File Metadata Schema

```sql
-- V /data/metadata/files.duckdb
CREATE TABLE files (
    id BIGINT PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    path VARCHAR NOT NULL,           -- Relativni cesta
    size_bytes BIGINT,
    checksum_md5 VARCHAR(32),
    content_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT now(),
    created_by VARCHAR,
    tags JSON,                       -- User-defined tags
    is_public BOOLEAN DEFAULT false,
    is_encrypted BOOLEAN DEFAULT false,

    -- Metadata pro sliced files
    is_sliced BOOLEAN DEFAULT false,
    slices_count INTEGER,
    manifest JSON
);

CREATE INDEX idx_files_project ON files(project_id);
CREATE INDEX idx_files_created ON files(created_at);
```

### File Service

```python
import hashlib
from pathlib import Path
from datetime import datetime
import duckdb

class OnPremFileService:
    def __init__(self, base_path: str = '/data/files',
                 metadata_db: str = '/data/metadata/files.duckdb'):
        self.base_path = Path(base_path)
        self.metadata_db = metadata_db

    def upload_file(self, project_id: str, filename: str,
                   content: bytes, tags: dict = None) -> dict:
        """Upload file to local storage."""
        # Generate path
        date_path = datetime.now().strftime('%Y/%m/%d')
        file_id = self._generate_file_id()
        relative_path = f"{project_id}/{date_path}/{file_id}_{filename}"
        full_path = self.base_path / relative_path

        # Ensure directory exists
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Write file
        full_path.write_bytes(content)

        # Calculate checksum
        checksum = hashlib.md5(content).hexdigest()

        # Save metadata
        conn = duckdb.connect(self.metadata_db)
        conn.execute("""
            INSERT INTO files (id, project_id, name, path, size_bytes,
                             checksum_md5, tags, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [file_id, project_id, filename, relative_path,
              len(content), checksum, tags, datetime.now()])
        conn.close()

        return {
            'id': file_id,
            'path': relative_path,
            'size': len(content),
            'checksum': checksum
        }

    def get_file(self, project_id: str, file_id: int) -> tuple[bytes, dict]:
        """Download file from local storage."""
        conn = duckdb.connect(self.metadata_db, read_only=True)
        result = conn.execute("""
            SELECT path, name, size_bytes, checksum_md5
            FROM files WHERE id = ? AND project_id = ?
        """, [file_id, project_id]).fetchone()
        conn.close()

        if not result:
            raise FileNotFoundError(f"File {file_id} not found")

        path, name, size, checksum = result
        full_path = self.base_path / path
        content = full_path.read_bytes()

        return content, {'name': name, 'size': size, 'checksum': checksum}

    def get_presigned_path(self, project_id: str, filename: str) -> str:
        """Get path for direct upload (replaces S3 presigned URL)."""
        date_path = datetime.now().strftime('%Y/%m/%d')
        staging_path = f"{project_id}/staging/{date_path}/{filename}"
        full_path = self.base_path / staging_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        return str(full_path)

    def _generate_file_id(self) -> int:
        # Use timestamp-based ID or sequence
        return int(datetime.now().timestamp() * 1000000)
```

### Import Integration

```python
def import_table_from_file(project_id: str, file_id: int,
                          bucket: str, table: str):
    """Import data from Storage File to Storage Table."""
    file_service = OnPremFileService()

    # Get file metadata
    conn_meta = duckdb.connect('/data/metadata/files.duckdb', read_only=True)
    file_info = conn_meta.execute(
        "SELECT path, name FROM files WHERE id = ?", [file_id]
    ).fetchone()
    conn_meta.close()

    file_path = f"/data/files/{file_info[0]}"

    # Import to DuckDB table
    conn_data = duckdb.connect(f'/data/duckdb/project_{project_id}_main.duckdb')

    if file_path.endswith('.parquet'):
        conn_data.execute(f"""
            CREATE OR REPLACE TABLE {bucket}.{table} AS
            SELECT * FROM read_parquet('{file_path}')
        """)
    elif file_path.endswith('.csv'):
        conn_data.execute(f"""
            CREATE OR REPLACE TABLE {bucket}.{table} AS
            SELECT * FROM read_csv('{file_path}', header=true)
        """)

    conn_data.close()
```

## Migrace z S3

Pro existujici instalace s S3:

```python
# Sync S3 to local
aws s3 sync s3://keboola-files /data/files/

# Update metadata
conn = duckdb.connect('/data/metadata/files.duckdb')
conn.execute("""
    UPDATE files
    SET path = replace(path, 's3://keboola-files/', '')
    WHERE path LIKE 's3://%'
""")
```

## Reference

- [DuckDB BLOB](https://duckdb.org/docs/sql/data_types/blob.html)
- [DuckDB Read CSV](https://duckdb.org/docs/data/csv/overview)
- [DuckDB Read Parquet](https://duckdb.org/docs/data/parquet/overview)
- ADR-001: DuckDB jako microservice
- ADR-002: Organizace DuckDB souboru
