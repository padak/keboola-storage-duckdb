test-admin-key

# Project

```
{
  "id": "padak",
  "name": "Padak test",
  "db_path": "project_padak",
  "created_at": "2025-12-19T00:34:50.512759+01:00",
  "updated_at": "2025-12-19T00:34:50.567237+01:00",
  "size_bytes": 0,
  "table_count": 0,
  "bucket_count": 0,
  "status": "active",
  "settings": {
    "additionalProp1": {}
  },
  "api_key": "proj_padak_admin_76e633b7c341a71cce2f57d6fb319fc0"
}
```


# Bucket

```
{
  "buckets": [
    {
      "name": "bucket01",
      "table_count": 0,
      "description": null
    }
  ],
  "total": 1
}
```

# Table

```
{
  "name": "products",
  "bucket": "bucket01",
  "columns": [
    {
      "name": "product_id",
      "type": "INTEGER",
      "nullable": false,
      "ordinal_position": 1
    },
    {
      "name": "sku",
      "type": "VARCHAR",
      "nullable": true,
      "ordinal_position": 2
    },
    {
      "name": "product_name",
      "type": "VARCHAR",
      "nullable": true,
      "ordinal_position": 3
    },
    {
      "name": "brand",
      "type": "VARCHAR",
      "nullable": true,
      "ordinal_position": 4
    },
    {
      "name": "category",
      "type": "VARCHAR",
      "nullable": true,
      "ordinal_position": 5
    },
    {
      "name": "price",
      "type": "DECIMAL(10,2)",
      "nullable": true,
      "ordinal_position": 6
    },
    {
      "name": "cost",
      "type": "DECIMAL(10,2)",
      "nullable": true,
      "ordinal_position": 7
    },
    {
      "name": "stock_quantity",
      "type": "INTEGER",
      "nullable": true,
      "ordinal_position": 8
    },
    {
      "name": "rating",
      "type": "DECIMAL(2,1)",
      "nullable": true,
      "ordinal_position": 9
    },
    {
      "name": "review_count",
      "type": "INTEGER",
      "nullable": true,
      "ordinal_position": 10
    },
    {
      "name": "status",
      "type": "VARCHAR",
      "nullable": true,
      "ordinal_position": 11
    },
    {
      "name": "warehouse",
      "type": "VARCHAR",
      "nullable": true,
      "ordinal_position": 12
    },
    {
      "name": "weight_kg",
      "type": "DECIMAL(5,2)",
      "nullable": true,
      "ordinal_position": 13
    },
    {
      "name": "is_featured",
      "type": "BOOLEAN",
      "nullable": true,
      "ordinal_position": 14
    },
    {
      "name": "created_at",
      "type": "TIMESTAMP",
      "nullable": true,
      "ordinal_position": 15
    },
    {
      "name": "updated_at",
      "type": "TIMESTAMP",
      "nullable": true,
      "ordinal_position": 16
    }
  ],
  "row_count": 0,
  "size_bytes": 274432,
  "primary_key": [
    "product_id"
  ],
  "created_at": null
}
```


# Upload Key

```
{
  "upload_key": "05a49253-20bd-4f55-94ae-dca1d070cf33",
  "upload_url": "/projects/padak/files/upload/05a49253-20bd-4f55-94ae-dca1d070cf33",
  "expires_at": "2025-12-19T23:41:29.691196+00:00"
}
```


# Uploaded File

```
{
  "upload_key": "05a49253-20bd-4f55-94ae-dca1d070cf33",
  "staging_path": "staging/05a49253-20bd-4f55-94ae-dca1d070cf33_products_10k.csv",
  "size_bytes": 1602119,
  "checksum_sha256": "4d7eb94622aac0d49ff61ca36cac78ef19611dc55869dcf2817fdc37810885be"
}
```

# Register File

```
{
  "id": "c064ccae-2329-4914-bafc-ead5429d379d",
  "project_id": "padak",
  "name": "products_10k.csv",
  "path": "project_padak/2025/12/18/c064ccae-2329-4914-bafc-ead5429d379d_products_10k.csv",
  "size_bytes": 1602119,
  "content_type": "text/csv",
  "checksum_sha256": "4d7eb94622aac0d49ff61ca36cac78ef19611dc55869dcf2817fdc37810885be",
  "is_staged": false,
  "created_at": "2025-12-19T00:44:14.327253+01:00",
  "expires_at": null,
  "tags": null
}
```

# Uploaded file to table

```
{
  "imported_rows": 10000,
  "table_rows_after": 10000,
  "table_size_bytes": 1323008,
  "warnings": []
}
```

# Table Snapshot

```
{
  "id": "snap_products_20251218_234828_795",
  "project_id": "padak",
  "bucket_name": "bucket01",
  "table_name": "products",
  "snapshot_type": "manual",
  "row_count": 10000,
  "size_bytes": 293242,
  "created_at": "2025-12-19T00:48:28.847436+01:00",
  "created_by": null,
  "expires_at": "2026-03-19T00:48:28.847074+01:00",
  "description": "testovaci snapshot"
}
```

# Branch
 
```
{
  "id": "92c617b9",
  "project_id": "padak",
  "name": "padak dev",
  "created_at": "2025-12-19T00:50:44.644130+01:00",
  "created_by": null,
  "description": "dev branch",
  "table_count": 0,
  "size_bytes": 0
}
```

# Workspace

```
{
  "id": "ws_c22bac07",
  "name": "my workspace",
  "project_id": "padak",
  "branch_id": null,
  "created_at": "2025-12-19T00:55:34.571448+01:00",
  "expires_at": "2025-12-19T02:55:34.550405+01:00",
  "size_bytes": 798720,
  "size_limit_gb": 10,
  "status": "active",
  "connection": {
    "host": "localhost",
    "port": 5432,
    "database": "workspace_ws_c22bac07",
    "username": "ws_ws_c22bac07_47c6b299",
    "password": "wX6DQQwBNxjInkDVJWv4ueABP3HqfI-D",
    "ssl_mode": "prefer",
    "connection_string": null
  }
}
```
