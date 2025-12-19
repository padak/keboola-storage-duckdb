# ADR-013: SQL Object Naming Convention (Workspace & SQL Transformations)

## Status

**DRAFT** - Needs decision

## Datum

2024-12-19

## Kontext

Uzivatele potrebuji psat SQL dotazy, ktere funguji stejne v:

1. **Workspace** - interaktivni prace v DBeaveru, DataGripu, psql
2. **SQL Transformation** - automatizovane joby v Keboola

### Problem

DuckDB pouziva koncept **ATTACH** pro pripojeni vice databazi. Kazda tabulka je samostatny `.duckdb` soubor:

```
/data/duckdb/project_123/
├── in_c_sales/
│   ├── orders.duckdb       # tabulka "data" uvnitr
│   └── customers.duckdb    # tabulka "data" uvnitr
└── out_c_reports/
    └── summary.duckdb      # tabulka "data" uvnitr
```

Pri ATTACH vznikne alias:
```sql
ATTACH 'in_c_sales/orders.duckdb' AS some_alias (READ_ONLY);
SELECT * FROM some_alias.data;  -- "data" je vzdy stejne
```

### Pozadavky

1. **Konzistence** - SQL napsane ve workspace musi fungovat v SQL transformation
2. **Citelnost** - nazvy musi byt srozumitelne
3. **Keboola kompatibilita** - idealne podobne soucasne konvenci (`in.c-bucket.table`)
4. **SQL-friendly** - bez nutnosti uvozovek pokud mozno

### Soucasna Keboola konvence

V Snowflake/BigQuery backendu:
```sql
SELECT * FROM "in.c-sales"."orders";
SELECT * FROM "out.c-reports"."summary";
```

Poznamka: Tecky a pomlcky vyzaduji uvozovky.

## Varianty

### Varianta A: Keboola plna kompatibilita

```sql
SELECT * FROM "in.c-sales"."orders";
SELECT * FROM "out.c-reports"."summary";

-- JOIN priklad
SELECT o.*, c.name
FROM "in.c-sales"."orders" o
JOIN "in.c-sales"."customers" c ON o.customer_id = c.id;
```

**Implementace:**
```sql
ATTACH 'orders.duckdb' AS _att_orders (READ_ONLY);
CREATE SCHEMA IF NOT EXISTS "in.c-sales";
CREATE VIEW "in.c-sales"."orders" AS SELECT * FROM _att_orders.data;
```

| Pro | Proti |
|-----|-------|
| 100% kompatibilni s Keboola prod | Vyzaduje uvozovky vsude |
| Snadna migrace SQL skriptu | Neprijemne pro interaktivni psani |
| | Tecky v nazvech jsou nestandardni |

### Varianta B: Underscore konvence (Doporuceno)

```sql
SELECT * FROM in_c_sales.orders;
SELECT * FROM out_c_reports.summary;

-- JOIN priklad
SELECT o.*, c.name
FROM in_c_sales.orders o
JOIN in_c_sales.customers c ON o.customer_id = c.id;
```

**Implementace:**
```sql
ATTACH 'orders.duckdb' AS _att_in_c_sales_orders (READ_ONLY);
CREATE SCHEMA IF NOT EXISTS in_c_sales;
CREATE VIEW in_c_sales.orders AS SELECT * FROM _att_in_c_sales_orders.data;
```

| Pro | Proti |
|-----|-------|
| Bez uvozovek | Mala zmena oproti prod Keboola |
| SQL-friendly | Potreba mapovani pri migraci |
| Citelne | |
| Standardni identifikatory | |

### Varianta C: Jednoduche nazvy

```sql
SELECT * FROM sales.orders;
SELECT * FROM sales.customers;
SELECT * FROM reports.summary;

-- JOIN priklad
SELECT o.*, c.name
FROM sales.orders o
JOIN sales.customers c ON o.customer_id = c.id;
```

**Implementace:**
```sql
ATTACH 'orders.duckdb' AS _att_sales_orders (READ_ONLY);
CREATE SCHEMA IF NOT EXISTS sales;
CREATE VIEW sales.orders AS SELECT * FROM _att_sales_orders.data;
```

| Pro | Proti |
|-----|-------|
| Nejcistsi syntax | Ztrata informace o in/out |
| Bez prefixu | Potencialni kolize nazvu |
| Kratke nazvy | Nutna dokumentace mapovani |

### Varianta D: Hybrid s aliasy

```sql
-- Plne nazvy (oficialni)
SELECT * FROM in_c_sales.orders;

-- Kratke aliasy (convenience)
SELECT * FROM sales.orders;  -- alias pro in_c_sales
```

**Implementace:**
```sql
-- Plny nazev
CREATE SCHEMA IF NOT EXISTS in_c_sales;
CREATE VIEW in_c_sales.orders AS SELECT * FROM _att_orders.data;

-- Alias schema
CREATE SCHEMA IF NOT EXISTS sales;
CREATE VIEW sales.orders AS SELECT * FROM in_c_sales.orders;
```

| Pro | Proti |
|-----|-------|
| Flexibilita | Komplexnejsi implementace |
| Zpetna kompatibilita | Dva zpusoby = zmatenost |
| Kratke nazvy pro interaktivni praci | Vice views = vice udrzby |

## Porovnani

| Kritérium | A (Keboola) | B (Underscore) | C (Simple) | D (Hybrid) |
|-----------|-------------|----------------|------------|------------|
| Kompatibilita s prod | 100% | 95% | 70% | 95% |
| Citelnost | Stredni | Vysoka | Nejvyssi | Vysoka |
| SQL-friendly | Nizka | Vysoka | Nejvyssi | Vysoka |
| Migrace z prod | Zadna | Jednoducha | Slozita | Jednoducha |
| Komplexita impl. | Stredni | Nizka | Nizka | Vysoka |

## Doporuceni

**Varianta B (Underscore konvence)** jako nejlepsi kompromis:

1. `in.c-sales` → `in_c_sales`
2. `out.c-reports` → `out_c_reports`

### Mapovaci pravidla

```
Original Keboola    →  DuckDB Schema
─────────────────────────────────────
in.c-{bucket}       →  in_c_{bucket}
out.c-{bucket}      →  out_c_{bucket}
sys.c-{bucket}      →  sys_c_{bucket}

Tabulka             →  View ve schema
─────────────────────────────────────
{table}             →  {table}
```

### Priklady

| Keboola prod | DuckDB on-prem |
|--------------|----------------|
| `"in.c-sales"."orders"` | `in_c_sales.orders` |
| `"in.c-crm"."customers"` | `in_c_crm.customers` |
| `"out.c-analytics"."report"` | `out_c_analytics.report` |

### SQL migrace

Pro automatickou migraci SQL z prod do on-prem:

```python
def migrate_sql(sql: str) -> str:
    # "in.c-sales" -> in_c_sales
    sql = re.sub(r'"in\.c-([^"]+)"', r'in_c_\1', sql)
    sql = re.sub(r'"out\.c-([^"]+)"', r'out_c_\1', sql)
    # Remove remaining quotes around table names
    sql = re.sub(r'"(\w+)"', r'\1', sql)
    return sql
```

## Implementace

### 1. Zmena v `attach_project_tables()`

```python
def attach_project_tables(self) -> int:
    """ATTACH all project tables and create schema/views."""
    attached = 0
    schemas_created = set()

    for bucket in buckets:
        bucket_name = bucket["name"]  # e.g., "in_c_sales"

        # Create schema if not exists
        if bucket_name not in schemas_created:
            self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {bucket_name}")
            schemas_created.add(bucket_name)

        for table in tables:
            table_name = table["name"]  # e.g., "orders"

            # Internal attach alias (hidden)
            attach_alias = f"_att_{bucket_name}_{table_name}"

            # ATTACH the file
            self._conn.execute(
                f"ATTACH '{table_path}' AS {attach_alias} (READ_ONLY)"
            )

            # Create view with clean name
            self._conn.execute(
                f"CREATE VIEW {bucket_name}.{table_name} AS "
                f"SELECT * FROM {attach_alias}.data"
            )

            attached += 1

    return attached
```

### 2. Workspace listing

```sql
-- Uzivatel vidi schemas a views
SHOW SCHEMAS;
-- in_c_sales
-- out_c_reports
-- main (workspace)

SELECT * FROM information_schema.tables WHERE table_schema = 'in_c_sales';
-- orders
-- customers
```

## Rozhodnuti

[ ] Varianta A - Keboola plna kompatibilita
[ ] Varianta B - Underscore konvence (Doporuceno)
[ ] Varianta C - Jednoduche nazvy
[ ] Varianta D - Hybrid

## Reference

- [ADR-009: File per Table](009-duckdb-file-per-table.md)
- [ADR-010: SQL Interface](010-duckdb-sql-interface.md)
- [DuckDB ATTACH](https://duckdb.org/docs/sql/statements/attach.html)
- [DuckDB Schemas](https://duckdb.org/docs/sql/statements/create_schema.html)
