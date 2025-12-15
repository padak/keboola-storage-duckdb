# ADR-002: Organizace DuckDB souboru - jeden projekt = jeden soubor

## Status

**Superseded** by ADR-009 (2024-12-16)

> **Pozor:** Toto rozhodnuti bylo nahrazeno ADR-009: 1 DuckDB soubor per tabulka.
> Duvod: Lepsi paralelismus pri ETL importech, jednodussi dev branches (CoW),
> validovano Codex GPT-5 analyzou a ATTACH testy (4096 soucasnych ATTACH OK).

## Datum

2024-12-11 (accepted), 2024-12-16 (superseded)

## Kontext

Pri implementaci DuckDB backendu pro Keboola Storage API potrebujeme rozhodnout, jak organizovat DuckDB soubory. DuckDB je embedded databaze, kde kazda databaze je jeden soubor na disku.

Existuji tri hlavni moznosti:
1. Jeden soubor pro cely backend (vsechny projekty)
2. Jeden soubor per projekt
3. Jeden soubor per bucket

### DuckDB omezeni

Z technickeho researche vyplyva:

- **Concurrency**: DuckDB je single-writer, multi-reader
- **ATTACH**: Lze pripojit vice DuckDB souboru a delat cross-database JOINy
- **Velikost**: Prakticky neomezena (testovano az 10 TB)
- **Write omezeni**: V jedne transakci lze zapisovat pouze do jedne databaze

## Rozhodnuti

**Zvolili jsme Variantu B: Jeden DuckDB soubor = Jeden Keboola projekt.**

### Mapovani konceptu

| Keboola | DuckDB |
|---------|--------|
| Backend | Adresar s .duckdb soubory |
| Project | `project_{id}.duckdb` soubor |
| Bucket | Schema v databazi |
| Table | Tabulka ve schema |
| Workspace | Schema s prefixem `WORKSPACE_` |

### Struktura

```
/data/duckdb/
├── project_123.duckdb
│   ├── in_c_customers/           # bucket jako schema
│   │   ├── customers             # tabulka
│   │   └── orders
│   ├── out_c_reports/
│   │   └── monthly_sales
│   └── WORKSPACE_789/            # workspace jako schema
│       └── temp_analysis
├── project_124.duckdb
└── project_125.duckdb
```

## Duvody

### Proc ne jeden soubor pro cely backend (Varianta A)?

- Single point of failure - pad jednoho souboru = pad vsech projektu
- Vsichni tenanti sdili jeden soubor - bezpecnostni riziko
- Velky soubor = delsi recovery a backup
- Single-writer omezeni by limitovalo paralelni operace

### Proc ne jeden soubor per bucket (Varianta C)?

- Prilis mnoho souboru ke sprave
- Kazdy JOIN mezi buckety vyzaduje ATTACH
- Slozitejsi connection management
- Over-engineering pro nas use case

### Proc jeden soubor per projekt (Varianta B)?

1. **Izolace**: Projekty jsou hlavni tenant boundary v Keboola
2. **Cross-bucket JOINy**: Buckety v projektu jsou v jednom souboru = nativni SQL JOIN bez ATTACH
3. **Cross-project JOINy**: Stale mozne pres ATTACH (read-only)
4. **Rozumna velikost**: Jeden projekt = rozumna velikost souboru
5. **Recovery**: Pad jednoho projektu neovlivni ostatni
6. **Paralelismus**: Ruzne projekty mohou byt zapisovany paralelne (ruzne soubory)

## Dusledky

### Pozitivni

- Jasna izolace mezi projekty
- Nativni JOINy mezi buckety v projektu
- Moznost paralelniho zapisu do ruznych projektu
- Jednoduchy backup/restore per projekt
- Snadne smazani projektu (smazat jeden soubor)

### Negativni

- Cross-project JOINy vyzaduji explicitni ATTACH
- Potreba connection managementu per projekt
- Vice souboru ke sprave nez u jednoho globalniho souboru

### Neutralni

- Workspaces jsou implementovany jako schemas s prefixem `WORKSPACE_`
- Kazdy projekt ma vlastni namespace, kolize jmen nehrozí

## Cross-project JOIN priklad

Pokud uzivatel potrebuje JOIN mezi projekty:

```python
# DuckDB API Service
conn = duckdb.connect('project_123.duckdb')
conn.execute("ATTACH 'project_124.duckdb' AS other (READ_ONLY)")

result = conn.execute("""
    SELECT a.*, b.category
    FROM main.in_c_customers.orders AS a
    JOIN other.in_c_products.categories AS b
        ON a.category_id = b.id
""").fetchall()

conn.execute("DETACH other")
```

## Reference

- [DuckDB ATTACH dokumentace](https://duckdb.org/docs/stable/sql/statements/attach.html)
- [DuckDB Concurrency](https://duckdb.org/docs/stable/connect/concurrency.html)
- [DuckDB Working with Huge Databases](https://duckdb.org/docs/stable/guides/performance/working_with_huge_databases.html)
- ADR-001: DuckDB jako Python microservice
- docs/duckdb-technical-research.md
