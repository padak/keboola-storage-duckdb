# ADR-003: Dev branches jako samostatne DuckDB soubory

## Status

Superseded by [ADR-007](007-duckdb-cow-branching.md)

## Datum

2024-12-11

## Kontext

Keboola Storage podporuje dev branches - izolovane vyvojove prostredi v ramci projektu. Potrebujeme rozhodnout, jak implementovat branches v DuckDB:

**Varianta A: Schema-based branching**
- Vsechny branche v jednom souboru
- Kazda branch = schema prefix (`dev_456_bucket_name`)

**Varianta B: Separate files per branch**
- Kazda branch = samostatny DuckDB soubor
- `project_123_main.duckdb`, `project_123_branch_456.duckdb`

## Rozhodnuti

**Zvolili jsme Variantu B: Kazda branch = samostatny DuckDB soubor.**

### Struktura souboru

```
/data/duckdb/
├── project_123_main.duckdb           # default branch
├── project_123_branch_456.duckdb     # dev branch "feature-x"
├── project_123_branch_789.duckdb     # dev branch "experiment"
├── project_124_main.duckdb
└── ...
```

## Duvody

### Proc ne schema-based (Varianta A)?

1. **Riziko poskozeni**: Chyba v dev branch muze potencialne poskodit cely soubor
2. **Single-writer omezeni**: Nelze paralelne pracovat na vice branchich
3. **Slozity namespace**: Schema jmena by byla dlouha a neprehledna
4. **Recovery**: Pri problemu nutno obnovit cely soubor

### Proc separate files (Varianta B)?

1. **Uplna izolace**: Zmeny v branch nemohou fyzicky poskodit main
2. **Paralelni prace**: Ruzne branche = ruzne soubory = paralelni zapis
3. **Jednoduchy merge**: `ATTACH branch + INSERT INTO main SELECT FROM branch`
4. **Snadne smazani**: `rm project_123_branch_456.duckdb`
5. **Jednoduchy rollback**: Smazat branch soubor, zacit znovu
6. **Nezavisly backup**: Kazda branch ma vlastni backup lifecycle

## Dusledky

### Pozitivni

- Maximalni izolace mezi branchemi
- Moznost paralelniho vyvoje na vice branchich
- Jednoducha sprava (soubory = jednoznacna identifikace)
- Snadne testovani branch operaci

### Negativni

- Vice souboru na disku
- Cross-branch dotazy vyzaduji explicitni ATTACH
- Kopirovani dat pri vytvoreni branche (pokud se kopiruje obsah)

## Implementace

### Vytvoreni branch

```python
def create_branch(project_id: str, branch_id: str, copy_data: bool = True):
    main_path = f'project_{project_id}_main.duckdb'
    branch_path = f'project_{project_id}_branch_{branch_id}.duckdb'

    if copy_data:
        # Kopirovat cely soubor (vcetne dat)
        shutil.copy(main_path, branch_path)
    else:
        # Kopirovat pouze schema (prazdne tabulky)
        copy_schema_only(main_path, branch_path)
```

### Merge branch

```python
def merge_branch(project_id: str, branch_id: str, strategy: str = 'replace'):
    main_conn = duckdb.connect(f'project_{project_id}_main.duckdb')
    branch_path = f'project_{project_id}_branch_{branch_id}.duckdb'

    main_conn.execute(f"ATTACH '{branch_path}' AS branch (READ_ONLY)")

    for schema, table in get_branch_tables(branch_id):
        if strategy == 'replace':
            main_conn.execute(f"""
                CREATE OR REPLACE TABLE {schema}.{table} AS
                SELECT * FROM branch.{schema}.{table}
            """)

    main_conn.execute("DETACH branch")
```

### Smazani branch

```python
def delete_branch(project_id: str, branch_id: str):
    branch_path = f'project_{project_id}_branch_{branch_id}.duckdb'
    os.remove(branch_path)
```

## Reference

- ADR-002: Organizace DuckDB souboru
- [DuckDB ATTACH dokumentace](https://duckdb.org/docs/stable/sql/statements/attach.html)
