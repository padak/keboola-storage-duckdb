# ADR-012: Branch-First API Design

## Status

APPROVED (2024-12-19)

## Kontext

Stavajici implementace (ADR-007) ma omezenou podporu pro dev branches:

1. **Endpointy pro buckety/tabulky nemaji branch parametr**
2. **Nelze vytvorit tabulku pouze v branchi** (bez existence v main)
3. **Main je implicitni**, ne explicitni branch
4. **Nekonzistentni URL struktura** - branches maji jine URL nez main

### Problem

```python
# Aktualni stav - nelze urcit branch
POST /projects/123/buckets/sales/tables
# Kam se vytvori tabulka? Vzdy do main. Branch neni mozna.

# Pro branch existuji jen specialni endpointy:
POST /projects/123/branches/456/tables/sales/orders/pull
# Ale ne plne CRUD na branch tabulkach
```

### Pozadavky

1. Vsechny operace musi podporovat volbu branch
2. Main je jen specialni branch (konzistentni API)
3. Tabulky mohou existovat pouze v branchi (ne v main)
4. Ciste REST API bez query parametru pro kontext

## Rozhodnuti

**Branch jako soucas URL path pro vsechny resource operace.**

### Nova URL struktura

```
/projects/{project_id}/branches/{branch_id}/buckets
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/preview
/projects/{project_id}/branches/{branch_id}/buckets/{bucket_name}/tables/{table_name}/import/file
...
```

### Specialni branch ID: `default`

- `branch_id = "default"` znamena produkcni projekt (main)
- Vsechny existujici dev branches maji UUID/short-id
- URL pro main: `/projects/123/branches/default/buckets`

### Porovnani

| Operace | Stare API | Nove API |
|---------|-----------|----------|
| List buckets (main) | `GET /projects/123/buckets` | `GET /projects/123/branches/default/buckets` |
| List buckets (branch) | N/A | `GET /projects/123/branches/abc/buckets` |
| Create table (main) | `POST /projects/123/buckets/b/tables` | `POST /projects/123/branches/default/buckets/b/tables` |
| Create table (branch) | N/A | `POST /projects/123/branches/abc/buckets/b/tables` |
| Delete table (branch) | N/A | `DELETE /projects/123/branches/abc/buckets/b/tables/t` |

### Chovani podle branch typu

#### Default branch (main)
- Standardni CRUD operace
- Zadny CoW (prime operace na main)
- Tabulky existuji fyzicky v `/data/duckdb/project_{id}/`

#### Dev branches
- **READ**: Live View - cti z main pokud tabulka neni v branch
- **WRITE**: Copy-on-Write - zkopiruj z main pred prvnim zapisem
- **CREATE**: Tabulka existuje pouze v branch (ne v main)
- **DELETE**: Smaze tabulku z branch (pokud byla zkopirana/vytvorena)
- Fyzicka lokace: `/data/duckdb/project_{id}_branch_{branch_id}/`

### Nove moznosti

```python
# 1. Vytvorit tabulku pouze v branchi
POST /projects/123/branches/feature-x/buckets/test/tables
{"name": "experiment", "columns": [...]}
# Tabulka existuje JEN v branch, ne v main

# 2. Listovat buckety/tabulky v branchi
GET /projects/123/branches/feature-x/buckets
# Vraci: buckety z main + buckety vytvorene v branch

# 3. Smazat tabulku z branch (ne z main)
DELETE /projects/123/branches/feature-x/buckets/test/tables/experiment
# Smaze jen z branch, main neovlivnen
```

### Response rozsireni

Pro tabulky v branch pridame pole indikujici stav:

```json
{
  "name": "orders",
  "bucket_name": "sales",
  "source": "main",      // "main" | "branch" | "branch_only"
  "row_count": 1500,
  ...
}
```

- `main`: Tabulka existuje v main, branch ji nema (Live View)
- `branch`: Tabulka byla zkopirana do branch (CoW)
- `branch_only`: Tabulka existuje pouze v branch

### Branch management endpointy (beze zmeny)

```
POST   /projects/{id}/branches                              # Create branch
GET    /projects/{id}/branches                              # List branches
GET    /projects/{id}/branches/{branch_id}                  # Branch detail
DELETE /projects/{id}/branches/{branch_id}                  # Delete branch
POST   /projects/{id}/branches/{branch_id}/pull/{bucket}/{table}  # Pull from main
```

## Duvody

### Proc branch v path a ne query param?

1. **Semantika**: Branch je kontext/kontejner, ne filtr
2. **Bezpecnost**: Query param se snadno zapomene → operace na main
3. **Caching**: Ruzne URL = ruzne resources = prirozeny caching
4. **Self-documenting**: URL jasne ukazuje branch kontext
5. **REST konvence**: Hierarchicke resources patri do path

### Proc `default` a ne `main`?

- `main` koliduje s potencialnim nazvem user-created branch
- `default` je rezervovane klicove slovo
- Konzistentni s Keboola produkci (default branch concept)

### Proc ne zpetna kompatibilita?

Novy system je API v1, muze byt prelozen na v2. Pro MVP neni zpetna kompatibilita nutna.

## Implementace

### Faze 1: Rozsireni routeru

1. Pridat `branch_id` do vsech bucket/table routeru
2. Implementovat `get_table_source()` - urceni zda main/branch/branch_only
3. Upravit `create_table()` - podpora vytvoreni pouze v branch

### Faze 2: Storage vrstva

1. Rozsirit `ProjectDBManager` o branch-aware metody
2. Implementovat Live View pro READ operace
3. Implementovat CoW trigger pro WRITE operace

### Faze 3: Migrace

1. Existujici data zustava v `/data/duckdb/project_{id}/`
2. `default` branch mapuje na existujici strukturu
3. Zadna migrace dat - jen URL zmena

## Dusledky

### Pozitivni

- **Unifikovane API**: Jeden pattern pro main i branches
- **Plna funkcionalita**: Vsechny operace dostupne v branch
- **Jasna semantika**: URL explicitne urcuje kontext
- **Rozsiritelnost**: Snadne pridani dalsich branch-specific features

### Negativni

- **Delsi URL**: `/branches/default/` pridano ke vsem cestam
- **Breaking change**: Stare URL prestane fungovat
- **Komplexnejsi implementace**: Kazdy endpoint musi resit branch logiku

### Neutralni

- **Testovani**: Vice test kombinaci (main vs branch pro kazdy endpoint)

## Reference

- ADR-007: Copy-on-Write Branching (zakladni CoW logika)
- ADR-009: Per-table DuckDB files (storage struktura)
- GitHub URL design: `/repo/tree/{branch}/path`
- GitLab URL design: `/repo/-/tree/{branch}/path`
