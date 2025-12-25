# Phase 19: Advanced Table Profiling

**Status:** DONE (2024-12-25)

## Overview

Implementation of advanced statistical profiling for DuckDB tables, providing data scientists and data engineers with comprehensive data quality insights, distribution analysis, and pattern detection.

## Features Implemented

### 1. Core Statistics (Enhanced)

| Metric | Description | DuckDB Function |
|--------|-------------|-----------------|
| Skewness | Distribution asymmetry (0=symmetric) | `SKEWNESS(col)` |
| Kurtosis | Tail heaviness (0=normal) | `KURTOSIS(col)` |
| Extended Percentiles | Q01, Q05, Q25, Q50, Q75, Q95, Q99 | `QUANTILE_CONT(col, [0.01, ...])` |

### 2. Cardinality Analysis

| Class | Criteria | Use Case |
|-------|----------|----------|
| `unique` | 100% distinct values | Primary key candidate |
| `high` | >90% distinct | High cardinality dimension |
| `medium` | 50-90% distinct | Regular dimension |
| `low` | 1-50% distinct | Categorical/enum candidate |
| `very_low` | <1% distinct | Enum recommended |
| `constant` | Single value | Data quality issue |

### 3. Outlier Detection (IQR Method)

```
Lower bound = Q25 - 1.5 * IQR
Upper bound = Q75 + 1.5 * IQR
where IQR = Q75 - Q25
```

Returns:
- `outlier_count`: Number of values outside bounds
- `outlier_lower_bound`: Lower threshold
- `outlier_upper_bound`: Upper threshold

### 4. Data Quality Score

Scoring algorithm (starts at 100, deductions):
- -5 points per column with >50% nulls
- -2 points per column with >5% outliers
- Quality labels: Excellent (90+), Good (75-90), Fair (50-75), Poor (<50)

### 5. Quality Issues & Recommendations

| Issue Type | Severity | Example Message |
|------------|----------|-----------------|
| `high_nulls` | warning | "Column has 75.0% null values" |
| `outliers` | warning | "156 outliers detected (8.9%)" |
| `skewed` | info | "Highly right-skewed distribution (skewness=2.41)" |
| `pk_candidate` | info | "100% unique, non-null - potential primary key" |
| `enum_candidate` | info | "Only 5 distinct values - consider ENUM type" |
| `constant` | info | "Column has constant value" |

### 6. String Column Analysis

| Metric | Description |
|--------|-------------|
| `avg_length` | Average string length |
| `min_length` | Minimum string length |
| `max_length` | Maximum string length |
| `empty_count` | Count of empty strings |
| `whitespace_only_count` | Count of whitespace-only strings |

### 7. Pattern Detection

Regex-based detection for common data patterns:

| Pattern | Regex |
|---------|-------|
| `email` | `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$` |
| `uuid` | `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$` |
| `url` | `^https?://` |
| `phone` | `^\+?[0-9\s\-\(\)]{10,20}$` |
| `ipv4` | `^(?:(?:25[0-5]\|2[0-4][0-9]\|[01]?[0-9][0-9]?)\.){3}...` |
| `date_iso` | `^\d{4}-\d{2}-\d{2}$` |
| `datetime_iso` | `^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}` |

### 8. Column Correlations

Pearson correlation coefficient between numeric columns:
- Only returns correlations with |r| > 0.3
- Classified as `strong` (|r| > 0.7) or `moderate` (0.3-0.7)
- Top 20 correlations returned, sorted by absolute value

## API Changes

### Endpoint

```
POST /projects/{id}/branches/{branch}/buckets/{bucket}/tables/{table}/profile
```

### Query Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `mode` | `basic`, `full`, `distribution`, `quality` | `basic` | Profile mode |

### Mode Differences

| Mode | Features |
|------|----------|
| `basic` | All core stats, cardinality, outliers, quality score/issues |
| `distribution` | Basic + histogram data |
| `quality` | Basic + pattern detection + correlations |
| `full` | All features enabled |

### Response Schema (new fields)

```python
class ColumnStatistics:
    # Existing
    column_name, column_type, min, max, count, null_percentage

    # New - Cardinality
    approx_unique: int
    cardinality_ratio: float  # 0-1
    cardinality_class: str    # unique/high/medium/low/very_low/constant

    # New - Distribution
    avg, std: float
    skewness, kurtosis: float
    q01, q05, q25, q50, q75, q95, q99: Any

    # New - Outliers
    outlier_count: int
    outlier_lower_bound, outlier_upper_bound: float

    # New - Histogram (distribution mode)
    histogram: dict

    # New - String stats
    avg_length, min_length, max_length: float/int
    empty_count, whitespace_only_count: int

    # New - Patterns (quality/full mode)
    detected_patterns: list[DetectedPattern]

class TableProfileResponse:
    # Existing
    table_name, bucket_name, row_count, column_count, statistics

    # New
    quality_score: float           # 0-100
    quality_issues: list[QualityIssue]
    correlations: list[ColumnCorrelation]  # quality/full mode
```

## CLI Changes

### New Options

```bash
keboola-duckdb tables profile PROJECT BUCKET TABLE [OPTIONS]

Options:
  --mode, -m      Profile mode: basic, full, distribution, quality
  --quality, -q   Show data quality report with recommendations
  --distribution, -d   Show distribution details (percentiles, kurtosis, histograms)
  --correlations, -r   Show column correlations
  --columns, -c   Filter specific columns
  --json          Full JSON output
```

### Mode Behavior

When using `-m <mode>`, the CLI automatically enables the corresponding display options:

| Mode | Enables |
|------|---------|
| `basic` | Default table view with alerts |
| `quality` | `-q` (quality report) + `-r` (correlations) |
| `distribution` | `-d` (distribution details + histograms) |
| `full` | `-q` + `-r` + `-d` (all features) |

### Output Enhancements

**Default output includes:**
- Quality score with color-coded label
- Cardinality class column
- Skewness column
- Outlier count column
- Alert badges (PK?, ENUM?, OUT, SKEW, CONST)

**With `-q` (quality):**
- Warnings section (outliers, high nulls)
- Recommendations section (PK candidates, ENUM suggestions, skewness warnings)

**With `-r` (correlations):**
- Strong correlations with visual bars
- Moderate correlations list

**With `-d` (distribution):**
- Per-column percentile breakdown
- Skewness/kurtosis interpretation
- Outlier bounds
- Text-based histogram visualization (for numeric columns)

## DuckDB Functions Used

```sql
-- Single-pass numeric statistics
SELECT
    COUNT(*) as total_count,
    COUNT(DISTINCT col) as unique_count,
    AVG(col), STDDEV(col),
    SKEWNESS(col), KURTOSIS(col),
    QUANTILE_CONT(col, [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])
FROM table_data;

-- IQR outlier detection
SELECT COUNT(*) FROM table_data
WHERE col < q25 - 1.5 * (q75 - q25)
   OR col > q75 + 1.5 * (q75 - q25);

-- Pattern detection
SELECT COUNT(*) FROM table_data
WHERE regexp_full_match(col, '^[email-regex]$');

-- Correlation
SELECT CORR(col1, col2) FROM table_data;

-- Histogram (for distribution mode)
SELECT HISTOGRAM(col) FROM table_data;
```

## Performance Considerations

1. **Per-column queries**: Each column is analyzed separately to avoid memory issues with wide tables
2. **Pattern detection**: Only runs in `full` or `quality` mode to avoid regex overhead
3. **Correlations**: Limited to first 10 numeric columns, top 20 results
4. **Histogram**: Only computed in `distribution` or `full` mode

## Example Output

```
Table: padak_cli.sample
Rows: 10,000 | Columns: 16 | Quality: 96% (Excellent)

╭────────────────┬───────────┬─────────────┬────────┬──────────┬────────┬────────╮
│ Column         │ Type      │ Cardinality │ Nulls% │ Avg      │ Skew   │ Alerts │
├────────────────┼───────────┼─────────────┼────────┼──────────┼────────┼────────┤
│ product_id     │ INTEGER   │ UNIQUE      │ 0.0%   │ 5,000.50 │ 0.00   │ PK?    │
│ price          │ DOUBLE    │ MED (88%)   │ 0.0%   │ 269.92   │ 2.41   │ SKEW   │
│ status         │ VARCHAR   │ VER (0%)    │ 0.0%   │ -        │ -      │ ENUM?  │
╰────────────────┴───────────┴─────────────┴────────┴──────────┴────────┴────────╯

Warnings:
  ! price: 888 outliers detected (8.9%)

Column Correlations:
  Strong (|r| > 0.7):
    price <-> cost: +0.99 ███████████████████
```

## Future Enhancements (Post-MVP)

1. ~~**Histogram visualization** - Text-based bar charts in CLI~~ - DONE (2024-12-25)
2. **Pattern display in CLI** - Show detected email/UUID patterns
3. **Type-based filtering** - `--numeric`, `--string` flags
4. **Export to file** - `--output profile.json`
5. **Trend detection** - Compare profiles over time
6. **Sampling mode** - Profile sample for very large tables
7. **Custom pattern definitions** - User-defined regex patterns

## Files Changed

### API Service (`duckdb-api-service/`)
- `src/database.py` - `get_table_profile()`, `_get_column_stats()`, `_detect_patterns()`, `_get_correlations()`
- `src/models/responses.py` - `ColumnStatistics`, `TableProfileResponse`, new models
- `src/routers/table_schema.py` - `profile_table()` endpoint with mode parameter

### CLI (`cli/`)
- `src/keboola_duckdb_cli/commands/tables.py` - `profile_table()` command
- `src/keboola_duckdb_cli/client.py` - `post()` with params support

## References

- DuckDB SUMMARIZE: https://duckdb.org/docs/stable/guides/meta/summarize.html
- DuckDB Statistical Functions: https://duckdb.org/docs/sql/functions/aggregates
- DuckDB Regex: https://duckdb.org/docs/stable/sql/functions/regular_expressions.html
