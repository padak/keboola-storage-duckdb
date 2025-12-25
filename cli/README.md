# Keboola DuckDB CLI

Command line tool for Keboola DuckDB Storage API.

## Installation

```bash
pip install -e .
```

## Configuration

```bash
# Set API URL and key
keboola-duckdb config set url http://localhost:8000
keboola-duckdb config set api-key <your-api-key>

# Or use environment variables
export KEBOOLA_DUCKDB_URL=http://localhost:8000
export KEBOOLA_DUCKDB_API_KEY=<your-api-key>
```

## Usage

```bash
# List projects (requires admin key)
keboola-duckdb projects list

# List buckets in a project
keboola-duckdb buckets list <project-id>

# List tables
keboola-duckdb tables list <project-id> <bucket-name>

# Preview table data
keboola-duckdb tables preview <project-id> <bucket-name> <table-name> --limit 10

# Import CSV to table
keboola-duckdb tables import <project-id> <bucket-name> <table-name> data.csv

# Export table to CSV
keboola-duckdb tables export <project-id> <bucket-name> <table-name> output.csv

# File operations
keboola-duckdb files upload <project-id> myfile.csv
keboola-duckdb files list <project-id>
keboola-duckdb files download <project-id> <file-id> downloaded.csv
```

## Global Options

- `--json` / `-j`: Output as JSON
- `--verbose` / `-v`: Show debug information
- `--help` / `-h`: Show help
