# DuckDB SiStat Extension

[![CI](https://github.com/fklezin/duckdb-sistat/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/fklezin/duckdb-sistat/actions/workflows/MainDistributionPipeline.yml)

Query Slovenia's [SiStat](https://pxweb.stat.si/sistat/sl/Home/Help) open data portal directly from DuckDB. No external Python scripts or ETL pipelines required — just SQL.

This extension integrates the **Statistical Office of the Republic of Slovenia (SURS)** data into your analytical workflows.

## Features

- **Discover Data**: List all 1000+ available datasets with `SISTAT_Tables(language := 'en')`.
- **Inspect Metadata**: View dimensions, variables, and allowed values with `SISTAT_DataStructure(table_id, language := 'en')`.
- **Direct Querying**: Read full datasets into DuckDB tables with `SISTAT_Read(table_id, language := 'en')`; use SQL `WHERE` and `LIMIT` as needed.
- **Live Data**: Always fetches the latest data from the official API.

All functions accept an optional `language` argument (e.g. `'en'`, `'sl'`). You can pass `table_id` with or without the `.px` suffix; the extension normalizes it.

## Installation

### From Community Extensions (Recommended)
*Once accepted into the DuckDB Community Repository:*

```sql
INSTALL sistat FROM community;
LOAD sistat;
```

### Manual Build
To build the extension from source:

```bash
git clone https://github.com/fklezin/duckdb-sistat
cd duckdb-sistat
make
```

Then load it in DuckDB:
```sql
LOAD 'build/release/extension/sistat/sistat.duckdb_extension';
```

## Usage

**Typical flow:** discover tables → pick a `table_id` → (optional) inspect structure → read data with `WHERE`/`LIMIT` as needed. For a full query guide and best practices, see [docs/sistat-queries.md](../docs/sistat-queries.md).

### 1. Find Data Tables
List tables and narrow down by keyword. Prefer stable `table_id` in scripts; titles can change.

```sql
SELECT title, table_id, updated
FROM SISTAT_Tables(language := 'en')
WHERE LOWER(title) LIKE '%demographics%'
ORDER BY updated DESC
LIMIT 5;
```

| title | table_id | updated |
|-------|----------|---------|
| Population by age... | 05C1002S | 2024-01-01 |

### 2. Inspect Data Structure
Before reading, check dimensions and value codes so you can filter correctly.

```sql
SELECT variable_code, variable_text, position, value_codes, value_texts
FROM SISTAT_DataStructure('05C1002S', language := 'en')
ORDER BY position;
```

### 3. Query the Data
Read the dataset. Put `WHERE` and `LIMIT` on the table-valued result. Treat `NULL`, `''`, and `'-'` as missing; use `TRY_CAST(value AS DOUBLE)` for numeric analysis.

```sql
SELECT
  "KOHEZIJSKA REGIJA",
  "STAROST",
  "POLLETJE",
  "SPOL",
  TRY_CAST(value AS DOUBLE) AS value_num
FROM SISTAT_Read('05C1002S', language := 'en')
WHERE value IS NOT NULL AND value <> '' AND value <> '-'
LIMIT 500;
```

For a one-off full load:

```sql
CREATE TABLE population_data AS
SELECT * FROM SISTAT_Read('05C1002S', language := 'en');
```

### Querying tips
- Start from **metadata** (`SISTAT_Tables`, then `SISTAT_DataStructure`) before reading large tables.
- **Filter early** with `WHERE` on `SISTAT_Read(...)` to reduce transferred rows.
- Prefer **explicit column selection** over `SELECT *` for stable queries.
- For **reproducibility**, materialize a snapshot into a local table (e.g. with `CURRENT_TIMESTAMP`).

Full patterns and examples: [docs/sistat-queries.md](../docs/sistat-queries.md).

## Configuration

The extension uses DuckDB's built-in HTTP capabilities. It respects proxy settings if configured in DuckDB.

## Contributing

Contributions are welcome. Please feel free to submit a pull request.

## License
MIT
