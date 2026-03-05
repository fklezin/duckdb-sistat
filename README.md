# DuckDB SiStat Extension

[![CI](https://github.com/fklezin/duckdb-sistat/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/fklezin/duckdb-sistat/actions/workflows/MainDistributionPipeline.yml)
[![SiStat Table Count](https://github.com/fklezin/duckdb-sistat/actions/workflows/SistatTableCount.yml/badge.svg)](https://github.com/fklezin/duckdb-sistat/actions/workflows/SistatTableCount.yml)

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
-- @README.md
-- INSTALL sistat FROM community;
-- LOAD sistat;
SELECT 'community_install_example' AS status;
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
-- @README.md
LOAD 'build/release/extension/sistat/sistat.duckdb_extension';
SELECT 'local_load_ok' AS status;
```

## Usage

**Typical flow:** discover tables → pick a `table_id` → (optional) inspect structure → read data with `WHERE`/`LIMIT` as needed.

### 1. Find Data Tables
List tables and narrow down by keyword. Prefer stable `table_id` in scripts; titles can change.

```sql
-- @README.md
SELECT title, table_id, updated
FROM SISTAT_Tables(language := 'en')
ORDER BY updated DESC
LIMIT 5;
```

| title | table_id | updated |
|-------|----------|---------|
| Population by age... | 05C1002S | 2024-01-01 |

### 2. Inspect Data Structure
Before reading, check dimensions and value codes so you can filter correctly.

```sql
-- @README.md
SELECT variable_code, variable_text, position, value_codes, value_texts
FROM SISTAT_DataStructure('05C1002S', language := 'en')
ORDER BY position;
```

### 3. Query the Data
Read the dataset. Put `WHERE` and `LIMIT` on the table-valued result. Treat `NULL`, `''`, and `'-'` as missing; use `TRY_CAST(value AS DOUBLE)` for numeric analysis.

```sql
-- @README.md
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
-- @README.md
CREATE TABLE population_data AS
SELECT * FROM SISTAT_Read('05C1002S', language := 'en');
SELECT COUNT(*) AS loaded_rows FROM population_data;
```

### 4. End-to-End Example
This example shows the full workflow in one script.

```sql
-- @README.md
-- 1) Find a table
SELECT title, table_id
FROM SISTAT_Tables(language := 'en')
WHERE LOWER(title) LIKE '%population%'
LIMIT 1;

-- 2) Inspect available dimensions
SELECT variable_code, variable_text
FROM SISTAT_DataStructure('05C1002S', language := 'en')
ORDER BY position;

-- 3) Materialize data locally
CREATE OR REPLACE TABLE population_data AS
SELECT *
FROM SISTAT_Read('05C1002S', language := 'en')
WHERE value IS NOT NULL AND value <> '' AND value <> '-';

-- 4) Run analysis
SELECT
  "SPOL" AS sex_code,
  AVG(TRY_CAST(value AS DOUBLE)) AS avg_value
FROM population_data
GROUP BY 1
ORDER BY 1;
```

### Querying tips
- Start from **metadata** (`SISTAT_Tables`, then `SISTAT_DataStructure`) before reading large tables.
- **Filter early** with `WHERE` on `SISTAT_Read(...)` to reduce transferred rows.
- Prefer **explicit column selection** over `SELECT *` for stable queries.
- For **reproducibility**, materialize a snapshot into a local table (e.g. with `CURRENT_TIMESTAMP`).

## Usecases

### Which Slovenian wine region likely produces the most wine?

SiStat exposes:
- wine-growing-region vineyard area (`1528309S.px`, language `sl`)
- national usable wine production in `1000 hl` (`1563407S.px`, code `PROIZVODNJA IN PORABA = '1.1.'`, year `2020`)

There is currently no direct regional wine-production-in-litres table in the SiStat catalog.  
For a practical estimate, allocate national usable production to wine regions by their share of vineyard area.

Inputs used:
- National usable production (2020): `725.46` thousand hl = `72,546,000` litres
- Vineyard area shares (2020): Podravje `6,244.2 ha`, Posavje `2,552.1 ha`, Primorje `6,437.4 ha`

Estimated litres by wine region (2020):

| Region | Estimated litres | Share |
|---|---:|---:|
| Primorje | 30,656,217 | 42.26% |
| Podravje | 29,736,160 | 40.99% |
| Posavje | 12,153,623 | 16.75% |

Quick plot (estimated, in million litres):

```text
Primorje | ############################### 30.66M
Podravje | ##############################  29.74M
Posavje  | ############                    12.15M
```

Reproducible national input query:

```sql
-- @README.md
SELECT
  "LETO" AS year,
  TRY_CAST(value AS DOUBLE) AS usable_production_thousand_hl
FROM SISTAT_Read('1563407S', language := 'en')
WHERE "PROIZVODNJA IN PORABA" = '1.1.'
  AND "VINO" = '00'
  AND "LETO" = '2020';
```

## Configuration

The extension uses DuckDB's built-in HTTP capabilities. It respects proxy settings if configured in DuckDB.

## Contributing

Contributions are welcome. Please feel free to submit a pull request.

## License
MIT
