# DuckDB SiStat Extension

[![CI](https://github.com/fklezin/duckdb-sistat/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/fklezin/duckdb-sistat/actions/workflows/MainDistributionPipeline.yml)
[![SiStat Table Count](https://github.com/fklezin/duckdb-sistat/actions/workflows/SistatTableCount.yml/badge.svg)](https://github.com/fklezin/duckdb-sistat/actions/workflows/SistatTableCount.yml)

Query Slovenia's [SiStat](https://pxweb.stat.si/sistat/sl/Home/Help) open data portal directly from DuckDB. No external Python scripts or ETL pipelines required — just SQL.

This extension integrates the **Statistical Office of the Republic of Slovenia (SURS)** data into your analytical workflows.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Usecases](#usecases)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Discover Data**: List all 1000+ available datasets with `SISTAT_Tables(language := 'en')`.
- **Inspect Metadata**: View dimensions, variables, and allowed values with `SISTAT_DataStructure(table_id, language := 'en')`.
- **Direct Querying**: Read full datasets into DuckDB tables with `SISTAT_Read(table_id, language := 'en')`; use SQL `WHERE` and `LIMIT` as needed.
- **Live Data**: Always fetches the latest data from the official API.
- **Current Table Count**: Latest successful count run reports **4,597 tables** (see [SiStat Table Count workflow](https://github.com/fklezin/duckdb-sistat/actions/workflows/SistatTableCount.yml)).

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

### Top 10 grape varieties (share of vineyards, latest year)

Using SiStat table `1528317S.px`, aggregate vineyard area by grape variety and rank the top 10.
This gives variety shares in the latest available year (`2020`) and includes examples like Refošk, Chardonnay, and Modra frankinja.
The combined category `Ostale bele sorte` is excluded.

```sql
-- @README.md
WITH latest_year AS (
  SELECT MAX(TRY_CAST("LETO" AS INTEGER)) AS y
  FROM SISTAT_Read('1528317S', language := 'sl')
),
agg AS (
  SELECT
    "VINSKE SORTE" AS sort_code,
    SUM(TRY_CAST(value AS DOUBLE)) AS area_ha
  FROM SISTAT_Read('1528317S', language := 'sl'), latest_year
  WHERE value IS NOT NULL
    AND value <> ''
    AND TRY_CAST("LETO" AS INTEGER) = latest_year.y
  GROUP BY 1
),
filtered AS (
  SELECT *
  FROM agg
  WHERE sort_code <> '1.14'
),
labels(sort_code, grape_variety) AS (
  VALUES
    ('1.04', 'Laski rizling'),
    ('2.08', 'Refosk'),
    ('1.02', 'Chardonnay'),
    ('1.09', 'Sauvignon'),
    ('1.05', 'Malvazija'),
    ('2.10', 'Zametovka'),
    ('2.04', 'Merlot'),
    ('1.08', 'Rumeni muskat'),
    ('2.05', 'Modra frankinja'),
    ('1.06', 'Rebula')
),
named AS (
  SELECT
    f.sort_code,
    COALESCE(l.grape_variety, f.sort_code) AS grape_variety,
    f.area_ha
  FROM filtered f
  LEFT JOIN labels l USING (sort_code)
),
filtered_total AS (
  SELECT SUM(area_ha) AS total_area
  FROM named
)
SELECT
  grape_variety,
  ROUND(area_ha, 1) AS area_ha,
  ROUND(100.0 * area_ha / filtered_total.total_area, 2) AS share_all_pct,
  repeat('#', CAST(ROUND(100.0 * area_ha / filtered_total.total_area) AS INTEGER)) AS bar
FROM named, filtered_total
ORDER BY area_ha DESC
LIMIT 10;
```

## Configuration

The extension uses DuckDB's built-in HTTP capabilities. It respects proxy settings if configured in DuckDB.

## Contributing

Contributions are welcome. Please feel free to submit a pull request.

## License
MIT
