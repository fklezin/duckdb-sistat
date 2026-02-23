# DuckDB SiStat Extension

[![CI](https://github.com/fklezin/duckdb-sistat/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/fklezin/duckdb-sistat/actions/workflows/MainDistributionPipeline.yml)

Query Slovenia's [SiStat](https://pxweb.stat.si/sistat/sl/Home/Help) open data portal directly from DuckDB. No external Python scripts or ETL pipelines required — just SQL.

This extension integrates the **Statistical Office of the Republic of Slovenia (SURS)** data into your analytical workflows.

## Features

- **Discover Data**: List all 1000+ available datasets with `SISTAT_Tables()`.
- **Inspect Metadata**: View dimensions, variables, and allowed values with `SISTAT_DataStructure()`.
- **Direct Querying**: Read full datasets into DuckDB tables with `SISTAT_Read()`.
- **Live Data**: Always fetches the latest data from the official API.

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

### 1. Find Data Tables
Search for datasets. For example, to find population / demographics data:

```sql
SELECT title, table_id, updated 
FROM SISTAT_Tables() 
WHERE title ILIKE '%demographics%' 
LIMIT 5;
```

| title | table_id | updated |
|-------|----------|---------|
| Population by age... | 05C1002S | 2024-01-01 |

### 2. Inspect Data Structure
Before reading a dataset, check its dimensions (e.g., Year, Region, Gender) to understand what columns you will get.

```sql
SELECT 
    variable_code, 
    variable_text, 
    length(value_codes) as num_options 
FROM SISTAT_DataStructure('05C1002S');
```

### 3. Query the Data
Pull the actual data. The extension automatically maps the JSON response to a relational table format.

```sql
CREATE TABLE population_data AS 
SELECT * FROM SISTAT_Read('05C1002S');

SELECT * FROM population_data LIMIT 10;
```

## Configuration

The extension uses DuckDB's built-in HTTP capabilities. It respects proxy settings if configured in DuckDB.

## Contributing

Contributions are welcome. Please feel free to submit a pull request.

## License
MIT
