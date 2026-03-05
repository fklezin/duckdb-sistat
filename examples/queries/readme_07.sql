-- @README.md block 7
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
