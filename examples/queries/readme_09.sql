-- @README.md block 9
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
dim AS (
  SELECT value_codes, value_texts
  FROM SISTAT_DataStructure('1528317S', language := 'sl')
  WHERE variable_code = 'VINSKE SORTE'
),
labels AS (
  SELECT
    list_extract(code_list, i) AS sort_code,
    list_extract(text_list, i) AS raw_name
  FROM (
    SELECT
      string_split(replace(replace(replace(value_codes, '[', ''), ']', ''), '"', ''), ',') AS code_list,
      string_split(replace(replace(replace(value_texts, '[', ''), ']', ''), '"', ''), ',') AS text_list
    FROM dim
  ) s,
  range(1, length(code_list) + 1) t(i)
),
named AS (
  SELECT
    a.sort_code,
    trim(regexp_replace(l.raw_name, '^(Bele sorte- |Rdeče sorte- )', '')) AS grape_variety,
    a.area_ha
  FROM agg a
  JOIN labels l USING (sort_code)
),
filtered AS (
  SELECT *
  FROM named
  WHERE lower(grape_variety) NOT LIKE 'ostale %'
    AND lower(grape_variety) <> 'ni podatka o sorti'
),
filtered_total AS (
  SELECT SUM(area_ha) AS total_area
  FROM filtered
)
SELECT
  grape_variety,
  ROUND(area_ha, 1) AS area_ha,
  ROUND(100.0 * area_ha / filtered_total.total_area, 2) AS share_all_pct,
  repeat('#', CAST(ROUND(100.0 * area_ha / filtered_total.total_area) AS INTEGER)) AS bar
FROM filtered, filtered_total
ORDER BY area_ha DESC
LIMIT 10;
