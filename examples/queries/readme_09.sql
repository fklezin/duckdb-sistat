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
total AS (
  SELECT SUM(area_ha) AS total_area
  FROM agg
)
SELECT
  CASE sort_code
    WHEN '1.14' THEN 'Ostale bele sorte'
    WHEN '1.04' THEN 'Laski rizling'
    WHEN '2.08' THEN 'Refosk'
    WHEN '1.02' THEN 'Chardonnay'
    WHEN '1.09' THEN 'Sauvignon'
    WHEN '1.05' THEN 'Malvazija'
    WHEN '2.10' THEN 'Zametovka'
    WHEN '2.04' THEN 'Merlot'
    WHEN '1.08' THEN 'Rumeni muskat'
    WHEN '2.05' THEN 'Modra frankinja'
    ELSE sort_code
  END AS grape_variety,
  ROUND(area_ha, 1) AS area_ha,
  ROUND(100.0 * area_ha / total.total_area, 2) AS share_all_pct
FROM agg, total
ORDER BY area_ha DESC
LIMIT 10;
