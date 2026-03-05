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
