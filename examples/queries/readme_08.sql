-- @README.md block 8
-- @README.md
SELECT
  "LETO" AS year,
  TRY_CAST(value AS DOUBLE) AS usable_production_thousand_hl
FROM SISTAT_Read('1563407S', language := 'en')
WHERE "PROIZVODNJA IN PORABA" = '1.1.'
  AND "VINO" = '00'
  AND "LETO" = '2020';
