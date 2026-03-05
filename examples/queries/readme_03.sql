-- @README.md block 3
-- @README.md
SELECT title, table_id, updated
FROM SISTAT_Tables(language := 'en')
ORDER BY updated DESC
LIMIT 5;
