-- @README.md block 6
-- @README.md
CREATE TABLE population_data AS
SELECT * FROM SISTAT_Read('05C1002S', language := 'en');
SELECT COUNT(*) AS loaded_rows FROM population_data;
