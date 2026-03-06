#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-$ROOT_DIR/build/release/duckdb}"
EXT_PATH="${EXT_PATH:-$ROOT_DIR/build/release/extension/sistat/sistat.duckdb_extension}"

if [[ ! -x "$DUCKDB_BIN" ]]; then
  echo "duckdb binary not found at $DUCKDB_BIN" >&2
  exit 1
fi

if [[ ! -f "$EXT_PATH" ]]; then
  echo "extension not found at $EXT_PATH" >&2
  exit 1
fi

run_query() {
  local sql="$1"
  local attempt
  local output

  for attempt in 1 2 3 4 5 6 7 8; do
    if output="$("$DUCKDB_BIN" -csv -c "LOAD '$EXT_PATH'; COPY ($sql) TO STDOUT (FORMAT CSV, HEADER FALSE);" 2>/dev/null)"; then
      printf '%s\n' "$output"
      return 0
    fi
    sleep $((attempt * 2))
  done

  echo "query failed after retries: $sql" >&2
  return 1
}

assert_eq() {
  local actual="$1"
  local expected="$2"
  local label="$3"

  if [[ "$actual" != "$expected" ]]; then
    echo "FAIL: $label" >&2
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    exit 1
  fi

  echo "ok: $label"
}

tables_ready="$(run_query "SELECT CASE WHEN COUNT(*) > 4000 THEN 1 ELSE 0 END FROM SISTAT_Tables(language := 'en')")"
assert_eq "$tables_ready" "1" "english table catalog is populated"

known_table="$(run_query "SELECT COUNT(*) FROM SISTAT_Tables(language := 'en') WHERE table_id = '05C1002S.px' AND url = 'https://pxweb.stat.si/SiStatData/api/v1/en/Data/05C1002S.px'")"
assert_eq "$known_table" "1" "known english table is discoverable"

normalized_structure="$(run_query "SELECT COUNT(*) FROM ((SELECT * FROM SISTAT_DataStructure('05C1002S', language := 'en') EXCEPT SELECT * FROM SISTAT_DataStructure('05C1002S.px', language := 'en')) UNION ALL (SELECT * FROM SISTAT_DataStructure('05C1002S.px', language := 'en') EXCEPT SELECT * FROM SISTAT_DataStructure('05C1002S', language := 'en')))")"
assert_eq "$normalized_structure" "0" "table id normalization preserves data structure output"

sex_dimension="$(run_query "SELECT COUNT(*) FROM SISTAT_DataStructure('05C1002S', language := 'en') WHERE variable_code = 'SPOL' AND variable_text = 'SEX'")"
assert_eq "$sex_dimension" "1" "expected dimension metadata is present"

read_populated="$(run_query "SELECT CASE WHEN COUNT(*) > 30000 THEN 1 ELSE 0 END FROM SISTAT_Read('05C1002S', language := 'en')")"
assert_eq "$read_populated" "1" "read returns populated dataset"

known_fact="$(run_query "SELECT COUNT(*) FROM SISTAT_Read('05C1002S', language := 'en') WHERE \"KOHEZIJSKA REGIJA\" = '0' AND \"STAROST\" = '999' AND \"POLLETJE\" = '2008H1' AND \"SPOL\" = '0' AND TRY_CAST(value AS BIGINT) = 2025866")"
assert_eq "$known_fact" "1" "known historical row is present"
