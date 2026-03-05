#!/usr/bin/env bash
set -euo pipefail

QUERY_DIR="${1:-examples/queries}"
RESULT_DIR="${2:-examples/results}"
DB_BIN="${3:-./build/release/duckdb}"
EXT_PATH="${4:-build/release/extension/sistat/sistat.duckdb_extension}"

mkdir -p "${RESULT_DIR}"
rm -f "${RESULT_DIR}"/readme_*.txt

shopt -s nullglob
queries=("${QUERY_DIR}"/readme_*.sql)
if [ "${#queries[@]}" -eq 0 ]; then
	echo "No README query files found in ${QUERY_DIR}" >&2
	exit 1
fi

for query_file in "${queries[@]}"; do
	base="$(basename "${query_file}" .sql)"
	out_file="${RESULT_DIR}/${base}.txt"
	tmp_sql="$(mktemp)"

	{
		echo "-- Auto-generated runner prelude"
		echo "LOAD '${EXT_PATH}';"
		cat "${query_file}"
	} > "${tmp_sql}"

	"${DB_BIN}" -csv ":memory:" < "${tmp_sql}" > "${out_file}"
	rm -f "${tmp_sql}"

	non_empty_lines="$(grep -cve '^[[:space:]]*$' "${out_file}" || true)"
	if [ "${non_empty_lines}" -lt 2 ]; then
		echo "Query ${query_file} returned no data rows" >&2
		exit 1
	fi

	echo "Ran ${query_file} -> ${out_file}"
done

echo "Executed ${#queries[@]} README query files"
