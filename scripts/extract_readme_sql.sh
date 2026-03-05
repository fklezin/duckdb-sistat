#!/usr/bin/env bash
set -euo pipefail

README_PATH="${1:-README.md}"
OUT_DIR="${2:-examples/queries}"

mkdir -p "${OUT_DIR}"
rm -f "${OUT_DIR}"/readme_*.sql

awk -v out_dir="${OUT_DIR}" '
BEGIN {
	in_sql = 0
	block = 0
}
/^```sql[[:space:]]*$/ {
	in_sql = 1
	block++
	file = sprintf("%s/readme_%02d.sql", out_dir, block)
	print "-- @README.md block " block > file
	next
}
/^```[[:space:]]*$/ && in_sql {
	in_sql = 0
	close(file)
	next
}
in_sql {
	print >> file
}
' "${README_PATH}"

count="$(find "${OUT_DIR}" -maxdepth 1 -type f -name 'readme_*.sql' | wc -l | tr -d ' ')"
echo "Extracted ${count} SQL blocks from ${README_PATH} into ${OUT_DIR}"
