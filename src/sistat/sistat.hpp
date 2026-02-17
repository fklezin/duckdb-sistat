#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {
namespace sistat {

constexpr const char *BASE_URL = "https://pxweb.stat.si/SiStatData/api/v1/";
constexpr const char *DATA_PATH = "Data/";
constexpr const char *DEFAULT_LANGUAGE = "en";

inline string TableUrl(const string &lang, const string &table_id) {
	return string(BASE_URL) + lang + "/" + DATA_PATH + table_id;
}

inline string NormalizeTableId(const string &table_id) {
	if (table_id.size() >= 3 && table_id[table_id.size() - 3] == '.' &&
	    (table_id[table_id.size() - 2] == 'p' || table_id[table_id.size() - 2] == 'P') &&
	    (table_id[table_id.size() - 1] == 'x' || table_id[table_id.size() - 1] == 'X')) {
		return table_id;
	}
	return table_id + ".px";
}

} // namespace sistat
} // namespace duckdb
