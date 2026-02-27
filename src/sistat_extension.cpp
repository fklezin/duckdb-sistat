#define DUCKDB_EXTENSION_MAIN

#include "sistat_extension.hpp"
#include "duckdb.hpp"
#ifndef __EMSCRIPTEN__
#include "sistat/sistat_data_functions.hpp"
#include "sistat/sistat_info_functions.hpp"
#endif

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
#ifndef __EMSCRIPTEN__
	SistatDataFunctions::Register(loader);
	SistatInfoFunctions::Register(loader);
#else
	(void)loader;
#endif
}

void SistatExtension::Load(ExtensionLoader &db) {
	LoadInternal(db);
}

std::string SistatExtension::Name() {
	return "sistat";
}

std::string SistatExtension::Version() const {
#ifdef EXT_VERSION_SISTAT
	return EXT_VERSION_SISTAT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void sistat_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadStaticExtension<duckdb::SistatExtension>();
}

DUCKDB_EXTENSION_API void sistat_duckdb_cpp_init(duckdb::DatabaseInstance &db) {
	sistat_init(db);
}

DUCKDB_EXTENSION_API const char *sistat_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
