#define DUCKDB_EXTENSION_MAIN

#include "sistat_extension.hpp"
#include "duckdb.hpp"
#include "sistat/sistat_data_functions.hpp"
#include "sistat/sistat_info_functions.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &db) {
	SistatDataFunctions::Register(db);
	SistatInfoFunctions::Register(db);
}

void SistatExtension::Load(ExtensionLoader &db) {
	LoadInternal(db.GetDatabaseInstance());
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
    db_wrapper.LoadExtension<duckdb::SistatExtension>();
}

DUCKDB_EXTENSION_API const char *sistat_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}
