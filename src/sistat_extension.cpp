#define DUCKDB_EXTENSION_MAIN

#include "sistat_extension.hpp"
#include "duckdb.hpp"
#include "sistat/sistat_data_functions.hpp"
#include "sistat/sistat_info_functions.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	SistatDataFunctions::Register(loader);
	SistatInfoFunctions::Register(loader);
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

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sistat, loader) {
	duckdb::LoadInternal(loader);
}
}
#endif
