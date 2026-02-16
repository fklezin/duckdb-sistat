#pragma once

namespace duckdb {

class DatabaseInstance;
struct SistatInfoFunctions {
	static void Register(DatabaseInstance &db);
};

} // namespace duckdb
