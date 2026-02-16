#pragma once

namespace duckdb {

class DatabaseInstance;
struct SistatDataFunctions {
	static void Register(DatabaseInstance &db);
};

} // namespace duckdb
