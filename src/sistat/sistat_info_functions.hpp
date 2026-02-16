#pragma once

namespace duckdb {

class ExtensionLoader;
struct SistatInfoFunctions {
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
