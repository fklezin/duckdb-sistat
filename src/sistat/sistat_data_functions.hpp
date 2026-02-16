#pragma once

namespace duckdb {

class ExtensionLoader;
struct SistatDataFunctions {
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
