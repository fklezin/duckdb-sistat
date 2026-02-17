#include "sistat_data_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"
#include "sistat.hpp"
#include "http_request.hpp"

using duckdb_yyjson::yyjson_arr_get;
using duckdb_yyjson::yyjson_arr_size;
using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_get_num;
using duckdb_yyjson::yyjson_get_str;
using duckdb_yyjson::yyjson_get_uint;
using duckdb_yyjson::yyjson_is_arr;
using duckdb_yyjson::yyjson_is_null;
using duckdb_yyjson::yyjson_is_num;
using duckdb_yyjson::yyjson_is_obj;
using duckdb_yyjson::yyjson_is_str;
using duckdb_yyjson::yyjson_is_uint;
using duckdb_yyjson::yyjson_obj_get;
using duckdb_yyjson::yyjson_read;
using duckdb_yyjson::yyjson_val;

namespace duckdb {

namespace {

static bool IsStatisticalSymbol(const string &s) {
	if (s.empty()) {
		return true;
	}
	if (s == "-" || s == "..." || s == "z" || s == "M" || s == "N") {
		return true;
	}
	return false;
}

struct SISTAT_Read_Impl {

	struct BindData final : TableFunctionData {
		string table_id;
		string table_url;
		string language;
		vector<string> dimension_names;
		BindData(string table_id_p, string table_url_p, string language_p, vector<string> dimension_names_p)
		    : table_id(std::move(table_id_p)), table_url(std::move(table_url_p)), language(std::move(language_p)),
		      dimension_names(std::move(dimension_names_p)) {
		}
	};

	struct DataRow {
		vector<string> dimension_values;
		string value;
	};

	struct State final : GlobalTableFunctionState {
		vector<DataRow> rows;
		idx_t current_row = 0;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		if (input.inputs.empty()) {
			throw InvalidInputException("SISTAT_Read: table_id is required.");
		}
		string table_id = StringValue::Get(input.inputs[0]);
		if (table_id.empty()) {
			throw InvalidInputException("SISTAT_Read: table_id cannot be empty.");
		}

		string lang = sistat::DEFAULT_LANGUAGE;
		auto it = input.named_parameters.find("language");
		if (it != input.named_parameters.end() && !it->second.IsNull() && it->second.type() == LogicalType::VARCHAR) {
			lang = it->second.GetValue<string>();
		}
		if (lang.empty()) {
			lang = sistat::DEFAULT_LANGUAGE;
		}

		string normalized_id = sistat::NormalizeTableId(table_id);
		string table_url = sistat::TableUrl(lang, normalized_id);

		HttpSettings settings = HttpRequest::ExtractHttpSettings(context, table_url);
		HttpResponseData resp = HttpRequest::ExecuteHttpRequest(settings, table_url, "GET", {}, "", "");

		if (!resp.error.empty()) {
			throw IOException("SISTAT_Read: %s", resp.error.c_str());
		}
		if (resp.status_code != 200) {
			throw IOException("SISTAT_Read: HTTP %d - %s", resp.status_code, resp.body.c_str());
		}

		yyjson_doc *doc = yyjson_read(resp.body.c_str(), resp.body.size(), 0);
		if (!doc) {
			throw IOException("SISTAT_Read: Invalid metadata JSON");
		}

		yyjson_val *root = yyjson_doc_get_root(doc);
		yyjson_val *variables = yyjson_is_obj(root) ? yyjson_obj_get(root, "variables") : nullptr;
		if (!yyjson_is_arr(variables)) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_Read: Expected object with 'variables' array");
		}

		vector<string> dimension_names;
		size_t n_var = yyjson_arr_size(variables);
		for (size_t i = 0; i < n_var; i++) {
			yyjson_val *var_obj = yyjson_arr_get(variables, i);
			if (!yyjson_is_obj(var_obj)) {
				continue;
			}
			yyjson_val *code_val = yyjson_obj_get(var_obj, "code");
			if (yyjson_is_str(code_val)) {
				dimension_names.push_back(yyjson_get_str(code_val));
			}
		}
		yyjson_doc_free(doc);

		for (const auto &name : dimension_names) {
			names.emplace_back(name);
			return_types.push_back(LogicalType::VARCHAR);
		}
		names.emplace_back("value");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(normalized_id, table_url, lang, dimension_names);
	}

	static string BuildQueryJson(const vector<string> &dimension_names) {

		string s = "{\"query\":[";
		for (size_t i = 0; i < dimension_names.size(); i++) {
			if (i > 0) {
				s += ",";
			}
			s += "{\"code\":\"" + dimension_names[i] + "\",\"selection\":{\"filter\":\"all\",\"values\":[]}}";
		}
		s += "],\"response\":{\"format\":\"json-stat\"}}";
		return s;
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {

		auto &bind_data = input.bind_data->Cast<BindData>();
		auto state = make_uniq_base<GlobalTableFunctionState, State>();
		State *state_ptr = static_cast<State *>(state.get());

		string body = BuildQueryJson(bind_data.dimension_names);
		HttpSettings settings = HttpRequest::ExtractHttpSettings(context, bind_data.table_url);
		duckdb_httplib_openssl::Headers headers;
		HttpResponseData resp =
		    HttpRequest::ExecuteHttpRequest(settings, bind_data.table_url, "POST", headers, body, "application/json");

		if (!resp.error.empty()) {
			throw IOException("SISTAT_Read: %s", resp.error.c_str());
		}
		if (resp.status_code != 200) {
			throw IOException("SISTAT_Read: HTTP %d - %s", resp.status_code, resp.body.c_str());
		}

		yyjson_doc *doc = yyjson_read(resp.body.c_str(), resp.body.size(), 0);
		if (!doc) {
			throw IOException("SISTAT_Read: Invalid JSON-stat response");
		}

		yyjson_val *root = yyjson_doc_get_root(doc);
		yyjson_val *dataset = yyjson_is_obj(root) ? yyjson_obj_get(root, "dataset") : nullptr;
		if (!yyjson_is_obj(dataset)) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_Read: Expected root object with 'dataset'");
		}

		yyjson_val *dim = yyjson_obj_get(dataset, "dimension");
		if (!yyjson_is_obj(dim)) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_Read: dataset.dimension missing");
		}

		yyjson_val *id_arr = yyjson_obj_get(dim, "id");
		if (!yyjson_is_arr(id_arr)) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_Read: dataset.dimension.id must be array");
		}

		yyjson_val *size_arr = yyjson_obj_get(dim, "size");
		if (!yyjson_is_arr(size_arr)) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_Read: dataset.dimension.size must be array");
		}

		size_t num_dim = yyjson_arr_size(id_arr);
		if (yyjson_arr_size(size_arr) != num_dim) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_Read: dimension id/size length mismatch");
		}

		vector<string> dim_ids;
		vector<size_t> sizes;
		for (size_t i = 0; i < num_dim; i++) {
			yyjson_val *id_val = yyjson_arr_get(id_arr, i);
			if (yyjson_is_str(id_val)) {
				dim_ids.push_back(yyjson_get_str(id_val));
			} else {
				dim_ids.emplace_back("");
			}
			yyjson_val *sz_val = yyjson_arr_get(size_arr, i);
			if (yyjson_is_uint(sz_val)) {
				sizes.push_back(yyjson_get_uint(sz_val));
			} else {
				sizes.push_back(0);
			}
		}

		vector<vector<string>> codes_per_dim(num_dim);
		for (size_t d = 0; d < num_dim; d++) {
			yyjson_val *dim_obj = yyjson_obj_get(dim, dim_ids[d].c_str());
			if (!yyjson_is_obj(dim_obj)) {
				yyjson_doc_free(doc);
				throw IOException("SISTAT_Read: dimension.%s missing", dim_ids[d].c_str());
			}
			yyjson_val *cat = yyjson_obj_get(dim_obj, "category");
			if (!yyjson_is_obj(cat)) {
				yyjson_doc_free(doc);
				throw IOException("SISTAT_Read: dimension.%s.category missing", dim_ids[d].c_str());
			}
			yyjson_val *index_obj = yyjson_obj_get(cat, "index");
			if (!yyjson_is_obj(index_obj)) {
				yyjson_doc_free(doc);
				throw IOException("SISTAT_Read: dimension.%s.category.index missing", dim_ids[d].c_str());
			}
			vector<string> codes(sizes[d]);
			size_t iter_idx, iter_max;
			yyjson_val *key = nullptr;
			yyjson_val *val = nullptr;
			yyjson_obj_foreach(index_obj, iter_idx, iter_max, key, val) {
				if (yyjson_is_str(key) && yyjson_is_uint(val)) {
					size_t pos = yyjson_get_uint(val);
					if (pos < codes.size()) {
						codes[pos] = yyjson_get_str(key);
					}
				}
			}
			codes_per_dim[d] = std::move(codes);
		}

		yyjson_val *value_arr = yyjson_obj_get(dataset, "value");
		if (!yyjson_is_arr(value_arr)) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_Read: dataset.value must be array");
		}

		size_t total_cells = 1;
		for (size_t s : sizes) {
			total_cells *= s;
		}

		vector<size_t> stride(num_dim);
		stride[num_dim - 1] = 1;
		for (size_t d = num_dim - 1; d > 0; d--) {
			stride[d - 1] = stride[d] * sizes[d];
		}

		state_ptr->rows.reserve(total_cells);

		for (size_t flat_idx = 0; flat_idx < total_cells; flat_idx++) {

			vector<string> dim_vals(num_dim);
			size_t rem = flat_idx;
			for (size_t d = 0; d < num_dim; d++) {
				size_t i = rem / stride[d];
				rem %= stride[d];
				if (i < codes_per_dim[d].size()) {
					dim_vals[d] = codes_per_dim[d][i];
				}
			}

			yyjson_val *val_ele = yyjson_arr_get(value_arr, flat_idx);
			string value_str;
			if (yyjson_is_num(val_ele)) {
				double dbl = yyjson_get_num(val_ele);
				value_str = to_string(dbl);
			} else if (yyjson_is_str(val_ele)) {
				value_str = yyjson_get_str(val_ele);
			} else if (yyjson_is_null(val_ele)) {
				value_str.clear();
			}

			if (IsStatisticalSymbol(value_str)) {
				state_ptr->rows.push_back(DataRow {std::move(dim_vals), value_str});
			} else {
				state_ptr->rows.push_back(DataRow {std::move(dim_vals), std::move(value_str)});
			}
		}

		yyjson_doc_free(doc);
		return std::move(state);
	}

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

		auto &state = input.global_state->Cast<State>();
		auto &bind_data = input.bind_data->Cast<BindData>();
		idx_t num_dim = bind_data.dimension_names.size();
		idx_t count = 0;
		idx_t limit = MinValue<idx_t>(state.current_row + STANDARD_VECTOR_SIZE, state.rows.size());

		for (; state.current_row < limit; state.current_row++, count++) {
			auto &row = state.rows[state.current_row];
			for (idx_t d = 0; d < num_dim; d++) {
				output.data[d].SetValue(count, row.dimension_values[d]);
			}
			output.data[num_dim].SetValue(count, row.value);
		}
		output.SetCardinality(count);
	}

	static void Register(ExtensionLoader &loader) {

		TableFunction func("SISTAT_Read", {LogicalType::VARCHAR}, Execute, Bind, Init);
		func.named_parameters["language"] = LogicalType::VARCHAR;
		loader.RegisterFunction(func);
	}
};

} // namespace

void SistatDataFunctions::Register(ExtensionLoader &loader) {
	SISTAT_Read_Impl::Register(loader);
}

} // namespace duckdb
