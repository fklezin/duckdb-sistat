#include "sistat_info_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"
#include "sistat.hpp"
#include "http_request.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

namespace {

struct SISTAT_Tables_Impl {

	struct BindData final : TableFunctionData {
		string list_url;
		string language;
		explicit BindData(string list_url_p, string language_p)
		    : list_url(std::move(list_url_p)), language(std::move(language_p)) {
		}
	};

	struct TableRow {
		string title;
		string table_id;
		string updated;
		string url;
	};

	struct State final : GlobalTableFunctionState {
		vector<TableRow> rows;
		idx_t current_row = 0;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		string lang = sistat::DEFAULT_LANGUAGE;
		auto it = input.named_parameters.find("language");
		if (it != input.named_parameters.end() && !it->second.IsNull() && it->second.type() == LogicalType::VARCHAR) {
			lang = it->second.GetValue<string>();
		}
		if (lang.empty()) {
			lang = sistat::DEFAULT_LANGUAGE;
		}

		string list_url =
		    string(sistat::BASE_URL) + lang + "/" + sistat::DATA_PATH;

		names.emplace_back("title");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("table_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("updated");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("url");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(list_url, lang);
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {

		auto &bind_data = input.bind_data->Cast<BindData>();
		auto state = make_uniq_base<GlobalTableFunctionState, State>();
		State *state_ptr = static_cast<State *>(state.get());

		HttpSettings settings = HttpRequest::ExtractHttpSettings(context, bind_data.list_url);
		HttpResponseData resp =
		    HttpRequest::ExecuteHttpRequest(settings, bind_data.list_url, "GET", {}, "", "");

		if (!resp.error.empty()) {
			throw IOException("SISTAT_Tables: %s", resp.error.c_str());
		}
		if (resp.status_code != 200) {
			throw IOException("SISTAT_Tables: HTTP %d - %s", resp.status_code, resp.body.c_str());
		}

		yyjson_doc *doc = yyjson_read(resp.body.c_str(), resp.body.size(), 0);
		if (!doc) {
			throw IOException("SISTAT_Tables: Invalid JSON from list endpoint");
		}

		yyjson_val *root = yyjson_doc_get_root(doc);
		if (!yyjson_is_arr(root)) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_Tables: Expected JSON array");
		}

		size_t n = yyjson_arr_size(root);
		state_ptr->rows.reserve(n);

		for (size_t i = 0; i < n; i++) {
			yyjson_val *obj = yyjson_arr_get(root, i);
			if (!yyjson_is_obj(obj)) {
				continue;
			}
			TableRow row;
			yyjson_val *v = yyjson_obj_get(obj, "text");
			if (yyjson_is_str(v)) {
				row.title = yyjson_get_str(v);
			}
			v = yyjson_obj_get(obj, "id");
			if (yyjson_is_str(v)) {
				row.table_id = yyjson_get_str(v);
			}
			v = yyjson_obj_get(obj, "updated");
			if (yyjson_is_str(v)) {
				row.updated = yyjson_get_str(v);
			}
			row.url = bind_data.list_url + row.table_id;
			state_ptr->rows.push_back(std::move(row));
		}

		yyjson_doc_free(doc);
		return std::move(state);
	}

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

		auto &state = input.global_state->Cast<State>();
		idx_t count = 0;
		idx_t limit = MinValue<idx_t>(state.current_row + STANDARD_VECTOR_SIZE, state.rows.size());

		for (; state.current_row < limit; state.current_row++, count++) {
			auto &row = state.rows[state.current_row];
			output.data[0].SetValue(count, row.title);
			output.data[1].SetValue(count, row.table_id);
			output.data[2].SetValue(count, row.updated);
			output.data[3].SetValue(count, row.url);
		}
		output.SetCardinality(count);
	}

	static void Register(ExtensionLoader &loader) {

		TableFunction func("SISTAT_Tables", {}, Execute, Bind, Init);
		func.named_parameters["language"] = LogicalType::VARCHAR;
        loader.RegisterFunction(func);
	}
};

struct SISTAT_DataStructure_Impl {

	struct BindData final : TableFunctionData {
		string table_id;
		string table_url;
		string language;
		BindData(string table_id_p, string table_url_p, string language_p)
		    : table_id(std::move(table_id_p)), table_url(std::move(table_url_p)),
		      language(std::move(language_p)) {
		}
	};

	struct VariableRow {
		string table_id;
		string variable_code;
		string variable_text;
		int64_t position;
		string value_codes_json;
		string value_texts_json;
	};

	struct State final : GlobalTableFunctionState {
		vector<VariableRow> rows;
		idx_t current_row = 0;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		if (input.inputs.empty()) {
			throw InvalidInputException("SISTAT_DataStructure: table_id is required.");
		}
		string table_id = StringValue::Get(input.inputs[0]);
		if (table_id.empty()) {
			throw InvalidInputException("SISTAT_DataStructure: table_id cannot be empty.");
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

		names.emplace_back("table_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("variable_code");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("variable_text");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("position");
		return_types.push_back(LogicalType::BIGINT);
		names.emplace_back("value_codes");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("value_texts");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(normalized_id, table_url, lang);
	}

	static string JsonToString(yyjson_val *val) {
		if (!val) {
			return "[]";
		}
		const char *json = yyjson_val_write(val, YYJSON_WRITE_NOFLAG, nullptr);
		if (!json) {
			return "[]";
		}
		string s(json);
		free((void *)json);
		return s;
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {

		auto &bind_data = input.bind_data->Cast<BindData>();
		auto state = make_uniq_base<GlobalTableFunctionState, State>();
		State *state_ptr = static_cast<State *>(state.get());

		HttpSettings settings = HttpRequest::ExtractHttpSettings(context, bind_data.table_url);
		HttpResponseData resp =
		    HttpRequest::ExecuteHttpRequest(settings, bind_data.table_url, "GET", {}, "", "");

		if (!resp.error.empty()) {
			throw IOException("SISTAT_DataStructure: %s", resp.error.c_str());
		}
		if (resp.status_code != 200) {
			throw IOException("SISTAT_DataStructure: HTTP %d - %s", resp.status_code, resp.body.c_str());
		}

		yyjson_doc *doc = yyjson_read(resp.body.c_str(), resp.body.size(), 0);
		if (!doc) {
			throw IOException("SISTAT_DataStructure: Invalid JSON");
		}

		yyjson_val *root = yyjson_doc_get_root(doc);
		yyjson_val *variables = yyjson_is_obj(root) ? yyjson_obj_get(root, "variables") : nullptr;
		if (!yyjson_is_arr(variables)) {
			yyjson_doc_free(doc);
			throw IOException("SISTAT_DataStructure: Expected object with 'variables' array");
		}

		size_t n = yyjson_arr_size(variables);
		state_ptr->rows.reserve(n);

		for (size_t pos = 0; pos < n; pos++) {
			yyjson_val *var_obj = yyjson_arr_get(variables, pos);
			if (!yyjson_is_obj(var_obj)) {
				continue;
			}
			VariableRow row;
			row.table_id = bind_data.table_id;
			row.position = static_cast<int64_t>(pos);

			yyjson_val *v = yyjson_obj_get(var_obj, "code");
			if (yyjson_is_str(v)) {
				row.variable_code = yyjson_get_str(v);
			}
			v = yyjson_obj_get(var_obj, "text");
			if (yyjson_is_str(v)) {
				row.variable_text = yyjson_get_str(v);
			}
			row.value_codes_json = JsonToString(yyjson_obj_get(var_obj, "values"));
			row.value_texts_json = JsonToString(yyjson_obj_get(var_obj, "valueTexts"));
			state_ptr->rows.push_back(std::move(row));
		}

		yyjson_doc_free(doc);
		return std::move(state);
	}

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

		auto &state = input.global_state->Cast<State>();
		idx_t count = 0;
		idx_t limit = MinValue<idx_t>(state.current_row + STANDARD_VECTOR_SIZE, state.rows.size());

		for (; state.current_row < limit; state.current_row++, count++) {
			auto &row = state.rows[state.current_row];
			output.data[0].SetValue(count, row.table_id);
			output.data[1].SetValue(count, row.variable_code);
			output.data[2].SetValue(count, row.variable_text);
			output.data[3].SetValue(count, Value::BIGINT(row.position));
			output.data[4].SetValue(count, row.value_codes_json);
			output.data[5].SetValue(count, row.value_texts_json);
		}
		output.SetCardinality(count);
	}

	static void Register(ExtensionLoader &loader) {

		TableFunction func("SISTAT_DataStructure", {LogicalType::VARCHAR}, Execute, Bind, Init);
		func.named_parameters["language"] = LogicalType::VARCHAR;
        loader.RegisterFunction(func);
	}
};

} // namespace

void SistatInfoFunctions::Register(ExtensionLoader &loader) {
	SISTAT_Tables_Impl::Register(loader);
	SISTAT_DataStructure_Impl::Register(loader);
}

} // namespace duckdb
