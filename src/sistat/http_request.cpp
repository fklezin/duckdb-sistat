#include "http_request.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/settings.hpp"

#include <chrono>
#include <thread>

namespace duckdb {

static constexpr idx_t DEFAULT_HTTP_MAX_CONCURRENT = 32;
static constexpr idx_t DEFAULT_HTTP_RETRY_COUNT = 4;
static constexpr idx_t DEFAULT_HTTP_RETRY_BACKOFF_MS = 250;

static void ParseUrl(const string &url, string &proto_host_port, string &path) {
	auto scheme_end = url.find("://");
	if (scheme_end == string::npos) {
		throw IOException("Invalid URL: missing scheme");
	}

	auto path_start = url.find('/', scheme_end + 3);
	if (path_start == string::npos) {
		proto_host_port = url;
		path = "/";
	} else {
		proto_host_port = url.substr(0, path_start);
		path = url.substr(path_start);
	}
}

static string NormalizeHeaderName(const string &name) {
	string result;
	result.reserve(name.size());
	bool capitalize_next = true;
	for (char c : name) {
		if (c == '-') {
			result += c;
			capitalize_next = true;
		} else if (capitalize_next) {
			result += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
			capitalize_next = false;
		} else {
			result += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
		}
	}
	return result;
}

static bool IsRetryableError(duckdb_httplib_openssl::Error error) {
	switch (error) {
	case duckdb_httplib_openssl::Error::Unknown:
	case duckdb_httplib_openssl::Error::Connection:
	case duckdb_httplib_openssl::Error::Read:
	case duckdb_httplib_openssl::Error::Write:
	case duckdb_httplib_openssl::Error::ConnectionTimeout:
	case duckdb_httplib_openssl::Error::ProxyConnection:
	case duckdb_httplib_openssl::Error::SSLConnection:
		return true;
	default:
		return false;
	}
}

static string FormatTransportError(const string &url, const string &method, duckdb_httplib_openssl::Error error,
                                   idx_t attempt, idx_t max_attempts) {
	return StringUtil::Format("HTTP transport error (%s) for %s %s [attempt %d/%d]",
	                          duckdb_httplib_openssl::to_string(error).c_str(), method.c_str(), url.c_str(),
	                          static_cast<int>(attempt), static_cast<int>(max_attempts));
}

HttpSettings HttpRequest::ExtractHttpSettings(ClientContext &context, const string &url) {

	HttpSettings settings;
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &config = db.config;

	settings.timeout = 30;
	settings.keep_alive = true;
	settings.max_concurrency = DEFAULT_HTTP_MAX_CONCURRENT;
	settings.use_cache = true;
	settings.follow_redirects = true;

	ClientContextFileOpener opener(context);
	FileOpenerInfo info;
	info.file_path = url;

	FileOpener::TryGetCurrentSetting(&opener, "http_timeout", settings.timeout, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_keep_alive", settings.keep_alive, &info);

	FileOpener::TryGetCurrentSetting(&opener, "http_proxy", settings.proxy, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_proxy_username", settings.proxy_username, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_proxy_password", settings.proxy_password, &info);

	string custom_user_agent;
	if (FileOpener::TryGetCurrentSetting(&opener, "http_user_agent", custom_user_agent, &info) &&
	    !custom_user_agent.empty()) {
		settings.user_agent = custom_user_agent;
	} else {
		settings.user_agent = StringUtil::Format("%s %s", config.UserAgent(), DuckDB::SourceID());
	}

	return settings;
}

HttpResponseData HttpRequest::ExecuteHttpRequest(const HttpSettings &settings, const string &url, const string &method,
                                                 const duckdb_httplib_openssl::Headers &headers,
                                                 const string &request_body, const string &content_type) {

	HttpResponseData result;
	result.status_code = 0;
	result.content_length = -1;

	try {
		string proto_host_port, path;
		ParseUrl(url, proto_host_port, path);
		idx_t max_attempts = DEFAULT_HTTP_RETRY_COUNT;
		string last_error;

		for (idx_t attempt = 1; attempt <= max_attempts; attempt++) {
			duckdb_httplib_openssl::Client client(proto_host_port);
			client.set_follow_location(settings.follow_redirects);
			client.set_decompress(false);
			client.enable_server_certificate_verification(false);

			auto timeout_sec = static_cast<time_t>(settings.timeout);
			client.set_read_timeout(timeout_sec, 0);
			client.set_write_timeout(timeout_sec, 0);
			client.set_connection_timeout(timeout_sec, 0);
			client.set_keep_alive(settings.keep_alive);

			if (!settings.proxy.empty()) {
				string proxy_host;
				idx_t proxy_port = 80;
				string proxy_copy = settings.proxy;
				HTTPUtil::ParseHTTPProxyHost(proxy_copy, proxy_host, proxy_port);
				client.set_proxy(proxy_host, static_cast<int>(proxy_port));
				if (!settings.proxy_username.empty()) {
					client.set_proxy_basic_auth(settings.proxy_username, settings.proxy_password);
				}
			}

			duckdb_httplib_openssl::Headers req_headers = headers;
			if (req_headers.find("User-Agent") == req_headers.end()) {
				req_headers.insert({"User-Agent", settings.user_agent});
			}

			duckdb_httplib_openssl::Result res(nullptr, duckdb_httplib_openssl::Error::Unknown);

			if (StringUtil::CIEquals(method, "HEAD")) {
				res = client.Head(path, req_headers);
			} else if (StringUtil::CIEquals(method, "DELETE")) {
				res = client.Delete(path, req_headers);
			} else if (StringUtil::CIEquals(method, "POST")) {
				string ct = content_type.empty() ? "application/octet-stream" : content_type;
				res = client.Post(path, req_headers, request_body, ct);
			} else if (StringUtil::CIEquals(method, "PUT")) {
				string ct = content_type.empty() ? "application/octet-stream" : content_type;
				res = client.Put(path, req_headers, request_body, ct);
			} else if (StringUtil::CIEquals(method, "PATCH")) {
				string ct = content_type.empty() ? "application/octet-stream" : content_type;
				res = client.Patch(path, req_headers, request_body, ct);
			} else {
				res = client.Get(path, req_headers);
			}

			if (res.error() != duckdb_httplib_openssl::Error::Success) {
				last_error = FormatTransportError(url, method, res.error(), attempt, max_attempts);
				if (attempt < max_attempts && IsRetryableError(res.error())) {
					std::this_thread::sleep_for(
					    std::chrono::milliseconds(DEFAULT_HTTP_RETRY_BACKOFF_MS * static_cast<int64_t>(attempt)));
					continue;
				}
				result.error = last_error;
				return result;
			}

			result.status_code = res->status;
			string response_body = res->body;

			for (auto &header : res->headers) {
				string normalized_key = NormalizeHeaderName(header.first);
				if (StringUtil::CIEquals(header.first, "Content-Type")) {
					result.content_type = header.second;
				} else if (StringUtil::CIEquals(header.first, "Content-Length")) {
					try {
						result.content_length = std::stoll(header.second);
					} catch (...) {
					}
				}
				bool found = false;
				for (idx_t i = 0; i < result.header_keys.size(); i++) {
					if (StringUtil::CIEquals(result.header_keys[i].GetValue<string>(), normalized_key)) {
						result.header_values[i] = Value(header.second);
						found = true;
						break;
					}
				}
				if (!found) {
					result.header_keys.push_back(Value(normalized_key));
					result.header_values.push_back(Value(header.second));
				}
			}

			result.body = response_body;
			try {
				if (GZipFileSystem::CheckIsZip(response_body.data(), response_body.size())) {
					result.body = GZipFileSystem::UncompressGZIPString(response_body);
				}
			} catch (...) {
			}
			return result;
		}

		result.error = last_error.empty() ? StringUtil::Format("HTTP request failed for %s %s", method.c_str(), url.c_str())
		                                  : last_error;

	} catch (std::exception &e) {
		result.error = e.what();
	}

	return result;
}

} // namespace duckdb
