#include <clickhouse/client.h>
#include <cstring>
#include <exception>
#include "generator/statement_generator.h"
#include "ast/sql_query_proto_to_string.h"

#include <iostream>

static bool
RunQuery(std::string &ret, chfuzz::StatementGenerator &gen, chfuzz::ClientContext &cli, const sql_query_grammar::SQLQuery &sq) {
	bool success = false;

	ret.resize(0);
	sql_fuzzer::SQLQueryToString(ret, sq);
	try {
		std::cout << "Running query: " << ret << std::endl;

		cli.LogQuery(ret);
		success = true;
	} catch (const std::exception & e) {
		std::cout << "Got exception " << e.what() << std::endl;
		if (std::strstr(e.what(), "Broken pipe")) {
			std::cerr << "Server crashed, exiting" << std::endl;
			std::exit(1);
		}
	}
	gen.UpdateGenerator(sq, success);
	return success;
}

int main() {
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	chfuzz::ClientContext cli(clickhouse::ClientOptions().SetHost("localhost"));
	chfuzz::RandomGenerator rg(0);
	chfuzz::StatementGenerator gen;
	sql_query_grammar::SQLQuery sq;
	std::string ret;
	int nsuccessfull = 0;

	cli.LogQuery("DROP DATABASE IF EXISTS s0;");
	cli.LogQuery("CREATE DATABASE s0;");
	cli.LogQuery("USE s0;");

	cli.LogQuery("SET allow_experimental_json_type = 1;");
	cli.LogQuery("SET allow_experimental_inverted_index = 1;");
	cli.LogQuery("SET allow_experimental_full_text_index = 1;");
	cli.LogQuery("SET allow_experimental_codecs = 1;");
	cli.LogQuery("SET allow_experimental_live_view = 1;");
	cli.LogQuery("SET allow_experimental_window_view = 1;");
	cli.LogQuery("SET allow_experimental_funnel_functions = 1;");
	cli.LogQuery("SET allow_experimental_nlp_functions = 1;");
	cli.LogQuery("SET allow_experimental_hash_functions = 1;");
	cli.LogQuery("SET allow_experimental_object_type = 1;");
	cli.LogQuery("SET allow_experimental_variant_type = 1;");
	cli.LogQuery("SET allow_experimental_dynamic_type = 1;");
	cli.LogQuery("SET allow_experimental_annoy_index = 1;");
	cli.LogQuery("SET allow_experimental_usearch_index = 1;");
	cli.LogQuery("SET allow_experimental_bigint_types = 1;");
	cli.LogQuery("SET allow_experimental_window_functions = 1;");
	cli.LogQuery("SET allow_experimental_geo_types = 1;");
	cli.LogQuery("SET allow_experimental_map_type = 1;");
	cli.LogQuery("SET allow_deprecated_error_prone_window_functions = 1;");
	cli.LogQuery("SET allow_suspicious_low_cardinality_types = 1;");
	cli.LogQuery("SET allow_suspicious_fixed_string_types = 1;");
	cli.LogQuery("SET allow_suspicious_indices = 1;");
	cli.LogQuery("SET allow_suspicious_codecs = 1;");
	cli.LogQuery("SET allow_hyperscan = 1;");
	cli.LogQuery("SET allow_simdjson = 1;");
	cli.LogQuery("SET allow_deprecated_syntax_for_merge_tree = 1;");
	cli.LogQuery("SET allow_suspicious_primary_key = 1;");
	cli.LogQuery("SET allow_suspicious_ttl_expressions = 1;");
	cli.LogQuery("SET allow_suspicious_variant_types = 1;");
	cli.LogQuery("SET enable_deflate_qpl_codec = 1;");
	cli.LogQuery("SET enable_zstd_qat_codec = 1;");
	cli.LogQuery("SET allow_create_index_without_type = 1;");
	cli.LogQuery("SET allow_experimental_s3queue = 1;");
	cli.LogQuery("SET allow_experimental_analyzer = 1;");
	cli.LogQuery("SET type_json_skip_duplicated_paths = 1;");

	ret.reserve(4096);
	for (int i = 0 ; i < 30 && nsuccessfull < 5; i++) {
		sq.Clear();
		gen.GenerateNextCreateTable(cli, rg, sq.mutable_inner_query()->mutable_create_table());
		nsuccessfull += (RunQuery(ret, gen, cli, sq) ? 1 : 0);
	}
	while (true) {
		sq.Clear();
		(void) gen.GenerateNextStatement(cli, rg, sq);
		RunQuery(ret, gen, cli, sq);
	}
	return 0;
}
