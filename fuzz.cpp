#include <clickhouse/client.h>
#include <cstring>
#include "generator/statement_generator.h"
#include "ast/sql_query_proto_to_string.h"

static bool
RunQuery(std::string &ret, chfuzz::StatementGenerator &gen, chfuzz::ClientContext &cli, const sql_query_grammar::SQLQuery &sq) {
	ret.resize(0);
	sql_fuzzer::SQLQueryToString(ret, sq);
	const bool success = cli.LogQuery(ret);
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

	cli.LogQuery("SET allow_experimental_json_type = 1,"
				 "allow_experimental_inverted_index = 1,"
				 "allow_experimental_full_text_index = 1,"
				 "allow_experimental_codecs = 1,"
				 "allow_experimental_live_view = 1,"
				 "allow_experimental_window_view = 1,"
				 "allow_experimental_funnel_functions = 1,"
				 "allow_experimental_nlp_functions = 1,"
				 "allow_experimental_hash_functions = 1,"
				 "allow_experimental_object_type = 1,"
				 "allow_experimental_variant_type = 1,"
				 "allow_experimental_dynamic_type = 1,"
				 "allow_experimental_annoy_index = 1,"
				 "allow_experimental_usearch_index = 1,"
				 "allow_experimental_bigint_types = 1,"
				 "allow_experimental_window_functions = 1,"
				 "allow_experimental_geo_types = 1,"
				 "allow_experimental_map_type = 1,"
				 "allow_deprecated_error_prone_window_functions = 1,"
				 "allow_suspicious_low_cardinality_types = 1,"
				 "allow_suspicious_fixed_string_types = 1,"
				 "allow_suspicious_indices = 1,"
				 "allow_suspicious_codecs = 1,"
				 "allow_hyperscan = 1,"
				 "allow_simdjson = 1,"
				 "allow_deprecated_syntax_for_merge_tree = 1,"
				 "allow_suspicious_primary_key = 1,"
				 "allow_suspicious_ttl_expressions = 1,"
				 "allow_suspicious_variant_types = 1,"
				 "enable_deflate_qpl_codec = 1,"
				 "enable_zstd_qat_codec = 1,"
				 "allow_create_index_without_type = 1,"
				 "allow_experimental_s3queue = 1,"
				 "allow_experimental_analyzer = 1,"
				 "type_json_skip_duplicated_paths = 1,"
				 "allow_experimental_transactions = 1;");

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
