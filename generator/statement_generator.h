#pragma once

#include "client_context.h"
#include "random_generator.h"
#include "sql_catalog.h"
#include "sql_grammar.pb.h"

namespace chfuzz {

class StatementGenerator {
private:
	uint32_t table_counter = 0;
	std::map<uint32_t, SQLTable> staged_tables, tables;

	std::vector<uint32_t> ids;
	int depth = 0, width = 0, max_depth = 10, max_width = 10, max_tables = 10;
public:

	SQLType* BottomType(RandomGenerator &rg, sql_query_grammar::BottomTypeName *tp);
	SQLType* RandomNextType(RandomGenerator &rg, const bool allow_nullable, uint32_t &col_counter, sql_query_grammar::TopTypeName *tp);

	int GenerateNextCreateTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::CreateTable *sq);
	int GenerateNextDropTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::DropTable *sq);
	int GenerateNextInsert(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Insert *sq);
	int GenerateNextDelete(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Delete *sq);
	int GenerateNextTruncate(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Truncate *sq);
	int GenerateNextOptimizeTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::OptimizeTable *sq);
	int GenerateNextCheckTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::CheckTable *sq);
	int GenerateNextDescTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::DescTable *sq);
	int GenerateNextExchangeTables(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ExchangeTables *sq);
	int GenerateTopSelect(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::TopSelect *sq);

	int GenerateNextExplain(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ExplainQuery *sq);
	int GenerateNextQuery(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::SQLQueryInner *sq);
	int GenerateNextStatement(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::SQLQuery &sq);

	void UpdateGenerator(const sql_query_grammar::SQLQuery &sq, const bool success);
};

}
