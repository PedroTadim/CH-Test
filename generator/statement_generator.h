#pragma once

#include <cstdint>

#include "client_context.h"
#include "random_generator.h"
#include "sql_catalog.h"
#include "sql_grammar.pb.h"

namespace chfuzz {

class SQLRelationCol {
public:
	std::string rel_name;
	std::string name;
	SQLType* tp;

	SQLRelationCol() {}

	SQLRelationCol(const std::string rname, const std::string n, SQLType* sqltp) : rel_name(rname), name(n), tp(sqltp) {}
};

class SQLRelation {
public:
	std::string name;
	std::vector<SQLRelationCol> cols;

	SQLRelation() {}

	SQLRelation(const std::string n) : name(n) {}
};

class QueryLevel {
public:
	bool global_aggregate = false, inside_aggregate = false;
	uint32_t level, aliases_counter = 0;
	std::vector<SQLRelationCol> gcols;
	std::vector<SQLRelation> rels;

	QueryLevel() {}

	QueryLevel(const uint32_t n) : level(n) {}
};

class StatementGenerator {
private:
	uint32_t table_counter = 0, current_level = 0;
	std::map<uint32_t, SQLTable> staged_tables, tables;

	std::vector<uint32_t> ids;
	uint32_t depth = 0, width = 0, max_depth = 4, max_width = 4, max_tables = 10;

	std::map<uint32_t, QueryLevel> levels;

	void AppendDecimal(RandomGenerator &rg, std::string &ret, const uint32_t left, const uint32_t right);

	void StrAppendBottomValue(RandomGenerator &rg, std::string &ret, SQLType* tp);
	void StrAppendMap(RandomGenerator &rg, std::string &ret, MapType *mt);
	void StrAppendArray(RandomGenerator &rg, std::string &ret, ArrayType *at);
	void StrAppendTuple(RandomGenerator &rg, std::string &ret, TupleType *at);
	void StrAppendAnyValue(RandomGenerator &rg, std::string &ret, SQLType *tp);

	void StrBuildJSONArray(RandomGenerator &rg, const int depth, const int width, std::string &ret);
	void StrBuildJSONElement(RandomGenerator &rg, std::string &ret);
	void StrBuildJSON(RandomGenerator &rg, const int depth, const int width, std::string &ret);

	int GenerateNextCreateTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::CreateTable *sq);
	int GenerateNextDropTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::DropTable *sq);
	int GenerateNextInsert(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Insert *sq);
	int GenerateNextDelete(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Delete *sq);
	int GenerateNextTruncate(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Truncate *sq);
	int GenerateNextOptimizeTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::OptimizeTable *sq);
	int GenerateNextCheckTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::CheckTable *sq);
	int GenerateNextDescTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::DescTable *sq);
	int GenerateNextExchangeTables(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ExchangeTables *sq);

	int AddFieldAccess(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ComplicatedExpr *cexpr);
	int GenerateSubquery(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Select *sel);
	int GenerateColRef(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int GenerateLiteralValue(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int GeneratePredicate(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int GenerateExpression(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr);

	int GenerateOrderBy(ClientContext &cc, RandomGenerator &rg, const uint32_t ncols, sql_query_grammar::OrderByStatement *ob);
	int GenerateLimit(ClientContext &cc, RandomGenerator &rg, const bool has_order_by, const bool has_distinct, const uint32_t ncols, sql_query_grammar::LimitStatement *ls);
	int GenerateGroupBy(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::GroupByStatement *gb);
	int AddWhereFilter(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::BinaryExpr *bexpr);
	int GenerateWherePredicate(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int AddJoinClause(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::BinaryExpr *bexpr);
	int GenerateArrayJoin(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ArrayJoin *aj);
	int GenerateFromElement(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::TableOrSubquery *tos);
	int GenerateJoinConstraint(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::JoinConstraint *jc);
	int GenerateFromStatement(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::FromStatement *ft);
	int GenerateSelect(ClientContext &cc, RandomGenerator &rg, const bool top, const uint32_t ncols, sql_query_grammar::Select *sel);

	int GenerateTopSelect(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::TopSelect *sq);
	int GenerateNextExplain(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ExplainQuery *sq);
	int GenerateNextQuery(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::SQLQueryInner *sq);

	SQLType* BottomType(RandomGenerator &rg, const bool allow_dynamic_types, sql_query_grammar::BottomTypeName *tp);
	SQLType* GenerateArraytype(RandomGenerator &rg, const bool allow_nullable, const bool allow_dynamic_types);
	SQLType* GenerateArraytype(RandomGenerator &rg, const bool allow_nullable, const bool allow_dynamic_types, uint32_t &col_counter, sql_query_grammar::TopTypeName *tp);

	SQLType* RandomNextType(RandomGenerator &rg, const bool allow_nullable, const bool allow_dynamic_types);
	SQLType* RandomNextType(RandomGenerator &rg, const bool allow_nullable, const bool allow_dynamic_types, uint32_t &col_counter, sql_query_grammar::TopTypeName *tp);
public:

	int GenerateNextStatement(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::SQLQuery &sq);

	void UpdateGenerator(const sql_query_grammar::SQLQuery &sq, const bool success);
};

}
