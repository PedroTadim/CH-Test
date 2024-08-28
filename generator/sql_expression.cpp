#include "sql_types.h"
#include "statement_generator.h"

namespace chfuzz {

int StatementGenerator::GenerateLiteralValue(ClientContext &cc, RandomGenerator &rg, SQLType *tp, sql_query_grammar::Expr *expr) {
	return 0;
}

int StatementGenerator::GeneratePredicate(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr) {

	return 0;
}

int StatementGenerator::GenerateExpression(ClientContext &cc, RandomGenerator &rg, SQLType *tp, sql_query_grammar::Expr *expr) {

	return 0;
}

}
