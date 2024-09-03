#pragma once

#include "sql_types.h"

namespace chfuzz {

class SQLColumn {
public:
	uint32_t cname;
	SQLType *tp;
};

class SQLTable {
public:
	uint32_t tname, col_counter = 0;
	sql_query_grammar::TableEngineValues teng;
	std::map<uint32_t, SQLColumn> cols, staged_cols;
	std::vector<uint32_t> engine_cols;
};

}
