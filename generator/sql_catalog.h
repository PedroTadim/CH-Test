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
	std::map<uint32_t, SQLColumn> cols;
};

}
