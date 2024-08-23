#pragma once

#include "sql_types.h"

#include <unordered_map>

namespace chfuzz {

class SQLColumn {
public:
	uint32_t cname;
	SQLType *tp;
};

class SQLTable {
public:
	uint32_t tname, col_counter = 0;
	std::unordered_map<uint32_t, SQLColumn> cols;
};

}
