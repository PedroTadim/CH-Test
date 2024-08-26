#pragma once

#include <cstdint>
#include <string>
#include <optional>
#include <memory>
#include <vector>

#include "random_generator.h"
#include "sql_grammar.pb.h"

namespace chfuzz {

class SQLType {
public:
	virtual const std::string TypeName() = 0;
};

class BoolType : public SQLType {
public:
	const std::string TypeName() {
		return "Bool";
	};
};

class IntType : public SQLType {
public:
	const uint32_t size;
	const bool is_unsigned;
	IntType(const uint32_t s, const bool isu) : size(s), is_unsigned(isu) {}

	const std::string TypeName() {
		std::string ret;

		ret += is_unsigned ? "U" : "";
		ret += "Int";
		ret += std::to_string(size);
		return ret;
	};
};

class FloatType : public SQLType {
public:
	const uint32_t size;
	FloatType(const uint32_t s) : size(s) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Float";
		ret += std::to_string(size);
		return ret;
	};
};

class DateType : public SQLType {
public:
	const bool has_time, extended;
	DateType(const bool ht, const bool ex) : has_time(ht), extended(ex) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Date";
		ret += has_time ? "Time" : "";
		if (extended) {
			ret += has_time ? "64" : "32";
		}
		return ret;
	};
};

class DecimalType : public SQLType {
public:
	const std::optional<uint32_t> precision, scale;
	DecimalType(const std::optional<uint32_t> p, const std::optional<uint32_t> s) : precision(p), scale(s) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Decimal";
		if (precision.has_value()) {
			ret += "(";
			ret += precision.value();
			if (scale.has_value()) {
				ret += ",";
				ret += scale.value();
			}
			ret += ")";
		}
		return ret;
	};
};

class StringType : public SQLType {
public:
	std::optional<uint32_t> precision;
	StringType(const std::optional<uint32_t> p) : precision(p) {}

	const std::string TypeName() {
		std::string ret;

		if (precision.has_value()) {
			ret += "FixedString(";
			ret += precision.value();
			ret += ")";
		} else {
			ret += "String";
		}
		return ret;
	};
};

class DynamicType : public SQLType {
public:
	std::optional<uint32_t> ntypes;
	DynamicType(const std::optional<uint32_t> n) : ntypes(n) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Dynamic";
		if (ntypes.has_value()) {
			ret += "(max_types=";
			ret += ntypes.value();
			ret += ")";
		}
		return ret;
	};
};

class LowCardinality : public SQLType {
public:
	SQLType* subtype;
	LowCardinality(SQLType* s) : subtype(s) {}

	const std::string TypeName() {
		std::string ret;

		ret += "LowCardinality(";
		ret += subtype->TypeName();
		ret += ")";
		return ret;
	};
};


class MapType : public SQLType {
public:
	SQLType* key, *value;
	MapType(SQLType* k, SQLType* v) : key(k), value(v) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Map(";
		ret += key->TypeName();
		ret += ",";
		ret += value->TypeName();
		ret += ")";
		return ret;
	};
};

class ArrayType : public SQLType {
public:
	SQLType* subtype;
	ArrayType(SQLType* s) : subtype(s) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Array(";
		ret += subtype->TypeName();
		ret += ")";
		return ret;
	};
};

class SubType {
public:
	uint32_t cname;
	SQLType* subtype;

	SubType(uint32_t n, SQLType* s) : cname(n), subtype(s) {}
};

class TupleType : public SQLType {
public:
	std::vector<SubType> subtypes;
	TupleType(std::vector<SubType> s) : subtypes(s) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Tuple(";
		for (uint32_t i = 0 ; i < subtypes.size(); i++) {
			if (i != 0) {
				ret += ",";
			}
			ret += subtypes[i].cname;
			ret += " ";
			ret += subtypes[i].subtype->TypeName();
		}
		ret += ")";
		return ret;
	};
};

class NestedType : public SQLType {
	std::vector<SubType> subtypes;

public:
	NestedType(std::vector<SubType> s) : subtypes(s) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Nested(";
		for (uint32_t i = 0 ; i < subtypes.size(); i++) {
			if (i != 0) {
				ret += ",";
			}
			ret += subtypes[i].cname;
			ret += " ";
			ret += subtypes[i].subtype->TypeName();
		}
		ret += ")";
		return ret;
	};
};

class JSONType : public SQLType {
	std::vector<SubType> subtypes;

public:
	JSONType(std::vector<SubType> s) : subtypes(s) {}

	const std::string TypeName() {
		return "JSON";
	};
};

class Nullable : public SQLType {
public:
	SQLType* subtype;
	Nullable(SQLType* s) : subtype(s) {}

	const std::string TypeName() {
		std::string ret;

		ret += "Nullable(";
		ret += subtype->TypeName();
		ret += ")";
		return ret;
	};
};

SQLType* RandomIntType(RandomGenerator &rg, sql_query_grammar::Integers &res);
SQLType* RandomFloatType(RandomGenerator &rg, sql_query_grammar::Integers &res);
SQLType* RandomDateType(RandomGenerator &rg, sql_query_grammar::Dates &res);

}
