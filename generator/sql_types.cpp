#include "sql_types.h"
#include "statement_generator.h"
#include <cstdint>

namespace chfuzz {

std::tuple<SQLType*, sql_query_grammar::Integers>
RandomIntType(RandomGenerator &rg) {
	std::uniform_int_distribution<uint32_t> next_dist(1, 12);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			return std::make_tuple(new IntType(8, true), sql_query_grammar::Integers::UInt8);
		case 2:
			return std::make_tuple(new IntType(16, true), sql_query_grammar::Integers::UInt16);
		case 3:
			return std::make_tuple(new IntType(32, true), sql_query_grammar::Integers::UInt32);
		case 4:
			return std::make_tuple(new IntType(64, true), sql_query_grammar::Integers::UInt64);
		case 5:
			return std::make_tuple(new IntType(128, true), sql_query_grammar::Integers::UInt128);
		case 6:
			return std::make_tuple(new IntType(256, true), sql_query_grammar::Integers::UInt256);
		case 7:
			return std::make_tuple(new IntType(8, false), sql_query_grammar::Integers::Int8);
		case 8:
			return std::make_tuple(new IntType(16, false), sql_query_grammar::Integers::Int16);
		case 9:
			return std::make_tuple(new IntType(32, false), sql_query_grammar::Integers::Int32);
		case 10:
			return std::make_tuple(new IntType(64, false), sql_query_grammar::Integers::Int64);
		case 11:
			return std::make_tuple(new IntType(128, false), sql_query_grammar::Integers::Int128);
		case 12:
			return std::make_tuple(new IntType(256, false), sql_query_grammar::Integers::Int256);
		default:
			assert(0);
	}
}

std::tuple<SQLType*, sql_query_grammar::FloatingPoints>
RandomFloatType(RandomGenerator &rg) {
	std::uniform_int_distribution<uint32_t> next_dist(1, 2);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			return std::make_tuple(new FloatType(32), sql_query_grammar::FloatingPoints::Float32);
		case 2:
			return std::make_tuple(new FloatType(64), sql_query_grammar::FloatingPoints::Float64);
		default:
			assert(0);
	}
}

std::tuple<SQLType*, sql_query_grammar::Dates>
RandomDateType(RandomGenerator &rg) {
	std::uniform_int_distribution<uint32_t> next_dist(1, 4);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			return std::make_tuple(new DateType(false, false), sql_query_grammar::Dates::Date);
		case 2:
			return std::make_tuple(new DateType(false, true), sql_query_grammar::Dates::Date32);
		case 3:
			return std::make_tuple(new DateType(true, false), sql_query_grammar::Dates::DateTime);
		case 4:
			return std::make_tuple(new DateType(true, true), sql_query_grammar::Dates::DateTime64);
		default:
			assert(0);
	}
}

SQLType* StatementGenerator::BottomType(RandomGenerator &rg, const bool allow_dynamic_types, sql_query_grammar::BottomTypeName *tp) {
	SQLType* res = nullptr;
	std::uniform_int_distribution<uint32_t> next_dist(1, allow_dynamic_types ? 10 : 7);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1: {
			//int
			sql_query_grammar::Integers nint;
			std::tie(res, nint) = RandomIntType(rg);
			if (tp) {
				tp->set_integers(nint);
			}
		} break;
		case 2: {
			//float
			sql_query_grammar::FloatingPoints nflo;
			std::tie(res, nflo) = RandomFloatType(rg);
			if (tp) {
				tp->set_floats(nflo);
			}
		} break;
		case 3: {
			//decimal
			sql_query_grammar::Decimal *dec = tp ? tp->mutable_decimal() : nullptr;
			std::optional<uint32_t> precision = std::nullopt, scale = std::nullopt;

			if (rg.NextBool()) {
				precision = std::optional<uint32_t>((rg.NextRandomUInt32() % 10) + 1);

				if (dec) {
					dec->set_precision(precision.value());
				}
				if (rg.NextBool()) {
					scale = std::optional<uint32_t>(rg.NextRandomUInt32() % precision.value());
				}
			}
			res = new DecimalType(precision, scale);
		} break;
		case 4: {
			//boolean
			if (tp) {
				tp->set_boolean(true);
			}
			res = new BoolType();
		} break;
		case 5: {
			//string
			std::optional<uint32_t> swidth = std::nullopt;

			if (rg.NextBool()) {
				if (tp) {
					tp->set_sql_string(true);
				}
			} else {
				swidth = std::optional<uint32_t>(rg.NextRandomUInt32() % 100);
				if (tp) {
					tp->set_fixed_string(swidth.value());
				}
			}
			res = new StringType(swidth);
		} break;
		case 6: {
			//dates
			sql_query_grammar::Dates dd;
			std::tie(res, dd) = RandomDateType(rg);
			if (tp) {
				tp->set_dates(dd);
			}
		} break;
		case 7: {
			//LowCardinality
			sql_query_grammar::LowCardinality *lcard = tp ? tp->mutable_lcard() : nullptr;
			std::uniform_int_distribution<uint32_t> next_dist(1, 4);
			const uint32_t nopt = next_dist(rg.gen);
			SQLType* sub = nullptr;

			switch (nopt) {
				case 1: {
					//int
					sql_query_grammar::Integers nint;
					std::tie(sub, nint) = RandomIntType(rg);
					if (lcard) {
						lcard->set_integers(nint);
					}
				} break;
				case 2: {
					//float
					sql_query_grammar::FloatingPoints nflo;
					std::tie(sub, nflo) = RandomFloatType(rg);
					if (lcard) {
						lcard->set_floats(nflo);
					}
				} break;
				case 3: {
					//dates
					sql_query_grammar::Dates dd;
					std::tie(sub, dd) = RandomDateType(rg);
					if (lcard) {
						lcard->set_dates(dd);
					}
				} break;
				default: {
					//string
					std::optional<uint32_t> swidth = std::nullopt;

					if (rg.NextBool()) {
						if (lcard) {
							lcard->set_sql_string(true);
						}
					} else {
						swidth = std::optional<uint32_t>(rg.NextRandomUInt32() % 100);
						if (lcard) {
							lcard->set_fixed_string(swidth.value());
						}
					}
					sub = new StringType(swidth);
				} break;
			}
			res = new LowCardinality(sub);
		} break;
		case 8:
		case 9:
			res = new JSONType({});
			if (tp) {
				tp->mutable_json();
			}
			break;
		case 10: {
			//dynamic
			sql_query_grammar::Dynamic *dyn = tp ? tp->mutable_dynamic() : nullptr;
			std::optional<uint32_t> ntypes = std::nullopt;

			if (rg.NextBool()) {
				ntypes = std::optional<uint32_t>(rg.NextRandomUInt32() % 100);
				if (dyn) {
					dyn->set_ntypes(ntypes.value());
				}
			}
			res = new DynamicType(ntypes);
		} break;
		default:
			assert(0);
	}
	return res;
}

SQLType* StatementGenerator::GenerateArraytype(RandomGenerator &rg, const bool allow_nullable, const bool allow_dynamic_types,
											   uint32_t &col_counter, sql_query_grammar::TopTypeName *tp) {
	this->depth++;
	SQLType* k = this->RandomNextType(rg, true, allow_dynamic_types, col_counter, tp);
	this->depth--;
	return new ArrayType(k);
}

SQLType* StatementGenerator::GenerateArraytype(RandomGenerator &rg, const bool allow_nullable, const bool allow_dynamic_types) {
	uint32_t col_counter = 0;

	return GenerateArraytype(rg, allow_nullable, allow_dynamic_types, col_counter, nullptr);
}

SQLType* StatementGenerator::RandomNextType(RandomGenerator &rg, const bool allow_nullable, const bool allow_dynamic_types,
											uint32_t &col_counter, sql_query_grammar::TopTypeName *tp) {
	const uint32_t noption = rg.NextMediumNumber();

	if (noption < 21 && allow_nullable) {
		//nullable
		return new Nullable(BottomType(rg, false, tp ? tp->mutable_nullable() : nullptr));
	} else if (noption < 71 || this->depth == this->max_depth) {
		//non nullable
		return BottomType(rg, allow_dynamic_types, tp ? tp->mutable_non_nullable() : nullptr);
	} else if (noption < 81 || this->max_width <= this->width + 1) {
		//array
		return GenerateArraytype(rg, true, allow_dynamic_types, col_counter, tp ? tp->mutable_array() : nullptr);
	} else if (noption < 91) {
		//map
		sql_query_grammar::MapType *mt = tp ? tp->mutable_map() : nullptr;

		this->depth++;
		SQLType* k = this->RandomNextType(rg, false, allow_dynamic_types, col_counter, mt ? mt->mutable_key() : nullptr);
		this->width++;
		SQLType* v = this->RandomNextType(rg, true, allow_dynamic_types, col_counter, mt ? mt->mutable_value() : nullptr);
		this->depth--;
		this->width--;
		return new MapType(k, v);
	} else {
		//tuple
		sql_query_grammar::TupleType *tt = tp ? tp->mutable_tuple() : nullptr;
		const uint32_t ncols = (rg.NextMediumNumber() % (std::min<uint32_t>(5, this->max_width - this->width))) + UINT32_C(2);
		std::vector<SubType> subtypes;

		this->depth++;
		for (uint32_t i = 0 ; i < ncols ; i++) {
			const uint32_t cname = col_counter++;
			sql_query_grammar::TypeColumnDef *tcd = tp ? (
				(i == 0) ? tt->mutable_value1() : ((i == 1) ? tt->mutable_value2() : tt->add_others())) : nullptr;

			if (tcd) {
				tcd->mutable_col()->set_column("c" + std::to_string(cname));
			}
			SQLType* k = this->RandomNextType(rg, true, allow_dynamic_types, col_counter, tcd ? tcd->mutable_type_name() : nullptr);
			subtypes.push_back(SubType(cname, k));
		}
		this->depth--;
		return new TupleType(subtypes);
	} /*else if (noption < 91) {
		//nested
	} else {
		//variant
	}*/
	return nullptr;
}

SQLType* StatementGenerator::RandomNextType(RandomGenerator &rg, const bool allow_nullable, const bool allow_dynamic_types) {
	uint32_t col_counter = 0;

	return RandomNextType(rg, allow_nullable, allow_dynamic_types, col_counter, nullptr);
}

void StatementGenerator::AppendDecimal(RandomGenerator &rg, std::string &ret, const uint32_t left, const uint32_t right) {
	ret += rg.NextBool() ? "-" : "";
	if (left > 0) {
		std::uniform_int_distribution<uint32_t> next_dist(1, left);
		const uint32_t nlen = next_dist(rg.gen);

		ret += std::max<char>(rg.NextDigit(), '1');
		for (uint32_t j = 1; j < nlen; j++) {
			ret += rg.NextDigit();
		}
	} else {
		ret += "0";
	}
	if (right > 0) {
		std::uniform_int_distribution<uint32_t> next_dist(1, right);
		const uint32_t nlen = next_dist(rg.gen);

		ret += ".";
		for (uint32_t j = 0; j < nlen; j++) {
			ret += rg.NextDigit();
		}
	}
}

void StatementGenerator::StrAppendBottomValue(RandomGenerator &rg, std::string &ret, SQLType* tp) {
	IntType *itp;
	DateType *dtp;
	DecimalType *detp;
	StringType *stp;

	if ((itp = dynamic_cast<IntType*>(tp))) {
		if (itp->is_unsigned) {
			switch (itp->size) {
				case 8:
					ret += std::to_string(rg.NextRandomUInt8());
					break;
				case 16:
					ret += std::to_string(rg.NextRandomUInt16());
					break;
				case 32:
					ret += std::to_string(rg.NextRandomUInt32());
					break;
				default:
					ret += std::to_string(rg.NextRandomUInt64());
					break;
			}
		} else {
			switch (itp->size) {
				case 8:
					ret += std::to_string(rg.NextRandomInt8());
					break;
				case 16:
					ret += std::to_string(rg.NextRandomInt16());
					break;
				case 32:
					ret += std::to_string(rg.NextRandomInt32());
					break;
				default:
					ret += std::to_string(rg.NextRandomInt64());
					break;
			}
		}
	} else if (dynamic_cast<FloatType*>(tp)) {
		ret += std::to_string(rg.NextRandomDouble());
	} else if ((dtp = dynamic_cast<DateType*>(tp))) {
		if (dtp->has_time) {
			ret += std::to_string(dtp->extended ? rg.NextDateTime64() : rg.NextDateTime());
		} else {
			ret += std::to_string(dtp->extended ? rg.NextDateTime64() : rg.NextDate32());
		}
	} else if ((detp = dynamic_cast<DecimalType*>(tp))) {
		const uint32_t right = detp->scale.value_or(0), left = detp->precision.value_or(10) - right;

		AppendDecimal(rg, ret, left, right);
	} else if ((stp = dynamic_cast<StringType*>(tp))) {
		const uint32_t limit = stp->precision.value_or(100);

		ret += "'";
		rg.NextString(ret, limit);
		ret += "'";
	} else if (dynamic_cast<BoolType*>(tp)) {
		ret += rg.NextBool() ? "TRUE" : "FALSE";
	} else {
		assert(0);
	}
}

void StatementGenerator::StrAppendMap(RandomGenerator &rg, std::string &ret, MapType *mt) {
	const uint32_t limit = (rg.NextSmallNumber() - 1);

	ret += "{";
	for (uint32_t i = 0 ; i < limit; i++) {
		if (i != 0) {
			ret += ", ";
		}
		StrAppendAnyValue(rg, ret, mt->key);
		ret += ":";
		StrAppendAnyValue(rg, ret, mt->value);
	}
	ret += "}";
}

void StatementGenerator::StrAppendArray(RandomGenerator &rg, std::string &ret, ArrayType *at) {
	const uint32_t limit = (rg.NextSmallNumber() - 1);

	ret += "[";
	for (uint32_t i = 0 ; i < limit; i++) {
		if (i != 0) {
			ret += ", ";
		}
		StrAppendAnyValue(rg, ret, at->subtype);
	}
	ret += "]";
}

void StatementGenerator::StrAppendTuple(RandomGenerator &rg, std::string &ret, TupleType *at) {
	ret += "(";
	for (uint32_t i = 0 ; i < at->subtypes.size(); i++) {
		if (i != 0) {
			ret += ", ";
		}
		StrAppendAnyValue(rg, ret, at->subtypes[i].subtype);
	}
	ret += ")";
}

void StatementGenerator::StrBuildJSONArray(RandomGenerator &rg, const int depth, const int width, std::string &ret) {
	std::uniform_int_distribution<int> jopt(1, 3);
	int nelems = 0, next_width = 0;

	if (width) {
		std::uniform_int_distribution<int> alen(1, width);
		nelems = alen(rg.gen);
	}
	ret += "[";
	next_width = nelems;
	for (int j = 0 ; j < nelems ; j++) {
		if (j != 0) {
			ret += ",";
		}
		if (depth) {
			const int noption = jopt(rg.gen);

			switch (noption) {
			case 1: //object
				StrBuildJSON(rg, depth - 1, next_width, ret);
				break;
			case 2: //array
				StrBuildJSONArray(rg, depth - 1, next_width, ret);
				break;
			case 3: //others
				StrBuildJSONElement(rg, ret);
				break;
			default:
				assert(0);
			}
		} else {
			StrBuildJSONElement(rg, ret);
		}
		next_width--;
	}
	ret += "]";
}

void StatementGenerator::StrBuildJSONElement(RandomGenerator &rg, std::string &ret) {
	std::uniform_int_distribution<int> opts(1, 10);
	const int noption = opts(rg.gen);

	switch (noption) {
		case 1:
			ret += "false";
		break;
		case 2:
			ret += "true";
		break;
		case 3:
			ret += "null";
		break;
		case 4:
		case 5:
		case 6:
		case 7: { //number
			std::uniform_int_distribution<int> numbers(-1000, 1000);
			ret += std::to_string(numbers(rg.gen));
		} break;
		case 8:
		case 9:
		case 10: { //string
			std::uniform_int_distribution<int> slen(0, 10);
			std::uniform_int_distribution<uint8_t> chars(32, 128);
			const int nlen = slen(rg.gen);

			ret += '"';
			for (int i = 0 ; i < nlen ; i++) {
				const uint8_t nchar = chars(rg.gen);

				switch (nchar) {
				case 128:
					ret += "😂";
					break;
				case static_cast<int>('"'):
					ret += "a";
					break;
				case static_cast<int>('\\'):
					ret += "b";
					break;
				case static_cast<int>('\''):
					ret += "''";
					break;
				default:
					ret += static_cast<char>(nchar);
				}
			}
			ret += '"';
		} break;
		default:
			assert(0);
	}
}

void StatementGenerator::StrBuildJSON(RandomGenerator &rg, const int depth, const int width, std::string &ret) {
	ret += "{";
	if (depth && width) {
		std::uniform_int_distribution<int> childd(1, width);
		const int nchildren = childd(rg.gen);

		for (int i = 0 ; i < nchildren ; i++) {
			std::uniform_int_distribution<int> jopt(1, 3);
			const int noption = jopt(rg.gen);

			if (i != 0) {
				ret += ",";
			}
			ret += "\"c";
			ret += rg.NextJsonCol();
			ret += "\":";
			switch (noption) {
			case 1: //object
				StrBuildJSON(rg, depth - 1, width, ret);
				break;
			case 2: //array
				StrBuildJSONArray(rg, depth - 1, width, ret);
				break;
			case 3: //others
				StrBuildJSONElement(rg, ret);
				break;
			default:
				assert(0);
			}
		}
	}
	ret += "}";
}

void StatementGenerator::StrAppendAnyValue(RandomGenerator &rg, std::string &ret, SQLType *tp) {
	MapType *mt;
	Nullable *nl;
	ArrayType *at;
	TupleType *ttp;
	LowCardinality *lc;

	if (dynamic_cast<IntType*>(tp) || dynamic_cast<FloatType*>(tp) || dynamic_cast<DateType*>(tp) ||
		dynamic_cast<DecimalType*>(tp) || dynamic_cast<StringType*>(tp) || dynamic_cast<BoolType*>(tp)) {
		StrAppendBottomValue(rg, ret, tp);
	} else if ((lc = dynamic_cast<LowCardinality*>(tp))) {
		StrAppendBottomValue(rg, ret, lc->subtype);
	} else if ((nl = dynamic_cast<Nullable*>(tp))) {
		if (rg.NextMediumNumber() < 6) {
			ret += "NULL";
		} else {
			StrAppendAnyValue(rg, ret, nl->subtype);
		}
	} else if (dynamic_cast<JSONType*>(tp)) {
		std::uniform_int_distribution<int> dopt(1, 3), wopt(1, 3);

		ret += "'";
		StrBuildJSON(rg, dopt(rg.gen), wopt(rg.gen), ret);
		ret += "'";
	} else if (dynamic_cast<DynamicType*>(tp)) {
		uint32_t col_counter = 0;
		SQLType *next = RandomNextType(rg, true, false, col_counter, nullptr);

		StrAppendAnyValue(rg, ret, next);
		delete next;
	} else if (this->depth == this->max_depth) {
		ret += "1";
	} else if ((mt = dynamic_cast<MapType*>(tp))) {
		this->depth++;
		StrAppendMap(rg, ret, mt);
		this->depth--;
	} else if ((at = dynamic_cast<ArrayType*>(tp))) {
		this->depth++;
		StrAppendArray(rg, ret, at);
		this->depth--;
	} else if ((ttp = dynamic_cast<TupleType*>(tp))) {
		this->depth++;
		StrAppendTuple(rg, ret, ttp);
		this->depth--;
	} else {
		assert(0);
	}
}

}
