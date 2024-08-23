#include "sql_types.h"
#include "statement_generator.h"
#include <cstdint>

namespace chfuzz {

SQLType* RandomIntType(RandomGenerator &rg, sql_query_grammar::Integers &res) {
	std::uniform_int_distribution<uint32_t> next_dist(1, 12);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			res = sql_query_grammar::Integers::UInt8;
			return new IntType(8, true);
		case 2:
			res = sql_query_grammar::Integers::UInt16;
			return new IntType(16, true);
		case 3:
			res = sql_query_grammar::Integers::UInt32;
			return new IntType(32, true);
		case 4:
			res = sql_query_grammar::Integers::UInt64;
			return new IntType(64, true);
		case 5:
			res = sql_query_grammar::Integers::UInt128;
			return new IntType(128, true);
		case 6:
			res = sql_query_grammar::Integers::UInt256;
			return new IntType(256, true);
		case 7:
			res = sql_query_grammar::Integers::Int8;
			return new IntType(8, false);
		case 8:
			res = sql_query_grammar::Integers::Int16;
			return new IntType(16, false);
		case 9:
			res = sql_query_grammar::Integers::Int32;
			return new IntType(32, false);
		case 10:
			res = sql_query_grammar::Integers::Int64;
			return new IntType(64, false);
		case 11:
			res = sql_query_grammar::Integers::Int128;
			return new IntType(128, false);
		default:
			res = sql_query_grammar::Integers::Int256;
			return new IntType(256, false);
	}
}

SQLType* RandomFloatType(RandomGenerator &rg, sql_query_grammar::FloatingPoints &res) {
	std::uniform_int_distribution<uint32_t> next_dist(1, 12);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			res = sql_query_grammar::FloatingPoints::Float32;
			return new FloatType(32);
		default:
			res = sql_query_grammar::FloatingPoints::Float64;
			return new FloatType(64);
	}
}

SQLType* RandomDateType(RandomGenerator &rg, sql_query_grammar::Dates &res) {
	std::uniform_int_distribution<uint32_t> next_dist(1, 12);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			res = sql_query_grammar::Dates::Date;
			return new DateType(false, false);
		case 2:
			res = sql_query_grammar::Dates::Date32;
			return new DateType(false, true);
		case 3:
			res = sql_query_grammar::Dates::DateTime;
			return new DateType(true, false);
		default:
			res = sql_query_grammar::Dates::DateTime64;
			return new DateType(true, true);
	}
}

SQLType* StatementGenerator::BottomType(RandomGenerator &rg, sql_query_grammar::BottomTypeName *tp) {
	SQLType* res = nullptr;
	std::uniform_int_distribution<uint32_t> next_dist(1, 10);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1: {
			//int
			sql_query_grammar::Integers nint;
			res = RandomIntType(rg, nint);
			tp->set_integers(nint);
		} break;
		case 2: {
			//float
			sql_query_grammar::FloatingPoints nflo;
			res = RandomFloatType(rg, nflo);
			tp->set_floats(nflo);
		} break;
		case 3: {
			//decimal
			sql_query_grammar::Decimal *dec = tp->mutable_decimal();
			std::optional<uint32_t> precision = std::nullopt, scale = std::nullopt;

			if (rg.NextBool()) {
				precision = std::optional<uint32_t>((rg.NextRandomUInt32() % 76) + 1);

				dec->set_precision(precision.value());
				if (rg.NextBool()) {
					scale = std::optional<uint32_t>(rg.NextRandomUInt32() % precision.value());
				}
			}
			res = new DecimalType(precision, scale);
		} break;
		case 4: {
			//boolean
			tp->set_boolean(true);
			res = new BoolType();
		} break;
		case 5: {
			//string
			std::optional<uint32_t> swidth = std::nullopt;

			if (rg.NextBool()) {
				tp->set_sql_string(true);
			} else {
				swidth = std::optional<uint32_t>(rg.NextRandomUInt32() % 100);
				tp->set_fixed_string(swidth.value());
			}
			res = new StringType(swidth);
		} break;
		case 6: {
			//dates
			sql_query_grammar::Dates dd;
			res = RandomDateType(rg, dd);
			tp->set_dates(dd);
		} break;
		case 7: {
			//dynamic
			sql_query_grammar::Dynamic *dyn = tp->mutable_dynamic();
			std::optional<uint32_t> ntypes = std::nullopt;

			if (rg.NextBool()) {
				ntypes = std::optional<uint32_t>(rg.NextRandomUInt32() % 100);
				dyn->set_ntypes(ntypes.value());
			}
			res = new DynamicType(ntypes);
		} break;
		case 8: {
			//LowCardinality
			sql_query_grammar::LowCardinality *lcard = tp->mutable_lcard();
			std::uniform_int_distribution<uint32_t> next_dist(1, 4);
			const uint32_t nopt = next_dist(rg.gen);
			SQLType* sub = nullptr;

			switch (nopt) {
				case 1: {
					//int
					sql_query_grammar::Integers nint;
					sub = RandomIntType(rg, nint);
					lcard->set_integers(nint);
				} break;
				case 2: {
					//float
					sql_query_grammar::FloatingPoints nflo;
					sub = RandomFloatType(rg, nflo);
					lcard->set_floats(nflo);
				} break;
				case 3: {
					//dates
					sql_query_grammar::Dates dd;
					sub = RandomDateType(rg, dd);
					lcard->set_dates(dd);
				} break;
				default: {
					//string
					std::optional<uint32_t> swidth = std::nullopt;

					if (rg.NextBool()) {
						lcard->set_sql_string(true);
					} else {
						swidth = std::optional<uint32_t>(rg.NextRandomUInt32() % 100);
						lcard->set_fixed_string(swidth.value());
					}
					sub = new StringType(swidth);
				} break;
			}
			res = new LowCardinality(sub);
		} break;
		default: {
			tp->mutable_json();
			res = new JSONType({});
		}
	}
	return res;
}

SQLType* StatementGenerator::RandomNextType(RandomGenerator &rg, const bool allow_nullable, uint32_t &col_counter, sql_query_grammar::TopTypeName *tp) {
	const uint32_t noption = rg.NextMediumNumber();

	if (noption < 21 && allow_nullable) {
		//nullable
		return BottomType(rg, tp->mutable_nullable());
	} else if (noption < 71 || this->depth == this->max_depth) {
		//non nullable
		return BottomType(rg, tp->mutable_non_nullable());
	} else if (noption < 81) {
		//array
		this->depth++;
		SQLType* k = this->RandomNextType(rg, true, col_counter, tp->mutable_array());
		this->depth--;
		return new ArrayType(k);
	} else if (noption < 91 || this->width <= (this->max_width - 1)) {
		//map
		sql_query_grammar::MapType *mt = tp->mutable_map();

		this->depth++;
		SQLType* k = this->RandomNextType(rg, false, col_counter, mt->mutable_key());
		SQLType* v = this->RandomNextType(rg, true, col_counter, mt->mutable_value());
		this->depth--;
		return new MapType(k, v);
	} else {
		//tuple
		sql_query_grammar::TupleType *tt = tp->mutable_tuple();
		const uint32_t ncols = rg.NextMediumNumber() % (std::min<uint32_t>(5, this->max_width - this->width));
		std::vector<SubType> subtypes;

		for (uint32_t i = 0 ; i < ncols ; i++) {
			const uint32_t cname = col_counter++;
			sql_query_grammar::TypeColumnDef *tcd =
				(i == 0) ? tt->mutable_value1() : ((i == 1) ? tt->mutable_value2() : tt->add_others());

			tcd->mutable_col()->set_column(cname);
			this->depth++;
			SQLType* k = this->RandomNextType(rg, true, col_counter, tcd->mutable_type_name());
			this->depth--;
			subtypes.push_back(SubType(cname, k));
		}
		return new TupleType(subtypes);
	} /*else if (noption < 91) {
		//nested
	} else {
		//variant
	}*/
	return nullptr;
}

}
