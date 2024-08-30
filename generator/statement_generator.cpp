#include "statement_generator.h"
#include "sql_catalog.h"
#include "sql_types.h"

#include <algorithm>
#include <cstdint>
#include <sys/types.h>

namespace chfuzz {

int StatementGenerator::GenerateNextCreateTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::CreateTable *ct) {
	SQLTable next;
	const uint32_t tname = this->table_counter++;

	next.tname = tname;
	ct->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(next.tname));

	const uint32_t ncols = (rg.NextMediumNumber() % 5) + 1;
	sql_query_grammar::CreateTableDef *ctdef = ct->mutable_def();
	std::uniform_int_distribution<uint32_t> table_engine(1, sql_query_grammar::TableEngine::TableEngineValues_MAX);
	const uint32_t nopt = table_engine(rg.gen);
	sql_query_grammar::TableEngine *te = ct->mutable_engine();
	sql_query_grammar::TableEngine_TableEngineValues val = (sql_query_grammar::TableEngine_TableEngineValues) nopt;

	te->set_engine(val);
	next.teng = val;
	for (uint32_t i = 0 ; i < ncols ; i++) {
		SQLColumn col;
		const uint32_t cname = next.col_counter++;
		sql_query_grammar::ColumnDef *cd = i == 0 ? ctdef->mutable_col_def() : ctdef->add_other_col_defs();
		sql_query_grammar::TypeName *tn = cd->mutable_type();

		col.cname = cname;
		cd->mutable_col()->set_column("c" + std::to_string(cname));
		this->ids.push_back(cname);
		this->max_depth = 4;
		col.tp = RandomNextType(rg, true, true, next.col_counter, tn->mutable_type());
		this->max_depth = 10;

		next.cols[cname] = std::move(col);
	}
	if ((val >= sql_query_grammar::TableEngine_TableEngineValues::TableEngine_TableEngineValues_CollapsingMergeTree &&
		 val <= sql_query_grammar::TableEngine_TableEngineValues::TableEngine_TableEngineValues_VersionedCollapsingMergeTree)) {
		//params for engine
		const uint32_t limit = val == sql_query_grammar::TableEngine_TableEngineValues::TableEngine_TableEngineValues_VersionedCollapsingMergeTree ? 2 : 1;

		for (uint32_t i = 0 ; i < limit; i++) {
			SQLColumn col;
			const uint32_t cname = next.col_counter++;
			sql_query_grammar::ColumnDef *cd = ctdef->add_other_col_defs();
			sql_query_grammar::TypeName *tn = cd->mutable_type();

			col.cname = cname;
			cd->mutable_col()->set_column("c" + std::to_string(cname));
			this->ids.push_back(cname);
			col.tp = new IntType(8, i == 1);
			tn->mutable_type()->mutable_non_nullable()->set_integers(sql_query_grammar::Integers::Int32);

			te->add_cols()->set_column("c" + std::to_string(cname));
		}
	}

	if ((val >= sql_query_grammar::TableEngine_TableEngineValues::TableEngine_TableEngineValues_MergeTree &&
		 val <= sql_query_grammar::TableEngine_TableEngineValues::TableEngine_TableEngineValues_VersionedCollapsingMergeTree)) {
		sql_query_grammar::TableOrderBy *tob = ct->mutable_order();

		if (rg.NextSmallNumber() < 7) {
			const uint32_t ocols = (rg.NextMediumNumber() % std::min<uint32_t>(ids.size(), UINT32_C(3))) + 1;

			std::shuffle(ids.begin(), ids.end(), rg.gen);
			for (uint32_t i = 0; i < ocols ; i++) {
				tob->add_exprs()->mutable_comp_expr()->mutable_expr_stc()->mutable_col()->mutable_col()->set_column("c" + std::to_string(ids[i]));
			}
		}
	}

	ids.clear();
	this->staged_tables[tname] = std::move(next);
	return 0;
}

int StatementGenerator::GenerateNextDropTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::DropTable *dp) {
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

	dp->set_if_empty(rg.NextSmallNumber() < 4);
	dp->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	return 0;
}

int StatementGenerator::GenerateNextOptimizeTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::OptimizeTable *ot) {
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

	if (rg.NextSmallNumber() < 4) {
		sql_query_grammar::DeduplicateExpr *dde = ot->mutable_dedup();

		if (rg.NextSmallNumber() < 6) {
			sql_query_grammar::ExprColumnList *ecl = dde->mutable_col_list();
			const uint32_t ocols = (rg.NextMediumNumber() % std::min<uint32_t>(t.cols.size(), UINT32_C(4))) + 1;

			for (const auto &col : t.cols) {
				ids.push_back(col.first);
			}
			std::shuffle(ids.begin(), ids.end(), rg.gen);
			for (uint32_t i = 0; i < ocols; i++) {
				sql_query_grammar::ExprColumn *ec = i == 0 ? ecl->mutable_col() : ecl->add_extra_cols();

				ec->mutable_col()->set_column("c" + std::to_string(ids[i]));
			}
			ids.clear();
		}
	}
	ot->set_final(rg.NextSmallNumber() < 4);
	ot->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	return 0;
}

int StatementGenerator::GenerateNextCheckTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::CheckTable *ct) {
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

	ct->set_single_result(rg.NextSmallNumber() < 4);
	ct->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	return 0;
}

int StatementGenerator::GenerateNextDescTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::DescTable *dt) {
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

	dt->set_sub_cols(rg.NextSmallNumber() < 4);
	dt->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	return 0;
}

int StatementGenerator::GenerateNextInsert(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Insert *ins) {
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);
	const uint32_t nrows = rg.NextMediumNumber(), ncols = t.cols.size();
	std::string ret;
	uint32_t sign_col = 10000;

	switch (t.teng) {
		case sql_query_grammar::TableEngine_TableEngineValues::TableEngine_TableEngineValues_CollapsingMergeTree:
			sign_col = t.cols.size() - 1;
			break;
		default:
			break;
	}

	ins->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	for (const auto &entry : t.cols) {
		ins->add_cols()->set_column("c" + std::to_string(entry.second.cname));
	}
	this->max_depth = 4;
	for (uint32_t i = 0 ; i < nrows; i++) {
		uint32_t j = 0;

		if (i != 0) {
			ret += ", ";
		}
		ret += "(";
		for (const auto &entry : t.cols) {
			if (j != 0) {
				ret += ", ";
			}
			if (j == sign_col) {
				ret += rg.NextBool() ? "1" : "-1";
			} else {
				StrAppendAnyValue(rg, ret, entry.second.tp);
			}
			j++;
		}
		ret += ")";
	}
	this->max_depth = 10;
	ins->set_query(ret);
	return 0;
}

int StatementGenerator::GenerateNextDelete(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Delete *del) {
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

	del->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	del->mutable_where()->mutable_expr()->mutable_expr()->mutable_lit_val()->set_special_val(sql_query_grammar::SpecialVal::VAL_TRUE);
	return 0;
}

int StatementGenerator::GenerateNextTruncate(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Truncate *trunc) {
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

	trunc->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	return 0;
}

int StatementGenerator::GenerateNextExchangeTables(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ExchangeTables *et) {
	for (const auto &entries : this->tables) {
		this->ids.push_back(entries.first);
	}
	std::shuffle(this->ids.begin(), this->ids.end(), rg.gen);
	et->mutable_est1()->mutable_table_name()->set_table("t" + std::to_string(this->ids[0]));
	et->mutable_est2()->mutable_table_name()->set_table("t" + std::to_string(this->ids[1]));
	ids.clear();
	return 0;
}

int StatementGenerator::GenerateNextQuery(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::SQLQueryInner *sq) {
	const uint32_t create_table = 5 * (tables.size() < this->max_tables),
				   drop_table = 2 * (int)!tables.empty(),
				   insert = 30 * (int)!tables.empty(),
				   light_delete = 5 * (int)!tables.empty(),
				   truncate = 2 * (int)!tables.empty(),
				   optimize_table = 2 * (int)!tables.empty(),
				   check_table = 2 * (int)!tables.empty(),
				   desc_table = 2 * (int)!tables.empty(),
				   exchange_tables = 1 * (int)(tables.size() > 1),
				   select_query = 200,
				   prob_space = create_table + drop_table + insert + light_delete + truncate + optimize_table +
				   				check_table + desc_table + exchange_tables + select_query;

	assert(this->ids.empty());
	if (prob_space) {
		std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
		const uint32_t nopt = next_dist(rg.gen);

		if (create_table && nopt < create_table) {
			return GenerateNextCreateTable(cc, rg, sq->mutable_create_table());
		} else if (drop_table && nopt < (create_table + drop_table)) {
			return GenerateNextDropTable(cc, rg, sq->mutable_drop_table());
		} else if (insert && nopt < (create_table + drop_table + insert)) {
			return GenerateNextInsert(cc, rg, sq->mutable_insert());
		} else if (light_delete && nopt < (create_table + drop_table + insert + light_delete)) {
			return GenerateNextDelete(cc, rg, sq->mutable_del());
		} else if (truncate && nopt < (create_table + drop_table + insert + light_delete + truncate)) {
			return GenerateNextTruncate(cc, rg, sq->mutable_trunc());
		} else if (optimize_table && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table)) {
			return GenerateNextOptimizeTable(cc, rg, sq->mutable_opt());
		} else if (check_table && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table + check_table)) {
			return GenerateNextCheckTable(cc, rg, sq->mutable_check());
		} else if (desc_table && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table + check_table + desc_table)) {
			return GenerateNextDescTable(cc, rg, sq->mutable_desc());
		} else if (exchange_tables && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table + check_table + desc_table +
											  exchange_tables)) {
			return GenerateNextExchangeTables(cc, rg, sq->mutable_exchange());
		} else {
			return GenerateTopSelect(cc, rg, sq->mutable_select());
		}
	} else {
		return GenerateTopSelect(cc, rg, sq->mutable_select());
	}
}

int StatementGenerator::GenerateNextExplain(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ExplainQuery *eq) {

	if (rg.NextSmallNumber() < 10) {
		eq->set_expl((sql_query_grammar::ExplainQuery_ExplainValues) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::ExplainQuery::ExplainValues_MAX) + 1));
	}
	return GenerateNextQuery(cc, rg, eq->mutable_inner_query());
}

int StatementGenerator::GenerateNextStatement(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::SQLQuery &sq) {
	const uint32_t noption = rg.NextMediumNumber();

	if (noption < 15) {
		return GenerateNextExplain(cc, rg, sq.mutable_explain());
	} else {
		return GenerateNextQuery(cc, rg, sq.mutable_inner_query());
	}
}

void StatementGenerator::UpdateGenerator(const sql_query_grammar::SQLQuery &sq, const bool success) {
	const sql_query_grammar::SQLQueryInner &query = sq.has_inner_query() ? sq.inner_query() : sq.explain().inner_query();

	if (sq.has_inner_query() && query.has_create_table()) {
		const uint32_t tname = static_cast<uint32_t>(std::stoul(query.create_table().est().table_name().table().substr(1)));

		if (success) {
			this->tables[tname] = std::move(this->staged_tables[tname]);
		}
		this->staged_tables.erase(tname);
	} else if (sq.has_inner_query() && query.has_drop_table()) {
		const uint32_t tname = static_cast<uint32_t>(std::stoul(query.drop_table().est().table_name().table().substr(1)));

		if (success) {
			this->tables.erase(tname);
		}
	} else if (sq.has_inner_query() && query.has_exchange()) {
		const uint32_t tname1 = static_cast<uint32_t>(std::stoul(query.exchange().est1().table_name().table().substr(1))),
					   tname2 = static_cast<uint32_t>(std::stoul(query.exchange().est2().table_name().table().substr(1)));
		SQLTable tx = std::move(this->tables[tname1]), ty = std::move(this->tables[tname2]);

		tx.tname = tname2;
		ty.tname = tname1;
		this->tables[tname2] = std::move(tx);
		this->tables[tname1] = std::move(ty);
	}
}

}
