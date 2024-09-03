#include "statement_generator.h"
#include "sql_catalog.h"
#include "sql_types.h"

#include <algorithm>
#include <cstdint>
#include <sys/types.h>

namespace chfuzz {

int StatementGenerator::GenerateTableOrderBy(ClientContext &cc, RandomGenerator &rg, const SQLTable &t, sql_query_grammar::TableOrderBy *tob) {
	if (rg.NextSmallNumber() < 7) {
		for (const auto &col : t.cols) {
			ids.push_back(col.first);
		}
		std::shuffle(ids.begin(), ids.end(), rg.gen);
		const uint32_t ocols = (rg.NextMediumNumber() % std::min<uint32_t>(ids.size(), UINT32_C(3))) + 1;

		std::shuffle(ids.begin(), ids.end(), rg.gen);
		for (uint32_t i = 0; i < ocols ; i++) {
			tob->add_exprs()->mutable_comp_expr()->mutable_expr_stc()->mutable_col()->mutable_col()->set_column("c" + std::to_string(ids[i]));
		}
		ids.clear();
	}
	return 0;
}

const std::vector<sql_query_grammar::TableEngineValues> like_engs = {
  sql_query_grammar::TableEngineValues::Memory,
  sql_query_grammar::TableEngineValues::MergeTree,
  sql_query_grammar::TableEngineValues::ReplacingMergeTree,
  sql_query_grammar::TableEngineValues::SummingMergeTree,
  sql_query_grammar::TableEngineValues::AggregatingMergeTree,
  sql_query_grammar::TableEngineValues::StripeLog,
  sql_query_grammar::TableEngineValues::Log,
  sql_query_grammar::TableEngineValues::TinyLog};

int StatementGenerator::GenerateNextCreateTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::CreateTable *ct) {
	SQLTable next;
	const uint32_t tname = this->table_counter++;

	next.tname = tname;
	ct->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(next.tname));

	if (tables.empty() || rg.NextSmallNumber() < 9) {
		sql_query_grammar::CreateTableDef *ctdef = ct->mutable_table_def();
		sql_query_grammar::ColumnsDef *colsdef = ctdef->mutable_def();
		sql_query_grammar::TableEngine *te = ctdef->mutable_engine();
		std::uniform_int_distribution<uint32_t> table_engine(1, sql_query_grammar::TableEngineValues_MAX);
		const uint32_t ncols = (rg.NextMediumNumber() % 5) + 1, nopt = table_engine(rg.gen);
		sql_query_grammar::TableEngineValues val = (sql_query_grammar::TableEngineValues) nopt;

		te->set_engine(val);
		next.teng = val;
		for (uint32_t i = 0 ; i < ncols ; i++) {
			SQLColumn col;
			const uint32_t cname = next.col_counter++;
			sql_query_grammar::ColumnDef *cd = i == 0 ? colsdef->mutable_col_def() : colsdef->add_other_col_defs();
			sql_query_grammar::TypeName *tn = cd->mutable_type();

			col.cname = cname;
			cd->mutable_col()->set_column("c" + std::to_string(cname));
			this->max_depth = 4;
			col.tp = RandomNextType(rg, true, true, next.col_counter, tn->mutable_type());
			this->max_depth = 10;

			next.cols[cname] = std::move(col);
		}
		if ((next.teng >= sql_query_grammar::TableEngineValues::CollapsingMergeTree &&
			 next.teng <= sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree)) {
			//params for engine
			const uint32_t limit = next.teng == sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree ? 2 : 1;

			for (uint32_t i = 0 ; i < limit; i++) {
				SQLColumn col;
				const uint32_t cname = next.col_counter++;
				sql_query_grammar::ColumnDef *cd = colsdef->add_other_col_defs();
				sql_query_grammar::TypeName *tn = cd->mutable_type();

				col.cname = cname;
				cd->mutable_col()->set_column("c" + std::to_string(cname));
				col.tp = new IntType(8, i == 1);
				tn->mutable_type()->mutable_non_nullable()->set_integers(i == 1 ? sql_query_grammar::Integers::UInt8 : sql_query_grammar::Integers::Int8);

				te->add_cols()->set_column("c" + std::to_string(cname));

				next.engine_cols.push_back(cname);
			}
		}
		if (rg.NextSmallNumber() < 5) {
			GenerateSelect(cc, rg, true, next.cols.size(), ct->mutable_as_select_stmt());
		}
	} else {
		sql_query_grammar::TableLike *tl = ct->mutable_table_like();
		const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

		tl->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
		for (const auto &col : t.cols) {
			next.cols[col.first] = col.second;
		}
		next.col_counter = t.col_counter;
		if (rg.NextSmallNumber() < 6) {
			sql_query_grammar::TableEngine *te = tl->mutable_engine();

			next.teng = rg.PickRandomlyFromVector(like_engs);
			te->set_engine(next.teng);
		} else {
			next.teng = t.teng;
		}
	}
	if ((next.teng >= sql_query_grammar::TableEngineValues::MergeTree &&
		 next.teng <= sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree)) {
		GenerateTableOrderBy(cc, rg, next, ct->mutable_order());
	}

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
	ot->set_final(t.teng != sql_query_grammar::TableEngineValues::Memory &&
				  t.teng != sql_query_grammar::TableEngineValues::MergeTree &&
				  rg.NextSmallNumber() < 4);
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

	ins->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	for (const auto &entry : t.cols) {
		ins->add_cols()->set_column("c" + std::to_string(entry.second.cname));
	}

	if (rg.NextSmallNumber() < 10) {
		const uint32_t nrows = rg.NextMediumNumber();
		std::string ret;
		uint32_t sign_col = 10000;

		switch (t.teng) {
			case sql_query_grammar::TableEngineValues::CollapsingMergeTree:
				sign_col = t.cols.size() - 1;
				break;
			default:
				break;
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
	} else {
		GenerateSelect(cc, rg, true, t.cols.size(), ins->mutable_select());
	}
	return 0;
}

int StatementGenerator::GenerateUptDelWhere(ClientContext &cc, RandomGenerator &rg, const SQLTable &t, sql_query_grammar::Expr *expr) {
	if (rg.NextSmallNumber() < 8) {
		SQLRelation rel("");

		assert(this->current_level == 0);
		for (const auto &entry : t.cols) {
			rel.cols.push_back(SQLRelationCol("", "c" + std::to_string(entry.first), entry.second.tp));
		}
		this->levels[this->current_level].rels.push_back(std::move(rel));
		GenerateWherePredicate(cc, rg, expr);
		this->levels.clear();
	} else {
		expr->mutable_lit_val()->set_special_val(sql_query_grammar::SpecialVal::VAL_TRUE);
	}
	return 0;
}

int StatementGenerator::GenerateNextDelete(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Delete *del) {
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

	del->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	GenerateUptDelWhere(cc, rg, t, del->mutable_where()->mutable_expr()->mutable_expr());
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

int StatementGenerator::GenerateAlterTable(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::AlterTable *at) {
	SQLTable &t = const_cast<SQLTable &>(rg.PickValueRandomlyFromMap(this->tables));
	const std::string tname = "t" + std::to_string(t.tname);
	const uint32_t alter_order_by = 3 * (t.teng >= sql_query_grammar::TableEngineValues::MergeTree &&
		 								 t.teng <= sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree),
				   heavy_delete = 20,
				   heavy_update = 20,
				   add_column = 2 * (t.cols.size() < 10),
				   materialize_column = 2,
				   drop_column = 2 * (t.cols.size() > 1),
				   rename_column = 2,
				   modify_column = 2,
				   prob_space = alter_order_by + heavy_delete + heavy_update + add_column + materialize_column + drop_column + rename_column + modify_column;
	std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
	const uint32_t nopt = next_dist(rg.gen);

	if (alter_order_by && nopt < (alter_order_by + 1)) {
		GenerateTableOrderBy(cc, rg, t, at->mutable_order());
	} else if (heavy_delete && nopt < (heavy_delete + alter_order_by + 1)) {
		GenerateUptDelWhere(cc, rg, t, at->mutable_del()->mutable_expr()->mutable_expr());
	} else if (add_column && nopt < (heavy_delete + alter_order_by + add_column + 1)) {
		SQLColumn col;
		const uint32_t cname = t.col_counter++, next_option = rg.NextSmallNumber();
		sql_query_grammar::AddColumn *add_col = at->mutable_add_column();
		sql_query_grammar::ColumnDef *cd = add_col->mutable_new_col();
		sql_query_grammar::TypeName *tn = cd->mutable_type();

		col.cname = cname;
		cd->mutable_col()->set_column("c" + std::to_string(cname));
		this->max_depth = 4;
		col.tp = RandomNextType(rg, true, true, t.col_counter, tn->mutable_type());
		this->max_depth = 10;

		if (next_option < 4) {
			const SQLColumn &col = rg.PickValueRandomlyFromMap(t.cols);
			add_col->mutable_after()->set_column("c" + std::to_string(col.cname));
		} else if (next_option < 8) {
			add_col->set_first(true);
		}
		t.staged_cols[cname] = std::move(col);
	} else if (materialize_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + 1)) {
		const SQLColumn &col = rg.PickValueRandomlyFromMap(t.cols);

		at->mutable_materialize_column()->set_column("c" + std::to_string(col.cname));
	} else if (drop_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + 1)) {
		const SQLColumn &col = rg.PickValueRandomlyFromMap(t.cols);

		at->mutable_drop_column()->set_column("c" + std::to_string(col.cname));
	} else if (rename_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + 1)) {
		const uint32_t ncname = t.col_counter++;
		const SQLColumn &col = rg.PickValueRandomlyFromMap(t.cols);
		sql_query_grammar::RenameCol *rcol = at->mutable_rename_column();

		rcol->mutable_old_name()->set_column("c" + std::to_string(col.cname));
		rcol->mutable_new_name()->set_column("c" + std::to_string(ncname));
	} else if (modify_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + modify_column + 1)) {
		SQLColumn ncol;
		const SQLColumn &ocol = rg.PickValueRandomlyFromMap(t.cols);
		const uint32_t next_option = rg.NextSmallNumber();
		sql_query_grammar::AddColumn *add_col = at->mutable_modify_column();
		sql_query_grammar::ColumnDef *cd = add_col->mutable_new_col();
		sql_query_grammar::TypeName *tn = cd->mutable_type();

		ncol.cname = ocol.cname;
		cd->mutable_col()->set_column("c" + std::to_string(ocol.cname));
		this->max_depth = 4;
		ncol.tp = RandomNextType(rg, true, true, t.col_counter, tn->mutable_type());
		this->max_depth = 10;

		if (next_option < 4) {
			const SQLColumn &col = rg.PickValueRandomlyFromMap(t.cols);
			add_col->mutable_after()->set_column("c" + std::to_string(col.cname));
		} else if (next_option < 8) {
			add_col->set_first(true);
		}
		t.staged_cols[ncol.cname] = std::move(ncol);
	} else {
		SQLRelation rel("");
		sql_query_grammar::Update *upt = at->mutable_update();

		for (const auto &col : t.cols) {
			this->ids.push_back(col.first);
		}
		std::shuffle(this->ids.begin(), this->ids.end(), rg.gen);

		assert(this->current_level == 0);
		for (const auto &entry : t.cols) {
			rel.cols.push_back(SQLRelationCol("", "c" + std::to_string(entry.first), entry.second.tp));
		}
		this->levels[this->current_level].rels.push_back(std::move(rel));

		const uint32_t nupdates = (rg.NextMediumNumber() % std::min<uint32_t>(this->ids.size(), UINT32_C(4))) + 1;
		for (uint32_t i = 0 ; i < nupdates; i++) {
			const SQLColumn &col = t.cols.at(this->ids[i]);
			sql_query_grammar::UpdateSet *uset = i == 0 ? upt->mutable_update() : upt->add_other_updates();

			uset->mutable_col()->set_column("c" + std::to_string(col.cname));
			GenerateExpression(cc, rg, uset->mutable_expr());
		}
		this->ids.clear();
		this->levels.clear();
		GenerateUptDelWhere(cc, rg, t, upt->mutable_where()->mutable_expr()->mutable_expr());
	}
	at->mutable_est()->mutable_table_name()->set_table(tname);
	return 0;
}

int StatementGenerator::GenerateNextQuery(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::SQLQueryInner *sq) {
	const uint32_t create_table = 4 * (tables.size() < this->max_tables),
				   drop_table = 2 * (int)!tables.empty(),
				   insert = 30 * (int)!tables.empty(),
				   light_delete = 5 * (int)!tables.empty(),
				   truncate = 2 * (int)!tables.empty(),
				   optimize_table = 2 * (int)!tables.empty(),
				   check_table = 2 * (int)!tables.empty(),
				   desc_table = 2 * (int)!tables.empty(),
				   exchange_tables = 1 * (int)(tables.size() > 1),
				   alter_table = 5 * (int)!tables.empty(),
				   select_query = 300,
				   prob_space = create_table + drop_table + insert + light_delete + truncate + optimize_table +
				   				check_table + desc_table + exchange_tables + alter_table + select_query;

	assert(this->ids.empty());
	if (prob_space) {
		std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
		const uint32_t nopt = next_dist(rg.gen);

		if (create_table && nopt < (create_table + 1)) {
			return GenerateNextCreateTable(cc, rg, sq->mutable_create_table());
		} else if (drop_table && nopt < (create_table + drop_table + 1)) {
			return GenerateNextDropTable(cc, rg, sq->mutable_drop_table());
		} else if (insert && nopt < (create_table + drop_table + insert + 1)) {
			return GenerateNextInsert(cc, rg, sq->mutable_insert());
		} else if (light_delete && nopt < (create_table + drop_table + insert + light_delete + 1)) {
			return GenerateNextDelete(cc, rg, sq->mutable_del());
		} else if (truncate && nopt < (create_table + drop_table + insert + light_delete + truncate + 1)) {
			return GenerateNextTruncate(cc, rg, sq->mutable_trunc());
		} else if (optimize_table && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table + 1)) {
			return GenerateNextOptimizeTable(cc, rg, sq->mutable_opt());
		} else if (check_table && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table + check_table + 1)) {
			return GenerateNextCheckTable(cc, rg, sq->mutable_check());
		} else if (desc_table && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table + check_table + desc_table + 1)) {
			return GenerateNextDescTable(cc, rg, sq->mutable_desc());
		} else if (exchange_tables && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table + check_table + desc_table +
											  exchange_tables + 1)) {
			return GenerateNextExchangeTables(cc, rg, sq->mutable_exchange());
		} else if (alter_table && nopt < (create_table + drop_table + insert + light_delete + truncate + optimize_table + check_table + desc_table +
										  exchange_tables + alter_table + 1)) {
			return GenerateAlterTable(cc, rg, sq->mutable_alter_table());
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

	if (noption < 11) {
		return GenerateNextExplain(cc, rg, sq.mutable_explain());
	} /*else if (this->in_transaction && noption < 41) {
		if (rg.NextBool()) {
			sq.set_commit_trans(true);
		} else {
			sq.set_rollback_trans(true);
		}
		this->in_transaction = false;
		return 0;
	} else if (!this->in_transaction && noption < 16) {
		sq.set_start_trans(true);
		this->in_transaction = true;
		return 0;
	}*/ else {
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
	} else if (sq.has_inner_query() && query.has_alter_table()) {
		const sql_query_grammar::AlterTable &at = sq.inner_query().alter_table();
		const uint32_t tname = static_cast<uint32_t>(std::stoul(at.est().table_name().table().substr(1)));
		SQLTable &t = this->tables[tname];

		if (at.has_add_column()) {
			const uint32_t cname = static_cast<uint32_t>(std::stoul(at.add_column().new_col().col().column().substr(1)));

			if (success) {
				t.cols[cname] = std::move(t.staged_cols[cname]);
			}
			t.staged_cols.erase(cname);
		} else if (at.has_drop_column()) {
			const uint32_t cname = static_cast<uint32_t>(std::stoul(at.drop_column().column().substr(1)));

			if (success) {
				t.cols.erase(cname);
			}
		} else if (at.has_rename_column()) {
			const uint32_t old_cname = static_cast<uint32_t>(std::stoul(at.rename_column().old_name().column().substr(1))),
						   new_cname = static_cast<uint32_t>(std::stoul(at.rename_column().new_name().column().substr(1)));

			if (success) {
				t.cols[new_cname] = std::move(t.cols[old_cname]);
				t.cols.erase(old_cname);
			}
		} else if (at.has_modify_column()) {
			const uint32_t cname = static_cast<uint32_t>(std::stoul(at.modify_column().new_col().col().column().substr(1)));

			if (success) {
				t.cols.erase(cname);
				t.cols[cname] = std::move(t.staged_cols[cname]);
			}
			t.staged_cols.erase(cname);
		}
	}
}

}
