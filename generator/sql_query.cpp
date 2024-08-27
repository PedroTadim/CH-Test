#include "sql_types.h"
#include "statement_generator.h"
#include <cstdint>
#include <string>

namespace chfuzz {

int StatementGenerator::GenerateArrayJoin(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ArrayJoin *aj) {
	sql_query_grammar::ExprColAlias *eca = aj->mutable_constraint();
	sql_query_grammar::Expr *expr = eca->mutable_expr();

	SQLRelation rel("");
	SQLType *tp;
	std::string cname = "c" + std::to_string(this->levels[this->current_level].aliases_counter++);

	aj->set_left(rg.NextBool());
	if (rg.NextSmallNumber() < 8) {
		const SQLRelation &rel1 = rg.PickRandomlyFromVector(this->levels[this->current_level].rels);
		const SQLRelationCol &col1 = rg.PickRandomlyFromVector(rel.cols);

		sql_query_grammar::ExprSchemaTableColumn *estc1 = expr->mutable_comp_expr()->mutable_expr_stc();
		if (rel1.name != "") {
			estc1->mutable_table()->set_table(rel1.name);
		}
		estc1->mutable_col()->mutable_col()->set_column(col1.name);
		tp = col1.tp;
		rel.cols.push_back(SQLRelationCol(cname, col1.tp));
	} else {
		uint32_t col_counter = 0;
		tp = GenerateArraytype(rg, true, true, col_counter, nullptr);

		GenerateExpression(cc, rg, tp, expr);
	}
	rel.cols.push_back(SQLRelationCol(cname, tp));
	eca->mutable_col_alias()->set_column(cname);
	this->levels[this->current_level].rels.push_back(std::move(rel));
	return 0;
}

int StatementGenerator::GenerateFromElement(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::TableOrSubquery *tos) {
	int res = 0;
	std::string name;

	name += "t";
	name += std::to_string(this->levels[this->current_level].rels.size());
	name += "d";
	name += std::to_string(this->current_level);
	SQLRelation rel(name);

	if (this->tables.empty() || rg.NextSmallNumber() < 5) {
		std::vector<SQLType*> cols;
		sql_query_grammar::JoinedDerivedQuery *jdq = tos->mutable_jdq();
		uint32_t ncols = std::min((uint32_t) this->width, (rg.NextMediumNumber() % UINT32_C(5)) + 1);

		for (uint32_t i = 0 ; i < ncols; i++) {
			uint32_t col_counter = 0;
			SQLType *next = RandomNextType(rg, true, true, col_counter, nullptr);

			cols.push_back(next);
		}

		std::map<uint32_t, QueryLevel> levels_backup;
		for (const auto &entry : this->levels) {
			levels_backup[entry.first] = std::move(entry.second);
		}
		this->levels.clear();

		this->depth++;
		this->current_level++;
		res = std::max(res, GenerateSelect(cc, rg, cols, jdq->mutable_select()));
		this->current_level--;
		this->depth--;

		for (const auto &entry : levels_backup) {
			this->levels[entry.first] = std::move(entry.second);
		}

		jdq->mutable_table_alias()->set_table(name);
		for (uint32_t i = 0 ; i < ncols; i++) {
			rel.cols.push_back(SQLRelationCol("c" + std::to_string(i), cols[i]));
		}
	} else {
		std::string name;
		sql_query_grammar::JoinedTable *jt = tos->mutable_jt();
		const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

		jt->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
		for (const auto &entry : t.cols) {
			rel.cols.push_back(SQLRelationCol("c" + std::to_string(entry.first), entry.second.tp));
		}
	}
	this->levels[this->current_level].rels.push_back(std::move(rel));
	return 0;
}

void StatementGenerator::AddJoinClause(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::BinaryExpr *bexpr) {
	const SQLRelation *rel1 = &rg.PickRandomlyFromVector(this->levels[this->current_level].rels),
					  *rel2 = &this->levels[this->current_level].rels.back();

	if (rel1->name == rel2->name) {
		rel1 = &this->levels[this->current_level].rels[this->levels[this->current_level].rels.size() - 2];
	}
	if (rg.NextSmallNumber() < 4) {
		//swap
		const SQLRelation *rel3 = rel1;
		rel1 = rel2;
		rel2 = rel3;
	}
	if (rg.NextSmallNumber() < 8) {
		bexpr->set_op(sql_query_grammar::BinaryOperator::BINOP_EQ);
	} else {
		bexpr->set_op((sql_query_grammar::BinaryOperator) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::BinaryOperator::BINOP_LEGR) + 1));
	}
	const SQLRelationCol &col1 = rg.PickRandomlyFromVector(rel1->cols),
						 &col2 = rg.PickRandomlyFromVector(rel2->cols);
	sql_query_grammar::ExprSchemaTableColumn *estc1 = bexpr->mutable_lhs()->mutable_comp_expr()->mutable_expr_stc(),
											 *estc2 = bexpr->mutable_rhs()->mutable_comp_expr()->mutable_expr_stc();
	if (rel1->name != "") {
		estc1->mutable_table()->set_table(rel1->name);
	}
	if (rel2->name != "") {
		estc1->mutable_table()->set_table(rel2->name);
	}

	estc1->mutable_table()->set_table(rel1->name);
	estc2->mutable_table()->set_table(rel2->name);
	estc1->mutable_col()->mutable_col()->set_column(col1.name);
	estc2->mutable_col()->mutable_col()->set_column(col2.name);
}

int StatementGenerator::GenerateJoinConstraint(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::JoinConstraint *jc) {
	if (rg.NextSmallNumber() < 8) {
		//using clause
		bool generated = false;

		if (rg.NextSmallNumber() < 3) {
			//using clause
			const SQLRelation &rel1 = rg.PickRandomlyFromVector(this->levels[this->current_level].rels),
							  &rel2 = this->levels[this->current_level].rels.back();
			std::vector<std::string> cols1, cols2, intersect;

			for (const auto &entry : rel1.cols) {
				cols1.push_back(entry.name);
			}
			for (const auto &entry : rel2.cols) {
				cols2.push_back(entry.name);
			}
			std::set_intersection(cols1.begin(), cols1.end(), cols2.begin(), cols2.end(), std::back_inserter(intersect));

			if (!intersect.empty()) {
				sql_query_grammar::ExprColumnList *ecl = jc->mutable_using_expr()->mutable_col_list();
				const uint32_t nclauses = std::min<uint32_t>(UINT32_C(3), ((uint32_t)rg.NextRandomUInt32() % intersect.size()) + 1);

				std::shuffle(intersect.begin(), intersect.end(), rg.gen);
				for (uint32_t i = 0 ; i < nclauses; i++) {
					sql_query_grammar::ExprColumn *ec = i == 0 ? ecl->mutable_col() : ecl->add_extra_cols();

					ec->mutable_col()->set_column(intersect[i]);
				}
				generated = true;
			}
		}
		if (!generated) {
			//joining clause
			const uint32_t nclauses = (rg.NextSmallNumber() % 3) + 1;
			sql_query_grammar::BinaryExpr *bexpr = jc->mutable_on_expr()->mutable_comp_expr()->mutable_binary_expr();

			for (uint32_t i = 0 ; i < nclauses; i++) {
				if (i == nclauses - 1) {
					AddJoinClause(cc, rg, bexpr);
				} else {
					AddJoinClause(cc, rg, bexpr->mutable_lhs()->mutable_comp_expr()->mutable_binary_expr());
					if (rg.NextSmallNumber() < 8) {
						bexpr->set_op(sql_query_grammar::BinaryOperator::BINOP_AND);
					} else {
						bexpr->set_op(sql_query_grammar::BinaryOperator::BINOP_OR);
					}
					bexpr = bexpr->mutable_rhs()->mutable_comp_expr()->mutable_binary_expr();
				}
			}
		}
	} else {
		//random clause
		GeneratePredicate(cc, rg, jc->mutable_on_expr());
	}
	return 0;
}

int StatementGenerator::GenerateFromStatement(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::FromStatement *ft) {
	sql_query_grammar::JoinClause *jc = ft->mutable_tos()->mutable_join_clause();
	const uint32_t njoined = std::min((uint32_t) this->width, (rg.NextMediumNumber() % UINT32_C(4)) + 1);

	GenerateFromElement(cc, rg, jc->mutable_tos());
	for (uint32_t i = 1 ; i < njoined; i++) {
		sql_query_grammar::JoinClauseCore *jcc = jc->add_clauses();

		this->width++;
		if (rg.NextSmallNumber() < 3) {
			GenerateArrayJoin(cc, rg, jcc->mutable_arr());
		} else {
			sql_query_grammar::JoinCore *core = jcc->mutable_core();
			sql_query_grammar::JoinCore_JoinType jt = (sql_query_grammar::JoinCore_JoinType) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::JoinCore::JoinType_MAX) + 1);

			core->set_global(rg.NextSmallNumber() < 3);
			core->set_join_op(jt);
			if (rg.NextSmallNumber() < 4) {
				switch (jt) {
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_LEFT:
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_INNER:
						core->set_join_const((sql_query_grammar::JoinCore_JoinConst) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::JoinCore::JoinConst_MAX) + 1));
						break;
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_RIGHT:
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_FULL:
						core->set_join_const((sql_query_grammar::JoinCore_JoinConst) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::JoinCore_JoinConst::JoinCore_JoinConst_ANTI) + 1));
						break;
				}
			}
			GenerateFromElement(cc, rg, core->mutable_tos());
			GenerateJoinConstraint(cc, rg, core->mutable_join_constraint());
		}
	}
	this->width -= (njoined - 1);
	return 0;
}

int StatementGenerator::GenerateSelect(ClientContext &cc, RandomGenerator &rg, std::vector<SQLType*> cols, sql_query_grammar::Select *sel) {
	int res = 0;

	this->levels[this->current_level] = QueryLevel(this->current_level);
	if (this->depth < this->max_depth && rg.NextSmallNumber() < 3) {
		sql_query_grammar::SetQuery *setq = sel->mutable_set_query();

		setq->set_set_op((sql_query_grammar::SetQuery_SetOp) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::SetQuery::SetOp_MAX) + 1));
		setq->set_s_or_d((sql_query_grammar::AllOrDistinct) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::AllOrDistinct_MAX) + 1));

		this->depth++;
		this->current_level++;
		res = std::max<int>(res, GenerateSelect(cc, rg, cols, setq->mutable_sel1()));
		res = std::max<int>(res, GenerateSelect(cc, rg, cols, setq->mutable_sel2()));
		this->current_level--;
		this->depth--;
	} else {
		sql_query_grammar::SelectStatementCore *ssc = sel->mutable_select_core();

		if (rg.NextSmallNumber() < 3) {
			ssc->set_s_or_d((sql_query_grammar::AllOrDistinct) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::AllOrDistinct_MAX) + 1));
		}
		if (this->depth < this->max_depth && this->width < this->max_width && rg.NextSmallNumber() < 10) {
			this->depth++;
			res = std::max<int>(res, GenerateFromStatement(cc, rg, ssc->mutable_from()));
			this->depth--;
		}
	}
	this->levels.erase(this->current_level);
	return res;
}

int StatementGenerator::GenerateTopSelect(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::TopSelect *ts) {
	int res = 0;
	uint32_t ncols = std::min((uint32_t) this->width, (rg.NextMediumNumber() % UINT32_C(5)) + 1);
	std::vector<SQLType*> cols;

	assert(this->levels.empty());
	for (uint32_t i = 0 ; i < ncols; i++) {
		uint32_t col_counter = 0;
		cols.push_back(RandomNextType(rg, true, true, col_counter, nullptr));
	}

	if ((res = GenerateSelect(cc, rg, cols, ts->mutable_sel()))) {
		return res;
	}
	if (rg.NextSmallNumber() < 3) {
		ts->set_format((sql_query_grammar::OutFormat) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::OutFormat_MAX) + 1));
	}
	for (uint32_t i = 0 ; i < ncols; i++) {
		delete cols[i];
	}
	return res;
}

}
