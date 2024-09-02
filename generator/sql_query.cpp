#include "sql_types.h"
#include "statement_generator.h"
#include <cstdint>
#include <string>

namespace chfuzz {

int StatementGenerator::GenerateArrayJoin(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ArrayJoin *aj) {
	SQLType *tp = nullptr;
	SQLRelation rel("");
	std::string cname = "c" + std::to_string(this->levels[this->current_level].aliases_counter++);
	sql_query_grammar::ExprColAlias *eca = aj->mutable_constraint();
	sql_query_grammar::Expr *expr = eca->mutable_expr();

	aj->set_left(rg.NextBool());
	if (rg.NextSmallNumber() < 8) {
		const SQLRelation &rel1 = rg.PickRandomlyFromVector(this->levels[this->current_level].rels);
		const SQLRelationCol &col1 = rg.PickRandomlyFromVector(rel1.cols);
		sql_query_grammar::ExprSchemaTableColumn *estc = expr->mutable_comp_expr()->mutable_expr_stc();
		sql_query_grammar::ExprColumn *ecol = estc->mutable_col();

		if (rel1.name != "") {
			estc->mutable_table()->set_table(rel1.name);
		}
		ecol->mutable_col()->set_column(col1.name);
		AddFieldAccess(cc, rg, expr, 16);
		AddJSONAccess(cc, rg, ecol, 31);
		tp = col1.tp;
	} else {
		tp = GenerateArraytype(rg, true, true);
		GenerateExpression(cc, rg, expr);
	}
	rel.cols.push_back(SQLRelationCol("", cname, tp));
	this->levels[this->current_level].rels.push_back(std::move(rel));
	eca->mutable_col_alias()->set_column(cname);
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

	if (this->depth < this->max_depth && this->width < this->max_width && (this->tables.empty() || rg.NextSmallNumber() < 5)) {
		std::vector<SQLType*> cols;
		sql_query_grammar::JoinedDerivedQuery *jdq = tos->mutable_jdq();
		sql_query_grammar::Select *sel = jdq->mutable_select();
		uint32_t ncols = std::min(this->max_width - this->width, (rg.NextMediumNumber() % UINT32_C(5)) + 1);

		for (uint32_t i = 0 ; i < ncols; i++) {
			cols.push_back(RandomNextType(rg, true, true));
		}

		std::map<uint32_t, QueryLevel> levels_backup;
		for (const auto &entry : this->levels) {
			levels_backup[entry.first] = std::move(entry.second);
		}
		this->levels.clear();

		this->current_level++;
		res = std::max(res, GenerateSelect(cc, rg, false, cols.size(), sel));
		this->current_level--;

		for (const auto &entry : levels_backup) {
			this->levels[entry.first] = std::move(entry.second);
		}

		jdq->mutable_table_alias()->set_table(name);
		if (sel->has_select_core()) {
			const sql_query_grammar::SelectStatementCore &scc = sel->select_core();

			for (uint32_t i = 0; i < scc.result_columns_size(); i++) {
				rel.cols.push_back(SQLRelationCol(name, scc.result_columns(i).eca().col_alias().column(), cols[i]));
			}
		} else {
			for (uint32_t i = 0; i < cols.size(); i++) {
				rel.cols.push_back(SQLRelationCol(name, "`" + std::to_string(i + 1) + "`", cols[i]));
			}
		}
	} else if (!this->tables.empty()) {
		sql_query_grammar::JoinedTable *jt = tos->mutable_jt();
		const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);
		const std::string tname = "t" + std::to_string(t.tname);

		jt->mutable_est()->mutable_table_name()->set_table(tname);
		for (const auto &entry : t.cols) {
			rel.cols.push_back(SQLRelationCol(name, "c" + std::to_string(entry.first), entry.second.tp));
		}
		jt->mutable_table_alias()->set_table(name);
		jt->set_final(rg.NextSmallNumber() < 3);
	} else {
		//fallback case
		sql_query_grammar::JoinedDerivedQuery *jdq = tos->mutable_jdq();
		sql_query_grammar::Select *sel = jdq->mutable_select();
		sql_query_grammar::SelectStatementCore *ssc = sel->mutable_select_core();
		sql_query_grammar::ExprColAlias *eca = ssc->add_result_columns()->mutable_eca();

		GenerateLiteralValue(cc, rg, eca->mutable_expr());
		eca->mutable_col_alias()->set_column("c0");
		rel.cols.push_back(SQLRelationCol(name, "c0", nullptr));
		jdq->mutable_table_alias()->set_table(name);
	}
	this->levels[this->current_level].rels.push_back(std::move(rel));
	return 0;
}

int StatementGenerator::AddJoinClause(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::BinaryExpr *bexpr) {
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
	bexpr->set_op(rg.NextSmallNumber() < 8 ? sql_query_grammar::BinaryOperator::BINOP_EQ : (sql_query_grammar::BinaryOperator) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::BinaryOperator::BINOP_LEGR) + 1));
	const SQLRelationCol &col1 = rg.PickRandomlyFromVector(rel1->cols),
						 &col2 = rg.PickRandomlyFromVector(rel2->cols);
	sql_query_grammar::Expr *expr1 = bexpr->mutable_lhs(), *expr2 = bexpr->mutable_rhs();
	sql_query_grammar::ExprSchemaTableColumn *estc1 = expr1->mutable_comp_expr()->mutable_expr_stc(),
											 *estc2 = expr2->mutable_comp_expr()->mutable_expr_stc();
	sql_query_grammar::ExprColumn *ecol1 = estc1->mutable_col(), *ecol2 = estc2->mutable_col();

	if (rel1->name != "") {
		estc1->mutable_table()->set_table(rel1->name);
	}
	if (rel2->name != "") {
		estc1->mutable_table()->set_table(rel2->name);
	}
	ecol1->mutable_col()->set_column(col1.name);
	ecol2->mutable_col()->set_column(col2.name);
	AddFieldAccess(cc, rg, expr1, 16);
	AddFieldAccess(cc, rg, expr2, 16);
	AddJSONAccess(cc, rg, ecol1, 31);
	AddJSONAccess(cc, rg, ecol2, 31);
	return 0;
}

int StatementGenerator::GenerateJoinConstraint(ClientContext &cc, RandomGenerator &rg, const bool allow_using, sql_query_grammar::JoinConstraint *jc) {
	if (rg.NextSmallNumber() < 8) {
		bool generated = false;

		if (allow_using && rg.NextSmallNumber() < 3) {
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
		if (this->width < this->max_width && !generated) {
			//joining clause
			const uint32_t nclauses = std::min(this->max_width - this->width, (rg.NextSmallNumber() % 3) + 1);
			sql_query_grammar::BinaryExpr *bexpr = jc->mutable_on_expr()->mutable_comp_expr()->mutable_binary_expr();

			for (uint32_t i = 0 ; i < nclauses; i++) {
				if (i == nclauses - 1) {
					AddJoinClause(cc, rg, bexpr);
				} else {
					AddJoinClause(cc, rg, bexpr->mutable_lhs()->mutable_comp_expr()->mutable_binary_expr());
					bexpr->set_op(rg.NextSmallNumber() < 8 ? sql_query_grammar::BinaryOperator::BINOP_AND : sql_query_grammar::BinaryOperator::BINOP_OR);
					bexpr = bexpr->mutable_rhs()->mutable_comp_expr()->mutable_binary_expr();
				}
			}
		}
	} else {
		//random clause
		GenerateExpression(cc, rg, jc->mutable_on_expr());
	}
	return 0;
}

int StatementGenerator::AddWhereFilter(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::BinaryExpr *bexpr) {
	const SQLRelation &rel1 = rg.PickRandomlyFromVector(this->levels[this->current_level].rels);
	const SQLRelationCol &col = rg.PickRandomlyFromVector(rel1.cols);

	bexpr->set_op(rg.NextSmallNumber() < 8 ? sql_query_grammar::BinaryOperator::BINOP_EQ : (sql_query_grammar::BinaryOperator) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::BinaryOperator::BINOP_LEGR) + 1));
	sql_query_grammar::Expr *lexpr = bexpr->mutable_lhs(), *rexpr = bexpr->mutable_rhs();
	if (rg.NextSmallNumber() < 3) {
		sql_query_grammar::Expr *aux = lexpr;
		lexpr = rexpr;
		rexpr = aux;
	}
	sql_query_grammar::ExprSchemaTableColumn *estc = lexpr->mutable_comp_expr()->mutable_expr_stc();
	sql_query_grammar::ExprColumn *ecol = estc->mutable_col();

	if (rel1.name != "") {
		estc->mutable_table()->set_table(rel1.name);
	}
	ecol->mutable_col()->set_column(col.name);
	AddFieldAccess(cc, rg, lexpr, 16);
	AddJSONAccess(cc, rg, ecol, 31);
	GenerateLiteralValue(cc, rg, rexpr);
	return 0;
}

int StatementGenerator::GenerateWherePredicate(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr) {
	this->depth++;
	if (!this->levels[this->current_level].rels.empty() && rg.NextSmallNumber() < 8) {
		const uint32_t nclauses = std::max(std::min(this->max_width - this->width, (rg.NextSmallNumber() % 4) + 1), UINT32_C(1));
		sql_query_grammar::BinaryExpr *bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

		for (uint32_t i = 0 ; i < nclauses; i++) {
			this->width++;
			if (i == nclauses - 1) {
				AddWhereFilter(cc, rg, bexpr);
			} else {
				AddWhereFilter(cc, rg, bexpr->mutable_lhs()->mutable_comp_expr()->mutable_binary_expr());
				bexpr->set_op(rg.NextSmallNumber() < 8 ? sql_query_grammar::BinaryOperator::BINOP_AND : sql_query_grammar::BinaryOperator::BINOP_OR);
				bexpr = bexpr->mutable_rhs()->mutable_comp_expr()->mutable_binary_expr();
			}
		}
		this->width -= nclauses;
	} else {
		//random clause
		GenerateExpression(cc, rg, expr);
	}
	this->depth--;
	return 0;
}

int StatementGenerator::GenerateFromStatement(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::FromStatement *ft) {
	sql_query_grammar::JoinClause *jc = ft->mutable_tos()->mutable_join_clause();
	const uint32_t njoined = std::min(this->max_width - this->width, (rg.NextMediumNumber() % UINT32_C(4)) + 1);

	this->depth++;
	this->width++;
	GenerateFromElement(cc, rg, jc->mutable_tos());
	for (uint32_t i = 1 ; i < njoined; i++) {
		sql_query_grammar::JoinClauseCore *jcc = jc->add_clauses();

		this->depth++;
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
						core->set_join_const((sql_query_grammar::JoinCore_JoinConst) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::JoinCore_JoinConst::JoinCore_JoinConst_ANTI) + 1));
						break;
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_FULL:
						core->set_join_const(sql_query_grammar::JoinCore_JoinConst::JoinCore_JoinConst_ALL);
						break;
				}
			}
			GenerateFromElement(cc, rg, core->mutable_tos());
			GenerateJoinConstraint(cc, rg, njoined == 1, core->mutable_join_constraint());
		}
	}
	this->width -= njoined;
	this->depth -= njoined;
	return 0;
}

int StatementGenerator::GenerateGroupBy(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::GroupByStatement *gbs) {
	std::vector<SQLRelationCol> available_cols;

	if (!this->levels[this->current_level].rels.empty()) {
		for (const auto &entry : this->levels[this->current_level].rels) {
			available_cols.insert(available_cols.end(), entry.cols.begin(), entry.cols.end());
		}
		std::shuffle(available_cols.begin(), available_cols.end(), rg.gen);
	}

	this->depth++;
	if (rg.NextSmallNumber() < (available_cols.empty() ? 3 : 8)) {
		std::vector<SQLRelationCol> gcols;
		sql_query_grammar::GroupByList *gbl = gbs->mutable_glist();
		sql_query_grammar::ExprList *elist = gbl->mutable_exprs();
		const uint32_t nclauses = std::min<uint32_t>(this->max_width - this->width, std::min<uint32_t>(UINT32_C(5), ((uint32_t)rg.NextRandomUInt32() % (available_cols.empty() ? 5 : available_cols.size())) + 1));
		const bool has_gs = rg.NextSmallNumber() < 4, has_totals = rg.NextSmallNumber() < 4;

		for (uint32_t i = 0 ; i < nclauses; i++) {
			sql_query_grammar::Expr *expr = i == 0 ? elist->mutable_expr() : elist->add_extra_exprs();

			this->width++;
			if (!available_cols.empty() && rg.NextSmallNumber() < 10) {
				const SQLRelationCol &rel_col = available_cols[i];
				sql_query_grammar::ExprSchemaTableColumn *estc = expr->mutable_comp_expr()->mutable_expr_stc();
				sql_query_grammar::ExprColumn *ecol = estc->mutable_col();

				if (rel_col.rel_name != "") {
					estc->mutable_table()->set_table(rel_col.rel_name);
				}
				ecol->mutable_col()->set_column(rel_col.name);
				AddFieldAccess(cc, rg, expr, 16);
				AddJSONAccess(cc, rg, ecol, 31);
				gcols.push_back(rel_col);
			} else {
				GenerateExpression(cc, rg, expr);
			}
		}
		this->width -= nclauses;
		this->levels[this->current_level].gcols = std::move(gcols);

		if (has_gs) {
			gbl->set_gs((sql_query_grammar::GroupByList_GroupingSets) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::GroupByList::GroupingSets_MAX) + 1));
		}
		gbl->set_with_totals(has_totals);

		if (!has_gs && !has_totals && rg.NextSmallNumber() < 5) {
			GenerateExpression(cc, rg, gbs->mutable_having_expr());
		}
	} else {
		gbs->set_gall(true);
	}
	this->depth--;
	return 0;
}

int StatementGenerator::GenerateOrderBy(ClientContext &cc, RandomGenerator &rg, const uint32_t ncols, sql_query_grammar::OrderByStatement *ob) {
	std::vector<SQLRelationCol> available_cols;

	if (!this->levels[this->current_level].gcols.empty()) {
		for (const auto &entry : this->levels[this->current_level].gcols) {
			available_cols.push_back(entry);
		}
	} else if (!this->levels[this->current_level].global_aggregate) {
		for (const auto &entry : this->levels[this->current_level].rels) {
			available_cols.insert(available_cols.end(), entry.cols.begin(), entry.cols.end());
		}
	}
	if (!available_cols.empty()) {
		std::shuffle(available_cols.begin(), available_cols.end(), rg.gen);
	}
	const uint32_t nclauses = std::min<uint32_t>(this->max_width - this->width, std::min<uint32_t>(UINT32_C(5), ((uint32_t)rg.NextRandomUInt32() % (available_cols.empty() ? 5 : available_cols.size())) + 1));

	this->depth++;
	for (uint32_t i = 0 ; i < nclauses; i++) {
		sql_query_grammar::ExprOrderingTerm *eot = i == 0 ? ob->mutable_ord_term() : ob->add_extra_ord_terms();
		sql_query_grammar::Expr *expr = eot->mutable_expr();
		const int next_option = rg.NextSmallNumber();

		this->width++;
		if (!available_cols.empty() && next_option < 6) {
			const SQLRelationCol &src = available_cols[i];
			sql_query_grammar::ExprSchemaTableColumn *estc = expr->mutable_comp_expr()->mutable_expr_stc();
			sql_query_grammar::ExprColumn *ecol = estc->mutable_col();

			if (src.rel_name != "") {
				estc->mutable_table()->set_table(src.rel_name);
			}
			ecol->mutable_col()->set_column(src.name);
			AddFieldAccess(cc, rg, expr, 16);
			AddJSONAccess(cc, rg, ecol, 31);
		} else if (next_option < 9) {
			sql_query_grammar::LiteralValue *lv = expr->mutable_lit_val();

			lv->set_uint_lit((rg.NextRandomUInt64() % ncols) + 1);
		} else {
			GenerateExpression(cc, rg, expr);
		}
		if (rg.NextSmallNumber() < 7) {
			eot->set_asc_desc(rg.NextBool() ? sql_query_grammar::ExprOrderingTerm_AscDesc::ExprOrderingTerm_AscDesc_ASC : sql_query_grammar::ExprOrderingTerm_AscDesc::ExprOrderingTerm_AscDesc_DESC);
		}
	}
	this->width -= nclauses;
	this->depth--;
	return 0;
}

int StatementGenerator::GenerateLimit(ClientContext &cc, RandomGenerator &rg, const bool has_order_by, const bool has_distinct,
									  const uint32_t ncols, sql_query_grammar::LimitStatement *ls) {
	uint32_t nlimit = 0;
	const int next_option = rg.NextSmallNumber();

	this->depth++;
	if (next_option < 3) {
		nlimit = 0;
	} else if (next_option < 5) {
		nlimit = 1;
	} else if (next_option < 7) {
		nlimit = 2;
	} else if (next_option < 9) {
		nlimit = 10;
	} else {
		nlimit = rg.NextRandomUInt32();
	}
	ls->set_limit(nlimit);
	if (rg.NextSmallNumber() < 6) {
		uint32_t noffset = 0;
		const int next_option = rg.NextSmallNumber();

		if (next_option < 3) {
			noffset = 0;
		} else if (next_option < 5) {
			noffset = 1;
		} else if (next_option < 7) {
			noffset = 2;
		} else if (next_option < 9) {
			noffset = 10;
		} else {
			noffset = rg.NextRandomUInt32();
		}
		ls->set_offset(noffset);
	}
	ls->set_with_ties(has_order_by && !has_distinct && rg.NextSmallNumber() < 7);
	if (rg.NextSmallNumber() < 4) {
		const int next_option = rg.NextSmallNumber();
		sql_query_grammar::Expr *expr = ls->mutable_limit_by();

		if (this->depth >= this->max_depth || next_option < 4) {
			sql_query_grammar::LiteralValue *lv = expr->mutable_lit_val();

			lv->set_uint_lit((rg.NextRandomUInt64() % ncols) + 1);
		} else {
			GenerateExpression(cc, rg, expr);
		}
	}
	this->depth--;
	return 0;
}

int StatementGenerator::GenerateSelect(ClientContext &cc, RandomGenerator &rg, const bool top, const uint32_t ncols, sql_query_grammar::Select *sel) {
	int res = 0;

	this->levels[this->current_level] = QueryLevel(this->current_level);
	if (this->depth < this->max_depth && this->max_width > this->width + 1 && rg.NextSmallNumber() < 3) {
		sql_query_grammar::SetQuery *setq = sel->mutable_set_query();

		setq->set_set_op((sql_query_grammar::SetQuery_SetOp) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::SetQuery::SetOp_MAX) + 1));
		setq->set_s_or_d((sql_query_grammar::AllOrDistinct) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::AllOrDistinct_MAX) + 1));

		this->depth++;
		this->current_level++;
		this->width++;
		res = std::max<int>(res, GenerateSelect(cc, rg, false, ncols, setq->mutable_sel1()));
		this->width++;
		res = std::max<int>(res, GenerateSelect(cc, rg, false, ncols, setq->mutable_sel2()));
		this->current_level--;
		this->depth--;
		this->width-=2;
	} else {
		sql_query_grammar::SelectStatementCore *ssc = sel->mutable_select_core();

		if (rg.NextSmallNumber() < 4) {
			ssc->set_s_or_d((sql_query_grammar::AllOrDistinct) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::AllOrDistinct_MAX) + 1));
		}
		if (this->depth < this->max_depth && this->width < this->max_width && rg.NextSmallNumber() < 10) {
			res = std::max<int>(res, GenerateFromStatement(cc, rg, ssc->mutable_from()));
		}

		if (this->depth < this->max_depth && rg.NextSmallNumber() < 5) {
			GenerateWherePredicate(cc, rg, ssc->mutable_pre_where()->mutable_expr()->mutable_expr());
		}
		if (this->depth < this->max_depth && rg.NextSmallNumber() < 5) {
			GenerateWherePredicate(cc, rg, ssc->mutable_where()->mutable_expr()->mutable_expr());
		}

		if (this->depth < this->max_depth && this->width < this->max_width && rg.NextSmallNumber() < 4) {
			GenerateGroupBy(cc, rg, ssc->mutable_groupby());
		} else {
			this->levels[this->current_level].global_aggregate = rg.NextSmallNumber() < 3;
		}

		this->depth++;
		for (uint32_t i = 0 ; i < ncols; i++) {
			sql_query_grammar::ExprColAlias *eca = ssc->add_result_columns()->mutable_eca();

			this->width++;
			GenerateExpression(cc, rg, eca->mutable_expr());
			if (!top) {
				const std::string cname = "c" + std::to_string(this->levels[this->current_level].aliases_counter++);

				SQLRelation rel("");
				rel.cols.push_back(SQLRelationCol("", cname, nullptr));
				this->levels[this->current_level].rels.push_back(std::move(rel));
				eca->mutable_col_alias()->set_column(cname);
			}
		}
		this->depth--;
		this->width -= ncols;

		if (this->depth < this->max_depth && this->width < this->max_width && rg.NextSmallNumber() < 4) {
			GenerateOrderBy(cc, rg, ncols, ssc->mutable_orderby());
		}
		if (rg.NextSmallNumber() < 3) {
			GenerateLimit(cc, rg, ssc->has_orderby(), ssc->s_or_d() == sql_query_grammar::AllOrDistinct::DISTINCT, ncols, ssc->mutable_limit());
		}
	}
	this->levels.erase(this->current_level);
	return res;
}

int StatementGenerator::GenerateTopSelect(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::TopSelect *ts) {
	int res = 0;
	const uint32_t ncols = std::max(std::min(this->max_width - this->width, (rg.NextMediumNumber() % UINT32_C(5)) + 1), UINT32_C(1));

	assert(this->levels.empty());
	if ((res = GenerateSelect(cc, rg, true, ncols, ts->mutable_sel()))) {
		return res;
	}
	if (rg.NextSmallNumber() < 3) {
		ts->set_format((sql_query_grammar::OutFormat) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::OutFormat_MAX) + 1));
	}
	return res;
}

}
