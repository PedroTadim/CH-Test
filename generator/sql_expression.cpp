#include "sql_types.h"
#include "sql_funcs.h"
#include "statement_generator.h"

#include <cstdint>
#include <sys/types.h>

namespace chfuzz {

int StatementGenerator::AddFieldAccess(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr,
									   const uint32_t nested_prob) {
	if (rg.NextMediumNumber() < nested_prob) {
		const int noption = rg.NextMediumNumber();
		sql_query_grammar::FieldAccess *fa = expr->mutable_field();

		this->depth++;
		if (noption < 41) {
			fa->set_array_index(rg.NextRandomUInt32() % 5);
		} else if (noption < 71) {
			fa->set_tuple_index(rg.NextRandomUInt32() % 5);
		} else if (this->depth >= this->max_depth || noption < 81) {
			std::string ret = "c";

			ret += rg.NextJsonCol();
			fa->mutable_array_key()->set_column(ret);
		} else {
			this->GenerateExpression(cc, rg, fa->mutable_array_expr());
		}
		this->depth--;
	}
	return 0;
}

int StatementGenerator::AddJSONAccess(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::ExprColumn *expr,
									  const uint32_t json_prob) {
	if (rg.NextMediumNumber() < json_prob) {
		sql_query_grammar::TypeName *tpn = nullptr;
		sql_query_grammar::JSONColumns *subcols = expr->mutable_subcols();
		const uint32_t noption = rg.NextMediumNumber(),
					   nvalues = std::max(std::min(this->max_width - this->width, rg.NextSmallNumber() % 5), UINT32_C(1));

		this->depth++;
		for (uint32_t i = 0; i < nvalues; i++) {
			std::string ret = "c";
			const uint32_t noption2 = rg.NextMediumNumber();
			sql_query_grammar::JSONColumn *jcol = i == 0 ? subcols->mutable_jcol() : subcols->add_other_jcols();

			this->width++;
			if (noption2 < 31) {
				jcol->set_json_col(true);
			} else if (noption2 < 61) {
				jcol->set_json_array(0);
			}
			ret += rg.NextJsonCol();
			jcol->mutable_col()->set_column(ret);
		}
		if (noption < 4) {
			tpn = subcols->mutable_json_cast();
		} else if (noption < 8) {
			tpn = subcols->mutable_json_reinterpret();
		}
		this->width -= nvalues;
		if (tpn) {
			uint32_t col_counter = 0;
			SQLType *tp = RandomNextType(rg, true, true, col_counter, tpn->mutable_type());
			delete tp;
		}
		this->depth--;
	}
	expr->set_null(rg.NextMediumNumber() < 4);
	return 0;
}

int StatementGenerator::GenerateLiteralValue(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr) {
	const int noption = rg.NextLargeNumber();
	sql_query_grammar::LiteralValue *lv = expr->mutable_lit_val();
	uint32_t nested_prob = 0;

	if (noption < 101) {
		lv->set_int_lit(rg.NextRandomInt64());
	} else if (noption < 201) {
		lv->set_uint_lit(rg.NextRandomUInt64());
	} else if (noption < 251) {
		lv->set_uint_lit(rg.NextDateTime());
	} else if (noption < 301) {
		lv->set_uint_lit(rg.NextDateTime64());
	} else if (noption < 351) {
		lv->set_uint_lit(rg.NextDate32());
	} else if (noption < 451) {
		std::string ret;
		std::uniform_int_distribution<uint32_t> next_dist(0, 30);
		const uint32_t left = next_dist(rg.gen), right = next_dist(rg.gen);

		AppendDecimal(rg, ret, left, right);
		lv->set_no_quote_str(ret);
	} else if (noption < 601) {
		std::string ret;

		ret += "'";
		rg.NextString(ret, 100000);
		ret += "'";
		lv->set_no_quote_str(ret);
	} else if (noption < 701) {
		lv->set_special_val((sql_query_grammar::SpecialVal) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::SpecialVal_MAX) + 1));
		nested_prob = 3;
	} else if (noption < 801) {
		lv->set_special_val(rg.NextBool() ? sql_query_grammar::SpecialVal::VAL_ONE : sql_query_grammar::SpecialVal::VAL_ZERO);
	} else if (noption < 951) {
		sql_query_grammar::LiteralValue *lv = expr->mutable_lit_val();
		std::string ret;
		std::uniform_int_distribution<int> dopt(1, 3), wopt(1, 3);

		ret += "'";
		StrBuildJSON(rg, dopt(rg.gen), wopt(rg.gen), ret);
		ret += "'::JSON";
		lv->set_no_quote_str(ret);
	} else {
		lv->set_special_val(sql_query_grammar::SpecialVal::VAL_NULL);
	}
	AddFieldAccess(cc, rg, expr, nested_prob);
	return 0;
}

int StatementGenerator::GenerateColRef(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr) {
	std::vector<SQLRelationCol> available_cols;

	if (this->levels[this->current_level].gcols.empty() ||
		!this->levels[this->current_level].global_aggregate ||
		this->levels[this->current_level].inside_aggregate) {
		for (const auto &entry : this->levels[this->current_level].rels) {
			available_cols.insert(available_cols.end(), entry.cols.begin(), entry.cols.end());
		}
	} else if (!this->levels[this->current_level].gcols.empty()) {
		for (const auto &entry : this->levels[this->current_level].gcols) {
			available_cols.push_back(entry);
		}
	}

	if (available_cols.empty()) {
		return this->GenerateLiteralValue(cc, rg, expr);
	}
	const SQLRelationCol &col = rg.PickRandomlyFromVector(available_cols);
	sql_query_grammar::ExprSchemaTableColumn *estc = expr->mutable_comp_expr()->mutable_expr_stc();
	sql_query_grammar::ExprColumn *ecol = estc->mutable_col();

	if (col.rel_name != "") {
		estc->mutable_table()->set_table(col.rel_name);
	}
	ecol->mutable_col()->set_column(col.name);
	AddFieldAccess(cc, rg, expr, 16);
	AddJSONAccess(cc, rg, ecol, 31);
	return 0;
}

int StatementGenerator::GenerateSubquery(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Select *sel) {
	this->current_level++;
	this->GenerateSelect(cc, rg, true, 1, sel);
	this->current_level--;
	return 0;
}

int StatementGenerator::GeneratePredicate(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr) {
	if (this->depth < this->max_depth) {
		const int noption = rg.NextLargeNumber();

		if (noption < 101) {
			sql_query_grammar::ComplicatedExpr *cexpr = expr->mutable_comp_expr();
			sql_query_grammar::UnaryExpr *unexp = cexpr->mutable_unary_expr();

			unexp->set_unary_op(sql_query_grammar::UnaryOperator::UNOP_NOT);
			this->depth++;
			if (rg.NextSmallNumber() < 5) {
				this->GeneratePredicate(cc, rg, unexp->mutable_expr());
			} else {
				this->GenerateExpression(cc, rg, unexp->mutable_expr());
			}
			this->depth--;
		} else if (this->max_width > this->width + 1 && noption < 301) {
			sql_query_grammar::ComplicatedExpr *cexpr = expr->mutable_comp_expr();
			sql_query_grammar::BinaryExpr *bexpr = cexpr->mutable_binary_expr();

			this->depth++;
			if (rg.NextSmallNumber() < 5) {
				bexpr->set_op(rg.NextBool() ? sql_query_grammar::BinaryOperator::BINOP_AND : sql_query_grammar::BinaryOperator::BINOP_OR);

				this->GeneratePredicate(cc, rg, bexpr->mutable_lhs());
				this->width++;
				this->GeneratePredicate(cc, rg, bexpr->mutable_rhs());
			} else {
				bexpr->set_op((sql_query_grammar::BinaryOperator) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::BinaryOperator_MAX) + 1));
				this->GenerateExpression(cc, rg, bexpr->mutable_lhs());
				this->width++;
				this->GenerateExpression(cc, rg, bexpr->mutable_rhs());
			}
			this->width--;
			this->depth--;
		} else if (this->max_width > this->width + 2 && noption < 401) {
			sql_query_grammar::ComplicatedExpr *cexpr = expr->mutable_comp_expr();
			sql_query_grammar::ExprBetween *bexpr = cexpr->mutable_expr_between();

			bexpr->set_not_(rg.NextBool());
			this->depth++;
			this->GenerateExpression(cc, rg, bexpr->mutable_expr1());
			this->width++;
			this->GenerateExpression(cc, rg, bexpr->mutable_expr2());
			this->width++;
			this->GenerateExpression(cc, rg, bexpr->mutable_expr3());
			this->width-=2;
			this->depth--;
		} else if (this->width < this->max_width && noption < 501) {
			const uint32_t nclauses = std::min(this->max_width - this->width, (rg.NextSmallNumber() % 4) + 1);
			sql_query_grammar::ComplicatedExpr *cexpr = expr->mutable_comp_expr();
			sql_query_grammar::ExprIn *ein = cexpr->mutable_expr_in();
			sql_query_grammar::ExprList *elist = ein->mutable_expr();

			ein->set_not_(rg.NextBool());
			ein->set_global(rg.NextBool());

			this->depth++;
			for (uint32_t i = 0 ; i < nclauses; i++) {
				sql_query_grammar::Expr* expr = i == 0 ? elist->mutable_expr() : elist->add_extra_exprs();

				this->GenerateExpression(cc, rg, expr);
			}
			if (rg.NextBool()) {
				this->GenerateSubquery(cc, rg, ein->mutable_sel());
			} else {
				sql_query_grammar::ExprList *elist2 = ein->mutable_exprs();

				for (uint32_t i = 0 ; i < nclauses; i++) {
					sql_query_grammar::Expr* expr = i == 0 ? elist2->mutable_expr() : elist2->add_extra_exprs();

					this->GenerateExpression(cc, rg, expr);
				}
			}
			this->depth--;
		} else if (this->max_width > this->width + 1 && noption < 601) {
			sql_query_grammar::ComplicatedExpr *cexpr = expr->mutable_comp_expr();
			sql_query_grammar::ExprAny *eany = cexpr->mutable_expr_any();

			eany->set_op((sql_query_grammar::BinaryOperator) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::BinaryOperator::BINOP_LEGR) + 1));
			eany->set_anyall(rg.NextBool());
			this->depth++;
			this->GenerateExpression(cc, rg, eany->mutable_expr());
			this->width++;
			this->GenerateSubquery(cc, rg, eany->mutable_sel());
			this->width--;
			this->depth--;
		} else if (noption < 701) {
			sql_query_grammar::ComplicatedExpr *cexpr = expr->mutable_comp_expr();
			sql_query_grammar::ExprNullTests *enull = cexpr->mutable_expr_null_tests();

			enull->set_not_(rg.NextBool());
			this->depth++;
			this->GenerateExpression(cc, rg, enull->mutable_expr());
			this->depth--;
		} else if (noption < 801) {
			sql_query_grammar::ComplicatedExpr *cexpr = expr->mutable_comp_expr();
			sql_query_grammar::ExprExists *exists = cexpr->mutable_expr_exists();

			exists->set_not_(rg.NextBool());
			this->depth++;
			this->GenerateSubquery(cc, rg, exists->mutable_select());
			this->depth--;
		} else if (this->max_width > this->width + 1 && noption < 901) {
			sql_query_grammar::ComplicatedExpr *cexpr = expr->mutable_comp_expr();
			sql_query_grammar::ExprLike *elike = cexpr->mutable_expr_like();

			elike->set_not_(rg.NextBool());
			elike->set_keyword((sql_query_grammar::ExprLike_PossibleKeywords) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::ExprLike::PossibleKeywords_MAX) + 1));
			this->depth++;
			this->GenerateExpression(cc, rg, elike->mutable_expr1());
			this->width++;
			this->GenerateExpression(cc, rg, elike->mutable_expr2());
			this->width--;
			this->depth--;
		} else {
			return this->GenerateExpression(cc, rg, expr);
		}
		AddFieldAccess(cc, rg, expr, 0);
	} else {
		return this->GenerateLiteralValue(cc, rg, expr);
	}

	return 0;
}

int StatementGenerator::GenerateExpression(ClientContext &cc, RandomGenerator &rg, sql_query_grammar::Expr *expr) {
	const int noption = rg.NextLargeNumber();

	if (rg.NextSmallNumber() < 3) {
		sql_query_grammar::ParenthesesExpr *paren = expr->mutable_comp_expr()->mutable_par_expr();
		sql_query_grammar::ExprColAlias *eca = paren->mutable_expr();

		if (rg.NextSmallNumber() < 4) {
			const std::string cname = "c" + std::to_string(this->levels[this->current_level].aliases_counter++);

			SQLRelation rel("");
			rel.cols.push_back(SQLRelationCol("", cname, nullptr));
			this->levels[this->current_level].rels.push_back(std::move(rel));
			eca->mutable_col_alias()->set_column(cname);
		}
		expr = eca->mutable_expr();
	}

	if (noption < 151) {
		return this->GenerateLiteralValue(cc, rg, expr);
	} else if (this->depth >= this->max_depth || noption < 401) {
		return this->GenerateColRef(cc, rg, expr);
	} else if (noption < 451) {
		this->depth++;
		int res = this->GeneratePredicate(cc, rg, expr);
		this->depth--;
		return res;
	} else if (noption < 501) {
		uint32_t col_counter = 0;
		sql_query_grammar::CastExpr *casexpr = expr->mutable_comp_expr()->mutable_cast_expr();

		this->depth++;
		SQLType* tp = RandomNextType(rg, true, true, col_counter, casexpr->mutable_type_name()->mutable_type());
		this->GenerateExpression(cc, rg, casexpr->mutable_expr());
		this->depth--;
		delete tp;
	} else if (noption < 526) {
		sql_query_grammar::UnaryExpr *uexpr = expr->mutable_comp_expr()->mutable_unary_expr();

		this->depth++;
		uexpr->set_unary_op((sql_query_grammar::UnaryOperator) ((rg.NextRandomUInt32() % (uint32_t) sql_query_grammar::UnaryOperator::UNOP_PLUS) + 1));
		this->GenerateExpression(cc, rg, uexpr->mutable_expr());
		this->depth--;
	} else if (this->max_width > this->width + 2 && noption < 551) {
		sql_query_grammar::CondExpr *conexpr = expr->mutable_comp_expr()->mutable_expr_cond();

		this->depth++;
		this->GenerateExpression(cc, rg, conexpr->mutable_expr1());
		this->width++;
		this->GenerateExpression(cc, rg, conexpr->mutable_expr2());
		this->width++;
		this->GenerateExpression(cc, rg, conexpr->mutable_expr3());
		this->width-=2;
		this->depth--;
	} else if (this->max_width > this->width + 1 && noption < 601) {
		sql_query_grammar::ExprCase *caseexp = expr->mutable_comp_expr()->mutable_expr_case();
		const uint32_t nwhen = std::min(this->max_width - this->width, rg.NextSmallNumber() % 4);

		this->depth++;
		if (rg.NextSmallNumber() < 5) {
			this->GenerateExpression(cc, rg, caseexp->mutable_expr());
		}
		for (uint32_t i = 0 ; i < nwhen; i++) {
			sql_query_grammar::ExprWhenThen *wt = i == 0 ? caseexp->mutable_when_then() : caseexp->add_extra_when_thens();

			this->GenerateExpression(cc, rg, wt->mutable_when_expr());
			this->GenerateExpression(cc, rg, wt->mutable_then_expr());
			this->width++;
		}
		this->width -= nwhen;
		if (rg.NextSmallNumber() < 5) {
			this->GenerateExpression(cc, rg, caseexp->mutable_else_expr());
		}
		this->depth--;
	} else if (noption < 651) {
		this->depth++;
		this->GenerateSubquery(cc, rg, expr->mutable_comp_expr()->mutable_subquery());
		this->depth--;
	} else if (this->max_width > this->width + 1 && noption < 701) {
		sql_query_grammar::BinaryExpr *bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

		this->depth++;
		bexpr->set_op((sql_query_grammar::BinaryOperator) ((rg.NextRandomUInt32() % 7) + 10));
		this->GenerateExpression(cc, rg, bexpr->mutable_lhs());
		this->width++;
		this->GenerateExpression(cc, rg, bexpr->mutable_rhs());
		this->width--;
		this->depth--;
	} else if (this->width < this->max_width && noption < 751) {
		sql_query_grammar::ArraySequence *arr = expr->mutable_comp_expr()->mutable_array();
		const uint32_t nvalues = std::min(this->max_width - this->width, rg.NextSmallNumber() % 8);

		this->depth++;
		for (uint32_t i = 0 ; i < nvalues; i++) {
			this->GenerateExpression(cc, rg, arr->add_values());
			this->width++;
		}
		this->depth--;
		this->width -= nvalues;
	} else if (this->width < this->max_width && noption < 801) {
		sql_query_grammar::TupleSequence *tupl = expr->mutable_comp_expr()->mutable_tuple();
		const uint32_t nvalues = std::min(this->max_width - this->width, rg.NextSmallNumber() % 8),
					   ncols = std::min(this->max_width - this->width, (rg.NextSmallNumber() % 4) + 1);

		this->depth++;
		for (uint32_t i = 0 ; i < ncols; i++) {
			sql_query_grammar::ExprList *elist = tupl->add_values();

			for (uint32_t j = 0 ; j < nvalues; j++) {
				sql_query_grammar::Expr *el = j == 0 ? elist->mutable_expr() : elist->add_extra_exprs();

				this->GenerateExpression(cc, rg, el);
				this->width++;
			}
			this->width -= nvalues;
			this->width++;
		}
		this->width -= ncols;
		this->depth--;
	} else {
		//func
		sql_query_grammar::SQLFuncCall *func_call = expr->mutable_comp_expr()->mutable_func_call();
		const bool allow_aggr = !this->levels[this->current_level].inside_aggregate && this->levels[this->current_level].allow_aggregates;
		const uint32_t nfuncs = CHFuncs.size() + (allow_aggr ? CHAggrs.size() : 0);
		std::uniform_int_distribution<uint32_t> next_dist(0, nfuncs - 1);
		const uint32_t nopt = next_dist(rg.gen);
		uint32_t generated_params = 0;

		this->depth++;
		if (nopt >= CHFuncs.size()) {
			//aggregate
			const CHAggregate &agg = CHAggrs[nopt - CHFuncs.size()];

			this->levels[this->current_level].inside_aggregate = true;
			const uint32_t max_params = std::min(this->max_width - this->width, std::min(agg.max_params, UINT32_C(5)));
			if (max_params > 0 && max_params >= agg.min_params) {
				std::uniform_int_distribution<uint32_t> nparams(agg.min_params, max_params);
				const uint32_t nopt = nparams(rg.gen);

				for (uint32_t i = 0 ; i < nopt; i++) {
					this->GenerateExpression(cc, rg, func_call->add_params());
					this->width++;
					generated_params++;
				}
			} else if (agg.min_params > 0) {
				for (uint32_t i = 0 ; i < agg.min_params; i++) {
					GenerateLiteralValue(cc, rg, func_call->add_params());
				}
			}

			const uint32_t max_args = std::min(this->max_width - this->width, std::min(agg.max_args, UINT32_C(5)));
			if (max_args > 0 && max_args >= agg.min_args) {
				std::uniform_int_distribution<uint32_t> nparams(agg.min_args, max_args);
				const uint32_t nopt = nparams(rg.gen);

				for (uint32_t i = 0 ; i < nopt; i++) {
					this->GenerateExpression(cc, rg, func_call->add_args());
					this->width++;
					generated_params++;
				}
			} else if (agg.min_args > 0) {
				for (uint32_t i = 0 ; i < agg.min_args; i++) {
					GenerateLiteralValue(cc, rg, func_call->add_args());
				}
			}
			this->levels[this->current_level].inside_aggregate = false;

			func_call->set_distinct(agg.support_distinct && rg.NextSmallNumber() < 5);
			func_call->set_respect_nulls(agg.support_respect_nulls && rg.NextSmallNumber() < 5);
			func_call->set_func((sql_query_grammar::SQLFunc)agg.fnum);
		} else {
			//function
			const CHFunction &func = CHFuncs[nopt];

			const uint32_t max_args = std::min(this->max_width - this->width, std::min(func.max_args, UINT32_C(5)));
			if (max_args > 0 && max_args >= func.min_args) {
				std::uniform_int_distribution<uint32_t> nparams(func.min_args, max_args);
				const uint32_t nopt = nparams(rg.gen);

				for (uint32_t i = 0 ; i < nopt; i++) {
					this->GenerateExpression(cc, rg, func_call->add_args());
					this->width++;
					generated_params++;
				}
			} else if (func.min_args > 0) {
				for (uint32_t i = 0 ; i < func.min_args; i++) {
					GenerateLiteralValue(cc, rg, func_call->add_args());
				}
			}
			func_call->set_func((sql_query_grammar::SQLFunc)func.fnum);
		}
		this->depth--;
		this->width -= generated_params;
	}
	AddFieldAccess(cc, rg, expr, 16);
	return 0;
}

}
