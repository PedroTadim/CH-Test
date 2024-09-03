#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <limits>
#include <random>

#include "sql_grammar.pb.h"

using namespace sql_query_grammar;

#define CONV_FN(TYPE, VAR_NAME) void TYPE##ToString(std::string &ret, const TYPE& VAR_NAME)

namespace sql_fuzzer {

static constexpr char hex_digits[] = "0123456789ABCDEF";
static constexpr char digits[] = "0123456789";

CONV_FN(Expr, expr);
CONV_FN(Select, select);

// ~~~~Numbered values to string~~~
// WARNING does not include space at the end

CONV_FN(Column, col) {
  ret += col.column();
}

// WARNING does not include space at the end
CONV_FN(Table, table) {
  ret += table.table();
}

CONV_FN(ExprColAlias, eca) {
  ExprToString(ret, eca.expr());
  if (eca.has_col_alias()) {
    ret += " AS ";
    ColumnToString(ret, eca.col_alias());
  }
}

// WARNING does not include space at the end
/*CONV_FN(Schema, schema) {
  if (schema.main()) {
    return "main";
  }
  if (schema.temp()) {
    return "temp";
  }
  std::string ret("Schema");
  ret += std::to_string(schema.schema() % kMaxSchemaNumber);
  return ret;
}*/

void ConvertToSqlString(std::string &ret, const std::string& s) {
  for (size_t i = 0; i < s.length() ; i++) {
    const char &c = s[i];

    switch(c) {
      case '\'':
        ret += "\\'";
        break;
      case '\\':
        ret += "\\\\";
        break;
      case '\b':
        ret += "\\b";
        break;
      case '\f':
        ret += "\\f";
        break;
      case '\r':
        ret += "\\r";
        break;
      case '\n':
        ret += "\\n";
        break;
      case '\t':
        ret += "\\t";
        break;
      case '\0':
        ret += "\\0";
        break;
      case '\a':
        ret += "\\a";
        break;
      case '\v':
        ret += "\\v";
        break;
      default: {
        if (c > static_cast<char>(31) && c < static_cast<char>(127)) {
          ret += c;
        } else {
          const uint8_t &x = static_cast<uint8_t>(c);

          ret += "\\x";
          ret += hex_digits[(x & 0xF0) >> 4];
          ret += hex_digits[x & 0x0F];
        }
      }
    }
  }
}

CONV_FN(ExprSchemaTable, st) {
  /*if (st.has_schema_name()) {
    ret += SchemaToString(st.schema_name());
    ret += ".";
  }*/
  TableToString(ret, st.table_name());
}


CONV_FN(TypeName, top);
CONV_FN(TopTypeName, top);

CONV_FN(JSONColumn, jcol) {
  ret += ".";
  if (jcol.has_json_col()) {
    ret += "^";
  }
  ColumnToString(ret, jcol.col());
  if (jcol.has_json_array()) {
    const uint32_t limit = (jcol.json_array() % 4) + 1;

    for (uint32_t j = 0; j < limit; j++) {
      ret += "[]";
    }
  }
}

CONV_FN(JSONColumns, jcols) {
  JSONColumnToString(ret, jcols.jcol());
  for (int i = 0; i < jcols.other_jcols_size(); i++) {
    JSONColumnToString(ret, jcols.other_jcols(i));
  }
  if (jcols.has_json_cast()) {
    ret += "::";
    TypeNameToString(ret, jcols.json_cast());
  } else if (jcols.has_json_reinterpret()) {
    ret += ".:`";
    TypeNameToString(ret, jcols.json_reinterpret());
    ret += "`";
  }
}

CONV_FN(FieldAccess, fa) {
  using FieldType = FieldAccess::NestedOneofCase;
  switch (fa.nested_oneof_case()) {
    case FieldType::kArrayIndex:
      ret += "[";
      ret += fa.array_index() < 0 ? "-" : "";
      ret += std::to_string(std::abs(fa.array_index()) % 10);
      ret += "]";
      break;
    case FieldType::kArrayExpr:
      ret += "[";
      ExprToString(ret, fa.array_expr());
      ret += "]";
      break;
    case FieldType::kArrayKey:
      ret += "['";
      ColumnToString(ret, fa.array_key());
      ret += "']";
      break;
    case FieldType::kTupleIndex:
      ret += ".";
      ret += std::to_string((fa.tuple_index() % 9) + 1);
      break;
    default:
      ret += "[1]";
  }
}

CONV_FN(ExprColumn, ec) {
  ColumnToString(ret, ec.col());
  if (ec.has_subcols()) {
    JSONColumnsToString(ret, ec.subcols());
  }
  if (ec.null()) {
    ret += ".null";
  }
}

CONV_FN(ExprSchemaTableColumn, stc) {
  /*if (stc.has_schema()) {
    ret += SchemaToString(stc.schema());
    ret += ".";
  }*/
  if (stc.has_table()) {
    TableToString(ret, stc.table());
    ret += ".";
  }
  ExprColumnToString(ret, stc.col());
}

CONV_FN(ExprList, me) {
  ExprToString(ret, me.expr());
  for (int i = 0; i < me.extra_exprs_size(); i++) {
    ret += ", ";
    ExprToString(ret, me.extra_exprs(i));
  }
}

// ~~~~Expression stuff~~~~
// WARNING does not include space
CONV_FN(NumericLiteral, nl) {
  if (nl.digits_size() > 0) {
    ret += digits[std::min<uint32_t>(UINT32_C(1), nl.digits(0) % 10)];
    for (int i = 1; i < nl.digits_size(); i++) {
      ret += digits[nl.digits(i) % 10];
    }
  }
  if (nl.decimal_point()) {
    if (nl.digits_size() == 0) {
      ret += "0";
    }
    ret += ".";
    if (nl.dec_digits_size() == 0) {
      ret += "0";
    } else {
      for (int i = 0; i < nl.dec_digits_size(); i++) {
        ret += digits[nl.dec_digits(i) % 10];
      }
    }
  }
  if (nl.exp_digits_size() > 0) {
    if (nl.digits_size() == 0 && !nl.decimal_point()) {
      ret += "1";
    }
    ret += "E";
    if (nl.negative_exp()) {
      ret += "-";
    }
    for (int i = 0; i < nl.exp_digits_size(); i++) {
      ret += digits[nl.exp_digits(i) % 10];
    }
  }
  if (nl.digits_size() == 0 && !nl.decimal_point() && nl.exp_digits_size() == 0) {
    ret += "1";
  }
}

static void BuildJson(std::string &ret, const int depth, const int width, std::mt19937 &gen);
static void AddJsonArray(std::string &ret, const int depth, const int width, std::mt19937 &gen);
static void AddJsonElement(std::string &ret, std::mt19937 &gen);

static void
AddJsonArray(std::string &ret, const int depth, const int width, std::mt19937 &gen) {
  std::uniform_int_distribution<int> jopt(1, 3);
  int nelems = 0, next_width = 0;

  if (width) {
    std::uniform_int_distribution<int> alen(1, width);
    nelems = alen(gen);
  }
  ret += "[";
  next_width = nelems;
  for (int j = 0 ; j < nelems ; j++) {
    if (j != 0) {
      ret += ",";
    }
    if (depth) {
      const int noption = jopt(gen);

      switch (noption) {
        case 1: //object
          BuildJson(ret, depth - 1, next_width, gen);
          break;
        case 2: //array
          AddJsonArray(ret, depth - 1, next_width, gen);
          break;
        case 3: //others
          AddJsonElement(ret, gen);
          break;
        default:
          assert(0);
      }
    } else {
      AddJsonElement(ret, gen);
    }
    next_width--;
  }
  ret += "]";
}

static void
AddJsonElement(std::string &ret, std::mt19937 &gen) {
  std::uniform_int_distribution<int> opts(1, 10);
  const int noption = opts(gen);

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
        ret += std::to_string(numbers(gen));
      } break;
      case 8:
      case 9:
      case 10: { //string
        std::uniform_int_distribution<int> slen(0, 200);
        std::uniform_int_distribution<uint8_t> chars(32, 127);
        const int nlen = slen(gen);

        ret += '"';
        for (int i = 0 ; i < nlen ; i++) {
          const uint8_t nchar = chars(gen);

          switch (nchar) {
            case 127:
              ret += "😂";
              break;
            case static_cast<int>('"'):
              ret += "\\\"";
              break;
            case static_cast<int>('\\'):
              ret += "\\\\";
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

static void
BuildJson(std::string &ret, const int depth, const int width, std::mt19937 &gen) {
  ret += "{";
  if (depth) {
    std::uniform_int_distribution<int> childd(1, 10);
    const int nchildren = childd(gen);

    for (int i = 0 ; i < nchildren ; i++) {
      std::uniform_int_distribution<int> copt(0, 5);
      std::uniform_int_distribution<int> jopt(1, 3);
      const int noption = jopt(gen);

      if (i != 0) {
        ret += ",";
      }
      ret += "\"c";
      ret += std::to_string(copt(gen));
      ret += "\":";
      switch (noption) {
        case 1: //object
          BuildJson(ret, depth - 1, width, gen);
          break;
        case 2: //array
          AddJsonArray(ret, depth - 1, width, gen);
          break;
        case 3: //others
          AddJsonElement(ret, gen);
          break;
      default:
          assert(0);
      }
    }
  }
  ret += "}";
}

// WARNING does not include space
CONV_FN(LiteralValue, lit_val) {
  using LitValType = LiteralValue::LitValOneofCase;
  switch (lit_val.lit_val_oneof_case()) {
    case LitValType::kIntLit:
      ret += std::to_string(lit_val.int_lit());
      break;
    case LitValType::kUintLit:
      ret += std::to_string(lit_val.uint_lit());
      break;
    case LitValType::kStringLit:
      ret += '\'';
      ConvertToSqlString(ret, lit_val.string_lit());
      ret += '\'';
      break;
    case LitValType::kNoQuoteStr:
      ret += lit_val.no_quote_str();
      break;
    case LitValType::kHexString: {
      const std::string &s = lit_val.hex_string();
      ret += "x'";
      for (size_t i = 0; i < s.length(); i++) {
          const uint8_t &x = static_cast<uint8_t>(s[i]);
          ret += hex_digits[(x & 0xF0) >> 4];
          ret += hex_digits[x & 0x0F];
      }
      ret += '\'';
      } break;
    case LitValType::kHeredoc:
      ret += "$heredoc$";
      ret += lit_val.heredoc();
      ret += "$heredoc$";
      break;
    case LitValType::kNumericLit:
      NumericLiteralToString(ret, lit_val.numeric_lit());
      break;
    case LitValType::kJsonValue:
    case LitValType::kJsonString: {
      const bool is_json_value = lit_val.lit_val_oneof_case() == LitValType::kJsonValue;
      std::mt19937 gen(is_json_value ? lit_val.json_value() : lit_val.json_string());
      std::uniform_int_distribution<int> dopt(1, 3), wopt(1, 3);

      ret += "$jstr$";
      BuildJson(ret, dopt(gen), wopt(gen), gen);
      ret += "$jstr$";
      if (is_json_value) {
        ret += "::JSON";
      }
    } break;
    case LitValType::kSpecialVal: {
      switch (lit_val.special_val()) {
        case SpecialVal::VAL_NULL:
          ret += "NULL";
          break;
        case SpecialVal::VAL_TRUE:
          ret += "TRUE";
          break;
        case SpecialVal::VAL_FALSE:
          ret += "FALSE";
          break;
        case SpecialVal::VAL_ZERO:
          ret += "0";
          break;
        case SpecialVal::VAL_ONE:
          ret += "1";
          break;
        case SpecialVal::VAL_MINUS_ONE:
          ret += "-1";
          break;
        case SpecialVal::VAL_EMPTY_STRING:
          ret += "''";
          break;
        case SpecialVal::VAL_EMPTY_ARRAY:
          ret += "[]";
          break;
        case SpecialVal::VAL_EMPTY_TUPLE:
          ret += "()";
          break;
        case SpecialVal::VAL_MINUS_ZERO_FP:
          ret += "-0.0";
          break;
        case SpecialVal::VAL_PLUS_ZERO_FP:
          ret += "+0.0";
          break;
        case SpecialVal::VAL_ZERO_FP:
          ret += "0.0";
          break;
        case SpecialVal::VAL_INF:
          ret += "inf";
          break;
        case SpecialVal::VAL_PLUS_INF:
          ret += "+inf";
          break;
        case SpecialVal::VAL_MINUS_INF:
          ret += "-inf";
          break;
        case SpecialVal::VAL_NAN:
          ret += "nan";
          break;
        case SpecialVal::VAL_HAPPY:
          ret += "'😂'";
          break;
        case SpecialVal::VAL_TEN_HAPPY:
          ret += "'😂😂😂😂😂😂😂😂😂😂'";
          break;
        case SpecialVal::MIN_INT32:
          ret += std::to_string(std::numeric_limits<int32_t>::min());
          break;
        case SpecialVal::MAX_INT32:
          ret += std::to_string(std::numeric_limits<int32_t>::max());
          break;
        case SpecialVal::MIN_INT64:
          ret += std::to_string(std::numeric_limits<int64_t>::min());
          break;
        case SpecialVal::MAX_INT64:
          ret += std::to_string(std::numeric_limits<int64_t>::max());
          break;
        case SpecialVal::VAL_NULL_CHAR:
          ret += "'\\0'";
          break;
        case SpecialVal::VAL_DEFAULT:
          ret += "DEFAULT";
          break;
        case SpecialVal::VAL_STAR:
          ret += "*";
          break;
      }
    } break;
    default:
      ret += "1";
  }
}

CONV_FN(UnaryExpr, uexpr) {
  switch (uexpr.unary_op()) {
    case UNOP_MINUS:
      ret += "-";
      break;
    case UNOP_PLUS:
      ret += "+";
      break;
    case UNOP_NOT:
      ret += "NOT ";
      break;
  }
  ExprToString(ret, uexpr.expr());
}

CONV_FN(BinaryOperator, bop) {
  switch (bop) {
    case BINOP_LE:
      ret += " < ";
      break;
    case BINOP_LEQ:
      ret += " <= ";
      break;
    case BINOP_GR:
      ret += " > ";
      break;
    case BINOP_GREQ:
      ret += " >= ";
      break;
    case BINOP_EQ:
      ret += " = ";
      break;
    case BINOP_EQEQ:
      ret += " == ";
      break;
    case BINOP_NOTEQ:
      ret += " != ";
      break;
    case BINOP_LEGR:
      ret += " <> ";
      break;
    case BINOP_AND:
      ret += " AND ";
      break;
    case BINOP_OR:
      ret += " OR ";
      break;
    case BINOP_CONCAT:
      ret += " || ";
      break;
    case BINOP_STAR:
      ret += " * ";
      break;
    case BINOP_SLASH:
      ret += " / ";
      break;
    case BINOP_PERCENT:
      ret += " % ";
      break;
    case BINOP_PLUS:
      ret += " + ";
      break;
    case BINOP_MINUS:
      ret += " - ";
      break;
  }
}

CONV_FN(BinaryExpr, bexpr) {
  ExprToString(ret, bexpr.lhs());
  BinaryOperatorToString(ret, bexpr.op());
  ExprToString(ret, bexpr.rhs());
}

CONV_FN(ParenthesesExpr, pexpr) {
  ret += "(";
  ExprColAliasToString(ret, pexpr.expr());
  for (int i = 0; i < pexpr.other_exprs_size(); i++) {
    ret += ", ";
    ExprColAliasToString(ret, pexpr.other_exprs(i));
  }
  ret += ")";
}

CONV_FN(LowCardinality, lc) {
  using LcardTypeNameType = LowCardinality::LcardOneOfCase;
  switch (lc.lcard_one_of_case()) {
    case LcardTypeNameType::kIntegers:
      ret += Integers_Name(lc.integers());
      break;
    case LcardTypeNameType::kFloats:
      ret += FloatingPoints_Name(lc.floats());
      break;
    case LcardTypeNameType::kSqlString:
      ret += "String";
      break;
    case LcardTypeNameType::kFixedString:
      ret += "FixedString(";
      ret += std::to_string(std::max<uint32_t>(1, lc.fixed_string()));
      ret += ")";
      break;
    case LcardTypeNameType::kDates:
      ret += Dates_Name(lc.dates());
      break;
    default:
      ret += "Int";
  }
}

CONV_FN(BottomTypeName, btn) {
  using BottomTypeNameType = BottomTypeName::BottomOneOfCase;
  switch (btn.bottom_one_of_case()) {
    case BottomTypeNameType::kIntegers:
      ret += Integers_Name(btn.integers());
      break;
    case BottomTypeNameType::kFloats:
      ret += FloatingPoints_Name(btn.floats());
      break;
    case BottomTypeNameType::kDecimal: {
      const sql_query_grammar::Decimal &dec = btn.decimal();
      ret += "Decimal";
      if (dec.has_precision()) {
        const uint32_t precision = (dec.precision() % 76) + 1;

        ret += "(";
        ret += std::to_string(precision);
        if (dec.has_scale()) {
          ret += ",";
          ret += std::to_string(dec.scale() % precision);
        }
        ret += ")";
      }
    } break;
    case BottomTypeNameType::kSqlString:
      ret += "String";
      break;
    case BottomTypeNameType::kFixedString:
      ret += "FixedString(";
      ret += std::to_string(std::max<uint32_t>(1, btn.fixed_string()));
      ret += ")";
      break;
    case BottomTypeNameType::kUuid:
      ret += "UUID";
      break;
    case BottomTypeNameType::kDates:
      ret += Dates_Name(btn.dates());
      break;
    case BottomTypeNameType::kJson: {
      const sql_query_grammar::JsonDef &jdef = btn.json();

      ret += "JSON";
      if (jdef.spec_size() > 0) {
        ret += "(";
        for (int i = 0 ; i < jdef.spec_size(); i++) {
          const sql_query_grammar::JsonDefItem &jspec = jdef.spec(i);

          if (i != 0) {
            ret += ",";
          }
          if (jspec.has_max_dynamic_types()) {
            ret += "max_dynamic_types=";
            ret += std::to_string(jspec.max_dynamic_types() % 255);
          } else if (jspec.has_column()) {
            const sql_query_grammar::JsonCol &jcol = jspec.column();

            ColumnToString(ret, jcol.col());
            for (int j = 0 ; j < jcol.cols_size(); j++) {
              ret += ".";
              ColumnToString(ret, jcol.cols(j));
            }
            ret += " ";
            TopTypeNameToString(ret, jcol.type());
          } else {
            ret += "max_dynamic_paths=";
            ret += std::to_string((jspec.has_max_dynamic_paths() ? jspec.max_dynamic_paths() : jspec.def_paths()) % 100);
          }
        }
        ret += ")";
      }
    } break;
    case BottomTypeNameType::kDynamic:
      ret += "Dynamic";
      if (btn.dynamic().has_ntypes()) {
        ret += "(max_types=";
        ret += std::to_string((btn.dynamic().ntypes() % 255) + 1);
        ret += ")";
      }
      break;
    case BottomTypeNameType::kLcard:
      LowCardinalityToString(ret, btn.lcard());
      break;
    default:
      ret += "Int";
  }
}

CONV_FN(TypeColumnDef, col_def) {
  ColumnToString(ret, col_def.col());
  ret += " ";
  TopTypeNameToString(ret, col_def.type_name());
}

CONV_FN(TopTypeName, ttn) {
  using TopTypeNameType = TopTypeName::TypeOneofCase;
  switch (ttn.type_oneof_case()) {
    case TopTypeNameType::kNonNullable:
      BottomTypeNameToString(ret, ttn.non_nullable());
      break;
    case TopTypeNameType::kNullable:
      ret += "Nullable(";
      BottomTypeNameToString(ret, ttn.nullable());
      ret += ")";
      break;
    case TopTypeNameType::kArray:
      ret += "Array(";
      TopTypeNameToString(ret, ttn.array());
      ret += ")";
      break;
    case TopTypeNameType::kMap:
      ret += "Map(";
      TopTypeNameToString(ret, ttn.map().key());
      ret += ",";
      TopTypeNameToString(ret, ttn.map().value());
      ret += ")";
      break;
    case TopTypeNameType::kTuple:
      ret += "Tuple(";
      TypeColumnDefToString(ret, ttn.tuple().value1());
      ret += ",";
      TypeColumnDefToString(ret, ttn.tuple().value2());
      for (int i = 0 ; i < ttn.tuple().others_size(); i++) {
        ret += ",";
        TypeColumnDefToString(ret, ttn.tuple().others(i));
      }
      ret += ")";
      break;
    case TopTypeNameType::kNested:
      ret += "Nested(";
      TypeColumnDefToString(ret, ttn.nested().type1());
      for (int i = 0 ; i < ttn.nested().others_size(); i++) {
        ret += ", ";
        TypeColumnDefToString(ret, ttn.nested().others(i));
      }
      ret += ")";
      break;
    case TopTypeNameType::kVariant:
      ret += "Variant(";
      TopTypeNameToString(ret, ttn.variant().value1());
      ret += ",";
      TopTypeNameToString(ret, ttn.variant().value2());
      for (int i = 0 ; i < ttn.variant().others_size(); i++) {
        ret += ",";
        TopTypeNameToString(ret, ttn.variant().others(i));
      }
      ret += ")";
      break;
    default:
      ret += "Int";
  }
}

CONV_FN(TypeName, tp) {
  TopTypeNameToString(ret, tp.type());
}

CONV_FN(CastExpr, cexpr) {
  ret += "CAST(";
  ExprToString(ret, cexpr.expr());
  ret += ", '";
  TypeNameToString(ret, cexpr.type_name());
  ret += "')";
}

CONV_FN(ExprLike, e) {
  ExprToString(ret, e.expr1());
  ret += " ";
  if (e.not_())
    ret += "NOT ";
  ret += "LIKE ";
  ExprToString(ret, e.expr2());
}

CONV_FN(CondExpr, e) {
  ret += "(";
  ExprToString(ret, e.expr1());
  ret += " ? ";
  ExprToString(ret, e.expr2());
  ret += " : ";
  ExprToString(ret, e.expr3());
  ret += ")";
}

CONV_FN(ExprNullTests, e) {
  ExprToString(ret, e.expr());
  ret += " IS";
  ret += e.not_() ? " NOT" : "";
  ret += " NULL";
}

CONV_FN(ExprBetween, e) {
  ExprToString(ret, e.expr1());
  ret += " ";
  if (e.not_())
    ret += "NOT ";
  ret += "BETWEEN ";
  ExprToString(ret, e.expr2());
  ret += " AND ";
  ExprToString(ret, e.expr3());
}

CONV_FN(ExprIn, e) {
  const sql_query_grammar::ExprList &elist = e.expr();

  if (elist.extra_exprs_size() == 0) {
    ExprToString(ret, elist.expr());
  } else {
    ret += "(";
    ExprListToString(ret, e.expr());
    ret += ")";
  }
  ret += " ";
  if (e.global())
    ret += "GLOBAL ";
  if (e.not_())
    ret += "NOT ";
  ret += "IN (";
  if (e.has_exprs()) {
    ExprListToString(ret, e.exprs());
  } else if (e.has_sel()) {
    SelectToString(ret, e.sel());
  } else {
    ret += "1";
  }
  ret += ")";
}

CONV_FN(ExprAny, ea) {
  ExprToString(ret, ea.expr());
  BinaryOperatorToString(ret, static_cast<sql_query_grammar::BinaryOperator>(((static_cast<int>(ea.op()) % 8) + 1)));
  ret += ea.anyall() ? "ALL" : "ANY";
  ret += "(";
  SelectToString(ret, ea.sel());
  ret += ")";
}

CONV_FN(ExprExists, e) {
  if (e.not_())
    ret += "NOT ";
  ret += "EXISTS (";
  SelectToString(ret, e.select());
  ret += ")";
}

// WARNING no space at end
CONV_FN(ExprWhenThen, e) {
  ret += "WHEN ";
  ExprToString(ret, e.when_expr());
  ret += " THEN ";
  ExprToString(ret, e.then_expr());
}

CONV_FN(ExprCase, e) {
  ret += "CASE ";
  if (e.has_expr()) {
    ExprToString(ret, e.expr());
    ret += " ";
  }
  ExprWhenThenToString(ret, e.when_then());
  ret += " ";
  for (int i = 0; i < e.extra_when_thens_size(); i++) {
    ExprWhenThenToString(ret, e.extra_when_thens(i));
    ret += " ";
  }
  if (e.has_else_expr()) {
    ret += "ELSE ";
    ExprToString(ret, e.else_expr());
    ret += " ";
  }
  ret += "END";
}

CONV_FN(SQLFuncCall, e) {
  ret += SQLFunc_Name(e.func()).substr(4);
  if (e.params_size()) {
    ret += '(';
    for (int i = 0 ; i < e.params_size(); i++) {
      if (i != 0) {
        ret += ",";
      }
      ExprToString(ret, e.params(i));
    }
    ret += ')';
  }
  ret += '(';
  if (e.distinct()) {
    ret += "DISTINCT ";
  }
  for (int i = 0 ; i < e.args_size(); i++) {
    if (i != 0) {
      ret += ",";
    }
    ExprToString(ret, e.args(i));
  }
  ret += ')';
  if (e.respect_nulls()) {
    ret += " RESPECT NULLS";
  }
}

CONV_FN(ComplicatedExpr, expr) {
  using ExprType = ComplicatedExpr::ComplicatedExprOneofCase;
  switch (expr.complicated_expr_oneof_case()) {
    case ExprType::kExprStc:
      ExprSchemaTableColumnToString(ret, expr.expr_stc());
      break;
    case ExprType::kUnaryExpr:
      UnaryExprToString(ret, expr.unary_expr());
      break;
    case ExprType::kBinaryExpr:
      BinaryExprToString(ret, expr.binary_expr());
      break;
    case ExprType::kParExpr:
      ParenthesesExprToString(ret, expr.par_expr());
      break;
    case ExprType::kCastExpr:
      CastExprToString(ret, expr.cast_expr());
      break;
    case ExprType::kExprBetween:
      ExprBetweenToString(ret, expr.expr_between());
      break;
    case ExprType::kExprIn:
      ExprInToString(ret, expr.expr_in());
      break;
    case ExprType::kExprAny:
      ExprAnyToString(ret, expr.expr_any());
      break;
    case ExprType::kExprNullTests:
      ExprNullTestsToString(ret, expr.expr_null_tests());
      break;
    case ExprType::kExprCase:
      ExprCaseToString(ret, expr.expr_case());
      break;
    case ExprType::kExprExists:
      ExprExistsToString(ret, expr.expr_exists());
      break;
    case ExprType::kExprLike:
      ExprLikeToString(ret, expr.expr_like());
      break;
    case ExprType::kExprCond:
      CondExprToString(ret, expr.expr_cond());
      break;
    case ExprType::kSubquery:
      ret += "(";
      SelectToString(ret, expr.subquery());
      ret += ")";
      break;
    case ExprType::kFuncCall:
      SQLFuncCallToString(ret, expr.func_call());
      break;
    case ExprType::kArray: {
      const sql_query_grammar::ArraySequence &vals = expr.array();

      ret += "[";
      for (int i = 0 ; i < vals.values_size(); i++) {
        if (i != 0) {
          ret += ",";
        }
        ExprToString(ret, vals.values(i));
      }
      ret += "]";
    } break;
    case ExprType::kTuple: {
      const sql_query_grammar::TupleSequence &vals = expr.tuple();

      ret += "(";
      for (int i = 0 ; i < vals.values_size(); i++) {
        if (i != 0) {
          ret += ",";
        }
        ret += "(";
        ExprListToString(ret, vals.values(i));
        ret += ")";
      }
      ret += ")";
    } break;
    default:
      ret += "1";
  }
}

CONV_FN(Expr, expr) {
  if (expr.has_lit_val()) {
    LiteralValueToString(ret, expr.lit_val());
  } else if (expr.has_comp_expr()) {
    ComplicatedExprToString(ret, expr.comp_expr());
  } else {  // default
    ret += "1";
  }
  if (expr.has_field()) {
    FieldAccessToString(ret, expr.field());
  }
}

// ~~~~SELECT~~~~

// WARNING no space at end
CONV_FN(ResultColumn, rc) {
  if (rc.has_etc()) {
    ExprSchemaTableColumnToString(ret, rc.etc());
  } else if (rc.has_eca()) {
    ExprColAliasToString(ret, rc.eca());
  } else if (rc.has_table_star()) {
    TableToString(ret, rc.table_star());
    ret += ".*";
  } else {
    ret += "*";
  }
}

CONV_FN(JoinedQuery, tos);
CONV_FN(TableOrSubquery, tos);

CONV_FN(ExprColumnList, cl) {
  ExprColumnToString(ret, cl.col());
  for (int i = 0; i < cl.extra_cols_size(); i++) {
    ret += ", ";
    ExprColumnToString(ret, cl.extra_cols(i));
  }
}

CONV_FN(JoinConstraint, jc) {
  // oneof
  if (jc.has_on_expr()) {
    ret += " ON ";
    ExprToString(ret, jc.on_expr());
  } else if (jc.has_using_expr()) {
    ret += " USING (";
    ExprColumnListToString(ret, jc.using_expr().col_list());
    ret += ")";
  } else {
    ret += " ON TRUE";
  }
}

CONV_FN(JoinCore, jcc) {
  const bool has_cross_or_paste = jcc.join_op() > sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_FULL;

  if (jcc.global()) {
    ret += " GLOBAL";
  }
  if (jcc.join_op() != sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_INNER ||
      !jcc.has_join_const() ||
      jcc.join_const() < sql_query_grammar::JoinCore_JoinConst::JoinCore_JoinConst_SEMI) {
    ret += " ";
    ret += JoinCore_JoinType_Name(jcc.join_op());
  }
  if (!has_cross_or_paste && jcc.has_join_const()) {
    ret += " ";
    ret += JoinCore_JoinConst_Name(jcc.join_const());
  }
  ret += " JOIN ";
  TableOrSubqueryToString(ret, jcc.tos());
  if (!has_cross_or_paste) {
    JoinConstraintToString(ret, jcc.join_constraint());
  }
}

CONV_FN(ArrayJoin, aj) {
  if (aj.left()) {
    ret += " LEFT";
  }
  ret += " ARRAY JOIN ";
  ExprColAliasToString(ret, aj.constraint());
}

CONV_FN(JoinClauseCore, jcc) {
  if (jcc.has_core()) {
    JoinCoreToString(ret, jcc.core());
  } else if (jcc.has_arr()) {
    ArrayJoinToString(ret, jcc.arr());
  }
}

CONV_FN(JoinClause, jc) {
  TableOrSubqueryToString(ret, jc.tos());
  for (int i = 0; i < jc.clauses_size(); i++) {
    JoinClauseCoreToString(ret, jc.clauses(i));
  }
}

CONV_FN(JoinedDerivedQuery, tos) {
  ret += "(";
  SelectToString(ret, tos.select());
  ret += ") ";
  TableToString(ret, tos.table_alias());
}

CONV_FN(JoinedTable, jt) {
  ExprSchemaTableToString(ret, jt.est());
  if (jt.has_table_alias()) {
    ret += " ";
    TableToString(ret, jt.table_alias());
  }
  if (jt.final()) {
    ret += " FINAL";
  }
}

CONV_FN(TableOrSubquery, tos) {
  // oneof
  if (tos.has_jt()) {
    JoinedTableToString(ret, tos.jt());
  } else if (tos.has_jq()) {
    JoinedQueryToString(ret, tos.jq());
  } else if (tos.has_jdq()) {
    JoinedDerivedQueryToString(ret, tos.jdq());
  } else {
    ret += "(SELECT 1 c0) t0";
  }
}

CONV_FN(JoinedQuery, tos) {
  if (tos.tos_list_size() > 0) {
   TableOrSubqueryToString(ret, tos.tos_list(0));
    for (int i = 1; i < tos.tos_list_size(); i++) {
      ret += ", ";
      TableOrSubqueryToString(ret, tos.tos_list(i));
    }
  } else {
    JoinClauseToString(ret, tos.join_clause());
  }
}

CONV_FN(FromStatement, fs) {
  ret += "FROM ";
  JoinedQueryToString(ret, fs.tos());
}

CONV_FN(ColumnComparison, cc) {
  ExprSchemaTableColumnToString(ret, cc.col());
  BinaryOperatorToString(ret, cc.op());
  ExprToString(ret, cc.expr());
}

CONV_FN(ExprComparisonHighProbability, echp) {
  if (echp.has_cc()) {
    ColumnComparisonToString(ret, echp.cc());
  } else if (echp.has_expr()) {
    ExprToString(ret, echp.expr());
  }
}

CONV_FN(WhereStatement, ws) {
  ExprComparisonHighProbabilityToString(ret, ws.expr());
}

CONV_FN(GroupByList, gbl) {
  ExprListToString(ret, gbl.exprs());
  if (gbl.has_gs()) {
    ret += " WITH ";
    ret += GroupByList_GroupingSets_Name(gbl.gs());
  }
  if (gbl.with_totals()) {
    ret += " WITH TOTALS";
  }
}


CONV_FN(GroupByStatement, gbs) {
  ret += "GROUP BY ";
  if (gbs.has_glist()) {
    GroupByListToString(ret, gbs.glist());
  } else {
    ret += "ALL";
  }
  if (gbs.has_having_expr()) {
    ret += " HAVING ";
    ExprToString(ret, gbs.having_expr());
  }
}

CONV_FN(ExprOrderingTerm, eot) {
  ExprToString(ret, eot.expr());
  if (eot.has_asc_desc()) {
    ret += " ";
    ret += ExprOrderingTerm_AscDesc_Name(eot.asc_desc());
  }
}

CONV_FN(OrderByStatement, obs) {
  ret += "ORDER BY ";
  ExprOrderingTermToString(ret, obs.ord_term());
  for (int i = 0; i < obs.extra_ord_terms_size(); i++) {
    ret += ", ";
    ExprOrderingTermToString(ret, obs.extra_ord_terms(i));
  }
}

CONV_FN(LimitStatement, ls) {
  ret += "LIMIT ";
  ret += std::to_string(ls.limit());
  if (ls.has_offset()) {
    ret += ", ";
    ret += std::to_string(ls.offset());
  }
  if (ls.with_ties()) {
    ret += " WITH TIES";
  }
  if (ls.has_limit_by()) {
    ret += " BY ";
    ExprToString(ret, ls.limit_by());
  }
}

CONV_FN(SelectStatementCore, ssc) {
  ret += "SELECT ";
  ret += AllOrDistinct_Name(ssc.s_or_d());
  ret += " ";
  if (ssc.result_columns_size() == 0) {
    ret += "*";
  } else {
    ResultColumnToString(ret, ssc.result_columns(0));
    for (int i = 1; i < ssc.result_columns_size(); i++) {
      ret += ", ";
      ResultColumnToString(ret, ssc.result_columns(i));
    }
  }
  if (ssc.has_from()) {
    ret += " ";
    FromStatementToString(ret, ssc.from());
  }
  if (ssc.has_pre_where()) {
    ret += " PREWHERE ";
    WhereStatementToString(ret, ssc.pre_where());
  }
  if (ssc.has_where()) {
    ret += " WHERE ";
    WhereStatementToString(ret, ssc.where());
  }
  if (ssc.has_groupby()) {
    ret += " ";
    GroupByStatementToString(ret, ssc.groupby());
  }
  /*if (ssc.has_window()) {
    ret += WindowStatementToString(ssc.window());
    ret += " ";
  }*/
  if (ssc.has_orderby()) {
    ret += " ";
    OrderByStatementToString(ret, ssc.orderby());
  }
  if (ssc.has_limit()) {
    ret += " ";
    LimitStatementToString(ret, ssc.limit());
  }
}

CONV_FN(SetQuery, setq) {
  ret += "(";
  SelectToString(ret, setq.sel1());
  ret += ") ";
  ret += SetQuery_SetOp_Name(setq.set_op());
  ret += " ";
  ret += AllOrDistinct_Name(setq.s_or_d());
  ret += " (";
  SelectToString(ret, setq.sel2());
  ret += ")";
}

CONV_FN(Select, select) {
  /*if (select.has_with()) {
    ret += WithStatementToString(select.with());
    ret += " ";
  }*/
  if (select.has_select_core()) {
    SelectStatementCoreToString(ret, select.select_core());
  } else if (select.has_set_query()) {
    SetQueryToString(ret, select.set_query());
  } else {
    ret += "1";
  }
}

CONV_FN(ColumnDef, cdf) {
  ColumnToString(ret, cdf.col());
  ret += " ";
  TypeNameToString(ret, cdf.type());
  if (cdf.has_nullable()) {
    ret += cdf.nullable() ? "" : " NOT";
    ret += " NULL";
  }
}

CONV_FN(ColumnsDef, ct) {
  ColumnDefToString(ret, ct.col_def());
  for (int i = 0 ; i < ct.other_col_defs_size(); i++) {
    ret += ", ";
    ColumnDefToString(ret, ct.other_col_defs(i));
  }
}

CONV_FN(TableKey, to) {
  if (to.exprs_size() == 0) {
    ret += "tuple()";
  } else {
    ret += "(";
    for (int i = 0 ; i < to.exprs_size(); i++) {
      if (i != 0) {
        ret += ", ";
      }
      ExprToString(ret, to.exprs(i));
    }
    ret += ")";
  }
}

CONV_FN(TableEngine, te) {
  ret += " ENGINE = ";
  ret += TableEngineValues_Name(te.engine());
  ret += "(";
  for (int i = 0 ; i < te.cols_size(); i++) {
    if (i != 0) {
      ret += ", ";
    }
    ColumnToString(ret, te.cols(i));
  }
  ret += ")";
}

CONV_FN(CreateTableDef, create_table) {
  ret += "(";
  ColumnsDefToString(ret, create_table.def());
  ret += ")";
  TableEngineToString(ret, create_table.engine());
}

CONV_FN(TableLike, table_like) {
  ret += "AS ";
  ExprSchemaTableToString(ret, table_like.est());
  if (table_like.has_engine()) {
    TableEngineToString(ret, table_like.engine());
  }
}

CONV_FN(CreateTable, create_table) {
  ret += "CREATE ";
  if (create_table.is_temp()) {
    ret += "TEMPORARY ";
  }
  ret += "TABLE ";
  if (create_table.if_not_exists()) {
    ret += "IF NOT EXISTS ";
  }
  ExprSchemaTableToString(ret, create_table.est());
  ret += " ";
  if (create_table.has_table_def()) {
    CreateTableDefToString(ret, create_table.table_def());
  } else if (create_table.has_table_like()) {
    TableLikeToString(ret, create_table.table_like());
  }
  if (create_table.has_order()) {
    ret += " ORDER BY ";
    TableKeyToString(ret, create_table.order());
  }
  if (create_table.has_partition_by()) {
    ret += " PARTITION BY ";
    TableKeyToString(ret, create_table.partition_by());
  }
  if (create_table.has_primary_key()) {
    ret += " PRIMARY KEY ";
    TableKeyToString(ret, create_table.primary_key());
  }
  if (create_table.allow_nullable()) {
    ret += " SETTINGS allow_nullable_key = 1";
  }
  if (create_table.has_table_def() && create_table.has_as_select_stmt()) {
    ret += " AS (";
    SelectToString(ret, create_table.as_select_stmt());
    ret += ")";
  }
}

CONV_FN(DropTable, d) {
  ret += "DROP TABLE ";
  if (d.if_exists())
    ret += "IF EXISTS ";
  if (d.if_empty())
    ret += "IF EMPTY ";
  ExprSchemaTableToString(ret, d.est());
  for (int i = 0; i < d.other_tables_size(); i++) {
    ret += ", ";
    ExprSchemaTableToString(ret, d.other_tables(i));
  }
}

CONV_FN(ValuesStatement, values) {
  ret += "VALUES (";
  ExprListToString(ret, values.expr_list());
  ret += ")";
  for (int i = 0; i < values.extra_expr_lists_size(); i++) {
    ret += ", (";
    ExprListToString(ret, values.extra_expr_lists(i));
    ret += ")";
  }
}

CONV_FN(Insert, insert) {
  ret += "INSERT INTO ";
  ExprSchemaTableToString(ret, insert.est());
  if (insert.cols_size()) {
    ret += " (";
    for (int i = 0; i < insert.cols_size(); i++) {
      if (i != 0) {
        ret += ", ";
      }
      ColumnToString(ret, insert.cols(i));
    }
    ret += ")";
  }
  ret += " ";
  if (insert.has_values()) {
    ValuesStatementToString(ret, insert.values());
  } else if (insert.has_select()) {
    ret += "SELECT * FROM (";
    SelectToString(ret, insert.select());
    ret += ")";
  } else if (insert.has_query()) {
    ret += "VALUES ";
    ret += insert.query();
  } else {
    ret += "VALUES (0)";
  }
}

CONV_FN(Delete, del) {
  ret += "DELETE FROM ";
  ExprSchemaTableToString(ret, del.est());
  ret += " WHERE ";
  WhereStatementToString(ret, del.where());
}

CONV_FN(Truncate, trunc) {
  ret += "TRUNCATE ";
  if (trunc.has_est()) {
    ExprSchemaTableToString(ret, trunc.est());
  } else { //don't truncate schema at the moment
    ExprSchemaTableToString(ret, trunc.def_est());
  }
}

CONV_FN(CheckTable, ct) {
  ret += "CHECK TABLE ";
  ExprSchemaTableToString(ret, ct.est());
  if (ct.has_single_result()) {
    ret += " SETTINGS check_query_single_value_result = ";
    ret += ct.single_result() ? "1" : "0";
  }
}

CONV_FN(DescTable, dt) {
  ret += "DESCRIBE TABLE ";
  ExprSchemaTableToString(ret, dt.est());
  if (dt.has_sub_cols()) {
    ret += " SETTINGS describe_include_subcolumns = ";
    ret += dt.sub_cols() ? "1" : "0";
  }
}

CONV_FN(DeduplicateExpr, de) {
  ret += " DEDUPLICATE";
  if (de.has_col_list()) {
    ret += " BY ";
    ExprColumnListToString(ret, de.col_list());
  }
}

CONV_FN(OptimizeTable, ot) {
  ret += "OPTIMIZE TABLE ";
  ExprSchemaTableToString(ret, ot.est());
  if (ot.final()) {
    ret += " FINAL";
  }
  if (ot.has_dedup()) {
    DeduplicateExprToString(ret, ot.dedup());
  }
}

CONV_FN(ExchangeTables, et) {
  ret += "EXCHANGE TABLES ";
  ExprSchemaTableToString(ret, et.est1());
  ret += " AND ";
  ExprSchemaTableToString(ret, et.est2());
}

CONV_FN(UpdateSet, upt) {
  ColumnToString(ret, upt.col());
  ret += " = ";
  ExprToString(ret, upt.expr());
}

CONV_FN(Update, upt) {
  UpdateSetToString(ret, upt.update());
  for (int i = 0; i < upt.other_updates_size(); i++) {
    ret += ", ";
    UpdateSetToString(ret, upt.other_updates(i));
  }
  ret += " WHERE ";
  WhereStatementToString(ret, upt.where());
}

CONV_FN(AddColumn, add) {
  ColumnDefToString(ret, add.new_col());
  if (add.has_after()) {
    ret += " AFTER ";
    ColumnToString(ret, add.after());
  } else if (add.has_first()) {
    ret += " FIRST";
  }
}

CONV_FN(AlterTable, alter) {
  ret += "ALTER TABLE ";
  ExprSchemaTableToString(ret, alter.est());
  ret += " ";
  using AlterType = AlterTable::AlterOneofCase;
  switch (alter.alter_oneof_case()) {
    case AlterType::kDel:
      ret += "DELETE WHERE ";
      WhereStatementToString(ret, alter.del());
      break;
    case AlterType::kUpdate:
      ret += "UPDATE ";
      UpdateToString(ret, alter.update());
      break;
    case AlterType::kOrder:
      ret += "MODIFY ORDER BY ";
      TableKeyToString(ret, alter.order());
      break;
    case AlterType::kMaterializeColumn:
      ret += "MATERIALIZE COLUMN ";
      ColumnToString(ret, alter.materialize_column());
      break;
    case AlterType::kAddColumn:
      ret += "ADD COLUMN ";
      AddColumnToString(ret, alter.add_column());
      break;
    case AlterType::kDropColumn:
      ret += "DROP COLUMN ";
      ColumnToString(ret, alter.drop_column());
      break;
    case AlterType::kRenameColumn:
      ret += "RENAME COLUMN ";
      ColumnToString(ret, alter.rename_column().old_name());
      ret += " TO ";
      ColumnToString(ret, alter.rename_column().new_name());
      break;
    case AlterType::kModifyColumn:
      ret += "MODIFY COLUMN ";
      AddColumnToString(ret, alter.modify_column());
      break;
    case AlterType::kDeleteMask:
      ret += " APPLY DELETED MASK";
      break;
    default:
      ret += " DELETE WHERE TRUE";
  }
}

CONV_FN(TopSelect, top) {
  SelectToString(ret, top.sel());
  if (top.has_format()) {
    ret += " FORMAT ";
    ret += OutFormat_Name(top.format()).substr(4);
  }
}

CONV_FN(SQLQueryInner, query) {
  using QueryType = SQLQueryInner::QueryInnerOneofCase;
  switch (query.query_inner_oneof_case()) {
    case QueryType::kSelect:
      TopSelectToString(ret, query.select());
      break;
    case QueryType::kCreateTable:
      CreateTableToString(ret, query.create_table());
      break;
    case QueryType::kDropTable:
      DropTableToString(ret, query.drop_table());
      break;
    case QueryType::kInsert:
      InsertToString(ret, query.insert());
      break;
    case QueryType::kDel:
      DeleteToString(ret, query.del());
      break;
    case QueryType::kTrunc:
      TruncateToString(ret, query.trunc());
      break;
    case QueryType::kOpt:
      OptimizeTableToString(ret, query.opt());
      break;
    case QueryType::kCheck:
      CheckTableToString(ret, query.check());
      break;
    case QueryType::kDesc:
      DescTableToString(ret, query.desc());
      break;
    case QueryType::kExchange:
      ExchangeTablesToString(ret, query.exchange());
      break;
    case QueryType::kAlterTable:
      AlterTableToString(ret, query.alter_table());
      break;
    default:
      TopSelectToString(ret, query.def_select());
  }
}

CONV_FN(ExplainQuery, explain) {
  ret += "EXPLAIN";
  if (explain.has_expl()) {
    ret += " ";
    ret += ExplainQuery_ExplainValues_Name(explain.expl());
    std::replace(ret.begin(), ret.end(), '_', ' ');
  }
  if (explain.has_expl() && explain.expl() <= sql_query_grammar::ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE) {
    for (int i = 0; i < explain.opts_size(); i++) {
      std::string ostr = "";
      const sql_query_grammar::ExplainOption &eopt = explain.opts(i);

      if (i != 0) {
        ret += ",";
      }
      ret += " ";
      switch (explain.expl()) {
        case sql_query_grammar::ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PLAN:
          ostr += ExpPlanOpt_Name(static_cast<sql_query_grammar::ExpPlanOpt>((eopt.opt() % 5) + 1));
          break;
        case sql_query_grammar::ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PIPELINE:
          ostr += ExpPipelineOpt_Name(static_cast<sql_query_grammar::ExpPipelineOpt>((eopt.opt() % 3) + 1));
          break;
        case sql_query_grammar::ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE:
          ostr += ExpQueryTreeOpt_Name(static_cast<sql_query_grammar::ExpQueryTreeOpt>((eopt.opt() % 3) + 1));
          break;
        default:
          assert(0);
      }
      ostr = ostr.substr(5);
      std::transform(ostr.begin(), ostr.end(), ostr.begin(), [](unsigned char c){ return std::tolower(c); });
      ret += ostr;
      ret += " = ";
      ret += eopt.val() ? "1" : "0";
    }
  }
  ret += " ";
  SQLQueryInnerToString(ret, explain.inner_query());
}

// ~~~~QUERY~~~~
CONV_FN(SQLQuery, query) {
  using QueryType = SQLQuery::QueryOneofCase;
  switch (query.query_oneof_case()) {
    case QueryType::kInnerQuery:
      SQLQueryInnerToString(ret, query.inner_query());
      break;
    case QueryType::kExplain:
      ExplainQueryToString(ret, query.explain());
      break;
    case QueryType::kStartTrans:
      ret += "BEGIN TRANSACTION";
      break;
    case QueryType::kCommitTrans:
      ret += "COMMIT";
      break;
    case QueryType::kRollbackTrans:
      ret += "ROLLBACK";
      break;
    default:
      SQLQueryInnerToString(ret, query.def_query());
  }
  ret += ";";
}

}
