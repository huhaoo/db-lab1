#ifndef WING_FUNCTIONS
#define WING_FUNCTIONS
#include"type/field.hpp"
#include"parser/expr.hpp"
#include"catalog/db.hpp"
namespace wing
{
	static Field convert_expr_to_field(Expr *s)
	{
		if(s->type_==ExprType::LITERAL_FLOAT){ LiteralFloatExpr *S=(LiteralFloatExpr*) s; return Field::CreateFloat(FieldType::FLOAT64,8,S->literal_value_); }
		if(s->type_==ExprType::LITERAL_INTEGER){ LiteralIntegerExpr *S=(LiteralIntegerExpr*) s; return Field::CreateInt(FieldType::INT64,8,S->literal_value_); }
		if(s->type_==ExprType::LITERAL_STRING){ LiteralStringExpr *S=(LiteralStringExpr*) s; return Field::CreateString(FieldType::VARCHAR,S->literal_value_); }
		assert(0);
	}
	static std::string get_pk_from_table_name(const DB &db,const std::string &table_name)
	{
		const DBSchema& dbs=db.GetDBSchema();
		auto id=dbs.Find(table_name); assert(id.has_value());
		return dbs[id.value()].GetPrimaryKeySchema().name_;
	}
	static std::tuple<std::string_view, bool, bool> convert_bound_from_pair_to_tuple(std::pair<Field,bool> p)
	{
		if(p.first.type_==FieldType::EMPTY) return {std::string_view(),false,false};
		return {p.first.GetView(),true,p.second};
	}
}
#endif