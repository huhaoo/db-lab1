#ifndef WING_FUNCTIONS
#define WING_FUNCTIONS
#include"type/field.hpp"
#include"parser/expr.hpp"
#include"catalog/db.hpp"
#include"type/static_field.hpp"
#include"catalog/schema.hpp"
#include<string>
#include<vector>
#include<iostream>
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
		if(p.first.type_==FieldType::EMPTY) return {std::string_view(),true,false};
		return {p.first.GetView(),false,p.second};
	}
	static uint64_t uint64_hash(uint64_t w)
	{
		w = (w+0xfd7046c5) + (w<<3);
		w = (w+0xfd7046c5) + (w>>3);
		w = (w^0xb55a4f09) ^ (w<<16);
		w = (w^0xb55a4f09) ^ (w>>16);
		return w;
	}
	static std::vector<Field> convert_to_field_vector(const StaticFieldRef* ref,const TableSchema& s)
	{
		std::vector<Field> ret;
		for(size_t i=0;i<s.Size();i++)
		{
			switch(s[i].type_)
			{
			case FieldType::INT32:
			case FieldType::INT64:
				ret.push_back(Field::CreateInt(s[i].type_,s[i].type_==FieldType::INT32?4:8,ref[i].ReadInt()));
				break;
			case FieldType::FLOAT64:
				ret.push_back(Field::CreateFloat(s[i].type_,8,ref[i].ReadFloat()));
				break;
			case FieldType::CHAR:
			case FieldType::VARCHAR:
				ret.push_back(Field::CreateString(s[i].type_,ref[i].ReadStringView()));
				break;
			default:
				ret.push_back(Field());
				break;
			}
		}
		return ret;
	}
	static Field convert_expr_to_field(const Expr* e)
	{
		switch (e->type_)
		{
		case ExprType::LITERAL_FLOAT:
			return Field::CreateFloat(FieldType::FLOAT64,8,((const LiteralFloatExpr*)e)->literal_value_);
		case ExprType::LITERAL_INTEGER:
			return Field::CreateInt(FieldType::INT64,8,((const LiteralIntegerExpr*)e)->literal_value_);
		case ExprType::LITERAL_STRING:
			return Field::CreateString(FieldType::CHAR,((const LiteralStringExpr*)e)->literal_value_);
		default:
			assert(0);
		}
	}
	class timer
	{
	private:
		time_t t;
	public:
		timer():t(0){}
		inline void start(){ t-=clock(); }
		inline void end(){ t+=clock(); }
		void clear(bool output=false){ if(output) printf("The sum of time between starts and ends are: %.6lfs\n",(double)t/CLOCKS_PER_SEC); t=0; }
		~timer(){ clear(true); }
	};
	static timer timer_;
}
#endif