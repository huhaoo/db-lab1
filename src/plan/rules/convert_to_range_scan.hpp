#ifndef WING_RANGE_SCAN_HPP
#define WING_RANGE_SCAN_HPP
#include "plan/rules/rule.hpp"
#include "catalog/db.hpp"
#include "functions/functions.hpp"
namespace wing
{
class ConvertToRangeScanRule : public OptRule
{
public:
	ConvertToRangeScanRule(DB& _db):db(_db){ }
	bool Match(const PlanNode* node) override
	{
		if(node->type_==PlanType::SeqScan)
		{
			return true;
			SeqScanPlanNode *Node=(SeqScanPlanNode*)node;
			const std::string &table_name=Node->table_name_;
			auto r=get_bound(Node->predicate_,get_pk_from_table_name(db,table_name));
			if(r.first.first.type_!=FieldType::EMPTY||r.second.first.type_!=FieldType::EMPTY) return true;
		}
		return false;
	}
	std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node) override
	{
		// printf("Apply convert to range scan rule.\n");
		SeqScanPlanNode *Node=(SeqScanPlanNode*)(node.get());
		const std::string &table_name=Node->table_name_;
		auto r=get_bound(Node->predicate_,get_pk_from_table_name(db,table_name));
		RangeScanPlanNode* Rs=new RangeScanPlanNode;
		std::unique_ptr<PlanNode> rs((PlanNode*)Rs);
		Rs->output_schema_=std::move(Node->output_schema_);
		Rs->predicate_=std::move(Node->predicate_);
		Rs->table_bitset_=std::move(Node->table_bitset_);
		Rs->table_name_=std::move(Node->table_name_);
		Rs->range_l_=r.first; Rs->range_r_=r.second;
		return rs;
	}
private:
	DB& db;
	typedef std::pair<Field,bool> bound;
	static std::pair<bound,bound> get_bound(const PredicateVec &predicate,const std::string &column_name)
	{
		std::pair<bound,bound> r;
		for(const auto &i:predicate.GetVec())
		{
			Expr *s1=i.expr_->ch0_.get(),*s2=i.expr_->ch1_.get();
			if(s1->type_==ExprType::COLUMN&&(s2->type_==ExprType::LITERAL_FLOAT||s2->type_==ExprType::LITERAL_INTEGER||s2->type_==ExprType::LITERAL_STRING)&&((ColumnExpr*)s1)->column_name_==column_name)
			{
				Field val=convert_expr_to_field(s2);
				if(i.expr_->op_==OpType::EQ) r={{val,1},{val,1}};
				if(i.expr_->op_==OpType::GEQ&&(r.first.first.type_==FieldType::EMPTY||r.first.first<=val)) r.first={val,1};
				if(i.expr_->op_==OpType::GT&&(r.first.first.type_==FieldType::EMPTY||r.first.first<val)) r.first={val,0};
				if(i.expr_->op_==OpType::LEQ&&(r.second.first.type_==FieldType::EMPTY||r.second.first>=val)) r.second={val,1};
				if(i.expr_->op_==OpType::LT&&(r.second.first.type_==FieldType::EMPTY||r.second.first>val)) r.second={val,0};
			}
			// The symmety one seems never appears.
		}
		return r;
	}
};
}
#endif