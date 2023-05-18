#ifndef WING_USE_SCAN_AS_HASH_TABLE_HPP
#define WING_USE_SCAN_AS_HASH_TABLE_HPP
#include "plan/plan.hpp"
#include "plan/rules/rule.hpp"
#define print_log printf("Running on line %d at file \"%s\"\n",__LINE__,__FILE__),fflush(stdout)
namespace wing
{
class UseScanAsHashTable : public OptRule
{
public:
	UseScanAsHashTable(){ }
	bool Match(const PlanNode* node) override
	{
		return node->type_==PlanType::HashJoin&&!(node->ch_->type_==PlanType::SeqScan||node->ch_->type_==PlanType::RangeScan)&&(node->ch2_->type_==PlanType::SeqScan||node->ch2_->type_==PlanType::RangeScan);
	}
	std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node) override
	{
		HashJoinPlanNode *h=(HashJoinPlanNode*)node.get();
		std::swap(h->ch_,h->ch2_); std::swap(h->left_hash_exprs_,h->right_hash_exprs_);
		return node;
	}
};
}
#endif