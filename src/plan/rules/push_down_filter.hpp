#ifndef SAKURA_PUSHDOWN_FILTER_H__
#define SAKURA_PUSHDOWN_FILTER_H__

#include "common/logging.hpp"
#include "plan/expr_utils.hpp"
#include "plan/output_schema.hpp"
#include "plan/rules/rule.hpp"
#include <iostream>

namespace wing {

/**
 * FilterPlanNodes are generated on the top of Subqueries, Tables and Joins.
 * For example,
 * select * from (select * from A where A.b = 1) where A.a = 1;
 *               Filter [A.a = 1]
 *                     |
 *                  Project
 *                     |
 *               Filter [A.b = 1]
 *                     |
 *              SeqScan [Table: A]
 * We should swap Project and the first Filter, and combine the two Filters.
 * Another example,
 * select * from (select sum(a) as c from A) where c < 100;
 *               Filter [c < 100]
 *                     |
 *         Aggregate [Group predicate: NULL]
 *                     |
 *                 SeqScan [Table: A]
 * We should put the filter predicate into the group predicate of the aggregate
 * node.
 *
 * Filters can be swapped with Project, OrderBy, Distinct, SeqScan.
 * Filters should be combined with other Filters. This is done by this OptLRule.
 * Filters can be combined with the predicate in Aggregate and Join.
 * Filters cannot be swapped with Limit.
 */
static bool is_query_plan(const PlanNode* node)
{
	return node->type_ == PlanType::Project ||
	       node->type_ == PlanType::Aggregate ||
	       node->type_ == PlanType::Order ||
	       node->type_ == PlanType::Distinct ||
	       node->type_ == PlanType::Filter ||
	       node->type_ == PlanType::Join ||
	       node->type_ == PlanType::SeqScan || 
	       node->type_ == PlanType::HashJoin || 
	       node->type_ == PlanType::RangeScan;
}
static bool is_query_plan(const std::unique_ptr<PlanNode> &node){ return is_query_plan(node.get()); }
class PushDownFilterRule : public OptRule {
 public:
	bool Match(const PlanNode* node) override {
		if (node->type_ == PlanType::Filter) {
			auto t_node = static_cast<const FilterPlanNode*>(node);
			// t_node->ch_ should be non-null.
			if (is_query_plan(t_node->ch_)) return true;
		}
		return false;
	}
	std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node) override {
		auto t_node = static_cast<FilterPlanNode*>(node.get());
		if (t_node->ch_->type_ == PlanType::Distinct ||
				t_node->ch_->type_ == PlanType::Order) {
			auto ch = std::move(t_node->ch_);
			t_node->ch_ = std::move(ch->ch_);
			ch->ch_ = std::move(node);
			return ch;
		} else if (t_node->ch_->type_ == PlanType::Filter) {
			auto ch = std::move(t_node->ch_);
			auto t_ch = static_cast<FilterPlanNode*>(ch.get());
			t_ch->predicate_.Append(std::move(t_node->predicate_));
			return ch;
		} else if (t_node->ch_->type_ == PlanType::Project) {
			auto proj = std::move(t_node->ch_);
			auto t_proj = static_cast<ProjectPlanNode*>(proj.get());
			t_node->predicate_.ApplyExpr(
					t_proj->output_exprs_, t_proj->output_schema_);
			t_node->ch_ = std::move(t_proj->ch_);
			t_proj->ch_ = std::move(node);
			return proj;
		} else if (t_node->ch_->type_ == PlanType::Join) {
			auto A = static_cast<FilterPlanNode*>(node.get());
			auto B = static_cast<JoinPlanNode*>(A->ch_.get());
			B->predicate_.Append(std::move(A->predicate_));
			return std::move(A->ch_);
		} else if (t_node->ch_->type_ == PlanType::Aggregate) {
			auto agg = std::move(t_node->ch_);
			auto t_agg = static_cast<AggregatePlanNode*>(agg.get());
			t_node->predicate_.ApplyExpr(t_agg->output_exprs_, t_agg->output_schema_);
			t_agg->group_predicate_.Append(std::move(t_node->predicate_));
			return agg;
		} else if (t_node->ch_->type_ == PlanType::SeqScan) {
			auto seq = std::move(t_node->ch_);
			auto t_seq = static_cast<SeqScanPlanNode*>(seq.get());
			t_seq->predicate_.Append(std::move(t_node->predicate_));
			return seq;
		} else if (t_node->ch_->type_ == PlanType::RangeScan) {
			auto rseq = std::move(t_node->ch_);
			auto t_rseq = static_cast<RangeScanPlanNode*>(rseq.get());
			t_rseq->predicate_.Append(std::move(t_node->predicate_));
			return rseq;
		} else if (t_node->ch_->type_ == PlanType::HashJoin) {
			auto A = static_cast<FilterPlanNode*>(node.get());
			auto B = static_cast<HashJoinPlanNode*>(A->ch_.get());
			B->predicate_.Append(std::move(A->predicate_));
			return std::move(A->ch_);
		} 
		DB_ERR("Invalid node.");
	}
};
class PushdownJoinPredicateRule:public OptRule
{
public:
	bool Match(const PlanNode* node) override
	{
		if(node->type_==PlanType::Join||node->type_==PlanType::HashJoin)
		{
			const PredicateVec &p=(node->type_==PlanType::Join?((const JoinPlanNode*)node)->predicate_:((const HashJoinPlanNode*)node)->predicate_);
//			std::cout<<node->ToString()<<std::endl;
			for(const auto &i:p.GetVec()) if((!i.CheckLeft(node->ch_ ->table_bitset_)&&!i.CheckRight(node->ch_ ->table_bitset_))
			                               ||(!i.CheckLeft(node->ch2_->table_bitset_)&&!i.CheckRight(node->ch2_->table_bitset_)))
				return true; // Check if there any expr only about one child
		}
		return false;
	}
	std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node_unique_ptr) override
	{
		// std::cout<<node_unique_ptr->ToString()<<std::endl;
		PlanNode* node=node_unique_ptr.get();
		PredicateVec &p_ref=(node->type_==PlanType::Join?((JoinPlanNode*)node)->predicate_:((HashJoinPlanNode*)node)->predicate_);
		PredicateVec p_temp; p_temp.swap(p_ref);
		PredicateVec f,s1,s2;
		for(auto &i:p_temp.GetVec())
		{
			bool is1=(i.CheckLeft(node->ch_->table_bitset_)||i.CheckRight(node->ch_->table_bitset_)),is2=(i.CheckLeft(node->ch2_->table_bitset_)||i.CheckRight(node->ch2_->table_bitset_));
			if(!is1) s2.Append(std::move(i));
			else if(!is2) s1.Append(std::move(i));
			else f.Append(std::move(i));
		}
		// printf("Apply pushdown rule. sizeof: f(%d) s1(%d) s2(%d)\n",f.GetVec().size(),s1.GetVec().size(),s2.GetVec().size());
		p_ref.swap(f);
		if(!s1.GetVec().empty())
		{
			FilterPlanNode *Filter=new FilterPlanNode;
			std::unique_ptr<PlanNode> filter((PlanNode*)Filter);
			Filter->predicate_.swap(s1);
			Filter->output_schema_=node->ch_->output_schema_;
			Filter->table_bitset_=node->ch_->table_bitset_;
			filter->ch_=std::move(node->ch_); node->ch_=std::move(filter);
		}
		if(!s2.GetVec().empty())
		{
			FilterPlanNode *Filter=new FilterPlanNode;
			std::unique_ptr<PlanNode> filter((PlanNode*)Filter);
			Filter->predicate_.swap(s2);
			Filter->output_schema_=node->ch2_->output_schema_;
			Filter->table_bitset_=node->ch2_->table_bitset_;
			filter->ch_=std::move(node->ch2_); node->ch2_=std::move(filter);
		}
		return node_unique_ptr;
	}
};
}  // namespace wing

#endif