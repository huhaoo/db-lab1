#include "execution/executor.hpp"

#include "catalog/schema.hpp"
#include "execution/delete_executor.hpp"
#include "execution/filter_executor.hpp"
#include "execution/insert_executor.hpp"
#include "execution/print_executor.hpp"
#include "execution/project_executor.hpp"
#include "execution/seqscan_executor.hpp"
#include"functions/functions.hpp"
#include <iostream>

#define print_log printf("Running on line %d at file \"%s\"\n",__LINE__,__FILE__),fflush(stdout)

namespace wing {

std::unique_ptr<Executor> ExecutorGenerator::Generate(
    const PlanNode* plan, DB& db, size_t txn_id) {
  if (plan == nullptr) {
    throw DBException("Invalid PlanNode.");
  }

  else if (plan->type_ == PlanType::Project) {
    auto project_plan = static_cast<const ProjectPlanNode*>(plan);
    return std::make_unique<ProjectExecutor>(project_plan->output_exprs_,
        project_plan->ch_->output_schema_,
        Generate(project_plan->ch_.get(), db, txn_id));
  }

  else if (plan->type_ == PlanType::Filter) {
    auto filter_plan = static_cast<const FilterPlanNode*>(plan);
    return std::make_unique<FilterExecutor>(filter_plan->predicate_.GenExpr(),
        filter_plan->ch_->output_schema_,
        Generate(filter_plan->ch_.get(), db, txn_id));
  }

  else if (plan->type_ == PlanType::Print) {
    auto print_plan = static_cast<const PrintPlanNode*>(plan);
    return std::make_unique<PrintExecutor>(
        print_plan->values_, print_plan->num_fields_per_tuple_);
  }

  else if (plan->type_ == PlanType::Insert) {
    auto insert_plan = static_cast<const InsertPlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(insert_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", insert_plan->table_name_);
    }
    auto& tab = db.GetDBSchema()[table_schema_index.value()];
    auto gen_pk = tab.GetAutoGenFlag()
                      ? db.GetGenPKHandle(txn_id, tab.GetName())
                      : nullptr;
    return std::make_unique<InsertExecutor>(
        db.GetModifyHandle(txn_id, insert_plan->table_name_),
        Generate(insert_plan->ch_.get(), db, txn_id),
        FKChecker(tab.GetFK(), tab, txn_id, db), gen_pk, tab);
  }

  else if (plan->type_ == PlanType::SeqScan) {
    auto seqscan_plan = static_cast<const SeqScanPlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(seqscan_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", seqscan_plan->table_name_);
    }
    return std::make_unique<SeqScanExecutor>(
        db.GetIterator(txn_id, seqscan_plan->table_name_),
        seqscan_plan->predicate_.GenExpr(), seqscan_plan->output_schema_);
  }

  else if (plan->type_ == PlanType::RangeScan) {
    auto rangescan_plan = static_cast<const RangeScanPlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(rangescan_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", rangescan_plan->table_name_);
    }
    return std::make_unique<SeqScanExecutor>(
        db.GetRangeIterator(txn_id, rangescan_plan->table_name_,convert_bound_from_pair_to_tuple(rangescan_plan->range_l_),convert_bound_from_pair_to_tuple(rangescan_plan->range_r_)),
        rangescan_plan->predicate_.GenExpr(), rangescan_plan->output_schema_);
  }

  else if (plan->type_ == PlanType::Delete) {
    auto delete_plan = static_cast<const DeletePlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(delete_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", delete_plan->table_name_);
    }
    auto& tab = db.GetDBSchema()[table_schema_index.value()];
    return std::make_unique<DeleteExecutor>(
        db.GetModifyHandle(txn_id, delete_plan->table_name_),
        Generate(delete_plan->ch_.get(), db, txn_id),
        FKChecker(tab.GetFK(), tab, txn_id, db),
        PKChecker(tab.GetName(), tab.GetHidePKFlag(), txn_id, db), tab);
  }

  else if (plan->type_ == PlanType::Join) {
    auto join_plan = static_cast<const JoinPlanNode*>(plan);
    // std::cout<<join_plan->ToString()<<std::endl;
    return std::make_unique<NestloopJoinExecutor>(join_plan->predicate_.GenExpr(), join_plan->ch_->output_schema_, join_plan->ch2_->output_schema_,join_plan->output_schema_,
                                                  Generate(join_plan->ch_.get(), db, txn_id), Generate(join_plan->ch2_.get(), db, txn_id));
  }
  else if (plan->type_ == PlanType::HashJoin) {
    auto join_plan = static_cast<const HashJoinPlanNode*>(plan);
    //  std::cout<<join_plan->ToString()<<std::endl;
    // return std::make_unique<NestloopJoinExecutor>(join_plan->predicate_.GenExpr(), join_plan->ch_->output_schema_, join_plan->ch2_->output_schema_,join_plan->output_schema_,
    //                                           Generate(join_plan->ch_.get(), db, txn_id), Generate(join_plan->ch2_.get(), db, txn_id));
    return std::make_unique<HashJoinExecutor>(join_plan->predicate_.GenExpr(), join_plan->ch_->output_schema_, join_plan->ch2_->output_schema_,join_plan->output_schema_,
                                              Generate(join_plan->ch_.get(), db, txn_id), Generate(join_plan->ch2_.get(), db, txn_id),
                                              join_plan->left_hash_exprs_,join_plan->right_hash_exprs_);
  }

  else if (plan->type_ == PlanType::Aggregate) {
    auto aggregate_plan = static_cast<const AggregatePlanNode*>(plan);
    // std::cout<<aggregate_plan->ToString()<<std::endl;
    return std::make_unique<HashAggregateExecutor>(aggregate_plan->group_predicate_.GenExpr(),aggregate_plan->ch_->output_schema_,aggregate_plan->output_schema_,
                                                   Generate(aggregate_plan->ch_.get(), db, txn_id),aggregate_plan->output_exprs_,aggregate_plan->group_by_exprs_);
  }

  else if (plan->type_ == PlanType::Order) {
    auto order_plan = static_cast<const OrderByPlanNode*>(plan);
    // std::cout<<order_plan->ToString()<<std::endl;
    return std::make_unique<OrderExecutor>(order_plan->ch_->output_schema_,order_plan->output_schema_,Generate(order_plan->ch_.get(),db,txn_id),order_plan->order_by_exprs_);
  }
  else if (plan->type_ == PlanType::Limit) {
    auto limit_plan = static_cast<const LimitPlanNode*>(plan);
    // std::cout<<limit_plan->ToString()<<std::endl;
    return std::make_unique<LimitExecutor>(Generate(limit_plan->ch_.get(),db,txn_id),limit_plan->offset_,limit_plan->limit_size_);
  }
  else if (plan->type_ == PlanType::Distinct) {
    auto distinct_plan = static_cast<const DistinctPlanNode*>(plan);
    return std::make_unique<DistinctExecutor>(Generate(distinct_plan->ch_.get(),db,txn_id),distinct_plan->output_schema_);
  }
  
  throw DBException("Unsupported plan node.");
}

}  // namespace wing