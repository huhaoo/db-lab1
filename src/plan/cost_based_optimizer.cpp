#include <queue>

#include "plan/optimizer.hpp"
#include "rules/convert_to_hash_join.hpp"
#include "plan/card_est.hpp"
#include "plan/cost_model.hpp"

#include <iostream>
#define print_log printf("Running on line %d at file \"%s\"\n",__LINE__,__FILE__),fflush(stdout)

namespace wing {

std::unique_ptr<PlanNode> Apply(std::unique_ptr<PlanNode> plan,
    const std::vector<std::unique_ptr<OptRule>>& rules, const DB& db) {
  for (auto& a : rules) {
    if (a->Match(plan.get())) {
      plan = a->Transform(std::move(plan));
      break;
    }
  }
  if (plan->ch2_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
    plan->ch2_ = Apply(std::move(plan->ch2_), rules, db);
  } else if (plan->ch_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
  }
  return plan;
}

size_t GetTableNum(const PlanNode* plan) {
  /* We don't want to consider values clause in cost based optimizer. */
  if (plan->type_ == PlanType::Print) {
    return 10000;
  }
  
  if (plan->type_ == PlanType::SeqScan) {
    return 1;
  }

  size_t ret = 0;
  if (plan->ch2_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
    ret += GetTableNum(plan->ch2_.get());
  } else if (plan->ch_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
  }
  return ret;
}

bool CheckIsAllJoin(const PlanNode* plan) {
  if (plan->type_ == PlanType::Print || plan->type_ == PlanType::SeqScan || plan->type_ == PlanType::RangeScan) {
    return true;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckIsAllJoin(plan->ch_.get()) && CheckIsAllJoin(plan->ch2_.get());
}

bool CheckHasStat(const PlanNode* plan, const DB& db) {
  if (plan->type_ == PlanType::Print) {
    return false;
  }
  if (plan->type_ == PlanType::SeqScan) {
    auto stat = db.GetTableStat(static_cast<const SeqScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ == PlanType::RangeScan) {
    auto stat = db.GetTableStat(static_cast<const RangeScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckHasStat(plan->ch_.get(), db) && CheckHasStat(plan->ch2_.get(), db);
}

/** 
 * Check whether we can use cost based optimizer. 
 * For simplicity, we only use cost based optimizer when:
 * (1) The root plan node is Project, and there is only one Project.
 * (2) The other plan nodes can only be Join or SeqScan or RangeScan.
 * (3) The number of tables is <= 10. 
 * (4) All tables have statistics.
*/
bool CheckCondition(const PlanNode* plan, const DB& db) {
  if (GetTableNum(plan) > 10) return false;
  if (plan->type_ != PlanType::Project && plan->type_ != PlanType::Aggregate) return false;
  if (!CheckIsAllJoin(plan->ch_.get())) return false;
  return CheckHasStat(plan->ch_.get(), db);
}

std::unique_ptr<PlanNode> CostBasedOptimizer::Optimize(
    std::unique_ptr<PlanNode> plan, DB& db) {
  if (CheckCondition(plan.get(), db)) {
    return CostBasedOptimizer_(std::move(plan),db).solve();
  } else {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    plan = Apply(std::move(plan), R, db);
    return plan;
  }
}

void CostBasedOptimizer_::find_scan_node(PlanNode* p)
{
  pred.Append(p->type_==PlanType::Join     ?std::move(((JoinPlanNode*)p)     ->predicate_):
              p->type_==PlanType::SeqScan  ?std::move(((SeqScanPlanNode*)p)  ->predicate_):
              p->type_==PlanType::RangeScan?std::move(((RangeScanPlanNode*)p)->predicate_):
                                            PredicateVec());
  if(p->type_==PlanType::Join)
  {
    find_scan_node(p->ch_.get());
    find_scan_node(p->ch2_.get());
  }
  else if(p->type_==PlanType::SeqScan||p->type_==PlanType::RangeScan)
  {
    scan.push_back(p); n++;
  }
  else
  {
    assert(p->ch_&&!p->ch2_);
    find_scan_node(p->ch_.get());
  }
}
void CostBasedOptimizer_::dp()
{
  for(int i=0;i<n;i++)
  {
    int I=(1<<n)-1-i; PredicateVec _pred; for(auto &t:pred.GetVec()) if(!t.CheckLeft(bitvec[I])&&!t.CheckRight(bitvec[I])) _pred.Append({PredicateVec::_trans(t.expr_->clone()), t.left_bits_, t.right_bits_});

    bitvec[1<<i]=scan[i]->table_bitset_; plan_[1<<i]=scan[i]->clone();
    summary[1<<i]=CardEstimator::EstimateTable(scan[i]->type_==PlanType::SeqScan?((SeqScanPlanNode*)scan[i])->table_name_:((RangeScanPlanNode*)scan[i])->table_name_,_pred,scan[i]->output_schema_,db);
    cost[1<<i]=CostCalculator::SeqScanCost(summary[1<<i].size_);
  }
  for(int i=0;i<(1<<n);i++) if(i&(i-1)) bitvec[i]=bitvec[i&(i-1)]|bitvec[i-(i&(i-1))];
  for(int i=0;i<(1<<n);i++) if(i&(i-1))
  {
    int J=0; double C=-1;
    int I=(1<<n)-1-i; PredicateVec _pred; for(auto &t:pred.GetVec()) if(!t.CheckLeft(bitvec[I])&&!t.CheckRight(bitvec[I])) _pred.Append({PredicateVec::_trans(t.expr_->clone()), t.left_bits_, t.right_bits_});
    for(int j=i&(i-1);j;j=i&(j-1))
    {
      double _C=cost[j]+cost[i-j]+(ConvertToHashJoinRule().Check(plan_[j].get(),plan_[i-j].get(),pred)?CostCalculator::HashJoinCost:CostCalculator::NestloopJoinCost)(summary[j].size_,summary[i-j].size_);
      if(C>_C||C<0){ C=_C; J=j; }
    }
    split_point[i]=J; cost[i]=C; summary[i]=CardEstimator::EstimateJoinEq(_pred,summary[J],summary[i-J]);
    JoinPlanNode *p=new JoinPlanNode;
    p->ch_=plan_[J]->clone(); p->ch2_=plan_[i-J]->clone(); p->table_bitset_=bitvec[i];
    p->output_schema_.Append(p->ch_->output_schema_); p->output_schema_.Append(p->ch2_->output_schema_);
    plan_[i]=std::unique_ptr<PlanNode>((PlanNode*)p);
  }
}
std::unique_ptr<PlanNode> CostBasedOptimizer_::solve()
{
  if(n) return plan->clone();
  assert(CheckCondition(plan.get(),db));
  find_scan_node(plan.get());
  // std::cout<<pred.ToString()<<std::endl;
  while(true)
  {
    for(const auto &i:pred.GetVec()) if(i.expr_->op_==OpType::EQ&&i.expr_->ch0_->type_==ExprType::COLUMN&&i.expr_->ch1_->type_==ExprType::COLUMN)
    {
      ColumnExpr *a=(ColumnExpr*)(i.expr_->ch0_.get()),*b=(ColumnExpr *)(i.expr_->ch1_.get()); 
      for(const auto &j:pred.GetVec()) if(j.expr_->op_==OpType::EQ&&j.expr_->ch0_->type_==ExprType::COLUMN&&j.expr_->ch1_->type_==ExprType::COLUMN)
      {
        ColumnExpr *c=(ColumnExpr*)(j.expr_->ch0_.get()),*d=(ColumnExpr *)(j.expr_->ch1_.get());
        if(a->id_in_column_name_table_==d->id_in_column_name_table_){ std::swap(a,b); std::swap(c,d); }
        else if(a->id_in_column_name_table_==c->id_in_column_name_table_) std::swap(a,b);
        else if(b->id_in_column_name_table_==d->id_in_column_name_table_) std::swap(c,d);
        if(b->id_in_column_name_table_!=c->id_in_column_name_table_||a->id_in_column_name_table_==d->id_in_column_name_table_) continue;
        int flag=0;
        for(const auto &k:pred.GetVec()) if(k.expr_->op_==OpType::EQ&&k.expr_->ch0_->type_==ExprType::COLUMN&&k.expr_->ch1_->type_==ExprType::COLUMN)
        {
          ColumnExpr *e=(ColumnExpr*)(k.expr_->ch0_.get()),*f=(ColumnExpr *)(k.expr_->ch1_.get());
          if(a->id_in_column_name_table_==f->id_in_column_name_table_) std::swap(e,f);
          if(a->id_in_column_name_table_!=e->id_in_column_name_table_||d->id_in_column_name_table_!=f->id_in_column_name_table_) continue;
          flag=1;
        }
        if(!flag)
        {
          BinaryConditionExpr expr(OpType::EQ,a->clone(),d->clone());
          pred.Append(PredicateVec::Create(&expr));
          goto label1;
        }
      }
    }
    break;
    label1:;
  }
  // std::cout<<pred.ToString()<<std::endl;
  bitvec.resize(1<<n); plan_.resize(1<<n); cost.resize(1<<n); summary.resize(1<<n); split_point.resize(1<<n);
  dp();
  plan->ch_=std::move(plan_[(1<<n)-1]->clone());
  (plan->ch_->type_==PlanType::Join     ?((JoinPlanNode*)     (plan->ch_).get())->predicate_:
   plan->ch_->type_==PlanType::SeqScan  ?((SeqScanPlanNode*)  (plan->ch_).get())->predicate_:
                                         ((RangeScanPlanNode*)(plan->ch_).get())->predicate_)=pred.clone();
  // logical_optimize
  plan=LogicalOptimizer::Optimize(std::move(plan),db);
  std::vector<std::unique_ptr<OptRule>> R;
  R.push_back(std::make_unique<ConvertToHashJoinRule>());
  plan = Apply(std::move(plan), R, db);
  // print((1<<n)-1,0);
  // std::cout<<plan->ToString()<<std::endl<<std::endl;

  return plan->clone();
}

void CostBasedOptimizer_::print(int S,int prefix)
{
  printf("%s%.6lf\n",std::string(prefix,' ').c_str(),summary[S].size_);
  if(!split_point[S]) return;
  print(split_point[S],prefix+4);
  print(S-split_point[S],prefix+4);
}

}  // namespace wing
