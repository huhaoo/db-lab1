#ifndef SAKURA_OPTIMIZER_H__
#define SAKURA_OPTIMIZER_H__

#include "catalog/db.hpp"
#include "plan/plan.hpp"
#include "plan/rules/rule.hpp"
#include "plan/card_est.hpp"

namespace wing {

class LogicalOptimizer {
 public:
  // Apply some rules to plan.
  static std::unique_ptr<PlanNode> Apply(std::unique_ptr<PlanNode> plan,
      const std::vector<std::unique_ptr<OptRule>>& rules);
  // Optimize the plan using logical rules.
  static std::unique_ptr<PlanNode> Optimize(
      std::unique_ptr<PlanNode> plan, DB& db);
};

class CostBasedOptimizer {
 public:
  // Optimize the plan using logical-to-physical rules and join reordering
  // rules.
  static std::unique_ptr<PlanNode> Optimize(
      std::unique_ptr<PlanNode> plan, DB& db);
};

class CostBasedOptimizer_ {
 public:
  // Optimize the plan using logical-to-physical rules and join reordering
  // rules.
  CostBasedOptimizer_(std::unique_ptr<PlanNode> _plan, DB& _db):plan(std::move(_plan)),db(_db),n(0){}
  std::unique_ptr<PlanNode> solve();
 private:
  void find_scan_node(PlanNode*);
  void dp();
  void print(int,int);
  std::unique_ptr<PlanNode> plan; DB& db;
  int n; // number of scan node;
  std::vector<PlanNode*> scan; // the scan nodes
  PredicateVec pred; // the predicate vector
  // These variance are for a subset of all scan nodes
  std::vector<BitVector> bitvec; // the bit_vector
  std::vector<std::unique_ptr<PlanNode> > plan_; // The best plan
  std::vector<double> cost; // the estimate cost of the plan of S
  std::vector<CardEstimator::Summary> summary; // the estimate info
  std::vector<int> split_point; // the estimate info
};

}  // namespace wing

#endif