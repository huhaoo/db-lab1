#ifndef SAKURA_COST_MODEL_H__
#define SAKURA_COST_MODEL_H__

namespace wing {

class CostCalculator{
 public:
  static double HashJoinCost(double build_size, double probe_size) {
    return build_size*2+probe_size;
  }

  /* Calculate the cost of nestloop join. */
  static double NestloopJoinCost(double build_size, double probe_size) {
    return build_size*probe_size;
  }
  
  /* Calculate the cost of sequential scan. */
  static double SeqScanCost(double size) {
    return size;
  }
};

}

#endif