#ifndef SAKURA_SEQSCAN_EXECUTOR_H__
#define SAKURA_SEQSCAN_EXECUTOR_H__

#include "execution/executor.hpp"
#include"functions/functions.hpp"
#include <iostream>

namespace wing {

class SeqScanExecutor : public Executor {
 public:
  SeqScanExecutor(std::unique_ptr<Iterator<const uint8_t*>> iter,
      const std::unique_ptr<Expr>& predicate, const OutputSchema& input_schema)
    : iter_(std::move(iter)), predicate_(predicate.get(), input_schema) {}
  void Init() override { iter_->Init(); }
  InputTuplePtr Next() override {
    auto result = iter_->Next();
    while (
        result && (predicate_ && predicate_.Evaluate(result).ReadInt() == 0)) {
      result = iter_->Next();
    }
    if (result) {
      output_size++;
      return result;
    } else {
      // if(output_size){ printf("Scan finish, output size= %lu\n",output_size); fflush(stdout); output_size=0; }
      return {};
    }
  }

 private:
  size_t output_size=0;
  std::unique_ptr<Iterator<const uint8_t*>> iter_;
  ExprFunction predicate_;
};

}  // namespace wing

#endif