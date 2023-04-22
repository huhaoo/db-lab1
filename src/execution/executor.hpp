#ifndef SAKURA_EXECUTOR_H__
#define SAKURA_EXECUTOR_H__

#include <numeric>

#include "plan/plan.hpp"
#include "execution/exprdata.hpp"
#include "catalog/db.hpp"
#include "parser/expr.hpp"
#include "storage/storage.hpp"

#define print_log printf("Running on line %d at file \"%s\"\n",__LINE__,__FILE__),fflush(stdout)

namespace wing {

/**
 * Init(): Only allocate memory and set some flags, don't evaluate expressions or read/write tuples.
 * Next(): Do operations for each tuple. Return invalid result if it has completed.
 * Clear(): Release the unused memory. E.g. JoinExecutor after returning results.
 *
 * The first Next() returns the first tuple. The i-th Next() returns the i-th tuple.
 * It is illegal to invoke Next() after Next() returns invalid result.
 * Ensure that Init is invoked only once before executing.
 *
 * You should ensure that the InputTuplePtr is valid until Next() is invoked again.
 */
class Executor {
 public:
  virtual ~Executor() = default;
  virtual void Init() = 0;
  virtual InputTuplePtr Next() = 0;
};

class ExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(const PlanNode* plan, DB& db, size_t txn_id);

 private:
};


class NestloopJoinExecutor:public Executor
{
 public:
  NestloopJoinExecutor(const std::unique_ptr<Expr>& expr, const OutputSchema& In1, const OutputSchema& In2, const OutputSchema &Out, std::unique_ptr<Executor> ch1,std::unique_ptr<Executor> ch2)
      : predicate_(JoinExprFunction(expr.get(),In1,In2)), is1(In1), is2(In2), os(Out), c1(std::move(ch1)), c2(std::move(ch2))
      {
        v1=new StaticFieldRef[is1.Size()];
        v2=new StaticFieldRef[is2.Size()];
        out=new StaticFieldRef[os.Size()];
      }
  ~NestloopJoinExecutor()override{ delete[] v1; delete[] v2; delete[] out; }
  void Init()override{ print_log; c1->Init(); c2->Init(); started=false; }
  InputTuplePtr Next()override
  {
    print_log;
    if(!started){ started=true; V2=Next2(); }
    while(true)
    {
      if(!V2) return InputTuplePtr();
      if(!Next1()){ c1->Init(); V2=Next2(); continue; }
      if(predicate_&&predicate_.Evaluate(v1,v2).ReadInt()==0) continue;
      merge(); return out;
    }
  }
private:
  JoinExprFunction predicate_;
  const OutputSchema& is1,is2,os;
  std::unique_ptr<Executor> c1,c2;
  bool started;
  StaticFieldRef *v1,*v2,*out;
  bool V2;

  bool Next1()
  {
    InputTuplePtr r=c1->Next();
    if(is1.IsRaw()){ if(!r) return false; Tuple::DeSerialize(v1,r.Data(),is1.GetCols()); }
    else{ if(!r) return false; memcpy(v1,r.Data(),sizeof(StaticFieldRef*)*is1.Size()); }
    return true;
  }
  bool Next2()
  {
    InputTuplePtr r=c2->Next();
    if(is2.IsRaw()){ if(!r) return false; Tuple::DeSerialize(v2,r.Data(),is2.GetCols()); }
    else{ if(!r) return false; memcpy(v2,r.Data(),sizeof(StaticFieldRef*)*is2.Size()); }
    return true;
  }
  void merge(){ memcpy(out,v1,sizeof(StaticFieldRef)*is1.Size()); memcpy(out+is1.Size(),v2,sizeof(StaticFieldRef)*is2.Size()); }
};

}  // namespace wing

#endif