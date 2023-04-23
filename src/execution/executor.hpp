#ifndef SAKURA_EXECUTOR_H__
#define SAKURA_EXECUTOR_H__

#include <numeric>

#include "plan/plan.hpp"
#include "execution/exprdata.hpp"
#include "catalog/db.hpp"
#include "parser/expr.hpp"
#include "storage/storage.hpp"
#include "common/murmurhash.hpp"

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
  NestloopJoinExecutor(const std::unique_ptr<Expr>& expr, const OutputSchema& In1, const OutputSchema& In2, const OutputSchema& Out, std::unique_ptr<Executor> ch1,std::unique_ptr<Executor> ch2)
      : predicate_(expr.get(),Out), is1(In1), is2(In2), os(Out), c1(std::move(ch1)), c2(std::move(ch2)), t(In1){ read=false; out=new StaticFieldRef[os.Size()]; }
  ~NestloopJoinExecutor()override{ delete[] out; }
  void Init()override{ c1->Init(); c2->Init(); }
  InputTuplePtr Next()override
  {
    if(!read){ read=true; v2=c2->Next(); p=0; while((v1=c1->Next())) t.Append(v1.Data()); }
    while(true)
    {
      if(!v2) return v2;
      if(p==t.GetPointerVec().size()){ p=0; v2=c2->Next(); continue; } v1=t.GetPointerVec()[p++];
      merge(); if(predicate_&&predicate_.Evaluate(out).ReadInt()==false) continue;
      return out;
    }
  }
private:
  ExprFunction predicate_;
  const OutputSchema& is1,is2,os;
  std::unique_ptr<Executor> c1,c2;
  bool read;
  InputTuplePtr v2;
  InputTuplePtr v1; TupleStore t; size_t p;
  StaticFieldRef *out;
  void merge()
  {
    print_log;
    memcpy(out,v1.Data(),sizeof(StaticFieldRef*)*is1.Size());
    print_log;
    if(is2.IsRaw()) Tuple::DeSerialize(out+is1.Size(),v2.Data(),is2.GetCols());
    else memcpy(out+is1.Size(),v2.Data(),sizeof(StaticFieldRef*)*is2.Size());
  }
  friend class HashJoinExecutor;
};

class HashJoinExecutor:public NestloopJoinExecutor
{
 public:
  HashJoinExecutor(const std::unique_ptr<Expr>& expr, const OutputSchema& In1, const OutputSchema& In2, const OutputSchema& Out, std::unique_ptr<Executor> ch1,std::unique_ptr<Executor> ch2,const std::vector<std::unique_ptr<Expr>> &ex1,const std::vector<std::unique_ptr<Expr>> &ex2)
      :NestloopJoinExecutor(expr,In1,In2,Out,std::move(ch1),std::move(ch2)){ for(size_t i=0;i<ex1.size();i++)
        { e1.push_back(std::make_pair(ExprFunction(ex1[i].get(),In1),ex1[i]->ret_type_));
          e2.push_back(std::make_pair(ExprFunction(ex2[i].get(),In2),ex2[i]->ret_type_)); } }
  InputTuplePtr Next()override
  {
    if(!read){ read=true; while((v1=c1->Next())){ uint64_t H=hash(v1,e1); t.Append(v1.Data()); h[H].push_back((StaticFieldRef*)t.GetPointerVec().back()); } Next2(); }
    while(true)
    {
      if(!v2) return v2;
      if(T==nullptr||p==T->size()){ Next2(); continue; } v1=(*T)[p++];
      merge(); if(predicate_&&predicate_.Evaluate(out).ReadInt()==false) continue;
      return out;
    }
  }
 private:
  std::vector<std::pair<ExprFunction,RetType>> e1,e2;
  std::unordered_map<uint64_t,std::vector<StaticFieldRef*> > h;
  std::vector<StaticFieldRef*> *T;
  static const uint64_t sed=1000000007,mul=998244353;
  uint64_t hash_(InputTuplePtr in,std::pair<ExprFunction,RetType> e,uint64_t sed)
  {
    assert(e.second!=RetType::FLOAT);
    if(e.second==RetType::INT) return sed*mul+e.first.Evaluate(in).ReadInt();
    return wing::utils::Hash(e.first.Evaluate(in).ReadStringView(),sed);
  }
  uint64_t hash(InputTuplePtr in,std::vector<std::pair<ExprFunction,RetType>> e){ uint64_t ret=sed; for(auto i:e) ret=hash_(in,i,ret); return ret; }
  void Next2()
  {
    v2=c2->Next(); if(!v2){ T=nullptr; return; }
    p=0; uint64_t H=hash(v2,e2);
    if(h.count(H)) T=&h[H]; else T=nullptr;
  }
};

}  // namespace wing

#endif