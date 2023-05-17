#ifndef SAKURA_EXECUTOR_H__
#define SAKURA_EXECUTOR_H__

#include <numeric>

#include "catalog/db.hpp"
#include "execution/exprdata.hpp"
#include "parser/expr.hpp"
#include "plan/plan.hpp"
#include "storage/storage.hpp"
#include "common/murmurhash.hpp"
#include <iostream>

#define print_log printf("Running on line %d at file \"%s\"\n",__LINE__,__FILE__),fflush(stdout)

namespace wing {

/**
 * Init(): Only allocate memory and set some flags, don't evaluate expressions
 * or read/write tuples. Next(): Do operations for each tuple. Return invalid
 * result if it has completed.
 *
 * The first Next() returns the first tuple. The i-th Next() returns the i-th
 * tuple. It is illegal to invoke Next() after Next() returns invalid result.
 * Ensure that Init is invoked only once before executing.
 *
 * You should ensure that the InputTuplePtr is valid until Next() is invoked
 * again.
 */
class Executor {
 public:
  virtual ~Executor() = default;
  virtual void Init() = 0;
  virtual InputTuplePtr Next() = 0;
};

class ExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(
      const PlanNode* plan, DB& db, size_t txn_id);

 private:
};

// TASK1
class NestloopJoinExecutor:public Executor
{
 public:
  NestloopJoinExecutor(const std::unique_ptr<Expr>& expr, const OutputSchema& In1, const OutputSchema& In2, const OutputSchema& Out, std::unique_ptr<Executor> ch1,std::unique_ptr<Executor> ch2)
      : predicate_(expr.get(),Out), is1(In1), is2(In2), os(Out), c1(std::move(ch1)), c2(std::move(ch2)), t(In1){ read=false; out=new StaticFieldRef[os.Size()]; /*assert(Out.IsRaw()==false);*/ }
  ~NestloopJoinExecutor()override{ delete[] out; }
  void Init()override{ c1->Init(); c2->Init(); }
  InputTuplePtr Next()override
  {
    if(!read){ read=true; v2=c2->Next(); p=0; while((v1=c1->Next())) t.Append(v1.Data()); }
    while(true)
    {
      if(!v2)
      {
        // if(output_size){ printf("Join finish, output size= %lu\n",output_size); fflush(stdout); output_size=0; }
        return v2;
      }
      if(p==t.GetPointerVec().size()){ p=0; v2=c2->Next(); continue; } v1=t.GetPointerVec()[p++];
      merge(); if(predicate_&&predicate_.Evaluate(out).ReadInt()==false) continue;
      output_size++;
      return out;
    }
  }
private:
  size_t output_size=0;
  ExprFunction predicate_;
  const OutputSchema is1,is2,os;
  std::unique_ptr<Executor> c1,c2;
  bool read;
  InputTuplePtr v2;
  InputTuplePtr v1; TupleStore t; size_t p;
  StaticFieldRef *out;
  void merge()
  {
    memcpy(out,v1.Data(),sizeof(StaticFieldRef*)*is1.Size());
    if(is2.IsRaw()) Tuple::DeSerialize(out+is1.Size(),v2.Data(),is2.GetCols());
    else memcpy(out+is1.Size(),v2.Data(),sizeof(StaticFieldRef*)*is2.Size());
  }
  friend class HashJoinExecutor;
};

class naive_hash
{
  static const uint64_t sed=0x1a2b3c4d5e6fllu;
  uint64_t hash_(InputTuplePtr in,std::pair<ExprFunction,RetType> e,uint64_t sed)
  {
    //assert(e.second!=RetType::FLOAT);
    if(e.second==RetType::INT)
    {
      sed = (sed+0xfd7046c5) + (sed<<3);
      sed = (sed+0xfd7046c5) + (sed>>3);
      sed = (sed^0xb55a4f09) ^ (sed<<16);
      sed = (sed^0xb55a4f09) ^ (sed>>16);
      return sed+e.first.Evaluate(in).ReadInt();
    }
    return wing::utils::Hash(e.first.Evaluate(in).ReadStringView(),sed);
  }
public:
  uint64_t hash(InputTuplePtr in,std::vector<std::pair<ExprFunction,RetType>> e){ if(e.empty()) return 0; if(e.size()==1&&e[0].second==RetType::INT) return e[0].first.Evaluate(in).ReadInt(); uint64_t ret=sed; for(auto i:e) ret=hash_(in,i,ret); return ret; }
};
class HashJoinExecutor:public NestloopJoinExecutor,public naive_hash
{
 public:
  HashJoinExecutor(const std::unique_ptr<Expr>& expr, const OutputSchema& In1, const OutputSchema& In2, const OutputSchema& Out, std::unique_ptr<Executor> ch1,std::unique_ptr<Executor> ch2,const std::vector<std::unique_ptr<Expr>> &ex1,const std::vector<std::unique_ptr<Expr>> &ex2)
      :NestloopJoinExecutor(expr,In1,In2,Out,std::move(ch1),std::move(ch2)){ for(size_t i=0;i<ex1.size();i++)
        { e1.push_back(std::make_pair(ExprFunction(ex1[i].get(),In1),ex1[i]->ret_type_));
          e2.push_back(std::make_pair(ExprFunction(ex2[i].get(),In2),ex2[i]->ret_type_)); } }
  InputTuplePtr Next()override
  {
    if(!read)
    {
      std::vector<uint64_t> H;
      std::vector<StaticFieldRef*> refs;
      read=true; while((v1=c1->Next())){ H.push_back(hash(v1,e1)); t.Append(v1.Data()); refs.push_back((StaticFieldRef*)t.GetPointerVec().back()); }
      while(hash_map_size<H.size()) hash_map_size<<=1;; h.resize(hash_map_size);
      for(size_t i=0;i<H.size();i++) h[H[i]%hash_map_size].push_back({H[i],refs[i]});
      Next2();
    }
    while(true)
    {
      if(!v2)
      {
        // if(output_size){ printf("Hash join finish, output size= %lu\n",output_size); fflush(stdout); output_size=0; }
        return v2;
      }
      if(T==nullptr||p==T->size()){ Next2(); continue; } auto V1=(*T)[p++];
      if(V1.first!=H2) continue; v1=V1.second;
      merge(); if(predicate_&&predicate_.Evaluate(out).ReadInt()==false) continue;
      output_size++;
      return out;
    }
  }
 private:
  size_t hash_map_size=1;
  std::vector<std::pair<ExprFunction,RetType>> e1,e2;
  std::vector<std::vector<std::pair<uint64_t,StaticFieldRef*> > > h;
  std::vector<std::pair<uint64_t,StaticFieldRef*> > *T;
  uint64_t H2;
  void Next2()
  {
    v2=c2->Next(); if(!v2){ T=nullptr; return; }
    p=0; H2=hash(v2,e2); T=&h[H2%hash_map_size];
  }
};

// TASK2
class HashAggregateExecutor:public Executor,public naive_hash
{
 public:
  HashAggregateExecutor(const std::unique_ptr<Expr>& Gc, const OutputSchema& Is, const OutputSchema& Os, std::unique_ptr<Executor> ch1,
                        const std::vector<std::unique_ptr<Expr>> &Oe,const std::vector<std::unique_ptr<Expr>> &Ge)
      : is(Is.GetCols()), os(Os), gc(Gc.get(),Is), e(std::move(ch1)), t(Is)
      {
        // print_log;
        //assert(Oe.size()==Os.Size()); assert(Os.IsRaw()==false);
        for(size_t i=0;i<Oe.size();i++) oe.push_back(AggregateExprFunction(Oe[i].get(),is));
        for(size_t i=0;i<Ge.size();i++) ge.push_back(std::make_pair(ExprFunction(Ge[i].get(),is),Ge[i]->ret_type_));
        out=new StaticFieldRef[Os.Size()]; _gc=(bool)gc;
      }
  ~HashAggregateExecutor()override{ delete[] out; for(auto i:a) for(auto j:i.second) delete[] j; }
  void Init()override{ e->Init(); calculated=false; }
  void calc()
  {
    InputTuplePtr v;
    while((v=e->Next()))
    {
      input_size++;
      t.Append(v.Data()); StaticFieldRef* v=(StaticFieldRef*)t.GetPointerVec().back();
      std::vector<size_t> &H=h[hash(v,ge)]; int id=-1;
      for(auto i:H) if(in_same_group(ge,v,a[i].first)) id=i;
      if(id==-1)
      {
        H.push_back(id=a.size());
        std::vector<AggregateIntermediateData*> m;
        for(auto i:oe) m.push_back(new AggregateIntermediateData[i.GetImmediateDataSize()]);
        if(_gc) m.push_back(new AggregateIntermediateData[gc.GetImmediateDataSize()]);
        a.push_back({v,m});
        for(size_t i=0;i<oe.size();i++) oe[i].FirstEvaluate(a[id].second[i],v);
        if(_gc) gc.FirstEvaluate(a[id].second.back(),v);
      }
      else
      {
        for(size_t i=0;i<oe.size();i++) oe[i].Aggregate(a[id].second[i],v);
        if(_gc) gc.Aggregate(a[id].second.back(),v);
      }
    }
    // std::cout<<"Aggregate init finished, input size= "<<input_size<<std::endl;
    ptr=0;
  }
  InputTuplePtr Next()override
  {
    if(!calculated){ calc(); calculated=true; }
    while(true)
    {
      if(ptr==a.size()) return InputTuplePtr();
      auto &ret=a[ptr++];
      if(_gc&&gc.LastEvaluate(ret.second.back(),ret.first).ReadInt()==0) continue;
      for(size_t i=0;i<oe.size();i++) out[i]=oe[i].LastEvaluate(ret.second[i],ret.first);
      return out;
    }
  }
private:
  size_t input_size=0;

  OutputSchema is,os; // Input/Output Schema
  AggregateExprFunction gc; // Group Checker
  bool _gc;
  std::unique_ptr<Executor> e; // child Executor
  std::vector<AggregateExprFunction> oe; // Output Expr (Function), return type is equal to output schema
  std::vector<std::pair<ExprFunction,RetType> > ge; // Group Expr (Function)
  TupleStore t; // Tuple store

  std::unordered_map<uint64_t,std::vector<size_t> > h; // Hash map
  std::vector<std::pair<StaticFieldRef*,std::vector<AggregateIntermediateData*> > > a; // groups store: first tuple in the group, and all the intermediatedata. the intermediatedata of checker is the last one.

  bool calculated; size_t ptr;

  StaticFieldRef* out;

  bool in_same_group(std::vector<std::pair<ExprFunction,RetType> > e,StaticFieldRef* a,StaticFieldRef* b)
  {
    for(auto i:e)
    {
      switch (i.second)
      {
      case RetType::INT:
        if(i.first.Evaluate(a).ReadInt()!=i.first.Evaluate(b).ReadInt()) return false;
        break;
      case RetType::FLOAT:
        if(i.first.Evaluate(a).ReadFloat()!=i.first.Evaluate(b).ReadFloat()) return false;
        break;
      case RetType::STRING:
        if(i.first.Evaluate(a).ReadStringView()!=i.first.Evaluate(b).ReadStringView()) return false;
        break;
      default:
        assert(false);
      }
    }
    return true;
  }
};

// TASK3
namespace _SORT_
{
  static std::vector<std::pair<RetType, bool>> oe;
  static int cmp(StaticFieldRef *a,StaticFieldRef *b)
  {
    for(int i=0;i<oe.size();i++)
    {
      switch (oe[i].first)
      {
      case RetType::INT:
        if(a[i].ReadInt()!=b[i].ReadInt()) return (a[i].ReadInt()>b[i].ReadInt())^oe[i].second;
        break;
      case RetType::FLOAT:
        if(a[i].ReadFloat()!=b[i].ReadFloat()) return (a[i].ReadFloat()>b[i].ReadFloat())^oe[i].second;
        break;
      case RetType::STRING:
      {
        auto d=(a[i].ReadStringView()<=>b[i].ReadStringView());
        if(d!=d.equivalent) return (d==d.greater)^oe[i].second;
        break;
      }
      default:
        assert(false);
      }
    }
    return false;
  }
}

class OrderExecutor:public Executor
{
public:
  OrderExecutor(const OutputSchema& Is,const OutputSchema& Os,std::unique_ptr<Executor> ch,std::vector<std::pair<RetType, bool>> Oe):is(Is),os(Os),e(std::move(ch)),oe(Oe),t(Is){ out=new StaticFieldRef[Os.Size()]; }
  ~OrderExecutor()override{ delete[] out; }
  void Init()override{ e->Init(); calculated=false; }
  void calc()
  {
    InputTuplePtr v; while((v=e->Next())){ t.Append(v.Data()); t_.push_back((StaticFieldRef*)(t.GetPointerVec().back())); }
    _SORT_::oe=oe; std::sort(t_.begin(),t_.end(),_SORT_::cmp);
    ptr=0;
  }
  InputTuplePtr Next() override
  {
    if(!calculated){ calc(); calculated=true; }
    if(ptr==t_.size()) return InputTuplePtr();
    memcpy(out,t_[ptr++]+oe.size(),sizeof(StaticFieldRef*)*os.Size());
    return out;
  }
private:
  OutputSchema is,os; // Input/Output Schema
  std::unique_ptr<Executor> e; // child Executor
  std::vector<std::pair<RetType, bool>> oe; // Orderby Expr
  TupleStore t; std::vector<StaticFieldRef *> t_;
  StaticFieldRef* out;
  size_t ptr;
  bool calculated;
};

class LimitExecutor:public Executor
{
public:
  LimitExecutor(std::unique_ptr<Executor> ch,size_t d,size_t r):e(std::move(ch)),drop(d),rest(r){}
  void Init()override{ e->Init(); }
  InputTuplePtr Next_(){ auto v=e->Next(); if(!v) drop=rest=0; return v; }
  InputTuplePtr Next()override{ while(drop){ drop--; Next_(); } if(rest){ rest--; return Next_(); } return InputTuplePtr(); }
private:
  std::unique_ptr<Executor> e;
  size_t drop,rest;
};

class DistinctExecutor:public Executor
{
public:
  DistinctExecutor(std::unique_ptr<Executor> ch,const OutputSchema &S):e(std::move(ch)),s(S),t(S){}
  void Init()override{ e->Init(); }
  InputTuplePtr Next()override
  {
    while(true)
    {
      InputTuplePtr v=e->Next();
      if(!v) return v;
      StaticFieldRef *V=(StaticFieldRef*)v.Data();
      int flag=true;
      if(!t.GetPointerVec().empty())
      {
        flag=false; auto &T=t.GetPointerVec().back();
        for(size_t i=0;i<s.Size();i++)
        {
          switch (s[i].type_)
          {
          case FieldType::INT32:
          case FieldType::INT64:
            if(V[i].ReadInt()!=((StaticFieldRef*)T)[i].ReadInt()) flag=true;
            break;
          case FieldType::FLOAT64:
            if(V[i].ReadFloat()!=((StaticFieldRef*)T)[i].ReadFloat()) flag=true;
            break;
          case FieldType::CHAR:
          case FieldType::VARCHAR:
            if(V[i].ReadStringView()!=((StaticFieldRef*)T)[i].ReadStringView()) flag=true;
            break;
          default:
            assert(false);
          }
        }
      }
      if(!flag) continue;
      t.Append(v.Data());
      return v;
    }
  }
private:
  std::unique_ptr<Executor> e;
  OutputSchema s;
  TupleStore t;
};
}  // namespace wing

#endif