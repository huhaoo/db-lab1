#ifndef SAKURA_CARD_EST_H__
#define SAKURA_CARD_EST_H__

#include <string>
#include "catalog/db.hpp"
#include "plan/plan_expr.hpp"
#include "parser/expr.hpp"
#include "plan/output_schema.hpp"
#include "functions/functions.hpp"

namespace wing {

class CardEstimator {
 public:
  
  // Necessary data for a group of tables. 
  class Summary {
    public:
      double size_{0};
      std::vector<std::pair<int, double>> distinct_rate_;
  };

  // Use DB statistics to estimate the size of the output of seq scan.
  // We assume that columns are uniformly distributed and independent.
  // You should consider predicates which contain two operands and one is a constant.
  // There are some cases:
  // (1) A.a = 1; 1 = A.a; Use CountMinSketch.
  // (2) A.a > 1; 1 > A.a; or A.a <= 1; Use the maximum element and the minimum element of the table.
  // (3) You should ignore other predicates, such as A.a * 2 + A.b < 1000 and A.a < A.b.
  // (4) 1 > 2; Return 0. You can ignore it, because it should be filtered before optimization.
  // You should check the type of each column and write codes for each case, unfortunately.
  static Summary EstimateTable(std::string_view table_name, const PredicateVec& predicates, const OutputSchema& schema, DB& db) {
    Summary ret;
    const TableStatistics* st=db.GetTableStat(table_name);
    assert(st!=nullptr); 
    const TableSchema& ts=db.GetDBSchema()[db.GetDBSchema().Find(table_name).value()];
    double size=st->GetTupleNum(); size_t tuple_num=st->GetTupleNum(); std::map<int,double> dist_rate; std::map<int,int> id;
    for(size_t i=0;i<schema.Size();i++){ dist_rate[schema[i].id_]=st->GetDistinctRate(i); auto ID=ts.Find(schema[i].column_name_); assert(ID.has_value()); id[schema[i].id_]=ID.value(); }
    for(const auto &i:predicates.GetVec())
    {
      if(i.expr_->op_==OpType::EQ)
      {
        Expr *ch0,*ch1;
        ch0=i.expr_->ch0_.get(); ch1=i.expr_->ch1_.get();
        if(ch0->type_==ExprType::COLUMN&&(ch1->type_==ExprType::LITERAL_FLOAT||ch1->type_==ExprType::LITERAL_INTEGER||ch1->type_==ExprType::LITERAL_STRING))
        {
          int _id=((ColumnExpr*)(ch0))->id_in_column_name_table_; if(!id.count(_id)) continue;
          dist_rate[_id]=-1; size*=st->GetCountMinSketch(id[_id]).GetFreqCount(convert_expr_to_field(ch1).ToString())/tuple_num;
        }
        ch0=i.expr_->ch1_.get(); ch1=i.expr_->ch0_.get();
        if(ch0->type_==ExprType::COLUMN&&(ch1->type_==ExprType::LITERAL_FLOAT||ch1->type_==ExprType::LITERAL_INTEGER||ch1->type_==ExprType::LITERAL_STRING))
        {
          int _id=((ColumnExpr*)(ch0))->id_in_column_name_table_; if(!id.count(_id)) continue;
          dist_rate[_id]=-1; size*=st->GetCountMinSketch(id[_id]).GetFreqCount(convert_expr_to_field(ch1).ToString())/tuple_num;
        }
      }
      else if(i.expr_->op_==OpType::GEQ||i.expr_->op_==OpType::GT||i.expr_->op_==OpType::LEQ||i.expr_->op_==OpType::LT)
      {
        Expr *ch0,*ch1;
        ch0=i.expr_->ch0_.get(); ch1=i.expr_->ch1_.get();
        if(ch0->type_==ExprType::COLUMN&&(ch1->type_==ExprType::LITERAL_FLOAT||ch1->type_==ExprType::LITERAL_INTEGER))
        {
          int _id=((ColumnExpr*)(ch0))->id_in_column_name_table_; if(!id.count(_id)) continue; int __id=id[_id]; double rate;
          if(ch1->type_==ExprType::LITERAL_FLOAT) rate=(((LiteralFloatExpr*)ch1)->literal_value_-st->GetMin(__id).ReadFloat())/(st->GetMax(__id).ReadFloat()-st->GetMin(__id).ReadFloat());
          if(ch1->type_==ExprType::LITERAL_INTEGER) rate=(double)(((LiteralIntegerExpr*)ch1)->literal_value_-st->GetMin(__id).ReadInt()+1)/(st->GetMax(__id).ReadInt()-st->GetMin(__id).ReadInt()+1);
          if(rate<0+1e-5||rate>1-1e-5)
          {
            // if(ch1->type_==ExprType::LITERAL_INTEGER) printf("%ld %ld   %ld\n",st->GetMax(__id).ReadInt(),st->GetMin(__id).ReadInt(),((LiteralIntegerExpr*)ch1)->literal_value_);
            continue;
          }
          size*=(i.expr_->op_==OpType::LEQ||i.expr_->op_==OpType::LT?rate:1-rate);
        }
        ch0=i.expr_->ch1_.get(); ch1=i.expr_->ch0_.get();
        if(ch0->type_==ExprType::COLUMN&&(ch1->type_==ExprType::LITERAL_FLOAT||ch1->type_==ExprType::LITERAL_INTEGER))
        {
          int _id=((ColumnExpr*)(ch0))->id_in_column_name_table_; if(!id.count(_id)) continue; int __id=id[_id]; double rate;
          if(ch1->type_==ExprType::LITERAL_FLOAT) rate=(((LiteralFloatExpr*)ch1)->literal_value_-st->GetMin(__id).ReadFloat())/(st->GetMax(__id).ReadFloat()-st->GetMin(__id).ReadFloat());
          if(ch1->type_==ExprType::LITERAL_INTEGER) rate=(double)(((LiteralIntegerExpr*)ch1)->literal_value_-st->GetMin(__id).ReadInt()+1)/(st->GetMax(__id).ReadInt()-st->GetMin(__id).ReadInt()+1);
          if(rate<0+1e-5||rate>1-1e-5)
          {
            // if(ch1->type_==ExprType::LITERAL_INTEGER) printf("%ld %ld   %ld\n",st->GetMax(__id).ReadInt(),st->GetMin(__id).ReadInt(),((LiteralIntegerExpr*)ch1)->literal_value_);
            continue;
          }
          size*=(i.expr_->op_==OpType::LEQ||i.expr_->op_==OpType::LT?rate:1-rate);
        }
      }
    }
    ret.size_=size; for(auto i:dist_rate) ret.distinct_rate_.push_back({i.first,i.second<0?1/size:i.second});
    return ret;
  }

  // Only consider about equality predicates such as 'A.a = B.b'
  // For other join predicates, you should ignore them.
  static Summary EstimateJoinEq(
    const PredicateVec& predicates, 
    const Summary& build, 
    const Summary& probe
  ) {
    Summary ret;
    ret.distinct_rate_=build.distinct_rate_; for(auto i:probe.distinct_rate_) ret.distinct_rate_.push_back(i);; ret.size_=build.size_*probe.size_;
    std::map<int,double> b(build.distinct_rate_.begin(),build.distinct_rate_.end()),p(probe.distinct_rate_.begin(),probe.distinct_rate_.end());
    double rate=1;
    for(const auto &i:predicates.GetVec())
      if(i.expr_->op_==OpType::EQ&&i.expr_->ch0_->type_==ExprType::COLUMN&&i.expr_->ch1_->type_==ExprType::COLUMN)
      {
        ColumnExpr *ch0=(ColumnExpr*)(i.expr_->ch0_.get()),*ch1=(ColumnExpr*)(i.expr_->ch1_.get());
        int id0=ch0->id_in_column_name_table_,id1=ch1->id_in_column_name_table_;
        if(b.count(id0)&&p.count(id1)){ rate=std::max({rate,build.size_*b[id0],probe.size_*p[id1]}); }
        else if(p.count(id0)&&b.count(id1)){ rate=std::max({rate,build.size_*b[id1],probe.size_*p[id0]}); }
      }
    ret.size_/=rate;
    return ret;
  }
};

}

#endif