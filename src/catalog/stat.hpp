#ifndef SAKURA_DBINFO_H__
#define SAKURA_DBINFO_H__

#include <string>
#include <vector>

#include "type/field.hpp"

namespace wing {

/* CountMinSketch. */
class CountMinSketch {
 public:
  const static uint32_t kDefaultHashCounts = 8;
  const static uint32_t kDefaultHashBuckets = 2027;
  const static uint64_t sed=0x12345678;
  CountMinSketch(size_t buckets, size_t funcs)
    : buckets_(buckets), funcs_(funcs), data_(buckets * funcs) {}
  CountMinSketch() : CountMinSketch(kDefaultHashBuckets, kDefaultHashCounts) {}
  /* Get estimated count of a data. */
  double GetFreqCount(std::string_view data) const;
  /* Add the count of data. */
  void AddCount(std::string_view data, double value = 1.0);

 private:
  /* buckets_ is the row size, and funcs_ is the column size of the sketch. */
  size_t buckets_, funcs_;
  /* sketch data */
  std::vector<double> data_;
};

/* HyperLogLog. */
class HyperLL {
 public:
  const static size_t kDefaultRegCount = 1024;
  const static uint64_t sed=0x12345678;
  const static double _alpha[128];
  HyperLL(size_t reg_count) : m(reg_count),n(reg_count),alpha(n<128?_alpha[n]:(double)0.7213/(1+(double)1.079/n)) { }
  HyperLL() : HyperLL(kDefaultRegCount) {}
  /* Add data into the set. */
  void Add(std::string_view data);
  /* Get estimated distinct counts. */
  double GetDistinctCounts() const;

 private:
  std::vector<uint8_t> m;
  size_t n{0}; double alpha;
};

/* The statistics of a table. Information stored in it cannot be modified. */
class TableStatistics {
 public:
  TableStatistics(size_t tuple_num, std::vector<Field>&& max,
      std::vector<Field>&& min, std::vector<double>&& distinct_rate,
      std::vector<CountMinSketch>&& freq)
    : tuple_num_(tuple_num),
      max_(std::move(max)),
      min_(std::move(min)),
      distinct_rate_(std::move(distinct_rate)),
      freq_(std::move(freq)) {}

  /* Get maximum value of column. */
  const Field& GetMax(int col) const { return max_[col]; }

  /* Get minimum value of column. */
  const Field& GetMin(int col) const { return min_[col]; }

  /* Get distinct rate of column. i.e., (distinct count) / (column count) */
  double GetDistinctRate(int col) const { return distinct_rate_[col]; }

  /* Get distinct rate of all column. */
  const std::vector<double>& GetDistinctRate() const { return distinct_rate_; }

  /* Get count min sketch for one col. */
  const CountMinSketch& GetCountMinSketch(int col) const { return freq_[col]; }

  /* Get number of tuples. */
  size_t GetTupleNum() const { return tuple_num_; }

 private:
  /* Tuple number. */
  size_t tuple_num_;
  /* Maximum value for each column. */
  std::vector<Field> max_;
  /* Minimum value for each column. */
  std::vector<Field> min_;
  /* Distinct rate for each column. */
  std::vector<double> distinct_rate_;
  /* Countminsketch for each column. */
  std::vector<CountMinSketch> freq_;
};

}  // namespace wing

#endif