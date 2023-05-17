#include "catalog/stat.hpp"

#include "common/murmurhash.hpp"
#include "functions/functions.hpp"
#include <limits>
#include <iostream>

namespace wing {

void CountMinSketch::AddCount(std::string_view key, double value) {
	uint64_t Sed=sed;
	for(int i=0;i<funcs_;i++,Sed=uint64_hash(Sed))
	{
		uint64_t h=utils::Hash(key,Sed);
		data_[i*buckets_+h%buckets_]+=value;
	}
}

double CountMinSketch::GetFreqCount(std::string_view key) const {
	uint64_t Sed=sed;
	double ans=std::numeric_limits<double>::max();
	for(int i=0;i<funcs_;i++,Sed=uint64_hash(Sed))
	{
		uint64_t h=utils::Hash(key,Sed);
		ans=std::min(ans,data_[i*buckets_+h%buckets_]);
	}
	return ans;
}

void HyperLL::Add(std::string_view key)
{
	uint64_t h=utils::Hash(key,sed);
	int i=h%n; h/=n;
	for(int j=0;j<40;j++) // assume n<=2^24
		if((h>>j)&1) break;
		else m[i]=std::max(int(m[i]),j+1);
}

double HyperLL::GetDistinctCounts() const
{
	double Z=0;
	for(auto i:m) Z+=(double)1/(1ull<<i);
	Z=1/Z;
	return alpha*n*n*Z*2;
}

const double HyperLL::_alpha[128]=
{0,0,0.351194, 0.471386, 0.532435, 0.569448, 0.594306, 0.612161, 0.625609, 
0.636104, 0.644524, 0.651428, 0.657194, 0.66208, 0.666274, 0.669914, 
0.673102, 0.675918, 0.678423, 0.680666, 0.682687, 0.684516, 0.68618, 
0.6877, 0.689094, 0.690377, 0.691562, 0.692659, 0.693679, 0.694629, 
0.695515, 0.696345, 0.697123, 0.697854, 0.698542, 0.699191, 0.699804, 
0.700384, 0.700934, 0.701455, 0.701951, 0.702422, 0.702871, 0.7033, 
0.703708, 0.704099, 0.704473, 0.704831, 0.705174, 0.705503, 0.705819, 
0.706123, 0.706415, 0.706696, 0.706966, 0.707227, 0.707479, 0.707721, 
0.707956, 0.708182, 0.708401, 0.708613, 0.708818, 0.709016, 0.709208, 
0.709395, 0.709576, 0.709751, 0.709921, 0.710086, 0.710247, 0.710403, 
0.710555, 0.710702, 0.710846, 0.710985, 0.711122, 0.711254, 0.711383, 
0.711509, 0.711632, 0.711752, 0.711868, 0.711982, 0.712094, 0.712202, 
0.712309, 0.712412, 0.712514, 0.712613, 0.71271, 0.712804, 0.712897, 
0.712988, 0.713077, 0.713164, 0.713249, 0.713332, 0.713414, 0.713494, 
0.713572, 0.713649, 0.713725, 0.713798, 0.713871, 0.713942, 0.714012, 
0.71408, 0.714147, 0.714213, 0.714278, 0.714342, 0.714404, 0.714466, 
0.714526, 0.714585, 0.714643, 0.714701, 0.714757, 0.714812, 0.714867, 
0.71492, 0.714973, 0.715024, 0.715075, 0.715126, 0.715175, 0.715223};

}  // namespace wing