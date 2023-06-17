#include "lock_manager.hpp"

#include <memory>
#include <shared_mutex>

#include "common/exception.hpp"
#include "common/logging.hpp"
#include "fmt/core.h"
#include "transaction/txn.hpp"
#include "transaction/txn_manager.hpp"
#include <map>
#include <list>
#include <stdio.h>
#include <optional>
#define print_log printf("Running on line %d at file \"%s\"\n",__LINE__,__FILE__),fflush(stdout)

// TODO
namespace wing {
const char* format_lockmode(const wing::LockMode& mode) {
	switch (mode) {
		case wing::LockMode::S:   return "S";
		case wing::LockMode::X:   return "X";
		case wing::LockMode::IS:  return "IS";
		case wing::LockMode::IX:  return "IX";
		case wing::LockMode::SIX: return "SIX";
	}
}
template<class T> T &get_value_in_map(std::unordered_map<std::string,std::unique_ptr<T> > &m,std::string_view s)
{
	if(!m.count(std::string(s))) m[std::string(s)]=std::make_unique<T>();
	return *m[std::string(s)];
}
inline bool can_upgrade(LockMode a,LockMode b)
{
	switch(a)
	{
	case LockMode::IS:
		return b!=LockMode::IS;
	case LockMode::S:
	case LockMode::IX:
		return b==LockMode::X||b==LockMode::SIX;
	case LockMode::SIX:
		return b==LockMode::X;
	case LockMode::X:
		return false;
	}
}
inline bool stricter(LockMode a,LockMode b)
{
	switch(a)
	{
	case LockMode::IS:
		return b==LockMode::IS;
	case LockMode::S:
		return b==LockMode::S||b==LockMode::IS;
	case LockMode::IX:
		return b==LockMode::IX||b==LockMode::IS;
	case LockMode::SIX:
		return b!=LockMode::X;
	case LockMode::X:
		return true;
	}
}
inline bool coexist(LockMode a,LockMode b)
{
	switch (a)
	{
	case LockMode::IS:
		return b!=LockMode::X;
	case LockMode::IX:
		return b==LockMode::IS||b==LockMode::IX;
	case LockMode::S:
		return b==LockMode::IS||b==LockMode::S;
	case LockMode::SIX:
		return b==LockMode::IS;
	case LockMode::X:
		return false;
	}
}
std::optional<LockMode> cmode;
bool RemoveLockTable(LockManager::LockRequestList &lock_table,LockMode mode, Txn *txn);
void AcquireLock(LockManager::LockRequestList &lock_table,LockMode mode, Txn *txn)
{
  cmode.reset();
  std::unique_lock<std::mutex> lock(lock_table.latch_);
	// printf("before txn %d acquire, update lock: %d, Waiting list :\n",txn->txn_id_,lock_table.upgrading_);
	// for(auto i:lock_table.list_) printf("(%d, %s, %d) ",i->txn_id_,format_lockmode(i->mode_),i->active_);; putchar(10);
#define THROW(a,b) { txn->state_=TxnState::ABORTED; /*printf("txn %ld aborted.\n",txn->txn_id_);*/ fflush(stdout); throw a(b); return; }
	LockManager::LockRequest *cur=nullptr; for(auto &i:lock_table.list_) if(i->txn_id_==txn->txn_id_) cur=i.get();
	if(cur!=nullptr&&cur->mode_==mode) return ;
	if(cur!=nullptr)
	{
		if(lock_table.upgrading_!=INVALID_TXN_ID) THROW(MultiUpgradeException,"Multi-upgrade.");
		if(!can_upgrade(cur->mode_,mode)) THROW(TxnInvalidBehaviorException,"Can't upgrade.");
		lock_table.upgrading_=txn->txn_id_;
		while(true)
		{
			bool conflict=false;
			for(auto i:lock_table.list_){ if(i->txn_id_!=txn->txn_id_&&i->active_&&!coexist(i->mode_,mode)){ conflict=true; if(!(txn->txn_id_<i->txn_id_)){ RemoveLockTable(lock_table,cur->mode_,txn); THROW(TxnDLAbortException,"Wait die."); } } }
			if(!conflict) break;
			lock_table.cv_.wait(lock);
		}
		lock_table.upgrading_=INVALID_TXN_ID; cmode=cur->mode_; cur->mode_=mode;
	}
	else
	{
		lock_table.list_.push_back(std::make_unique<LockManager::LockRequest>(txn->txn_id_,mode)); cur=lock_table.list_.back().get();
		while(true)
		{
			bool conflict=(lock_table.upgrading_!=INVALID_TXN_ID);
			for(auto i:lock_table.list_){ if(i->txn_id_==txn->txn_id_) break; if(!coexist(i->mode_,mode)){ conflict=true; if(!(txn->txn_id_<i->txn_id_)){ RemoveLockTable(lock_table,cur->mode_,txn); THROW(TxnDLAbortException,"Wait die."); } } }
			if(!conflict) break;
			lock_table.cv_.wait(lock);
		}
    cur->active_=true;
	}
#undef THROW
}
bool RemoveLockTable(LockManager::LockRequestList &lock_table,LockMode mode, Txn *txn)
{
	for(auto i=lock_table.list_.begin();i!=lock_table.list_.end();i++) if((*i)->txn_id_==txn->txn_id_&&(*i)->mode_==mode)
	{
		lock_table.list_.erase(i);
		if(lock_table.upgrading_==txn->txn_id_) lock_table.upgrading_=INVALID_TXN_ID;
    return true;
	}
  return false;
}
void ReleaseLock(LockManager::LockRequestList &lock_table,LockMode mode, Txn *txn)
{
 lock_table.latch_.lock();
  if(RemoveLockTable(lock_table,mode,txn)) lock_table.cv_.notify_all();
 lock_table.latch_.unlock();
}
void LockManager::AcquireTableLock(std::string_view table_name, LockMode mode, Txn *txn)
{
	// printf("txn %ld acquire a %s lock in table %s: acquire\n",txn->txn_id_,format_lockmode(mode),std::string(table_name).c_str()); fflush(stdout);
	if(txn->state_==TxnState::ABORTED){ throw TxnInvalidBehaviorException("Txn is aborted."); return ; }
	if(txn->state_==TxnState::SHRINKING){ txn->state_=TxnState::ABORTED; throw TxnInvalidBehaviorException("Txn is shrinking."); return ; }
 tuple_lock_table_latch_.lock();
	LockRequestList &lock_table=get_value_in_map(table_lock_table_,table_name);
 tuple_lock_table_latch_.unlock();
	AcquireLock(lock_table,mode,txn);
  if(cmode.has_value()) txn->table_lock_set_[cmode.value()].erase(std::string(table_name));
	txn->table_lock_set_[mode].insert(std::string(table_name));
	// printf("txn %ld acquire a %s lock in table %s: success\n",txn->txn_id_,format_lockmode(mode),std::string(table_name).c_str()); fflush(stdout);
}

void LockManager::ReleaseTableLock(std::string_view table_name, LockMode mode, Txn *txn)
{
	// printf("txn %ld release a %s lock in table %s\n",txn->txn_id_,format_lockmode(mode),std::string(table_name).c_str()); fflush(stdout);
  if(txn->state_==TxnState::GROWING) txn->state_=TxnState::SHRINKING;
 tuple_lock_table_latch_.lock();
	LockRequestList &lock_table=get_value_in_map(table_lock_table_,table_name);
 tuple_lock_table_latch_.unlock();
	ReleaseLock(lock_table,mode,txn);
	txn->table_lock_set_[mode].erase(std::string(table_name));
}

void LockManager::AcquireTupleLock(std::string_view table_name,std::string_view key, LockMode mode, Txn *txn)
{
	// printf("txn %ld acquire a %s lock in table %s tuple %s: acquire\n",txn->txn_id_,format_lockmode(mode),std::string(table_name).c_str(),std::string(key).c_str()); fflush(stdout);
 tuple_lock_table_latch_.lock();
  int valid=false;
  if(mode==LockMode::S&&(txn->table_lock_set_[LockMode::S]  .count(std::string(table_name))||
                         txn->table_lock_set_[LockMode::X]  .count(std::string(table_name))||
                         txn->table_lock_set_[LockMode::IS] .count(std::string(table_name))||
                         txn->table_lock_set_[LockMode::IX] .count(std::string(table_name))||
                         txn->table_lock_set_[LockMode::SIX].count(std::string(table_name)))) valid=true;
  if(mode==LockMode::X&&(txn->table_lock_set_[LockMode::X]  .count(std::string(table_name))||
                         txn->table_lock_set_[LockMode::IX] .count(std::string(table_name))||
                         txn->table_lock_set_[LockMode::SIX].count(std::string(table_name)))) valid=true;
  if(!valid)
  {
    tuple_lock_table_latch_.unlock();
    // printf("txn %ld aborted.\n",txn->txn_id_);
    throw TxnInvalidBehaviorException("Invalid tuple lock."); return ;
  }
	LockRequestList &lock_table=get_value_in_map(tuple_lock_table_[std::string(table_name)],key);
 tuple_lock_table_latch_.unlock();
	AcquireLock(lock_table,mode,txn);
  if(cmode.has_value()) txn->tuple_lock_set_[cmode.value()][std::string(table_name)].erase(std::string(key));
	txn->tuple_lock_set_[mode][std::string(table_name)].insert(std::string(key));
	// printf("txn %ld acquire a %s lock in table %s tuple %s: success\n",txn->txn_id_,format_lockmode(mode),std::string(table_name).c_str(),std::string(key).c_str()); fflush(stdout);
}

void LockManager::ReleaseTupleLock(std::string_view table_name,std::string_view key, LockMode mode, Txn *txn)
{
	// printf("txn %ld release a %s lock in table %s tuple %s\n",txn->txn_id_,format_lockmode(mode),std::string(table_name).c_str(),std::string(key).c_str()); fflush(stdout);
  if(txn->state_==TxnState::GROWING) txn->state_=TxnState::SHRINKING;
 tuple_lock_table_latch_.lock();
	LockRequestList &lock_table=get_value_in_map(tuple_lock_table_[std::string(table_name)],key);
 tuple_lock_table_latch_.unlock();
	ReleaseLock(lock_table,mode,txn);
	txn->tuple_lock_set_[mode][std::string(table_name)].erase(std::string(key));
}
}  // namespace wing