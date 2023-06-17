#include "txn_manager.hpp"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "storage/bplus-tree-storage.hpp"
#include "storage/storage.hpp"
#include "transaction/lock_manager.hpp"
namespace wing {
std::unordered_map<txn_id_t, std::unique_ptr<Txn>> TxnManager::txn_table_ = {};
std::shared_mutex TxnManager::rw_latch_ = {};

Txn* TxnManager::Begin() {
  auto txn = new Txn(txn_id_++);
  std::unique_lock lock(rw_latch_);
  txn_table_[txn->txn_id_] = std::unique_ptr<Txn>(txn);
  return txn;
}

void TxnManager::Commit(Txn* txn) {
  txn->state_ = TxnState::COMMITTED;
  // Release all the locks
  ReleaseAllLocks(txn);
}

void TxnManager::Abort(Txn* txn) {
  // P4 TODO: rollback
  txn->state_ = TxnState::ABORTED;
  while(!txn->modify_records_.empty())
  {
    auto t=txn->modify_records_.top(); txn->modify_records_.pop();
    FieldType table_type=storage_.GetDBSchema().GetTables()[storage_.GetDBSchema().Find(t.table_name_).value()].GetPrimaryKeySchema().type_;
    switch(table_type)
    {
    case FieldType::INT32:
    case FieldType::INT64:
     {
      auto &table_=*(BPlusTreeTable<IntegerKeyCompare>*)storage_.GetTable(std::string_view(t.table_name_));
      if(t.type_==ModifyType::DELETE){ assert(t.old_value_.has_value()); table_.Insert(t.key_,t.old_value_.value()); }
      if(t.type_==ModifyType::INSERT) table_.Delete(t.key_);
      if(t.type_==ModifyType::UPDATE){ assert(t.old_value_.has_value()); table_.Update(t.key_,t.old_value_.value()); }
      break;
     }
    case FieldType::FLOAT64:
     {
      auto &table_=*(BPlusTreeTable<FloatKeyCompare>*)storage_.GetTable(std::string_view(t.table_name_));
      if(t.type_==ModifyType::DELETE){ assert(t.old_value_.has_value()); table_.Insert(t.key_,t.old_value_.value()); }
      if(t.type_==ModifyType::INSERT) table_.Delete(t.key_);
      if(t.type_==ModifyType::UPDATE){ assert(t.old_value_.has_value()); table_.Update(t.key_,t.old_value_.value()); }
      break;
     }
    case FieldType::CHAR:
    case FieldType::VARCHAR:
     {
      auto &table_=*(BPlusTreeTable<StringKeyCompare>*)storage_.GetTable(std::string_view(t.table_name_));
      if(t.type_==ModifyType::DELETE){ assert(t.old_value_.has_value()); table_.Insert(t.key_,t.old_value_.value()); }
      if(t.type_==ModifyType::INSERT) table_.Delete(t.key_);
      if(t.type_==ModifyType::UPDATE){ assert(t.old_value_.has_value()); table_.Update(t.key_,t.old_value_.value()); }
      break;
     }
    default:
      assert(0);
    }
  }
  // Release all the locks.
  ReleaseAllLocks(txn);
}

// Don't need latches here because txn is already committed or aborted.
void TxnManager::ReleaseAllLocks(Txn* txn) {
  // Release all the locks. Tuple locks first.
  auto tuple_lock_set_copy = txn->tuple_lock_set_;
  for (auto& [mode, table_map] : tuple_lock_set_copy) {
    for (auto& [table_name, key_set] : table_map) {
      for (auto& key : key_set) {
        lock_manager_.ReleaseTupleLock(table_name, key, mode, txn);
      }
    }
  }
  // Release table locks.
  auto table_lock_set_copy = txn->table_lock_set_;
  for (auto& [mode, table_set] : table_lock_set_copy) {
    for (auto& table_name : table_set) {
      lock_manager_.ReleaseTableLock(table_name, mode, txn);
    }
  }
}

}  // namespace wing