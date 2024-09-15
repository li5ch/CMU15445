//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

    auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
        // 已经锁了直接返回，判断是否锁升级
        if (UpgradeLockTable(txn, lock_mode, oid)) {
            table_lock_map_[oid]=
        }
        // 加锁请求放入队列,同步唤醒
        // 隔离级别，判断不同级别能否请求锁类型
        // 多粒度锁检查，当前加锁后，对应祖先节点也要加锁
        // 加锁成功后，更新lock_set

        return true;
    }

    auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

    auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
        return true;
    }

    auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
        return true;
    }

    void LockManager::UnlockAll() {
        // You probably want to unlock all table and txn locks here.
    }

    void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

    void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

    auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

    auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
        std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
        return edges;
    }

    void LockManager::RunCycleDetection() {
        while (enable_cycle_detection_) {
            std::this_thread::sleep_for(cycle_detection_interval);
            {  // TODO(students): detect deadlock
            }
        }
    }

    auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
        if (txn->IsTableIntentionExclusiveLocked(oid)) {
            if (lock_mode == LockMode::INTENTION_EXCLUSIVE)
                return true;
            if (!CanLockUpgrade(LockMode::INTENTION_EXCLUSIVE, lock_mode)) {
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
            }

        }
        if (txn->IsTableExclusiveLocked(oid) && lock_mode == LockMode::EXCLUSIVE) {
            return true;
        }
        if (txn->IsTableSharedIntentionExclusiveLocked(oid) && lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
            return true;
        }
        if (txn->IsTableIntentionSharedLocked(oid) && lock_mode == LockMode::INTENTION_SHARED) {
            return true;
        }
        if (txn->IsTableSharedLocked(oid) && lock_mode == LockMode::SHARED) {
            return true;
        }
        // 加锁放入队列


        return false;
    }

    auto
    LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
        return false;
    }

    auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
        return matrix_[int(l1)][int(l2)];
    }

    auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {

        return false;
    }

    void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {}

    auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
        return isUpgrade[int(curr_lock_mode)][int(requested_lock_mode)];
    }

    auto
    LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool {
        if (row_lock_mode == LockMode::SHARED) {
            return txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid);
        }
        if (row_lock_mode == LockMode::EXCLUSIVE) {
            return txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
                   txn->IsTableIntentionExclusiveLocked(oid);
        }
        return false;
    }

    auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                                std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
        return false;
    }

}  // namespace bustub
