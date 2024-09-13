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
        if (txn->IsTableExclusiveLocked(oid)) {

        }
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
        return false;
    }

    auto
    LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool {
        return false;
    }

    auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                                std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
        return false;
    }

}  // namespace bustub
