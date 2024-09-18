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
        // 隔离级别，判断不同级别能否请求锁类型
        if (!CanTxnTakeLock(txn, lock_mode)) {
            return false;
        }
        // 已经锁了相同直接返回，不同判断能否锁升级，FIFO wait等待
        std::shared_ptr<LockRequestQueue> request_queue;
        {
            std::scoped_lock<std::mutex> lock(table_lock_map_latch_);
            if (!table_lock_map_.count(oid)) {
                table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
            }
            request_queue = table_lock_map_[oid];
        }
        {
            std::scoped_lock<std::mutex> lock(request_queue.get()->latch_);
            auto iter = request_queue.get()->request_queue_.begin();
            while (iter != request_queue.get()->request_queue_.end()) {

                if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_) {
                    if (lock_mode == (*iter)->lock_mode_) {
                        return true;
                    }
                    if (!CanLockUpgrade((*iter)->lock_mode_, lock_mode)) {
                        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
                    }
                    break;
                }
            }
            if (iter != request_queue.get()->request_queue_.end()) {
                request_queue->request_queue_.erase(iter);
                InsertOrDeleteTableLock(txn, lock_mode, oid, true);
                request_queue.get()->upgrading_ = txn->GetTransactionId();
                iter = request_queue.get()->request_queue_.begin();
                while (iter != request_queue.get()->request_queue_.end()) {
                    if (!(*iter)->granted_) {
                        break;
                    }
                    iter++;
                }
                auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
                request_queue.get()->request_queue_.insert(iter, lock_request);
            }
        }

        // 加锁请求放入队列,FIFO wait等待
        auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
        if (request_queue.get()->upgrading_ != INVALID_TXN_ID) {
            std::scoped_lock<std::mutex> lock(request_queue->latch_);
            request_queue.get()->request_queue_.push_back(lock_request);
        }

        while (true) {
            // 队列头是当前事务时，加锁退出
            {
                std::unique_lock<std::mutex> lock(request_queue->latch_);
                request_queue->cv_.wait(lock, [&] {
                    if (request_queue.get()->upgrading_ != INVALID_TXN_ID) {
                        if (request_queue.get()->upgrading_ != txn->GetTransactionId()) {
                            return false;
                        } else { return true; }
                    }


                    for (auto &request: request_queue.get()->request_queue_) {
                        if (request->granted_ == false) {
                            if (request == lock_request) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    }
                    return false;
                });
                // 多粒度锁检查，当前加锁后，对应祖先节点也要加锁
                // granted lock<=>更新txn的lock_set
                GrantNewLocksIfPossible(request_queue.get());
                if (txn->GetTransactionId() == request_queue->upgrading_) {
                    request_queue->upgrading_ = INVALID_TXN_ID;
                }
                InsertOrDeleteTableLock(txn, lock_mode, oid, false);
                return true;
            }
        }


        return false;
    }

    auto
    LockManager::InsertOrDeleteTableLock(Transaction *txn, LockMode lockMode, table_oid_t oid, bool isDelete) -> void {
        switch (lockMode) {
            case LockMode::SHARED:
                if (txn->GetSharedTableLockSet()->count(oid) && isDelete) {
                    txn->GetSharedTableLockSet()->erase(oid);
                }
                if (!txn->GetSharedTableLockSet()->count(oid) && !isDelete) {
                    txn->GetSharedTableLockSet()->insert(oid);
                }
                break;
            case LockMode::EXCLUSIVE:
                if (txn->GetExclusiveTableLockSet()->count(oid) && isDelete) {
                    txn->GetExclusiveTableLockSet()->erase(oid);
                }
                if (!txn->GetExclusiveTableLockSet()->count(oid) && !isDelete) {
                    txn->GetExclusiveTableLockSet()->insert(oid);
                }
                break;
            case LockMode::INTENTION_SHARED:
                if (txn->GetIntentionSharedTableLockSet()->count(oid) && isDelete) {
                    txn->GetIntentionSharedTableLockSet()->erase(oid);
                }
                if (!txn->GetIntentionSharedTableLockSet()->count(oid) && !isDelete) {
                    txn->GetIntentionSharedTableLockSet()->insert(oid);
                }
                break;
            case LockMode::INTENTION_EXCLUSIVE:
                if (txn->GetIntentionExclusiveTableLockSet()->count(oid) && isDelete) {
                    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
                }
                if (!txn->GetIntentionExclusiveTableLockSet()->count(oid) && !isDelete) {
                    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
                }
                break;
            case LockMode::SHARED_INTENTION_EXCLUSIVE:
                if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) && isDelete) {
                    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
                }
                if (!txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) && !isDelete) {
                    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
                }
                break;
        }
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

    void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
        std::scoped_lock lock(waits_for_latch_);
        waits_for_[t1].push_back(t2);
    }

    void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {

    }

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

    // 检查；drop current lock；add new lock, upgrade_tx
    auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
        InsertOrDeleteTableLock(txn, lock_mode, oid, true);
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
        // 不同隔离级别判断

        return false;
    }

    void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
        // 判断upgrading_？
        std::scoped_lock(lock_request_queue->latch_);
        auto iter = lock_request_queue->request_queue_.begin();
        while (iter != lock_request_queue->request_queue_.end()) {
            if ((*iter)->granted_ == false) {
                break;
            }
            iter++;
        }
        (*iter)->granted_ = true;
    }

    auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
        return isUpgrade[int(curr_lock_mode)][int(requested_lock_mode)];
    }

    // 多粒度锁判断，行锁判断父节点表锁
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
