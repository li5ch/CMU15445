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
		auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode,
														  oid);
		{
			std::unique_lock<std::mutex> q_lock(request_queue.get()->latch_);
			auto iter = request_queue.get()->request_queue_.begin();
			while (iter != request_queue.get()->request_queue_.end()) {
				if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_) {
					if (lock_mode == (*iter)->lock_mode_) {
						return true;
					}
					if (!CanLockUpgrade((*iter)->lock_mode_, lock_mode)) {
						txn->SetState(TransactionState::ABORTED);
						throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
					}
					break;
				}
				iter++;
			}
			if (iter != request_queue.get()->request_queue_.end()) {
				InsertOrDeleteTableLock(txn, (*iter)->lock_mode_, oid, true);
				request_queue->request_queue_.erase(iter);
				if (request_queue.get()->upgrading_ != INVALID_TXN_ID) {
					txn->SetState(TransactionState::ABORTED);
					throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
				}
				request_queue.get()->upgrading_ = txn->GetTransactionId();

			}

			// 队列头是当前事务时，加锁退出
			request_queue->cv_.wait(q_lock, [&] {
				if (request_queue->request_queue_.empty()) {
					request_queue.get()->request_queue_.push_back(lock_request);
					return true;
				}
				for (auto &request: request_queue.get()->request_queue_) {
					// 如果granted检查
					if (request->granted_) {
						if (request->txn_id_ == lock_request->txn_id_ &&
							request->lock_mode_ == lock_request->lock_mode_) {
							return true;
						}
						if (!AreLocksCompatible(lock_mode, request->lock_mode_)) {
							// 放入队列
							// 加锁请求放入队列,FIFO wait等待
							if (request_queue.get()->upgrading_ == INVALID_TXN_ID) {
								request_queue.get()->request_queue_.push_back(lock_request);
							} else {
								iter = request_queue.get()->request_queue_.begin();
								while (iter != request_queue.get()->request_queue_.end()) {
									if (!(*iter)->granted_) {
										break;
									}
									iter++;
								}
								request_queue.get()->request_queue_.insert(iter, lock_request);
							}
							return false;
						}
					}
				}
				return true;
			});
			// 多粒度锁检查，当前加锁后，对应祖先节点也要加锁
			// granted lock<=>更新txn的lock_set
			if (txn->GetState() == TransactionState::ABORTED) {
				request_queue->request_queue_.remove(lock_request);
				request_queue.get()->cv_.notify_all();
				return false;
			}
			lock_request->granted_ = true;
			if (txn->GetTransactionId() == request_queue->upgrading_) {
				request_queue->upgrading_ = INVALID_TXN_ID;
			}
		}
		InsertOrDeleteTableLock(txn, lock_mode, oid, false);
		LOG_INFO("txn:%d,lock mode:%d,oid:%d lock succ", txn->GetTransactionId(), lock_mode, oid);
		return true;
	}

	auto LockManager::InsertOrDeleteRowLock(Transaction *txn, LockMode lockMode, const table_oid_t &oid, const RID &rid,
											bool isDelete) -> void {
		switch (lockMode) {
			case LockMode::SHARED:
				if (!txn->GetSharedRowLockSet()->count(oid)) {
					(*txn->GetSharedRowLockSet())[oid] = std::unordered_set<RID>{};
				}

				if ((*txn->GetSharedRowLockSet())[oid].count(rid) && isDelete) {
					(*txn->GetSharedRowLockSet())[oid].erase(rid);
				}
				if (!(*txn->GetSharedRowLockSet())[oid].count(rid) && !isDelete) {
					(*txn->GetSharedRowLockSet())[oid].insert(rid);
				}


				break;
			case LockMode::EXCLUSIVE:
				if (!txn->GetExclusiveRowLockSet()->count(oid)) {
					(*txn->GetExclusiveRowLockSet())[oid] = std::unordered_set<RID>{};
				}
				if ((*txn->GetExclusiveRowLockSet())[oid].count(rid) && isDelete) {
					(*txn->GetExclusiveRowLockSet())[oid].erase(rid);
				}
				if (!(*txn->GetExclusiveRowLockSet())[oid].count(rid) && !isDelete) {
					(*txn->GetExclusiveRowLockSet())[oid].insert(rid);
				}

				break;
			default:
				return;
		}

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

	auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
		// 判断有没有持有锁
		if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
			!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
			!txn->IsTableSharedLocked(oid)) {
			txn->SetState(TransactionState::ABORTED);
			throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
		}
		if ((txn->GetSharedRowLockSet()->count(oid) && !txn->GetSharedRowLockSet().get()->at(oid).empty()) ||
			((txn->GetExclusiveRowLockSet()->count(oid) && !txn->GetExclusiveRowLockSet().get()->at(oid).empty()))) {
			txn->SetState(TransactionState::ABORTED);
			throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
		}
		// update txn state
		std::shared_ptr<LockRequestQueue> request_queue;
		{
			std::unique_lock<std::mutex> lock(table_lock_map_latch_);
			request_queue = table_lock_map_[oid];
		}
		std::shared_ptr<LockRequest> lock_request;
		{
			std::unique_lock<std::mutex> q_lock(request_queue->latch_);
			for (auto &request: request_queue.get()->request_queue_) {
				if (request->granted_ && request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid) {
					lock_request = request;
				}
			}
			request_queue.get()->request_queue_.remove(lock_request);

			if (txn->GetIntentionSharedTableLockSet()->count(oid)) {
				txn->GetIntentionSharedTableLockSet()->erase(oid);
			}

			if (txn->GetExclusiveTableLockSet()->count(oid)) {
				txn->GetExclusiveTableLockSet()->erase(oid);
				txn->SetState(TransactionState::SHRINKING);
			}
			if (txn->GetSharedTableLockSet()->count(oid)) {
				txn->GetSharedTableLockSet()->erase(oid);
				if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)
					txn->SetState(TransactionState::SHRINKING);
				if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
					throw TransactionAbortException(txn->GetTransactionId(),
													AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
				}
			}
			if (txn->GetIntentionExclusiveTableLockSet()->count(oid)) {
				txn->GetIntentionExclusiveTableLockSet()->erase(oid);
			}
			if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid)) {
				txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
			}

			// notify the request lock queue
			request_queue->cv_.notify_all();

		}
		LOG_INFO(" txn:%d,oid:%d unlock succ", txn->GetTransactionId(), oid);

		return true;
	}

	auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
		// 隔离级别，判断不同级别能否请求锁类型
		// 检查多粒度锁，表锁冲突
		if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
			txn->SetState(TransactionState::ABORTED);
			throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
		}
		if (!CanTxnTakeLock(txn, lock_mode)) {
			return false;
		}
		// 已经锁了相同直接返回，不同判断能否锁升级，FIFO wait等待
		std::shared_ptr<LockRequestQueue> request_queue;
		{
			std::unique_lock<std::mutex> lock(row_lock_map_latch_);
			if (!row_lock_map_.count(rid)) {
				row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
			}
			request_queue = row_lock_map_[rid];
		}

		auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode,
														  oid, rid);
		{
			std::unique_lock<std::mutex> q_lock(request_queue.get()->latch_);
			auto iter = request_queue.get()->request_queue_.begin();
			while (iter != request_queue.get()->request_queue_.end()) {
				if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_) {
					if (lock_mode == (*iter)->lock_mode_) {
						return true;
					}
					if (!CanLockUpgrade((*iter)->lock_mode_, lock_mode)) {
						txn->SetState(TransactionState::ABORTED);
						throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
					}
					break;
				}
				iter++;
			}
			if (iter != request_queue.get()->request_queue_.end()) {
				InsertOrDeleteRowLock(txn, (*iter)->lock_mode_, oid, rid, true);
				request_queue->request_queue_.erase(iter);
				if (request_queue.get()->upgrading_ != INVALID_TXN_ID) {
					txn->SetState(TransactionState::ABORTED);
					throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
				}
				request_queue.get()->upgrading_ = txn->GetTransactionId();

			}


			// 队列头是当前事务时，加锁退出
			request_queue->cv_.wait(q_lock, [&] {
				if (request_queue->request_queue_.empty()) {
					request_queue.get()->request_queue_.push_back(lock_request);
					return true;
				}
				for (auto &request: request_queue.get()->request_queue_) {
					// 如果granted检查
					if (request->granted_) {
						if (request->txn_id_ == lock_request->txn_id_ &&
							request->lock_mode_ == lock_request->lock_mode_) {
							return true;
						}
						if (!AreLocksCompatible(lock_mode, request->lock_mode_)) {
							// 放入队列
							// 加锁请求放入队列,FIFO wait等待
							if (request_queue.get()->upgrading_ == INVALID_TXN_ID) {
								request_queue.get()->request_queue_.push_back(lock_request);
							} else {
								iter = request_queue.get()->request_queue_.begin();
								while (iter != request_queue.get()->request_queue_.end()) {
									if (!(*iter)->granted_) {
										break;
									}
									iter++;
								}
								request_queue.get()->request_queue_.insert(iter, lock_request);
							}
							return false;
						}
					}
				}
				return true;
			});
			// 多粒度锁检查，当前加锁后，对应祖先节点也要加锁
			// granted lock<=>更新txn的lock_set
			if (txn->GetState() == TransactionState::ABORTED) {
				request_queue->request_queue_.remove(lock_request);
				request_queue.get()->cv_.notify_all();
				return false;
			}
			lock_request->granted_ = true;
			if (txn->GetTransactionId() == request_queue->upgrading_) {
				request_queue->upgrading_ = INVALID_TXN_ID;
			}
		}
		InsertOrDeleteRowLock(txn, lock_mode, oid, rid, false);
		LOG_INFO("txn:%d,lock mode:%d,oid:%d rid:%s lock succ", txn->GetTransactionId(), lock_mode, oid,
				 rid.ToString().c_str());
		return true;
	}

	auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
		// 判断有没有持有锁

		if (!txn->IsRowExclusiveLocked(oid, rid) && !txn->IsRowSharedLocked(oid, rid)) {
			txn->SetState(TransactionState::ABORTED);
			throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
		}
		std::shared_ptr<LockRequestQueue> request_queue;
		{
			std::unique_lock<std::mutex> lock(row_lock_map_latch_);
			request_queue = row_lock_map_[rid];
		}

		std::shared_ptr<LockRequest> lock_request;
		{
			std::unique_lock<std::mutex> lock(request_queue->latch_);
			for (auto &request: request_queue.get()->request_queue_) {
				if (request->granted_ && request->txn_id_ == txn->GetTransactionId() && request->rid_ == rid) {
					lock_request = request;
				}
			}
			request_queue.get()->request_queue_.remove(lock_request);


			if (txn->GetExclusiveRowLockSet()->count(oid) && txn->GetExclusiveRowLockSet()->at(oid).count(rid)) {
				txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
				if (!force)
					txn->SetState(TransactionState::SHRINKING);
			}
			if (txn->GetSharedRowLockSet()->count(oid) && txn->GetSharedRowLockSet()->at(oid).count(rid)) {
				txn->GetSharedRowLockSet()->at(oid).erase(rid);
				if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && !force)
					txn->SetState(TransactionState::SHRINKING);
				if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
					throw TransactionAbortException(txn->GetTransactionId(),
													AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
				}
			}

			// notify the request lock queue
			request_queue->cv_.notify_all();
		}
		LOG_INFO("txn:%d,oid:%d rid:%s unlock succ", txn->GetTransactionId(), oid,
				 rid.ToString().c_str());
		return true;
	}

	void LockManager::UnlockAll() {
		// You probably want to unlock all table and txn locks here.
		for (auto r: row_lock_map_) {
			for (auto &request: r.second->request_queue_)
				UnlockRow(txn_manager_->GetTransaction(request->txn_id_), request->oid_, request->rid_);
		}
		for (auto t: table_lock_map_) {
			for (auto &request: t.second->request_queue_)
				UnlockTable(txn_manager_->GetTransaction(request->txn_id_), request->oid_);
		}

	}

	void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
		for (auto &v: waits_for_[t1]) {
			if (v == t2)return;
		}
		waits_for_[t1].push_back(t2);
	}

	void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
		waits_for_[t1].erase(std::remove(waits_for_[t1].begin(), waits_for_[t1].end(), t2), waits_for_[t1].end());
	}

	auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
		// 从最小的txn id开始
		if (active_txn_set_.empty()) return false;
		auto k = active_txn_set_.begin();
		while (k != active_txn_set_.end()) {
			if (DFS(*k)) {
				for (auto t: waits_txn_set_) {
					*txn_id = std::max(*txn_id, t);
				}
				waits_txn_set_.clear();
				return true;
			}
			waits_txn_set_.clear();
			k++;
		}
		return false;
	}

	auto LockManager::DFS(txn_id_t txnId) -> bool {
		if (waits_txn_set_.count(txnId)) {
			return true;
		}
		waits_txn_set_.insert(txnId);
		for (auto &t: waits_for_[txnId]) {
			if (DFS(t)) return true;
		}
		return false;
	}

	auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
		std::scoped_lock lock(waits_for_latch_);
		std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
		for (const auto &[key, value]: waits_for_) {
			for (const auto &t: value)
				edges.push_back({key, t});
		}
		return edges;
	}

	void LockManager::RunCycleDetection() {
		while (enable_cycle_detection_) {
			std::this_thread::sleep_for(cycle_detection_interval);
			{
				std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
				std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
				txn_id_t txn_id;
				waits_for_.clear();
				active_txn_set_.clear();
				// 构建wait-graph
				for (auto r: row_lock_map_) {
					txn_id_t waits_txn_id;
					{
						std::unique_lock<std::mutex> q_lock(r.second->latch_);
						for (auto request: r.second.get()->request_queue_) {
							if (request->granted_) {
								waits_txn_id = request->txn_id_;
								active_txn_set_.insert(waits_txn_id);
								txn_row_lock_map_[request->txn_id_] = request->rid_;
							} else {
								AddEdge(request->txn_id_, waits_txn_id);
							}
						}
					}

				}
				for (auto r: table_lock_map_) {
					txn_id_t waits_txn_id;
					{
						std::unique_lock<std::mutex> q_lock(r.second->latch_);
						for (auto request: r.second.get()->request_queue_) {
							if (request->granted_) {
								waits_txn_id = request->txn_id_;
								active_txn_set_.insert(waits_txn_id);
								txn_table_lock_map_[request->txn_id_] = request->oid_;
							} else {
								AddEdge(request->txn_id_, waits_txn_id);
							}
						}
					}

				}

				if (HasCycle(&txn_id)) {
					auto tx = txn_manager_->GetTransaction(txn_id);
					tx->SetState(TransactionState::ABORTED);
					active_txn_set_.erase(txn_id);
					for (auto t: active_txn_set_) {
						RemoveEdge(t, txn_id);
					}
					if (txn_table_lock_map_.count(txn_id)) {
						// 从队列里移除锁
						auto q = table_lock_map_[txn_table_lock_map_[txn_id]].get();
						std::scoped_lock(q->latch_);
						for (auto r: q->request_queue_) {
							if (r->granted_ && r->txn_id_ == txn_id) {
								q->request_queue_.remove(r);
								break;
							}
						}
						q->cv_.notify_all();
					}
					if (txn_row_lock_map_.count(txn_id)) {
						// 从队列里移除锁
						auto q = row_lock_map_[txn_row_lock_map_[txn_id]].get();
						std::scoped_lock(q->latch_);
						for (auto r: q->request_queue_) {
							if (r->granted_ && r->txn_id_ == txn_id) {
								q->request_queue_.remove(r);
								break;
							}
						}
						q->cv_.notify_all();
					}
				}
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

		switch (txn->GetIsolationLevel()) {
			case IsolationLevel::READ_UNCOMMITTED:
				if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
					lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || txn->GetState() != TransactionState::GROWING) {
					txn->SetState(TransactionState::ABORTED);
					throw TransactionAbortException(txn->GetTransactionId(),
													AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);

				}
				break;
			case IsolationLevel::REPEATABLE_READ:
				if (txn->GetState() == TransactionState::SHRINKING) {
					txn->SetState(TransactionState::ABORTED);
					throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
				}
				break;
			case IsolationLevel::READ_COMMITTED:
				if (txn->GetState() == TransactionState::SHRINKING &&
					(lock_mode != LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
					txn->SetState(TransactionState::ABORTED);
					throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
				}
				break;
		}
		return true;
	}

	void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
		// 判断upgrading_？
		auto iter = lock_request_queue->request_queue_.begin();
		while (iter != lock_request_queue->request_queue_.end()) {
			if (!(*iter)->granted_) {
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

	// SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE
	bool LockManager::matrix_[5][5] = {{true,  false, true,  false, false},
									   {false, false, false, false, false},
									   {true,  false, true,  true,  true},
									   {false, false, true,  true,  false},
									   {false, false, true,  false, false}};
	bool LockManager::isUpgrade[5][5] = {
		// SHARED
		{false, true,  false, false, true},
		// EXCLUSIVE
		{false, false, false, false, false},
		// INTENTION_SHARED
		{true,  true,  true,  true,  true},
		// INTENTION_EXCLUSIVE
		{false, true,  false, false, true},
		// SHARED_INTENTION_EXCLUSIVE
		{false, true,  false, false, false}
	};

}  // namespace bustub
