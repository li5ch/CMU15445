//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

	SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(
		exec_ctx), plan_(plan) {
	}

	void SeqScanExecutor::Init() {
		auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
		auto lock_mode = LockManager::LockMode::INTENTION_SHARED;
		if (GetExecutorContext()->IsDelete())lock_mode = LockManager::LockMode::INTENTION_EXCLUSIVE;
		try {
			if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
				exec_ctx_->GetLockManager()->LockTable(GetExecutorContext()->GetTransaction(),
													   lock_mode, table_info->oid_);

			}
			iter_.emplace(table_info->table_->MakeEagerIterator());
		} catch (const TransactionAbortException &e) {
			LOG_ERROR("TransactionAbortException: %s", e.what());
			throw ExecutionException("lock failed");
		}

	}

	auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
		// 加锁，不满足的解锁
		auto lock_mode = LockManager::LockMode::SHARED;
		if (GetExecutorContext()->IsDelete()) {  // 假设已实现GetIsDelete
			lock_mode = LockManager::LockMode::EXCLUSIVE;
		}
		auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());

		while (!iter_->IsEnd()) {
			if (iter_->GetTuple().first.is_deleted_) {
				++(*iter_);
				continue;
			}

			try {
				bool res = true;
				if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED)
					res = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
															   lock_mode,
															   table_info->oid_,
															   iter_->GetRID());

				if (res) {
					if (plan_->filter_predicate_ != nullptr) {
						auto t = iter_->GetTuple().second;
						auto value = plan_->filter_predicate_->Evaluate(&t, table_info->schema_);
						if (!value.IsNull() && value.GetAs<bool>()) {
							*tuple = iter_->GetTuple().second;
							*rid = iter_->GetRID();
							UnlockRowIfRequired(exec_ctx_->GetTransaction(), table_info->oid_, iter_->GetRID());
							++(*iter_);
							return true;
						} else {
							UnlockRowIfRequired(exec_ctx_->GetTransaction(), table_info->oid_, iter_->GetRID());
							++(*iter_);
						}
					} else {
						*tuple = iter_->GetTuple().second;
						*rid = iter_->GetRID();
						UnlockRowIfRequired(exec_ctx_->GetTransaction(), table_info->oid_, iter_->GetRID());
						++(*iter_);

						return true;
					}
				} else {
					throw ExecutionException("SeqScan Executor Get Table Lock Failed");
				}

			} catch (TransactionAbortException &e) {
				LOG_ERROR("TransactionAbortException: %s", e.GetInfo().c_str());
				throw ExecutionException("SeqScan Executor TransactionAbortException: " + std::string(e.what()));
			} catch (...) {

				throw ExecutionException("SeqScan Executor Unknown Exception");
			}
		}
		return false;
	}

	void SeqScanExecutor::UnlockRowIfRequired(Transaction *txn, oid_t table_oid, const RID &rid) {
		if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
			if (!exec_ctx_->GetLockManager()->UnlockRow(txn, table_oid, rid)) {
				throw ExecutionException("SeqScan Executor Get Table ULock Failed");
			}
		}
	}

}  // namespace bustub
