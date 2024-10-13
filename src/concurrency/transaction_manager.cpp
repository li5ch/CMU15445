//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"

namespace bustub {

	void TransactionManager::Commit(Transaction *txn) {
		// Release all the locks.
		ReleaseLocks(txn);

		txn->SetState(TransactionState::COMMITTED);
	}

	void TransactionManager::Abort(Transaction *txn) {
		/* revert all the changes in write set */
		for (auto &record: *txn->GetWriteSet()) {
			if (record.table_heap_->GetTuple(record.rid_).first.is_deleted_) {
				record.table_heap_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, txn->GetTransactionId(), false},
													record.rid_);
			} else {
				record.table_heap_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, record.rid_);
			}
		}
		std::for_each(txn->GetIndexWriteSet()->begin(), txn->GetIndexWriteSet()->end(),
					  [&](IndexWriteRecord i) {
						  if (i.wtype_ == WType::DELETE)
							  i.catalog_->GetIndex(i.index_oid_)->index_->InsertEntry(i.tuple_, i.rid_, txn);
						  else {
							  i.catalog_->GetIndex(i.index_oid_)->index_->DeleteEntry(i.tuple_, i.rid_, txn);
						  }
					  });
		ReleaseLocks(txn);
		txn->SetState(TransactionState::ABORTED);
	}

	void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

	void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
