//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
	IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
		: AbstractExecutor(exec_ctx), plan_(plan),
		  tree_{dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(exec_ctx_->GetCatalog()->GetIndex(
			  plan_->GetIndexOid())->index_.get())}, iter_{tree_->GetBeginIterator()} {}

	void IndexScanExecutor::Init() {
		tableInfo = exec_ctx_->GetCatalog()->GetTable(exec_ctx_->GetCatalog()->GetIndex(
			plan_->GetIndexOid())->table_name_);
	}

	auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {

		while (!iter_.IsEnd()) {
			const auto &pair = *iter_;
			auto res = tableInfo->table_->GetTuple(pair.second);
			*rid = pair.second;
			if (res.first.is_deleted_) {
				++iter_;
			} else {
				*tuple = res.second;
				++iter_;
				return true;
			}

		}
		return false;
	}

}  // namespace bustub
