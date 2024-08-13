//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

	InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
								   std::unique_ptr<AbstractExecutor> &&child_executor)
		: AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

	void InsertExecutor::Init() {
		child_executor_->Init();
	}

	auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
		auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
		while (1) {
			if (child_executor_->Next(tuple, rid)) {
				auto r = table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID,
																   INVALID_TXN_ID, false}, *tuple,
														 exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
														 plan_->GetTableOid());
				auto index = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);

				for (auto &i: index) {
					auto k = tuple->KeyFromTuple(table_info->schema_, i->key_schema_, i->index_->GetKeyAttrs());
					auto result = i->index_->InsertEntry(k, *r, exec_ctx_->GetTransaction());
					BUSTUB_ENSURE(result, "InsertEntry cannot fail");
				}
				BUSTUB_ENSURE(r != std::nullopt, "Sequential insertion cannot fail");
			} else {
				return false;
			}
		}


	}

}  // namespace bustub
