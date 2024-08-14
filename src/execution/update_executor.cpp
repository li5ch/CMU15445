//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

	UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
								   std::unique_ptr<AbstractExecutor> &&child_executor)
		: AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
		// As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
	}

	void UpdateExecutor::Init() {
		child_executor_->Init();
	}

	auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
		if (is_end) return false;
		int32_t update_count{0};
		Tuple is_update_tuple;
		RID empty_rid;
		auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
		while (1) {
			if (child_executor_->Next(&is_update_tuple, &empty_rid)) {
				std::vector<Value> values;
				values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
				for (auto e: plan_->target_expressions_) {
					values.push_back(e->Evaluate(&is_update_tuple, child_executor_->GetOutputSchema()));
				}
				auto to_update_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
				table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, empty_rid);
				table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID,
														  INVALID_TXN_ID, false}, to_update_tuple,
												exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
												plan_->GetTableOid());
				update_count++;
				auto index = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
				for (auto &i: index) {

					auto k = is_update_tuple.KeyFromTuple(table_info->schema_, i->key_schema_,
														  i->index_->GetKeyAttrs());
					i->index_->DeleteEntry(k, empty_rid, exec_ctx_->GetTransaction());
					auto result = i->index_->InsertEntry(
						to_update_tuple.KeyFromTuple(table_info->schema_, i->key_schema_,
													 i->index_->GetKeyAttrs()), empty_rid, exec_ctx_->GetTransaction());
					BUSTUB_ENSURE(result, "InsertEntry cannot fail");
				}
			} else {
				std::vector<Value> values{};
				values.reserve(GetOutputSchema().GetColumnCount());
				values.emplace_back(TypeId::INTEGER, update_count);
				*tuple = Tuple{values, &GetOutputSchema()};
				is_end = true;
				return true;
			}
		}
	}

}  // namespace bustub
