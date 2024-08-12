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
        auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
        while (1) {
            if (child_executor_->Next(tuple, rid)) {
                std::vector<Value> values;
                values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
                for (auto e: plan_->target_expressions_) {
                    values.push_back(e->Evaluate(tuple, child_executor_->GetOutputSchema()));
                }
                auto to_update_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
                table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);
                table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID,
                                                          INVALID_TXN_ID, false}, to_update_tuple,
                                                exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
                                                plan_->GetTableOid());
                auto index = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
                for (auto &i: index) {
                    i->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
                    auto k = tuple->KeyFromTuple(table_info->schema_, i->key_schema_, i->index_->GetKeyAttrs());
                    auto result = i->index_->InsertEntry(k, *rid, exec_ctx_->GetTransaction());
                    BUSTUB_ENSURE(result != true, "InsertEntry cannot fail");
                }
            } else {
                return false;
            }
        }
    }

}  // namespace bustub
