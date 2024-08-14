//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

    DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

    void DeleteExecutor::Init() {
        child_executor_->Init();
    }

    auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
        if (is_end_) return false;
        Tuple delete_tuple;
        RID empty_rid;
        int delete_count = 0;
        auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
        while (1) {
            if (child_executor_->Next(&delete_tuple, &empty_rid)) {
                table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, empty_rid);
                auto index = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
                for (auto &i: index) {
                    auto k = delete_tuple.KeyFromTuple(table_info->schema_, i->key_schema_,
                                                       i->index_->GetKeyAttrs());
                    i->index_->DeleteEntry(k, empty_rid, exec_ctx_->GetTransaction());
                }
                delete_count++;
            } else {
                std::vector<Value> values{};
                values.reserve(GetOutputSchema().GetColumnCount());
                values.emplace_back(TypeId::INTEGER, delete_count);
                *tuple = Tuple{values, &GetOutputSchema()};
                is_end_ = true;
                return true;
            }
        }
    }

}  // namespace bustub
