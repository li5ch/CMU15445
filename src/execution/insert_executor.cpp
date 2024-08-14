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
        if (is_end_) return false;
        Tuple to_insert_tuple{};
        RID emit_rid;
        int32_t insert_count = 0;
        auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
        while (1) {
            if (child_executor_->Next(&to_insert_tuple, &emit_rid)) {
                auto r = table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID,
                                                                   INVALID_TXN_ID, false}, to_insert_tuple,
                                                         exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
                                                         plan_->GetTableOid());
                auto index = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);

                std::for_each(index.begin(), index.end(),
                              [&to_insert_tuple, &r, &table_info, &exec_ctx = exec_ctx_](IndexInfo *i) {
                                  i->index_->InsertEntry(
                                      to_insert_tuple.KeyFromTuple(table_info->schema_, i->key_schema_,
                                                                   i->index_->GetKeyAttrs()), *r,
                                      exec_ctx->GetTransaction());
                              });

                for (auto &i: index) {
                    auto k = to_insert_tuple.KeyFromTuple(table_info->schema_, i->key_schema_,
                                                          i->index_->GetKeyAttrs());
                    auto result = i->index_->InsertEntry(k, *r, exec_ctx_->GetTransaction());
                    BUSTUB_ENSURE(result, "InsertEntry cannot fail");
                }
                if (r != std::nullopt) {
                    insert_count++;
                }
                BUSTUB_ENSURE(r != std::nullopt, "Sequential insertion cannot fail");

            } else {
                is_end_ = true;
                std::vector<Value> values{};
                values.reserve(GetOutputSchema().GetColumnCount());
                values.emplace_back(TypeId::INTEGER, insert_count);
                *tuple = Tuple{values, &GetOutputSchema()};
                return true;
            }
        }


    }

}  // namespace bustub
