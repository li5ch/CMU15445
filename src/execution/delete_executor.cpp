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

        auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
        while (1) {
            if (child_executor_->Next(tuple, rid)) {
                table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);
                auto index = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
                for (auto &i: index) {
                    i->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
                }
            } else {
                return false;
            }
        }
    }

}  // namespace bustub
