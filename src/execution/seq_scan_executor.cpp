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
        iter_.emplace(table_info->table_->MakeIterator());
    }

    auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
        while (!iter_->IsEnd()) {
            if (iter_->GetTuple().first.is_deleted_) {
                ++(*iter_);
            } else {
                *tuple = iter_->GetTuple().second;
                *rid = iter_->GetRID();
                ++(*iter_);
                return true;
            }
        }
        return false;
    }

}  // namespace bustub
