//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

    NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_exe_(std::move(child_executor)),
          tree_{dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(exec_ctx_->GetCatalog()->GetIndex(
              plan_->GetIndexOid())->index_.get())} {
        if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
            // Note for 2023 Spring: You ONLY need to implement left join and inner join.
            throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
        }
    }

    void NestIndexJoinExecutor::Init() {
        child_exe_->Init();
    }

    auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
        Tuple t;
        RID r;
        auto index_info = exec_ctx_->GetCatalog()->GetTable(exec_ctx_->GetCatalog()->GetIndex(
            plan_->GetIndexOid())->table_name_);
        auto expr = dynamic_cast<const ColumnValueExpression*>( plan_->KeyPredicate());
        while (child_exe_->Next(&t, &r)) {
            auto key = t.GetValue(child_exe_->GetOutputSchema(),.)
            tree_->ScanKey()
        }
        return false;
    }

}  // namespace bustub
