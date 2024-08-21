//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

    NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                                   std::unique_ptr<AbstractExecutor> &&left_executor,
                                                   std::unique_ptr<AbstractExecutor> &&right_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)),
          right_executor_(std::move(right_executor)) {
        if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
            // Note for 2023 Spring: You ONLY need to implement left join and inner join.
            throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
        }
    }

    void NestedLoopJoinExecutor::Init() {
        left_executor_->Init();
        right_executor_->Init();
    }

    auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
        auto filter_expr = plan_->GetLeftPlan();
        Tuple left, right;
        RID left_rid, right_rid;
        while (left_executor_->Next(&left, &left_rid)) {
            while (right_executor_->Next(&right, &right_rid)) {

                auto value = plan_->Predicate()->EvaluateJoin(&left, left_executor_->GetOutputSchema(), &right,
                                                              right_executor_->GetOutputSchema());
                if()
            }
        }

        return false;
    }

}  // namespace bustub
