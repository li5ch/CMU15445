//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

    HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                       std::unique_ptr<AbstractExecutor> &&left_child,
                                       std::unique_ptr<AbstractExecutor> &&right_child)
        : AbstractExecutor(exec_ctx), plan_(plan), left_child(std::move(left_child)),
          right_child(std::move(right_child)) {
        if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
            // Note for 2023 Spring: You ONLY need to implement left join and inner join.
            throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
        }
    }

    void HashJoinExecutor::Init() {
        left_child->Init();
        right_child->Init();
    }

    auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

        return false;
    }

}  // namespace bustub
