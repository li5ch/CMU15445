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
#include "type/value_factory.h"

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
        auto index_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
        auto expr = dynamic_cast<const ColumnValueExpression *>( plan_->KeyPredicate().get());
        while (child_exe_->Next(&t, &r)) {
            auto key = t.GetValue(&child_exe_->GetOutputSchema(), expr->GetColIdx());
            std::vector<RID> result;
            Tuple key_tuple = Tuple({key}, &exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->key_schema_);
            tree_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
            for (auto &pair: result) {
                auto res = index_info->table_->GetTuple(pair);
                std::vector<Value> values;
                values.reserve(
                    plan_->InnerTableSchema().GetColumnCount() + child_exe_->GetOutputSchema().GetColumnCount());
                for (size_t i = 0; i < child_exe_->GetOutputSchema().GetColumnCount(); i++) {
                    values.push_back(t.GetValue(&child_exe_->GetOutputSchema(), i));
                }
                for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
                    values.push_back(res.second.GetValue(&plan_->InnerTableSchema(), i));
                }
                *tuple = Tuple{values, &GetOutputSchema()};
                return true;
            }
            if (plan_->GetJoinType() == JoinType::LEFT) {
                std::vector<Value> values;
                values.reserve(
                    plan_->InnerTableSchema().GetColumnCount() + child_exe_->GetOutputSchema().GetColumnCount());
                for (size_t i = 0; i < child_exe_->GetOutputSchema().GetColumnCount(); i++) {
                    values.push_back(t.GetValue(&child_exe_->GetOutputSchema(), i));
                }
                for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
                    values.push_back(
                        ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
                }
                *tuple = Tuple{values, &GetOutputSchema()};
                return true;
            }
        }
        return false;
    }

}  // namespace bustub
