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
#include "type/value_factory.h"

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
		Tuple left;
		RID left_rid;

		while (left_executor_->Next(&left, &left_rid)) {
			right_executor_->Init();
			Tuple t;
			RID rrid;
			while (right_executor_->Next(&t, &rrid)) {
				right_tuple_.push_back(t);
			}
			for (uint32_t idx = 0; idx < right_tuple_.size(); idx++) {

				auto value = plan_->Predicate()->EvaluateJoin(&left, left_executor_->GetOutputSchema(),
															  &right_tuple_[idx],
															  right_executor_->GetOutputSchema());
				if (!value.IsNull() && value.GetAs<bool>()) {

					std::vector<Value> values;
					for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
						values.push_back(left.GetValue(&left_executor_->GetOutputSchema(), i));
					}
					for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
						values.push_back(right_tuple_[idx].GetValue(&right_executor_->GetOutputSchema(), i));
					}

					*tuple = Tuple{values, &GetOutputSchema()};
					return true;
				}
			}
			if (plan_->GetJoinType() == JoinType::LEFT) {
				std::vector<Value> values;
				for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
					values.push_back(left.GetValue(&left_executor_->GetOutputSchema(), i));
				}
				for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
					values.push_back(
						ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
				}

				*tuple = Tuple{values, &GetOutputSchema()};
				return true;
			}
		}

		return false;
	}

}  // namespace bustub
