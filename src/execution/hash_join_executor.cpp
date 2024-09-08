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
#include "execution/expressions/column_value_expression.h"
#include "type/value_factory.h"
#include "execution/expressions/comparison_expression.h"

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
		Tuple t;
		RID r;

		while (right_child->Next(&t, &r)) {
			size_t cur_hash = 0;
			for (const auto &expr: plan_->RightJoinKeyExpressions()) {
				auto k = expr->Evaluate(&t, right_child->GetOutputSchema());
				if (!k.IsNull())
					cur_hash = HashUtil::CombineHashes(cur_hash, HashUtil::HashValue(&k));
			}
			ht_[cur_hash].push_back(t);
			LOG_INFO("right hash table tt%zu", cur_hash);
		}
	}

	auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
		Tuple t;
		RID r;

		while (left_child->Next(&t, &r)) {
			size_t cur_hash = 0;
			for (const auto &expr: plan_->LeftJoinKeyExpressions()) {
				auto k = expr->Evaluate(&t, left_child->GetOutputSchema());
				if (!k.IsNull())
					cur_hash = HashUtil::CombineHashes(cur_hash, HashUtil::HashValue(&k));
			}
			if (ht_.count(cur_hash)) {
				LOG_INFO("left hash table tt%zu", cur_hash);
				for (auto &right_tuple: ht_[cur_hash]) {

				}

				int idx = 0;
				bool founded = true;
				for (const auto &expr: plan_->LeftJoinKeyExpressions()) {
					ex

					auto cexpr = ComparisonExpression(expr, plan_->RightJoinKeyExpressions()[idx++],
													  ComparisonType::Equal);
					auto k = cexpr.EvaluateJoin(&t, left_child->GetOutputSchema(), &ht_[cur_hash],
												right_child->GetOutputSchema());
					if (k.IsNull() || !k.GetAs<bool>()) {
						founded = false;
						break;
					}
				}
				if (founded) {
					std::vector<Value> values;
					values.reserve(
						left_child->GetOutputSchema().GetColumnCount() +
						right_child->GetOutputSchema().GetColumnCount());
					for (uint32_t i = 0; i < left_child->GetOutputSchema().GetColumnCount(); ++i) {
						values.push_back(t.GetValue(&left_child->GetOutputSchema(), i));
					}
					for (uint32_t i = 0; i < right_child->GetOutputSchema().GetColumnCount(); ++i) {
						values.push_back(ht_[cur_hash].GetValue(&right_child->GetOutputSchema(), i));
					}

					*tuple = Tuple{values, &GetOutputSchema()};
					return true;
				}

			}
			if (JoinType::LEFT == plan_->GetJoinType()) {
				std::vector<Value> values;
				values.reserve(
					left_child->GetOutputSchema().GetColumnCount() + right_child->GetOutputSchema().GetColumnCount());
				for (uint32_t i = 0; i < left_child->GetOutputSchema().GetColumnCount(); ++i) {
					values.push_back(t.GetValue(&left_child->GetOutputSchema(), i));
				}
				for (uint32_t i = 0; i < right_child->GetOutputSchema().GetColumnCount(); ++i) {
					values.push_back(
						ValueFactory::GetNullValueByType(right_child->GetOutputSchema().GetColumn(i).GetType()));
				}

				*tuple = Tuple{values, &GetOutputSchema()};
				return true;
			}

		}
		return false;
	}

}  // namespace bustub
