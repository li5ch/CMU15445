#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
#include "execution/expressions/logic_expression.h"

namespace bustub {

	auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
		std::vector<AbstractPlanNodeRef> children;
		for (const auto &child: plan->GetChildren()) {
			children.emplace_back(OptimizeNLJAsHashJoin(child));
		}
		auto optimized_plan = plan->CloneWithChildren(std::move(children));

		if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
			const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
			// Has exactly two children
			std::vector<AbstractExpressionRef> left_key_expr, right_key_expr;
			BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
			auto f = [&](const ComparisonExpression *expr) {
				if (expr->comp_type_ == ComparisonType::Equal) {
					if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
						left_expr != nullptr) {
						if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
							right_expr != nullptr) {
							// Ensure both exprs have tuple_id == 0
							auto left_expr_tuple_0 =
								std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(),
																		left_expr->GetReturnType());
							auto right_expr_tuple_0 =
								std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(),
																		right_expr->GetReturnType());
							// Now it's in form of <column_expr> = <column_expr>. Let's match an index for them.
							if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
								left_key_expr.push_back(left_expr_tuple_0);
								right_key_expr.push_back(right_expr_tuple_0);
							}
							if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
								left_key_expr.push_back(right_expr_tuple_0);
								right_key_expr.push_back(left_expr_tuple_0);
							}
							// Ensure right child is table scan
						}
					}
				}
				return plan;
			};

			// Check if expr is equal condition where one is for the left table, and one is for the right table.
			if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr !=
																									  nullptr) {
				if (expr->logic_type_ == LogicType::And) {
					if (const auto *left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
						left_expr != nullptr) {
						f(left_expr);

						if (const auto *right_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
							right_expr != nullptr) {
							f(right_expr);
						}
					}

					return std::make_shared<HashJoinPlanNode>(
						nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
						nlj_plan.GetRightPlan(),
						std::vector<AbstractExpressionRef>{std::move(left_key_expr)},
						std::vector<AbstractExpressionRef>{std::move(right_key_expr)},
						nlj_plan.GetJoinType());

				}
			}

			if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr !=
																										   nullptr) {
				if (expr->comp_type_ == ComparisonType::Equal) {
					if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
						left_expr != nullptr) {
						if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
							right_expr != nullptr) {
							// Ensure both exprs have tuple_id == 0
							auto left_expr_tuple_0 =
								std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(),
																		left_expr->GetReturnType());
							auto right_expr_tuple_0 =
								std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(),
																		right_expr->GetReturnType());
							// Now it's in form of <column_expr> = <column_expr>. Let's match an index for them.
							if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
								return std::make_shared<HashJoinPlanNode>(
									nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
									nlj_plan.GetRightPlan(),
									std::vector<AbstractExpressionRef>{std::move(left_expr_tuple_0)},
									std::vector<AbstractExpressionRef>{std::move(right_expr_tuple_0)},
									nlj_plan.GetJoinType());
							}
							if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
								return std::make_shared<HashJoinPlanNode>(
									nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
									nlj_plan.GetRightPlan(),
									std::vector<AbstractExpressionRef>{std::move(right_expr_tuple_0)},
									std::vector<AbstractExpressionRef>{std::move(left_expr_tuple_0)},
									nlj_plan.GetJoinType());
							}
							// Ensure right child is table scan

						}
					}
				}
			}
		}

		// TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
		// Note for 2023 Spring: You should at least support join keys of the form:
		// 1. <column expr> = <column expr>
		// 2. <column expr> = <column expr> AND <column expr> = <column expr>
		return plan;
	}


}  // namespace bustub
