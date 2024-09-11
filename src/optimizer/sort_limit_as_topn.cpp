#include "optimizer/optimizer.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/limit_plan.h"

namespace bustub {

    auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
        // TODO(student): implement sort + limit -> top N optimizer rule
        std::vector<AbstractPlanNodeRef> children;
        for (const auto &child: plan->GetChildren()) {
            children.emplace_back(OptimizeSortLimitAsTopN(child));
        }
        auto optimized_plan = plan->CloneWithChildren(std::move(children));

        if (optimized_plan->GetType() == PlanType::Limit && !optimized_plan->GetChildren().empty() &&
            optimized_plan->GetChildren()[0]->GetType() == PlanType::Sort) {
            auto sort_plan = reinterpret_cast<const SortPlanNode *>(optimized_plan->GetChildren()[0].get());
            auto limit_plan = reinterpret_cast<const LimitPlanNode *>(optimized_plan.get());

            return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan->GetChildPlan(),
                                                  sort_plan->GetOrderBy(),
                                                  limit_plan->GetLimit());

        }

        return optimized_plan;
    }

}  // namespace bustub
