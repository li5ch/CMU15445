#include "execution/executors/sort_executor.h"

namespace bustub {

    SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

    void SortExecutor::Init() {
        child_executor_->Init();
        Tuple t;
        RID r;
        tuples.clear();
        while (child_executor_->Next(&t, &r)) {
            tuples.push_back(t);
        }
        std::sort(tuples.begin(), tuples.end(),
                  [&](const Tuple &a,
                      const Tuple &b) {

                      for (const auto &expr: plan_->GetOrderBy()) {
                          if (expr.first == OrderByType::ASC || expr.first == OrderByType::DEFAULT) {
                              if (static_cast<bool>(expr.second->Evaluate(&a,
                                                                          child_executor_->GetOutputSchema()).CompareLessThan(
                                  expr.second->Evaluate(&b, child_executor_->GetOutputSchema()))))
                                  return true;
                              else if (static_cast<bool>(expr.second->Evaluate(&a,
                                                                               child_executor_->GetOutputSchema()).CompareGreaterThan(
                                  expr.second->Evaluate(&b, child_executor_->GetOutputSchema())))) {
                                  return false;
                              }
                          }
                          if (expr.first == OrderByType::DESC) {
                              if (static_cast<bool>(expr.second->Evaluate(&a,
                                                                          child_executor_->GetOutputSchema()).CompareLessThan(
                                  expr.second->Evaluate(&b, child_executor_->GetOutputSchema()))))
                                  return false;
                              else if (static_cast<bool>(expr.second->Evaluate(&a,
                                                                               child_executor_->GetOutputSchema()).CompareGreaterThan(
                                  expr.second->Evaluate(&b, child_executor_->GetOutputSchema())))) {
                                  return true;
                              }
                          }

                      }
                      return false;
                  });

        iter = tuples.begin();
    }

    auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
        if (iter != tuples.end()) {
            *tuple = (*iter);
            *rid = (*iter).GetRid();
            iter++;
            return true;
        }
        return false;
    }

}  // namespace bustub
