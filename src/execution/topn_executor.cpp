#include "execution/executors/topn_executor.h"

namespace bustub {

	TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
							   std::unique_ptr<AbstractExecutor> &&child_executor)
		: AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

	void TopNExecutor::Init() {
		child_executor_->Init();
		Tuple t;
		RID r;
		auto cmp = [&](const Tuple &a, const Tuple &b) {
			for (auto &expr: plan_->GetOrderBy()) {
				switch (expr.first) {
					case OrderByType::ASC:
						if (static_cast<bool>(expr.second->Evaluate(&a, plan_->OutputSchema()).
							CompareLessThan(expr.second->Evaluate(&b, plan_->OutputSchema())))) {
							return true;
						}
						if (static_cast<bool>(expr.second->Evaluate(&a, plan_->OutputSchema()).
							CompareGreaterThan(expr.second->Evaluate(&b, plan_->OutputSchema())))) {
							return false;
						}
						break;
					case OrderByType::DEFAULT:
						if (static_cast<bool>(expr.second->Evaluate(&a, plan_->OutputSchema()).
							CompareLessThan(expr.second->Evaluate(&b, plan_->OutputSchema())))) {
							return true;
						}
						if (static_cast<bool>(expr.second->Evaluate(&a, plan_->OutputSchema()).
							CompareGreaterThan(expr.second->Evaluate(&b, plan_->OutputSchema())))) {
							return false;
						}
						break;
					case OrderByType::INVALID:
					case OrderByType::DESC:
						if (static_cast<bool>(expr.second->Evaluate(&a, plan_->OutputSchema()).
							CompareLessThan(expr.second->Evaluate(&b, plan_->OutputSchema())))) {
							return false;
						}
						if (static_cast<bool>(expr.second->Evaluate(&a, plan_->OutputSchema()).
							CompareGreaterThan(expr.second->Evaluate(&b, plan_->OutputSchema())))) {
							return true;
						}
						break;
				}
			}
			return false;
		};
		std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
		while (child_executor_->Next(&t, &r)) {
			if (pq.size() == plan_->GetN()) {
				auto u = pq.top();
				if (cmp(t, u)) {
					pq.push(t);
					pq.pop();
				}
			} else if (pq.size() < plan_->GetN()) {
				pq.push(t);
			}
		}

		tuples.resize(plan_->GetN());
		auto i = int(plan_->GetN()) - 1;
		while (!pq.empty()) {
			const auto &u = pq.top();
			LOG_INFO("%d", i);
			tuples[i--] = u;
			pq.pop();
		}
		iter = tuples.begin();
	}

	auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
		if (iter != tuples.end()) {
			*tuple = *iter;
			*rid = tuple->GetRid();
			iter++;
			return true;
		}
		return false;
	}

	auto TopNExecutor::GetNumInHeap() -> size_t {
		return tuples.size();
	};

}  // namespace bustub
