//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "common/util/hash_util.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
//    class SimpleJoinHashTable {
//    public:
//        /**
//         * Construct a new SimpleJoinHashTable instance.
//         * @param agg_exprs the aggregation expressions
//         * @param agg_types the types of aggregations
//         */
//        SimpleJoinHashTable(const std::vector<AbstractExpressionRef> &agg_exprs,
//                            const std::vector<AggregationType> &agg_types)
//            : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}
//
//        /** @return The initial aggregate value for this aggregation executor */
//        auto GenerateInitialAggregateValue() -> AggregateValue {
//            std::vector<Value> values{};
//            for (const auto &agg_type: agg_types_) {
//                switch (agg_type) {
//                    case AggregationType::CountStarAggregate:
//                        // Count start starts at zero.
//                        values.emplace_back(ValueFactory::GetIntegerValue(0));
//                        break;
//                    case AggregationType::CountAggregate:
//                    case AggregationType::SumAggregate:
//                    case AggregationType::MinAggregate:
//                    case AggregationType::MaxAggregate:
//                        // Others starts at null.
//                        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
//                        break;
//                }
//            }
//            return {values};
//        }
//
//        /**
//         * TODO(Student)
//         *
//         * Combines the input into the aggregation result.
//         * @param[out] result The output aggregate value
//         * @param input The input value
//         */
//        void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
//            for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
//                switch (agg_types_[i]) {
//                    case AggregationType::CountStarAggregate:
//                        result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
//                        break;
//                    case AggregationType::CountAggregate:
//                        if (result->aggregates_[i].IsNull()) {
//                            result->aggregates_[i] = ValueFactory::GetIntegerValue(0);
//                        }
//                        if (!input.aggregates_[i].IsNull()) {
//                            result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
//                        }
//                        break;
//                    case AggregationType::SumAggregate:
//                        if (result->aggregates_[i].IsNull()) {
//                            result->aggregates_[i] = input.aggregates_[i];
//                        } else if (!input.aggregates_[i].IsNull()) {
//                            result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
//                        }
//                        break;
//                    case AggregationType::MinAggregate:
//                        if (result->aggregates_[i].IsNull()) {
//                            result->aggregates_[i] = input.aggregates_[i];
//                        } else if (!input.aggregates_[i].IsNull()) {
//                            result->aggregates_[i] = result->aggregates_[i].Min(input.aggregates_[i]);
//                        }
//                        break;
//                    case AggregationType::MaxAggregate:
//                        if (result->aggregates_[i].IsNull()) {
//                            result->aggregates_[i] = input.aggregates_[i];
//                        } else if (!input.aggregates_[i].IsNull()) {
//                            result->aggregates_[i] = result->aggregates_[i].Max(input.aggregates_[i]);
//                        }
//                        break;
//                }
//            }
//        }
//
//        /**
//         * Inserts a value into the hash table and then combines it with the current aggregation.
//         * @param agg_key the key to be inserted
//         * @param agg_val the value to be inserted
//         */
//        void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
//            if (ht_.count(agg_key) == 0) {
//                ht_.insert({agg_key, GenerateInitialAggregateValue()});
//            }
//            CombineAggregateValues(&ht_[agg_key], agg_val);
//        }
//
//        void Initial() {
//            ht_.insert({{std::vector<Value>()}, GenerateInitialAggregateValue()});
//        }
//
//        /**
//         * Clear the hash table
//         */
//        void Clear() { ht_.clear(); }
//
//        /** An iterator over the aggregation hash table */
//        class Iterator {
//        public:
//            /** Creates an iterator for the aggregate map. */
//            explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}
//
//            /** @return The key of the iterator */
//            auto Key() -> const AggregateKey & { return iter_->first; }
//
//            /** @return The value of the iterator */
//            auto Val() -> const AggregateValue & { return iter_->second; }
//
//            /** @return The iterator before it is incremented */
//            auto operator++() -> Iterator & {
//                ++iter_;
//                return *this;
//            }
//
//            /** @return `true` if both iterators are identical */
//            auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }
//
//            /** @return `true` if both iterators are different */
//            auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }
//
//        private:
//            /** Aggregates map */
//            std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
//        };
//
//        /** @return Iterator to the start of the hash table */
//        auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }
//
//        /** @return Iterator to the end of the hash table */
//        auto End() -> Iterator { return Iterator{ht_.cend()}; }
//
//        auto Size() -> size_t { return ht_.size(); }
//
//    private:
//        /** The hash table is just a map from aggregate keys to aggregate values */
//        std::unordered_map<AggregateKey, AggregateValue> ht_{};
//        /** The aggregate expressions that we have */
//        const std::vector<AbstractExpressionRef> &agg_exprs_;
//        /** The types of aggregations that we have */
//        const std::vector<AggregationType> &agg_types_;
//    };

    class HashJoinExecutor : public AbstractExecutor {
    public:
        /**
         * Construct a new HashJoinExecutor instance.
         * @param exec_ctx The executor context
         * @param plan The HashJoin join plan to be executed
         * @param left_child The child executor that produces tuples for the left side of join
         * @param right_child The child executor that produces tuples for the right side of join
         */
        HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_child,
                         std::unique_ptr<AbstractExecutor> &&right_child);

        /** Initialize the join */
        void Init() override;

        /**
         * Yield the next tuple from the join.
         * @param[out] tuple The next tuple produced by the join.
         * @param[out] rid The next tuple RID, not used by hash join.
         * @return `true` if a tuple was produced, `false` if there are no more tuples.
         */
        auto Next(Tuple *tuple, RID *rid) -> bool override;

        /** @return The output schema for the join */
        auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

    private:
        /** The NestedLoopJoin plan node to be executed. */
        const HashJoinPlanNode *plan_;
        std::unique_ptr<AbstractExecutor> left_child;
        std::unique_ptr<AbstractExecutor> right_child;
        std::unordered_map<hash_t, std::vector<Tuple>> ht_{};
        int idx{-1};
        std::unordered_map<hash_t, std::vector<Tuple>::iterator> ht_iter_{};
        Tuple left_tuple;
    };

}  // namespace bustub
