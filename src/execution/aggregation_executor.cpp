//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();

  Tuple tuple;
  RID rid;
  // 将每个 tuple 分配到指定的聚合分组，并更新该分组的聚合值
  while (child_->Next(&tuple, &rid)) {
    // 获取该 tuple 对应聚合键，这个聚合键表明了它的所属分组，聚合键的本质是用于分组的列的值
    AggregateKey agg_key = this->MakeAggregateKey(&tuple);
    // 获取该 tuple 对应的聚合值，聚合值的本质是待聚合的列的值
    AggregateValue agg_value = this->MakeAggregateValue(&tuple);
    // 将该 tuple 的聚合值合并到对应聚合分组的聚合值中，对于不同的聚合函数有不同的聚合方式
    aht_.InsertCombine(agg_key, agg_value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    // 如果存在 group by 子句，则不需要对每个分组的聚合结果进行进一步的聚合
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }

    AggregateValue result_value;
    // 如果对空表执行聚合操作直接返回初始化后的元组
    if (aht_.CheckIsNullTable(&result_value)) {
      *tuple = Tuple(result_value.aggregates_, &plan_->OutputSchema());
      *rid = tuple->GetRid();
      return true;
    }

    return false;
  }

  // 获取当前迭代器对应聚合分组的聚合键和和聚合值
  AggregateKey agg_key = aht_iterator_.Key();
  AggregateValue agg_value = aht_iterator_.Val();
  std::vector<Value> values;
  values.reserve(agg_key.group_bys_.size() + agg_value.aggregates_.size());

  // 向结果集中追加聚合分组的键
  for (const auto &group_by_key : agg_key.group_bys_) {
    values.emplace_back(group_by_key);
  }
  // 向结果集中追加聚合分组的聚合值
  for (const auto &group_by_value : agg_value.aggregates_) {
    values.emplace_back(group_by_value);
  }

  *tuple = Tuple(values, &plan_->OutputSchema());
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
