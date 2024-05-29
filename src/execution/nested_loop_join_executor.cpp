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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  // 从 left_executor_ 和 right_executor_ 中获取两张表中的所有元组
  Tuple tuple;
  RID rid;
  std::vector<Tuple> left_tuples;
  std::vector<Tuple> right_tuples;
  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples.emplace_back(tuple);
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples.emplace_back(tuple);
  }

  // 依次连接 left_tuples 和 right_tuples 中的所有元组
  Schema left_schema = left_executor_->GetOutputSchema();
  Schema right_schema = right_executor_->GetOutputSchema();
  for (auto &left_tuple : left_tuples) {
    // 标识 left_tuple 是否需要进行左外连接
    bool need_left_join = true;

    for (auto &right_tuple : right_tuples) {
      // 如果 left_tuple 中的连接字段与 right_tuple 中的连接字段的值相匹配，则 left_tuple 不需要进行左外连接
      Value join_result = plan_->predicate_->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      if (!join_result.IsNull() && join_result.GetAs<bool>()) {
        // left_tuple 不需要进行左外连接
        need_left_join = false;

        // 依次将 left_tuple 和 right_tuple 中的值追加到结果集中
        std::vector<Value> values;
        for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.emplace_back(left_tuple.GetValue(&left_schema, i));
        }
        for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_schema, i));
        }
        results_.emplace(values, &plan_->OutputSchema());
      }
    }

    // 对 left_tuple 进行左外连接，right_tuple 中的字段均以对应的空值追加到结果集中
    if (need_left_join && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_schema, i));
      }
      for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      results_.emplace(values, &plan_->OutputSchema());
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 从结果集队列中依次获取所有连接后的元组
  if (results_.empty()) {
    return false;
  }
  *tuple = results_.front();
  results_.pop();
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
