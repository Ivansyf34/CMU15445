#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_->Init();

  Tuple child_tuple{};
  RID child_rid;
  while (child_->Next(&child_tuple, &child_rid)) {
    child_tuples_.push_back(child_tuple);
  }

  // 对 child_tuples_ 进行排序
  std::sort(child_tuples_.begin(), child_tuples_.end(),
            [order_bys = plan_->GetOrderBy(), schema = child_->GetOutputSchema()](const Tuple &tuple_a,
                                                                                  const Tuple &tuple_b) {
              for (const auto &order_key : order_bys) {
                auto val_a = order_key.second->Evaluate(&tuple_a, schema);
                auto val_b = order_key.second->Evaluate(&tuple_b, schema);

                switch (order_key.first) {
                  case OrderByType::INVALID:
                  case OrderByType::DEFAULT:
                  case OrderByType::ASC:
                    if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
                      return true;
                    } else if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
                      return false;
                    }
                    break;

                  case OrderByType::DESC:
                    if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
                      return true;
                    } else if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
                      return false;
                    }
                    break;

                  default:
                    break;
                }
              }
              return false;  // 如果所有键都相等，则认为它们相等
            });

  // 初始化迭代器
  child_iter_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_iter_ == child_tuples_.end()) {
    return false;
  }

  *tuple = *child_iter_;
  *rid = tuple->GetRid();
  ++child_iter_;

  return true;
}

}  // namespace bustub
