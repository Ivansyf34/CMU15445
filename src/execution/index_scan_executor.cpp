//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())),
      table_iter_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == tree_->GetEndIterator()) {
    return false;
  }

  // 获取元组的 rid 并填充元组内容
  *rid = (*table_iter_).second;
  auto result = table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++table_iter_;
  return result;
}

}  // namespace bustub
