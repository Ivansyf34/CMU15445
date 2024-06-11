//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                         table_info->oid_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;

  // 获取待删除的表信息及其索引列表
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  // 从子执行器 Values 中逐个获取元组并插入到表中，同时更新所有的索引
  int delete_count = 0;
  while (child_executor_->Next(tuple, rid)) {
    exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                         table_info->oid_, *rid);

    table_info->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    for (const auto &index : index_info) {
      // 根据索引的模式从数据元组中构造索引元组，并从索引中删除
      Tuple key_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                            index->index_->GetMetadata()->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    delete_count++;
  }

  // 这里的 tuple 不再对应实际的数据行，而是用来存储插入操作的影响行数
  std::vector<Value> result{Value(INTEGER, delete_count)};
  *tuple = Tuple(result, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub
