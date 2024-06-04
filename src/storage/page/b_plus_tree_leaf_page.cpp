//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertNodeAfter(const KeyType &key, const ValueType &value) {
  array_[GetSize()].first = key;
  array_[GetSize()].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertNodeBefore(const KeyType &key, const ValueType &value) {
  for (int i = 0; i < GetSize(); i++) {
    array_[i + 1] = array_[i];
  }
  array_[0].first = key;
  array_[0].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &KeyCmp) const
    -> bool {
  int left = 0;
  int right = GetSize() - 1;

  // 二分查找
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (KeyCmp(KeyAt(mid), key) > 0) {
      right = mid - 1;
    } else if (KeyCmp(KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else {
      *value = array_[mid].second;
      return true;
    }
  }

  // 如果未找到匹配的键值，则返回false
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &KeyCmp)
    -> bool {
  // 查找插入位置
  int insert_pos = 0;
  while (insert_pos < GetSize() && KeyCmp(key, array_[insert_pos].first) > 0) {
    insert_pos++;
  }
  if (insert_pos < GetSize() && KeyCmp(key, array_[insert_pos].first) == 0) {
    return false;
  }

  // 将新的键值和值插入到数组中
  for (int i = GetSize(); i > insert_pos; i--) {
    array_[i] = array_[i - 1];
  }
  array_[insert_pos] = std::make_pair(key, value);
  IncreaseSize(1);  // 增加节点大小
  return GetSize() <= GetMaxSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &KeyCmp) -> int {
  int left = 0;
  int right = GetSize() - 1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (KeyCmp(KeyAt(mid), key) > 0) {
      right = mid - 1;
    } else if (KeyCmp(KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else {
      return mid;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &KeyCmp) -> bool {
  // 找到要删除的键的位置
  int index = KeyIndex(key, KeyCmp);
  if (index >= GetSize() || KeyCmp(KeyAt(index), key) != 0) {
    return false;  // 键不存在
  }

  // 删除键值对
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                                                  BufferPoolManager *buffer_pool_manager_) {
  // 移动第一个键值对到兄弟节点的末尾
  recipient->InsertNodeAfter(array_[GetSize() - 1].first, array_[GetSize() - 1].second);

  // 将剩余的键值对左移
  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }

  // 更新当前节点的大小
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,
                                                   BufferPoolManager *buffer_pool_manager_) {
  // 移动最后一个键值对到兄弟节点的开头
  recipient->InsertNodeBefore(array_[GetSize() - 1].first, array_[GetSize() - 1].second);

  // 更新当前节点的大小
  IncreaseSize(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
