//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &KeyCmp, bool leftmost,
                                            bool rightmost) -> page_id_t {
  if (leftmost) {
    return array_[0].second;
  }
  if (rightmost) {
    return array_[GetSize() - 1].second;
  }
  if (KeyCmp(KeyAt(1), key) > 0) {
    return array_[0].second;
  }
  int left = 1;
  int right = GetSize() - 1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (KeyCmp(KeyAt(mid), key) > 0) {
      right = mid - 1;
    } else if (KeyCmp(KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else {
      return array_[mid].second;
    }
  }
  return array_[left - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &KeyCmp)
    -> bool {
  // 查找插入位置
  int insert_pos = 0;
  while (insert_pos < GetSize() && KeyCmp(key, array_[insert_pos].first) > 0) {
    insert_pos++;
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> page_id_t {
  assert(GetSize() == 1);
  auto page_id = array_[0].second;
  SetSize(0);
  return page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize(); i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;  // 如果未找到则返回-1
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const KeyType &key, const ValueType &value) {
  array_[GetSize()].first = key;
  array_[GetSize()].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeBefore(const KeyType &key, const ValueType &value) {
  for (int i = 0; i < GetSize(); i++) {
    array_[i + 1] = array_[i];
  }
  array_[0].first = key;
  array_[0].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                                                      BufferPoolManager *buffer_pool_manager_) {
  // 移动第一个键值对到兄弟节点的末尾
  recipient->CopyLastFrom(array_[0], buffer_pool_manager_);

  // 将剩余的键值对左移
  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }

  // 更新当前节点的大小
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager_) {
  // 更新父节点中的分隔键
  array_[GetSize()].first = pair.first;
  array_[GetSize()].second = pair.second;

  // 更新子节点的父指针
  auto child_page = buffer_pool_manager_->FetchPage(pair.second);
  auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager_->UnpinPage(pair.second, true);

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
                                                       BufferPoolManager *buffer_pool_manager_) {
  // 获取当前节点的最后一个键值对
  MappingType pair = array_[GetSize() - 1];

  // 将键值对移动到兄弟节点的开头
  recipient->CopyFirstFrom(pair, buffer_pool_manager_);

  // 更新当前节点的大小
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager_) {
  // 将现有的键值对右移
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i - 1];
  }

  // 更新父节点中的分隔键
  array_[0].first = pair.first;
  array_[0].second = pair.second;

  // 更新子节点的父指针
  auto child_page = buffer_pool_manager_->FetchPage(pair.second);
  auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager_->UnpinPage(pair.second, true);

  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
