#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, int op, Transaction *transaction) -> Page * {
  page_id_t page_id = root_page_id_;
  while (true) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (op == 0) {
      page->RLatch();
    } else if (op == 10) {
      if (node->GetSize() < node->GetMaxSize()) {
        ReleaseLatchFromQueue(transaction);
      }
      page->WLatch();
      transaction->AddIntoPageSet(page);
    } else if (op == 2) {
      if (node->GetSize() > node->GetMinSize()) {
        ReleaseLatchFromQueue(transaction);
      }
      page->WLatch();
      transaction->AddIntoPageSet(page);
    }
    if (node->IsLeafPage()) {
      // 如果当前节点是叶子节点，返回该节点
      return page;
    }
    if (node != nullptr) {
      // 如果当前节点是内部节点，则继续搜索
      if (op == 0) {
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      }
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      page_id = internal_node->Lookup(key, comparator_, false, false);
    } else {
      return nullptr;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    Page *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueueTwo(Transaction *transaction) {
  int i = 2;
  while (!transaction->GetPageSet()->empty() && i > 0) {
    Page *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    i--;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_page_id_latch_.RLock();
  if (IsEmpty()) {
    root_page_id_latch_.RUnlock();
    return false;
  }
  // 调用 FindLeaf 函数找到包含给定键值的叶子节点
  auto *leaf_page = FindLeaf(key, 0, transaction);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 在叶子节点中查找键值
  ValueType value;
  bool found = leaf_node->Lookup(key, &value, comparator_);
  result->push_back(value);

  // 释放页面
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  root_page_id_latch_.RUnlock();

  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_id_latch_.WLock();
  // 初始根节点
  if (IsEmpty()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    page->WLatch();
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf->SetNextPageId(INVALID_PAGE_ID);

    leaf->Insert(key, value, comparator_);
    page->WUnlatch();
    root_page_id_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }

  // 找到插入位置
  auto *leaf_page = FindLeaf(key, 1, transaction);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 在叶子节点中插入键值
  auto tag = leaf_node->Insert(key, value, comparator_);
  if (tag) {
    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    root_page_id_latch_.WUnlock();
    return true;
  }
  if (!tag && leaf_node->GetSize() <= leaf_node->GetMaxSize()) {
    // 已存在
    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    root_page_id_latch_.WUnlock();
    return false;
  }
  // 如果叶子节点已满，需要分裂叶子节点
  auto *new_leaf_node = SplitLeaf(leaf_node, transaction);

  // 更新父节点
  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction);

  ReleaseLatchFromQueue(transaction);
  root_page_id_latch_.WUnlock();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *old_leaf_node, Transaction *transaction) -> LeafPage * {
  // 创建一个新的叶子节点
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  // page->WLatch();
  transaction->AddIntoPageSet(page);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto *new_leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  new_leaf_node->Init(page_id, old_leaf_node->GetParentPageId(), old_leaf_node->GetMaxSize());

  // 计算分裂点位置
  int split_point = (old_leaf_node->GetSize() + 1) / 2;

  // 将原叶子节点的后半部分移动到新的叶子节点中
  for (int i = split_point; i < old_leaf_node->GetSize(); ++i) {
    new_leaf_node->Insert(old_leaf_node->KeyAt(i), old_leaf_node->ValueAt(i), comparator_);
  }

  // 更新原叶子节点的大小
  old_leaf_node->SetSize(split_point);

  // 更新新叶子节点的指针
  new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());
  old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());

  return new_leaf_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *old_internal_node, Transaction *transaction) -> InternalPage * {
  // 创建一个新的内部节点
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  // page->WLatch();
  transaction->AddIntoPageSet(page);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto *new_internal_node = reinterpret_cast<InternalPage *>(page->GetData());
  new_internal_node->Init(page_id, old_internal_node->GetParentPageId(), old_internal_node->GetMaxSize());

  // 计算分裂点位置
  int split_point = (old_internal_node->GetSize() + 1) / 2;

  // 将原内部节点的后半部分移动到新的内部节点中
  /*  new_internal_node->SetSize(1);
   new_internal_node->SetValueAt(0, old_internal_node->ValueAt(split_point)); */
  for (int i = split_point + 1; i < old_internal_node->GetSize(); ++i) {
    new_internal_node->Insert(old_internal_node->KeyAt(i), old_internal_node->ValueAt(i), comparator_);
  }
  // 更新原内部节点的大小
  old_internal_node->SetSize(split_point + 1);

  return new_internal_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *left_child, const KeyType &key, BPlusTreePage *right_child,
                                      Transaction *transaction) {
  // 如果父节点为空，说明左子节点是根节点
  if (left_child->IsRootPage()) {
    // 创建一个新的根节点
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    // page->WLatch();
    transaction->AddIntoPageSet(page);
    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(0, left_child->GetPageId());
    new_root->SetValueAt(1, right_child->GetPageId());
    new_root->SetSize(2);

    left_child->SetParentPageId(new_root->GetPageId());
    right_child->SetParentPageId(new_root->GetPageId());

    buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    return;
  }
  page_id_t page_id = left_child->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(page_id);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 插入新的键值对到父节点中
  if (parent_node->Insert(key, right_child->GetPageId(), comparator_)) {
    // 释放资源
    buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return;
  }

  // 如果父节点已满，需要进行分裂
  auto *new_parent_node = SplitInternal(parent_node, transaction);
  right_child->SetParentPageId(new_parent_node->GetPageId());

  ReleaseLatchFromQueueTwo(transaction);
  buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(right_child->GetPageId(), true);

  // 将分裂后的父节点插入到原父节点的父节点
  InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_latch_.WLock();
  if (IsEmpty()) {
    root_page_id_latch_.WUnlock();
    return;
  }
  // 找到要删除的叶子节点
  auto *leaf_page = FindLeaf(key, 2, transaction);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 从叶子节点中删除键值对
  if (!leaf_node->RemoveAndDeleteRecord(key, comparator_)) {
    ReleaseLatchFromQueue(transaction);
    root_page_id_latch_.WUnlock();
    return;  // 删除失败，键不存在
  }

  // 如果删除后叶子节点的元素数目少于最小允许元素数目，进行合并或借位
  if (leaf_node->GetSize() < leaf_node->GetMinSize()) {
    CoalesceOrRedistribute(leaf_node, transaction);
  }

  ReleaseLatchFromQueue(transaction);
  root_page_id_latch_.WUnlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceOrRedistribute(BPlusTreePage *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    AdjustRoot(node, transaction);
    return;
  }

  page_id_t parent_page_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  parent_page->WLatch();
  transaction->AddIntoPageSet(parent_page);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  int node_index = parent_node->ValueIndex(node->GetPageId());
  int sibling_index = node_index == 0 ? 1 : node_index - 1;

  page_id_t sibling_page_id = parent_node->ValueAt(sibling_index);
  auto sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  sibling_page->WLatch();
  transaction->AddIntoPageSet(sibling_page);
  auto *sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());

  bool coalesce = Coalesce(sibling_node, node, parent_node, node_index, transaction);

  if (!coalesce) {
    Redistribute(sibling_node, node, parent_node, node_index, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Coalesce(BPlusTreePage *neighbor_node, BPlusTreePage *node, InternalPage *parent, int index,
                              Transaction *transaction) -> bool {
  // 放不下了
  if ((neighbor_node->GetSize() + node->GetSize()) > neighbor_node->GetMaxSize()) {
    return false;
  }

  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *sibling_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {
      for (int i = leaf_node->GetSize() - 1; i >= 0; --i) {
        sibling_node->InsertNodeBefore(leaf_node->KeyAt(i), leaf_node->ValueAt(i));
      }
    } else {
      // 将节点的内容合并到邻居节点中
      for (int i = 0; i < leaf_node->GetSize(); ++i) {
        sibling_node->InsertNodeAfter(leaf_node->KeyAt(i), leaf_node->ValueAt(i));
      }
      sibling_node->SetNextPageId(leaf_node->GetNextPageId());
    }
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *sibling_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {
      for (int i = internal_node->GetSize() - 1; i >= 0; --i) {
        sibling_node->InsertNodeBefore(internal_node->KeyAt(i), internal_node->ValueAt(i));
      }
    } else {
      // 将节点的内容合并到邻居节点中
      for (int i = 0; i < internal_node->GetSize(); ++i) {
        sibling_node->InsertNodeAfter(internal_node->KeyAt(i), internal_node->ValueAt(i));
      }
    }
  }

  // 更新父节点
  parent->Remove(index);

  if (parent->GetSize() < parent->GetMinSize()) {
    CoalesceOrRedistribute(parent, transaction);
  }

  ReleaseLatchFromQueue(transaction);
  assert(buffer_pool_manager_->DeletePage(node->GetPageId()));
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage *neighbor_node, BPlusTreePage *node, InternalPage *parent, int index,
                                  Transaction *transaction) {
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *sibling_node = reinterpret_cast<LeafPage *>(neighbor_node);

    if (index == 0) {
      sibling_node->MoveFirstToEndOf(leaf_node, buffer_pool_manager_);
      parent->SetKeyAt(1, sibling_node->KeyAt(0));
    } else {
      leaf_node->MoveLastToFrontOf(sibling_node, buffer_pool_manager_);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *sibling_node = reinterpret_cast<InternalPage *>(neighbor_node);

    if (index == 0) {
      sibling_node->MoveFirstToEndOf(internal_node, buffer_pool_manager_);
      parent->SetKeyAt(1, sibling_node->KeyAt(0));
    } else {
      internal_node->MoveLastToFrontOf(sibling_node, buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, Transaction *transaction) {
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }

  auto *root_node = reinterpret_cast<InternalPage *>(old_root_node);
  if (root_node->GetSize() == 1) {
    root_page_id_ = root_node->RemoveAndReturnOnlyChild();
    auto new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    new_root_page->WLatch();
    auto *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    ReleaseLatchFromQueue(transaction);
    transaction->AddIntoPageSet(new_root_page);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  root_page_id_latch_.RLock();
  page_id_t page_id = root_page_id_;
  while (true) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    page->RLatch();
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      // 如果当前节点是叶子节点，返回该节点
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      auto *leaf_node = reinterpret_cast<LeafPage *>(node);
      root_page_id_latch_.RUnlock();
      return INDEXITERATOR_TYPE(leaf_node, 0, buffer_pool_manager_);
    }
    if (node != nullptr) {
      // 如果当前节点是内部节点，则继续搜索
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      page_id = internal_node->Lookup(internal_node->KeyAt(0), comparator_, true, false);
    }
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  root_page_id_latch_.RLock();
  auto *leaf_page = FindLeaf(key, 0);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  root_page_id_latch_.RUnlock();
  return INDEXITERATOR_TYPE(leaf_node, leaf_node->KeyIndex(key, comparator_), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  root_page_id_latch_.RLock();
  page_id_t page_id = root_page_id_;
  while (true) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    page->RLatch();
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      // 如果当前节点是叶子节点，返回该节点
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      auto *leaf_node = reinterpret_cast<LeafPage *>(node);
      root_page_id_latch_.RUnlock();
      return INDEXITERATOR_TYPE(leaf_node, leaf_node->GetSize(), buffer_pool_manager_);
    }
    if (node != nullptr) {
      // 如果当前节点是内部节点，则继续搜索
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      page_id = internal_node->Lookup(internal_node->KeyAt(0), comparator_, false, true);
    }
  }
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
