INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_id_latch_.WLock();
  if (IsEmpty()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      root_page_id_latch_.WUnlock();
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf->SetNextPageId(INVALID_PAGE_ID);

    leaf->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    root_page_id_latch_.WUnlock();
    return true;
  }
  root_page_id_latch_.WUnlock();

  auto leaf_page = FindLeaf(key, false, transaction);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  if (leaf_node->DetectInsert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
    leaf_node->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  auto *new_leaf_node = SplitLeaf(leaf_node, transaction);
  if (comparator_(key, new_leaf_node->KeyAt(0)) < 0) {
    leaf_node->Insert(key, value, comparator_);
  } else {
    new_leaf_node->Insert(key, value, comparator_);
  }

  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction);

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *old_leaf_node, Transaction *transaction) -> LeafPage * {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto *new_leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  new_leaf_node->Init(page_id, old_leaf_node->GetParentPageId(), old_leaf_node->GetMaxSize());

  int split_point = old_leaf_node->GetSize() / 2;
  for (int i = split_point; i < old_leaf_node->GetSize(); ++i) {
    new_leaf_node->Insert(old_leaf_node->KeyAt(i), old_leaf_node->ValueAt(i), comparator_);
  }
  old_leaf_node->SetSize(split_point);

  new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());
  old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());

  return new_leaf_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *old_internal_node, Transaction *transaction) -> InternalPage * {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto *new_internal_node = reinterpret_cast<InternalPage *>(page->GetData());
  new_internal_node->Init(page_id, old_internal_node->GetParentPageId(), old_internal_node->GetMaxSize());

  int split_point = old_internal_node->GetSize() / 2;
  for (int i = split_point + 1; i < old_internal_node->GetSize(); ++i) {
    new_internal_node->Insert(old_internal_node->KeyAt(i), old_internal_node->ValueAt(i), comparator_);
  }
  old_internal_node->SetSize(split_point + 1);

  return new_internal_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *left_child, const KeyType &key, BPlusTreePage *right_child,
                                      Transaction *transaction) {
  if (left_child->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(0, left_child->GetPageId());
    new_root->SetValueAt(1, right_child->GetPageId());
    new_root->SetSize(2);

    left_child->SetParentPageId(new_root->GetPageId());
    right_child->SetParentPageId(new_root->GetPageId());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    UpdateRootPageId(0);
    return;
  }

  page_id_t page_id = left_child->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(page_id);
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot fetch parent page");
  }
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  if (parent_node->Insert(key, right_child->GetPageId(), comparator_)) {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  auto *new_parent_node = SplitInternal(parent_node, transaction);
  right_child->SetParentPageId(new_parent_node->GetPageId());

  InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
}
