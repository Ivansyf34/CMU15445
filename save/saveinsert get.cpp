#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

#include "storage/page/log.h"

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
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Transaction *transaction) -> Page *  {
  page_id_t page_id = root_page_id_;

  logToFile("FindLeaf");
  while (true) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    logToFile("FindLeaf pageid:" + std::to_string(node->GetPageId()));
    if (node->IsLeafPage()) {
      // 如果当前节点是叶子节点，返回该节点
      // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return page;
    } else if(node != nullptr){
      // 如果当前节点是内部节点，则继续搜索
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      page_id = internal_node->Lookup(key, comparator_);
    }else{
      return nullptr;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if(IsEmpty()){
    return false;
  }
  logToFile("GetValue " + std::to_string(*key.data_));
  logToFile("key" );
  // 调用 FindLeaf 函数找到包含给定键值的叶子节点
  Page *leaf_page = FindLeaf(key, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 在叶子节点中查找键值
  ValueType value;
  bool found = leaf_node->Lookup(key, &value, comparator_);

  // 如果找到了值，则将其添加到结果向量中
  if (found) {
      result->push_back(value);
      logToFile("GetValue success");
      logToFile("size" + std::to_string(result->size()));
  }

  // 释放页面
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);

  return true;

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
  // 初始根节点
  std::string message = "Insert";
  logToFile(message);
  if (IsEmpty()) {
    logToFile("New Root");
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    logToFile("根节点 "+ std::to_string(root_page_id_));

    leaf->Insert(key, value, comparator_);

    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }

  // 找到插入位置
  Page *leaf_page = FindLeaf(key, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 在叶子节点中插入键值
  if (leaf_node->Insert(key, value, comparator_)) {
      buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
      return true;
  }
  // 如果叶子节点已满，需要分裂叶子节点
  LeafPage *new_leaf_node = SplitLeaf(leaf_node);

  // 更新父节点
  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction);

  // 释放资源
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage* old_leaf_node) -> LeafPage* {
    // 创建一个新的叶子节点
    std::string message = "SplitLeaf old_leaf_node_id: " + std::to_string(old_leaf_node->GetPageId());
    logToFile(message);
    page_id_t page_id;
    auto page = buffer_pool_manager_->NewPage(&page_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
    new_leaf_node->Init(page_id, old_leaf_node->GetParentPageId(), old_leaf_node->GetMaxSize());
    

    // 计算分裂点位置
    int split_point = (old_leaf_node->GetSize() + 1) / 2;
    logToFile("分裂新leaf节点"+ std::to_string(page_id));
    logToFile("split_point"+ std::to_string(split_point));

    // 将原叶子节点的后半部分移动到新的叶子节点中
    for (int i = split_point; i < old_leaf_node->GetSize(); ++i) {
        new_leaf_node->InsertEntry(old_leaf_node->KeyAt(i), old_leaf_node->ValueAt(i), comparator_);
    }

    // 更新原叶子节点的大小
    old_leaf_node->SetSize(split_point);

    // 更新新叶子节点的指针
    new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());
    old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());

    return new_leaf_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage* old_internal_node) -> InternalPage* {
    std::string message = "SplitInternal old_leaf_node_: " + std::to_string(old_internal_node->GetPageId());
    logToFile(message);
    // 创建一个新的内部节点
    page_id_t page_id;
    auto page = buffer_pool_manager_->NewPage(&page_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto *new_internal_node = reinterpret_cast<InternalPage *>(page->GetData());
    new_internal_node->Init(page_id, old_internal_node->GetParentPageId(), old_internal_node->GetMaxSize());
    logToFile("分裂新节点 "+ std::to_string(page_id));

    // 计算分裂点位置
    int split_point = (old_internal_node->GetSize() + 1) / 2;

    // 将原内部节点的后半部分移动到新的内部节点中
    new_internal_node->SetValueAt(0, old_internal_node->ValueAt(split_point - 1));
    new_internal_node->SetSize(1);
    for (int i = split_point + 1; i < old_internal_node->GetSize(); ++i) {
      new_internal_node->InsertEntry(old_internal_node->KeyAt(i), old_internal_node->ValueAt(i), comparator_);
    }
    // 更新原内部节点的大小
    old_internal_node->SetSize(split_point);

    return new_internal_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage* left_child, const KeyType& key, BPlusTreePage* right_child, Transaction* transaction) {
    std::string message = "InsertIntoParent left: " + std::to_string(left_child->GetPageId()) + "  right: " + std::to_string(right_child->GetPageId());
    logToFile(message);
    logToFile("key" + std::to_string(*key.data_));
    // 如果父节点为空，说明左子节点是根节点
    if (left_child->IsRootPage()) {
        // 创建一个新的根节点
        auto page = buffer_pool_manager_->NewPage(&root_page_id_);
        auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
        new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
        logToFile("父节点为空，左子节点是根节点,创建一个新的根节点 "+ std::to_string(root_page_id_));
        
        new_root->SetKeyAt(1, key);
        new_root->SetValueAt(0, left_child->GetPageId());
        new_root->SetValueAt(1, right_child->GetPageId());
        new_root->SetSize(2);

        left_child->SetParentPageId(new_root->GetPageId());
        right_child->SetParentPageId(new_root->GetPageId());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        return;
    }
    page_id_t page_id = left_child->GetParentPageId();
    auto parent_page = buffer_pool_manager_->FetchPage(page_id);
    InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

    // 插入新的键值对到父节点中
    if(parent_node->Insert(key, right_child->GetPageId(), comparator_)){
      logToFile("InsertIntoParent success");
      // 释放资源
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      right_child->SetParentPageId(left_child->GetPageId());
      return;
    }

    // 如果父节点已满，需要进行分裂
    InternalPage* new_parent_node = SplitInternal(parent_node);

    // 将分裂后的父节点插入到原父节点的父节点中
    InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);

    // 释放资源
    buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
