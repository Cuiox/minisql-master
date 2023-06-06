#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 * BPlusTree::BPlusTree函数中，如果传入的leaf_max_size和internal_max_size是默认值0，即UNDEFINED_SIZE，那么需要自己根据keysize进行计算
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
	
	root_page_id_ = INVALID_PAGE_ID;
	IndexRootsPage *root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
	page_id_t root_page_id;
	bool flag = root_page->GetRootId(index_id, &root_page_id);
	buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
	if (flag == true) {
		root_page_id_ = root_page_id;
	}
}

void BPlusTree::Destroy(page_id_t current_page_id) {
}

/*
 * Helper function to decide whether current b+tree is empty
 * Returns true if this B+ tree has no keys and values.
 */
bool BPlusTree::IsEmpty() const {
	return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
	bool flag = false;
	Page *p = FindLeafPage(key);
	RowId value;
	LeafPage *node = reinterpret_cast<LeafPage *>(p->GetData());
	flag = node->Lookup(key, value, processor_);
	if (flag) {
		result.push_back(value);
	}
	buffer_pool_manager_->UnpinPage(p->GetPageId(), false);

	return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert entry
 * , otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true. Deal in ::InsertIntoLeaf.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
	if (IsEmpty()) {
		StartNewTree(key, value);
		return true;
	} else {
		return InsertIntoLeaf(key, value, transaction);
	}
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
	page_id_t root_page_id = INVALID_PAGE_ID;
	Page *root_page = buffer_pool_manager_->NewPage(root_page_id);
	if (root_page == nullptr) {
		throw std::runtime_error("out of memory");
	}
	root_page_id_ = root_page_id;
	UpdateRootPageId(1);
	LeafPage *leaf_node = reinterpret_cast<LeafPage *>(root_page->GetData());
	int key_size = processor_.GetKeySize();
	leaf_node->Init(root_page_id_, INVALID_PAGE_ID, key_size, leaf_max_size_); // where to init key_size?
	leaf_node->Insert(key, value, processor_);
	buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
	bool flag = false;

	Page *page = FindLeafPage(key);
	LeafPage *node = reinterpret_cast<LeafPage *>(page->GetData());
	int old_size = node->GetSize();
	int new_size = node->Insert(key, value, processor_);

	if (old_size == new_size) { // duplicate
		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
		flag = false;
	} else if (node->GetMaxSize() < new_size) { // overflow
		LeafPage *new_node = Split(node, transaction);
		InsertIntoParent(node, new_node->KeyAt(0), new_node, transaction);
		buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
		buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
		flag = true;
	} else { // normal
		buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
		flag = true;
	}

	return flag;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
	page_id_t new_page_id = INVALID_PAGE_ID;
	Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
	if (new_page == nullptr) {
		throw std::runtime_error("out of memory");
	}
	InternalPage *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
	new_node->SetPageType(IndexPageType::INTERNAL_PAGE); // 可以不用
	new_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
	node->MoveHalfTo(new_node, buffer_pool_manager_);
	// InsertIntoParent 交给上层的函数
	return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
	page_id_t new_page_id = INVALID_PAGE_ID;
	Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
	if (new_page == nullptr) {
		throw std::runtime_error("out of memory");
	}
	LeafPage *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
	new_node->SetPageType(IndexPageType::LEAF_PAGE);
	new_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
	node->MoveHalfTo(new_node);
	// update 链表
	new_node->SetNextPageId(node->GetNextPageId());
	node->SetNextPageId(new_node->GetPageId());
	// InsertIntoParent 交给上层的函数
	return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
	if (old_node->IsRootPage()) {
		page_id_t new_page_id = INVALID_PAGE_ID;
		Page* new_root = buffer_pool_manager_->NewPage(new_page_id);
		if (new_root == nullptr) {
			throw std::runtime_error("out of memory");
		}
		root_page_id_ = new_page_id;
		InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root->GetData());
		new_root_node->Init(new_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
		new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

		old_node->SetParentPageId(new_page_id);
		new_node->SetParentPageId(new_page_id);
		buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
		UpdateRootPageId(0);
	} else {
		Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
		InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
		parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
		if (parent_node->GetSize() >= parent_node->GetMaxSize()) { // 递归分裂
			InternalPage *new_parent_node = Split(parent_node, transaction);
			InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);
			buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
			buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
		} else {
			buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
		}
	}

	return;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
	if (IsEmpty()) {
		return;
	}
	Page *leaf_page = FindLeafPage(key);
	LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
	int old_size = leaf_node->GetSize();
	int new_size = leaf_node->RemoveAndDeleteRecord(key, processor_);
	if (old_size == new_size) {
		buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
	} else {
		CoalesceOrRedistribute(leaf_node, transaction);
		buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
	}
	return;
}

/* todo 合并 或 再分配
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
	bool flag = false;
	
	if (node->IsRootPage()) {
		return AdjustRoot(node);
	}
	if (node->GetSize() >= node->GetMinSize()) {
		return false;
	}
	Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
	InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
	int node_index = parent_node->ValueIndex(node->GetPageId());
	int neighbor_index = -1;
	if (node_index == 0) {
		neighbor_index = 1;
	} else {
		neighbor_index = node_index - 1;
	}
	page_id_t neighbor_page_id = parent_node->ValueAt(neighbor_index);
	Page *neighbor_page = buffer_pool_manager_->FetchPage(neighbor_page_id);
	if (neighbor_page != nullptr) {
		N* neighbor_node = reinterpret_cast<N *>(neighbor_page->GetData());
		if (node->GetSize() + neighbor_node->GetSize() >= node->GetMaxSize()) { // Redistribute
			Redistribute(neighbor_node, node, node_index);
			flag = false;
		} else { // Coalesce
			Coalesce(neighbor_node, node, parent_node, node_index, transaction);
			flag = true;
		}
		buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
		buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
	}

	return flag;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
	if (index == 0) {
		std::swap(neighbor_node, node);
		index = 1; // (0, 1) (neighbor_node, node)
	}

	node->MoveAllTo(neighbor_node);
	neighbor_node->SetNextPageId(node->GetNextPageId());

	parent->Remove(index);
	if (parent->GetSize() < parent->GetMinSize()) {
		return CoalesceOrRedistribute(parent, transaction);
	} else {
		return false;
	}
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
	if (index == 0) {
		std::swap(neighbor_node, node);
		index = 1; // (0, 1) (neighbor_node, node)
	}

	node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
	
	parent->Remove(index);
	if (parent->GetSize() < parent->GetMinSize()) {
		return CoalesceOrRedistribute(parent, transaction);
	} else {
		return false;
	}
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
	Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
	if (parent_page != nullptr) {
		InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
		if (index == 0) {
			neighbor_node->MoveFirstToEndOf(node);
			parent_node->SetKeyAt(1, neighbor_node->KeyAt(0));
		} else {
			neighbor_node->MoveLastToFrontOf(node);
			parent_node->SetKeyAt(index, node->KeyAt(0));
		}
		buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
	}
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
	Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
	if (parent_page != nullptr) {
		InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
		if (index == 0) {
			neighbor_node->MoveFirstToEndOf(node, parent_node->KeyAt(1), buffer_pool_manager_);
			parent_node->SetKeyAt(1, neighbor_node->KeyAt(0));
		} else {
			neighbor_node->MoveLastToFrontOf(node, parent_node->KeyAt(index), buffer_pool_manager_);
			parent_node->SetKeyAt(index, node->KeyAt(0));
		}
		buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
	}
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
	bool flag = false;
	// 对 Leaf 的判断是否必要
	if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) { // case 1
		InternalPage *inter_node = reinterpret_cast<InternalPage *>(old_root_node);
		page_id_t child_page_id = inter_node->RemoveAndReturnOnlyChild();
		root_page_id_ = child_page_id;
		UpdateRootPageId(0);
		Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
		if (new_root_page != nullptr) {
			InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
			new_root_node->SetParentPageId(INVALID_PAGE_ID);
			buffer_pool_manager_->UnpinPage(root_page_id_, true);
		} 
		flag = true;
	} else if (old_root_node->GetSize() == 0 && old_root_node->IsLeafPage()) { // case 2
		root_page_id_ = INVALID_PAGE_ID;
		UpdateRootPageId(0);
		flag = true;
	}

	return flag;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
	GenericKey *key;
	Page *left_most_leaf_page = FindLeafPage(key, -1, true);
  	return IndexIterator(left_most_leaf_page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
	Page *leaf_page = FindLeafPage(key);
	LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
   	return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, leaf_node->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
	Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
	BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
	while (!node->IsLeafPage()) {
		InternalPage *parent_node = reinterpret_cast<InternalPage *>(node);
		page_id_t child_page_id = parent_node->ValueAt(parent_node->GetSize() - 1); // right most
		Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
		BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
		page = child_page;
		node = child_node;
	} 
	LeafPage *end_node = reinterpret_cast<LeafPage *>(page->GetData());
	return IndexIterator(page->GetPageId(), buffer_pool_manager_, end_node->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
	if (IsEmpty()) {
		return nullptr;
	}
	if (page_id != -1) {
		return buffer_pool_manager_->FetchPage(page_id);
	} else {
		Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
		BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
		while (!node->IsLeafPage()) {
			InternalPage *parent_node = reinterpret_cast<InternalPage *>(node);
			page_id_t child_page_id = -1;
			if (leftMost) {
				child_page_id = parent_node->ValueAt(0);
			} else {
				child_page_id = parent_node->Lookup(key, processor_);
			}
			Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
			BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
			buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
			page = child_page;
			node = child_node;
		}
		return page;
	}
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
	Page *header_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
	if (header_page != nullptr) {
		IndexRootsPage *header_node = reinterpret_cast<IndexRootsPage *>(header_page->GetData());
		if (insert_record == 0) {
			header_node->Update(index_id_, root_page_id_);
		} else {
			header_node->Insert(index_id_, root_page_id_);
		}
		buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
	}
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
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
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
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
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}