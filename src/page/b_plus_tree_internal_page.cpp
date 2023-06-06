#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_ + INTERNAL_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

// kvp[0].value, (kvp[1].key, kvp[1].value), ... ()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void BPlusTreeInternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
	SetPageType(IndexPageType::INTERNAL_PAGE);	// PageType (4)
	SetSize(0);									// CurrentSize (4)
	SetPageId(page_id);							// PageId(4)
	SetParentPageId(parent_id);					// ParentPageId (4)
	SetMaxSize(max_size);						// MaxSize (4)
	SetKeySize(key_size);
}
/*
 * kvp[index].key Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *BPlusTreeInternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

/**
 * Helper method to set the `key` to kvp[index].key
*/
void BPlusTreeInternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

/**
 * Helper method to find and return the `value` at kvp[index].value 
*/
page_id_t BPlusTreeInternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

/**
 * Helper method to set the `value` to kvp[index].value
*/
void BPlusTreeInternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

/**
 * Helper method to find and return the index of `value`
*/
int BPlusTreeInternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

/**
 * Helper method to find and return the pointer to kvp[index]. (same as KeyAt(index))
*/
void *BPlusTreeInternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

/**
 * Helper method to copy all pair_num's pair from src to dest
*/
void BPlusTreeInternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t BPlusTreeInternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {

	int left = 1; // Start the search from the second key
	int right = GetSize() - 1;
	while (left < right) {
		int mid = left + (right - left) / 2;
		const GenericKey *key_at_mid = KeyAt(mid);
		int flag = KM.CompareKeys(key, key_at_mid);
		if (flag == -1) { // l < r
			right = mid - 1;
		} else if (flag == 1) { // l > r
			left = mid + 1;
		} else { // flag == 0; equal
			return ValueAt(mid); // kvp[index].key == key
			//index = mid;
			//break;
		}
	}
	return ValueAt(left); //return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void BPlusTreeInternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
	// 当B+树的insert操作递归更新父节点影响到根节点时，建立新的根节点
	SetSize(2);
	SetValueAt(0, old_value);
	SetKeyAt(1, new_key);
	SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value == old_value
 * @return:  new size after insertion
 */
int BPlusTreeInternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
	int old_value_index = ValueIndex(old_value);
	if (old_value_index == -1) { 
		LOG(INFO) << "old value not found";
	} else {
		int at_index = old_value_index + 1; // insert at this index
		for (int i = GetSize(); i > at_index; i--) {
			// TODO: 一次性 copy
			PairCopy(PairPtrAt(i), PairPtrAt(i-1), 1); // 后移，腾出位置
		}
		SetKeyAt(at_index, new_key);
		SetValueAt(at_index, new_value);
		IncreaseSize(1);
	}
	return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void BPlusTreeInternalPage::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
	int half_size = GetSize() / 2;
	int start_index = GetSize() - half_size;
	recipient->CopyNFrom(PairPtrAt(start_index), half_size, buffer_pool_manager);
	IncreaseSize(-half_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void BPlusTreeInternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
	for (int index = 0; index < size; index++) {
		GenericKey * key = reinterpret_cast<GenericKey *>(src + index * pair_size + key_off);
		page_id_t value = *reinterpret_cast<const page_id_t *>(src + index * pair_size + val_off);
		CopyLastFrom(key, value, buffer_pool_manager);
	}
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void BPlusTreeInternalPage::Remove(int index) {
	for (int i = index; i < GetSize() - 1; i++) {
		// kvp[i] = kvp[i+1]
		PairCopy(PairPtrAt(i), PairPtrAt(i+1), 1);
	}
	IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t BPlusTreeInternalPage::RemoveAndReturnOnlyChild() {
	page_id_t value = ValueAt(0);
	Remove(0);
	return value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void BPlusTreeInternalPage::MoveAllTo(BPlusTreeInternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
	recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
	recipient->CopyNFrom(PairPtrAt(1), GetSize() - 1, buffer_pool_manager);
	SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void BPlusTreeInternalPage::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
	recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
	Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void BPlusTreeInternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
	// Append the entry at the end.
	int end_index = GetSize();
	SetKeyAt(end_index, key);
	SetValueAt(end_index, value);
	IncreaseSize(1);
	// update children's parent
	Page *child_page = buffer_pool_manager->FetchPage(ValueAt(end_index));
	if (child_page != nullptr) {
		BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
		child_node->SetParentPageId(GetPageId());
		buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
	}
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void BPlusTreeInternalPage::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
	recipient->SetKeyAt(0, middle_key);
	recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager);
	IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void BPlusTreeInternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
	// Append the entry at the beginning
	int index = 0;
	for (int i = GetSize(); i > 0; i--) {
		PairCopy(PairPtrAt(i), PairPtrAt(i-1), 1);
	}
	//SetKeyAt(index, key);
	SetValueAt(index, value);
	IncreaseSize(1);
	// update children's parent
	Page *child_page = buffer_pool_manager->FetchPage(ValueAt(index));
	if (child_page != nullptr) {
		BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
		child_node->SetParentPageId(GetPageId());
		buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
	}
}