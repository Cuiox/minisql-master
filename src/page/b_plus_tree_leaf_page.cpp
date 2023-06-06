#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

/**
 * kvp[]: key value pair 数组
 * kvp[index].key
 * kvp[index].value
*/
#define pairs_off (data_ + LEAF_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void BPlusTreeLeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
	SetPageType(IndexPageType::LEAF_PAGE);	// PageType (4)
	SetSize(0);								// CurrentSize (4)
	SetPageId(page_id);						// PageId(4)
	SetParentPageId(parent_id);				// ParentPageId (4)
	SetNextPageId(INVALID_PAGE_ID);			// NextPageId (4) LeafPage 新增的域
	SetMaxSize(max_size);					// MaxSize (4)
	SetKeySize(key_size);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t BPlusTreeLeafPage::GetNextPageId() const {
  return next_page_id_;
}

void BPlusTreeLeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int BPlusTreeLeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
	//int index = -1; // -1: not found
	int left = 0;
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
			return mid; // kvp[index].key == key
			//index = mid;
			//break;
		}
	}
	return left; // kvp[index].key > key
	//return index;
}

/*
 * kvp[index].key Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *BPlusTreeLeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

/**
 * Helper method to set the `key` to kvp[index].key
*/
void BPlusTreeLeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

/**
 * Helper method to find and return the `value` at kvp[index].value 
*/
RowId BPlusTreeLeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

/**
 * Helper method to set the `value` to kvp[index].value
*/
void BPlusTreeLeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

/**
 * Helper method to find and return the pointer to kvp[index]. (same as KeyAt(index))
*/
void *BPlusTreeLeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

/**
 * Helper method to copy all pair_num's pair from src to dest
*/
void BPlusTreeLeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> BPlusTreeLeafPage::GetItem(int index) {
    // replace with your own code
    //return make_pair(nullptr, RowId());
	return make_pair(KeyAt(index), ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int BPlusTreeLeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
	int index = KeyIndex(key, KM); // key <= kvp[index].key
	if (key == KeyAt(index)) { // key == kvp[index].key, no insert
		return GetSize();
	} else { // key < kvp[index].key
		// TODO: 一次性 copy
		for (int i = GetSize(); i > index; i--) {
			// kvp[i] = kvp[i-1]
			PairCopy(PairPtrAt(i), PairPtrAt(i-1), 1);
		}
		SetKeyAt(index, key);
		SetValueAt(index, value);
		IncreaseSize(1);
		return GetSize();
	}
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * only when GetSize() == max_size
 */
void BPlusTreeLeafPage::MoveHalfTo(BPlusTreeLeafPage *recipient) {
	int start_index = GetSize() - GetSize() / 2;
	int half_size = GetSize() / 2;
	recipient->CopyNFrom(PairPtrAt(start_index), half_size);
	IncreaseSize(-half_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void BPlusTreeLeafPage::CopyNFrom(void *src, int size) {
	PairCopy(PairPtrAt(GetSize()), src, size);
	IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool BPlusTreeLeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
	bool flag = false;
	int index = KeyIndex(key, KM); // kvp[index].key >= key; 取 = 的情况
	if (KM.CompareKeys(key, KeyAt(index)) == 0) {
		value = ValueAt(index);
		flag = true;
	}

	return flag;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int BPlusTreeLeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
	int page_size = -1;
	int index = KeyIndex(key, KM);
	if (KM.CompareKeys(key, KeyAt(index)) != 0) {
		page_size = GetSize();
	} else {
		for (int i = index + 1; i < GetSize(); i++) {
			// kvp[i-1] = kvp[i]
			PairCopy(PairPtrAt(i-1), PairPtrAt(i), 1);
		}
		IncreaseSize(-1);
		page_size = GetSize();
	}

	return page_size;
}

/**
 * Remove one kv pair by index, return true if remove succeed
*/
bool BPlusTreeLeafPage::RemoveByIndex(int index) {
	bool flag = false;
	if (index < GetSize()) {
		for (int i = index; i < GetSize() - 1; i++) {
			PairCopy(PairPtrAt(i), PairPtrAt(i+1), 1);
		}
		IncreaseSize(-1);
		flag = true;
	}

	return flag;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page (Caller)
 */
void BPlusTreeLeafPage::MoveAllTo(BPlusTreeLeafPage *recipient) {
	recipient->CopyNFrom(PairPtrAt(0), GetSize());
	SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void BPlusTreeLeafPage::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
	int index = 0;
	recipient->CopyLastFrom(KeyAt(index), ValueAt(index));
	RemoveByIndex(index);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void BPlusTreeLeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
	int index = GetSize();
	SetKeyAt(index, key);
	SetValueAt(index, value);
	IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void BPlusTreeLeafPage::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
	int index = GetSize() - 1;
	recipient->CopyFirstFrom(KeyAt(index), ValueAt(index));
	RemoveByIndex(index);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void BPlusTreeLeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
	int index = 0;
	for (int i = GetSize(); i > 0; i--) {
		PairCopy(PairPtrAt(i), PairPtrAt(i-1), 1);
	}
	SetKeyAt(index, key);
	SetValueAt(index, value);
	IncreaseSize(1);
}