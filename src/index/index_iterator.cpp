#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<Page *>(buffer_pool_manager->FetchPage(current_page_id));
  node = reinterpret_cast<LeafPage *>(page->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
	return node->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
	item_index++;
	if (item_index == node->GetSize() && node->GetNextPageId() != INVALID_PAGE_ID) {
		// 跳到下一个 node
		Page *next_page = buffer_pool_manager->FetchPage(node->GetNextPageId());
		buffer_pool_manager->UnpinPage(page->GetPageId(), false);
		page = next_page;
		node = reinterpret_cast<LeafPage *>(page->GetData());
		item_index = 0;
	}
	return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}