#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid) : table_heap_(table_heap), row_(new Row(rid)) {
	if (row_->GetRowId().GetPageId() != INVALID_PAGE_ID) {
		table_heap_->GetTuple(row_, nullptr);
	}
	return;
}

TableIterator::TableIterator(const TableIterator &other) {
	table_heap_ = other.table_heap_;
	row_ = new Row(*other.row_);
}

TableIterator::~TableIterator() {
	delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
	return row_->GetRowId().Get() == itr.row_->GetRowId().Get();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
	return row_->GetRowId().Get() != itr.row_->GetRowId().Get();
}

const Row &TableIterator::operator*() {
	return *row_;
}

Row *TableIterator::operator->() {
	return row_;
}


TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
	if (this != &itr) { // 避免自赋值
		table_heap_ = itr.table_heap_;
		if (row_ != nullptr) {
			delete row_;
			row_ = NULL;
		}
		row_ = new Row(*itr.row_);
	}
	return *this;
}


// ++iter
TableIterator &TableIterator::operator++() {
	auto buffer_pool_manager = table_heap_->buffer_pool_manager_;
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(row_->GetRowId().GetPageId()));
	RowId next_rid;
	if (page->GetNextTupleRid(row_->GetRowId(), &next_rid) == false) {
		while (page->GetNextPageId() != INVALID_PAGE_ID) {
			auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(page->GetNextPageId()));
			buffer_pool_manager->UnpinPage(page->GetTablePageId(), false);
			page = next_page;
			if (page->GetFirstTupleRid(&next_rid)) {
				break;
			}
		}
	}
	delete row_;
	row_ = new Row(next_rid);
	if (*this != table_heap_->End()) {
		table_heap_->GetTuple(row_, nullptr);
	}
	buffer_pool_manager->UnpinPage(page->GetTablePageId(), false);

	return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
	TableIterator Temp(*this);
	++(*this);
	return Temp;
}
