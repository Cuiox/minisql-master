#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
	bool flag = false;
	TablePage* current_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetCurrentPageId()));
	if (current_page == nullptr) {
		flag = false;
	} else {
		while (current_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_) == false) {
			page_id_t next_page_id = current_page->GetNextPageId();
			if (next_page_id == INVALID_PAGE_ID) { // 最后一个数据页
				// 分配新的数据页
				TablePage* new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
				if (new_page == nullptr) {
					buffer_pool_manager_->UnpinPage(current_page->GetTablePageId(), false); // Unpin
					return false; // 
				} else {
					new_page->Init(next_page_id, current_page->GetPageId(), log_manager_, txn);
					current_page->SetNextPageId(next_page_id);
					buffer_pool_manager_->UnpinPage(current_page->GetTablePageId(), true);
					current_page = new_page;					
				}
			} else {
				buffer_pool_manager_->UnpinPage(current_page->GetTablePageId(), false);
				current_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
			}
		}
		// Insert Tuple success
		current_page_id_ = current_page->GetPageId();
		buffer_pool_manager_->UnpinPage(current_page_id_, true); // is_dirty
		flag = true;
	}
 
	return flag;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
	bool flag = false;
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
	if (page != nullptr) {
		Row old_row(rid);
		uint8_t fail_flag = 0;
		if (page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_, fail_flag)) {
			flag = true;
		} else {
			if (fail_flag == 2) {
				page->ApplyDelete(rid, txn, log_manager_);
				page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
			}		
		}
		buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
	}
	return flag;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  	if (page == nullptr) {
		LOG(ERROR) << "In TableHeap::ApplyDelete page which contains the tuple not found";
		return;
  	}
  // Step2: Delete the tuple from the page.
	page->ApplyDelete(rid, txn, log_manager_);
	buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
	return;
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
	bool flag = false;
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
	if (page != nullptr) {
		flag = page->GetTuple(row, schema_, txn, lock_manager_);
		buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
	}
	return flag;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
	auto page_id = first_page_id_;
	RowId rid;
	while (page_id != INVALID_PAGE_ID) {
		auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
		buffer_pool_manager_->UnpinPage(page_id, false);
		if (page->GetFirstTupleRid(&rid)) {
			break;
		}
		page_id = page->GetNextPageId();
	}

	return TableIterator(this, rid);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  	return TableIterator(this, INVALID_ROWID);
}
