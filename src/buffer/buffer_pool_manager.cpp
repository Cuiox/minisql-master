#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
	Page* p = nullptr;
	frame_id_t frame_id = -1;
	if (page_table_.find(page_id) != page_table_.end()) {
		frame_id = page_table_[page_id];
		p = &pages_[frame_id];
		//replacer_->Pin(frame_id);
		p->pin_count_++;
		//return p;
	} else {
		if (!free_list_.empty()) {
			frame_id = free_list_.front();
			free_list_.pop_front();
		} else {
			bool pick_success = replacer_->Victim(&frame_id);
			if (!pick_success) {
				return nullptr;
			}
		}
		p = &pages_[frame_id];
		if (p->IsDirty()) {
			FlushPage(p->page_id_);
		}
		page_table_.erase(p->page_id_);
		page_table_.emplace(page_id, frame_id);

		p->page_id_ = page_id;
		p->pin_count_ = 0; // or 1?
		p->is_dirty_ = false; // or false?

		disk_manager_->ReadPage(page_id, p->data_);
	}

	return p;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
	if (free_list_.empty() && replacer_->Size() == 0) {
		return nullptr;
	} else {
		frame_id_t frame_id = -1;
		page_id = disk_manager_->AllocatePage();
		if (!free_list_.empty()) { // pick from free list
			frame_id = free_list_.front();
			free_list_.pop_front();
		} else { // pick from replacer
			bool pick_success = replacer_->Victim(&frame_id);
			if (!pick_success) {
				return nullptr;
			}
		}
		Page* p = &pages_[frame_id];
		if (p->IsDirty()) {
			FlushPage(p->page_id_);
		}
		page_table_.erase(p->page_id_);
		page_table_.emplace(page_id, frame_id);
		p->ResetMemory();
		p->page_id_ = page_id;
		p->pin_count_ = 0; // or 1?
		p->is_dirty_ = false; // or false?

		return p;
	}

}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
	bool flag = false;
	if (page_table_.find(page_id) == page_table_.end()) {
		flag = true; // not exist
	} else {
		frame_id_t frame_id = page_table_[page_id];
		Page* p = &pages_[frame_id];
		if (p->pin_count_ == 0) {
			disk_manager_->DeAllocatePage(page_id);
			page_table_.erase(page_id);
			free_list_.emplace_back(frame_id);
			// free_list_.push_back(frame_id);
			//p->ResetMemory();
			p->page_id_ = INVALID_PAGE_ID;
			p->is_dirty_ = false;
			flag = true;
		} else {
			flag = false; // non-zero pin-count; not need
		}		
	}

	return flag;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
	bool flag = false;
	if (page_table_.find(page_id) != page_table_.end()) {
		frame_id_t frame_id = page_table_[page_id];
		Page* p = &pages_[frame_id];
		p->pin_count_ = 0; // unpin
		if (is_dirty) {
			p->is_dirty_ = true;
		}
		replacer_->Unpin(frame_id);
		flag = true;
	} 
	// else {
	// 	flag = false;
	// }
	return flag;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
	bool flag = false;
	if (page_table_.find(page_id) != page_table_.end()) {
		frame_id_t frame_id = page_table_[page_id];
		Page* p = &pages_[frame_id];
		if (p->is_dirty_) { // only dirty page needs flush
			disk_manager_->WritePage(page_id, p->data_);
			p->is_dirty_ = false;
		}
		flag = true;
	}

	return flag;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}