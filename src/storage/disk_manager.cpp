#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 * 从磁盘中分配一个空闲页，并返回空闲页的逻辑页号
 */
page_id_t DiskManager::AllocatePage() {
	int32_t logical_page_id = INVALID_PAGE_ID;
	DiskFileMetaPage* meta_page_ = reinterpret_cast<DiskFileMetaPage*>(meta_data_);

	for (uint32_t i = 0; i < (PAGE_SIZE - 8) / 4; i++) {
		if (meta_page_->extent_used_page_[i] < BitmapPage<PAGE_SIZE>::GetMaxSupportedSize()) {
			BitmapPage<PAGE_SIZE> *bitmap = new BitmapPage<PAGE_SIZE>;
			ReadPhysicalPage(i * (BITMAP_SIZE + 1) + 1, (char*)bitmap); // Read
			
			uint32_t offset;
			bitmap->AllocatePage(offset);
			WritePhysicalPage(i * (BITMAP_SIZE + 1) +1, (char*)bitmap);
			delete bitmap;
			meta_page_->num_allocated_pages_++;
			if (meta_page_->extent_used_page_[i] == 0) {
      			meta_page_->num_extents_++;
      		}
			meta_page_->extent_used_page_[i]++;
			WritePhysicalPage(META_PAGE_ID, meta_data_);
			logical_page_id = i * BITMAP_SIZE + offset;
			break;
		}
	}
  	return logical_page_id;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
    DiskFileMetaPage *meta_page_ = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
    uint32_t extent = logical_page_id / BITMAP_SIZE;
    uint32_t offset = logical_page_id % BITMAP_SIZE;
    uint32_t physical_page_id = MapPageId(logical_page_id);
    BitmapPage<PAGE_SIZE> *bitmap = new BitmapPage<PAGE_SIZE>;
    ReadPhysicalPage(physical_page_id, (char*)bitmap);
    bitmap->DeAllocatePage(offset);
    WritePhysicalPage(physical_page_id, (char*)bitmap);
    delete bitmap;
    meta_page_->num_allocated_pages_--;
    meta_page_->extent_used_page_[extent]--;
    if (meta_page_->extent_used_page_[extent] == 0) {
        meta_page_->num_extents_--;
    }
    WritePhysicalPage(META_PAGE_ID, meta_data_);

    return;
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
    bool flag = false;
    BitmapPage<PAGE_SIZE> *bitmap = new BitmapPage<PAGE_SIZE>;
    uint32_t extent_id = logical_page_id / BITMAP_SIZE;
    uint32_t offset_id = logical_page_id % BITMAP_SIZE;
    uint32_t physical_page_id = extent_id * (BITMAP_SIZE + 1) + 1;
    ReadPhysicalPage(physical_page_id, (char*)bitmap);
    flag = bitmap->IsPageFree(offset_id);
    delete bitmap;
    return flag;
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
    uint32_t physical_page_id = 0;
    uint32_t extent_id = logical_page_id / BITMAP_SIZE;
    uint32_t offset_id = logical_page_id % BITMAP_SIZE;
    uint32_t bitmap_id = extent_id * (BITMAP_SIZE + 1) + 1;
    physical_page_id = bitmap_id + offset_id + 1;
    return physical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}