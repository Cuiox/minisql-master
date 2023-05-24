#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 * 分配一个空闲页，并通过page_offset返回所分配的空闲页位于该段中的下标（从0开始）
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
	bool flag = false;
	if (page_allocated_ < GetMaxSupportedSize() && next_free_page_ < GetMaxSupportedSize()) { // 还有空闲页
		page_offset = next_free_page_; // 该空闲页的下标，注意传的是地址
		uint32_t page_byte_index = next_free_page_ / 8; // 该空闲页的下标 / 8 = 该空闲页在 bytes 中的下标
		uint8_t page_bit_index = next_free_page_ % 8;
		// page_offset = page_byte_index * 8 + page_bit_index; 7,6,5,4,3,2,1,0; 小端?
		bytes[page_byte_index] = bytes[page_byte_index] | (0x01 << page_bit_index); // 将对应的bit置一
		page_allocated_++; // 并对总数加一
		// 更新 next_free_page_
		for (uint32_t i = next_free_page_; i < GetMaxSupportedSize(); i++) { // uint32_t i=next_free_page_ 亦可
		// DiskManagerTest.FreePageAllocationTest (8989 ms) i = 0
		// DiskManagerTest.FreePageAllocationTest (280 ms)  i = next_free_page_ 
			if (IsPageFree(i)) {
				next_free_page_ = i; // 找到第一个 free 的 page
				break;
			}
		}
		flag = true;
	} else {
		flag = false;
	}

	return flag;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) { 
	bool flag = false;
	if (page_offset < GetMaxSupportedSize()) { // 首先要在范围内
		if (!IsPageFree(page_offset)) { // 其次该页需要已经被分配
			// 尝试 DeAllocate Page
			uint32_t byte_index = page_offset / 8;
			uint8_t bit_index = page_offset % 8;
			bytes[byte_index] = bytes[byte_index] & (~(0x01 << bit_index));
			page_allocated_--;
			next_free_page_ = page_offset;
			flag = true;
		} else {
			flag = false;
		}
	} else {
		flag = false;
	}
	return flag;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
	return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
	bool flag = false;
	if ((bytes[byte_index] & (0x01 << bit_index))) { // (bytes[byte_index] & (0x01 << bit_index)) == 1
		flag = false;
	} else {
		flag = true;
	}

	return flag;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;