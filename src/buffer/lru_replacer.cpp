#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
	this->num_pages = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
	bool flag = false;
	if (Size() == 0) { // lru_list_.empty()
		flag = false;
	} else {
		*frame_id = lru_list_.back();
		hashmap.erase(*frame_id);
		lru_list_.pop_back();
		flag = true;
	}

	return flag;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
	if (hashmap.find(frame_id) != hashmap.end()) {
		lru_list_.erase(hashmap[frame_id]);
		hashmap.erase(frame_id);
	}

	return;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
	if (hashmap.find(frame_id) == hashmap.end() && Size() < num_pages) {
		lru_list_.push_front(frame_id);
		hashmap.emplace(frame_id, lru_list_.begin());
		// hashmap[frame_id] = lru_list_.begin();
	}

	return;
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
	//return hashmap.size();
	return lru_list_.size();
}