#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  this->num_pages=num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (Size()==0) return false;
  else {
    *frame_id = lru_list_.back();
    hashmap.erase(*frame_id);
    lru_list_.pop_back();
    return true;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (hashmap.find(frame_id)!=hashmap.end()){
    lru_list_.erase(hashmap[frame_id]);
    hashmap.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (Size()<num_pages && hashmap.find(frame_id)==hashmap.end()){
    lru_list_.push_front(frame_id);
    hashmap[frame_id] = lru_list_.begin();
  }else return;
}

size_t LRUReplacer::Size() {
  return hashmap.size();
}