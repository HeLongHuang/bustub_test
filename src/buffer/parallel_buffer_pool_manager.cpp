//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"


namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  pool_size_ = pool_size;
  num_instances_ = num_instances;
  for (size_t i = 0 ; i < num_instances ; ++i){
    BufferPoolManager *buffer_manger = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    instance_.push_back(buffer_manger);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0 ; i < num_instances_ ; ++i){
    delete instance_[i];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_ * num_instances_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  // return nullptr;
  return instance_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  // return nullptr;
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  // return false;
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->UnpinPage(page_id,is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  // return false;
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  // return nullptr;

  Page *page;
  for (size_t i = 0 ; i < num_instances_ ; ++i){
    size_t cur_index = (start_index + i) % num_instances_;
    if ((page = instance_[cur_index]->NewPage(page_id)) != nullptr){
      start_index = (*page_id + 1) % num_instances_;
      return page;
    }
  }
  start_index++;
  return nullptr;

}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  // return false;
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0 ; i < num_instances_ ; ++i){
    instance_[i]->FlushAllPages();
  }
}

}  // namespace bustub
