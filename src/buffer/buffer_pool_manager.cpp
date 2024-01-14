//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }
/**
 * TODO(P1): Add implementation
 *
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 *
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 *
 * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
 * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
 * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
 *
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  if (!free_list_.empty()) {
    auto c = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    page_table_[*page_id] = c;
    replacer_->RecordAccess(c, AccessType::Unknown);
    replacer_->SetEvictable(c, false);
    *page_id = AllocatePage();
    Page *page= new Page();
    page->page_id_ = *page_id;
    page->pin_count_=1;
    return page;
  } else {
    frame_id_t id;
    auto c = replacer_->Evict(&id);
    if (c) {
      if (pages_[id].is_dirty_) {
        disk_manager_->WritePage(id,pages_[id].GetData());
        pages_[id].ResetMemory();
        page_table_.erase(pages_[id].page_id_);
      }
      *page_id = AllocatePage();
      page_table_[*page_id] = c;
      replacer_->RecordAccess(c, AccessType::Unknown);
      replacer_->SetEvictable(c, false);
      *page_id = AllocatePage();
      Page *page= new Page();
      page->page_id_ = *page_id;
      page->pin_count_=1;
      return page;
    } else {
      return nullptr;
    }
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  return nullptr;
}


/**
   * TODO(P1): Add implementation
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if(page_table_.count(page_id)==0||pages_[page_table_[page_id]].pin_count_==0) return false;
  auto fid = page_table_[page_id];
  pages_[fid].pin_count_--;
  if(pages_[fid].pin_count_==0){
    replacer_->SetEvictable(fid,true);
    pages_[fid].is_dirty_ = is_dirty;

  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { return false; }

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
