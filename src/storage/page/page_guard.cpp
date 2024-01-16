#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}


/** TODO(P1): Add implementation
   *
   * @brief Drop a page guard
   *
   * Dropping a page guard should clear all contents
   * (so that the page guard is no longer useful), and
   * it should tell the BPM that we are done using this page,
   * per the specification in the writeup.
 */
void BasicPageGuard::Drop() {
  // pin--
  bpm_->UnpinPage(page_->GetPageId(),page_->IsDirty());
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard(){
  if(page_!= nullptr){
    Drop();
  }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept{
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.page_->RLatch();
  guard_.Drop();
  guard_.page_->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {  guard_ = std::move(that.guard_); return *this; }

void WritePageGuard::Drop() {
  guard_.page_->WLatch();
  guard_.Drop();
  guard_.page_->WUnlatch();
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace bustub
