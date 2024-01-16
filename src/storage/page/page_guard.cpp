#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {
/** TODO(P1): Add implementation
 *
 * @brief Move constructor for BasicPageGuard
 *
 * When you call BasicPageGuard(std::move(other_guard)), you
 * expect that the new guard will behave exactly like the other
 * one. In addition, the old page guard should not be usable. For
 * example, it should not be possible to call .Drop() on both page
 * guards and have the pin count decrease by 2.
 */
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  page_ = that.page_;

  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { return *this; }

BasicPageGuard::~BasicPageGuard(){};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { return *this; }

void ReadPageGuard::Drop() {}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
