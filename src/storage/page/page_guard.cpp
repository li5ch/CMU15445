#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

	BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
		bpm_ = that.bpm_;
		is_dirty_ = that.is_dirty_;
		page_ = that.page_;

		that.bpm_ = nullptr;
		that.is_dirty_ = false;
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
		if (page_ == nullptr) return;
		if (page_->GetPageId() != INVALID_PAGE_ID) {
			bpm_->UnpinPage(page_->GetPageId(), page_->IsDirty());

		}
		bpm_ = nullptr;
		page_ = nullptr;
		is_dirty_ = false;
	}

	auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
		if (this != &that) {
			Drop();
			bpm_ = that.bpm_;
			page_ = that.page_;
			is_dirty_ = that.is_dirty_;
			that.bpm_ = nullptr;
			that.page_ = nullptr;
			that.is_dirty_ = false;
		}

		return *this;
	}

	BasicPageGuard::~BasicPageGuard() {
		if (page_ != nullptr) {
			Drop();
		}
	};  // NOLINT

	ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
		guard_ = std::move(that.guard_);
	}

	auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
		guard_ = std::move(that.guard_);
		return *this;
	}

	void ReadPageGuard::Drop() {
		if (guard_.page_ != nullptr) {
			guard_.page_->RUnlatch();
		}
		guard_.Drop();

	}

	ReadPageGuard::~ReadPageGuard() {
		Drop();
	}  // NOLINT

	WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
		guard_.page_->WLatch();
		guard_ = std::move(that.guard_);
		guard_.page_->WUnlatch();
	}

	auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
		guard_ = std::move(that.guard_);
		return *this;
	}

	void WritePageGuard::Drop() {
		if (guard_.page_) {
			guard_.page_->WUnlatch();
		}
		guard_.Drop();

	}

	WritePageGuard::~WritePageGuard() {

		Drop();
	}  // NOLINT

}  // namespace bustub
