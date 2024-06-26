#pragma once

#include "common/config.h"

namespace bustub {

	class BPlusTreeHeaderPage {
	public:
		// Delete all constructor / destructor to ensure memory safety
		BPlusTreeHeaderPage() = delete;

		BPlusTreeHeaderPage(const BPlusTreeHeaderPage &other) = delete;

		void SetRootPageId(page_id_t page_id) {
			root_page_id_ = page_id;
		}

		page_id_t root_page_id_;
	};

}  // namespace bustub
