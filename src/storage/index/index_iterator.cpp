/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
	INDEX_TEMPLATE_ARGUMENTS
	INDEXITERATOR_TYPE::IndexIterator() = default;

	INDEX_TEMPLATE_ARGUMENTS
	INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

	INDEX_TEMPLATE_ARGUMENTS
	auto INDEXITERATOR_TYPE::IsEnd() -> bool {
		auto node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
		if (node_index_ == node->GetSize() - 1 && node->GetNextPageId() == INVALID_PAGE_ID) {
			return true;
		}

		return false;
	}

	INDEX_TEMPLATE_ARGUMENTS
	auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
		auto node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
		return node->GetItem(node_index_);
	}

	INDEX_TEMPLATE_ARGUMENTS
	auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
		auto node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
		if (node_index_ == node->GetSize() - 1) {
			if (node->GetNextPageId() != INVALID_PAGE_ID) {
				page_ = bpm_->FetchPage(node->GetNextPageId());
				node_index_ = 0;
				return *this;
			}
		}
		node_index_++;
		return *this;
	}

	INDEX_TEMPLATE_ARGUMENTS
	auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
		return itr.bpm_ == bpm_ && itr.page_ == page_ && itr.node_index_ == node_index_;
	}

	INDEX_TEMPLATE_ARGUMENTS
	auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
		return !(itr.bpm_ == bpm_ && itr.page_ == page_ && itr.node_index_ == node_index_);
	}


	template
	class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

	template
	class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

	template
	class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

	template
	class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

	template
	class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
