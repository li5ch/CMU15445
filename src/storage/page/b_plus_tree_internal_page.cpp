//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page, page_id_t parent_id, int max_size) {
		SetSize(0);
		SetPageType(IndexPageType::INTERNAL_PAGE);
		SetMaxSize(max_size);
		SetPage(page);
		SetParentPage(parent_id);
	}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
	INDEX_TEMPLATE_ARGUMENTS
	auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
		// replace with your own code
		// TODO:assert
		return array_[index].first;
		KeyType key{};
		return key;
	}

	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLeafData(int index, B_PLUS_TREE_INTERNAL_PAGE_TYPE *other) {
		array_[0].second = other->array_[index].second;
		for (int i = index + 1; i < other->GetSize(); ++i) {
			array_[i - index] = other->array_[i];
		}
	}


	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index,
													const ValueType &valueType) { array_[index].second = valueType; }

	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyDataByIndex(int index, MappingType *array) {
		for (int i = index; i < GetSize(); ++i) {
			array_[i] = array[i];
		}
	}

	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteKeyIndex(int index) {

		std::move_backward(array_ + index + 1, array_ + GetSize(), array_ + index);
		IncreaseSize(-1);
	}

	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeParentAndLRNode(const B_PLUS_TREE_INTERNAL_PAGE_TYPE *node,
															  const KeyType &parent_key) {
		array_[GetSize()] = {parent_key, node->array_[0].second};
		for (int i = 1; i < node->GetSize(); ++i) {
			array_[i + GetSize()] = node->array_[i];
		}
		IncreaseSize(node->GetSize() + 1);
	}

	INDEX_TEMPLATE_ARGUMENTS
	auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetItem(int index) -> const MappingType & { return array_[index]; }

	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFrontNode(const MappingType &node) {
		std::move_backward(array_, array_ + GetSize(), array_ + 1);
		array_[0] = node;
		IncreaseSize(1);
	}


	INDEX_TEMPLATE_ARGUMENTS
	auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
		// replace with your own code
		auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
			return comparator(pair.first, k) < 0;
		});

		if (target == array_ + GetSize()) {
			if (comparator(KeyAt(GetSize() - 1), key) == 0)
				return GetSize() - 1;
			return GetSize();
		}
		return target - array_;
	}


	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
												const KeyComparator &comparator) {

		auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
			return comparator(pair.first, k) < 0;
		});
		if (target == array_ + GetSize()) {
			array_[GetSize()] = {key, value};
			IncreaseSize(1);
			return;
		}
		auto d = target - array_;
		std::move_backward(target, array_ + GetSize(), array_ + GetSize() + 1);
		array_[d] = {key, value};

		IncreaseSize(1);
	}

	INDEX_TEMPLATE_ARGUMENTS
	void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
														 const ValueType &new_value) {
		SetKeyAt(1, new_key);
		SetValueAt(0, old_value);
		SetValueAt(1, new_value);
		SetSize(2);
	}

	INDEX_TEMPLATE_ARGUMENTS
	auto
	B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
		auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
			return comparator(pair.first, k) < 0;
		});
		if (target == array_ + GetSize()) {
			return ValueAt(GetSize() - 1);
		}
		if (comparator(target->first, key) == 0) return target->second;
		return std::prev(target)->second;
	}

	INDEX_TEMPLATE_ARGUMENTS
	auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
		for (int i = 0; i < GetSize(); ++i) {
			if (array_[i].second == value) {
				return i;
			}
		}
		return -1;
	}
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
	INDEX_TEMPLATE_ARGUMENTS
	auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

// valuetype for internalNode should be page id_t
	template
	class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

	template
	class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

	template
	class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

	template
	class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

	template
	class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
