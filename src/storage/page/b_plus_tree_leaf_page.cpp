//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
        SetSize(0);
        SetPageType(IndexPageType::LEAF_PAGE);
        SetMaxSize(max_size);
    }

/**
 * Helper methods to set/get next page id
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
        // replace with your own code
        return array_[index].first;
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
        // replace with your own code
        return array_[index].second;
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) -> ValueType {
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
    void
    B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
        auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
            return comparator(pair.first, k) < 0;
        });
        if (target == array_ + GetSize()) {
            array_[GetSize()] = {key, value};
            IncreaseSize(1);
            if(GetSize()>)
            return;
        }

        std::move_backward(target + 1, array_ + GetSize(), array_ + GetSize() + 1);
        *target = {key, value};
        IncreaseSize(1);
        return;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLeafData(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *other) {
        for (int i = index; i < other->GetSize(); ++i) {
            array_[i] = other->array_[i];
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::GetData(MappingType *array) {
        for (int i = 0; i < GetSize(); ++i) {
            array[i] = array_[i];
        }
    }

    template
    class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

    template
    class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;

    template
    class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

    template
    class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

    template
    class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
