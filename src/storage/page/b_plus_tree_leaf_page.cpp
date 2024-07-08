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
    void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page, page_id_t parentPage, int max_size) {
        SetSize(0);
        SetPageType(IndexPageType::LEAF_PAGE);
        SetMaxSize(max_size);
        SetPage(page);
        SetParentPage(parentPage);
        next_page_id_ = INVALID_PAGE_ID;
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
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & { return array_[index]; }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
        // replace with your own code
        auto target = std::lower_bound(array_, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
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
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKey(const KeyType &key, const KeyComparator &comparator) -> bool {
        auto i = KeyIndex(key, comparator);
        if (i == GetSize() || comparator(array_[i].first, key)) {
            return false;
        }
        std::move_backward(array_ + i + 1, array_ + GetSize(), array_ + i);
        IncreaseSize(-1);
        return true;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFrontNode(const MappingType &node) {
        std::move_backward(array_, array_ + GetSize(), array_ + 1);
        array_[0] = node;
        IncreaseSize(1);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeRightNode(const B_PLUS_TREE_LEAF_PAGE_TYPE *other) {
        for (int i = 0; i < other->GetSize(); ++i) {
            array_[i + GetSize()] = other->array_[i];
        }
        SetNextPageId(other->GetNextPageId());
        IncreaseSize(other->GetSize());
    }


    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator,
                                            bool &ans) const -> ValueType {
        auto target = std::lower_bound(array_, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
            return comparator(pair.first, k) < 0;
        });

        if (target == array_ + GetSize()) {
            // TODO:没用？
            if (comparator(KeyAt(GetSize() - 1), key) == 0) ans = true;
            return ValueAt(GetSize() - 1);
        }
        if (comparator(target->first, key) == 0) {
            ans = true;
            return target->second;
        }
        return std::prev(target)->second;
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool
    B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
        auto target = std::lower_bound(array_, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
            return comparator(pair.first, k) < 0;
        });
        if (target == array_ + GetSize()) {
            array_[GetSize()] = {key, value};
            IncreaseSize(1);
            return true;
        }
        if (comparator(key, (*target).first) == 0)return false;
        auto d = target - array_;
        std::move_backward(target, array_ + GetSize(), array_ + GetSize() + 1);

        *(array_ + d) = {key, value};
        IncreaseSize(1);
        return true;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLeafData(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *other) {
        for (int i = index; i < other->GetSize(); ++i) {
            array_[i - index] = other->array_[i];
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
