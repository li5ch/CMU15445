#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

    INDEX_TEMPLATE_ARGUMENTS
    BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                              const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
        : index_name_(std::move(name)),
          bpm_(buffer_pool_manager),
          comparator_(std::move(comparator)),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size),
          header_page_id_(header_page_id) {
        //  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
        //  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
        root_page_id_ = INVALID_PAGE_ID;
    }

/*
 * Helper function to decide whether current b+tree is empty
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
        // Declaration of context instance.
        Context ctx;
        (void) ctx;
        auto root = GetRootPageId();
        auto read_page_guard = bpm_->FetchPage(root);
        read_page_guard->RLatch();
        auto node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        while (!node->IsLeafPage()) {
            auto n = reinterpret_cast<const InternalPage *>(read_page_guard->GetData());
            auto v = n->Lookup(key, comparator_);
            read_page_guard->RUnlatch();
            read_page_guard = bpm_->FetchPage(v);
            read_page_guard->RLatch();
            node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
            bpm_->UnpinPage(read_page_guard->GetPageId(), false);
        }
        if (node->IsLeafPage()) {
            auto n = reinterpret_cast<const LeafPage *>(read_page_guard->GetData());
            bpm_->UnpinPage(read_page_guard->GetPageId(), false);

            bool ans = false;
            auto v = n->Lookup(key, comparator_, ans);
            read_page_guard->RUnlatch();
            if (ans) {
                result->emplace_back(v);
                return true;
            }
        }
        return false;
    }


    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::SplitLeafNode(LeafPage *leaf_node, Page *new_page) -> LeafPage * {
        page_id_t pageId;
        auto newLeafPage = bpm_->NewPage(&pageId);
        newLeafPage->WLatch();
        auto root_page_leaf = reinterpret_cast<LeafPage *>(newLeafPage->GetData());
        root_page_leaf->Init(pageId, leaf_node->GetParentPage(), leaf_max_size_);
        root_page_leaf->CopyLeafData(leaf_node->GetMaxSize() / 2, leaf_node);
        root_page_leaf->SetSize(leaf_node->GetMaxSize() - (leaf_node->GetMaxSize()) / 2);
        root_page_leaf->SetNextPageId(leaf_node->GetNextPageId());
        leaf_node->SetNextPageId(pageId);
        leaf_node->SetSize(leaf_node->GetMaxSize() / 2);
        new_page = newLeafPage;
        return root_page_leaf;
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::SplitInternalNode(InternalPage *node) -> InternalPage * {
        page_id_t pageId;
        auto newLeafPage = bpm_->NewPage(&pageId);
        auto new_node = reinterpret_cast<InternalPage *>(newLeafPage->GetData());
        new_node->Init(pageId, node->GetParentPage(), internal_max_size_);
        new_node->CopyLeafData((node->GetMaxSize()) / 2 + 1, node);
        new_node->SetSize(node->GetSize() - (node->GetMaxSize()) / 2 - 1);
        node->SetSize((node->GetMaxSize()) / 2 + 1);

        return new_node;
    }


    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::Lookup(const KeyType &key, int operation_type) -> LeafPage * {
        // Declaration of context instance.
        // TODO:需要判断读写操作来加读写锁
        Context ctx;
        (void) ctx;
        auto root = GetRootPageId();
        auto read_page_guard = bpm_->FetchPage(root);
        read_page_guard->WLatch();
        auto node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        while (!node->IsLeafPage()) {
            auto n = reinterpret_cast<const InternalPage *>(read_page_guard->GetData());
            auto v = n->Lookup(key, comparator_);
            switch (operation_type) {
                case 0:
                    // insert
                    if (n->GetSize() < n->GetMaxSize() - 1) {
                        read_page_guard->WUnlatch();
                        bpm_->UnpinPage(read_page_guard->GetPageId(), false);
                    }
                    break;
                case 1:
                    if (n->GetSize() > std::ceil(n->GetMaxSize() / 2)) {
                        read_page_guard->WUnlatch();
                        bpm_->UnpinPage(read_page_guard->GetPageId(), false);
                    }
                    break;
                default:
                    break;
            }

            read_page_guard = bpm_->FetchPage(static_cast<page_id_t>(v));
            read_page_guard->WLatch();
            node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        }
        if (node->IsLeafPage()) {
            auto n = reinterpret_cast<LeafPage *>(read_page_guard->GetData());
            bpm_->UnpinPage(read_page_guard->GetPageId(), false);
            return n;
        }
        return nullptr;
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
        // Declaration of context instance.
        Context ctx;
        (void) ctx;
        if (IsEmpty()) {
            page_id_t pageId;
            auto page = bpm_->NewPage(&pageId);
            //    WritePageGuard guard = bpm_->FetchPageWrite(pageId);
            page->WLatch();
            auto root_page_leaf = reinterpret_cast<LeafPage *>(page->GetData());
            root_page_leaf->Init(pageId, INVALID_PAGE_ID, leaf_max_size_);
            root_page_id_ = pageId;
            root_page_leaf->Insert(key, value, comparator_);
            bpm_->UnpinPage(page->GetPageId(), true);
            page->WUnlatch();
        } else {
            // 递归向上分裂节点
            auto leaf_node = Lookup(key, 0);
            if (leaf_node) {
                auto leaf_page = bpm_->FetchPage(leaf_node->GetPage());
                leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
                leaf_node->Insert(key, value, comparator_);
                // insert to parent 递归
//                std::cout << "insert leaf node1:" << leaf_node->GetPage() << DrawBPlusTree() << std::endl;
                if (leaf_node->GetSize() >= leaf_node->GetMaxSize()) {
                    // split leaf node
                    Page *newPage = nullptr;
                    auto newNode = SplitLeafNode(leaf_node, newPage);
                    // insert to parent 递归
                    LOG_WARN("spilt leaf node:%d", newNode->GetPage());
//                    std::cout << "after split leaf node" << DrawBPlusTree() << std::endl;
//                    LOG_WARN("1spilt leaf node:%d", newNode->GetPage());
                    InsertToParent(newNode->KeyAt(0), leaf_node, newNode, txn);
                    leaf_page->WUnlatch();
                    newPage->WUnlatch();
                    bpm_->UnpinPage(leaf_node->GetPage(), true);
                    bpm_->UnpinPage(newNode->GetPage(), true);
                } else {
                    leaf_page->WUnlatch();
                    bpm_->UnpinPage(leaf_node->GetPage(), true);
                }

            } else {
                bpm_->UnpinPage(leaf_node->GetPage(), false);
            }

            return false;
        }
        return false;
    }
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
        // Declaration of context instance.x`
        Context ctx;
        (void) ctx;
        auto cur_node = Lookup(key, 1);
        auto cur_page = bpm_->FetchPage(cur_node->GetPage());
        cur_node = reinterpret_cast<LeafPage *>(cur_page->GetData());
        auto ok = cur_node->DeleteKey(key, comparator_);
        if (!ok) {
            cur_page->WUnlatch();
            bpm_->UnpinPage(cur_node->GetPage(), false);
            return;
        }
        if (cur_node->GetSize() >= std::ceil(cur_node->GetMaxSize() / 2) || cur_node->IsRootPage()) {
            cur_page->WUnlatch();
            bpm_->UnpinPage(cur_node->GetPage(), true);
            return;
        }

        auto parent_page = bpm_->FetchPage(cur_node->GetParentPage());
        auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
        page_id_t left_page_id = INVALID_PAGE_ID, right_page_id = INVALID_PAGE_ID;
        int j;
        for (int i = 0; i < parent_node->GetSize(); ++i) {
            if (parent_node->GetItem(i).second == cur_node->GetPage()) {
                j = i;
                if (i > 0) left_page_id = parent_node->GetItem(i - 1).second;
                if (i + 1 < parent_node->GetSize()) right_page_id = parent_node->GetItem(i + 1).second;
                break;
            }
        }
        if (left_page_id != INVALID_PAGE_ID) {
            auto le_page = bpm_->FetchPage(left_page_id);
            le_page->WLatch();
            auto le_node = reinterpret_cast<LeafPage *>(le_page->GetData());
            if (le_node->GetSize() > std::ceil(le_node->GetMaxSize() / 2)) {
                cur_node->InsertFrontNode(le_node->GetItem(le_node->GetSize() - 1));
                parent_node->SetKeyAt(j, le_node->KeyAt(le_node->GetSize() - 1));
                le_node->IncreaseSize(-1);
                bpm_->UnpinPage(left_page_id, true);
                bpm_->UnpinPage(parent_node->GetPage(), true);
                bpm_->UnpinPage(cur_node->GetPage(), true);
                parent_page->WUnlatch();
                cur_page->WUnlatch();
                le_page->WUnlatch();
                return;
            } else {
                le_node->MergeRightNode(cur_node);
                bpm_->DeletePage(cur_node->GetPage());
                bpm_->UnpinPage(left_page_id, true);
                bpm_->UnpinPage(cur_node->GetPage(), true);
                // 实现DeleteInternalKey方法，往上递归
                parent_node->DeleteKeyIndex(j);
                RebalanceInternalKey(parent_node, txn);
                cur_page->WUnlatch();
                le_page->WUnlatch();
                return;
            }
        }
        if (right_page_id != INVALID_PAGE_ID) {
            auto le_page = bpm_->FetchPage(right_page_id);
            le_page->WLatch();
            auto le_node = reinterpret_cast<LeafPage *>(le_page->GetData());
            if (le_node->GetSize() > std::ceil(le_node->GetMaxSize() / 2)) {
                cur_node->InsertFrontNode(le_node->GetItem(le_node->GetSize() - 1));
                parent_node->SetKeyAt(j, le_node->KeyAt(le_node->GetSize() - 1));
                le_node->IncreaseSize(-1);
                bpm_->UnpinPage(right_page_id, true);
                bpm_->UnpinPage(parent_node->GetPage(), true);
                bpm_->UnpinPage(cur_node->GetPage(), true);
                cur_page->WUnlatch();
                le_page->WUnlatch();
                return;
            } else {
                le_node->MergeRightNode(cur_node);
                bpm_->DeletePage(cur_node->GetPage());
                bpm_->UnpinPage(right_page_id, true);
                bpm_->UnpinPage(cur_node->GetPage(), true);
                // 实现DeleteInternalKey方法，往上递归
                parent_node->DeleteKeyIndex(j);
                RebalanceInternalKey(parent_node, txn);
                cur_page->WUnlatch();
                le_page->WUnlatch();
                return;
            }
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::RebalanceInternalKey(InternalPage *node, Transaction *txn) {
        if (node->GetSize() >= std::ceil(node->GetMaxSize() / 2)) {
            return;
        } else {
            if (node->IsRootPage()) {
                // TODO:?
                return;
            }
            auto parent_page = bpm_->FetchPage(node->GetParentPage());
            auto parent_page_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
            int j;
            page_id_t left_page_id = INVALID_PAGE_ID, right_page_id = INVALID_PAGE_ID;
            for (int i = 0; i <= parent_page_node->GetSize(); ++i) {
                if (parent_page_node->GetItem(i).second == node->GetPage()) {
                    j = i;
                    if (i > 0) left_page_id = node->GetItem(i - 1).second;
                    if (i + 1 < node->GetSize()) right_page_id = node->GetItem(i + 1).second;
                    break;
                }
            }
            if (left_page_id != INVALID_PAGE_ID) {
                auto le_page = bpm_->FetchPage(left_page_id);
                auto le_node = reinterpret_cast<InternalPage *>(le_page->GetData());
                if (le_node->GetSize() > std::ceil(le_node->GetMaxSize() / 2)) {
                    node->InsertFrontNode(parent_page_node->GetItem(j));
                    parent_page_node->SetKeyAt(j, le_node->KeyAt(le_node->GetSize() - 1));
                    le_node->IncreaseSize(-1);
                    bpm_->UnpinPage(left_page_id, true);
                    bpm_->UnpinPage(parent_page->GetPageId(), true);
                    return;
                } else {
                    le_node->MergeParentAndLRNode(node, parent_page_node->KeyAt(j));
                    bpm_->DeletePage(parent_page_node->GetPage());
                    bpm_->DeletePage(node->GetPage());
                    bpm_->UnpinPage(left_page_id, true);
                    // 实现DeleteInternalKey方法，往上递归
                    RebalanceInternalKey(node, txn);
                    return;
                }
            }
            if (right_page_id != INVALID_PAGE_ID) {
                auto le_page = bpm_->FetchPage(right_page_id);
                auto le_node = reinterpret_cast<InternalPage *>(le_page->GetData());
                if (le_node->GetSize() > std::ceil(le_node->GetMaxSize() / 2)) {
                    node->InsertFrontNode(parent_page_node->GetItem(j));
                    parent_page_node->SetKeyAt(j, le_node->KeyAt(le_node->GetSize() - 1));
                    le_node->IncreaseSize(-1);
                    bpm_->UnpinPage(right_page_id, true);
                    bpm_->UnpinPage(parent_page->GetPageId(), true);
                    return;
                } else {
                    le_node->MergeParentAndLRNode(node, parent_page_node->KeyAt(j));
                    bpm_->DeletePage(parent_page_node->GetPage());
                    bpm_->DeletePage(node->GetPage());
                    bpm_->UnpinPage(right_page_id, true);
                    // 实现DeleteInternalKey方法，往上递归
                    RebalanceInternalKey(node, txn);
                    return;
                }
            }
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertToParent(const KeyType &key, BPlusTreePage *old_node, BPlusTreePage *new_node,
                                        Transaction *txn) {
        if (old_node->IsRootPage()) {
            page_id_t pageId;
            auto page = bpm_->NewPage(&pageId);
            auto root_page = reinterpret_cast<InternalPage *>(page->GetData());
            root_page->Init(pageId, INVALID_PAGE_ID, internal_max_size_);
            root_page->PopulateNewRoot(old_node->GetPage(), key, new_node->GetPage());

            root_page_id_ = pageId;
            old_node->SetParentPage(root_page_id_);
            new_node->SetParentPage(root_page_id_);
            bpm_->UnpinPage(root_page_id_, true);
            return;
        }
        auto parent_page = bpm_->FetchPage(old_node->GetParentPage());
        auto pa_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
        if (pa_node->GetSize() < pa_node->GetMaxSize()) {
            pa_node->Insert(key, new_node->GetPage(), comparator_);
            bpm_->UnpinPage(pa_node->GetPage(), true);
            parent_page->WUnlatch();
            return;
        }

        pa_node->Insert(key, new_node->GetPage(), comparator_);
        auto k = pa_node->KeyAt(pa_node->GetMaxSize() / 2 + 1);
        auto node1 = SplitInternalNode(pa_node);

        for (int i = 0; i < node1->GetSize(); i++) {
            auto ch = bpm_->FetchPage(node1->ValueAt(i));
            auto ch_page = reinterpret_cast<BPlusTreePage *>(ch->GetData());
            ch_page->SetParentPage(node1->GetPage());
            bpm_->UnpinPage(ch->GetPageId(), true);
        }
        InsertToParent(k, pa_node, node1, txn);
        parent_page->WUnlatch();
        bpm_->UnpinPage(pa_node->GetPage(), true);
        bpm_->UnpinPage(node1->GetPage(), true);
    }


/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
        auto root = GetRootPageId();
        auto read_page_guard = bpm_->FetchPage(root);
        auto node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        while (!node->IsLeafPage()) {
            auto n = reinterpret_cast<const InternalPage *>(read_page_guard->GetData());
            read_page_guard = bpm_->FetchPage(n->ValueAt(0));
            node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        }
        if (node->IsLeafPage()) {
            return INDEXITERATOR_TYPE(bpm_, read_page_guard);
        }

        return INDEXITERATOR_TYPE();
    }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
        auto root = GetRootPageId();
        auto read_page_guard = bpm_->FetchPage(root);
        auto node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        while (!node->IsLeafPage()) {
            auto n = reinterpret_cast<const InternalPage *>(read_page_guard->GetData());
            auto v = n->Lookup(key, comparator_);
            read_page_guard = bpm_->FetchPage(static_cast<page_id_t>(v));
            node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        }
        if (node->IsLeafPage()) {
            auto n = reinterpret_cast<LeafPage *>(read_page_guard->GetData());
            auto i = n->KeyIndex(key, comparator_);
            return INDEXITERATOR_TYPE(bpm_, read_page_guard, i);
        }
        return INDEXITERATOR_TYPE();
    }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
        auto root = GetRootPageId();
        auto read_page_guard = bpm_->FetchPage(root);
        auto node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        while (!node->IsLeafPage()) {
            auto n = reinterpret_cast<const InternalPage *>(read_page_guard->GetData());
            read_page_guard = bpm_->FetchPage(n->ValueAt(n->GetSize() - 1));
            node = reinterpret_cast<const BPlusTreePage *>(read_page_guard->GetData());
        }
        if (node->IsLeafPage()) {
            auto n = reinterpret_cast<const LeafPage *>(read_page_guard->GetData());
            while (n->GetNextPageId() != INVALID_PAGE_ID) {
                read_page_guard = bpm_->FetchPage(n->GetNextPageId());
                n = reinterpret_cast<const LeafPage *>(read_page_guard->GetData());
            }

            return INDEXITERATOR_TYPE(bpm_, read_page_guard, n->GetSize());
        }
        return INDEXITERATOR_TYPE();
    }

/**
 * @return Page id of the root of this tree
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
        int64_t key;
        std::ifstream input(file_name);
        while (input >> key) {
            KeyType index_key;
            index_key.SetFromInteger(key);
            RID rid(key);
            Insert(index_key, rid, txn);
        }
    }
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
        int64_t key;
        std::ifstream input(file_name);
        while (input >> key) {
            KeyType index_key;
            index_key.SetFromInteger(key);
            Remove(index_key, txn);
        }
    }

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
        int64_t key;
        char instruction;
        std::ifstream input(file_name);
        while (input) {
            input >> instruction >> key;
            RID rid(key);
            KeyType index_key;
            index_key.SetFromInteger(key);
            switch (instruction) {
                case 'i':
                    Insert(index_key, rid, txn);
                    break;
                case 'd':
                    Remove(index_key, txn);
                    break;
                default:
                    break;
            }
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
        auto root_page_id = GetRootPageId();
        auto guard = bpm->FetchPageBasic(root_page_id);
        PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
        if (page->IsLeafPage()) {
            auto *leaf = reinterpret_cast<const LeafPage *>(page);
            std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

            // Print the contents of the leaf page.
            std::cout << "Contents: ";
            for (int i = 0; i < leaf->GetSize(); i++) {
                std::cout << leaf->KeyAt(i);
                if ((i + 1) < leaf->GetSize()) {
                    std::cout << ", ";
                }
            }
            std::cout << std::endl;
            std::cout << std::endl;

        } else {
            auto *internal = reinterpret_cast<const InternalPage *>(page);
            std::cout << "Internal Page: " << page_id << std::endl;

            // Print the contents of the internal page.
            std::cout << "Contents: ";
            for (int i = 0; i < internal->GetSize(); i++) {
                std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
                if ((i + 1) < internal->GetSize()) {
                    std::cout << ", ";
                }
            }
            std::cout << std::endl;
            std::cout << std::endl;
            for (int i = 0; i < internal->GetSize(); i++) {
                auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
                PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
            }
        }
    }

/**
 * This method is used for debug only, You don't need to modify
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
        if (IsEmpty()) {
            LOG_WARN("Drawing an empty tree");
            return;
        }

        std::ofstream out(outf);
        out << "digraph G {" << std::endl;
        auto root_page_id = GetRootPageId();
        auto guard = bpm->FetchPageBasic(root_page_id);
        ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
        out << "}" << std::endl;
        out.close();
    }

/**
 * This method is used for debug only, You don't need to modify
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
        std::string leaf_prefix("LEAF_");
        std::string internal_prefix("INT_");
        if (page->IsLeafPage()) {
            auto *leaf = reinterpret_cast<const LeafPage *>(page);
            // Print node name
            out << leaf_prefix << page_id;
            // Print node properties
            out << "[shape=plain color=green ";
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
                << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size="
                << leaf->GetSize()
                << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < leaf->GetSize(); i++) {
                out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Leaf node link if there is a next page
            if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
                out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
                out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
            }
        } else {
            auto *inner = reinterpret_cast<const InternalPage *>(page);
            // Print node name
            out << internal_prefix << page_id;
            // Print node properties
            out << "[shape=plain color=pink ";  // why not?
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
                << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size="
                << inner->GetSize()
                << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < inner->GetSize(); i++) {
                out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
                if (i > 0) {
                    out << inner->KeyAt(i);
                } else {
                    out << " ";
                }
                out << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print leaves
            for (int i = 0; i < inner->GetSize(); i++) {
                auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
                auto child_page = child_guard.template As<BPlusTreePage>();
                ToGraph(child_guard.PageId(), child_page, out);
                if (i > 0) {
                    auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
                    auto sibling_page = sibling_guard.template As<BPlusTreePage>();
                    if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
                        out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
                            << child_guard.PageId() << "};\n";
                    }
                }
                out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
                if (child_page->IsLeafPage()) {
                    out << leaf_prefix << child_guard.PageId() << ";\n";
                } else {
                    out << internal_prefix << child_guard.PageId() << ";\n";
                }
            }
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
        if (IsEmpty()) {
            return "()";
        }

        PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
        std::ostringstream out_buf;
        p_root.Print(out_buf);

        return out_buf.str();
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
        auto root_page_guard = bpm_->FetchPageBasic(root_id);
        auto root_page = root_page_guard.template As<BPlusTreePage>();
        PrintableBPlusTree proot;

        if (root_page->IsLeafPage()) {
            auto leaf_page = root_page_guard.template As<LeafPage>();
            proot.keys_ = leaf_page->ToString();
            proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

            return proot;
        }

        // draw internal page
        auto internal_page = root_page_guard.template As<InternalPage>();
        proot.keys_ = internal_page->ToString();
        proot.size_ = 0;
        for (int i = 0; i < internal_page->GetSize(); i++) {
            page_id_t child_id = internal_page->ValueAt(i);
            PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
            proot.size_ += child_node.size_;
            proot.children_.push_back(child_node);
        }

        return proot;
    }

    template
    class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

    template
    class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

    template
    class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

    template
    class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

    template
    class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
