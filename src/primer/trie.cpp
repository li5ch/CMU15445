#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (!root_) return nullptr;
  if (key.empty()) return nullptr;
  auto cur = root_;
  for (auto c : key) {
    if (cur->children_.count(c) == 0) {
      return nullptr;
    }
    cur = cur->children_.at(c);
  }
  auto node = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  return node->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  std::shared_ptr<TrieNode> newRoot{};
  if (!root_) {
    if (key.empty()) {
      std::shared_ptr<TrieNode> new_root_with_t =
          std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      return Trie{new_root_with_t};
    }
    newRoot = std::make_shared<TrieNode>();
  } else {
    if (key.empty()) {
      std::shared_ptr<TrieNode> new_root_with_v =
          std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value)));
      return Trie{new_root_with_v};
    }
    newRoot = root_->Clone();
  }
  auto cur = newRoot;
  auto pa = cur;
  for (int i = 0; i < int(key.size()) - 1; ++i) {
    if (pa->children_.count(key[i]) == 0) {
      cur = std::make_shared<TrieNode>();
      pa->children_[key[i]] = cur;
      pa = cur;
    } else {
      cur = std::shared_ptr<TrieNode>(std::move(cur->children_[key[i]])->Clone());
      pa->children_[key[i]] = cur;
      pa = cur;
    }
  }
  if (pa->children_.count(key.back())) {
    cur = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    pa->children_[key.back()] = cur;
  } else {
    pa->children_[key.back()] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  }
  return Trie{newRoot};

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
