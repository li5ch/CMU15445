//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

	LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
 * based on LRU.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @param[out] frame_id id of frame that is evicted.
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
	auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
//  latch_.lock();
		std::scoped_lock<std::mutex> lock(latch_);
		if (!fifo_q_.empty()) {
			auto it = fifo_q_.rbegin();
			while (it != fifo_q_.rend() && !node_store_[*it].is_evictable_) {
				it++;
			}
			if (it != fifo_q_.rend()) {
				*frame_id = *it;
				fifo_q_.erase(std::next(it).base());
				curr_size_--;
				node_store_.erase(*frame_id);
				node_2_lur_.erase(*frame_id);
//      latch_.unlock();
				return true;
			}
		}
		if (!k_lru_q_.empty()) {
			auto it = k_lru_q_.begin();
			while (it != k_lru_q_.end() && !node_store_[*it].is_evictable_) {
				it++;
			}
			if (it != k_lru_q_.end()) {
				*frame_id = *it;
				k_lru_q_.erase(it);
				node_2_lur_.erase(*frame_id);
				node_store_.erase(*frame_id);
				curr_size_--;
//      latch_.unlock();
				return true;
			}
		}
//  latch_.unlock();
		return false;
	}

	void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
		assert(size_t(frame_id) <= replacer_size_);
		latch_.lock();
		if (node_store_.count(frame_id) == 0) {
			curr_size_++;
			auto node = LRUKNode(frame_id, k_);
			node_store_[frame_id] = node;
			fifo_q_.emplace_front(frame_id);
			node_2_lur_[frame_id] = fifo_q_.begin();
		} else {
			auto old_size = node_store_[frame_id].curSize();
			node_store_[frame_id].add();

			if (old_size == k_) {
				// 需要将lru的节点进行更新
				auto it = node_2_lur_[frame_id];
				auto r = k_lru_q_.erase(it);
				decltype(it) p = k_lru_q_.end();
				for (auto c = r; c != k_lru_q_.end(); c++) {
					auto curNode = node_store_[*c];
					auto pNode = node_store_[frame_id];
					if (curNode.history_.front() > pNode.history_.front()) {
						p = c;
						break;
					}
				}
				auto res = k_lru_q_.insert(p, frame_id);
				node_2_lur_[frame_id] = res;

			} else {
				// 从fifoqq迁移到lru
				fifo_q_.erase(node_2_lur_[frame_id]);
				k_lru_q_.push_back(frame_id);
				node_2_lur_[frame_id] = k_lru_q_.end();
				node_2_lur_[frame_id]--;
			}
		}
		latch_.unlock();
	}

	void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
		latch_.lock();
		if (node_store_.count(frame_id) == 0) {
			throw bustub::Exception("invalid frame id");
		}
		if (node_store_[frame_id].is_evictable_ && !set_evictable) {
			curr_size_--;
		}
		if (!node_store_[frame_id].is_evictable_ && set_evictable) {
			curr_size_++;
		}
		node_store_[frame_id].is_evictable_ = set_evictable;
		latch_.unlock();
	}

	void LRUKReplacer::Remove(frame_id_t frame_id) {
		if (node_store_.count(frame_id) == 0) return;
		if (!node_store_[frame_id].is_evictable_) throw bustub::Exception("invalid frame id");
		auto node = node_store_[frame_id];
		node_store_.erase(frame_id);
		auto it = node_2_lur_[frame_id];
		node_2_lur_.erase(frame_id);
		if (node.curSize() == k_) {
			k_lru_q_.erase(it);
		}
		if (node.curSize() < k_) {
			fifo_q_.erase(it);
		}
	}

	auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
