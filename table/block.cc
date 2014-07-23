//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Decodes the blocks generated by block_builder.cc.

#include "table/block.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/comparator.h"
#include "table/block_hash_index.h"
#include "table/block_prefix_index.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"
#include "db/dbformat.h"

namespace rocksdb {

uint32_t Block::NumRestarts() const {
  assert(size_ >= 2*sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

Block::Block(const BlockContents& contents)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated),
      cachable_(contents.cachable),
      compression_type_(contents.compression_type) {
  if (size_ < sizeof(uint32_t)) {
    size_ = 0;  // Error marker
  } else {
    restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
    if (restart_offset_ > size_ - sizeof(uint32_t)) {
      // The size is too small for NumRestarts() and therefore
      // restart_offset_ wrapped around.
      size_ = 0;
    }
  }
}

Block::~Block() {
  if (owned_) {
    delete[] data_;
  }
}

// Helper routine: decode the next block entry starting at "p",
// storing the number of shared key bytes, non_shared key bytes,
// and the length of the value in "*shared", "*non_shared", and
// "*value_length", respectively.  Will not derefence past "limit".
//
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared,
                                      uint32_t* non_shared,
                                      uint32_t* value_length) {
  if (limit - p < 3) return nullptr;
  *shared = reinterpret_cast<const unsigned char*>(p)[0];
  *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
  *value_length = reinterpret_cast<const unsigned char*>(p)[2];
  if ((*shared | *non_shared | *value_length) < 128) {
    // Fast path: all three values are encoded in one byte each
    p += 3;
  } else {
    if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
  }

  if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
    return nullptr;
  }
  return p;
}

class Block::Iter : public Iterator {
 private:
  const Comparator* const comparator_;
  const char* const data_;      // underlying block contents
  uint32_t const restarts_;     // Offset of restart array (list of fixed32)
  uint32_t const num_restarts_; // Number of uint32_t entries in restart array

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t restart_index_;  // Index of restart block in which current_ falls
  IterKey key_;
  Slice value_;
  Status status_;
  BlockHashIndex* hash_index_;
  BlockPrefixIndex* prefix_index_;

  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }

  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    key_.Clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
  }

 public:
  Iter(const Comparator* comparator, const char* data, uint32_t restarts,
       uint32_t num_restarts, BlockHashIndex* hash_index,
       BlockPrefixIndex* prefix_index)
      : comparator_(comparator),
        data_(data),
        restarts_(restarts),
        num_restarts_(num_restarts),
        current_(restarts_),
        restart_index_(num_restarts_),
        hash_index_(hash_index),
        prefix_index_(prefix_index) {
    assert(num_restarts_ > 0);
  }

  virtual bool Valid() const { return current_ < restarts_; }
  virtual Status status() const { return status_; }
  virtual Slice key() const {
    assert(Valid());
    return key_.GetKey();
  }
  virtual Slice value() const {
    assert(Valid());
    return value_;
  }

  virtual void Next() {
    assert(Valid());
    ParseNextKey();
  }

  virtual void Prev() {
    assert(Valid());

    // Scan backwards to a restart point before current_
    const uint32_t original = current_;
    while (GetRestartPoint(restart_index_) >= original) {
      if (restart_index_ == 0) {
        // No more entries
        current_ = restarts_;
        restart_index_ = num_restarts_;
        return;
      }
      restart_index_--;
    }

    SeekToRestartPoint(restart_index_);
    do {
      // Loop until end of current entry hits the start of original entry
    } while (ParseNextKey() && NextEntryOffset() < original);
  }

  virtual void Seek(const Slice& target) {
    uint32_t index = 0;
    bool ok = false;
    if (prefix_index_) {
      ok = PrefixSeek(target, &index);
    } else {
      ok = hash_index_ ? HashSeek(target, &index)
        : BinarySeek(target, 0, num_restarts_ - 1, &index);
    }

    if (!ok) {
      return;
    }
    SeekToRestartPoint(index);
    // Linear search (within restart block) for first key >= target

    while (true) {
      if (!ParseNextKey() || Compare(key_.GetKey(), target) >= 0) {
        return;
      }
    }
  }
  virtual void SeekToFirst() {
    SeekToRestartPoint(0);
    ParseNextKey();
  }

  virtual void SeekToLast() {
    SeekToRestartPoint(num_restarts_ - 1);
    while (ParseNextKey() && NextEntryOffset() < restarts_) {
      // Keep skipping
    }
  }

 private:
  void CorruptionError() {
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::Corruption("bad entry in block");
    key_.Clear();
    value_.clear();
  }

  bool ParseNextKey() {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + restarts_;  // Restarts come right after data
    if (p >= limit) {
      // No more entries to return.  Mark as invalid.
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return false;
    }

    // Decode next entry
    uint32_t shared, non_shared, value_length;
    p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
    if (p == nullptr || key_.Size() < shared) {
      CorruptionError();
      return false;
    } else {
      key_.TrimAppend(shared, p, non_shared);
      value_ = Slice(p + non_shared, value_length);
      while (restart_index_ + 1 < num_restarts_ &&
             GetRestartPoint(restart_index_ + 1) < current_) {
        ++restart_index_;
      }
      return true;
    }
  }

  // Binary search in restart array to find the first restart point
  // with a key >= target (TODO: this comment is inaccurate)
  bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                  uint32_t* index) {
    assert(left <= right);

    while (left < right) {
      uint32_t mid = (left + right + 1) / 2;
      uint32_t region_offset = GetRestartPoint(mid);
      uint32_t shared, non_shared, value_length;
      const char* key_ptr =
          DecodeEntry(data_ + region_offset, data_ + restarts_, &shared,
                      &non_shared, &value_length);
      if (key_ptr == nullptr || (shared != 0)) {
        CorruptionError();
        return false;
      }
      Slice mid_key(key_ptr, non_shared);
      int cmp = Compare(mid_key, target);
      if (cmp < 0) {
        // Key at "mid" is smaller than "target". Therefore all
        // blocks before "mid" are uninteresting.
        left = mid;
      } else if (cmp > 0) {
        // Key at "mid" is >= "target". Therefore all blocks at or
        // after "mid" are uninteresting.
        right = mid - 1;
      } else {
        left = right = mid;
      }
    }

    *index = left;
    return true;
  }

  // Binary search in block_ids to find the first block
  // with a key >= target
  bool BinaryBlockIndexSeek(const Slice& target, uint32_t* block_ids,
                            uint32_t left, uint32_t right,
                            uint32_t* index) {
    assert(left <= right);

    while (left <= right) {
      uint32_t mid = (left + right) / 2;
      uint32_t region_offset = GetRestartPoint(block_ids[mid]);
      uint32_t shared, non_shared, value_length;
      const char* key_ptr =
          DecodeEntry(data_ + region_offset, data_ + restarts_, &shared,
                      &non_shared, &value_length);
      if (key_ptr == nullptr || (shared != 0)) {
        CorruptionError();
        return false;
      }
      Slice mid_key(key_ptr, non_shared);
      int cmp = Compare(mid_key, target);
      if (cmp < 0) {
        // Key at "target" is larger than "mid". Therefore all
        // blocks before or at "mid" are uninteresting.
        left = mid + 1;
      } else {
        // Key at "target" is <= "mid". Therefore all blocks
        // after "mid" are uninteresting.
        // If there is only one block left, we found it.
        if (left == right) break;
        right = mid;
      }
    }

    if (left == right) {
      *index = block_ids[left];
      return true;
    } else {
      assert(left > right);
      // Mark iterator invalid
      current_ = restarts_;
      return false;
    }
  }

  bool HashSeek(const Slice& target, uint32_t* index) {
    assert(hash_index_);
    auto restart_index = hash_index_->GetRestartIndex(target);
    if (restart_index == nullptr) {
      current_ = restarts_;
      return false;
    }

    // the elements in restart_array[index : index + num_blocks]
    // are all with same prefix. We'll do binary search in that small range.
    auto left = restart_index->first_index;
    auto right = restart_index->first_index + restart_index->num_blocks - 1;
    return BinarySeek(target, left, right, index);
  }

  bool PrefixSeek(const Slice& target, uint32_t* index) {
    assert(prefix_index_);
    uint32_t* block_ids = nullptr;
    uint32_t num_blocks = prefix_index_->GetBlocks(target, &block_ids);


    if (num_blocks == 0) {
      current_ = restarts_;
      return false;
    } else  {
      return BinaryBlockIndexSeek(target, block_ids, 0, num_blocks - 1, index);
    }
  }
};

Iterator* Block::NewIterator(const Comparator* cmp) {
  if (size_ < 2*sizeof(uint32_t)) {
    return NewErrorIterator(Status::Corruption("bad block contents"));
  }
  const uint32_t num_restarts = NumRestarts();
  if (num_restarts == 0) {
    return NewEmptyIterator();
  } else {
    return new Iter(cmp, data_, restart_offset_, num_restarts,
                    hash_index_.get(), prefix_index_.get());
  }
}

void Block::SetBlockHashIndex(BlockHashIndex* hash_index) {
  hash_index_.reset(hash_index);
}

void Block::SetBlockPrefixIndex(BlockPrefixIndex* prefix_index) {
  prefix_index_.reset(prefix_index);
}

}  // namespace rocksdb
