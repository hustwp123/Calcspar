//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#ifndef ROCKSDB_LITE
#include "util/hash_skiplist_rep.h"

#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "port/port.h"
#include "port/atomic_pointer.h"
#include "util/murmurhash.h"
#include "db/memtable.h"
#include "db/skiplist.h"

namespace rocksdb {
namespace {

class HashSkipListRep : public MemTableRep {
 public:
  HashSkipListRep(const MemTableRep::KeyComparator& compare, Arena* arena,
                  const SliceTransform* transform, size_t bucket_size,
                  int32_t skiplist_height, int32_t skiplist_branching_factor);

  virtual void Insert(KeyHandle handle) override;

  virtual bool Contains(const char* key) const override;

  virtual size_t ApproximateMemoryUsage() override;

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg,
                                         const char* entry)) override;

  virtual ~HashSkipListRep();

  virtual MemTableRep::Iterator* GetIterator() override;

  virtual MemTableRep::Iterator* GetIterator(const Slice& slice) override;

  virtual MemTableRep::Iterator* GetPrefixIterator(const Slice& prefix)
      override;

  virtual MemTableRep::Iterator* GetDynamicPrefixIterator() override;

 private:
  friend class DynamicIterator;
  typedef SkipList<const char*, const MemTableRep::KeyComparator&> Bucket;

  size_t bucket_size_;

  const int32_t skiplist_height_;
  const int32_t skiplist_branching_factor_;

  // Maps slices (which are transformed user keys) to buckets of keys sharing
  // the same transform.
  port::AtomicPointer* buckets_;

  // The user-supplied transform whose domain is the user keys.
  const SliceTransform* transform_;

  const MemTableRep::KeyComparator& compare_;
  // immutable after construction
  Arena* const arena_;

  inline size_t GetHash(const Slice& slice) const {
    return MurmurHash(slice.data(), slice.size(), 0) % bucket_size_;
  }
  inline Bucket* GetBucket(size_t i) const {
    return static_cast<Bucket*>(buckets_[i].Acquire_Load());
  }
  inline Bucket* GetBucket(const Slice& slice) const {
    return GetBucket(GetHash(slice));
  }
  // Get a bucket from buckets_. If the bucket hasn't been initialized yet,
  // initialize it before returning.
  Bucket* GetInitializedBucket(const Slice& transformed);

  class Iterator : public MemTableRep::Iterator {
   public:
    explicit Iterator(Bucket* list, bool own_list = true,
                      Arena* arena = nullptr)
        : list_(list), iter_(list), own_list_(own_list), arena_(arena) {}

    virtual ~Iterator() {
      // if we own the list, we should also delete it
      if (own_list_) {
        assert(list_ != nullptr);
        delete list_;
      }
    }

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const {
      return list_ != nullptr && iter_.Valid();
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const {
      assert(Valid());
      return iter_.key();
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() {
      assert(Valid());
      iter_.Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() {
      assert(Valid());
      iter_.Prev();
    }

    // Advance to the first entry with a key >= target
    virtual void Seek(const Slice& internal_key, const char* memtable_key) {
      if (list_ != nullptr) {
        const char* encoded_key =
            (memtable_key != nullptr) ?
                memtable_key : EncodeKey(&tmp_, internal_key);
        iter_.Seek(encoded_key);
      }
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() {
      if (list_ != nullptr) {
        iter_.SeekToFirst();
      }
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() {
      if (list_ != nullptr) {
        iter_.SeekToLast();
      }
    }
   protected:
    void Reset(Bucket* list) {
      if (own_list_) {
        assert(list_ != nullptr);
        delete list_;
      }
      list_ = list;
      iter_.SetList(list);
      own_list_ = false;
    }
   private:
    // if list_ is nullptr, we should NEVER call any methods on iter_
    // if list_ is nullptr, this Iterator is not Valid()
    Bucket* list_;
    Bucket::Iterator iter_;
    // here we track if we own list_. If we own it, we are also
    // responsible for it's cleaning. This is a poor man's shared_ptr
    bool own_list_;
    std::unique_ptr<Arena> arena_;
    std::string tmp_;       // For passing to EncodeKey
  };

  class DynamicIterator : public HashSkipListRep::Iterator {
   public:
    explicit DynamicIterator(const HashSkipListRep& memtable_rep)
      : HashSkipListRep::Iterator(nullptr, false),
        memtable_rep_(memtable_rep) {}

    // Advance to the first entry with a key >= target
    virtual void Seek(const Slice& k, const char* memtable_key) {
      auto transformed = memtable_rep_.transform_->Transform(ExtractUserKey(k));
      Reset(memtable_rep_.GetBucket(transformed));
      HashSkipListRep::Iterator::Seek(k, memtable_key);
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }
   private:
    // the underlying memtable
    const HashSkipListRep& memtable_rep_;
  };

  class EmptyIterator : public MemTableRep::Iterator {
    // This is used when there wasn't a bucket. It is cheaper than
    // instantiating an empty bucket over which to iterate.
   public:
    EmptyIterator() { }
    virtual bool Valid() const {
      return false;
    }
    virtual const char* key() const {
      assert(false);
      return nullptr;
    }
    virtual void Next() { }
    virtual void Prev() { }
    virtual void Seek(const Slice& internal_key,
                      const char* memtable_key) { }
    virtual void SeekToFirst() { }
    virtual void SeekToLast() { }
   private:
  };
};

HashSkipListRep::HashSkipListRep(const MemTableRep::KeyComparator& compare,
                                 Arena* arena, const SliceTransform* transform,
                                 size_t bucket_size, int32_t skiplist_height,
                                 int32_t skiplist_branching_factor)
    : MemTableRep(arena),
      bucket_size_(bucket_size),
      skiplist_height_(skiplist_height),
      skiplist_branching_factor_(skiplist_branching_factor),
      transform_(transform),
      compare_(compare),
      arena_(arena) {
  buckets_ = new port::AtomicPointer[bucket_size];

  for (size_t i = 0; i < bucket_size_; ++i) {
    buckets_[i].NoBarrier_Store(nullptr);
  }
}

HashSkipListRep::~HashSkipListRep() {
  delete[] buckets_;
}

HashSkipListRep::Bucket* HashSkipListRep::GetInitializedBucket(
    const Slice& transformed) {
  size_t hash = GetHash(transformed);
  auto bucket = GetBucket(hash);
  if (bucket == nullptr) {
    auto addr = arena_->AllocateAligned(sizeof(Bucket));
    bucket = new (addr) Bucket(compare_, arena_, skiplist_height_,
                               skiplist_branching_factor_);
    buckets_[hash].Release_Store(static_cast<void*>(bucket));
  }
  return bucket;
}

void HashSkipListRep::Insert(KeyHandle handle) {
  auto* key = static_cast<char*>(handle);
  assert(!Contains(key));
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = GetInitializedBucket(transformed);
  bucket->Insert(key);
}

bool HashSkipListRep::Contains(const char* key) const {
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = GetBucket(transformed);
  if (bucket == nullptr) {
    return false;
  }
  return bucket->Contains(key);
}

size_t HashSkipListRep::ApproximateMemoryUsage() {
  return sizeof(buckets_);
}

void HashSkipListRep::Get(const LookupKey& k, void* callback_args,
                          bool (*callback_func)(void* arg, const char* entry)) {
  auto transformed = transform_->Transform(k.user_key());
  auto bucket = GetBucket(transformed);
  if (bucket != nullptr) {
    Bucket::Iterator iter(bucket);
    for (iter.Seek(k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }
}

MemTableRep::Iterator* HashSkipListRep::GetIterator() {
  // allocate a new arena of similar size to the one currently in use
  Arena* new_arena = new Arena(arena_->BlockSize());
  auto list = new Bucket(compare_, new_arena);
  for (size_t i = 0; i < bucket_size_; ++i) {
    auto bucket = GetBucket(i);
    if (bucket != nullptr) {
      Bucket::Iterator itr(bucket);
      for (itr.SeekToFirst(); itr.Valid(); itr.Next()) {
        list->Insert(itr.key());
      }
    }
  }
  return new Iterator(list, true, new_arena);
}

MemTableRep::Iterator* HashSkipListRep::GetPrefixIterator(const Slice& prefix) {
  auto bucket = GetBucket(prefix);
  if (bucket == nullptr) {
    return new EmptyIterator();
  }
  return new Iterator(bucket, false);
}

MemTableRep::Iterator* HashSkipListRep::GetIterator(const Slice& slice) {
  return GetPrefixIterator(transform_->Transform(slice));
}

MemTableRep::Iterator* HashSkipListRep::GetDynamicPrefixIterator() {
  return new DynamicIterator(*this);
}

} // anon namespace

MemTableRep* HashSkipListRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Arena* arena,
    const SliceTransform* transform) {
  return new HashSkipListRep(compare, arena, transform, bucket_count_,
                             skiplist_height_, skiplist_branching_factor_);
}

MemTableRepFactory* NewHashSkipListRepFactory(
    size_t bucket_count, int32_t skiplist_height,
    int32_t skiplist_branching_factor) {
  return new HashSkipListRepFactory(bucket_count, skiplist_height,
      skiplist_branching_factor);
}

} // namespace rocksdb
#endif  // ROCKSDB_LITE
