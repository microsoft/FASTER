// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cinttypes>
#include <cstdint>

#include "hash_bucket.h"
#include "key_hash.h"

namespace FASTER {
namespace core {

/// The hash table itself: a sized array of HashBuckets.
template <class D>
class InternalHashTable {
 public:
  typedef D disk_t;
  typedef typename D::file_t file_t;

  InternalHashTable()
    : size_{ 0 }
    , buckets_{ nullptr }
    , disk_{ nullptr }
    , pending_checkpoint_writes_{ 0 }
    , pending_recover_reads_{ 0 }
    , checkpoint_pending_{ false }
    , checkpoint_failed_{ false }
    , recover_pending_{ false }
    , recover_failed_{ false } {
  }

  ~InternalHashTable() {
    if(buckets_) {
      aligned_free(buckets_);
    }
  }

  inline void Initialize(uint64_t new_size, uint64_t alignment) {
    assert(new_size < INT32_MAX);
    assert(Utility::IsPowerOfTwo(new_size));
    assert(Utility::IsPowerOfTwo(alignment));
    assert(alignment >= Constants::kCacheLineBytes);
    if(size_ != new_size) {
      size_ = new_size;
      if(buckets_) {
        aligned_free(buckets_);
      }
      buckets_ = reinterpret_cast<HashBucket*>(aligned_alloc(alignment,
                 size_ * sizeof(HashBucket)));
    }
    std::memset(buckets_, 0, size_ * sizeof(HashBucket));
    assert(pending_checkpoint_writes_ == 0);
    assert(pending_recover_reads_ == 0);
    assert(checkpoint_pending_ == false);
    assert(checkpoint_failed_ == false);
    assert(recover_pending_ == false);
    assert(recover_failed_ == false);
  }

  inline void Uninitialize() {
    if(buckets_) {
      aligned_free(buckets_);
      buckets_ = nullptr;
    }
    size_ = 0;
    assert(pending_checkpoint_writes_ == 0);
    assert(pending_recover_reads_ == 0);
    assert(checkpoint_pending_ == false);
    assert(checkpoint_failed_ == false);
    assert(recover_pending_ == false);
    assert(recover_failed_ == false);
  }

  /// Get the bucket specified by the hash.
  inline const HashBucket& bucket(KeyHash hash) const {
    return buckets_[hash.idx(size_)];
  }
  inline HashBucket& bucket(KeyHash hash) {
    return buckets_[hash.idx(size_)];
  }

  /// Get the bucket specified by the index. (Used by checkpoint/recovery.)
  inline const HashBucket& bucket(uint64_t idx) const {
    assert(idx < size_);
    return buckets_[idx];
  }
  /// (Used by GC and called by unit tests.)
  inline HashBucket& bucket(uint64_t idx) {
    assert(idx < size_);
    return buckets_[idx];
  }

  inline uint64_t size() const {
    return size_;
  }

  // Checkpointing and recovery.
  Status Checkpoint(disk_t& disk, file_t&& file, uint64_t& checkpoint_size);
  inline Status CheckpointComplete(bool wait);

  Status Recover(disk_t& disk, file_t&& file, uint64_t checkpoint_size);
  inline Status RecoverComplete(bool wait);

  void DumpDistribution(MallocFixedPageSize<HashBucket, disk_t>& overflow_buckets_allocator);

 private:
  // Checkpointing and recovery.
  class AsyncIoContext : public IAsyncContext {
   public:
    AsyncIoContext(InternalHashTable* table_)
      : table{ table_ } {
    }
    /// The deep-copy constructor
    AsyncIoContext(AsyncIoContext& other)
      : table{ other.table } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    InternalHashTable* table;
  };

 private:
  uint64_t size_;
  HashBucket* buckets_;

  /// State for ongoing checkpoint/recovery.
  disk_t* disk_;
  file_t file_;
  std::atomic<uint64_t> pending_checkpoint_writes_;
  std::atomic<uint64_t> pending_recover_reads_;
  std::atomic<bool> checkpoint_pending_;
  std::atomic<bool> checkpoint_failed_;
  std::atomic<bool> recover_pending_;
  std::atomic<bool> recover_failed_;
};

/// Implementations.
template <class D>
Status InternalHashTable<D>::Checkpoint(disk_t& disk, file_t&& file, uint64_t& checkpoint_size) {
  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<AsyncIoContext> context{ ctxt };
    if(result != Status::Ok) {
      context->table->checkpoint_failed_ = true;
    }
    if(--context->table->pending_checkpoint_writes_ == 0) {
      result = context->table->file_.Close();
      if(result != Status::Ok) {
        context->table->checkpoint_failed_ = true;
      }
      context->table->checkpoint_pending_ = false;
    }
  };

  assert(size_ % Constants::kNumMergeChunks == 0);
  disk_ = &disk;
  file_ = std::move(file);

  checkpoint_size = 0;
  checkpoint_failed_ = false;
  uint32_t chunk_size = static_cast<uint32_t>(size_ / Constants::kNumMergeChunks);
  uint32_t write_size = static_cast<uint32_t>(chunk_size * sizeof(HashBucket));
  assert(write_size % file_.alignment() == 0);
  assert(!checkpoint_pending_);
  assert(pending_checkpoint_writes_ == 0);
  checkpoint_pending_ = true;
  pending_checkpoint_writes_ = Constants::kNumMergeChunks;
  for(uint32_t idx = 0; idx < Constants::kNumMergeChunks; ++idx) {
    AsyncIoContext context{ this };
    RETURN_NOT_OK(file_.WriteAsync(&bucket(idx * chunk_size), idx * write_size, write_size,
                                   callback, context));
  }
  checkpoint_size = size_ * sizeof(HashBucket);
  return Status::Ok;
}

template <class D>
inline Status InternalHashTable<D>::CheckpointComplete(bool wait) {
  disk_->TryComplete();
  bool complete = !checkpoint_pending_.load();
  while(wait && !complete) {
    disk_->TryComplete();
    complete = !checkpoint_pending_.load();
    std::this_thread::yield();
  }
  if(!complete) {
    return Status::Pending;
  } else {
    return checkpoint_failed_ ? Status::IOError : Status::Ok;
  }
}

template <class D>
Status InternalHashTable<D>::Recover(disk_t& disk, file_t&& file, uint64_t checkpoint_size) {
  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<AsyncIoContext> context{ ctxt };
    if(result != Status::Ok) {
      context->table->recover_failed_ = true;
    }
    if(--context->table->pending_recover_reads_ == 0) {
      result = context->table->file_.Close();
      if(result != Status::Ok) {
        context->table->recover_failed_ = true;
      }
      context->table->recover_pending_ = false;
    }
  };

  assert(checkpoint_size > 0);
  assert(checkpoint_size % sizeof(HashBucket) == 0);
  assert(checkpoint_size % Constants::kNumMergeChunks == 0);
  disk_ = &disk;
  file_ = std::move(file);

  recover_failed_ = false;
  uint32_t read_size = static_cast<uint32_t>(checkpoint_size / Constants::kNumMergeChunks);
  uint32_t chunk_size = static_cast<uint32_t>(read_size / sizeof(HashBucket));
  assert(read_size % file_.alignment() == 0);

  Initialize(checkpoint_size / sizeof(HashBucket), file_.alignment());
  assert(!recover_pending_);
  assert(pending_recover_reads_.load() == 0);
  recover_pending_ = true;
  pending_recover_reads_ = Constants::kNumMergeChunks;
  for(uint32_t idx = 0; idx < Constants::kNumMergeChunks; ++idx) {
    AsyncIoContext context{ this };
    RETURN_NOT_OK(file_.ReadAsync(idx * read_size, &bucket(idx * chunk_size), read_size,
                                  callback, context));
  }
  return Status::Ok;
}

template <class D>
inline Status InternalHashTable<D>::RecoverComplete(bool wait) {
  disk_->TryComplete();
  bool complete = !recover_pending_.load();
  while(wait && !complete) {
    disk_->TryComplete();
    complete = !recover_pending_.load();
    std::this_thread::yield();
  }
  if(!complete) {
    return Status::Pending;
  } else {
    return recover_failed_ ? Status::IOError : Status::Ok;
  }
}

template <class D>
inline void InternalHashTable<D>::DumpDistribution(
  MallocFixedPageSize<HashBucket, disk_t>& overflow_buckets_allocator) {
  uint64_t table_size = size();
  uint64_t total_record_count = 0;
  uint64_t histogram[16] = { 0 };
  for(uint64_t bucket_idx = 0; bucket_idx < table_size; ++bucket_idx) {
    const HashBucket* bucket = &buckets_[bucket_idx];
    uint64_t count = 0;
    while(bucket) {
      for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
        if(!bucket->entries[entry_idx].load().unused()) {
          ++count;
          ++total_record_count;
        }
      }
      HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
      if(overflow_entry.unused()) {
        bucket = nullptr;
      } else {
        bucket = &overflow_buckets_allocator.Get(overflow_entry.address());
      }
    }
    if(count < 15) {
      ++histogram[count];
    } else {
      ++histogram[15];
    }
  }

  printf("number of hash buckets: %" PRIu64 "\n", table_size);
  printf("total record count: %" PRIu64 "\n", total_record_count);
  printf("histogram:\n");
  for(uint8_t idx = 0; idx < 15; ++idx) {
    printf("%2u : %" PRIu64 "\n", idx, histogram[idx]);
  }
  printf("15+: %" PRIu64 "\n", histogram[15]);
}

}
} // namespace FASTER::core
