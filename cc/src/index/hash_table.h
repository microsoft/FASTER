// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cinttypes>
#include <cstdint>

#include "../core/address.h"
#include "../core/key_hash.h"

#include "hash_bucket.h"

using namespace FASTER::core;

namespace FASTER {
namespace index {

/// The hash table itself: a sized array of HashBuckets.
template <class D, class HID>
class InternalHashTable {
 public:
  typedef D disk_t;
  typedef typename D::file_t file_t;

  typedef typename HID::key_hash_t key_hash_t;
  typedef typename HID::hash_bucket_t hash_bucket_t;

  InternalHashTable()
    : buckets_{ nullptr }
    , size_{ 0 }
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
      core::aligned_free(buckets_);
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
      buckets_ = reinterpret_cast<hash_bucket_t*>(core::aligned_alloc(alignment,
                 size_ * sizeof(hash_bucket_t)));
    }
    std::memset(reinterpret_cast<void*>(buckets_), 0, size_ * sizeof(hash_bucket_t));

    // No checkpointing is in progress
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

    // No checkpointing is in progress
    assert(pending_checkpoint_writes_ == 0);
    assert(pending_recover_reads_ == 0);
    assert(checkpoint_pending_ == false);
    assert(checkpoint_failed_ == false);
    assert(recover_pending_ == false);
    assert(recover_failed_ == false);
  }

  /// Get the bucket specified by the hash.
  inline const hash_bucket_t& bucket(key_hash_t hash) const {
    size_t index = hash.hash_table_index(size_);
    return buckets_[index];
  }
  inline hash_bucket_t& bucket(key_hash_t hash) {
    size_t index = hash.hash_table_index(size_);
    return buckets_[index];
  }

  /// Get the bucket specified by the index. (Used by checkpoint/recovery.)
  inline const hash_bucket_t& bucket(uint64_t idx) const {
    assert(idx < size_);
    return buckets_[idx];
  }
  /// (Used by GC and called by unit tests.)
  inline hash_bucket_t& bucket(uint64_t idx) {
    assert(idx < size_);
    return buckets_[idx];
  }

  inline uint64_t size() const {
    return size_;
  }

  // Checkpointing and recovery.
  Status Checkpoint(disk_t& disk, file_t&& file, uint64_t& checkpoint_size);

  template <class RC>
  Status Checkpoint(disk_t& disk, file_t&& file, uint64_t& checkpoint_size, const RC* read_cache);
  inline Status CheckpointComplete(bool wait);

  Status Recover(disk_t& disk, file_t&& file, uint64_t checkpoint_size);
  inline Status RecoverComplete(bool wait);

 private:
  // Checkpointing and recovery.
  class AsyncIoContext : public IAsyncContext {
   public:
    AsyncIoContext(InternalHashTable* table_)
      : table{ table_ }
      , memory{ nullptr } {
    }
    AsyncIoContext(InternalHashTable* table_, hash_bucket_t* memory_)
      : table{ table_ }
      , memory{ memory_ } {
    }

    /// The deep-copy constructor
    AsyncIoContext(AsyncIoContext& other)
      : table{ other.table }
      , memory{ other.memory } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    InternalHashTable* table;
    hash_bucket_t* memory; // non-null only when read-cache is enabled
  };

 private:
  hash_bucket_t* buckets_;
  uint64_t size_;

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
template <class D, class HID>
Status InternalHashTable<D, HID>::Checkpoint(disk_t& disk, file_t&& file, uint64_t& checkpoint_size) {
  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<AsyncIoContext> context{ ctxt };
    assert(!context->memory);

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
  uint32_t write_size = static_cast<uint32_t>(chunk_size * sizeof(hash_bucket_t));
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
  checkpoint_size = size_ * sizeof(hash_bucket_t);
  return Status::Ok;
}

template <class D, class HID>
template <class RC>
Status InternalHashTable<D, HID>::Checkpoint(disk_t& disk, file_t&& file, uint64_t& checkpoint_size,
                                            const RC* read_cache) {
  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<AsyncIoContext> context{ ctxt };
    if (context->memory != nullptr) {
      aligned_free(context->memory);
    }
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
  uint32_t write_size = static_cast<uint32_t>(chunk_size * sizeof(hash_bucket_t));
  assert(write_size % file_.alignment() == 0);
  assert(!checkpoint_pending_);
  assert(pending_checkpoint_writes_ == 0);
  checkpoint_pending_ = true;
  pending_checkpoint_writes_ = Constants::kNumMergeChunks;
  for(uint32_t idx = 0; idx < Constants::kNumMergeChunks; ++idx) {
    if (!read_cache) {
      AsyncIoContext context{ this };
      RETURN_NOT_OK(file_.WriteAsync(&bucket(idx * chunk_size), idx * write_size, write_size,
                                    callback, context));
    } else {
      hash_bucket_t* buckets_chunk = reinterpret_cast<hash_bucket_t*>(core::aligned_alloc(
                                        file_.alignment(), chunk_size * sizeof(hash_bucket_t)));
      memcpy(reinterpret_cast<void*>(buckets_chunk), &bucket(idx * chunk_size), write_size);
      for (uint32_t chunk = 0; chunk < chunk_size; chunk++) {
        read_cache->SkipBucket(&buckets_chunk[chunk]);
      }

      AsyncIoContext context{ this, buckets_chunk };
      RETURN_NOT_OK(file_.WriteAsync(buckets_chunk, idx * write_size, write_size,
                                    callback, context));
    }
  }
  checkpoint_size = size_ * sizeof(hash_bucket_t);
  return Status::Ok;
}

template <class D, class HID>
inline Status InternalHashTable<D, HID>::CheckpointComplete(bool wait) {
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

template <class D, class HID>
Status InternalHashTable<D, HID>::Recover(disk_t& disk, file_t&& file, uint64_t checkpoint_size) {
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
  assert(checkpoint_size % sizeof(hash_bucket_t) == 0);
  assert(checkpoint_size % Constants::kNumMergeChunks == 0);
  disk_ = &disk;
  file_ = std::move(file);

  recover_failed_ = false;
  uint32_t read_size = static_cast<uint32_t>(checkpoint_size / Constants::kNumMergeChunks);
  uint32_t chunk_size = static_cast<uint32_t>(read_size / sizeof(hash_bucket_t));
  assert(read_size % file_.alignment() == 0);

  Initialize(checkpoint_size / sizeof(hash_bucket_t), file_.alignment());
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

template <class D, class HID>
inline Status InternalHashTable<D, HID>::RecoverComplete(bool wait) {
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

}
} // namespace FASTER::core
