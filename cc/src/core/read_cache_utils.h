// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "address.h"
#include "internal_contexts.h"
#include "persistent_memory_malloc.h"
#include "record.h"
#include "status.h"

namespace FASTER {
namespace core {

template <class K, class V>
class ReadCacheEvictContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;

  /// Constructs and returns a context given a pointer to a record.
  ReadCacheEvictContext(record_t* record)
   : record_{ record } {
  }

  /// Copy constructor deleted -- operation never goes async
  ReadCacheEvictContext(const ReadCacheEvictContext& from) = delete;

  /// Invoked from within FASTER.
  inline const key_t& key() const {
    return record_->key();
  }
  inline uint32_t key_size() const {
    return key().size();
  }
  inline KeyHash get_key_hash() const {
    return key().GetHash();
  }

  inline void set_index_entry(HashBucketEntry entry_, AtomicHashBucketEntry* atomic_entry_) {
    entry = entry_;
    atomic_entry = atomic_entry_;

    assert(atomic_entry != nullptr);
  }

 protected:
  /// Copies this context into a passed-in pointer if the operation goes
  /// asynchronous inside FASTER
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    throw std::runtime_error{ "ReadCache Evict Context should *not* go async!" };
  }

 public:
  /// Hash table entry that (indirectly) leads to the record being read or modified.
  HashBucketEntry entry;
  /// Pointer to the atomic hash bucket entry
  AtomicHashBucketEntry* atomic_entry;
  /// Not used -- kept only for compilation purposes.
  IndexOperationType index_op_type;
  Status index_op_result;

 private:
  /// Pointer to the record
  record_t* record_;
};

typedef void(*ReadCacheEvictCallback)(void* readcache, Address from_head_address, Address to_head_address);

template <class D>
class ReadCachePersistentMemoryMalloc : public PersistentMemoryMalloc<D> {
 public:
  typedef D disk_t;
  typedef typename D::log_file_t log_file_t;

  ReadCachePersistentMemoryMalloc(bool has_no_backing_storage, uint64_t log_size, LightEpoch& epoch, disk_t& disk_, log_file_t& file_,
                                double log_mutable_fraction, bool pre_allocate_log,
                                ReadCacheEvictCallback evict_callback) // read-cache specific
    : PersistentMemoryMalloc<disk_t>(has_no_backing_storage, log_size, epoch, disk_, file_,
                                    log_mutable_fraction, pre_allocate_log)
    , evict_callback_{ evict_callback } {
      // evict callback should be provided
      assert(evict_callback_ != nullptr);
  }

  ~ReadCachePersistentMemoryMalloc() { }

  bool FlushAndEvict(bool wait) {
    bool success;
    Address tail_address = ShiftHeadAddressToTail(success);
    if (!success) {
      return false;
    }

    while (wait && this->safe_head_address < tail_address) {
      // wait until flush is done
      this->epoch_->ProtectAndDrain();
    }
    return true;
  }

  void SetReadCacheInstance(void* readcache) {
    readcache_ = readcache;
  }

 private:
  inline void PageAlignedShiftHeadAddress(uint32_t tail_page) final {
    //obtain local values of variables that can change
    Address current_head_address = this->head_address.load();
    Address current_flushed_until_address = this->flushed_until_address.load();

    if(tail_page <= (this->buffer_size_ - PersistentMemoryMalloc<D>::kNumHeadPages)) {
      // Desired head address is <= 0.
      return;
    }

    Address desired_head_address{
      tail_page - (this->buffer_size_ - PersistentMemoryMalloc<D>::kNumHeadPages), 0 };

    if(current_flushed_until_address < desired_head_address) {
      desired_head_address = Address{ current_flushed_until_address.page(), 0 };
    }

    // allow the readcache to clean up
    if (desired_head_address > current_head_address) {
      evict_callback_(readcache_, current_head_address, desired_head_address);
    }

    Address old_head_address;
    if(this->MonotonicUpdate(this->head_address, desired_head_address, old_head_address)) {
      typename PersistentMemoryMalloc<D>::OnPagesClosed_Context context{ this, desired_head_address, false };
      IAsyncContext* context_copy;
      Status result = context.DeepCopy(context_copy);
      assert(result == Status::Ok);
      this->epoch_->BumpCurrentEpoch(this->OnPagesClosed, context_copy);
    }
  }

  Address ShiftHeadAddressToTail(bool& success) {
    Address tail_address = this->GetTailAddress();

    // Attempt to shift read-only address
    Address old_read_only_address;
    if (!(success = this->MonotonicUpdate(this->read_only_address, tail_address, old_read_only_address))) {
      return tail_address;
    }
    // Shift read-only address to tail
    typename PersistentMemoryMalloc<D>::OnPagesMarkedReadOnly_Context ro_context{ this, tail_address, false };
    IAsyncContext* ro_context_copy;
    Status result = ro_context.DeepCopy(ro_context_copy);
    assert(result == Status::Ok);
    this->epoch_->BumpCurrentEpoch(this->OnPagesMarkedReadOnly, ro_context_copy);

    // Attempt to shift head address
    Address old_head_address;
    if (!(success = this->MonotonicUpdate(this->head_address, tail_address, old_head_address))) {
      return tail_address;
    }
    // Shift head address to tail
    typename PersistentMemoryMalloc<D>::OnPagesClosed_Context h_context{ this, tail_address, false };
    IAsyncContext* h_context_copy;
    result = h_context.DeepCopy(h_context_copy);
    assert(result == Status::Ok);
    this->epoch_->BumpCurrentEpoch(this->OnPagesClosed, h_context_copy);

    return tail_address;
  }

 private:
  void* readcache_;
  ReadCacheEvictCallback evict_callback_;
};


}
} // namespace FASTER::core