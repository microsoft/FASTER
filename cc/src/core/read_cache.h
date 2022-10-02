// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "address.h"
#include "internal_contexts.h"
#include "light_epoch.h"
#include "persistent_memory_malloc.h"
#include "read_cache_utils.h"
#include "record.h"
#include "status.h"

#include "device/null_disk.h"

namespace FASTER {
namespace core {

template <class K, class V, class D, class H>
class ReadCache {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;

  typedef D faster_disk_t;
  typedef typename D::file_t file_t;

  typedef H hash_index_t;
  typedef typename H::hash_bucket_t hash_bucket_t;
  typedef typename H::hash_bucket_entry_t hash_bucket_entry_t;

  // Read cache allocator
  typedef FASTER::device::NullDisk disk_t;
  typedef ReadCachePersistentMemoryMalloc<disk_t> hlog_t;

  ReadCache(LightEpoch& epoch, hash_index_t& hash_index,
            const std::string& filename, uint64_t log_size,
            double log_mutable_fraction, bool pre_allocate_log)
    : epoch_{ &epoch }
    , hash_index_{ &hash_index }
    , disk_{ filename, epoch, "" }
    , read_cache_{ true, log_size, epoch, disk_, disk_.log(), log_mutable_fraction, pre_allocate_log, EvictCallback} {
    // hash index should be entirely in memory
    assert(hash_index_->IsSync());
    // required when evict callback is called
    read_cache_.SetReadCacheInstance(static_cast<void*>(this));
  }

  template <class C>
  Status Read(C& pending_context, Address& address) const;

  template <class C>
  Address Skip(C& pending_context) const;

  template <class C>
  Address Skip(C& pending_context);

  template <class C>
  Address SkipAndInvalidate(C& pending_context);

  template <class C>
  Status Insert(ExecutionContext& exec_context, C& pending_context, record_t* record, Address insert_address);

  void Evict(Address from_head_address, Address to_head_address);

  Status Checkpoint(CheckpointState<file_t>& checkpoint);

  // Called from ReadCachePersistentMemoryMalloc, when head address is shifted
  static void EvictCallback(void* readcache, Address from_head_address, Address to_head_address) {
    typedef ReadCache<K, V, D, H> readcache_t;

    readcache_t* self = static_cast<readcache_t*>(readcache);
    self->Evict(from_head_address, to_head_address);
  }

  void SkipBucket(hash_bucket_t* const bucket);

 private:
  template <class C>
  bool TraceBackForKeyMatch(C& pending_context, Address& address) const;

  Address BlockAllocate(uint32_t record_size);
  const record_t* GetRecordPointer(Address address) const;

 private:
  LightEpoch* epoch_;
  hash_index_t* hash_index_;

  disk_t disk_;

 public:
  hlog_t read_cache_;
};

template <class K, class V, class D, class H>
template <class C>
inline Status ReadCache<K, V, D, H>::Read(C& pending_context, Address& address) const {
  if (pending_context.skip_read_cache) {
    address = Skip(pending_context);
    return Status::NotFound;
  }

  Address rc_address;
  const record_t* record;

  while (address.in_readcache()) {
    rc_address = address.readcache_address();

    record = reinterpret_cast<const record_t*>(read_cache_.Get(rc_address));
    assert(!record->header.tombstone);

    if (!record->header.invalid && pending_context.is_key_equal(record->key())) {
      if (rc_address >= read_cache_.safe_head_address.load()) {
        pending_context.Get(record);
        return Status::Ok;
      }
      assert(rc_address >= read_cache_.head_address.load());
    }

    address = record->header.previous_address();
  }
  // not handled by read cache
  return Status::NotFound;
}

template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::Skip(C& pending_context) const {
  Address address = pending_context.entry.address();
  const record_t* record;

  while (address.in_readcache()) {
    record = reinterpret_cast<const record_t*>(read_cache_.Get(address.readcache_address()));
    address = record->header.previous_address();
  }
  return address;
}

template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::Skip(C& pending_context) {
  Address address = pending_context.entry.address();
  record_t* record;

  while (address.in_readcache()) {
    record = reinterpret_cast<record_t*>(read_cache_.Get(address.readcache_address()));
    address = record->header.previous_address();
  }
  return address;
}


template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::SkipAndInvalidate(C& pending_context) {
  Address address = pending_context.entry.address();
  record_t* record;

  while (address.in_readcache()) {
    record = reinterpret_cast<record_t*>(read_cache_.Get(address.readcache_address()));
    if (pending_context.is_key_equal(record->key())) {
      // invalidate record if keys match
      record->header.invalid = true;
    }
    address = record->header.previous_address();
  }
  return address;
}

template <class K, class V, class D, class H>
template <class C>
inline Status ReadCache<K, V, D, H>::Insert(ExecutionContext& exec_context, C& pending_context, record_t* record, Address insert_address) {
  // Store previous info wrt expected hash bucket entry
  HashBucketEntry expected_entry = pending_context.entry;
  AtomicHashBucketEntry* atomic_entry = pending_context.atomic_entry;

  Status index_status = hash_index_->FindEntry(exec_context, pending_context);
  assert(index_status == Status::Ok); // record should exist
  // TODO: change this for hot-cold -- can also be Status::NotFound

  // find first non-read cache address
  Address hlog_address = Skip(pending_context);
  assert(!hlog_address.in_readcache());
  assert(hlog_address != HashBucketEntry::kInvalidEntry);

  // Restore expected hash bucket entry info to perform the hash bucket CAS
  pending_context.set_index_entry(expected_entry, atomic_entry);

  // Create new record
  record_t* new_record = reinterpret_cast<record_t*>(read_cache_.Get(insert_address));
  memcpy(new_record, record, record->size());
  new_record->header.previous_address_ = hlog_address.control();

  // Try to update hash index
  index_status = hash_index_->TryUpdateEntry(exec_context, pending_context, insert_address, true);
  assert(index_status == Status::Ok || index_status == Status::Aborted);
  if (index_status == Status::Aborted) {
    new_record->header.invalid = true;
  }
  return index_status;
}

template <class K, class V, class D, class H>
inline void ReadCache<K, V, D, H>::Evict(Address from_head_address, Address to_head_address) {
  // NOTE: Currently eviction process is single threaded -- might want to make it multithreaded in the future
  typedef ReadCacheEvictContext<K, V> rc_evict_context_t;
  static std::atomic<bool> eviction_in_progress{ false };

  // Evict one page at a time -- first page is smaller than the remaining ones
  // log_error("EVICT %llu %llu", to_head_address.control(), from_head_address.control());

  bool active = false;
  if (!eviction_in_progress.compare_exchange_strong(active, true)) {
    // wait until other thread finishes eviction
    do {
      std::this_thread::yield();
      active = eviction_in_progress.load();
    } while(active);
    return;
  }
  assert(eviction_in_progress.load());

  // not used in sync (hot) hash index; init invalid/empty context
  ExecutionContext exec_context;
  Address address = from_head_address;

  while (address < to_head_address) {
    record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(address));
    uint32_t record_size = record->size();

    if (record->header.IsNull()) {
      // reached end of the page -- move to next page!
      address = Address{ address.page() + 1, 0 };
      continue;
    }
    if (record->header.invalid) {
      // ignore invalid records
      address += record_size;
      continue;
    }
    // Assume for now that only a single entry per hash bucket lies in read cache
    assert(!record->header.previous_address().in_readcache());

    rc_evict_context_t context{ record };
    Status index_status = hash_index_->FindEntry(exec_context, context);
    // TODO: this can be Status::NotFound for cold log-resident records
    assert(index_status == Status::Ok);
    assert(context.entry != HashBucketEntry::kInvalidEntry);

    assert(!context.entry.rc_.readcache_ || context.entry.address().readcache_address() >= address);
    if (!context.entry.rc_.readcache_ || context.entry.address().readcache_address() != address) {
      address += record_size;
      continue;
    }
    // Hash index entry points to the entry -- need to CAS pointer to actual record in FASTER log

    while (!record->header.invalid && context.atomic_entry && context.entry.rc_.readcache_ &&
              (context.entry.address().readcache_address() == address) ) {
      index_status = hash_index_->TryUpdateEntry(exec_context, context, record->header.previous_address());
      if (index_status == Status::Ok) {
        break;
      }
      assert(index_status == Status::Aborted);
      // context.entry should reflect current hash index entry -- retry!
    }

    assert(address.offset() + record_size <= Address::kMaxOffset);
    address += record_size;
  }
  // log_error("EVICT DONE %llu %llu", to_head_address.control(), from_head_address.control());

  eviction_in_progress.store(false);
}

template <class K, class V, class D, class H>
inline void ReadCache<K, V, D, H>::SkipBucket(hash_bucket_t* const bucket) {
  assert(bucket != nullptr);
  for (uint32_t idx = 0; idx < hash_bucket_t::kNumEntries; idx++) {
    auto* entry = static_cast<hash_bucket_entry_t*>(&(bucket->entries[idx]));
    while (entry->in_readcache()) {
      record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(entry->address_));
      entry->address_ = record->header.previous_address_;
    }
  }
}

}
} // namespace FASTER::core
