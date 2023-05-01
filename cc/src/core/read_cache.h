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

// Read Cache that stores read-hot records in memory
// NOTE: Current implementation stores *at most one* record per hash index entry
template <class K, class V, class D, class H>
class ReadCache {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;

  typedef D faster_disk_t;
  typedef typename D::file_t file_t;
  typedef PersistentMemoryMalloc<faster_disk_t> faster_hlog_t;

  typedef H hash_index_t;
  typedef typename H::hash_bucket_t hash_bucket_t;
  typedef typename H::hash_bucket_entry_t hash_bucket_entry_t;

  // Read cache allocator
  typedef FASTER::device::NullDisk disk_t;
  typedef ReadCachePersistentMemoryMalloc<disk_t> hlog_t;

  constexpr static bool CopyToTail = true;

  ReadCache(LightEpoch& epoch, hash_index_t& hash_index, faster_hlog_t& faster_hlog,
            ReadCacheBlockAllocateCallback block_allocate_callback,
            ReadCacheConfig& config)
    : epoch_{ &epoch }
    , hash_index_{ &hash_index }
    , disk_{ "", epoch, "" }
    , faster_hlog_{ &faster_hlog }
    , faster_{ nullptr }
    , block_allocate_callback_{ block_allocate_callback }
    , read_cache_{ true, config.mem_size, epoch, disk_, disk_.log(), config.mutable_fraction,
                  config.pre_allocate_log, EvictCallback} {
    // hash index should be entirely in memory
    assert(hash_index_->IsSync());
    // required when evict callback is called
    read_cache_.SetReadCacheInstance(static_cast<void*>(this));
  }

  template <class C>
  Status Read(C& pending_context, Address& address);

  template <class C>
  Address Skip(C& pending_context) const;

  Address Skip(Address address);

  template <class C>
  Address Skip(C& pending_context);

  template <class C>
  Address SkipAndInvalidate(C& pending_context);

  template <class C>
  Status Insert(ExecutionContext& exec_context, C& pending_context,
                record_t* record, bool is_cold_log_record = false);

  // Eviction-related methods
  // Called from ReadCachePersistentMemoryMalloc, when head address is shifted
  static void EvictCallback(void* readcache, Address from_head_address, Address to_head_address) {
    typedef ReadCache<K, V, D, H> readcache_t;

    readcache_t* self = static_cast<readcache_t*>(readcache);
    self->Evict(from_head_address, to_head_address);
  }

  void Evict(Address from_head_address, Address to_head_address);

  void SetFasterInstance(void* faster) {
    faster_ = faster;
  }


  Status Checkpoint(CheckpointState<file_t>& checkpoint);
  void SkipBucket(hash_bucket_t* const bucket);

  void Dump(const std::string& filename);

 private:
  LightEpoch* epoch_;
  hash_index_t* hash_index_;

  void* faster_;
  ReadCacheBlockAllocateCallback block_allocate_callback_;

  disk_t disk_;
  faster_hlog_t* faster_hlog_;

 public:
  hlog_t read_cache_;
};

template <class K, class V, class D, class H>
template <class C>
inline Status ReadCache<K, V, D, H>::Read(C& pending_context, Address& address) {
  if (pending_context.skip_read_cache) {
    address = Skip(pending_context);
    return Status::NotFound;
  }

  if (address.in_readcache()) {
    Address rc_address = address.readcache_address();

    record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(rc_address));
    ReadCacheRecordInfo rc_record_info = ReadCacheRecordInfo{ record->header };

    assert(!rc_record_info.tombstone); // read cache does not store tombstones

    if (!rc_record_info.invalid && pending_context.is_key_equal(record->key())) {
      if (rc_address >= read_cache_.safe_head_address.load()) {
        pending_context.Get(record);

        if (CopyToTail && rc_address < read_cache_.read_only_address.load()) {
          ExecutionContext exec_context; // init invalid/empty context; not used in sync (hot) hash index

          Status status = Insert(exec_context, pending_context, record,
                              ReadCacheRecordInfo{ record->header }.in_cold_hlog);
          if (status == Status::Ok) {
            // Invalidate this record, since we copied it to the tail
            record->header.invalid = true;
          }
        }
        return Status::Ok;
      }
      assert(rc_address >= read_cache_.head_address.load());
    }

    address = rc_record_info.previous_address();
    assert(!address.in_readcache());
  }
  // not handled by read cache
  return Status::NotFound;
}

template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::Skip(C& pending_context) const {
  Address address = pending_context.entry.address();
  const record_t* record;

  if (address.in_readcache()) {
    assert(address.readcache_address() >= read_cache_.begin_address.load());
    record = reinterpret_cast<const record_t*>(read_cache_.Get(address.readcache_address()));
    address = ReadCacheRecordInfo{ record->header }.previous_address();
    assert(!address.in_readcache());
  }
  return address;
}

template <class K, class V, class D, class H>
inline Address ReadCache<K, V, D, H>::Skip(Address address) {
  const record_t* record;

  if (address.in_readcache()) {
    assert(address.readcache_address() >= read_cache_.begin_address.load());
    record = reinterpret_cast<const record_t*>(read_cache_.Get(address.readcache_address()));
    address = ReadCacheRecordInfo{ record->header }.previous_address();
    assert(!address.in_readcache());
  }
  return address;
}

template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::Skip(C& pending_context) {
  Address address = pending_context.entry.address();
  record_t* record;

  if (address.in_readcache()) {
    assert(address.readcache_address() >= read_cache_.begin_address.load());
    record = reinterpret_cast<record_t*>(read_cache_.Get(address.readcache_address()));
    address = ReadCacheRecordInfo{ record->header }.previous_address();
    assert(!address.in_readcache());
  }
  return address;
}


template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::SkipAndInvalidate(C& pending_context) {
  Address address = pending_context.entry.address();
  record_t* record;

  if (address.in_readcache()) {
    assert(address.readcache_address() >= read_cache_.begin_address.load());
    record = reinterpret_cast<record_t*>(read_cache_.Get(address.readcache_address()));
    if (pending_context.is_key_equal(record->key())) {
      // invalidate record if keys match
      record->header.invalid = true;
    }
    address = record->header.previous_address();
    assert(!address.in_readcache());
  }
  return address;
}

template <class K, class V, class D, class H>
template <class C>
inline Status ReadCache<K, V, D, H>::Insert(ExecutionContext& exec_context, C& pending_context,
                                            record_t* record, bool is_cold_log_record) {
  // Store previous info wrt expected hash bucket entry
  HashBucketEntry orig_expected_entry = pending_context.entry;
  AtomicHashBucketEntry* orig_atomic_entry = pending_context.atomic_entry;

  Status index_status = hash_index_->FindOrCreateEntry(exec_context, pending_context);
  assert(index_status == Status::Ok);

  bool new_index_entry = (pending_context.entry.address() == Address::kInvalidAddress);
  assert(is_cold_log_record || !new_index_entry);

  // find first non-read cache address
  Address hlog_address = Skip(pending_context);
  assert(!hlog_address.in_readcache());
  assert(is_cold_log_record || hlog_address != Address::kInvalidAddress);

  if (!is_cold_log_record || !new_index_entry) {
    // Restore expected hash bucket entry info to perform the hash bucket CAS
    pending_context.set_index_entry(orig_expected_entry, orig_atomic_entry);
  }
  if (pending_context.atomic_entry == nullptr) {
    return Status::Aborted;
  }

  // NOTE: it is possible for orig_expected_entry to be invalid (i.e., empty)
  // and the current hash index entry to be valid. This can happen when
  // a concurrent hot-cold compaction takes place, and GCs the hash index after
  // trimming the log.

  // Create new record
  Address new_address = (*block_allocate_callback_)(faster_, record->size());
  record_t* new_record = reinterpret_cast<record_t*>(read_cache_.Get(new_address));
  memcpy(new_record, record, record->size());
  // Replace header
  ReadCacheRecordInfo record_info {
    static_cast<uint16_t>(exec_context.version),
    is_cold_log_record, false, false, hlog_address.control()
  };
  new(new_record) record_t{ record_info.ToRecordInfo() };

  // Try to update hash index
  index_status = hash_index_->TryUpdateEntry(exec_context, pending_context, new_address, true);
  assert(index_status == Status::Ok || index_status == Status::Aborted);
  if (index_status == Status::Aborted) {
    new_record->header.invalid = true;
  }
  return index_status;
}

template <class K, class V, class D, class H>
inline void ReadCache<K, V, D, H>::Evict(Address from_head_address, Address to_head_address) {
  typedef ReadCacheEvictContext<K, V> rc_evict_context_t;

  static std::atomic<int> threads_evicting{ 0 };
  static std::atomic<bool> eviction_in_progress{ false };

  static std::vector<Address> record_addrs;
  static std::atomic<uint32_t> record_addrs_idx{ 0 };

  // Evict one page at a time -- first page is smaller than the remaining ones
  log_info("[tid=%u] ReadCache EVICT: [%lu] -> [%lu]",
          Thread::id(), to_head_address.control(), from_head_address.control());

  if (++threads_evicting == 1) {
    // First thread to arrive for this eviction range -- do the initialization
    record_addrs.clear();
    record_addrs_idx.store(0);

    // Traverse range & identify valid record addresses
    Address address = from_head_address;
    while (address < to_head_address) {
      record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(address));

      if (ReadCacheRecordInfo{ record->header }.IsNull()) {
        // reached end of the page -- move to next page!
        address = Address{ address.page() + 1, 0 };
        continue;
      }
      // add to valid addresses
      record_addrs.push_back(address);
      // move to next record
      if (address.offset() + record->size() <= Address::kMaxOffset) {
        address += record->size();
      } else {
        address = Address{ address.page() + 1, 0 };
      }
    }
    record_addrs.shrink_to_fit();

    // start eviction process for all threads
    assert(threads_evicting.load() == 1);
    eviction_in_progress.store(true);
  } else {
    while (!eviction_in_progress.load()) {
      std::this_thread::yield();
    }
  }
  assert(eviction_in_progress.load());

  // init invalid/empty context; not used in sync (hot) hash index
  ExecutionContext exec_context;

  Address faster_hlog_begin_address = faster_hlog_->begin_address.load();
  uint32_t idx;
  while(true) {
    // Fetch next record
    idx = record_addrs_idx++;
    if (idx >= record_addrs.size()) {
      break;
    }
    Address address = record_addrs[idx];

    record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(address));
    ReadCacheRecordInfo rc_record_info{ record->header };
    uint32_t record_size = record->size();

    assert(!rc_record_info.IsNull());
    // Assume (for now) that only a single entry per hash bucket lies in read cache
    assert(!rc_record_info.previous_address().in_readcache());

    rc_evict_context_t context{ record };
    Status index_status = hash_index_->FindEntry(exec_context, context);
    assert(index_status == Status::Ok || index_status == Status::NotFound);

    // status can be non-found if (1) entry was GCed after log trimming
    if (index_status == Status::NotFound) {
      // nothing to update -- continue to next record
      assert(context.entry == HashBucketEntry::kInvalidEntry);
      continue;
    }
    assert(context.entry != HashBucketEntry::kInvalidEntry);

    assert(!context.entry.rc_.readcache_ || context.entry.address().readcache_address() >= address);
    if (!context.entry.rc_.readcache_ || context.entry.address().readcache_address() != address) {
      continue;
    }
    assert(context.entry.address().readcache_address() == address);

    // Hash index entry points to the entry -- need to CAS pointer to actual record in FASTER log
    while (context.atomic_entry && context.entry.rc_.readcache_ &&
            (context.entry.address().readcache_address() == address)) {

      // Minor optimization when ReadCache evict runs concurent to HashIndex GC
      // If a to-be-evicted read cache entry points to a trimmed hlog address, try to elide the bucket entry
      // NOTE: there is still a chance that read-cache entry points to an invalid hlog address (race condition)
      //       this is handled by FASTER's Internal* methods
      Address updated_address = (rc_record_info.previous_address() >= faster_hlog_begin_address)
                                  ? rc_record_info.previous_address()
                                  : HashBucketEntry::kInvalidEntry;

      index_status = hash_index_->TryUpdateEntry(exec_context, context, updated_address);
      if (index_status == Status::Ok) {
        break;
      }
      assert(index_status == Status::Aborted);
      // context.entry should reflect current hash index entry -- retry!
    }
  };

  if (--threads_evicting == 0) {
    eviction_in_progress.store(false);
  } else {
    // thread barrier -- wait until all threads finish
    while (eviction_in_progress.load()) {
      std::this_thread::yield();
    }
  }

  log_info("[tid=%u] ReadCache EVICT: DONE!", Thread::id());
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
