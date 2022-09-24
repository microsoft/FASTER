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

  typedef D disk_t;
  typedef typename D::file_t file_t;

  typedef H hash_index_t;
  typedef typename H::hash_bucket_t hash_bucket_t;
  typedef typename H::hash_bucket_entry_t hash_bucket_entry_t;

  // Read cache allocator
  typedef FASTER::device::NullDisk mem_device_t;
  typedef ReadCachePersistentMemoryMalloc<mem_device_t> alloc_t;

  // FASTER hlog allocator
  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  ReadCache(LightEpoch& epoch, hlog_t& hlog, hash_index_t& hash_index,
            const std::string& filename, uint64_t log_size,
            double log_mutable_fraction, bool pre_allocate_log)
    : epoch_{ &epoch }
    , hlog_{ &hlog }
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
  hlog_t* hlog_;
  hash_index_t* hash_index_;

  mem_device_t disk_;

 public:
  alloc_t read_cache_;
};

template <class K, class V, class D, class H>
template <class C>
inline Status ReadCache<K, V, D, H>::Read(C& pending_context, Address& address) const {
  if (pending_context.skip_read_cache) {
    address = Skip(pending_context);
    return Status::NotFound;
  }

  if (TraceBackForKeyMatch(pending_context, address)) {
    Address head_address = read_cache_.head_address.load();

    if (head_address <= address) {
      const record_t* record = reinterpret_cast<const record_t*>(read_cache_.Get(address));
      assert(!record->header.tombstone); // read cache does not store tombstones
      if (record != nullptr && !record->header.invalid &&
          pending_context.is_key_equal(record->key())) {
        pending_context.Get(record);
        return Status::Ok;
      }
    }
  }
  // not handled by read cache
  return Status::NotFound;
}

template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::Skip(C& pending_context) const {
  Address address = pending_context.entry.address();
  const record_t* record = reinterpret_cast<const record_t*>(read_cache_.Get(address));

  while (record->header.rc_.readcache) {
    address = record->header.previous_address();
    record = reinterpret_cast<const record_t*>(read_cache_.Get(address));
  }
  return address;
}

template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::Skip(C& pending_context) {
  Address address = pending_context.entry.address();
  record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(address));

  while (record->header.rc_.readcache) {
    address = record->header.previous_address();
    record = reinterpret_cast<record_t*>(read_cache_.Get(address));
  }
  return address;
}


template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::SkipAndInvalidate(C& pending_context) {
  Address address = pending_context.entry.address();
  record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(address));

  while (record->header.rc_.readcache) {
    if (pending_context.is_key_equal(record->key())) {
      // invalidate record if keys match
      record->header.invalid = true;
    }
    address = record->header.previous_address();
    record = reinterpret_cast<record_t*>(read_cache_.Get(address));
  }
  return address;
}

template <class K, class V, class D, class H>
template <class C>
inline Status ReadCache<K, V, D, H>::Insert(ExecutionContext& exec_context, C& pending_context, record_t* record, Address insert_address) {
  Status index_status = hash_index_->FindEntry(exec_context, pending_context);
  assert(index_status == Status::Ok); // record should exist
  // TODO: change this for hot-cold -- can also be Status::NotFound

  // find first non-read cache address
  Address hlog_address = Skip(pending_context);

  // Create new record
  //uint32_t record_size = record->size();
  //Address new_address = BlockAllocate(record_size);
  record_t* new_record = reinterpret_cast<record_t*>(read_cache_.Get(insert_address));
  memcpy(new_record, record, record->size());
  new_record->header.previous_address_ = hlog_address.control(); // TODO: CHECK

  // Try to update hash index
  index_status = hash_index_->TryUpdateEntry(exec_context, pending_context, insert_address, true);
  assert(index_status == Status::Ok || index_status == Status::Aborted);
  if (index_status == Status::Aborted) {
    new_record->header.invalid = true;
  }
  return index_status;
}

template <class K, class V, class D, class H>
template <class C>
inline bool ReadCache<K, V, D, H>::TraceBackForKeyMatch(C& pending_context, Address& address) const {
  if (!address.in_readcache()) {
    return false;
  }

  do {
    const record_t* record = GetRecordPointer(address);
    if (record == nullptr) {
      return false;
    }
    if (record->header.invalid) {
      continue;
    }

    if (pending_context.is_key_equal(record->key())) {
      // TODO: compare addresses only
      if (address.control() >= read_cache_.safe_read_only_address.control()) {
        return true;
      }
    }

    address = record->header.previous_address();
  } while(address.in_readcache());

  return false;
}

template <class K, class V, class D, class H>
inline void ReadCache<K, V, D, H>::Evict(Address from_head_address, Address to_head_address) {
  typedef ReadCacheEvictContext<K, V> rc_evict_context_t;

  // not used in sync (hot) hash index; init invalid/empty context
  ExecutionContext exec_context;

  Address address = from_head_address;
  while (address < to_head_address) {
    record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(address));
    assert(!record->header.IsNull());

    uint32_t record_size = record->size();
    if (!record->header.invalid) {
      address += record_size;
      continue;
    }

    Address prev_address = record->header.previous_address();
    if (prev_address.in_readcache()) {
      // already evicted previous entry that pointed to hlog
      address += record_size;
      continue;
    }

    rc_evict_context_t context{ record };
    Status index_status = hash_index_->FindEntry(exec_context, context);
    assert(index_status == Status::Ok);

    while (context.atomic_entry && context.entry.rc_.readcache_) {
      index_status = hash_index_->TryUpdateEntry(exec_context, context, prev_address);
      if (index_status == Status::Ok) {
        break;
      }
      assert(index_status == Status::Aborted);
      // context.entry should reflect changes -- retry!
    }

    address += record_size;
  }
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

//template <class K, class V, class D, class H>
//inline Status ReadCache<K, V, D, H>::Checkpoint(CheckpointState<file_t>& checkpoint) {
//  // Checkpoint the read cache
//  auto path = disk_.relative_index_checkpoint_path(checkpoint.index_token);
//  file_t ht_file = disk_.NewFile(path + "rc.dat");
//  RETURN_NOT_OK(ht_file.Open(&disk_.handler()));
//  ///TODO
//}

/*template <class K, class V, class D, class H>
inline Address ReadCache<K, V, D, H>::BlockAllocate(uint32_t record_size) {
  uint32_t page;
  Address retval = read_cache_.Allocate(record_size, page);
  while (retval < read_cache_.read_only_address.load()) {
    //Refresh();
    epoch_->ProtectAndDrain();
    bool page_closed = (retval == Address::kInvalidAddress);
    while (page_closed) {
      page_closed = !read_cache_.NewPage(page);
      epoch_->ProtectAndDrain();
      //Refresh();
    }
    retval = read_cache_.Allocate(record_size, page);
  }
  return retval;
}*/

template <class K, class V, class D, class H>
inline const Record<K, V>* ReadCache<K, V, D, H>::GetRecordPointer(Address address) const {
  if (address.in_readcache()) {
    return (address >= read_cache_.head_address.load())
      ? reinterpret_cast<const record_t*>(read_cache_.Get(address))
      : nullptr;
  } else {
    return (address >= hlog_->head_address.load())
      ? reinterpret_cast<const record_t*>(hlog_->Get(address))
      : nullptr;
  }
}

}
} // namespace FASTER::core
