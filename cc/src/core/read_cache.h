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

  ReadCache(LightEpoch& epoch, hash_index_t& hash_index, faster_hlog_t& faster_hlog, ReadCacheConfig& config)
    : epoch_{ epoch }
    , hash_index_{ &hash_index }
    , disk_{ "", epoch, "" }
    , faster_hlog_{ &faster_hlog }
    , read_cache_{ true, config.mem_size, epoch, disk_, disk_.log(),
                   config.mutable_fraction, config.pre_allocate, EvictCallback}
    , evicted_to_{ Constants::kCacheLineBytes }     // First page starts with kCacheLineBytes offset
    , evicted_to_req_{ Constants::kCacheLineBytes } // First page starts with kCacheLineBytes offset
    , evicting_to_{ 0 }
    , evicting_threads_{ 0 }
    , eviction_in_progress_{ false }
    , evict_record_addrs_idx_{ 0 } {

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
  Address SkipAndInvalidate(const C& pending_context);

  template <class C>
  Status TryInsert(ExecutionContext& exec_context, C& pending_context,
                   record_t* record, bool is_cold_log_record = false);

  // Eviction-related methods
  // Called from ReadCachePersistentMemoryMalloc, when head address is shifted
  static void EvictCallback(void* readcache, Address from_head_address, Address to_head_address) {
    typedef ReadCache<K, V, D, H> readcache_t;

    readcache_t* self = static_cast<readcache_t*>(readcache);
    self->Evict(from_head_address, to_head_address);
  }

  void Evict(Address from_head_address, Address to_head_address);

  // Checkpoint-related methods
  Status Checkpoint(CheckpointState<file_t>& checkpoint);
  void SkipBucket(hash_bucket_t* const bucket) const;

 private:
  LightEpoch& epoch_;
  hash_index_t* hash_index_;

  disk_t disk_;
  faster_hlog_t* faster_hlog_;

 public:
  hlog_t read_cache_;

 private:
  /// Evicted-related members

  // Largest address for which record eviction has finished
  std::atomic<uint64_t> evicted_to_{ Constants::kCacheLineBytes };

  // Largest address for which *any* thread has requested eviction
  std::atomic<uint64_t> evicted_to_req_{ Constants::kCacheLineBytes };

  // Until address, for which eviction is currently performed
  std::atomic<uint64_t> evicting_to_{ 0 };

  // Keeps track of how many threads are participating in the compaction
  std::atomic<int> evicting_threads_{ 0 };

  // True if eviction is currently ongoing
  std::atomic<bool> eviction_in_progress_{ false };

  // Vector of valid records residing in soon-to-be evicted page(s)
  std::vector<Address> evict_record_addrs_;

  // Points to next-to-be-checked for eviction record
  // NOTE: multiple threads may participate in eviction
  std::atomic<uint32_t> evict_record_addrs_idx_{ 0 };

#ifdef STATISTICS
 public:
  void EnableStatsCollection() {
    collect_stats_ = true;
  }
  void DisableStatsCollection() {
    collect_stats_ = false;
  }
  // implementation at the end of this file
  void PrintStats() const;

 private:
  bool collect_stats_{ true };
  // Read
  std::atomic<uint64_t> read_calls_{ 0 };
  std::atomic<uint64_t> read_success_count_{ 0 };
  std::atomic<uint64_t> read_copy_to_tail_calls_{ 0 };
  std::atomic<uint64_t> read_copy_to_tail_success_count_{ 0 };
  // TryInsert
  std::atomic<uint64_t> try_insert_calls_{ 0 };
  std::atomic<uint64_t> try_insert_success_count_{ 0 };
  // Evict
  std::atomic<uint64_t> evicted_records_count_{ 0 };
  std::atomic<uint64_t> evicted_records_invalid_{ 0 };
#endif
};

template <class K, class V, class D, class H>
template <class C>
inline Status ReadCache<K, V, D, H>::Read(C& pending_context, Address& address) {
  if (pending_context.skip_read_cache) {
    address = Skip(pending_context);
    return Status::NotFound;
  }

  #ifdef STATISTICS
  if (collect_stats_) {
    ++read_calls_;
  }
  #endif

  if (address.in_readcache()) {
    Address rc_address = address.readcache_address();
    record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(rc_address));
    ReadCacheRecordInfo rc_record_info{ record->header };

    assert(!rc_record_info.tombstone); // read cache does not store tombstones

    if (!rc_record_info.invalid
        && rc_address >= read_cache_.safe_head_address.load()
        && pending_context.is_key_equal(record->key())
    ) {
      pending_context.Get(record);

      #ifdef STATISTICS
      if (collect_stats_) {
        ++read_success_count_;
      }
      #endif

      if (CopyToTail && rc_address < read_cache_.read_only_address.load()) {
        #ifdef STATISTICS
        if (collect_stats_) {
          ++read_copy_to_tail_calls_;
        }
        #endif

        ExecutionContext exec_context; // dummy context; not actually used
        Status status = TryInsert(exec_context, pending_context, record,
                                  ReadCacheRecordInfo{ record->header }.in_cold_hlog);
        if (status == Status::Ok) {
          // Invalidate this record, since we copied it to the tail
          record->header.invalid = true;

          #ifdef STATISTICS
          if (collect_stats_) {
            ++read_copy_to_tail_success_count_;
          }
          #endif
        }
      }
      return Status::Ok;
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
    assert(address.readcache_address() >= read_cache_.safe_head_address.load());
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
    assert(address.readcache_address() >= read_cache_.safe_head_address.load());
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
    assert(address.readcache_address() >= read_cache_.safe_head_address.load());
    record = reinterpret_cast<record_t*>(read_cache_.Get(address.readcache_address()));
    address = ReadCacheRecordInfo{ record->header }.previous_address();
    assert(!address.in_readcache());
  }
  return address;
}

template <class K, class V, class D, class H>
template <class C>
inline Address ReadCache<K, V, D, H>::SkipAndInvalidate(const C& pending_context) {
  Address address = pending_context.entry.address();
  record_t* record;

  if (address.in_readcache()) {
    assert(address.readcache_address() >= read_cache_.safe_head_address.load());
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
inline Status ReadCache<K, V, D, H>::TryInsert(ExecutionContext& exec_context, C& pending_context,
                                               record_t* record, bool is_cold_log_record) {
  #ifdef STATISTICS
  if (collect_stats_) {
    ++try_insert_calls_;
  }
  #endif

  // Store previous info wrt expected hash bucket entry
  HashBucketEntry orig_expected_entry{ pending_context.entry };
  bool invalid_hot_index_entry = (orig_expected_entry == HashBucketEntry::kInvalidEntry);
  // Expected hot index entry cannot be invalid, unless record is cold-resident,
  // otherwise, how did we even ended-up in this record? :)
  assert(is_cold_log_record || !invalid_hot_index_entry);

  Status index_status = hash_index_->FindOrCreateEntry(exec_context, pending_context);
  assert(pending_context.atomic_entry != nullptr);
  assert(index_status == Status::Ok);

  bool created_index_entry = (pending_context.entry.address() == Address::kInvalidAddress);
  // Address should not be invalid, unless record is cold-log-resident OR some deletion op took place
  if (!is_cold_log_record && created_index_entry) {
    return Status::Aborted;
  }

  // Find first non-read cache address, to be used as previous address for this RC record
  Address hlog_address = Skip(pending_context);
  assert(!hlog_address.in_readcache());
  // Again, address should not be invalid, unless record is cold-log-resident OR some deletion op took place
  if (!is_cold_log_record && (hlog_address == Address::kInvalidAddress)) {
    return Status::Aborted;
  }

  // Typically, we use as expected_entry, the original hot index entry we retrieved at the start of FASTER/F2's Read.
  // -----
  // Yet for F2, if this record is cold-resident AND new hot index entry was created, it means that there is no record
  // with the same key in hot log. Thus, we can safely keep this newly-created entry, as our expected entry, because
  // we can guarrantee that no newer record for this key has been inserted in the hot/cold log in the meantime.
  // (i.e., if they were, they would have been found by the cold_store.Read op that called this method).
  // NOTE: There is a slim case where we cannot guarrantee that (i.e., see F2's Read op). If so, we skip inserting to RC.
  if (!is_cold_log_record || !created_index_entry) {
    pending_context.entry = orig_expected_entry;
  }
  // Proactive check on expected entry, before actually allocating space in read-cache log
  if (pending_context.atomic_entry->load() != pending_context.entry) {
    return Status::Aborted;
  }

  // Try Allocate space in the RC log (best-effort)
  uint32_t page;
  Address new_address = read_cache_.Allocate(record->size(), page);
  if (new_address < read_cache_.read_only_address.load()) {
    // No space in this page -- ask for a new one and abort
    assert(new_address == Address::kInvalidAddress);
    // NOTE: Even if we wanted to retry multiple times here, it would be tricky to guarrantee correctness!
    //       Consider the case where we copy a read-cache-resident record that resides in RC's RO region.
    //       If we Allocate() after a successful NewPage(), then an RC eviction process might have been
    //       triggered, potentially invalidating our source record; we would have to duplicate the record...
    read_cache_.NewPage(page);
    return Status::Aborted;
  }

  // Populate new record
  record_t* new_record = reinterpret_cast<record_t*>(read_cache_.Get(new_address));
  memcpy(reinterpret_cast<void*>(new_record), reinterpret_cast<void*>(record), record->size());
  // Overwrite record header
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

  #ifdef STATISTICS
  if (index_status == Status::Ok) {
    if (collect_stats_) {
      ++try_insert_success_count_;
    }
  }
  #endif

  return index_status;
}

template <class K, class V, class D, class H>
inline void ReadCache<K, V, D, H>::Evict(Address from_head_address, Address to_head_address) {
  typedef ReadCacheEvictContext<K, V> rc_evict_context_t;

  // Keep in the largest address to evict until, from all threads
  uint64_t evicted_to_req;
  do {
    evicted_to_req = evicted_to_req_.load();
    if (Address{ evicted_to_req } >= to_head_address) {
      break;
    }
  } while(!evicted_to_req_.compare_exchange_strong(evicted_to_req, to_head_address.control()));

  do {
    assert(from_head_address <= Address{ evicted_to_.load() });
    from_head_address = Address{ evicted_to_.load() };
    to_head_address = Address{ evicted_to_req_.load() };
    assert(from_head_address <= to_head_address);

    if (from_head_address == to_head_address) {
      break; // completed eviction of all requested pages
    }

    log_debug("[tid=%u] ReadCache EVICT: [%lu] -> [%lu]",
              Thread::id(), to_head_address.control(), from_head_address.control());

    if (++evicting_threads_ == 1) {
      // First thread to arrive for this eviction range -- do the initialization
      evict_record_addrs_.clear();
      evict_record_addrs_idx_.store(0);

      evicting_to_.store(to_head_address.control());

      assert(to_head_address.offset() == 0);
      for (uint32_t page_idx = from_head_address.page();
          page_idx < to_head_address.page();
          ++page_idx) {

        uint64_t address_ = Address{ page_idx, 0 }.control();
        address_ = std::min(
          std::max(from_head_address.control(), address_), to_head_address.control());
        Address address{ address_ };

        uint64_t until_address_ = Address{page_idx + 1, 0}.control();
        until_address_ = std::min(to_head_address.control(), until_address_);
        Address until_address{ until_address_ };
        //log_debug("[PAGE: %u] from: %lu [offset: %lu]\t| to: %lu [offset: %lu]", page_idx,
        //          address_, address.offset(), until_address_, until_address.offset());

        #ifdef STATISTICS
        uint64_t invalid_records_count = 0;
        #endif

        uint64_t records_in_page = 0;
        while (address < until_address) {
          record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(address));
          if (ReadCacheRecordInfo{ record->header }.IsNull()) {
            break; // no more records in this page!
          }
          // add to valid addresses
          evict_record_addrs_.push_back(address);
          ++records_in_page;

          #ifdef STATISTICS
          if (collect_stats_) {
            invalid_records_count += (record->header.invalid);
          }
          #endif

          if (address.offset() + record->size() > Address::kMaxOffset) {
            break; // no more records in this page!
          }
          address += record->size();
        }
        // reached end of the page
        log_debug("[PAGE: %lu]: Found %lu records in page", page_idx, records_in_page);

        #ifdef STATISTICS
        if (collect_stats_) {
          evicted_records_count_ += records_in_page;
          evicted_records_invalid_ += invalid_records_count;
        }
        #endif
      }

      // init multi-threaded record eviction process
      eviction_in_progress_.store(true);
    } else {
      while (!eviction_in_progress_.load()) {
        std::this_thread::yield();
      }
    }
    assert(eviction_in_progress_.load());

    // dummy context; not used in sync (hot) hash index
    ExecutionContext exec_context;

    Address faster_hlog_begin_address{ faster_hlog_->begin_address.load() };
    uint32_t idx;
    while(true) {
      // Fetch next record
      idx = evict_record_addrs_idx_++;
      if (idx >= evict_record_addrs_.size()) {
        break;
      }
      Address address{ evict_record_addrs_[idx] };

      record_t* record = reinterpret_cast<record_t*>(read_cache_.Get(address));
      ReadCacheRecordInfo rc_record_info{ record->header };

      assert(!rc_record_info.IsNull());
      // Assume (for now) that only a single entry per hash bucket lies in read cache
      assert(!rc_record_info.previous_address().in_readcache());

      rc_evict_context_t context{ record };
      Status index_status = hash_index_->FindEntry(exec_context, context);
      assert(index_status == Status::Ok || index_status == Status::NotFound);

      // Index status can be non-found if:
      //  (1) entry was GCed after log trimming,
      //  (2) during insert two threads tried to insert the same record in RC, but
      //      only the first (i.e., later) one succeded, and this was invalidated
      if (index_status == Status::NotFound) {
        // nothing to update -- continue to next record
        assert(context.entry == HashBucketEntry::kInvalidEntry);
        continue;
      }
      assert(context.entry != HashBucketEntry::kInvalidEntry);
      assert(context.atomic_entry != nullptr);

      assert(
          // HI entry does not point to RC
          !context.entry.rc_.readcache_
          // HI points to this RC entry, or some later one
          || context.entry.address().readcache_address() >= address
      );
      if (
        // HI entry does not point to RC
        !context.entry.rc_.readcache_
        // HI points to an RC entry, which is in some larger address
        || context.entry.address().readcache_address() > address
      ) {
        continue;
      }
      assert(context.entry.address().readcache_address() == address);

      // Hash index entry points to the entry -- need to CAS pointer to actual record in FASTER log
      while (context.atomic_entry
             && context.entry.rc_.readcache_
             && (context.entry.address().readcache_address() == address)) {

        // Minor optimization when RC Evict() runs concurrent to HashIndex GC
        // If a to-be-evicted read cache entry points to a trimmed hlog address, try to elide the bucket entry
        // NOTE: there is still a chance that read-cache entry points to an invalid hlog address (race condition)
        //       this is handled by FASTER's Internal* methods
        Address updated_address = (rc_record_info.previous_address() >= faster_hlog_begin_address)
                                    ? rc_record_info.previous_address()
                                    : HashBucketEntry::kInvalidEntry;
        assert(!updated_address.in_readcache());

        index_status = hash_index_->TryUpdateEntry(exec_context, context, updated_address);
        if (index_status == Status::Ok) {
          break;
        }
        assert(index_status == Status::Aborted);
        // context.entry should reflect current hash index entry -- retry!
      }

      // new entry should point to either HybridLog, or to some later RC entry
      HashBucketEntry new_entry{ context.atomic_entry->load() };
      assert(!new_entry.rc_.readcache_ || new_entry.address().readcache_address() > address);
    }

    if (--evicting_threads_ == 0) {
      evict_record_addrs_.clear();

      evicted_to_.store(evicting_to_.load());
      eviction_in_progress_.store(false);
    } else {
      // thread barrier -- wait until all threads finish
      while (eviction_in_progress_.load()) {
        std::this_thread::yield();
      }
    }

    log_debug("[tid=%u] ReadCache EVICT: DONE! [%lu] -> [%lu]",
              Thread::id(), to_head_address.control(), from_head_address.control());

  } while (true);
}

template <class K, class V, class D, class H>
inline void ReadCache<K, V, D, H>::SkipBucket(hash_bucket_t* const bucket) const {
  Address head_address = read_cache_.head_address.load();
  assert(bucket != nullptr);

  for (uint32_t idx = 0; idx < hash_bucket_t::kNumEntries; idx++) {
    do {
      auto* atomic_entry = reinterpret_cast<AtomicHashBucketEntry*>(&(bucket->entries[idx]));
      hash_bucket_entry_t entry{ atomic_entry->load() };

      if (!entry.address().in_readcache()) {
        break;
      }

      // Retrieve hlog address, and replace entry with it
      Address rc_address{ entry.address().readcache_address() };
      assert(rc_address >= head_address);
      const record_t* record = reinterpret_cast<const record_t*>(read_cache_.Get(rc_address));

      hash_bucket_entry_t new_entry{ ReadCacheRecordInfo{ record->header }.previous_address(),
                                     entry.tag(), entry.tentative() };
      assert(!new_entry.address().in_readcache());

      HashBucketEntry expected_entry{ entry };
      if(atomic_entry->compare_exchange_strong(expected_entry, new_entry)) {
        break;
      }
    } while (true);
  }
}

#ifdef STATISTICS
template <class K, class V, class D, class H>
inline void ReadCache<K, V, D, H>::PrintStats() const {
  // Read
  fprintf(stderr, "Read Calls\t: %lu\n", read_calls_.load());
  double read_success_pct = (read_calls_.load() > 0)
      ? (static_cast<double>(read_success_count_.load()) / read_calls_.load()) * 100.0
      : std::numeric_limits<double>::quiet_NaN();
  fprintf(stderr, "Status::Ok (%%): %.2lf\n", read_success_pct);

  // Read [CopyToTail]
  fprintf(stderr, "\nRead [CopyToTail] Calls: %lu\n", read_copy_to_tail_calls_.load());
  double read_copy_to_tail_success_pct = (read_copy_to_tail_calls_.load() > 0)
      ? (static_cast<double>(read_copy_to_tail_success_count_.load()) / read_copy_to_tail_calls_.load()) * 100.0
      : std::numeric_limits<double>::quiet_NaN();
  fprintf(stderr, "Status::Ok (%%): %.2lf\n", read_copy_to_tail_success_pct);

  // TryInsert
  fprintf(stderr, "\nTryInsert Calls\t: %lu\n", try_insert_calls_.load());
  double try_insert_calls_success = (try_insert_calls_.load() > 0)
      ? (static_cast<double>(try_insert_success_count_.load()) / try_insert_calls_.load()) * 100.0
      : std::numeric_limits<double>::quiet_NaN();
  fprintf(stderr, "Status::Ok (%%): %.2lf\n", try_insert_calls_success);

  // Evicted Records
  fprintf(stderr, "\nEvicted Records\t: %lu\n", evicted_records_count_.load());
  double evicted_records_invalid_pct = (evicted_records_count_ > 0)
    ? (static_cast<double>(evicted_records_invalid_.load()) / evicted_records_count_.load()) * 100.0
    : std::numeric_limits<double>::quiet_NaN();
  fprintf(stderr, "Invalid (%%): %.2lf\n", evicted_records_invalid_pct);
}
#endif

}
} // namespace FASTER::core
