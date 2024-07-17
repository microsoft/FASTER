#pragma once

#include "../core/async.h"
#include "../core/async_result_types.h"
#include "../core/checkpoint_state.h"
#include "../core/gc_state.h"
#include "../core/grow_state.h"
#include "../core/internal_contexts.h"
#include "../core/light_epoch.h"
#include "../core/persistent_memory_malloc.h"
#include "../core/state_transitions.h"
#include "../core/status.h"

#include "hash_bucket.h"
#include "hash_table.h"
#include "index.h"

#ifdef TOML_CONFIG
#include "toml.hpp"
#endif

using namespace FASTER::core;

namespace FASTER {
namespace index {

class HotLogHashIndexDefinition {
 public:
  typedef IndexKeyHash<HotLogIndexBucketEntryDef, 0, 0> key_hash_t;
  typedef HotLogIndexHashBucket hash_bucket_t;
  typedef HotLogIndexHashBucketEntry hash_bucket_entry_t;
};

template <uint8_t N>
class ColdLogHashIndexDefinition {
 public:
  constexpr static uint16_t kNumEntries = N;                          // Total number of hash bucket entries
  constexpr static uint16_t kNumBuckets = (N >> 3);                   // Each bucket holds 8 hash bucket entries
  constexpr static uint8_t kInChunkIndexBits = core::log2((N >> 3));  // How many bits to index all different buckets?

  static_assert(kNumEntries >= 8 && kNumEntries <= 512,
    "ColdLogHashIndexDefinition: Total number of entries should be between 8 and 512");
  static_assert(core::is_power_of_2(kNumEntries),
    "ColdLogHashIndexDefinition: Total number of entries should be a power of 2");

  typedef IndexKeyHash<ColdLogIndexBucketEntryDef, kInChunkIndexBits, 3> key_hash_t;
  typedef ColdLogIndexHashBucket hash_bucket_t;
  typedef ColdLogIndexHashBucketEntry hash_bucket_entry_t;
  typedef ColdLogIndexHashBucketEntry hash_bucket_chunk_entry_t;
};

// Forward Declarations
template <class D, class HID, bool HasOverflowBucket>
struct HashBucketOverflowEntryHelper;

template <class D, class HID>
class FasterIndex;

template<class D, class HID = HotLogHashIndexDefinition>
class HashIndex : public IHashIndex<D> {
 public:
  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  typedef HID hash_index_definition_t;
  typedef typename HID::key_hash_t key_hash_t;
  typedef typename HID::hash_bucket_t hash_bucket_t;
  typedef typename HID::hash_bucket_entry_t hash_bucket_entry_t;

  typedef GcStateInMemIndex gc_state_t;
  typedef GrowState<hlog_t> grow_state_t;

  static constexpr bool HasOverflowBucket = has_overflow_entry<typename HID::hash_bucket_t>::value;
  friend struct HashBucketOverflowEntryHelper<D, HID, HasOverflowBucket>;

  // Make friend all templated instances of this class
  template <class Di, class HIDi>
  friend class FasterIndex;

  HashIndex(const std::string& root_path, disk_t& disk, LightEpoch& epoch,
            gc_state_t& gc_state, grow_state_t& grow_state)
    : IHashIndex<D>(root_path, disk, epoch)
    , gc_state_{ &gc_state }
    , grow_state_{ &grow_state } {
  }

  struct Config {
    // Used to support legacy API
    // i.e., initializing FasterKv class with index_table_size as first arg
    Config(uint64_t table_size_)
      : table_size{ table_size_ } {
    }

    #ifdef TOML_CONFIG
    explicit Config(const toml::value& table) {
      table_size = toml::find<uint64_t>(table, "table_size");

      // Warn if unexpected fields are found
      const std::vector<std::string> VALID_INDEX_FIELDS = { "table_size" };
      for (auto& it : toml::get<toml::table>(table)) {
        if (std::find(VALID_INDEX_FIELDS.begin(), VALID_INDEX_FIELDS.end(), it.first) == VALID_INDEX_FIELDS.end()) {
          fprintf(stderr, "WARNING: Ignoring invalid *index* field '%s'\n", it.first.c_str());
        }
      }
    }
    #endif

    uint64_t table_size;  // Size of the hash index table
  };

  void Initialize(const Config& config) {
    if(!Utility::IsPowerOfTwo(config.table_size)) {
      throw std::invalid_argument{ "Index size is not a power of 2" };
    }
    if(config.table_size > INT32_MAX) {
      throw std::invalid_argument{ "Cannot allocate such a large hash table" };
    }
    this->resize_info.version = 0;

    table_[0].Initialize(config.table_size, this->disk_.log().alignment());
    overflow_buckets_allocator_[0].Initialize(this->disk_.log().alignment(), this->epoch_);
  }
  void SetRefreshCallback(void* faster, RefreshCallback callback) {
    throw std::runtime_error{ "This should never be called" };
  }

  // Find the hash bucket entry, if any, corresponding to the specified hash.
  // The caller can use the "expected_entry" to CAS its desired address into the entry.
  template <class C>
  Status FindEntry(ExecutionContext& exec_context, C& pending_context) const;

  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // create a new entry. The caller can use the "expected_entry" to CAS its desired address into
  // the entry.
  template <class C>
  Status FindOrCreateEntry(ExecutionContext& exec_context, C& pending_context);

  template <class C>
  Status TryUpdateEntry(ExecutionContext& context, C& pending_context,
                        Address new_address, bool readcache = false);

  template <class C>
  Status UpdateEntry(ExecutionContext& context, C& pending_context, Address new_address);

  // Garbage Collect methods
  template <class TC, class CC>
  void GarbageCollectSetup(Address new_begin_address,
                          TC truncate_callback,
                          CC complete_callback,
                          IAsyncContext* callback_context = nullptr);

  template <class RC>
  bool GarbageCollect(RC* read_cache);

  // Index grow related methods
  void GrowSetup(GrowCompleteCallback callback);

  template<class R>
  void Grow();

  // Hash index ops do not go pending
  void StartSession() { };
  void StopSession() { };
  bool CompletePending() {
    return true;
  };
  void Refresh() { };

  // Checkpointing methods
  template <class RC>
  Status Checkpoint(CheckpointState<file_t>& checkpoint, const RC* read_cache);

  Status CheckpointComplete();
  Status WriteCheckpointMetadata(CheckpointState<file_t>& checkpoint);

  // Recovery methods
  Status Recover(CheckpointState<file_t>& checkpoint);
  Status RecoverComplete();

  void DumpDistribution() const;

  uint64_t size() const {
    return table_[this->resize_info.version].size();
  }
  uint64_t new_size() const {
    return table_[1 - this->resize_info.version].size();
  }
  constexpr static bool IsSync() {
    return true;
  }

 private:
  bool GetNextBucket(hash_bucket_t*& current, uint8_t version) {
    return HashBucketOverflowEntryHelper<D, HID, HasOverflowBucket>::GetNextBucket(
      overflow_buckets_allocator_[version], current);
  }
  bool GetNextBucket(const hash_bucket_t*& current, uint8_t version) const {
    return HashBucketOverflowEntryHelper<D, HID, HasOverflowBucket>::GetNextBucket(
      overflow_buckets_allocator_[version], current);
  }
  bool TryAllocateNextBucket(hash_bucket_t*& current, uint8_t version) {
    return HashBucketOverflowEntryHelper<D, HID, HasOverflowBucket>::TryAllocateNextBucket(
      overflow_buckets_allocator_[version], current);
  }
  bool AllocateNextBucket(hash_bucket_t*& current, uint8_t version) {
    return HashBucketOverflowEntryHelper<D, HID, HasOverflowBucket>::AllocateNextBucket(
      overflow_buckets_allocator_[version], current);
  }

  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // return an unused bucket entry.
  AtomicHashBucketEntry* FindTentativeEntry(key_hash_t hash, hash_bucket_t* bucket, uint8_t version,
                                            HashBucketEntry& expected_entry);

  bool HasConflictingEntry(key_hash_t hash, const hash_bucket_t* bucket, uint8_t version,
                            const AtomicHashBucketEntry* atomic_entry) const;

  // Helper functions needed for Grow
  void AddHashEntry(hash_bucket_t*& bucket, uint32_t& next_idx,
                    uint8_t version, hash_bucket_entry_t entry);

  template <class R>
  Address TraceBackForOtherChainStart(uint64_t old_size, uint64_t new_size, Address from_address,
                                      Address min_address, uint8_t side);

  // Remove tentative entries from the index
  void ClearTentativeEntries();

 private:
  gc_state_t* gc_state_;
  grow_state_t* grow_state_;

  // An array of size two, that contains the old and new versions of the hash-table
  InternalHashTable<disk_t, hash_index_definition_t> table_[2];
  // Allocator for the hash buckets that don't fit in the hash table.
  MallocFixedPageSize<hash_bucket_t, disk_t> overflow_buckets_allocator_[2];

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
  // FindEntry
  mutable std::atomic<uint64_t> find_entry_calls_{ 0 };
  mutable std::atomic<uint64_t> find_entry_success_count_{ 0 };
  // FindOrCreateEntry
  std::atomic<uint64_t> find_or_create_entry_calls_{ 0 };
  // TryUpdateEntry
  std::atomic<uint64_t> try_update_entry_calls_{ 0 };
  std::atomic<uint64_t> try_update_entry_success_count_{ 0 };
  // UpdateEntry
  std::atomic<uint64_t> update_entry_calls_{ 0 };
#endif
};

template <class D, class HID>
template <class C>
inline Status HashIndex<D, HID>::FindEntry(ExecutionContext& exec_context,
                                          C& pending_context) const {
  //static_assert(std::is_base_of<PendingContext<typename C::key_t>, C>::value,
  //              "PendingClass is not the base class of pending_context argument");

#ifdef STATISTICS
  if (collect_stats_) {
    ++find_entry_calls_;
  }
#endif

  key_hash_t hash{ pending_context.get_key_hash() };
  uint32_t version = this->resize_info.version;
  assert(version == 0 || version == 1);

  // Truncate the hash to get a bucket page_index < table[version].size.
  const hash_bucket_t* bucket = &table_[version].bucket(hash);
  assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

  while(true) {
    // Search through the bucket looking for our key.
    for(uint32_t entry_idx = 0; entry_idx < hash_bucket_t::kNumEntries; ++entry_idx) {
      hash_bucket_entry_t entry{ bucket->entries[entry_idx].load() };
      if(entry.unused()) {
        continue;
      }
      if(hash.tag() == entry.tag()) {
        // Found a matching tag.
        if(!entry.tentative()) {
          // If final key, return immediately
          pending_context.set_index_entry(entry,
              const_cast<AtomicHashBucketEntry*>(&bucket->entries[entry_idx]));

          #ifdef STATISTICS
          if (collect_stats_) {
            ++find_entry_success_count_;
          }
          #endif

          return Status::Ok;
        }
      }
    }
    // Go to next bucket in the chain (if any)
    if (!GetNextBucket(bucket, version)) {
      // No more buckets in the chain.
      pending_context.set_index_entry(HashBucketEntry::kInvalidEntry, nullptr);
      return Status::NotFound;
    }
  }
  assert(false);
  return Status::Corruption; // NOT REACHED
}

template <class D, class HID>
template<class C>
inline Status HashIndex<D, HID>::FindOrCreateEntry(ExecutionContext& exec_context,
                                                  C& pending_context) {
  //static_assert(std::is_base_of<PendingContext<typename C::key_t>, C>::value,
  //              "PendingClass is not the base class of pending_context argument");

#ifdef STATISTICS
  if (collect_stats_) {
    ++find_or_create_entry_calls_;
  }
#endif

  HashBucketEntry expected_entry;
  AtomicHashBucketEntry* atomic_entry;

  key_hash_t hash{ pending_context.get_key_hash() };
  const uint32_t version = this->resize_info.version;
  assert(version == 0 || version == 1);

  while(true) {
    hash_bucket_t* bucket = &table_[version].bucket(hash);
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

    atomic_entry = FindTentativeEntry(hash, bucket, version, expected_entry);
    if(expected_entry != HashBucketEntry::kInvalidEntry) {
      // Found an existing hash bucket entry; nothing further to check.
      pending_context.set_index_entry(expected_entry, atomic_entry);
      return Status::Ok;
    }
    // We have a free slot.
    assert(atomic_entry != nullptr);
    assert(expected_entry == HashBucketEntry::kInvalidEntry);

    // Try to install tentative tag in free slot.
    hash_bucket_entry_t desired{ Address::kInvalidAddress, hash.tag(), true };
    if(atomic_entry->compare_exchange_strong(expected_entry, desired)) {
      // See if some other thread is also trying to install this tag.
      if(HasConflictingEntry(hash, bucket, version, atomic_entry)) {
        // Back off and try again.
        atomic_entry->store(HashBucketEntry::kInvalidEntry);
      } else {
        // No other thread was trying to install this tag,
        // so we can clear our entry's "tentative" bit.
        expected_entry = hash_bucket_entry_t{ Address::kInvalidAddress, hash.tag(), false };
        atomic_entry->store(expected_entry);

        pending_context.set_index_entry(expected_entry, atomic_entry);
        return Status::Ok;
      }
    }
  }
  assert(false);
  return Status::Corruption; // NOT REACHED
}

template <class D, class HID>
template <class C>
inline Status HashIndex<D, HID>::TryUpdateEntry(ExecutionContext& context, C& pending_context,
                                                Address new_address, bool readcache) {
  assert(pending_context.atomic_entry != nullptr);
  // Try to atomically update the hash index entry based on expected entry
  key_hash_t hash{ pending_context.get_key_hash() };
  hash_bucket_entry_t new_entry{ new_address, hash.tag(), false, readcache };
  if (new_address == HashBucketEntry::kInvalidEntry) {
    // This handles (1) InternalDelete's & (2) ReadCache's eviction minor optimization that elides a record
    new_entry = HashBucketEntry::kInvalidEntry;
  }

  hash_bucket_entry_t before{ pending_context.entry };
  bool success = pending_context.atomic_entry->compare_exchange_strong(pending_context.entry, new_entry);
  hash_bucket_entry_t after{ pending_context.entry };

#ifdef STATISTICS
  if (collect_stats_) {
    ++try_update_entry_calls_;
    if (success) {
      ++try_update_entry_success_count_;
    }
  }
#endif

  /*
  if (success) {
  log_debug("{%p} [%llu %lu %d] -> [%llu %lu %d] SUCCESS",
    pending_context.atomic_entry,
    before.address().control(), before.tag(), before.tentative(),
    new_entry.address().control(), new_entry.tag(), new_entry.tentative());
    assert(before == after);
  } else {
  log_debug("{%p} [%llu %lu %d] -> [%llu %lu %d] FAILED! {%llu %lu %d}",
    pending_context.atomic_entry,
    before.address().control(), before.tag(), before.tentative(),
    new_entry.address().control(), new_entry.tag(), new_entry.tentative(),
    after.address().control(), after.tag(), after.tentative());
    assert(before != after);
  }
  */
  return success ? Status::Ok : Status::Aborted;
}

template <class D, class HID>
template <class C>
inline Status HashIndex<D, HID>::UpdateEntry(ExecutionContext& context, C& pending_context,
                                            Address new_address) {
  assert(pending_context.atomic_entry != nullptr);
  // Atomically update hash bucket entry
  key_hash_t hash{ pending_context.get_key_hash() };
  hash_bucket_entry_t new_entry{ new_address, hash.tag(), false };
  pending_context.atomic_entry->store(new_entry);

#ifdef STATISTICS
  if (collect_stats_) {
    ++update_entry_calls_;
  }
#endif

  return Status::Ok;
}

template <class D, class HID, bool HasOverflowEntry>
struct HashBucketOverflowEntryHelper {
  typedef D disk_t;

  typedef HID hash_index_definition_t;
  typedef HashIndex<disk_t, hash_index_definition_t> hash_index_t;
  typedef typename HID::hash_bucket_t hash_bucket_t;

  typedef MallocFixedPageSize<hash_bucket_t, disk_t> overflow_buckets_allocator_t;

  static bool GetNextBucket(overflow_buckets_allocator_t& overflow_buckets_allocator_,
                          hash_bucket_t*& current) {
    HashBucketOverflowEntry overflow_entry = current->overflow_entry.load();
    if(overflow_entry.unused()) {
      // No more buckets in the chain.
      return false;
    }
    current = &overflow_buckets_allocator_.Get(overflow_entry.address());
    assert(reinterpret_cast<size_t>(current) % Constants::kCacheLineBytes == 0);
    return true;
  }

  static bool GetNextBucket(const overflow_buckets_allocator_t& overflow_buckets_allocator_,
                            const hash_bucket_t*& current) {
    HashBucketOverflowEntry overflow_entry = current->overflow_entry.load();
    if(overflow_entry.unused()) {
      // No more buckets in the chain.
      return false;
    }
    current = &overflow_buckets_allocator_.Get(overflow_entry.address());
    assert(reinterpret_cast<size_t>(current) % Constants::kCacheLineBytes == 0);
    return true;
  }

  static bool TryAllocateNextBucket(overflow_buckets_allocator_t& overflow_buckets_allocator_,
                                  hash_bucket_t*& current) {
    // Allocate new bucket
    FixedPageAddress new_bucket_addr = overflow_buckets_allocator_.Allocate();
    HashBucketOverflowEntry new_bucket_entry{ new_bucket_addr };

    // Try updating overflow entry to point to new bucket
    // We expect current overflow_entry to be unused -- if some other thread managed to allocate first CAS will fail
    HashBucketOverflowEntry overflow_entry{ HashBucketEntry::kInvalidEntry };
    bool success = current->overflow_entry.compare_exchange_strong(overflow_entry, new_bucket_entry);

    /* do {
      success = current->overflow_entry.compare_exchange_strong(overflow_entry, new_bucket_entry);
    } while(!success && overflow_entry.unused()); */

    if(!success) {
      // Install failed, undo allocation
      overflow_buckets_allocator_.FreeAtEpoch(new_bucket_addr, 0);
      // Use the winner's entry
      current = &overflow_buckets_allocator_.Get(overflow_entry.address());
    } else {
      // Install succeeded; we have a new bucket on the chain. Return its first slot.
      current = &overflow_buckets_allocator_.Get(new_bucket_addr);
    }
    return success;
  }

  static bool AllocateNextBucket(overflow_buckets_allocator_t& overflow_buckets_allocator_,
                                hash_bucket_t*& current) {
    // Allocate new bucket
    FixedPageAddress new_bucket_addr = overflow_buckets_allocator_.Allocate();
    HashBucketOverflowEntry new_bucket_entry{ new_bucket_addr };
    // Update entry
    current->overflow_entry.store(new_bucket_entry);
    current = &overflow_buckets_allocator_.Get(new_bucket_addr);
    return true;
  }
};

template <class D, class HID>
struct HashBucketOverflowEntryHelper<D, HID, false> {
  typedef D disk_t;

  typedef HID hash_index_definition_t;
  typedef HashIndex<disk_t, hash_index_definition_t> hash_index_t;
  typedef typename HID::hash_bucket_t hash_bucket_t;

  typedef MallocFixedPageSize<hash_bucket_t, disk_t> overflow_buckets_allocator_t;

  static bool GetNextBucket(overflow_buckets_allocator_t& overflow_buckets_allocator_,
                          hash_bucket_t*& current) {
    return false; // No overflow buckets
  }

  static bool GetNextBucket(const overflow_buckets_allocator_t& overflow_buckets_allocator_,
                            const hash_bucket_t*& current) {
    return false;
  }

  static bool TryAllocateNextBucket(overflow_buckets_allocator_t& overflow_buckets_allocator_,
                                  hash_bucket_t*& current) {
    return false;
  }

  static bool AllocateNextBucket(overflow_buckets_allocator_t& overflow_buckets_allocator_,
                                hash_bucket_t*& current) {
    return false;
  }
};

template <class D, class HID>
inline AtomicHashBucketEntry* HashIndex<D, HID>::FindTentativeEntry(key_hash_t hash, hash_bucket_t* bucket,
                                                        uint8_t version, HashBucketEntry& expected_entry) {

  expected_entry = HashBucketEntry::kInvalidEntry;
  AtomicHashBucketEntry* atomic_entry = nullptr;
  // Try to find a slot that contains the right tag or that's free.
  while(true) {
    // Search through the bucket looking for our key. Last entry is reserved
    // for the overflow pointer.
    for(uint32_t entry_idx = 0; entry_idx < hash_bucket_t::kNumEntries; ++entry_idx) {
      hash_bucket_entry_t entry{ bucket->entries[entry_idx].load() };
      if(entry.unused()) {
        if(!atomic_entry) {
          // Found a free slot; keep track of it, and continue looking for a match.
          atomic_entry = &bucket->entries[entry_idx];
        }
        continue;
      }
      if(hash.tag() == entry.tag() && !entry.tentative()) {
        // Found a match. (So, the input hash matches the entry on 14 tag bits +
        // log_2(table size) address bits.) Return it to caller.
        expected_entry = entry;
        return &bucket->entries[entry_idx];
      }
    }

    // Go to next bucket in the chain
    if (!GetNextBucket(bucket, version)) {
      // No more buckets in the chain.
      if(atomic_entry) {
        // We found a free slot earlier (possibly inside an earlier bucket).
        assert(expected_entry == HashBucketEntry::kInvalidEntry);
        return atomic_entry;
      }
      if (!HasOverflowBucket) {
        // possible if two threads contest for the same entry
        // (i.e., tentative flag is set for the entry that matches the tag)
        continue;
      }

      // We didn't find any free slots, so allocate new bucket.
      if (TryAllocateNextBucket(bucket, version)) {
        assert(expected_entry == HashBucketEntry::kInvalidEntry);
        return &bucket->entries[0];
      }
      // Someone else managed to allocate a new bucket first
      assert(bucket != nullptr);
    }
  }
  assert(false);
  return nullptr; // NOT REACHED
}

template <class D, class HID>
inline bool HashIndex<D, HID>::HasConflictingEntry(key_hash_t hash, const hash_bucket_t* bucket,
                                                  uint8_t version,
                                                  const AtomicHashBucketEntry* atomic_entry) const {
  HashBucketEntry entry_ = atomic_entry->load();
  uint16_t tag = hash_bucket_entry_t(entry_).tag();
  while(true) {
    for(uint32_t entry_idx = 0; entry_idx < hash_bucket_t::kNumEntries; ++entry_idx) {
      HashBucketEntry entry_ = bucket->entries[entry_idx].load();
      hash_bucket_entry_t entry{ entry_ };
      if(!entry.unused() && entry.tag() == tag &&
          atomic_entry != &bucket->entries[entry_idx]) {
        // Found a conflict.
        return true;
      }
    }
    // Go to next bucket in the chain
    if (!GetNextBucket(bucket, version)) {
      // Reached the end of the bucket chain; no conflicts found.
      return false;
    }
  }
}

template <class D, class HID>
template <class TC, class CC>
inline void HashIndex<D, HID>::GarbageCollectSetup(Address new_begin_address, TC truncate_callback,
                                                CC complete_callback, IAsyncContext* callback_context) {
  uint64_t num_chunks = std::max(size() / gc_state_t::kHashTableChunkSize, ((uint64_t)1));
  gc_state_->Initialize(new_begin_address, truncate_callback, complete_callback, callback_context, num_chunks);
}

template <class D, class HID>
template <class RC>
inline bool HashIndex<D, HID>::GarbageCollect(RC* read_cache) {
  uint64_t chunk = gc_state_->next_chunk++;
  if(chunk >= gc_state_->num_chunks) {
    // No chunk left to clean.
    return false;
  }
  log_debug("MemIndex-GC: %lu/%lu [START...]", chunk + 1, gc_state_->num_chunks);
  log_debug("MemIndex-GC: begin-address: %lu", gc_state_->new_begin_address.control());

  uint8_t version = this->resize_info.version;
  uint64_t upper_bound;
  if(chunk + 1 < gc_state_->num_chunks) {
    // All chunks but the last chunk contain gc.kHashTableChunkSize elements.
    upper_bound = gc_state_t::kHashTableChunkSize;
  } else {
    // Last chunk might contain more or fewer elements.
    upper_bound = table_[version].size() - (chunk * gc_state_t::kHashTableChunkSize);
  }

  for(uint64_t idx = 0; idx < upper_bound; ++idx) {
    hash_bucket_t* bucket = &table_[version].bucket(chunk * gc_state_t::kHashTableChunkSize + idx);
    while(true) {
      for(uint32_t entry_idx = 0; entry_idx < hash_bucket_t::kNumEntries; ++entry_idx) {
        AtomicHashBucketEntry& atomic_entry = bucket->entries[entry_idx];
        hash_bucket_entry_t expected_entry{ atomic_entry.load() };

        Address address = expected_entry.address();
        if (read_cache != nullptr) {
          address = read_cache->Skip(address);
        }

        if(!expected_entry.unused() &&
            address != Address::kInvalidAddress &&
            address < gc_state_->new_begin_address
          ) {
          // The record that this entry points to was truncated; try to delete the entry.
          HashBucketEntry entry{ expected_entry };
          bool success = atomic_entry.compare_exchange_strong(entry, HashBucketEntry::kInvalidEntry);
          // If deletion failed, then some other thread must have added a new record to the entry.
        }
      }
      // Go to next bucket in the chain.
      if (!GetNextBucket(bucket, version)) {
        // No more buckets in the chain.
        break;
      }
    }
  }
  log_debug("MemIndex-GC: %lu/%lu [DONE!]", chunk + 1, gc_state_->num_chunks);

  // Done with this chunk--did some work.
  return true;
}

template <class D, class HID>
inline void HashIndex<D, HID>::GrowSetup(GrowCompleteCallback callback) {
  // Initialize index grow state
  uint8_t current_version = this->resize_info.version;
  assert(current_version == 0 || current_version == 1);
  uint8_t next_version = 1 - current_version;
  uint64_t num_chunks = std::max(size() / grow_state_t::kHashTableChunkSize, (uint64_t)1);
  grow_state_->Initialize(callback, current_version, num_chunks);

  // Initialize the next version of our hash table to be twice the size of the current version.
  table_[next_version].Initialize(size() * 2, this->disk_.log().alignment());
  overflow_buckets_allocator_[next_version].Initialize(this->disk_.log().alignment(), this->epoch_);
}

template <class D, class HID>
template <class R>
inline void HashIndex<D, HID>::Grow() {
  typedef R record_t;
  // FIXME: Not yet supported for ColdIndex -- AddHashEntry is the main reason..
  assert(HasOverflowBucket);

  // This thread won't exit until all hash table buckets have been split.
  Address head_address = grow_state_->hlog->head_address.load();
  Address begin_address = grow_state_->hlog->begin_address.load();
  uint8_t old_version = grow_state_->old_version;
  uint8_t new_version = grow_state_->new_version;

  for(uint64_t chunk = grow_state_->next_chunk++;
        chunk < grow_state_->num_chunks;
        chunk = grow_state_->next_chunk++) {

    uint64_t old_size = table_[old_version].size();
    uint64_t new_size = table_[new_version].size();
    // Currently only size-doubling is supported
    assert(new_size == old_size * 2);
    // Split this chunk.
    uint64_t upper_bound;
    if(chunk + 1 < grow_state_->num_chunks) {
      // All chunks but the last chunk contain kGrowHashTableChunkSize elements.
      upper_bound = grow_state_t::kHashTableChunkSize;
    } else {
      // Last chunk might contain more or fewer elements.
      upper_bound = old_size - (chunk * grow_state_t::kHashTableChunkSize);
    }

    for(uint64_t idx = 0; idx < upper_bound; ++idx) {
      // Split this (chain of) bucket(s).
      hash_bucket_t* old_bucket = &table_[old_version].bucket(
                                 chunk * grow_state_t::kHashTableChunkSize + idx);
      hash_bucket_t* new_bucket0 = &table_[new_version].bucket(
                                  chunk * grow_state_t::kHashTableChunkSize + idx);
      hash_bucket_t* new_bucket1 = &table_[new_version].bucket(
                                  old_size + chunk * grow_state_t::kHashTableChunkSize + idx);
      uint32_t new_entry_idx0 = 0;
      uint32_t new_entry_idx1 = 0;
      while(true) {
        for(uint32_t old_entry_idx = 0; old_entry_idx < hash_bucket_t::kNumEntries; ++old_entry_idx) {
          hash_bucket_entry_t old_entry{ old_bucket->entries[old_entry_idx].load() };
          if(old_entry.unused()) {
            // Nothing to do.
            continue;
          } else if(old_entry.address() < head_address) {
            // Can't tell which new bucket the entry should go into; put it in both.
            AddHashEntry(new_bucket0, new_entry_idx0, new_version, old_entry);
            AddHashEntry(new_bucket1, new_entry_idx1, new_version, old_entry);
            continue;
          }
          const record_t* record = reinterpret_cast<const record_t*>(
                                      grow_state_->hlog->Get(old_entry.address()));

          key_hash_t hash{ record->key().GetHash() };
          if(hash.hash_table_index(new_size) < old_size) {
            // Record's key hashes to the 0 side of the new hash table.
            AddHashEntry(new_bucket0, new_entry_idx0, new_version, old_entry);
            Address other_address = TraceBackForOtherChainStart<record_t>(old_size, new_size,
                                                    record->header.previous_address(), head_address, 0);
            if(other_address >= begin_address) {
              // We found a record that either is on disk or has a key that hashes to the 1 side of
              // the new hash table.
              AddHashEntry(new_bucket1, new_entry_idx1, new_version,
                           hash_bucket_entry_t{ other_address, old_entry.tag(), false });
            }
          } else {
            // Record's key hashes to the 1 side of the new hash table.
            AddHashEntry(new_bucket1, new_entry_idx1, new_version, old_entry);
            Address other_address = TraceBackForOtherChainStart<record_t>(old_size, new_size,
                                                    record->header.previous_address(), head_address, 1);
            if(other_address >= begin_address) {
              // We found a record that either is on disk or has a key that hashes to the 0 side of
              // the new hash table.
              AddHashEntry(new_bucket0, new_entry_idx0, new_version,
                           hash_bucket_entry_t{ other_address, old_entry.tag(), false });
            }
          }
        }
        // Go to next bucket in the chain.
        if (!GetNextBucket(old_bucket, old_version)) {
          // No more buckets in the chain.
          break;
        }
      }
    }
    // Done with this chunk.
    if(--grow_state_->num_pending_chunks == 0) {
      // Free the old hash table.
      table_[old_version].Uninitialize();
      overflow_buckets_allocator_[old_version].Uninitialize();
      break;
    }
  }
}

template <class D, class HID>
inline void HashIndex<D, HID>::AddHashEntry(hash_bucket_t*& bucket, uint32_t& next_idx,
                                          uint8_t version, hash_bucket_entry_t entry) {
  if(next_idx == hash_bucket_t::kNumEntries) {
    assert(HasOverflowBucket);
    // Need to allocate a new bucket, first.
    bool success = AllocateNextBucket(bucket, version);
    assert(success);
    next_idx = 0;
  }
  bucket->entries[next_idx].store(entry);
  ++next_idx;
}

template <class D, class HID>
template <class R>
inline Address HashIndex<D, HID>::TraceBackForOtherChainStart(uint64_t old_size, uint64_t new_size,
                                                            Address from_address, Address min_address,
                                                            uint8_t side) {
  typedef R record_t;

  assert(side == 0 || side == 1);
  // Search back as far as min_address.
  while(from_address >= min_address) {
    const record_t* record = reinterpret_cast<const record_t*>(grow_state_->hlog->Get(from_address));
    key_hash_t hash{ record->key().GetHash() };
    if((hash.hash_table_index(new_size) < old_size) != (side == 0)) {
      // Record's key hashes to the other side.
      return from_address;
    }
    from_address = record->header.previous_address();
  }
  return from_address;
}

template <class D, class HID>
template <class RC>
inline Status HashIndex<D, HID>::Checkpoint(CheckpointState<file_t>& checkpoint, const RC* read_cache) {
  // Checkpoint the hash table
  auto path = this->disk_.relative_index_checkpoint_path(checkpoint.index_token);
  file_t ht_file = this->disk_.NewFile(path + "ht.dat");
  RETURN_NOT_OK(ht_file.Open(&this->disk_.handler()));
  RETURN_NOT_OK(table_[this->resize_info.version].Checkpoint(this->disk_, std::move(ht_file),
                                              checkpoint.index_metadata.num_ht_bytes, read_cache));
  // Checkpoint overflow buckets
  file_t ofb_file = this->disk_.NewFile(path + "ofb.dat");
  RETURN_NOT_OK(ofb_file.Open(&this->disk_.handler()));
  RETURN_NOT_OK(overflow_buckets_allocator_[this->resize_info.version].Checkpoint(this->disk_,
                std::move(ofb_file), checkpoint.index_metadata.num_ofb_bytes, read_cache));

  checkpoint.index_checkpoint_started = true;
  return Status::Ok;
}

template <class D, class HID>
inline Status HashIndex<D, HID>::CheckpointComplete() {
  Status result = table_[this->resize_info.version].CheckpointComplete(false);
  if(result == Status::Pending) {
    return Status::Pending;
  } else if(result != Status::Ok) {
    return result;
  } else {
    return overflow_buckets_allocator_[this->resize_info.version].CheckpointComplete(false);
  }
}

template <class D, class HID>
inline Status HashIndex<D, HID>::WriteCheckpointMetadata(CheckpointState<file_t>& checkpoint) {
  // Get an overestimate for the ofb's tail, after we've finished fuzzy-checkpointing the ofb.
  // (Ensures that recovery won't accidentally reallocate from the ofb.)
  checkpoint.index_metadata.ofb_count =
    overflow_buckets_allocator_[this->resize_info.version].count();

  // Write checkpoint object to disk
  return IHashIndex<D>::WriteCheckpointMetadata(checkpoint);
}

template <class D, class HID>
inline Status HashIndex<D, HID>::Recover(CheckpointState<file_t>& checkpoint) {
  uint8_t version = this->resize_info.version;
  assert(table_[version].size() == checkpoint.index_metadata.table_size);
  auto path = this->disk_.relative_index_checkpoint_path(checkpoint.index_token);

  // Recover the main hash table.
  file_t ht_file = this->disk_.NewFile(path + "ht.dat");
  RETURN_NOT_OK(ht_file.Open(&this->disk_.handler()));
  RETURN_NOT_OK(table_[version].Recover(this->disk_, std::move(ht_file),
                                              checkpoint.index_metadata.num_ht_bytes));
  // Recover the hash table's overflow buckets.
  file_t ofb_file = this->disk_.NewFile(path + "ofb.dat");
  RETURN_NOT_OK(ofb_file.Open(&this->disk_.handler()));
  RETURN_NOT_OK(overflow_buckets_allocator_[version].Recover(this->disk_, std::move(ofb_file),
         checkpoint.index_metadata.num_ofb_bytes, checkpoint.index_metadata.ofb_count));

  return Status::Ok;
}

template <class D, class HID>
inline Status HashIndex<D, HID>::RecoverComplete() {
  Status result = table_[this->resize_info.version].RecoverComplete(true);
  if(result != Status::Ok) {
    return result;
  }
  result = overflow_buckets_allocator_[this->resize_info.version].RecoverComplete(true);
  if(result != Status::Ok) {
    return result;
  }

  ClearTentativeEntries();
  return Status::Ok;
}

template <class D, class HID>
inline void HashIndex<D, HID>::ClearTentativeEntries() {
  // Clear all tentative entries.
  uint8_t version = this->resize_info.version;
  for(uint64_t bucket_idx = 0; bucket_idx < table_[version].size(); ++bucket_idx) {
    hash_bucket_t* bucket = &table_[version].bucket(bucket_idx);
    while(true) {
      for(uint32_t entry_idx = 0; entry_idx < hash_bucket_t::kNumEntries; ++entry_idx) {
        hash_bucket_entry_t entry = bucket->entries[entry_idx].load();
        if(entry.tentative()) {
          bucket->entries[entry_idx].store(HashBucketEntry::kInvalidEntry);
        }
      }
      // Go to next bucket in the chain
      if (!GetNextBucket(bucket, version)) {
         // No more buckets in the chain.
        break;
      }
    }
  }
}

template <class D, class HID>
inline void HashIndex<D, HID>::DumpDistribution() const {
  const uint16_t kNumBucketsHistBuckets = 8;
  const uint16_t kNumEntriesHistBuckets = 16;

  // stat variables
  uint64_t total_entries = 0;
  uint64_t total_used_entries = 0;
  // Histogram of (even partially full) buckets per
  uint64_t num_buckets_hist[kNumBucketsHistBuckets] = { 0 };
  // Histogram of bucket entries per bucket
  uint64_t num_entries_hist[kNumEntriesHistBuckets] = { 0 };
  uint64_t total_bytes = 0;

  uint8_t version = this->resize_info.version;
  for(uint64_t bucket_idx = 0; bucket_idx < table_[version].size(); ++bucket_idx) {
    const hash_bucket_t* bucket = &table_[version].bucket(bucket_idx);

    uint16_t buckets_count = 0;
    uint16_t entries_count = 0;

    while(true) {
      ++buckets_count;
      for(uint32_t entry_idx = 0; entry_idx < hash_bucket_t::kNumEntries; ++entry_idx) {
        hash_bucket_entry_t entry{ bucket->entries[entry_idx].load() };
        if(!entry.unused()) {
          ++entries_count;
          ++total_used_entries;
        }
        ++total_entries;
      }

      total_bytes += sizeof(hash_bucket_t);
      if (!GetNextBucket(bucket, version)) {
        // No more buckets in the chain.
        break;
      }
    }

    buckets_count = std::min<uint16_t>(buckets_count, kNumBucketsHistBuckets - 1U);
    ++num_buckets_hist[buckets_count];

    entries_count = std::min<uint16_t>(entries_count, kNumEntriesHistBuckets - 1U);
    ++num_entries_hist[entries_count];
  }

  uint64_t total_buckets_in_hist = 0;
  for (uint8_t idx = 0; idx < kNumBucketsHistBuckets; ++idx) {
    total_buckets_in_hist += num_buckets_hist[idx];
  }
  uint64_t total_entries_in_hist = 0;
  for (uint8_t idx = 0; idx < kNumEntriesHistBuckets; ++idx) {
    total_entries_in_hist += num_entries_hist[idx];
  }

  fprintf(stderr, "\n==== DUMP DISTRIBUTION ====\n");
  fprintf(stderr, "Number of hash buckets : %" PRIu64 "\n", table_[version].size());
  fprintf(stderr, "Total used entries     : %" PRIu64 "\n", total_used_entries);
  fprintf(stderr, "Percent of used entries: %.2lf%%\n", static_cast<double>(total_used_entries) / total_entries * 100.0);
  fprintf(stderr, "Total in-mem GiB       : %.2lf GB\n",
        static_cast<double>(total_bytes) / (1024UL * 1024UL * 1024UL));
  // Buckets Histogram
  fprintf(stderr, "# Buckets Histogram:\n");
  for(uint8_t idx = 0; idx < kNumBucketsHistBuckets - 1; ++idx) {
    fprintf(stderr, "%2u : [%8lu] {%6.2lf%%}\n", idx, num_buckets_hist[idx],
      (total_buckets_in_hist > 0) ? static_cast<double>(num_buckets_hist[idx]) / total_buckets_in_hist * 100.0 : 0.0);
  }
  fprintf(stderr, "%2u+: [%8lu] {%6.2lf%%}\n", kNumBucketsHistBuckets - 1, num_buckets_hist[kNumBucketsHistBuckets - 1],
      (total_buckets_in_hist > 0) ? static_cast<double>(num_buckets_hist[kNumBucketsHistBuckets - 1]) / total_buckets_in_hist * 100.0 : 0.0);
  // Bucket Entries Histogram
  fprintf(stderr, "\n# Bucket Entries Histogram:\n");
  for(uint8_t idx = 0; idx < kNumEntriesHistBuckets - 1; ++idx) {
    fprintf(stderr, "%2u : [%8lu] {%6.2lf%%}\n", idx, num_entries_hist[idx],
      (total_buckets_in_hist > 0) ? static_cast<double>(num_entries_hist[idx]) / total_buckets_in_hist * 100.0 : 0.0);
  }
  fprintf(stderr, "%2u+: [%8lu] {%6.2lf%%}\n", kNumEntriesHistBuckets - 1, num_entries_hist[kNumEntriesHistBuckets - 1],
      (total_buckets_in_hist > 0) ? static_cast<double>(num_entries_hist[kNumEntriesHistBuckets - 1]) / total_buckets_in_hist * 100.0 : 0.0);
}

#ifdef STATISTICS
template <class D, class HID>
inline void HashIndex<D, HID>::PrintStats() const {
  // FindEntry
  fprintf(stderr, "FindEntry Calls\t: %lu\n", find_entry_calls_.load());
  if (find_entry_calls_.load() > 0) {
    fprintf(stderr, "Status::Ok (%%)\t: %.2lf\n",
      (static_cast<double>(find_entry_success_count_.load()) / find_entry_calls_.load()) * 100.0);
  }
  // FindOrCreateEntry
  fprintf(stderr, "FindOrCreateEntry Calls\t: %lu\n", find_or_create_entry_calls_.load());
  // TryUpdateEntry
  fprintf(stderr, "TryUpdateEntry Calls\t: %lu\n", try_update_entry_calls_.load());
  if (try_update_entry_calls_.load() > 0) {
    fprintf(stderr, "Status::Ok (%%)\t: %.2lf\n",
      (static_cast<double>(try_update_entry_success_count_.load()) / try_update_entry_calls_.load()) * 100.0);
  }
  // UpdateEntry
  fprintf(stderr, "UpdateEntry Calls\t: %lu\n\n", update_entry_calls_.load());

  DumpDistribution();
}
#endif

}
} // namespace FASTER::index
