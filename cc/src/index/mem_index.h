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

using namespace FASTER::core;

namespace FASTER {
namespace index {

class HotLogHashIndexDefinition {
 public:
  typedef HotLogIndexKeyHash key_hash_t;
  typedef HotLogIndexHashBucket hash_bucket_t;
  typedef HotLogIndexHashBucketEntry hash_bucket_entry_t;
};

class ColdLogHashIndexDefinition {
 public:
  typedef ColdLogIndexKeyHash key_hash_t;
  typedef ColdLogIndexHashBucket hash_bucket_t;
  typedef ColdLogIndexHashBucketEntry hash_bucket_entry_t;
};

// Forward Declaration
template <class D, class HID, bool HasOverflowBucket>
struct HashBucketOverflowEntryHelper;

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

  typedef GcStateWithIndex gc_state_t;
  typedef GrowState<hlog_t> grow_state_t;

  static constexpr bool HasOverflowBucket = has_overflow_entry<typeof(hash_bucket_t)>::value;
  friend struct HashBucketOverflowEntryHelper<D, HID, HasOverflowBucket>;

  HashIndex(disk_t& disk, LightEpoch& epoch, gc_state_t& gc_state, grow_state_t& grow_state)
    : disk_{ disk }
    , epoch_{ epoch }
    , gc_state_{ &gc_state }
    , grow_state_{ &grow_state } {
  }

  void Initialize(uint64_t new_size) {
    this->resize_info.version = 0;
    table_[0].Initialize(new_size, disk_.log().alignment());
    overflow_buckets_allocator_[0].Initialize(disk_.log().alignment(), epoch_);
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
  Status TryUpdateEntry(ExecutionContext& context, C& pending_context, Address new_address);

  template <class C>
  Status UpdateEntry(ExecutionContext& context, C& pending_context, Address new_address);

  // Garbage Collect methods
  void GarbageCollectSetup(Address new_begin_address,
                          GcState::truncate_callback_t truncate_callback,
                          GcState::complete_callback_t complete_callback);
  bool GarbageCollect();

  // Index grow related methods
  void GrowSetup(GrowCompleteCallback callback);

  template<class R>
  void Grow();

  // Hash index ops do not go pending
  void CompletePendingRequests() { };

  // Checkpointing methods
  Status Checkpoint(CheckpointState<file_t>& checkpoint);
  Status CheckpointComplete();
  Status WriteCheckpointMetadata(CheckpointState<file_t>& checkpoint);

  // Recovery methods
  Status Recover(CheckpointState<file_t>& checkpoint);
  Status RecoverComplete();
  Status ReadCheckpointMetadata(const Guid& token, CheckpointState<file_t>& checkpoint);

  void DumpDistribution();

  uint64_t size() const {
    return table_[this->resize_info.version].size();
  }
  uint64_t new_size() const {
    return table_[1 - this->resize_info.version].size();
  }

 private:
  // Checkpointing and recovery context
  class CheckpointMetadataIoContext : public IAsyncContext {
   public:
    CheckpointMetadataIoContext(Status* result_, std::atomic<bool>* completed_)
      : result{ result_ }
      , completed{ completed_ } {
    }
    /// The deep-copy constructor
    CheckpointMetadataIoContext(CheckpointMetadataIoContext& other)
      : result{ other.result }
      , completed{ other.completed } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    Status* result;
    std::atomic<bool>* completed;
  };

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

  // Remove tantative entries from the index
  void ClearTentativeEntries();

 private:
  disk_t& disk_;
  LightEpoch& epoch_;
  gc_state_t* gc_state_;
  grow_state_t* grow_state_;

  // An array of size two, that contains the old and new versions of the hash-table
  InternalHashTable<disk_t, hash_index_definition_t> table_[2];
  // Allocator for the hash buckets that don't fit in the hash table.
  MallocFixedPageSize<hash_bucket_t, disk_t> overflow_buckets_allocator_[2];
};

template <class D, class HID>
template <class C>
inline Status HashIndex<D, HID>::FindEntry(ExecutionContext& exec_context,
                                          C& pending_context) const {
  //static_assert(std::is_base_of<PendingContext<typename C::key_t>, C>::value,
  //              "PendingClass is not the base class of pending_context argument");

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
                                                Address new_address) {
  assert(pending_context.atomic_entry != nullptr);
  // Try to atomically update the hash index entry based on expected entry
  key_hash_t hash{ pending_context.get_key_hash() };
  hash_bucket_entry_t new_entry{ new_address, hash.tag(), false };
  bool success = pending_context.atomic_entry->compare_exchange_strong(pending_context.entry, new_entry);
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
    HashBucketOverflowEntry overflow_entry = current->overflow_entry.load();
    bool success;
    do {
      success = current->overflow_entry.compare_exchange_strong(overflow_entry, new_bucket_entry);
    } while(!success && overflow_entry.unused());

    if(!success) {
      // Install failed, undo allocation
      overflow_buckets_allocator_.FreeAtEpoch(new_bucket_addr, 0);
      // Use the winner's entry
      current = &overflow_buckets_allocator_.Get(
                      current->overflow_entry.load().address());
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

      // We didn't find any free slots, so allocate new bucket.
      assert(HasOverflowBucket);
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
inline void HashIndex<D, HID>::GarbageCollectSetup(Address new_begin_address,
                                                GcState::truncate_callback_t truncate_callback,
                                                GcState::complete_callback_t complete_callback) {
  uint64_t num_chunks = std::max(size() / gc_state_t::kHashTableChunkSize, (uint64_t)1);
  gc_state_->Initialize(new_begin_address, truncate_callback, complete_callback, num_chunks);
}

template <class D, class HID>
inline bool HashIndex<D, HID>::GarbageCollect() {
  uint64_t chunk = gc_state_->next_chunk++;
  if(chunk >= gc_state_->num_chunks) {
    // No chunk left to clean.
    return false;
  }
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
        if(!expected_entry.unused() && expected_entry.address() != Address::kInvalidAddress &&
            expected_entry.address() < gc_state_->new_begin_address) {
          // The record that this entry points to was truncated; try to delete the entry.
          HashBucketEntry entry{ expected_entry };
          atomic_entry.compare_exchange_strong(entry, HashBucketEntry::kInvalidEntry);
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
  table_[next_version].Initialize(size() * 2, disk_.log().alignment());
  overflow_buckets_allocator_[next_version].Initialize(disk_.log().alignment(), epoch_);
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
inline Status HashIndex<D, HID>::Checkpoint(CheckpointState<file_t>& checkpoint) {
  // Checkpoint the hash table
  auto path = disk_.relative_index_checkpoint_path(checkpoint.index_token);
  file_t ht_file = disk_.NewFile(path + "ht.dat");
  RETURN_NOT_OK(ht_file.Open(&disk_.handler()));
  RETURN_NOT_OK(table_[this->resize_info.version].Checkpoint(disk_, std::move(ht_file),
                                              checkpoint.index_metadata.num_ht_bytes));
  // Checkpoint overflow buckets
  file_t ofb_file = disk_.NewFile(path + "ofb.dat");
  RETURN_NOT_OK(ofb_file.Open(&disk_.handler()));
  RETURN_NOT_OK(overflow_buckets_allocator_[this->resize_info.version].Checkpoint(disk_,
                std::move(ofb_file), checkpoint.index_metadata.num_ofb_bytes));

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

  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<CheckpointMetadataIoContext> context{ ctxt };
    // callback is called only once
    assert(*(context->result) == Status::Corruption && context->completed->load() == false);
    // mark request completed
    *(context->result) = result;
    context->completed->store(true);
  };

  Status write_result{ Status::Corruption };
  std::atomic<bool> write_completed{ false };
  CheckpointMetadataIoContext context{ &write_result, &write_completed };

  // Create file
  auto filepath = disk_.relative_index_checkpoint_path(checkpoint.index_token) + "info.dat";
  file_t file = disk_.NewFile(filepath);
  RETURN_NOT_OK(file.Open(&disk_.handler()));

  // Create aligned buffer used in write async
  uint32_t write_size = sizeof(checkpoint.index_metadata);
  assert(write_size <= file.alignment());       // less than file alignment
  write_size += file.alignment() - write_size;  // pad to file alignment
  assert(write_size % file.alignment() == 0);
  uint8_t* buffer = reinterpret_cast<uint8_t*>(core::aligned_alloc(file.alignment(), write_size));
  memset(buffer, 0, write_size);
  memcpy(buffer, &checkpoint.index_metadata, sizeof(checkpoint.index_metadata));
  // Write to file
  RETURN_NOT_OK(file.WriteAsync(buffer, 0, write_size, callback, context));

  // Wait until disk I/O completes
  while(!write_completed) {
    disk_.TryComplete();
    std::this_thread::yield();
  }
  return write_result;
}

template <class D, class HID>
inline Status HashIndex<D, HID>::Recover(CheckpointState<file_t>& checkpoint) {
  uint8_t version = this->resize_info.version;
  assert(table_[version].size() == checkpoint.index_metadata.table_size);
  auto path = disk_.relative_index_checkpoint_path(checkpoint.index_token);

  // Recover the main hash table.
  file_t ht_file = disk_.NewFile(path + "ht.dat");
  RETURN_NOT_OK(ht_file.Open(&disk_.handler()));
  RETURN_NOT_OK(table_[version].Recover(disk_, std::move(ht_file),
                                              checkpoint.index_metadata.num_ht_bytes));
  // Recover the hash table's overflow buckets.
  file_t ofb_file = disk_.NewFile(path + "ofb.dat");
  RETURN_NOT_OK(ofb_file.Open(&disk_.handler()));
  RETURN_NOT_OK(overflow_buckets_allocator_[version].Recover(disk_, std::move(ofb_file),
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
inline Status HashIndex<D, HID>::ReadCheckpointMetadata(const Guid& token,
                                                      CheckpointState<file_t>& checkpoint) {

  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<CheckpointMetadataIoContext> context{ ctxt };
    // callback is called only once
    assert(*(context->result) == Status::Corruption && context->completed->load() == false);
    // mark request completed
    *(context->result) = result;
    context->completed->store(true);
  };

  Status read_result{ Status::Corruption };
  std::atomic<bool> read_completed{ false };
  CheckpointMetadataIoContext context{ &read_result, &read_completed };

  // Read from file
  auto filepath = disk_.relative_index_checkpoint_path(token) + "info.dat";
  file_t file = disk_.NewFile(filepath);
  RETURN_NOT_OK(file.Open(&disk_.handler()));

  // Create aligned buffer used in read async
  uint32_t read_size = sizeof(checkpoint.index_metadata);
  assert(read_size <= file.alignment());      // less than file alignment
  read_size += file.alignment() - read_size;  // pad to file alignment
  assert(read_size % file.alignment() == 0);
  uint8_t* buffer = reinterpret_cast<uint8_t*>(core::aligned_alloc(file.alignment(), read_size));
  memset(buffer, 0, read_size);
  // Write to file
  RETURN_NOT_OK(file.ReadAsync(0, buffer, read_size, callback, context));

  // Wait until disk I/O completes
  while(!read_completed) {
    disk_.TryComplete();
    std::this_thread::yield();
  }
  // Copy from buffer to struct
  memcpy(&checkpoint.index_metadata, buffer, sizeof(checkpoint.index_metadata));

  return read_result;
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
inline void HashIndex<D, HID>::DumpDistribution() {
  // stat variables
  uint64_t total_record_count = 0;
  uint64_t histogram[16] = { 0 };

  uint8_t version = this->resize_info.version;
  for(uint64_t bucket_idx = 0; bucket_idx < table_[version].size(); ++bucket_idx) {
    const hash_bucket_t* bucket = &table_[version].bucket(bucket_idx);
    uint64_t count = 0;

    while(true) {
      for(uint32_t entry_idx = 0; entry_idx < hash_bucket_t::kNumEntries; ++entry_idx) {
        hash_bucket_entry_t entry{ bucket->entries[entry_idx].load() };
        if(!entry.unused()) {
          ++count;
          ++total_record_count;
        }
      }
      if (!GetNextBucket(bucket, version)) {
        // No more buckets in the chain.
        break;
      }
    }
    if(count < 15) {
      ++histogram[count];
    } else {
      ++histogram[15];
    }
  }

  fprintf(stderr, "# of hash buckets: %" PRIu64 "\n", table_[version].size());
  fprintf(stderr, "Total record count: %" PRIu64 "\n", total_record_count);
  fprintf(stderr, "Histogram:\n");
  for(uint8_t idx = 0; idx < 15; ++idx) {
    fprintf(stderr, "%2u : %" PRIu64 "\n", idx, histogram[idx]);
  }
  fprintf(stderr, "15+: %" PRIu64 "\n", histogram[15]);
}

}
} // namespace FASTER::index
