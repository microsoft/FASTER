#pragma once

#include "async.h"
#include "gc_state.h"
#include "grow_state.h"
#include "hash_table.h"
#include "persistent_memory_malloc.h"
#include "state_transitions.h"
#include "status.h"

namespace FASTER {
namespace core {

class HashIndexEntryPendingContext : public IAsyncContext {

 protected:
  HashIndexEntryPendingContext(OperationType type_, IAsyncContext& caller_context_,
                 AsyncCallback caller_callback_)
    : type{ type_ }
    , caller_context{ &caller_context_ }
    , caller_callback{ caller_callback_ }
    , version{ UINT32_MAX }
    , phase{ Phase::INVALID }
    , result{ Status::Pending }
    , address{ Address::kInvalidAddress }
    , entry{ HashBucketEntry::kInvalidEntry } {
  }

 public:
  /// The deep-copy constructor.
  HashIndexEntryPendingContext(const HashIndexEntryPendingContext& other, IAsyncContext* caller_context_)
    : type{ other.type }
    , caller_context{ caller_context_ }
    , caller_callback{ other.caller_callback }
    , version{ other.version }
    , phase{ other.phase }
    , result{ other.result }
    , address{ other.address }
    , entry{ other.entry } {
  }

  /// Caller context.
  IAsyncContext* caller_context;
  /// Caller callback.
  AsyncCallback caller_callback;
  /// Checkpoint version.
  uint32_t version;
  /// Checkpoint phase.
  Phase phase;
  /// Type of operation (Read, Upsert, RMW, etc.).
  OperationType type;
  ///
  Status result;
  /// Address of the record being read or modified.
  Address address;
  /// Hash table entry that (indirectly) leads to the record being read or modified.
  HashBucketEntry entry;
};

template<class D>
class HashIndex {
 public:
  typedef D disk_t;
  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  typedef GcStateWithIndex gc_state_t;
  typedef GrowState<hlog_t> grow_state_t;


  HashIndex(uint64_t alignment, LightEpoch& epoch, gc_state_t& gc_state, grow_state_t& grow_state)
    : disk_alignment_{ alignment }
    , epoch_{ epoch }
    , gc_state_{ &gc_state }
    , grow_state_{ &grow_state } {
  }

  inline void Initialize(uint64_t new_size) {
    resize_info.version = 0;
    table_[0].Initialize(new_size, disk_alignment_);
    overflow_buckets_allocator_[0].Initialize(disk_alignment_, epoch_);
  }

  inline void DumpDistribution() {
    table_[resize_info.version].DumpDistribution(
      overflow_buckets_allocator_[resize_info.version]);
  }

  // Find the hash bucket entry, if any, corresponding to the specified hash.
  // The caller can use the "expected_entry" to CAS its desired address into the entry.
  inline Status FindEntry(KeyHash hash, HashBucketEntry& expected_entry,
                          AtomicHashBucketEntry*& atomic_entry) const;

  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // create a new entry. The caller can use the "expected_entry" to CAS its desired address into
  // the entry.
  inline Status FindOrCreateEntry(KeyHash hash, HashBucketEntry& expected_entry,
                                  AtomicHashBucketEntry*& atomic_entry);

  // Garbage Collect methods
  void GarbageCollectSetup(Address new_begin_address,
                          GcState::truncate_callback_t truncate_callback,
                          GcState::complete_callback_t complete_callback);
  bool GarbageCollect();

  // Index grow related methods
  void GrowSetup(GrowCompleteCallback callback);

  template<class R>
  void Grow();

  inline uint64_t size() const {
    return table_[resize_info.version].size();
  }
  inline uint64_t new_size() const {
    return table_[1 - resize_info.version].size();
  }

 private:
  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // return an unused bucket entry.
  inline AtomicHashBucketEntry* FindTentativeEntry(KeyHash hash, HashBucket* bucket, uint8_t version,
                                                    HashBucketEntry& expected_entry);
  inline bool HasConflictingEntry(KeyHash hash, const HashBucket* bucket, uint8_t version,
                                                const AtomicHashBucketEntry* atomic_entry) const;

  /// Helper functions needed for Grow
  void AddHashEntry(HashBucket*& bucket, uint32_t& next_idx, uint8_t version, HashBucketEntry entry);

  template <class R>
  Address TraceBackForOtherChainStart(uint64_t old_size, uint64_t new_size,
                                      Address from_address, Address min_address, uint8_t side);

 public:
  ResizeInfo resize_info;

 private:
  uint64_t disk_alignment_;
  LightEpoch& epoch_;
  gc_state_t* gc_state_;
  grow_state_t* grow_state_;

  // An array of size two, that contains the old and new versions of the hash-table
  InternalHashTable<disk_t> table_[2];
  // Allocator for the hash buckets that don't fit in the hash table.
  MallocFixedPageSize<HashBucket, disk_t> overflow_buckets_allocator_[2];
};

template<class D>
//inline const AtomicHashBucketEntry* HashIndex<D>::FindEntry(KeyHash hash,
//    HashBucketEntry& expected_entry) const {
inline Status HashIndex<D>::FindEntry(KeyHash hash, HashBucketEntry& expected_entry,
                                      AtomicHashBucketEntry*& atomic_entry) const {

  expected_entry = HashBucketEntry::kInvalidEntry;
  // Truncate the hash to get a bucket page_index < table[version].size.
  uint32_t version = resize_info.version;
  assert(version == 0 || version == 1);

  const HashBucket* bucket = &table_[version].bucket(hash);
  assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

  while(true) {
    // Search through the bucket looking for our key. Last entry is reserved
    // for the overflow pointer.
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
      if(entry.unused()) {
        continue;
      }
      if(hash.tag() == entry.tag()) {
        // Found a matching tag.
        // (So, the input hash matches the entry on 14 tag bits + log_2(table size) address bits.)
        if(!entry.tentative()) {
          // If (final key, return immediately)
          expected_entry = entry;
          atomic_entry = const_cast<AtomicHashBucketEntry*>(&bucket->entries[entry_idx]);
          return Status::Ok;
        }
      }
    }

    // Go to next bucket in the chain
    HashBucketOverflowEntry entry = bucket->overflow_entry.load();
    if(entry.unused()) {
      // No more buckets in the chain.
      assert(expected_entry == HashBucketEntry::kInvalidEntry);
      atomic_entry = nullptr;
      return Status::NotFound;
    }
    bucket = &overflow_buckets_allocator_[version].Get(entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }

  assert(false);
  return Status::Corruption; // NOT REACHED
}

template <class D>
inline Status HashIndex<D>::FindOrCreateEntry(KeyHash hash, HashBucketEntry& expected_entry,
                                              AtomicHashBucketEntry*& atomic_entry) {
  // Truncate the hash to get a bucket page_index < table[version].size.
  const uint32_t version = resize_info.version;
  assert(version == 0 || version == 1);

  while(true) {
    HashBucket* bucket = &table_[version].bucket(hash);
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

    atomic_entry = FindTentativeEntry(hash, bucket, version, expected_entry);
    if(expected_entry != HashBucketEntry::kInvalidEntry) {
      // Found an existing hash bucket entry; nothing further to check.
      return Status::Ok;
    }
    // We have a free slot.
    assert(atomic_entry);
    assert(expected_entry == HashBucketEntry::kInvalidEntry);

    // Try to install tentative tag in free slot.
    HashBucketEntry entry{ Address::kInvalidAddress, hash.tag(), true };
    if(atomic_entry->compare_exchange_strong(expected_entry, entry)) {
      // See if some other thread is also trying to install this tag.
      if(HasConflictingEntry(hash, bucket, version, atomic_entry)) {
        // Back off and try again.
        atomic_entry->store(HashBucketEntry::kInvalidEntry);
      } else {
        // No other thread was trying to install this tag,
        // so we can clear our entry's "tentative" bit.
        expected_entry = HashBucketEntry{ Address::kInvalidAddress, hash.tag(), false };
        atomic_entry->store(expected_entry);
        return Status::Ok;
      }
    }
  }
  assert(false);
  return Status::Corruption; // NOT REACHED
}

template <class D>
inline AtomicHashBucketEntry* HashIndex<D>::FindTentativeEntry(KeyHash hash,
    HashBucket* bucket, uint8_t version, HashBucketEntry& expected_entry) {

  expected_entry = HashBucketEntry::kInvalidEntry;
  AtomicHashBucketEntry* atomic_entry = nullptr;
  // Try to find a slot that contains the right tag or that's free.
  while(true) {
    // Search through the bucket looking for our key. Last entry is reserved
    // for the overflow pointer.
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
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
    HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
    if(overflow_entry.unused()) {
      // No more buckets in the chain.
      if(atomic_entry) {
        // We found a free slot earlier (possibly inside an earlier bucket).
        assert(expected_entry == HashBucketEntry::kInvalidEntry);
        return atomic_entry;
      }
      // We didn't find any free slots, so allocate new bucket.
      FixedPageAddress new_bucket_addr = overflow_buckets_allocator_[version].Allocate();
      bool success;
      do {
        HashBucketOverflowEntry new_bucket_entry{ new_bucket_addr };
        success = bucket->overflow_entry.compare_exchange_strong(overflow_entry,
                  new_bucket_entry);
      } while(!success && overflow_entry.unused());
      if(!success) {
        // Install failed, undo allocation; use the winner's entry
        overflow_buckets_allocator_[version].FreeAtEpoch(new_bucket_addr, 0);
      } else {
        // Install succeeded; we have a new bucket on the chain. Return its first slot.
        bucket = &overflow_buckets_allocator_[version].Get(new_bucket_addr);
        assert(expected_entry == HashBucketEntry::kInvalidEntry);
        return &bucket->entries[0];
      }
    }
    // Go to the next bucket.
    bucket = &overflow_buckets_allocator_[version].Get(overflow_entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }
  assert(false);
  return nullptr; // NOT REACHED
}

template <class D>
inline bool HashIndex<D>::HasConflictingEntry(KeyHash hash, const HashBucket* bucket,
    uint8_t version, const AtomicHashBucketEntry* atomic_entry) const {
  uint16_t tag = atomic_entry->load().tag();
  while(true) {
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
      if(entry != HashBucketEntry::kInvalidEntry &&
          entry.tag() == tag &&
          atomic_entry != &bucket->entries[entry_idx]) {
        // Found a conflict.
        return true;
      }
    }
    // Go to next bucket in the chain
    HashBucketOverflowEntry entry = bucket->overflow_entry.load();
    if(entry.unused()) {
      // Reached the end of the bucket chain; no conflicts found.
      return false;
    }
    // Go to the next bucket.
    bucket = &overflow_buckets_allocator_[version].Get(entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }
}

template <class D>
void HashIndex<D>::GarbageCollectSetup(Address new_begin_address,
                                      GcState::truncate_callback_t truncate_callback,
                                      GcState::complete_callback_t complete_callback) {
  uint64_t num_chunks = std::max(size() / gc_state_t::kHashTableChunkSize, (uint64_t)1);
  gc_state_->Initialize(new_begin_address, truncate_callback, complete_callback, num_chunks);
}

template <class D>
bool HashIndex<D>::GarbageCollect() {
  uint64_t chunk = gc_state_->next_chunk++;
  if(chunk >= gc_state_->num_chunks) {
    // No chunk left to clean.
    return false;
  }
  uint8_t version = resize_info.version;
  uint64_t upper_bound;
  if(chunk + 1 < gc_state_->num_chunks) {
    // All chunks but the last chunk contain gc.kHashTableChunkSize elements.
    upper_bound = gc_state_t::kHashTableChunkSize;
  } else {
    // Last chunk might contain more or fewer elements.
    upper_bound = table_[version].size() - (chunk * gc_state_t::kHashTableChunkSize);
  }

  for(uint64_t idx = 0; idx < upper_bound; ++idx) {
    HashBucket* bucket = &table_[version].bucket(chunk * gc_state_t::kHashTableChunkSize + idx);
    while(true) {
      for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
        AtomicHashBucketEntry& atomic_entry = bucket->entries[entry_idx];
        HashBucketEntry expected_entry = atomic_entry.load();
        if(!expected_entry.unused() && expected_entry.address() != Address::kInvalidAddress &&
            expected_entry.address() < gc_state_->new_begin_address) {
          // The record that this entry points to was truncated; try to delete the entry.
          atomic_entry.compare_exchange_strong(expected_entry, HashBucketEntry::kInvalidEntry);
          // If deletion failed, then some other thread must have added a new record to the entry.
        }
      }
      // Go to next bucket in the chain.
      HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
      if(overflow_entry.unused()) {
        // No more buckets in the chain.
        break;
      }
      bucket = &overflow_buckets_allocator_[version].Get(overflow_entry.address());
    }
  }
  // Done with this chunk--did some work.
  return true;
}

template <class D>
void HashIndex<D>::GrowSetup(GrowCompleteCallback callback) {
  // Initialize index grow state
  uint8_t current_version = resize_info.version;
  assert(current_version == 0 || current_version == 1);
  uint8_t next_version = 1 - current_version;
  uint64_t num_chunks = std::max(size() / grow_state_t::kHashTableChunkSize, (uint64_t)1);
  grow_state_->Initialize(callback, current_version, num_chunks);

  // Initialize the next version of our hash table to be twice the size of the current version.
  table_[next_version].Initialize(size() * 2, disk_alignment_);
  overflow_buckets_allocator_[next_version].Initialize(disk_alignment_, epoch_);
}

template <class D>
template <class R>
void HashIndex<D>::Grow() {
  typedef R record_t;

  // This thread won't exit until all hash table buckets have been split.
  Address head_address = grow_state_->hlog->head_address.load();
  Address begin_address = grow_state_->hlog->begin_address.load();
  uint8_t old_version = grow_state_->old_version;
  uint8_t new_version = grow_state_->new_version;

  for(uint64_t chunk = grow_state_->next_chunk++; chunk < grow_state_->num_chunks; chunk = grow_state_->next_chunk++) {
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
      HashBucket* old_bucket = &table_[old_version].bucket(
                                 chunk * grow_state_t::kHashTableChunkSize + idx);
      HashBucket* new_bucket0 = &table_[new_version].bucket(
                                  chunk * grow_state_t::kHashTableChunkSize + idx);
      HashBucket* new_bucket1 = &table_[new_version].bucket(
                                  old_size + chunk * grow_state_t::kHashTableChunkSize + idx);
      uint32_t new_entry_idx0 = 0;
      uint32_t new_entry_idx1 = 0;
      while(true) {
        for(uint32_t old_entry_idx = 0; old_entry_idx < HashBucket::kNumEntries; ++old_entry_idx) {
          HashBucketEntry old_entry = old_bucket->entries[old_entry_idx].load();
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
          KeyHash hash = record->key().GetHash();
          if(hash.idx(new_size) < old_size) {
            // Record's key hashes to the 0 side of the new hash table.
            AddHashEntry(new_bucket0, new_entry_idx0, new_version, old_entry);
            Address other_address = TraceBackForOtherChainStart<record_t>(old_size, new_size,
                                                    record->header.previous_address(), head_address, 0);
            if(other_address >= begin_address) {
              // We found a record that either is on disk or has a key that hashes to the 1 side of
              // the new hash table.
              AddHashEntry(new_bucket1, new_entry_idx1, new_version,
                           HashBucketEntry{ other_address, old_entry.tag(), false });
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
                           HashBucketEntry{ other_address, old_entry.tag(), false });
            }
          }
        }
        // Go to next bucket in the chain.
        HashBucketOverflowEntry overflow_entry = old_bucket->overflow_entry.load();
        if(overflow_entry.unused()) {
          // No more buckets in the chain.
          break;
        }
        old_bucket = &overflow_buckets_allocator_[old_version].Get(overflow_entry.address());
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

template <class D>
void HashIndex<D>::AddHashEntry(HashBucket*& bucket, uint32_t& next_idx, uint8_t version,
                                     HashBucketEntry entry) {
  if(next_idx == HashBucket::kNumEntries) {
    // Need to allocate a new bucket, first.
    FixedPageAddress new_bucket_addr = overflow_buckets_allocator_[version].Allocate();
    HashBucketOverflowEntry new_bucket_entry{ new_bucket_addr };
    bucket->overflow_entry.store(new_bucket_entry);
    bucket = &overflow_buckets_allocator_[version].Get(new_bucket_addr);
    next_idx = 0;
  }
  bucket->entries[next_idx].store(entry);
  ++next_idx;
}

template <class D>
template <class R>
Address HashIndex<D>::TraceBackForOtherChainStart(uint64_t old_size, uint64_t new_size,
                                          Address from_address, Address min_address, uint8_t side) {
  typedef R record_t;

  assert(side == 0 || side == 1);
  // Search back as far as min_address.
  while(from_address >= min_address) {
    const record_t* record = reinterpret_cast<const record_t*>(grow_state_->hlog->Get(from_address));
    KeyHash hash = record->key().GetHash();
    if((hash.idx(new_size) < old_size) != (side == 0)) {
      // Record's key hashes to the other side.
      return from_address;
    }
    from_address = record->header.previous_address();
  }
  return from_address;
}



}
} // namespace FASTER::core
