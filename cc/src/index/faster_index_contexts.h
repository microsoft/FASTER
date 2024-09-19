// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "../core/async.h"
#include "../core/async_result_types.h"
#include "../core/utility.h"

namespace FASTER {
namespace index {

template <class KH>
class HashIndexChunkKey {
 public:
  typedef KH key_hash_t;

  HashIndexChunkKey(uint64_t key_, uint16_t tag_)
    : key{ key_ }
    , tag{ tag_ } {
  }
  HashIndexChunkKey(const HashIndexChunkKey&) = default;

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(HashIndexChunkKey));
  }
  inline core::KeyHash GetHash() const {
    const key_hash_t hash{ key, tag };
    return core::KeyHash{ hash.control() };
  }

  inline bool operator==(const HashIndexChunkKey& other) const {
    return key == other.key;
  }
  inline bool operator!=(const HashIndexChunkKey& other) const {
    return key != other.key;
  }
 private:
  uint64_t key;
  uint16_t tag;
};

struct HashIndexChunkPos {
  uint8_t index;
  uint8_t tag;
};

/// A chunk consisting of `NB` hash buckets -- 8 * `NB` bucket entries (no overflow entries) [64 bytes each]
template<unsigned NB>
struct alignas(Constants::kCacheLineBytes) HashIndexChunkEntry {
  HashIndexChunkEntry() {
    std::memset(&entries, 0, sizeof(HashIndexChunkEntry<NB>));
  }
  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(HashIndexChunkEntry<NB>));
  }
  /// Number of buckets
  constexpr static uint32_t kNumBuckets = NB;
  static_assert(kNumBuckets >= 1 && kNumBuckets <= 64,
    "HashIndexChunkEntry: Number of buckets should be between 1 and 64.");
  static_assert(core::is_power_of_2(kNumBuckets),
    "HashIndexChunkEntry: Number of buckets should be a power of 2.");

  /// The entries.
  ColdLogIndexHashBucket entries[kNumBuckets];
};
// Make sure typical sizes for hash chunk are correct
static_assert(sizeof(HashIndexChunkEntry<64>) == 4096, "sizeof(HashIndexChunkEntry) != 4kB");
static_assert(sizeof(HashIndexChunkEntry<32>) == 2048, "sizeof(HashIndexChunkEntry) != 2kB");
static_assert(sizeof(HashIndexChunkEntry<16>) == 1024, "sizeof(HashIndexChunkEntry) != 1kB");
static_assert(sizeof(HashIndexChunkEntry<8>) == 512, "sizeof(HashIndexChunkEntry) != 512B");
static_assert(sizeof(HashIndexChunkEntry<4>) == 256, "sizeof(HashIndexChunkEntry) != 256B");
static_assert(sizeof(HashIndexChunkEntry<2>) == 128, "sizeof(HashIndexChunkEntry) != 128B");
static_assert(sizeof(HashIndexChunkEntry<1>) == 64, "sizeof(HashIndexChunkEntry) != 64B");

enum class HashIndexOp : uint8_t {
  FIND_ENTRY = 0,
  FIND_OR_CREATE_ENTRY,
  TRY_UPDATE_ENTRY,
  UPDATE_ENTRY,

  INVALID,
};

template <class HID>
class FasterIndexContext : public IAsyncContext {
 public:
  typedef typename HID::key_hash_t key_hash_t;

  typedef HashIndexChunkKey<key_hash_t> key_t;
  typedef HashIndexChunkEntry<HID::kNumBuckets> value_t;

  FasterIndexContext(OperationType op_type_, IAsyncContext& caller_context_, uint64_t io_id_,
                    concurrent_queue<AsyncIndexIOContext*>* thread_io_responses_,
                    HashBucketEntry entry_, key_t key, HashIndexChunkPos pos)
    : op_type{ op_type_ }
    , caller_context{ &caller_context_ }
    , io_id{ io_id_ }
    , thread_io_responses{ thread_io_responses_ }
    , result{ Status::Corruption }
    , expected_entry{ entry_ }
    , entry{ entry_ }
    , key_{ key }
    , pos_{ pos } {
  #ifdef STATISTICS
      hash_index_op = HashIndexOp::INVALID;
  #endif
  }
  /// Copy (and deep-copy) constructor.
  FasterIndexContext(const FasterIndexContext& other, IAsyncContext* caller_context_)
    : caller_context{ caller_context_ }
    , op_type{ other.op_type }
    , io_id{ other.io_id }
    , thread_io_responses{ other.thread_io_responses }
    , result{ other.result }
    , expected_entry{ other.expected_entry }
    , entry{ other.entry }
    , key_{ other.key_ }
    , pos_{ other.pos_ } {
  #ifdef STATISTICS
      hash_index_op = other.hash_index_op;
      hash_index = other.hash_index;
  #endif
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const key_t& key() const {
    return key_;
  }

 public:
  /// Operation Type
  OperationType op_type;
  /// Caller context
  IAsyncContext* caller_context;
  /// Unique id for (potential) I/O request
  uint64_t io_id;
  /// Queue where finished pending requests are pushed
  concurrent_queue<AsyncIndexIOContext*>* thread_io_responses;
  /// Result of the operation
  Status result;
  // Value expected to be found in the bucket
  HashBucketEntry expected_entry;
  /// Value actually *found* in the hash bucket entry
  HashBucketEntry entry;

 protected:
  /// Used to identify which hash index chunk to update
  key_t key_;
  /// Used to identify which hash bucket entry (inside chunk) to update
  HashIndexChunkPos pos_;

#ifdef STATISTICS
 public:
  void set_hash_index(void* hash_index_) {
    hash_index = hash_index_;
  }

  HashIndexOp hash_index_op;
  void* hash_index;
#endif
};

/// Used by FindEntry hash index method
template <class HID>
class FasterIndexReadContext : public FasterIndexContext<HID> {
 public:
  typedef typename FasterIndexContext<HID>::key_t key_t;
  typedef typename FasterIndexContext<HID>::value_t value_t;

  FasterIndexReadContext(OperationType op_type, IAsyncContext& caller_context, uint64_t io_id,
                        concurrent_queue<AsyncIndexIOContext*>* thread_io_responses,
                        key_t key, HashIndexChunkPos pos)
    : FasterIndexContext<HID>(op_type, caller_context, io_id, thread_io_responses,
                          HashBucketEntry::kInvalidEntry, key, pos) {
#ifdef STATISTICS
    this->hash_index_op = HashIndexOp::FIND_ENTRY;
#endif
  }
  /// Copy (and deep-copy) constructor.
  FasterIndexReadContext(const FasterIndexReadContext& other, IAsyncContext* caller_context_)
    : FasterIndexContext<HID>(other, caller_context_) {
  }

  inline void Get(const value_t& value) {
    const AtomicHashBucketEntry& atomic_entry = value.entries[this->pos_.index].entries[this->pos_.tag];
    this->entry = atomic_entry.load();
    this->result = (this->entry != HashBucketEntry::kInvalidEntry) ? Status::Ok : Status::NotFound;
  }
  inline bool GetAtomic(const value_t& value) {
    const AtomicHashBucketEntry& atomic_entry = value.entries[this->pos_.index].entries[this->pos_.tag];
    this->entry = atomic_entry.load();
    this->result = (this->entry != HashBucketEntry::kInvalidEntry) ? Status::Ok : Status::NotFound;
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, this->caller_context, context_copy);
  }
};

/// Used by FindOrCreateEntry, TryUpdateEntry & UpdateEntry hash index methods
/// NOTE: force argument distinguishes between TryUpdateEntry and UpdateEntry.
template <class HID>
class FasterIndexRmwContext : public FasterIndexContext<HID> {
 public:
  typedef typename FasterIndexContext<HID>::key_t key_t;
  typedef typename FasterIndexContext<HID>::value_t value_t;

  FasterIndexRmwContext(OperationType op_type, IAsyncContext& caller_context, uint64_t io_id,
                        concurrent_queue<AsyncIndexIOContext*>* thread_io_responses,
                        key_t key, HashIndexChunkPos pos, bool force,
                        HashBucketEntry expected_entry, HashBucketEntry desired_entry)
    : FasterIndexContext<HID>(op_type, caller_context, io_id,
                        thread_io_responses, expected_entry, key, pos)
    , desired_entry_{ desired_entry }
    , force_{ force } {
#ifdef STATISTICS
    if (is_find_or_create_entry_request()) {
      this->hash_index_op = HashIndexOp::FIND_OR_CREATE_ENTRY;
    } else {
      this->hash_index_op = force_ ? HashIndexOp::UPDATE_ENTRY : HashIndexOp::TRY_UPDATE_ENTRY;
    }
#endif
  }
  /// Copy (and deep-copy) constructor.
  FasterIndexRmwContext(const FasterIndexRmwContext& other, IAsyncContext* caller_context_)
    : FasterIndexContext<HID>(other, caller_context_)
    , desired_entry_{ other.desired_entry_ }
    , force_{ other.force_ } {
  }

  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  inline static constexpr uint32_t value_size(const value_t& old_value) {
    return sizeof(value_t);
  }

  inline void RmwInitial(value_t& value) {
    // Initialize chunk to free hash bucket entries
    std::memset(&value, 0, sizeof(value_t));
    // Perform update
    UpdateEntry(value);
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    // Duplicate old chunk
    std::memcpy(&value, &old_value, sizeof(value_t));
    // Perform update
    UpdateEntry(value);
  }
  inline bool RmwAtomic(value_t& value) {
    // Perform in-place update
    UpdateEntry(value);
    return true;
  }

  inline Address record_address() const {
    return desired_entry_.address();
  }

 private:
  inline bool is_find_or_create_entry_request() {
    return record_address() == Address::kInvalidAddress;
  }

  inline void UpdateEntry(value_t& value) {
    // (Try to) update single hash bucket entry
    AtomicHashBucketEntry* atomic_entry = &value.entries[this->pos_.index].entries[this->pos_.tag];
    HashBucketEntry local_expected_entry{ this->expected_entry };

    if (!is_find_or_create_entry_request()) {
      if (!force_) { // TryUpdateEntry
        bool success = atomic_entry->compare_exchange_strong(local_expected_entry, desired_entry_);
        this->result = success ? Status::Ok : Status::Aborted;
        this->entry = success ? desired_entry_ : local_expected_entry;
      } else { // UpdateEntry
        atomic_entry->store(desired_entry_);
        this->entry = desired_entry_;
        this->result = Status::Ok;
      }
    } else { // FindOrCreateEntry
      assert(desired_entry_.address() == Address::kInvalidAddress);
      bool success = atomic_entry->compare_exchange_strong(local_expected_entry, desired_entry_);
      // update entry with latest bucket value
      this->entry = success ? desired_entry_ : local_expected_entry;
      this->result = Status::Ok;
    }
    // NOTE: this->entry has the most recent value of atomic_entry
    assert(this->entry != HashBucketEntry::kInvalidEntry);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, this->caller_context, context_copy);
  }

 private:
   /// New hash bucket entry value
  HashBucketEntry desired_entry_;
  /// Denotes whether to use CAS or unprotected store atomic operation
  bool force_;
};

}
} // namespace FASTER::index