#pragma once

#include "../core/async.h"
#include "../core/async_result_types.h"
#include "../core/utility.h"

namespace FASTER {
namespace index {

class HashIndexChunkKey {
 public:
  HashIndexChunkKey(uint64_t key_, uint16_t tag_)
    : key{ key_ }
    , tag{ tag_ } {
  }
  HashIndexChunkKey(const HashIndexChunkKey&) = default;

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(HashIndexChunkKey));
  }
  inline core::KeyHash GetHash() const {
    core::ColdLogIndexKeyHash hash{ key, tag };
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

/// A chunk consisting of 64 hash buckets -- 512 bucket entries (no overflow entries)
struct alignas(Constants::kCacheLineBytes) HashIndexChunkEntry {
  HashIndexChunkEntry() {
    std::memset(&entries, 0, sizeof(HashIndexChunkEntry));
  }
  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(HashIndexChunkEntry));
  }
  /// Number of entries per bucket
  //static constexpr uint32_t kNumEntries = 64;   // 4096B
  //static constexpr uint32_t kNumEntries = 32;   // 2048B
  //static constexpr uint32_t kNumEntries = 16;   // 1024B
  //static constexpr uint32_t kNumEntries = 8;    //  512B
  static constexpr uint32_t kNumEntries = 4;    //  256B
  //static constexpr uint32_t kNumEntries = 2;    //  128B
  //static constexpr uint32_t kNumEntries = 1;    //  64B
  /// The entries.
  ColdLogIndexHashBucket entries[kNumEntries];
};
//static_assert(sizeof(HashIndexChunkEntry) == 4096, "sizeof(HashIndexChunkEntry) != 4kB");
//static_assert(sizeof(HashIndexChunkEntry) == 2048, "sizeof(HashIndexChunkEntry) != 2kB");
//static_assert(sizeof(HashIndexChunkEntry) == 1024, "sizeof(HashIndexChunkEntry) != 1kB");
//static_assert(sizeof(HashIndexChunkEntry) == 512, "sizeof(HashIndexChunkEntry) != 512B");
static_assert(sizeof(HashIndexChunkEntry) == 256, "sizeof(HashIndexChunkEntry) != 256B");
//static_assert(sizeof(HashIndexChunkEntry) == 128, "sizeof(HashIndexChunkEntry) != 128B");
//static_assert(sizeof(HashIndexChunkEntry) == 64, "sizeof(HashIndexChunkEntry) != 64B");

enum class HashIndexOp : uint8_t {
  FIND_ENTRY = 0,
  FIND_OR_CREATE_ENTRY,
  TRY_UPDATE_ENTRY,
  UPDATE_ENTRY,

  INVALID,
};

class FasterIndexContext : public IAsyncContext {
 public:
  typedef HashIndexChunkKey key_t;
  typedef HashIndexChunkEntry value_t;

  FasterIndexContext(OperationType op_type_, IAsyncContext& caller_context_, uint64_t io_id_,
                    concurrent_queue<AsyncIndexIOContext*>* thread_io_responses_,
                    HashBucketEntry entry_, HashIndexChunkKey key, HashIndexChunkPos pos)
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
  inline const HashIndexChunkKey& key() const {
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
  HashIndexChunkKey key_;
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
class FasterIndexReadContext : public FasterIndexContext {
 public:
  FasterIndexReadContext(OperationType op_type, IAsyncContext& caller_context, uint64_t io_id,
                        concurrent_queue<AsyncIndexIOContext*>* thread_io_responses,
                        HashIndexChunkKey key, HashIndexChunkPos pos)
    : FasterIndexContext(op_type, caller_context, io_id, thread_io_responses,
                          HashBucketEntry::kInvalidEntry, key, pos) {
#ifdef STATISTICS
    hash_index_op = HashIndexOp::FIND_ENTRY;
#endif
  }
  /// Copy (and deep-copy) constructor.
  FasterIndexReadContext(const FasterIndexReadContext& other, IAsyncContext* caller_context_)
    : FasterIndexContext(other, caller_context_) {
  }

  inline void Get(const HashIndexChunkEntry& value) {
    const AtomicHashBucketEntry& atomic_entry = value.entries[pos_.index].entries[pos_.tag];
    entry = atomic_entry.load();
    result = (entry != HashBucketEntry::kInvalidEntry) ? Status::Ok : Status::NotFound;
  }
  inline bool GetAtomic(const HashIndexChunkEntry& value) {
    const AtomicHashBucketEntry& atomic_entry = value.entries[pos_.index].entries[pos_.tag];
    entry = atomic_entry.load();
    result = (entry != HashBucketEntry::kInvalidEntry) ? Status::Ok : Status::NotFound;
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, caller_context, context_copy);
  }
};

/// Used by FindOrCreateEntry, TryUpdateEntry & UpdateEntry hash index methods
/// NOTE: force argument distinguishes between the update-related methods
class FasterIndexRmwContext : public FasterIndexContext {
 public:
  FasterIndexRmwContext(OperationType op_type, IAsyncContext& caller_context, uint64_t io_id,
                        concurrent_queue<AsyncIndexIOContext*>* thread_io_responses,
                        HashIndexChunkKey key, HashIndexChunkPos pos, bool force,
                        HashBucketEntry expected_entry, HashBucketEntry desired_entry)
    : FasterIndexContext(op_type, caller_context, io_id,
                        thread_io_responses, expected_entry, key, pos)
    , desired_entry_{ desired_entry }
    , force_{ force } {
#ifdef STATISTICS
    if (is_find_or_create_entry_request()) {
      hash_index_op = HashIndexOp::FIND_OR_CREATE_ENTRY;
    } else {
      hash_index_op = force_ ? HashIndexOp::UPDATE_ENTRY : HashIndexOp::TRY_UPDATE_ENTRY;
    }
#endif
  }
  /// Copy (and deep-copy) constructor.
  FasterIndexRmwContext(const FasterIndexRmwContext& other, IAsyncContext* caller_context_)
    : FasterIndexContext(other, caller_context_)
    , desired_entry_{ other.desired_entry_ }
    , force_{ other.force_ } {
  }

  inline static constexpr uint32_t value_size() {
    return sizeof(HashIndexChunkEntry);
  }
  inline static constexpr uint32_t value_size(const HashIndexChunkEntry& old_value) {
    return sizeof(HashIndexChunkEntry);
  }

  inline void RmwInitial(HashIndexChunkEntry& value) {
    // Initialize chunk to free hash bucket entries
    std::memset(&value, 0, sizeof(HashIndexChunkEntry));
    // Perform update
    UpdateEntry(value);
  }
  inline void RmwCopy(const HashIndexChunkEntry& old_value, HashIndexChunkEntry& value) {
    // Duplicate old chunk
    std::memcpy(&value, &old_value, sizeof(HashIndexChunkEntry));
    // Perform update
    UpdateEntry(value);
  }
  inline bool RmwAtomic(HashIndexChunkEntry& value) {
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

  inline void UpdateEntry(HashIndexChunkEntry& value) {
    // (Try to) update single hash bucket entry
    AtomicHashBucketEntry* atomic_entry = &value.entries[pos_.index].entries[pos_.tag];
    HashBucketEntry local_expected_entry{ expected_entry };

    if (!is_find_or_create_entry_request()) {
      if (!force_) { // TryUpdateEntry
        bool success = atomic_entry->compare_exchange_strong(local_expected_entry, desired_entry_);
        result = success ? Status::Ok : Status::Aborted;
        entry = success ? desired_entry_ : local_expected_entry;
      } else { // UpdateEntry
        atomic_entry->store(desired_entry_);
        entry = desired_entry_;
        result = Status::Ok;
      }
    } else { // FindOrCreateEntry
      assert(desired_entry_.address() == Address::kInvalidAddress);
      bool success = atomic_entry->compare_exchange_strong(local_expected_entry, desired_entry_);
      // update entry with latest bucket value
      entry = success ? desired_entry_ : local_expected_entry;
      result = Status::Ok;
    }
    // NOTE: this->entry has the most recent value of atomic_entry
    assert(entry != HashBucketEntry::kInvalidEntry);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, caller_context, context_copy);
  }

 private:
   /// New hash bucket entry value
  HashBucketEntry desired_entry_;
  /// Denotes whether to use CAS or unprotected store atomic operation
  bool force_;
};

}
} // namespace FASTER::index