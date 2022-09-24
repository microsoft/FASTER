#include "../core/async.h"
#include "../core/async_result_types.h"

namespace FASTER {
namespace index {

class HashIndexChunkKey {
 public:
  HashIndexChunkKey(uint64_t value)
    : key{ value } {
  }
  HashIndexChunkKey(const HashIndexChunkKey&) = default;

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(HashIndexChunkKey));
  }
  inline core::KeyHash GetHash() const {
    std::hash<uint64_t> hash_fn;
    return core::KeyHash{ hash_fn(key) };
  }

  inline bool operator==(const HashIndexChunkKey& other) const {
    return key == other.key;
  }
  inline bool operator!=(const HashIndexChunkKey& other) const {
    return key != other.key;
  }
 private:
  uint64_t key;
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
  /*HashIndexChunkEntry(const HashIndexChunkEntry& other) {
    std::memcpy(&entries, &other.entries, sizeof(HashIndexChunkEntry));
  }*/
  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(HashIndexChunkEntry));
  }
  /// Number of entries per bucket
  static constexpr uint32_t kNumEntries = 64;
  /// The entries.
  ColdLogIndexHashBucket entries[kNumEntries];
};
static_assert(sizeof(HashIndexChunkEntry) == 4096, "sizeof(HashIndexChunkEntry) != 4kB");


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
    , entry{ entry_ }
    , key_{ key }
    , pos_{ pos } {
  }
  /// Copy (and deep-copy) constructor.
  FasterIndexContext(const FasterIndexContext& other, IAsyncContext* caller_context_)
    : caller_context{ caller_context_ }
    , op_type{ other.op_type }
    , io_id{ other.io_id }
    , thread_io_responses{ other.thread_io_responses }
    , result{ other.result }
    , entry{ other.entry }
    , key_{ other.key_ }
    , pos_{ other.pos_ } {
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
  /// Value found in the hash bucket entry
  HashBucketEntry entry;

 protected:
  /// Used to identify which hash index chunk to update
  HashIndexChunkKey key_;
  /// Used to identify which hash bucket entry (inside chunk) to update
  HashIndexChunkPos pos_;
};

/// Used by FindEntry hash index method
class FasterIndexReadContext : public FasterIndexContext {
 public:
  FasterIndexReadContext(OperationType op_type, IAsyncContext& caller_context, uint64_t io_id,
                        concurrent_queue<AsyncIndexIOContext*>* thread_io_responses,
                        HashIndexChunkKey key, HashIndexChunkPos pos)
    : FasterIndexContext(op_type, caller_context, io_id, thread_io_responses,
                          HashBucketEntry::kInvalidEntry, key, pos) {
  }
  /// Copy (and deep-copy) constructor.
  FasterIndexReadContext(const FasterIndexReadContext& other, IAsyncContext* caller_context_)
    : FasterIndexContext(other, caller_context_) {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const HashIndexChunkKey& key() const {
    return key_;
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
/// NOTE: force argument distingishes between the update-related methods
class FasterIndexRmwContext : public FasterIndexContext {
 public:
  FasterIndexRmwContext(OperationType op_type, IAsyncContext& caller_context, uint64_t io_id,
                        concurrent_queue<AsyncIndexIOContext*>* thread_io_responses,
                        HashIndexChunkKey key, HashIndexChunkPos pos, bool force,
                        HashBucketEntry expected_entry_, HashBucketEntry desired_entry)
    : FasterIndexContext(op_type, caller_context, io_id,
                        thread_io_responses, expected_entry_, key, pos)
    , desired_entry_{ desired_entry }
    , force_{ force } {
  }
  /// Copy (and deep-copy) constructor.
  FasterIndexRmwContext(const FasterIndexRmwContext& other, IAsyncContext* caller_context_)
    : FasterIndexContext(other, caller_context_)
    , desired_entry_{ other.desired_entry_ }
    , force_{ other.force_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const HashIndexChunkKey& key() const {
    return key_;
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
  inline void UpdateEntry(HashIndexChunkEntry& value) {
    // (Try to) update single hash bucket entry
    AtomicHashBucketEntry* atomic_entry = &value.entries[pos_.index].entries[pos_.tag];
    if (record_address() != Address::kInvalidAddress) {
      if (!force_) { // TryUpdateEntry
        if (atomic_entry->compare_exchange_strong(entry, desired_entry_)) {
          entry = desired_entry_;
          result = Status::Ok;
        } else {
          result = Status::Aborted;
        }
      } else { // UpdateEntry
        atomic_entry->store(desired_entry_);
        entry = desired_entry_;
        result = Status::Ok;
      }
    } else { // FindOrCreateEntry
      assert(entry == HashBucketEntry::kInvalidEntry);
      assert(desired_entry_.address() == Address::kInvalidAddress);
      if (atomic_entry->compare_exchange_strong(entry, desired_entry_)) {
        // atomic entry was updated successfully; update local entry copy
        entry = desired_entry_;
      }
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