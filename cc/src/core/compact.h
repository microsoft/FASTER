// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "async.h"
#include "hash_bucket.h"
#include "internal_contexts.h"
#include "log_scan.h"
#include "record.h"

namespace FASTER {
namespace core {

template<class F>
struct CompactionThreadsContext {
 public:
  CompactionThreadsContext(LogPageIterator<F>* iter_, int n_threads)
    : iter{ iter_ } {
      for (int i = 0; i < n_threads; i++) {
        done.emplace_back(false);
      }
    }
  // non-copyable
  CompactionThreadsContext(const CompactionThreadsContext&) = delete;

  LogPageIterator<F>* iter;
  std::deque<std::atomic<bool>> done;
};

/// Holds the record info when a compaction request goes async
template<class K, class V>
struct CompactionPendingRecordEntry {
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;

  CompactionPendingRecordEntry(record_t* record_, const Address address_,
                              HashBucketEntry expected_entry_, Address search_min_offset_)
    : record{ record_ }
    , address{ address_}
    , expected_entry{ expected_entry_ }
    , search_min_offset{ search_min_offset_ }
    {}

  CompactionPendingRecordEntry(const CompactionPendingRecordEntry& from)
    : record{ from.record }
    , address{ from.address }
    , expected_entry{ from.expected_entry }
    , search_min_offset{ from.search_min_offset }
    {}

  // Pointer to the record
  record_t* record;
  // Logical address in the hybrid log
  Address address;
  // Hash bucket entry expect to see when CAS the hash bucket address
  HashBucketEntry expected_entry;
  // Lowest hybrid log address to check when traversing hash chain
  Address search_min_offset;
};


/// Exists context used by compaction algorithm.
template <class K, class V>
class CompactionExists : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<K, V> record_t;

  /// Constructs and returns a context given a pointer to a record.
  CompactionExists(record_t* record, Address record_address,
          void* records_info, void* records_queue)
   : record_{ record }
   , address_{ record_address }
   , records_info_{ records_info }
   , records_queue_{ records_queue }
  {}

  /// Copy constructor -- required for when the Exists operation goes async
  CompactionExists(const CompactionExists& from)
   : record_{ from.record_ }
   , address_{ from.address_ }
   , records_info_{ from.records_info_ }
   , records_queue_{ from.records_queue_ }
  {}

  /// Accessor for the key. Invoked from within FASTER.
  inline const key_t& key() const {
    return record_->key();
  }
  inline Address record_address() const {
    return address_;
  }
  inline void* records_info() const {
    return records_info_;
  }
  inline void* records_queue() const {
    return records_queue_;
  }

 protected:
  /// Copies this context into a passed-in pointer if the operation goes
  /// asynchronous inside FASTER.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  /// Pointer to the record
  record_t* record_;
  /// The address of the record that was read
  Address address_;
  /// Pointer to the records info map (stored in Compact method)
  void* records_info_;
  /// Pointer to the pedning records queue (stored in Compact method)
  void* records_queue_;
};

/// Copy to tail context used by compaction algorithm.
template <class K, class V>
class CompactionCopyToTailContext : public CopyToTailContextBase<K> {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;

  /// Constructs and returns a context given a pointer to a record.
  CompactionCopyToTailContext(record_t* record, const HashBucketEntry& expected_entry, void* dest_store)
   : CopyToTailContextBase<K>(expected_entry, dest_store)
   , record_{ record }
  {}
  /// Copy constructor deleted; copy to tail request doesn't go async
  CompactionCopyToTailContext(const CompactionCopyToTailContext& from) = delete;

  /// Accessor for the key
  inline const key_t& key() const final {
    return record_->key();
  }
  inline uint32_t key_size() const final {
    return record_->key().size();
  }
  inline KeyHash get_key_hash() const final {
    return record_->key().GetHash();
  }
  inline bool is_key_equal(const key_t& other) const final {
    return record_->key() == other;
  }
  inline uint32_t value_size() const final {
    return record_->value().size();
  }
  inline bool is_tombstone() const final {
    return record_->header.tombstone;
  }
  inline bool copy_at(void* dest, uint32_t alloc_size) const final {
    if (alloc_size != record_->size()) {
      return false;
    }
    memcpy(dest, record_, alloc_size);
    return true;
  }

 private:
  record_t* record_;
};


///////////////////////////////////////////////////////////
/// [OLD] Methods used by the old compaction method
///////////////////////////////////////////////////////////

/// Upsert context used by FASTER's compaction algorithm.
///
/// The following are template arguments.
///    K: The type on the key of each record.
///    V: The type on the value stored inside FASTER.
template <class K, class V>
class CompactionUpsert : public IAsyncContext {
 public:
  // Typedefs on the key and value required internally by FASTER.
  typedef K key_t;
  typedef V value_t;

  // Type signature on the record. Required by the constructor.
  typedef Record<K, V> record_t;

  /// Constructs and returns a context given a pointer to a record.
  CompactionUpsert(record_t* record)
   : key_(record->key())
   , value_(record->value())
  {}

  CompactionUpsert(key_t key, value_t value)
   : key_(key)
   , value_(value)
  {}

  /// Copy constructor. Required for when an Upsert operation goes async
  /// inside FASTER.
  CompactionUpsert(const CompactionUpsert& from)
   : key_(from.key_)
   , value_(from.value_)
  {}

  /// Accessor for the key. Invoked from within FASTER.
  inline const K& key() const {
    return key_;
  }

  /// Returns the size of the value. Invoked from within FASTER when creating
  /// a new key-value pair (because the key did not map to a value to begin
  /// with).
  inline static constexpr uint32_t value_size() {
    return V::size();
  }

  /// Stores this context's value into a passed in reference. This is
  /// typically invoked from within FASTER when a new record corresponding
  /// to the key-value pair is created at the tail of the hybrid log.
  inline void Put(V& val) {
    new(&val) V(value_);
  }

  /// Atomically stores this context's value into a passed in reference. This
  /// is typically invoked from within FASTER when performing an Upsert on a
  /// key-value pair in the HybridLog's mutable region.
  inline bool PutAtomic(V& val) {
    new(&val) V(value_);
    return true;
  }

 protected:
  /// Copies this context into a passed-in pointer if the operation goes
  /// asynchronous inside FASTER.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  /// The key the upsert must be performed against.
  K key_;

  /// The value that the key should map to after the Upsert operation.
  V value_;
};

/// Delete context used by FASTER's compaction algorithm.
///
/// The following are template arguments.
///    K: The type on the key of each record.
///    V: The type on the value stored inside FASTER.
template <class K, class V>
class CompactionDelete : public IAsyncContext {
 public:
  // Typedefs on the key and value required internally by FASTER.
  typedef K key_t;
  typedef V value_t;

  // Type signature on the record. Required by the constructor.
  typedef Record<K, V> record_t;

  /// Constructs and returns a context given a pointer to a record.
  CompactionDelete(record_t* record)
   : key_(record->key())
  {}

  /// Copy constructor. Required for when the operation goes async
  /// inside FASTER.
  CompactionDelete(const CompactionDelete& from)
   : key_(from.key_)
  {}

  /// Accessor for the key. Invoked from within FASTER.
  inline const K& key() const {
    return key_;
  }

  /// Returns the size of the value. Invoked from within FASTER when creating
  /// a new key-value pair (because the key did not map to a value to begin
  /// with).
  inline static constexpr uint32_t value_size() {
    return V::size();
  }

 protected:
  /// Copies this context into a passed-in pointer if the operation goes
  /// asynchronous inside FASTER.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  /// The key the delete must be performed against.
  K key_;
};

} // namespace core
} // namespace FASTER
