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

/// ConditionalInsert context used by compaction algorithm.
/// NOTE: This context is used in both single-log & hot-cold cases
template <class K, class V>
class CompactionConditionalInsertContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;

  /// Constructs and returns a context given a pointer to a record.
  CompactionConditionalInsertContext(record_t* record, Address record_address,
                                      void* pending_records)
   : record_{ record }
   , address_{ record_address }
   , pending_records_{ pending_records }
  {}

  /// Copy constructor -- required for when operation goes async
  CompactionConditionalInsertContext(const CompactionConditionalInsertContext& from)
   : record_{ from.record_ }
   , address_{ from.address_ }
   , pending_records_{ from.pending_records_ }
  {}

  /// Invoked from within FASTER.
  inline const key_t& key() const {
    return record_->key();
  }
  inline uint32_t value_size() const {
    return record_->value().size();
  }
  inline void write_deep_key_at(key_t* dst) const {
    // Copy the already "deep-written" key to the new destination
    memcpy(dst, &this->key(), this->key().size());
  }
  inline bool is_tombstone() const {
    return record_->header.tombstone;
  }
  inline bool Insert(void* dest, uint32_t alloc_size) const {
    if (alloc_size != record_->size()) {
      return false;
    }
    memcpy(dest, record_, alloc_size);
    return true;
  }
  inline void Put(void* rec) {
    // Manually copy value contents to new destination
    record_t* dest = reinterpret_cast<record_t*>(rec);
    memcpy(&dest->value(), &record_->value(), value_size());
  }
  inline bool PutAtomic(void *rec) {
    // Cannot guarrantee atomic upsert
    return false; // enforce fallback to RCU
  }

  /// Invoked from within callback
  // Retrieve the correct entry in records_info data strcture
  inline Address record_address() const {
    return address_;
  }
  // Keep track of active/pending requests
  inline void* records_info() const {
    return pending_records_;
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
  void* pending_records_;
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
