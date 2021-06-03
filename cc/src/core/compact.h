// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "core/async.h"

#include "hash_bucket.h"
#include "record.h"

namespace FASTER {
namespace core {

template<class K, class V>
struct CompactionPendingRecordEntry {

  CompactionPendingRecordEntry(Record<K, V> record_, HashBucketEntry expected_entry_)
    : record(record_)
    , expected_entry(expected_entry_)
    {}

  Record<K, V> record;
  HashBucketEntry expected_entry;
};


/// Exists context used by FASTER's compaction algorithm.
///
/// The following are template arguments.
///    K: The type on the key of each record.
///    V: The type on the value stored inside FASTER.
template <class K, class V, class CRI, class CRQ>
class CompactionExists : public IAsyncContext {
 public:
  // Typedefs on the key and value required internally by FASTER.
  typedef K key_t;
  typedef V value_t;

  // Type signature on the record. Required by the constructor.
  typedef Record<K, V> record_t;


  /// Constructs and returns a context given a pointer to a record.
  CompactionExists(record_t* record, Address record_address,
          CRI* records_info, CRQ* records_queue)
   : key_(record->key())
   , value_(record->value())
   , address_(record_address)
   , records_info_(records_info)
   , records_queue_(records_queue)
  {}

  /// Copy constructor. Required for when a Read operation goes async
  /// inside FASTER.
  CompactionExists(const CompactionExists& from)
   : key_(from.key_)
   , value_(from.value_)
   , address_(from.record_address)
   , records_info_(from.records_info)
   , records_queue_(from.records_queue)
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

  inline Address record_address() const {
    return address_;
  }

  inline CRI * records_info() const {
    return records_info_;
  }

  inline CRQ * records_queue() const {
    return records_queue_;
  }

  /// Stores this context's value into the passed in reference. This is
  /// typically invoked from within FASTER when a new record corresponding
  /// to the key-value pair is created at the tail of the hybrid log.
  inline void Get(V& val) {
    new(&val) V(value_);
  }

  inline bool GetAtomic(V& val) {
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
  /// The key the read must be performed against.
  K key_;

  /// The value that the key should map to after the Read operation.
  V value_;

  /// The address of the record that was read
  Address address_;

  /// Pointer to the records info map (stored in Compact method)
  CRI* records_info_;

  /// Pointer to the pedning records queue (stored in Compact method)
  CRQ* records_queue_;
};


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
