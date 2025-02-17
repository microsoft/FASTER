// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "async.h"
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


template<class F>
struct ConcurrentCompactionThreadsContext {
 public:
  ConcurrentCompactionThreadsContext()
    : iter{ nullptr }
    , active_threads{ 0 }
    , pages_available{ false }
    , to_other_store{ false } {
  }
  // non-copyable
  ConcurrentCompactionThreadsContext(const ConcurrentCompactionThreadsContext&) = delete;

  void Initialize(ConcurrentLogPageIterator<F>* iter_, size_t n_threads_, bool to_other_store_) {
    iter = iter_;
    to_other_store = to_other_store_;

    thread_finished.clear();
    for (size_t idx = 0; idx < n_threads_; ++idx) {
      thread_finished.emplace_back(false);
    }
    active_threads.store(0);
    pages_available.store(true);
  }

  void Reset() {
    iter = nullptr;
    thread_finished.clear();
    active_threads.store(0);
    pages_available.store(0);
    to_other_store = false;
  }

  ConcurrentLogPageIterator<F>* iter;
  std::deque<std::atomic<bool>> thread_finished;
  std::atomic<uint16_t> active_threads;
  std::atomic<bool> pages_available;
  bool to_other_store;
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
    : address_{ record_address }
    , pending_records_{ pending_records } {
    // Allocate memory to store this record
    record_ = std::make_unique<uint8_t[]>(record->size());
    memcpy(record_.get(), record, record->size());
  }

  /// Copy constructor -- required for when operation goes async
  CompactionConditionalInsertContext(CompactionConditionalInsertContext& from)
    : record_{ from.record_.get() }
    , address_{ from.address_ }
    , pending_records_{ from.pending_records_ } {
      from.record_.release();
  }

  /// Invoked from within FASTER.
  inline const key_t& key() const {
    return record()->key();
  }
  inline uint32_t key_size() const {
    return key().size();
  }
  inline KeyHash get_key_hash() const {
    return key().GetHash();
  }
  inline bool is_key_equal(const key_t& other) const {
    return key() == other;
  }
  inline uint32_t value_size() const {
    return record()->value().size();
  }
  inline void write_deep_key_at(key_t* dst) const {
    // Copy the already "deep-written" key to the new destination
    memcpy(reinterpret_cast<void*>(dst), &key(), key().size());
  }
  inline bool is_tombstone() const {
    return record()->header.tombstone;
  }
  inline Address orig_hlog_tail_address() const {
    // During compaction, both min_search_address & latest hash index address
    // *cannot* !=  Address::kInvalidAddress
    throw std::runtime_error{
      "CompactionConditionalInsertContext.orig_hlog_tail_address() called" };
  }
  inline bool Insert(void* dest, uint32_t alloc_size) const {
    if (alloc_size != record()->size()) {
      return false;
    }
    memcpy(dest, record(), alloc_size);
    return true;
  }
  inline void Put(void* rec) {
    // Manually copy value contents to new destination
    record_t* dest = reinterpret_cast<record_t*>(rec);
    memcpy(reinterpret_cast<void*>(&dest->value()),
           reinterpret_cast<void*>(&record()->value()), value_size());
  }
  inline bool PutAtomic(void *rec) {
    // Cannot guarantee atomic upsert
    return false; // enforce fallback to RCU
  }

  /// Invoked from within callback
  // Retrieve the correct entry in records_info data structure
  inline Address record_address() const {
    return address_;
  }
  // Keep track of active/pending requests
  inline void* records_info() const {
    return pending_records_;
  }

 protected:
  inline record_t* record() const {
    return reinterpret_cast<record_t*>(record_.get());
  }
  /// Copies this context into a passed-in pointer if the operation goes
  /// asynchronous inside FASTER.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  /// Pointer to the record
  std::unique_ptr<uint8_t[]> record_;
  /// The address of the record that was read
  Address address_;
  /// Pointer to the records info map (stored in Compact method)
  void* pending_records_;
};

class CompactionCheckpointContext : public IAsyncContext {
 public:
  CompactionCheckpointContext()
    : index_persisted_status{ Status::Corruption }
    , hlog_persisted_status{ Status::Corruption }
    , hlog_num_threads_persisted{ 0 }
    , ready_for_truncation{ false } {
      for (size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
        hlog_threads_persisted[idx].store(false);
      }
  }
  CompactionCheckpointContext(const CompactionCheckpointContext& from)
    : index_persisted_status{ from.index_persisted_status.load() }
    , hlog_persisted_status{ from.hlog_persisted_status.load() }
    , hlog_num_threads_persisted{ from.hlog_num_threads_persisted.load() }
    , ready_for_truncation{ from.ready_for_truncation.load() } {
      for (size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
        hlog_threads_persisted[idx] = from.hlog_threads_persisted[idx].load();
      }
  }

 protected:
  /// Copies this context into a passed-in pointer if the operation goes
  /// asynchronous inside FASTER.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 public:
  // index callback
  std::atomic<Status> index_persisted_status;
  // hlog callback
  std::atomic<Status> hlog_persisted_status;
  std::atomic<bool> hlog_threads_persisted[Thread::kMaxNumThreads];
  std::atomic<size_t> hlog_num_threads_persisted;
  //
  std::atomic<bool> ready_for_truncation;
};

class CompactionShiftBeginAddressContext : public IAsyncContext {
 public:
  CompactionShiftBeginAddressContext()
    : log_truncated{ false }
    , gc_completed{ false } {
  }
  CompactionShiftBeginAddressContext(const CompactionShiftBeginAddressContext& from)
    : log_truncated{ from.log_truncated.load() }
    , gc_completed{ from.gc_completed.load() } {
  }

 protected:
  /// Copies this context into a passed-in pointer if the operation goes
  /// asynchronous inside FASTER.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 public:
  std::atomic<bool> log_truncated;
  std::atomic<bool> gc_completed;
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
