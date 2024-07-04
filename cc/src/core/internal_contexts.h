// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <deque>
#include <string>
#include <unordered_map>

#include "address.h"
#include "guid.h"
#include "key_hash.h"
#include "native_buffer_pool.h"
#include "record.h"
#include "state_transitions.h"
#include "thread.h"

#include "hc_internal_contexts.h"

#include "../index/hash_bucket.h"

using namespace FASTER::index;

namespace FASTER {
namespace core {

/// Forward class declaration
class AsyncIOContext;
class AsyncIndexIOContext;

/// Internal contexts, used by FASTER.

enum class OperationType : uint8_t {
  Read,
  RMW,
  Upsert,
  Insert,
  Delete,
  ConditionalInsert,
};

enum class OperationStatus : uint8_t {
  SUCCESS,
  NOT_FOUND,
  RETRY_NOW,
  RETRY_LATER,
  RECORD_ON_DISK,
  INDEX_ENTRY_ON_DISK,
  ASYNC_TO_COLD_STORE,
  SUCCESS_UNMARK,
  NOT_FOUND_UNMARK,
  CPR_SHIFT_DETECTED,
  ABORTED,
  ABORTED_UNMARK,
};

enum class IndexOperationType : uint8_t {
  None,
  Retrieve,
  Update,
};

/// Internal FASTER context.
template <class K>
class PendingContext : public IAsyncContext {
 public:
  typedef K key_t;

 protected:
  PendingContext(OperationType type_, IAsyncContext& caller_context_,
                 AsyncCallback caller_callback_)
    : type{ type_ }
    , caller_context{ &caller_context_ }
    , caller_callback{ caller_callback_ }
    , version{ UINT32_MAX }
    , phase{ Phase::INVALID }
    , result{ Status::Pending }
    , address{ Address::kInvalidAddress }
    , index_op_type{ IndexOperationType::None }
    , index_op_result{ Status::Corruption }
    , entry{ HashBucketEntry::kInvalidEntry }
    , atomic_entry{ nullptr }
    , skip_read_cache{ false } {
  #ifdef STATISTICS
      num_iops = 0;
      num_record_invalidations = 0;
  #endif
  }

 public:
  /// The deep-copy constructor.
  PendingContext(const PendingContext& other, IAsyncContext* caller_context_)
    : type{ other.type }
    , caller_context{ caller_context_ }
    , caller_callback{ other.caller_callback }
    , version{ other.version }
    , phase{ other.phase }
    , result{ other.result }
    , address{ other.address }
    , index_op_type{ other.index_op_type }
    , index_op_result{ other.index_op_result }
    , entry{ other.entry }
    , atomic_entry{ other.atomic_entry }
    , skip_read_cache{ other.skip_read_cache } {
  #ifdef STATISTICS
      num_iops = other.num_iops;
      num_record_invalidations = other.num_record_invalidations;
  #endif
  }

 public:
  // Store the current phase and thread version
  inline void try_stamp_request(Phase phase_, uint32_t version_) {
    if (phase == Phase::INVALID || version == UINT32_MAX) {
      assert(phase == Phase::INVALID && version == UINT32_MAX);
      stamp_request(phase_, version_);
    }
  }

  inline void stamp_request(Phase phase_, uint32_t version_) {
    phase = phase_;
    version = version_;
  }

  /// Go async
  inline void go_async(Address address_) {
    address = address_;
  }

  inline void clear_index_op() {
    index_op_type = IndexOperationType::None;
    index_op_result = Status::Corruption;
  }

  inline void set_index_entry(HashBucketEntry entry_, AtomicHashBucketEntry* atomic_entry_) {
    entry = entry_;
    atomic_entry = atomic_entry_;
  }

  virtual uint32_t key_size() const = 0;
  virtual void write_deep_key_at(key_t* dst) const = 0;
  virtual KeyHash get_key_hash() const = 0;
  virtual bool is_key_equal(const key_t& other) const = 0;

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
  /// Result of operation.
  Status result;
  /// Address of the record being read or modified.
  Address address;
  // TODO: encapsulate the following into a single class
  /// Type of index operation that went pending
  IndexOperationType index_op_type;
  // Result of index operation
  Status index_op_result;
  /// Hash table entry that (indirectly) leads to the record being read or modified.
  HashBucketEntry entry;
  /// Pointer to the atomic hash bucket entry (hot index only; nullptr for cold index)
  AtomicHashBucketEntry* atomic_entry;
  /// Use (or skip) reading from read cache
  bool skip_read_cache;

#ifdef STATISTICS
  uint32_t num_iops;
  uint32_t num_record_invalidations;
#endif
};

/// FASTER's internal Read() context.

/// An internal Read() context that has gone async and lost its type information.
template <class K>
class AsyncPendingReadContext : public PendingContext<K> {
 public:
  typedef K key_t;
 protected:
  AsyncPendingReadContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_,
                          bool abort_if_tombstone_, Address min_search_offset_, uint64_t num_compaction_truncations_)
    : PendingContext<key_t>(OperationType::Read, caller_context_, caller_callback_)
    , abort_if_tombstone{ abort_if_tombstone_ }
    , min_search_offset{ min_search_offset_ }
    , num_compaction_truncations{ num_compaction_truncations_ }
    , expected_hlog_address{ Address::kInvalidAddress } {
  }
  /// The deep copy constructor.
  AsyncPendingReadContext(AsyncPendingReadContext& other, IAsyncContext* caller_context)
    : PendingContext<key_t>(other, caller_context)
    , abort_if_tombstone{ other.abort_if_tombstone }
    , min_search_offset{ other.min_search_offset }
    , num_compaction_truncations{ other.num_compaction_truncations }
    , expected_hlog_address{ other.expected_hlog_address } {
  }
 public:
  virtual void Get(const void* rec) = 0;
  virtual void GetAtomic(const void* rec) = 0;

  /// If true, Read will return ABORT (instead of NOT_FOUND), if record is tombstone
  bool abort_if_tombstone;
  /// Lower address that the Read op should consider
  /// Used when partially retrying Read to address Read-Compaction race condition
  Address min_search_offset;
  /// Used to infer whether a log truncation took place, after compacting entries to the log tail
  uint64_t num_compaction_truncations;
  /// Keeps latest hlog address, as found by hash index entry (after skipping read cache)
  Address expected_hlog_address;
};

/// A synchronous Read() context preserves its type information.
template <class RC, bool IsHCContext = is_hc_read_context<RC>>
class PendingReadContext : public AsyncPendingReadContext<typename RC::key_t> {
 public:
  typedef RC read_context_t;
  typedef typename read_context_t::key_t key_t;
  typedef typename read_context_t::value_t value_t;
  typedef Record<key_t, value_t> record_t;

  PendingReadContext(read_context_t& caller_context_, AsyncCallback caller_callback_,
                      bool abort_if_tombstone_, Address min_search_offset_, uint64_t num_compaction_truncations_)
    : AsyncPendingReadContext<key_t>(caller_context_, caller_callback_, abort_if_tombstone_,
                                      min_search_offset_, num_compaction_truncations_) {
  }
  /// The deep copy constructor.
  PendingReadContext(PendingReadContext& other, IAsyncContext* caller_context_)
    : AsyncPendingReadContext<key_t>(other, caller_context_) {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext<key_t>::caller_context,
                                            context_copy);
  }
 private:
  inline const read_context_t& read_context() const {
    return *static_cast<const read_context_t*>(PendingContext<key_t>::caller_context);
  }
  inline read_context_t& read_context() {
    return *static_cast<read_context_t*>(PendingContext<key_t>::caller_context);
  }
 public:
  /// Accessors.
  inline uint32_t key_size() const final {
    return hc_context_helper<IsHCContext>::key_size(read_context());
  }
  inline void write_deep_key_at(key_t* dst) const final {
    // this should never be called
    assert(false);
  }
  inline KeyHash get_key_hash() const final {
    return hc_context_helper<IsHCContext>::get_key_hash(read_context());
  }
  inline bool is_key_equal(const key_t& other) const final {
    return hc_context_helper<IsHCContext>::is_key_equal(read_context(), other);
  }
  inline void Get(const void* rec) final {
    hc_context_helper<IsHCContext>::template Get<read_context_t, record_t>(read_context(), rec);
  }
  inline void GetAtomic(const void* rec) final {
    hc_context_helper<IsHCContext>::template GetAtomic<read_context_t, record_t>(read_context(), rec);
  }
};

/// FASTER's internal Upsert() context.

/// An internal Upsert() context that has gone async and lost its type information.
template <class K>
class AsyncPendingUpsertContext : public PendingContext<K> {
 public:
  typedef K key_t;
 protected:
  AsyncPendingUpsertContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_)
    : PendingContext<key_t>(OperationType::Upsert, caller_context_, caller_callback_) {
  }
  /// The deep copy constructor.
  AsyncPendingUpsertContext(AsyncPendingUpsertContext& other, IAsyncContext* caller_context)
    : PendingContext<key_t>(other, caller_context) {
  }
 public:
  virtual void Put(void* rec) = 0;
  virtual bool PutAtomic(void* rec) = 0;
  virtual uint32_t value_size() const = 0;
};

/// A synchronous Upsert() context preserves its type information.
template <class UC, bool IsHCContext = false>
class PendingUpsertContext : public AsyncPendingUpsertContext<typename UC::key_t> {
 public:
  typedef UC upsert_context_t;
  typedef typename upsert_context_t::key_t key_t;
  typedef typename upsert_context_t::value_t value_t;
  typedef Record<key_t, value_t> record_t;

  PendingUpsertContext(upsert_context_t& caller_context_, AsyncCallback caller_callback_)
    : AsyncPendingUpsertContext<key_t>(caller_context_, caller_callback_) {
  }
  /// The deep copy constructor.
  PendingUpsertContext(PendingUpsertContext& other, IAsyncContext* caller_context_)
    : AsyncPendingUpsertContext<key_t>(other, caller_context_) {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext<key_t>::caller_context,
                                            context_copy);
  }
 private:
  inline const upsert_context_t& upsert_context() const {
    return *static_cast<const upsert_context_t*>(PendingContext<key_t>::caller_context);
  }
  inline upsert_context_t& upsert_context() {
    return *static_cast<upsert_context_t*>(PendingContext<key_t>::caller_context);
  }

 public:
  /// Accessors.
  inline uint32_t key_size() const final {
    return hc_context_helper<IsHCContext>::key_size(upsert_context());
  }
  inline void write_deep_key_at(key_t* dst) const final {
    hc_context_helper<IsHCContext>::write_deep_key_at(upsert_context(), dst);
  }
  inline KeyHash get_key_hash() const final {
    return hc_context_helper<IsHCContext>::get_key_hash(upsert_context());
  }
  inline bool is_key_equal(const key_t& other) const final {
    return hc_context_helper<IsHCContext>::is_key_equal(upsert_context(), other);
  }
  inline void Put(void* rec) final {
    hc_context_helper<IsHCContext>::template Put<upsert_context_t, record_t>(upsert_context(), rec);
  }
  inline bool PutAtomic(void* rec) final {
    return hc_context_helper<IsHCContext>::template PutAtomic<upsert_context_t, record_t>(upsert_context(), rec);
  }
  inline constexpr uint32_t value_size() const final {
    return upsert_context().value_size();
  }
};

/// FASTER's internal Rmw() context.
/// An internal Rmw() context that has gone async and lost its type information.
template <class K>
class AsyncPendingRmwContext : public PendingContext<K> {
 public:
  typedef K key_t;
 protected:
  AsyncPendingRmwContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_, bool create_if_not_exists_)
    : PendingContext<key_t>(OperationType::RMW, caller_context_, caller_callback_)
    , create_if_not_exists{ create_if_not_exists_ }
    , expected_hlog_address{ Address::kInvalidAddress } {
  }
  /// The deep copy constructor.
  AsyncPendingRmwContext(AsyncPendingRmwContext& other, IAsyncContext* caller_context)
    : PendingContext<key_t>(other, caller_context)
    , create_if_not_exists{ other.create_if_not_exists }
    , expected_hlog_address{ other.expected_hlog_address } {
  }
 public:
  /// Set initial value.
  virtual void RmwInitial(void* rec) = 0;
  /// RCU.
  virtual void RmwCopy(const void* old_rec, void* rec) = 0;
  /// in-place update.
  virtual bool RmwAtomic(void* rec) = 0;
  /// Get value size for initial value or in-place update
  virtual uint32_t value_size() const = 0;
  /// Get value size for RCU
  virtual uint32_t value_size(const void* old_rec) const = 0;

  /// If false, it will return NOT_FOUND instead of creating a new record
  /// NOTE: false when doing HC-RMW Copy from cold log operation; true otherwise
  bool create_if_not_exists;
  /// Keeps latest hlog address, as found by hash index entry (after skipping read cache)
  Address expected_hlog_address;
};

/// A synchronous Rmw() context preserves its type information.
template <class MC, bool IsHCContext = is_hc_rmw_context<MC>>
class PendingRmwContext : public AsyncPendingRmwContext<typename MC::key_t> {
 public:
  typedef MC rmw_context_t;
  typedef typename rmw_context_t::key_t key_t;
  typedef typename rmw_context_t::value_t value_t;
  typedef Record<key_t, value_t> record_t;

  PendingRmwContext(rmw_context_t& caller_context_, AsyncCallback caller_callback_, bool create_if_not_exists_)
    : AsyncPendingRmwContext<key_t>(caller_context_, caller_callback_, create_if_not_exists_) {
  }
  /// The deep copy constructor.
  PendingRmwContext(PendingRmwContext& other, IAsyncContext* caller_context_)
    : AsyncPendingRmwContext<key_t>(other, caller_context_) {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext<key_t>::caller_context,
                                            context_copy);
  }
 private:
  const rmw_context_t& rmw_context() const {
    return *static_cast<const rmw_context_t*>(PendingContext<key_t>::caller_context);
  }
  rmw_context_t& rmw_context() {
    return *static_cast<rmw_context_t*>(PendingContext<key_t>::caller_context);
  }
 public:
  /// Accessors.
  inline uint32_t key_size() const final {
    return hc_context_helper<IsHCContext>::key_size(rmw_context());
  }
  inline void write_deep_key_at(key_t* dst) const final {
    hc_context_helper<IsHCContext>::write_deep_key_at(rmw_context(), dst);
  }
  inline KeyHash get_key_hash() const final {
    return hc_context_helper<IsHCContext>::get_key_hash(rmw_context());
  }
  inline bool is_key_equal(const key_t& other) const final {
    return hc_context_helper<IsHCContext>::is_key_equal(rmw_context(), other);
  }
  /// Set initial value.
  inline void RmwInitial(void* rec) final {
    record_t* record = reinterpret_cast<record_t*>(rec);
    rmw_context().RmwInitial(record->value());
  }
  /// RCU.
  inline void RmwCopy(const void* old_rec, void* rec) final {
    const record_t* old_record = reinterpret_cast<const record_t*>(old_rec);
    record_t* record = reinterpret_cast<record_t*>(rec);
    rmw_context().RmwCopy(old_record->value(), record->value());
  }
  /// in-place update.
  inline bool RmwAtomic(void* rec) final {
    record_t* record = reinterpret_cast<record_t*>(rec);
    return rmw_context().RmwAtomic(record->value());
  }
  /// Get value size for initial value or in-place update
  inline constexpr uint32_t value_size() const final {
    return rmw_context().value_size();
  }
  /// Get value size for RCU
  inline constexpr uint32_t value_size(const void* old_rec) const final {
    const record_t* old_record = reinterpret_cast<const record_t*>(old_rec);
    return rmw_context().value_size(old_record->value());
  }
};

/// FASTER's internal Delete() context.

/// An internal Delete() context that has gone async and lost its type information.
template <class K>
class AsyncPendingDeleteContext : public PendingContext<K> {
 public:
  typedef K key_t;
 protected:
  AsyncPendingDeleteContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_)
    : PendingContext<key_t>(OperationType::Delete, caller_context_, caller_callback_) {
  }
  /// The deep copy constructor.
  AsyncPendingDeleteContext(AsyncPendingDeleteContext& other, IAsyncContext* caller_context)
    : PendingContext<key_t>(other, caller_context) {
  }
 public:
  /// Get value size for initial value
  virtual uint32_t value_size() const = 0;
};

/// A synchronous Delete() context preserves its type information.
template <class MC, bool IsHCContext = false>
class PendingDeleteContext : public AsyncPendingDeleteContext<typename MC::key_t> {
 public:
  typedef MC delete_context_t;
  typedef typename delete_context_t::key_t key_t;
  typedef typename delete_context_t::value_t value_t;
  typedef Record<key_t, value_t> record_t;

  PendingDeleteContext(delete_context_t& caller_context_, AsyncCallback caller_callback_)
    : AsyncPendingDeleteContext<key_t>(caller_context_, caller_callback_) {
  }
  /// The deep copy constructor.
  PendingDeleteContext(PendingDeleteContext& other, IAsyncContext* caller_context_)
    : AsyncPendingDeleteContext<key_t>(other, caller_context_) {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext<key_t>::caller_context,
                                            context_copy);
  }
 private:
  const delete_context_t& delete_context() const {
    return *static_cast<const delete_context_t*>(PendingContext<key_t>::caller_context);
  }
  delete_context_t& delete_context() {
    return *static_cast<delete_context_t*>(PendingContext<key_t>::caller_context);
  }
 public:
  /// Accessors.
  inline uint32_t key_size() const final {
    return hc_context_helper<IsHCContext>::key_size(delete_context());
  }
  inline void write_deep_key_at(key_t* dst) const final {
    hc_context_helper<IsHCContext>::write_deep_key_at(delete_context(), dst);
  }
  inline KeyHash get_key_hash() const final {
    return hc_context_helper<IsHCContext>::get_key_hash(delete_context());
  }
  inline bool is_key_equal(const key_t& other) const final {
    return hc_context_helper<IsHCContext>::is_key_equal(delete_context(), other);
  }
  /// Get value size for initial value
  inline uint32_t value_size() const final {
    return delete_context().value_size();
  }
};

// FASTER's internal ConditionalInsert

/// An internal ConditionalInsert() context that has gone async and lost its type information.
template <class K>
class AsyncPendingConditionalInsertContext : public PendingContext<K> {
 public:
  typedef K key_t;
  typedef AsyncPendingUpsertContext<key_t> async_pending_upsert_context_t;
  typedef AsyncPendingDeleteContext<key_t> async_pending_delete_context_t;
 protected:
  AsyncPendingConditionalInsertContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_,
                                        Address min_search_offset_, bool to_other_store_)
    : PendingContext<key_t>(OperationType::ConditionalInsert, caller_context_, caller_callback_)
    , min_search_offset{ min_search_offset_ }
    , orig_min_search_offset{ min_search_offset_ }
    , expected_hlog_address{ Address::kInvalidAddress }
    , to_other_store{ to_other_store_ }
    , io_context{ nullptr } {
  }
  /// The deep copy constructor.
  AsyncPendingConditionalInsertContext(AsyncPendingConditionalInsertContext& other, IAsyncContext* caller_context)
    : PendingContext<key_t>(other, caller_context)
    , min_search_offset{ other.min_search_offset }
    , orig_min_search_offset{ other.orig_min_search_offset }
    , expected_hlog_address{ other.expected_hlog_address }
    , to_other_store{ other.to_other_store }
    , io_context{ other.io_context } {
  }

 public:
  virtual uint32_t value_size() const = 0;
  virtual bool is_tombstone() const = 0;
  virtual Address orig_hlog_tail_address() const = 0;

  // Called when writing to same store
  virtual bool Insert(void* dest, uint32_t alloc_size) const = 0;

  // Used to provide a context for InternalUpsert/InternalDelete
  // methods used when performing hot-cold compaction
  virtual async_pending_upsert_context_t* ConvertToUpsertContext() = 0;
  virtual async_pending_delete_context_t* ConvertToDeleteContext() = 0;

 public:
  Address min_search_offset;
  Address orig_min_search_offset;
  /// Keeps latest hlog address, as found by hash index entry (after skipping read cache)
  Address expected_hlog_address;
  bool to_other_store;

  AsyncIOContext* io_context;
};

/// A synchronous ConditionalInsert() context preserves its type information.
template<class CIC>
class PendingConditionalInsertContext : public AsyncPendingConditionalInsertContext<typename CIC::key_t> {
 public:
  typedef CIC conditional_insert_context_t;
  typedef typename conditional_insert_context_t::key_t key_t;
  typedef typename conditional_insert_context_t::value_t value_t;
  typedef Record<key_t, value_t> record_t;

  // Upsert context definitions
  typedef AsyncPendingUpsertContext<key_t> async_pending_upsert_context_t;
  typedef PendingUpsertContext<CIC, true> pending_upsert_context_t;
  // Delete context definitions
  typedef AsyncPendingDeleteContext<key_t> async_pending_delete_context_t;
  typedef PendingDeleteContext<CIC, true> pending_delete_context_t;

  PendingConditionalInsertContext(conditional_insert_context_t& caller_context_, AsyncCallback caller_callback_,
                                  Address min_search_offset_, bool to_other_store_)
    : AsyncPendingConditionalInsertContext<key_t>(caller_context_, caller_callback_,
                                                  min_search_offset_, to_other_store_) {
  }
  /// The deep copy constructor.
  PendingConditionalInsertContext(PendingConditionalInsertContext& other, IAsyncContext* caller_context_)
    : AsyncPendingConditionalInsertContext<key_t>(other, caller_context_) {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext<key_t>::caller_context,
                                            context_copy);
  }
 private:
  const conditional_insert_context_t& conditional_insert_context() const {
    return *static_cast<const conditional_insert_context_t*>(PendingContext<key_t>::caller_context);
  }
  conditional_insert_context_t& conditional_insert_context() {
    return *static_cast<conditional_insert_context_t*>(PendingContext<key_t>::caller_context);
  }
 public:
  /// Accessors.
  inline uint32_t key_size() const final {
    return hc_context_helper<true>::key_size(conditional_insert_context());
  }
  inline void write_deep_key_at(key_t* dst) const final {
    return hc_context_helper<true>::write_deep_key_at(conditional_insert_context(), dst);
  }
  inline KeyHash get_key_hash() const final {
    return hc_context_helper<true>::get_key_hash(conditional_insert_context());
  }
  inline bool is_key_equal(const key_t& other) const final {
    return hc_context_helper<true>::is_key_equal(conditional_insert_context(), other);
  }
  inline uint32_t value_size() const final {
    return conditional_insert_context().value_size();
  }

  // ConditionalInsert contexts contains these additional methods
  inline bool is_tombstone() const final {
    return conditional_insert_context().is_tombstone();
  }
  inline Address orig_hlog_tail_address() const final {
    return conditional_insert_context().orig_hlog_tail_address();
  }
  inline bool Insert(void* dest, uint32_t alloc_size) const final {
    return conditional_insert_context().Insert(dest, alloc_size);
  }

  inline async_pending_upsert_context_t* ConvertToUpsertContext() final {
    pending_upsert_context_t context{ conditional_insert_context(), PendingContext<key_t>::caller_callback };
    // Deep copy context to heap
    IAsyncContext* context_copy;
    Status copy_status = context.DeepCopy(context_copy);
    assert(copy_status == Status::Ok);
    return static_cast<async_pending_upsert_context_t*>(context_copy);
  }

  inline async_pending_delete_context_t* ConvertToDeleteContext() final {
    pending_delete_context_t context{ conditional_insert_context(), PendingContext<key_t>::caller_callback };
    // Deep copy context to heap
    IAsyncContext* context_copy;
    Status copy_status = context.DeepCopy(context_copy);
    assert(copy_status == Status::Ok);
    return static_cast<async_pending_delete_context_t*>(context_copy);
  }
};

/// Context used for sync hash index operations
template <class K, class V>
class IndexContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;

  /// Constructs and returns a context given a pointer to a record.
  IndexContext(record_t* record)
    : record_{ record }
    , entry{ HashBucketEntry::kInvalidEntry }
    , atomic_entry{ nullptr } {
  }
  /// Copy constructor deleted -- op does not go async
  IndexContext(const IndexContext& from)
    : record_{ from.record_ }
    , entry{ from.entry }
    , atomic_entry{ from.atomic_entry } {
  }

  inline const key_t& key() const {
    return record_->key();
  }
  inline KeyHash get_key_hash() const {
    return key().GetHash();
  }

  inline void set_index_entry(HashBucketEntry entry_, AtomicHashBucketEntry* atomic_entry_) {
    entry = entry_;
    atomic_entry = atomic_entry_;
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
 public:
  ///
  HashBucketEntry entry;
  AtomicHashBucketEntry* atomic_entry;
};

/// Per-thread execution context. (Just the stuff that's checkpointed to disk.)
struct PersistentExecContext {
  PersistentExecContext()
    : serial_num{ 0 }
    , version{ 0 }
    , guid{} {
  }

  void Initialize(uint32_t version_, const Guid& guid_, uint64_t serial_num_) {
    serial_num = serial_num_;
    version = version_;
    guid = guid_;
  }

  uint64_t serial_num;
  uint32_t version;
  /// Unique identifier for this session.
  Guid guid;
};
static_assert(sizeof(PersistentExecContext) == 32, "sizeof(PersistentExecContext) != 32");

/// Per-thread execution context. (Also includes state kept in-memory-only.)
struct ExecutionContext : public PersistentExecContext {
  /// Default constructor.
  ExecutionContext()
    : phase{ Phase::INVALID }
    , io_id{ 0 } {
  }

  void Initialize(Phase phase_, uint32_t version_, const Guid& guid_, uint64_t serial_num_) {
    assert(retry_requests.empty());
    assert(pending_ios.empty());
    assert(io_responses.empty());
    assert(index_io_responses.empty());

    PersistentExecContext::Initialize(version_, guid_, serial_num_);
    phase = phase_;
    retry_requests.clear();
    io_id = 0;
    pending_ios.clear();
    io_responses.clear();
    index_io_responses.clear();
  }

  Phase phase;

  /// Retry request contexts are stored inside the deque.
  std::deque<IAsyncContext*> retry_requests;
  /// Assign a unique ID to every I/O request.
  /// NOTE: this includes index-related requests from e.g., cold index
  uint64_t io_id;
  /// For each pending I/O, maps io_id to the hash of the key being retrieved.
  std::unordered_map<uint64_t, KeyHash> pending_ios;

  /// The I/O completion thread hands the PendingContext back to the thread that issued the
  /// request.
  concurrent_queue<AsyncIOContext*> io_responses;

  /// The I/O completion thread hands the PendingContext back to the thread that issued the
  /// index-related request.
  concurrent_queue<AsyncIndexIOContext*> index_io_responses;
};
//static_assert(sizeof(ExecutionContext) == 216, "sizeof(ExecutionContext) != 216");

}
} // namespace FASTER::core
