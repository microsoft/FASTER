// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "faster.h"
#include "guid.h"
#include "internal_contexts.h"

namespace FASTER {
namespace core {

template <class K>
class HotColdContext : public IAsyncContext {
 public:
  typedef K key_t;

  HotColdContext(void* faster_hc_, IAsyncContext& caller_context_,
                  AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : faster_hc{ faster_hc_ }
    , caller_context{ &caller_context_ }
    , caller_callback{ caller_callback_ }
    , serial_num{ monotonic_serial_num_ }
    {}
  /// No copy constructor.
  HotColdContext(const HotColdContext& other) = delete;
  /// The deep-copy constructor.
  HotColdContext(HotColdContext& other, IAsyncContext* caller_context_)
    : faster_hc{ other.faster_hc }
    , caller_context{ caller_context }
    , caller_callback{ other.caller_callback }
    , serial_num{ serial_num }
    {}

  /// Points to FasterHC
  void* faster_hc;
  /// User context
  IAsyncContext* caller_context;
  /// User-provided callback
  AsyncCallback caller_callback;
  /// Request serial num
  uint64_t serial_num;
};

/// Context that holds user context for Read request
template <class RC>
class HotColdReadContext : public HotColdContext <typename RC::key_t> {
 public:
  typedef RC read_context_t;
  typedef typename read_context_t::key_t key_t;
  typedef typename read_context_t::value_t value_t;

  HotColdReadContext(void* faster_hc_, read_context_t& caller_context_,
                  AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : HotColdContext<key_t>(faster_hc_, caller_context_, caller_callback_, monotonic_serial_num_)
    {}
  /// The deep-copy constructor.
  HotColdReadContext(HotColdReadContext& other_, IAsyncContext* caller_context_)
    : HotColdContext<key_t>(other_, caller_context_)
    {}

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, HotColdContext<key_t>::caller_context,
                                            context_copy);
  }
 public:
  inline const read_context_t& read_context() const {
    return *static_cast<const read_context_t*>(HotColdContext<key_t>::caller_context);
  }
  inline read_context_t& read_context() {
    return *static_cast<read_context_t*>(HotColdContext<key_t>::caller_context);
  }
  /// Propagates calls to caller context
  inline const key_t& key() const {
    return read_context().key();
  }
  inline void Get(const value_t& value) {
    read_context().Get(value);
  }
  inline void GetAtomic(const value_t& value) {
    read_context().GetAtomic(value);
  }
};

template <class K, class V>
class AsyncHotColdRmwContext : public HotColdContext<K> {
 public:
  typedef K key_t;
  typedef V value_t;
 protected:
  AsyncHotColdRmwContext(void* faster_hc_, IAsyncContext& caller_context_, AsyncCallback caller_callback_,
                      uint64_t monotonic_serial_num_, HashBucketEntry& expected_entry_,
                      AsyncCallback hs_retry_if_not_found_callback_, AsyncCallback cs_read_callback_)
    : HotColdContext<key_t>(faster_hc_, caller_context_, caller_callback_, monotonic_serial_num_)
    , expected_entry{ expected_entry_ }
    , hot_store_retry_if_not_found_callback{ hs_retry_if_not_found_callback_ }
    , cold_store_read_callback{ cs_read_callback_ }
  {}
  /// The deep copy constructor.
  AsyncHotColdRmwContext(AsyncHotColdRmwContext& other, IAsyncContext* caller_context)
    : HotColdContext<key_t>(other, caller_context)
    , expected_entry{ other.expected_entry }
    , hot_store_retry_if_not_found_callback{ other.hot_store_retry_if_not_found_callback }
    , cold_store_read_callback{ other.cold_store_read_callback }
  {}
 public:
  /// Set initial value.
  virtual void RmwInitial(value_t& value) = 0;
  /// RCU.
  virtual void RmwCopy(const value_t& old_value, value_t& value) = 0;
  /// in-place update.
  virtual bool RmwAtomic(value_t& value) = 0;
  /// Get value size for initial value or in-place update
  virtual uint32_t value_size() const = 0;
  /// Get value size for RCU
  virtual uint32_t value_size(const value_t& value) const = 0;

  HashBucketEntry expected_entry;
  AsyncCallback cold_store_read_callback;
  AsyncCallback hot_store_retry_if_not_found_callback;
};

template <class MC>
class HotColdRmwContext : public AsyncHotColdRmwContext<typename MC::key_t, typename MC::value_t> {
 public:
  typedef MC rmw_context_t;
  typedef typename rmw_context_t::key_t key_t;
  typedef typename rmw_context_t::value_t value_t;

  HotColdRmwContext(void* faster_hc_, rmw_context_t& caller_context_, AsyncCallback caller_callback_,
                      uint64_t monotonic_serial_num_, HashBucketEntry& expected_entry_,
                      AsyncCallback hs_retry_if_not_found_callback_, AsyncCallback cs_read_callback_)
    : AsyncHotColdRmwContext<key_t, value_t>(faster_hc_, caller_context_, caller_callback_, monotonic_serial_num_, expected_entry_,
                                    hs_retry_if_not_found_callback_, cs_read_callback_)
    {}
  /// The deep-copy constructor.
  HotColdRmwContext(HotColdRmwContext& other_, IAsyncContext* caller_context_)
    : AsyncHotColdRmwContext<key_t, value_t>(other_, caller_context_)
    {}

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, HotColdContext<key_t>::caller_context,
                                            context_copy);
  }
 public:
  inline const rmw_context_t& rmw_context() const {
    return *static_cast<const rmw_context_t*>(HotColdContext<key_t>::caller_context);
  }
  inline rmw_context_t& rmw_context() {
    return *static_cast<rmw_context_t*>(HotColdContext<key_t>::caller_context);
  }
  /// Propagates calls to caller context
  inline const key_t& key() const {
    return rmw_context().key();
  }
  /// Set initial value.
  inline void RmwInitial(value_t& value) final {
    rmw_context().RmwInitial(value);
  }
  /// RCU.
  inline void RmwCopy(const value_t& old_value, value_t& value) final {
    rmw_context().RmwCopy(old_value, value);
  }
  /// in-place update.
  inline bool RmwAtomic(value_t& value) final {
    return rmw_context().RmwAtomic(value);
  }
  /// Get value size for initial value or in-place update
  inline constexpr uint32_t value_size() const final {
    return rmw_context().value_size();
  }
  /// Get value size for RCU
  inline constexpr uint32_t value_size(const value_t& value) const final {
    return rmw_context().value_size(value);
  }
};


template<class K, class V, class MC>
class HotColdRmwReadContext : public IAsyncContext {
 public:
  typedef MC hc_rmw_context_t;
  typedef K key_t;
  typedef V value_t;

  HotColdRmwReadContext(K key, hc_rmw_context_t* caller_context)
    : key_{ key }
    , caller_context_{ caller_context }
  {}
  /// Copy (and deep-copy) constructor.
  HotColdRmwReadContext(const HotColdRmwReadContext& other)
    : key_{ other.key_ }
    , caller_context_{ other.caller_context_ }
  {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const K& key() const {
    return key_;
  }
  inline const V& value() const {
    return value_;
  }

  inline void Get(const V& value) {
    value_.value = value.value;
  }
  inline void GetAtomic(const V& value) {
    value_.value = value.atomic_value.load();
  }

  inline const hc_rmw_context_t& hot_cold_rmw_context() const {
    return *static_cast<const hc_rmw_context_t*>(caller_context_);
  }
  inline hc_rmw_context_t& hot_cold_rmw_context() {
    return *static_cast<hc_rmw_context_t*>(caller_context_);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  K key_;
  V value_;

  /// HotColdRmw context
  IAsyncContext* caller_context_;
};

template <class MC>
class HotColdRmwCopyContext: public CopyToTailContextBase<typename MC::key_t> {
 public:
  // Typedefs on the key and value required internally by FASTER.
  typedef MC rmw_context_t;
  typedef typename rmw_context_t::key_t key_t;
  typedef typename rmw_context_t::value_t value_t;

  using key_or_shallow_key_t = std::remove_const_t<std::remove_reference_t<std::result_of_t<decltype(&MC::key)(MC)>>>;
  typedef Record<key_t, value_t> record_t;
  constexpr static const bool kIsShallowKey = !std::is_same<key_or_shallow_key_t, key_t>::value;

  HotColdRmwCopyContext(rmw_context_t* rmw_context, HashBucketEntry& expected_entry, void* dest_store)
    : CopyToTailContextBase<key_t>(expected_entry, dest_store)
    , rmw_context_{ rmw_context }
  {}
  /// Copy constructor deleted; copy to tail request doesn't go async
  HotColdRmwCopyContext(const HotColdRmwCopyContext& from) = delete;

  inline const key_t& key() const final {
    return rmw_context_->key();
  }
  inline uint32_t key_size() const final {
    return key().size();
  }
  inline KeyHash get_key_hash() const final {
    return key().GetHash();
  }
  inline bool is_key_equal(const key_t& other) const final {
    return key() == other;
  }
  inline uint32_t value_size() const final {
    return rmw_context_->value_size();
  }

  inline bool copy_at(void* dest, uint32_t alloc_size) const final {
    record_t* record = reinterpret_cast<record_t*>(dest);
    // write key
    key_t* key_dest = const_cast<key_t*>(&record->key());
    write_deep_key_at_helper<kIsShallowKey>::execute(rmw_context_->key(), key_dest);
    // write value -- Upsert's Put = Rmw's RmwInitial
    rmw_context_->RmwInitial(record->value());
    return true;
  }

 private:
  rmw_context_t* rmw_context_;
};

template<class K, class V, class D>
class FasterKvHC {
public:
  typedef FasterKv<K, V, D> faster_t;
  typedef FasterKvHC<K, V, D> faster_hc_t;
  typedef AsyncHotColdRmwContext<K, V> async_hc_rmw_context_t;

  typedef K key_t;
  typedef V value_t;

  FasterKvHC(uint64_t total_disk_size, double hot_log_perc,
              uint64_t hot_log_mem_size, uint64_t hot_log_table_size, const std::string& hot_log_filename,
              uint64_t cold_log_mem_size, uint64_t cold_log_table_size, const std::string& cold_log_filename,
              double hot_log_mutable_perc = 0.2, double cold_log_mutable_perc = 0,
              bool automatic_compaction = true, bool compaction_trigger_perc = 0.9,
              bool pre_allocate_logs = false, const std::string& hot_log_config = "", const std::string& cold_log_config = "")
  : hot_store{ hot_log_table_size, hot_log_mem_size, hot_log_filename, hot_log_mutable_perc, pre_allocate_logs, hot_log_config }
  , cold_store{ cold_log_table_size, cold_log_mem_size, cold_log_filename, cold_log_mutable_perc, pre_allocate_logs, cold_log_config }
  {
    // Calculate disk size for hot & cold logs
    assert(0 > hot_log_perc && hot_log_perc < 1);
    hot_log_disk_size_ = static_cast<uint64_t>(total_disk_size * hot_log_perc);
    cold_log_disk_size_ = total_disk_size - hot_log_disk_size_;

    // Launch background compaction check thread (if requested)
    if (automatic_compaction) {
      hot_log_disk_size_limit_ = static_cast<uint64_t>(hot_log_disk_size_ * compaction_trigger_perc);
      cold_log_disk_size_limit_ = static_cast<uint64_t>(cold_log_disk_size_ * compaction_trigger_perc);
      compaction_thread_ = std::move(std::thread(&FasterKvHC::CheckInternalLogsSize, this));
    }

  }

  FasterKvHC(uint64_t hot_table_size, uint64_t hot_log_size, const std::string& hot_filename,
              uint64_t cold_table_size, uint64_t cold_log_size, const std::string& cold_filename,
               double hot_log_mutable_fraction = 0.9, double cold_log_mutable_fraction = 0.9,
              bool pre_allocate_log = false, const std::string& config = "")
  : hot_store{ hot_table_size, hot_log_size, hot_filename, hot_log_mutable_fraction, pre_allocate_log, config }
  , cold_store{ cold_table_size, cold_log_size, cold_filename, cold_log_mutable_fraction, pre_allocate_log, config }
  {}


  // No copy constructor.
  FasterKvHC(const FasterKvHC& other) = delete;

 public:
  /// Thread-related operations
  Guid StartSession();
  uint64_t ContinueSession(const Guid& guid);
  void StopSession();
  void Refresh();

  /// Store interface
  template <class RC>
  Status Read(RC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class UC>
  Status Upsert(UC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class MC>
  Status Rmw(MC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class DC>
  Status Delete(DC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  bool CompletePending(bool wait = false);

  /// Checkpoint/recovery operations.
  bool Checkpoint(IndexPersistenceCallback index_persistence_callback,
                  HybridLogPersistenceCallback hybrid_log_persistence_callback,
                  Guid& token);
  bool CheckpointIndex(IndexPersistenceCallback index_persistence_callback, Guid& token);
  bool CheckpointHybridLog(HybridLogPersistenceCallback hybrid_log_persistence_callback,
                            Guid& token);
  /*
  Status Recover(const Guid& index_token, const Guid& hybrid_log_token, uint32_t& version,
                 std::vector<Guid>& session_ids);

  /// Log compaction entry method.
  bool Compact(uint64_t untilAddress);

  /// Truncating the head of the log.
  bool ShiftBeginAddress(Address address, GcState::truncate_callback_t truncate_callback,
                         GcState::complete_callback_t complete_callback);

  /// Make the hash table larger.
  bool GrowIndex(GrowState::callback_t caller_callback);
  */

  bool CompactHotLog(uint64_t until_address, bool shift_begin_address,
                      int n_threads = faster_t::kNumCompactionThreads);
  bool CompactColdLog(uint64_t until_address, bool shift_begin_address,
                      int n_threads = faster_t::kNumCompactionThreads);

  /// Statistics
  inline uint64_t Size() const {
    return hot_store.Size() + cold_store.Size();
  }
  inline void DumpDistribution() {
    printf("\n\nHOT LOG\n==========\n");
    hot_store.DumpDistribution();
    printf("\n\nCOLD LOG\n==========");
    cold_store.DumpDistribution();
  }

 public:
  faster_t hot_store;
  faster_t cold_store;

  // retry queue
  concurrent_queue<async_hc_rmw_context_t*> retry_rmw_requests;
 private:
  void CheckInternalLogsSize();
  void CompleteRmwRetryRequests();


  uint64_t hot_log_disk_size_;
  uint64_t cold_log_disk_size_;

  // compaction-related
  const std::chrono::milliseconds compaction_check_interval_ = 1000ms;
  std::thread compaction_thread_;
  uint64_t hot_log_disk_size_limit_;
  uint64_t cold_log_disk_size_limit_;
};

template<class K, class V, class D>
inline Guid FasterKvHC<K, V, D>::StartSession() {
  Guid guid = Guid::Create();
  assert(hot_store.StartSession(guid) == guid);
  assert(cold_store.StartSession(guid) == guid);
  return guid;
}

template <class K, class V, class D>
inline uint64_t FasterKvHC<K, V, D>::ContinueSession(const Guid& session_id) {
  uint64_t serial_num = hot_store.ContinueSession(session_id);
  cold_store.ContinueSession(session_id);
  return serial_num;
}

template <class K, class V, class D>
inline void FasterKvHC<K, V, D>::StopSession() {
  hot_store.StopSession();
  cold_store.StopSession();
}

template<class K, class V, class D>
inline bool FasterKvHC<K, V, D>::CompletePending(bool wait) {
  do {
    CompleteRmwRetryRequests();
    if(hot_store.CompletePending(wait) &&
        cold_store.CompletePending(wait) &&
        retry_rmw_requests.empty()) {
      return true;
    }
  } while(wait);
  return false;
}

template <class K, class V, class D>
template <class RC>
inline Status FasterKvHC<K, V, D>::Read(RC& context, AsyncCallback callback,
                                      uint64_t monotonic_serial_num) {
  typedef RC read_context_t;
  typedef HotColdReadContext<read_context_t> hc_read_context_t;

  auto hot_store_callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<HotColdReadContext<read_context_t>> context{ ctxt };

    if (result != Status::NotFound) {
      // call user-provided callback
      context->caller_callback(context->caller_context, result);
    } else {
      // issue request to cold log
      faster_hc_t * faster_hc = static_cast<faster_hc_t*>(context->faster_hc);
      Status status = faster_hc->cold_store.Read(context->read_context(),
                                                context->caller_callback,
                                                context->serial_num);
      if (status != Status::Pending) {
        context->caller_callback(context->caller_context, status);
      }
      // User-provided callback will eventually be called from cold log
    }
  };

  // Issue request to hot log
  hc_read_context_t hc_context{ this, context, callback, monotonic_serial_num };
  Status status = hot_store.Read(hc_context, hot_store_callback, monotonic_serial_num);
  if (status != Status::NotFound) {
    return status;
  }
  // Issue request on cold log
  return cold_store.Read(context, callback, monotonic_serial_num);
}


template <class K, class V, class D>
template <class UC>
inline Status FasterKvHC<K, V, D>::Upsert(UC& context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num) {
  return hot_store.Upsert(context, callback, monotonic_serial_num);
}


template <class K, class V, class D>
template <class MC>
inline Status FasterKvHC<K, V, D>::Rmw(MC& context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num) {
  typedef MC rmw_context_t;
  typedef HotColdRmwContext<rmw_context_t> hc_rmw_context_t;
  typedef HotColdRmwReadContext<K, V, hc_rmw_context_t> hc_rmw_read_context_t;
  typedef HotColdRmwCopyContext<MC> hc_rmw_copy_context_t;

  auto hot_store_retry_if_not_found_callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<hc_rmw_context_t> context{ ctxt };
    if (result != Status::NotFound) {
      // call user-provided callback
      context->caller_callback(context->caller_context, result);
    } else {
      // This can only happen if compaction moved this entry to cold -- retry entry
      faster_hc_t * faster_hc = static_cast<faster_hc_t*>(context->faster_hc);
      faster_hc->retry_rmw_requests.push(context.get());
    }
  };

  auto cold_store_read_callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<hc_rmw_read_context_t> rmw_read_context{ ctxt };
    CallbackContext<hc_rmw_context_t> context { &rmw_read_context->hot_cold_rmw_context() };
    faster_hc_t* faster_hc = static_cast<faster_hc_t*>(context->faster_hc);

    if (result == Status::Ok) {
      // Conditional copy to hot-log
      hc_rmw_copy_context_t copy_context{ &context->rmw_context(), context->expected_entry,
                                      static_cast<void*>(&faster_hc->hot_store) };
      Status status = faster_hc->hot_store.ConditionalCopyToTail(copy_context);
      if (status == Status::Pending) {
        // add to retry queue
        faster_hc->retry_rmw_requests.push(context.get());
        return;
      }
      assert(status == Status::Ok || status == Status::Aborted);
      // call rmw on hot log again -- we expect that hot log has the record
      status = faster_hc->hot_store.Rmw(*context.get(), context->hot_store_retry_if_not_found_callback,
                                      context->serial_num, false);
      if (status == Status::Pending) {
        return; // callback will be called
      }
      else if (status != Status::NotFound) {
        context->caller_callback(context->caller_context, status); // ok or error
      }
      // This can only happen if compaction moved this entry to cold -- retry request
      faster_hc->retry_rmw_requests.push(context.get());
    } else if (result == Status::NotFound) {
      // Issue Rmw to hot log -- a record will be created if not exists
      Status status = faster_hc->hot_store.Rmw(context->rmw_context(), context->caller_callback,
                                                context->serial_num, true);
      assert(status != Status::NotFound);
      if (status != Status::Pending) {
        context->caller_callback(context->caller_context, status); // ok or error
      }
    } else {
      assert(result != Status::Pending);
      context->caller_callback(context->caller_context, result); // error
    }

  };

  auto hot_store_rmw_callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<hc_rmw_context_t> context{ ctxt };

    if (result != Status::NotFound) {
      // call user-provided callback
      context->caller_callback(context->caller_context, result);
      return;
    }
    // issue read request to cold log
    faster_hc_t * faster_hc = static_cast<faster_hc_t*>(context->faster_hc);

    hc_rmw_read_context_t rmw_read_context { context->key(), context.get() };
    Status read_status = faster_hc->cold_store.Read(rmw_read_context,
                                  context->cold_store_read_callback, context->serial_num);
    if (read_status == Status::Ok) {
      // Conditional copy to hot-cold
      hc_rmw_copy_context_t copy_context{ &context->rmw_context(), context->expected_entry,
                                        static_cast<void*>(&faster_hc->hot_store) };
      Status status = faster_hc->hot_store.ConditionalCopyToTail(copy_context);
      if (status == Status::Pending) {
        // add to retry queue
        context.async = true;
        faster_hc->retry_rmw_requests.push(context.get());
        return;
      }
      assert(status == Status::Ok || status == Status::Aborted);
      // call rmw on hot log again -- we expect that hot log has the record
      status = faster_hc->hot_store.Rmw(*context.get(), context->hot_store_retry_if_not_found_callback,
                                      context->serial_num, false);
      if (status == Status::Pending) {
        return; // callback will be called
      }
      else if (status != Status::NotFound) {
        context->caller_callback(context->caller_context, status); // ok or error
      }
      // This can only happen if compaction moved this entry to cold -- retry request
      faster_hc->retry_rmw_requests.push(context.get());
    } else if (read_status == Status::NotFound) {
      // Issue Rmw to hot log -- a record will be created if not exists
      Status status = faster_hc->hot_store.Rmw(context->rmw_context(), context->caller_callback,
                                                context->serial_num, true);
      assert(status != Status::NotFound);
      if (status != Status::Pending) {
        context->caller_callback(context->caller_context, status); // ok or error
      }
    } else if (read_status != Status::Pending) {
      context->caller_callback(context->caller_context, read_status); // error
    }
  };

  // Keep hash bucket entry
  KeyHash hash = context.key().GetHash();
  HashBucketEntry entry;
  const AtomicHashBucketEntry* atomic_entry = hot_store.FindEntry(hash, entry);

  // Issue RMW request on hot log
  hc_rmw_context_t hc_rmw_context{ this, context, callback, monotonic_serial_num, entry,
                                  cold_store_read_callback, hot_store_retry_if_not_found_callback };
  Status status = hot_store.Rmw(hc_rmw_context, hot_store_rmw_callback, monotonic_serial_num, false);
  if (status != Status::NotFound) {
    return status;
  }

  // Entry not found in hot log - issue read request to cold log
  hc_rmw_read_context_t rmw_read_context { context.key(), &hc_rmw_context };
  Status read_status = cold_store.Read(rmw_read_context, cold_store_read_callback,
                                        monotonic_serial_num);
  if (read_status == Status::Ok) {
    // Conditional copy to hot log
    hc_rmw_copy_context_t copy_context{ &context, hc_rmw_context.expected_entry,
                                    static_cast<void*>(&hot_store) };
    Status status = hot_store.ConditionalCopyToTail(copy_context);
    if (status == Status::Pending) {
      // add to retry rmw queue
      retry_rmw_requests.push(&hc_rmw_context);
      return Status::Pending;
    }
    assert(status == Status::Ok || status == Status::Aborted);

    // call rmw on hot log again -- we expect that hot log has the record
    status = hot_store.Rmw(hc_rmw_context, hot_store_retry_if_not_found_callback,
                            monotonic_serial_num, false);
    if (status != Status::NotFound) {
      return status;
    }
    // This can only happen if compaction moved this entry to cold log -- retry
    retry_rmw_requests.push(&hc_rmw_context);
    return Status::Pending;
  }
  else if (read_status == Status::NotFound) {
    // Issue Rmw to hot log; will create an initial rmw
    // ... unless a concurrent upsert/rmw was performed in the meantime
    return hot_store.Rmw(context, callback, monotonic_serial_num, true);
  }
  else {
    // pending or error status
    return read_status;
  }
}

template <class K, class V, class D>
template <class DC>
inline Status FasterKvHC<K, V, D>::Delete(DC& context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num) {
  // Force adding a tombstone entry in the tail of the log
  return hot_store.Delete(context, callback, monotonic_serial_num, true);
}


template<class K, class V, class D>
inline bool FasterKvHC<K, V, D>::Checkpoint(IndexPersistenceCallback index_persistence_callback,
                                            HybridLogPersistenceCallback hybrid_log_persistence_callback,
                                            Guid& token) {
  // TODO: handle case where one store returns false.
//
//  hot_store.Checkpoint(index_persistence_callback,
//                      hybrid_log_persistence_callback, token);
//  cold_store.Checkpoint(index_persistence_callback,
//                      hybrid_log_persistence_callback, token);
  return false;
}

template<class K, class V, class D>
inline bool FasterKvHC<K, V, D>::CompactHotLog(uint64_t until_address, bool shift_begin_address, int n_threads) {
  return hot_store.CompactWithLookup(until_address, shift_begin_address, n_threads, &cold_store);
}

template<class K, class V, class D>
inline bool FasterKvHC<K, V, D>::CompactColdLog(uint64_t until_address, bool shift_begin_address, int n_threads) {
  return cold_store.CompactWithLookup(until_address, shift_begin_address, n_threads);
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::CheckInternalLogsSize() {
  Address until_address;

  while( true ) {
    if (hot_store.Size() > hot_log_disk_size_limit_) {
      // perform hot-cold compaction
      until_address = Address(hot_store.hlog.GetTailAddress().control() - hot_log_disk_size_limit_);
      bool success = CompactHotLog(until_address, true);
      if (!success) {
        fprintf(stderr, "warn: hot-cold compaction not successful\n");
      }
    }
    if (cold_store.Size() > cold_log_disk_size_limit_) {
      // perform cold-cold compaction
      until_address = Address(cold_store.hlog.GetTailAddress().control() - cold_log_disk_size_limit_);
      bool success = CompactColdLog(until_address, true);
      if (!success) {
        fprintf(stderr, "warn: cold-cold compaction not successful\n");
      }
    }

    std::this_thread::sleep_for(compaction_check_interval_);
  }
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::CompleteRmwRetryRequests() {
  async_hc_rmw_context_t* ctxt;
  while (retry_rmw_requests.try_pop(ctxt)) {
    // Re-issue RMW request
    CallbackContext<async_hc_rmw_context_t> context{ ctxt };
    Status status = Rmw(*context.get(), context->caller_callback, context->serial_num);
    if (status != Status::Pending) {
      // if done, callback user code
      context->caller_callback(context->caller_context, status);
    }
    else {
      // destroy this context, since a new context was created
      context.async = false;
    }
  }
}

}
} // namespace FASTER::core