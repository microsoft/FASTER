// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "faster.h"
#include "guid.h"
#include "internal_contexts.h"

namespace FASTER {
namespace core {

enum class ReadOperationStage {
  HOT_LOG_READ = 1,
  COLD_LOG_READ = 2,
};

enum class RmwOperationStage {
  HOT_LOG_RMW = 1,
  COLD_LOG_READ = 2,
  HOT_LOG_CONDITIONAL_INSERT = 3,
  WAIT_FOR_RETRY = 4,
};

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
    , caller_context{ caller_context_ }
    , caller_callback{ other.caller_callback }
    , serial_num{ other.serial_num }
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
template <class K, class V>
class AsyncHotColdReadContext : public HotColdContext<K> {
 public:
  typedef K key_t;
  typedef V value_t;

 protected:
  AsyncHotColdReadContext(void* faster_hc_, ReadOperationStage stage_, IAsyncContext& caller_context_,
                        AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : HotColdContext<key_t>(faster_hc_, caller_context_, caller_callback_, monotonic_serial_num_)
    , stage{ stage_ }
  {}
  /// The deep-copy constructor.
  AsyncHotColdReadContext(AsyncHotColdReadContext& other_, IAsyncContext* caller_context_)
    : HotColdContext<key_t>(other_, caller_context_)
    , stage{ other_.stage }
  {}
 public:
  virtual const key_t& key() const = 0;
  virtual void Get(const value_t& value) = 0;
  virtual void GetAtomic(const value_t& value) = 0;

  ReadOperationStage stage;
};

/// Context that holds user context for Read request
template <class RC>
class HotColdReadContext : public AsyncHotColdReadContext <typename RC::key_t, typename RC::value_t> {
 public:
  typedef RC read_context_t;
  typedef typename read_context_t::key_t key_t;
  typedef typename read_context_t::value_t value_t;

  HotColdReadContext(void* faster_hc_, ReadOperationStage stage_, read_context_t& caller_context_,
                    AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : AsyncHotColdReadContext<key_t, value_t>(faster_hc_, stage_, caller_context_,
                                            caller_callback_, monotonic_serial_num_)
    {}

  /// The deep-copy constructor.
  HotColdReadContext(HotColdReadContext& other_, IAsyncContext* caller_context_)
    : AsyncHotColdReadContext<key_t, value_t>(other_, caller_context_)
    {}

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, HotColdContext<key_t>::caller_context,
                                            context_copy);
  }
 private:
  inline const read_context_t& read_context() const {
    return *static_cast<const read_context_t*>(HotColdContext<key_t>::caller_context);
  }
  inline read_context_t& read_context() {
    return *static_cast<read_context_t*>(HotColdContext<key_t>::caller_context);
  }
 public:
  /// Propagates calls to caller context
  inline const key_t& key() const final {
    return read_context().key();
  }
  inline void Get(const value_t& value) final {
    read_context().Get(value);
  }
  inline void GetAtomic(const value_t& value) final {
    read_context().GetAtomic(value);
  }
};

/// Context that holds user context for Read request

// forward declaration
template<class K, class V>
class HotColdRmwReadContext;

template <class K, class V>
class AsyncHotColdRmwContext : public HotColdContext<K> {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef HotColdRmwReadContext<K, V> rmw_read_context_t;
 protected:
  AsyncHotColdRmwContext(void* faster_hc_, RmwOperationStage stage_, HashBucketEntry& expected_entry_,
                      IAsyncContext& caller_context_, AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : HotColdContext<key_t>(faster_hc_, caller_context_, caller_callback_, monotonic_serial_num_)
    , stage{ stage_ }
    , expected_entry{ expected_entry_ }
    , read_context{ nullptr }
  {}
  /// The deep copy constructor.
  AsyncHotColdRmwContext(AsyncHotColdRmwContext& other, IAsyncContext* caller_context)
    : HotColdContext<key_t>(other, caller_context)
    , stage{ other.stage }
    , expected_entry{ other.expected_entry }
    , read_context{ other.read_context }
  {}
 public:
  virtual const key_t& key() const = 0;
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

  RmwOperationStage stage;
  HashBucketEntry expected_entry;
  rmw_read_context_t* read_context;
};

template <class MC>
class HotColdRmwContext : public AsyncHotColdRmwContext<typename MC::key_t, typename MC::value_t> {
 public:
  typedef MC rmw_context_t;
  typedef typename rmw_context_t::key_t key_t;
  typedef typename rmw_context_t::value_t value_t;

  HotColdRmwContext(void* faster_hc_, RmwOperationStage stage_, HashBucketEntry& expected_entry_,
                  rmw_context_t& caller_context_, AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : AsyncHotColdRmwContext<key_t, value_t>(faster_hc_, stage_, expected_entry_,
                                            caller_context_, caller_callback_, monotonic_serial_num_)
    {}
  /// The deep-copy constructor.
  HotColdRmwContext(HotColdRmwContext& other_, IAsyncContext* caller_context_)
    : AsyncHotColdRmwContext<key_t, value_t>(other_, caller_context_)
    {}

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, HotColdContext<key_t>::caller_context,
                                            context_copy);
  }
 private:
  inline const rmw_context_t& rmw_context() const {
    return *static_cast<const rmw_context_t*>(HotColdContext<key_t>::caller_context);
  }
  inline rmw_context_t& rmw_context() {
    return *static_cast<rmw_context_t*>(HotColdContext<key_t>::caller_context);
  }
 public:
  /// Propagates calls to caller context
  inline const key_t& key() const final {
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


template<class K, class V>
class HotColdRmwReadContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  HotColdRmwReadContext(key_t key, IAsyncContext* rmw_context_)
    : key_{ key }
    , rmw_context{ rmw_context_ }
  {}
  /// Copy (and deep-copy) constructor.
  HotColdRmwReadContext(const HotColdRmwReadContext& other)
    : key_{ other.key_ }
    , rmw_context{ other.rmw_context }
  {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const key_t& key() const {
    return key_;
  }
  inline const value_t& value() const {
    return value_;
  }

  inline void Get(const value_t& value) {
    value_.value = value.value;
  }
  inline void GetAtomic(const value_t& value) {
    value_.value = value.atomic_value.load();
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    // need to deep copy rmw context, if didn't went async
    Status rmw_deep_copy_status = rmw_context->DeepCopy(rmw_context);
    if (rmw_deep_copy_status != Status::Ok) {
      return rmw_deep_copy_status;
    }
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 public:
  IAsyncContext* rmw_context; // HotColdRmw context

 private:
  key_t key_;
  value_t value_;
};

template <class MC, class RC>
class HotColdRmwConditionalInsertContext: public IAsyncContext {
 public:
  // Typedefs on the key and value required internally by FASTER.
  typedef MC rmw_context_t;
  typedef typename rmw_context_t::key_t key_t;
  typedef typename rmw_context_t::value_t value_t;
  typedef RC read_context_t;

  using key_or_shallow_key_t = std::remove_const_t<std::remove_reference_t<std::result_of_t<decltype(&MC::key)(MC)>>>;
  typedef Record<key_t, value_t> record_t;
  constexpr static const bool kIsShallowKey = !std::is_same<key_or_shallow_key_t, key_t>::value;

  HotColdRmwConditionalInsertContext(rmw_context_t* rmw_context, read_context_t* read_context, bool rmw_rcu)
    : rmw_context_{ rmw_context }
    , read_context_{ read_context }
    , rmw_rcu_{ rmw_rcu }
  {}
  /// Copy constructor deleted; copy to tail request doesn't go async
  HotColdRmwConditionalInsertContext(const HotColdRmwConditionalInsertContext& from)
    : rmw_context_{ from.rmw_context_ }
    , read_context_{ from.read_context_ }
    , rmw_rcu_{ from.rmw_rcu_ }
  {}

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    // need to deep copy read context, if didn't went async
    IAsyncContext* read_context = static_cast<IAsyncContext*>(read_context_);
    Status read_deep_copy_status = read_context_->DeepCopy(read_context);
    if (read_deep_copy_status != Status::Ok) {
      return read_deep_copy_status;
    }
    // need to deep copy rmw context, if didn't went async
    IAsyncContext* rmw_context = static_cast<IAsyncContext*>(rmw_context_);
    Status rmw_deep_copy_status = rmw_context_->DeepCopy(rmw_context);
    if (rmw_deep_copy_status != Status::Ok) {
      return rmw_deep_copy_status;
    }
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 public:
  inline const key_or_shallow_key_t& key() const {
    return rmw_context_->key();
  }
  inline uint32_t value_size() const {
    return rmw_context_->value_size();
  }
  inline bool is_tombstone() const {
    return false; // rmw never copies tombstone records
  }

  inline bool Insert(void* dest, uint32_t alloc_size) const {
    record_t* record = reinterpret_cast<record_t*>(dest);
    // write key
    key_t* key_dest = const_cast<key_t*>(&record->key());
    write_deep_key_at_helper<kIsShallowKey>::execute(rmw_context_->key(), key_dest);
    // write value
    if (rmw_rcu_) {
      // insert updated value
      rmw_context_->RmwCopy(read_context_->value(), record->value());
    } else {
      // Insert initial value
      rmw_context_->RmwInitial(record->value());
    }
    return true;
  }

 private:
  rmw_context_t* rmw_context_;
  read_context_t* read_context_;
  bool rmw_rcu_;
};

template<class K, class V, class D>
class FasterKvHC {
public:
  typedef FasterKv<K, V, D> faster_t;
  typedef FasterKvHC<K, V, D> faster_hc_t;
  typedef AsyncHotColdReadContext<K, V> async_hc_read_context_t;
  typedef AsyncHotColdRmwContext<K, V> async_hc_rmw_context_t;
  typedef HotColdRmwReadContext<K, V> hc_rmw_read_context_t;

  typedef K key_t;
  typedef V value_t;

  FasterKvHC(uint64_t total_disk_size, double hot_log_perc,
              uint64_t hot_log_mem_size, uint64_t hot_log_table_size, const std::string& hot_log_filename,
              uint64_t cold_log_mem_size, uint64_t cold_log_table_size, const std::string& cold_log_filename,
              double hot_log_mutable_perc = 0.2, double cold_log_mutable_perc = 0,
              bool automatic_compaction = true, double compaction_trigger_perc = 0.9,
              double hot_log_compaction_log_perc = 0.20, double cold_log_compaction_log_perc = 0.1,
              bool pre_allocate_logs = false, const std::string& hot_log_config = "", const std::string& cold_log_config = "")
  : hot_store{ hot_log_table_size, hot_log_mem_size, hot_log_filename, hot_log_mutable_perc, pre_allocate_logs, hot_log_config }
  , cold_store{ cold_log_table_size, cold_log_mem_size, cold_log_filename, cold_log_mutable_perc, pre_allocate_logs, cold_log_config }
  , hot_log_compaction_log_perc_{ hot_log_compaction_log_perc }
  , cold_log_compaction_log_perc_{ cold_log_compaction_log_perc }
  {
    // Calculate disk size for hot & cold logs
    assert(0.0 < hot_log_perc && hot_log_perc < 1.0);
    hot_log_disk_size_ = static_cast<uint64_t>(total_disk_size * hot_log_perc);
    cold_log_disk_size_ = total_disk_size - hot_log_disk_size_;

    is_compaction_active_.store(automatic_compaction);
    if (automatic_compaction) {
      // Launch background compaction check thread
      hot_log_disk_size_limit_ = static_cast<uint64_t>(hot_log_disk_size_ * compaction_trigger_perc);
      cold_log_disk_size_limit_ = static_cast<uint64_t>(cold_log_disk_size_ * compaction_trigger_perc);
      is_compaction_active_.store(true);
      compaction_thread_ = std::move(std::thread(&FasterKvHC::CheckInternalLogsSize, this));
    }
  }
  // No copy constructor.
  FasterKvHC(const FasterKvHC& other) = delete;

  ~FasterKvHC() {
    while (hot_store.system_state_.phase() != Phase::REST ||
          cold_store.system_state_.phase() != Phase::REST ) {
      std::this_thread::yield();
    }
    if (compaction_thread_.joinable()) {
      // shut down compaction thread
      is_compaction_active_.store(false);
      compaction_thread_.join();
    }
  }

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
  static void AsyncContinuePendingRead(IAsyncContext* ctxt, Status result);

  template<class C>
  Status InternalRmw(C& pending_context);
  static void AsyncContinuePendingRmw(IAsyncContext* ctxt, Status result);
  static void AsyncContinuePendingRmwRead(IAsyncContext* ctxt, Status result);

  void CheckInternalLogsSize();
  void CompleteRmwRetryRequests();

  uint64_t hot_log_disk_size_;
  uint64_t cold_log_disk_size_;

  // compaction-related
  const std::chrono::milliseconds compaction_check_interval_ = 250ms;
  uint64_t hot_log_disk_size_limit_;
  uint64_t cold_log_disk_size_limit_;
  double hot_log_compaction_log_perc_;
  double cold_log_compaction_log_perc_;

  std::thread compaction_thread_;
  std::atomic<bool> is_compaction_active_;
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
  // Finish pending ops before stopping session
  while(!CompletePending(false)) {
    std::this_thread::yield();
  }
  hot_store.StopSession();
  cold_store.StopSession();
}

template<class K, class V, class D>
inline bool FasterKvHC<K, V, D>::CompletePending(bool wait) {
  bool hot_store_done, cold_store_done;
  do {
    CompleteRmwRetryRequests();
    // Complete pending requests on both stores
    hot_store_done = hot_store.CompletePending(false);
    cold_store_done = cold_store.CompletePending(false);

    if(hot_store_done && cold_store_done &&
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

  // Issue request to hot log
  hc_read_context_t hc_context{ this, ReadOperationStage::HOT_LOG_READ,
                              context, callback, monotonic_serial_num };
  Status status = hot_store.Read(hc_context, AsyncContinuePendingRead, monotonic_serial_num, true);
  if (status != Status::NotFound) {
    if (status == Status::Aborted) {
      // found a tombstone -- no need to check cold log
      return Status::NotFound;
    }
    return status;
  }
  // Issue request on cold log
  return cold_store.Read(context, callback, monotonic_serial_num);
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::AsyncContinuePendingRead(IAsyncContext* ctxt, Status result) {
  CallbackContext<async_hc_read_context_t> context{ ctxt };

  if (context->stage == ReadOperationStage::HOT_LOG_READ) {
    if (result != Status::NotFound) {
      if (result == Status::Aborted) {
        // found a tombstone -- no need to check cold log
        result = Status::NotFound;
      }
      // call user-provided callback
      context->caller_callback(context->caller_context, result);
      return;
    }
    // issue request to cold log
    faster_hc_t * faster_hc = static_cast<faster_hc_t*>(context->faster_hc);

    context->stage = ReadOperationStage::COLD_LOG_READ;
    Status status = faster_hc->cold_store.Read(*context.get(),
                                              AsyncContinuePendingRead,
                                              context->serial_num);
    if (status != Status::Pending) {
      context->caller_callback(context->caller_context, status);
      return;
    }
    context.async = true;
    // User-provided callback will eventually be called from cold log
  }
  else if (context->stage == ReadOperationStage::COLD_LOG_READ) {
    // call user-provided callback
    context->caller_callback(context->caller_context, result);
  }
  else {
    assert(false); // not reachable
  }
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

  // Keep hash bucket entry
  KeyHash hash = context.key().GetHash();
  HashBucketEntry entry;
  const AtomicHashBucketEntry* atomic_entry = hot_store.FindEntry(hash, entry);

  hc_rmw_context_t hc_rmw_context{ this, RmwOperationStage::HOT_LOG_RMW, entry,
                                  context, callback, monotonic_serial_num };
  return InternalRmw(hc_rmw_context);
}

template <class K, class V, class D>
template <class C>
inline Status FasterKvHC<K, V, D>::InternalRmw(C& hc_rmw_context) {
  //typedef typename C::rmw_context_t rmw_context_t;
  typedef AsyncHotColdRmwContext<K, V> async_hc_rmw_context_t;
  typedef HotColdRmwReadContext<K, V> hc_rmw_read_context_t;
  typedef HotColdRmwConditionalInsertContext<async_hc_rmw_context_t, hc_rmw_read_context_t> hc_rmw_ci_context_t;

  uint64_t monotonic_serial_num = hc_rmw_context.serial_num;

  // Issue RMW request on hot log
  Status status = hot_store.Rmw(hc_rmw_context, AsyncContinuePendingRmw, monotonic_serial_num, false);
  if (status != Status::NotFound) {
    return status;
  }

  // Entry not found in hot log - issue read request to cold log
  hc_rmw_context.stage = RmwOperationStage::COLD_LOG_READ;
  hc_rmw_read_context_t rmw_read_context { hc_rmw_context.key(), &hc_rmw_context };
  Status read_status = cold_store.Read(rmw_read_context, AsyncContinuePendingRmwRead,
                                        monotonic_serial_num);

  if (read_status == Status::Ok || read_status == Status::NotFound) {
    // Conditional insert to hot log
    hc_rmw_context.stage = RmwOperationStage::HOT_LOG_CONDITIONAL_INSERT;

    bool rmw_rcu = (read_status == Status::Ok);
    hc_rmw_ci_context_t ci_context{ &hc_rmw_context, &rmw_read_context, rmw_rcu };
    Status ci_status = hot_store.ConditionalInsert(ci_context,  AsyncContinuePendingRmw,
                                                  hc_rmw_context.expected_entry.address(),
                                                  static_cast<void*>(&hot_store));
    if (ci_status == Status::Aborted || ci_status == Status::NotFound) {
      // add to retry rmw queue
      hc_rmw_context.stage = RmwOperationStage::WAIT_FOR_RETRY;
      hc_rmw_context.expected_entry = HashBucketEntry::kInvalidEntry;
      retry_rmw_requests.push(&hc_rmw_context);
      return Status::Pending;
    }
    // return to user
    assert (ci_status == Status::Ok || ci_status == Status::Pending);
    return ci_status;
  }

  // pending or error status
  return read_status;
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::AsyncContinuePendingRmw(IAsyncContext* ctxt, Status result) {
  typedef HotColdRmwConditionalInsertContext<async_hc_rmw_context_t, hc_rmw_read_context_t> hc_rmw_ci_context_t;

  CallbackContext<async_hc_rmw_context_t> context{ ctxt };
  faster_hc_t* faster_hc = static_cast<faster_hc_t*>(context->faster_hc);

  hc_rmw_read_context_t rmw_read_context { context->key(), context.get() };
  if (context->stage == RmwOperationStage::HOT_LOG_RMW) {
    if (result != Status::NotFound) {
      // call user-provided callback -- most common case
      context->caller_callback(context->caller_context, result);
      return;
    }
    // issue read request to cold log
    context->stage = RmwOperationStage::COLD_LOG_READ;
    Status read_status = faster_hc->cold_store.Read(rmw_read_context, AsyncContinuePendingRmwRead,
                                                    context->serial_num);
    if (read_status == Status::Pending) {
      context.async = true;
      return;
    }
    result = read_status;
    context->read_context = &rmw_read_context;
  }

  if (context->stage == RmwOperationStage::COLD_LOG_READ) {
    // result corresponds to cold log Read op status
    if (result == Status::Ok || result == Status::NotFound) {
      assert(context->read_context != nullptr);

      // issue conditional insert request to hot log
      context->stage = RmwOperationStage::HOT_LOG_CONDITIONAL_INSERT;

      bool rmw_rcu = (result == Status::Ok);
      hc_rmw_ci_context_t ci_context{ context.get(), context->read_context, rmw_rcu };
      Status ci_status = faster_hc->hot_store.ConditionalInsert(ci_context, AsyncContinuePendingRmw,
                                                                context->expected_entry.address(),
                                                                static_cast<void*>(&faster_hc->hot_store));
      if (ci_status == Status::Pending) {
        context.async = true;
        return;
      }
      result = ci_status;
    }
    else {
      // call user-provided callback -- error status
      context->caller_callback(context->caller_context, result);
      return;
    }
  }

  if (context->stage == RmwOperationStage::HOT_LOG_CONDITIONAL_INSERT) {
    // result corresponds to hot log Conditional Insert op status
    if (result != Status::Aborted && result != Status::NotFound) {
      // Status::Ok or error
      context->caller_callback(context->caller_context, result);
      return;
    }
    // add to retry queue
    context.async = true; // do not free context
    context->stage = RmwOperationStage::WAIT_FOR_RETRY;
    context->expected_entry = HashBucketEntry::kInvalidEntry;
    context->read_context = nullptr;
    faster_hc->retry_rmw_requests.push(context.get());
    return;
  }

  assert(false); // not reachable
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::AsyncContinuePendingRmwRead(IAsyncContext* ctxt, Status result) {
  CallbackContext<hc_rmw_read_context_t> rmw_read_context{ ctxt };
  rmw_read_context.async = true;
  async_hc_rmw_context_t* rmw_context = static_cast<async_hc_rmw_context_t*>(rmw_read_context->rmw_context);

  assert(rmw_context->stage == RmwOperationStage::COLD_LOG_READ);
  rmw_context->read_context = rmw_read_context.get();

  AsyncContinuePendingRmw(static_cast<IAsyncContext*>(rmw_context), result);
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
  while(is_compaction_active_.load()) {
    if (hot_store.Size() > hot_log_disk_size_limit_) {
      // calculate until address
      uint64_t until_address = hot_store.hlog.begin_address.control() + (
                    static_cast<uint64_t>(hot_store.Size() * hot_log_compaction_log_perc_));
      until_address = std::max(until_address, hot_store.hlog.GetTailAddress().control() - hot_log_disk_size_limit_);
      // round down address to page bounds
      until_address = until_address - Address(until_address).offset() + Address::kMaxOffset + 1;
      assert(until_address <= hot_store.hlog.safe_read_only_address.control());
      assert(until_address % hot_store.hlog.kPageSize == 0);
      //fprintf(stdout, "HOT: {%llu} [%llu %llu] %llu\n", hot_store.Size(), hot_store.hlog.begin_address.control(),
      //          hot_store.hlog.GetTailAddress().control(), until_address);
      // perform hot-cold compaction
      StartSession();
      if (!CompactHotLog(until_address, true)) {
        fprintf(stdout, "warn: hot-cold compaction not successful\n");
      }
      StopSession();
      //fprintf(stdout, "HOT: {%llu} Done!\n", hot_store.Size());
    }
    if (cold_store.Size() > cold_log_disk_size_limit_) {
      uint64_t until_address = cold_store.hlog.begin_address.control() + (
                    static_cast<uint64_t>(cold_store.Size() * cold_log_compaction_log_perc_));
      until_address = std::max(until_address, cold_store.hlog.GetTailAddress().control() - cold_log_disk_size_limit_);
      // round down address to page bounds
      until_address = until_address - Address(until_address).offset() + Address::kMaxOffset + 1;
      assert(until_address <= cold_store.hlog.safe_read_only_address.control());
      assert(until_address % cold_store.hlog.kPageSize == 0);
      //fprintf(stdout, "COLD: {%llu} [%llu %llu] %llu\n", cold_store.Size(), cold_store.hlog.begin_address.control(),
      //          cold_store.hlog.GetTailAddress().control(), until_address);
      // perform cold-cold compaction
      cold_store.StartSession();
      if (!CompactColdLog(until_address, true)) {
        fprintf(stdout, "warn: cold-cold compaction not successful\n");
      }
      cold_store.StopSession();
      //fprintf(stdout, "COLD: {%llu} Done!\n", cold_store.Size());
    }

    std::this_thread::sleep_for(compaction_check_interval_);
  }
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::CompleteRmwRetryRequests() {
  async_hc_rmw_context_t* ctxt;
  while (retry_rmw_requests.try_pop(ctxt)) {
    CallbackContext<async_hc_rmw_context_t> context{ ctxt };
    // Get hash bucket entry
    KeyHash hash = context->key().GetHash();
    HashBucketEntry entry;
    const AtomicHashBucketEntry* atomic_entry = hot_store.FindEntry(hash, entry);
    // Initialize rmw context vars
    context->expected_entry = entry;
    context->stage = RmwOperationStage::HOT_LOG_RMW;
    // Re-issue RMW request
    Status status = InternalRmw(*context.get());
    if (status == Status::Pending) {
      context.async = true;
      continue;
    }
    // if done, issue user callback
    context->caller_callback(context->caller_context, status);
    context.async = false;
  }
}

}
} // namespace FASTER::core