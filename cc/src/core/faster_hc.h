// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "faster.h"
#include "guid.h"
#include "hc_internal_contexts.h"
#include "internal_contexts.h"

#include "index/faster_index.h"

namespace FASTER {
namespace core {

template<class K, class V, class D>
class FasterKvHC {
public:

  typedef FasterKv<K, V, D, HashIndex<D>, FasterIndex<D>> hot_faster_store_t;
  typedef FasterKv<K, V, D, FasterIndex<D>, HashIndex<D>> cold_faster_store_t;

  typedef FasterKvHC<K, V, D> faster_hc_t;
  typedef AsyncHotColdReadContext<K, V> async_hc_read_context_t;
  typedef AsyncHotColdRmwContext<K, V> async_hc_rmw_context_t;
  typedef HotColdRmwReadContext<K, V> hc_rmw_read_context_t;
  typedef HotColdRmwConditionalInsertContext<K, V> hc_rmw_ci_context_t;

  typedef K key_t;
  typedef V value_t;

  constexpr static ReadCacheConfig default_rc_config{ 256_MiB, 0.9, false, true };

  FasterKvHC(uint64_t hot_log_table_size, uint64_t hot_log_mem_size, const std::string& hot_log_filename,
            uint64_t cold_log_table_size, uint64_t cold_log_mem_size, const std::string& cold_log_filename,
            double hot_log_mutable_perc = 0.6, double cold_log_mutable_perc = 0,
            ReadCacheConfig rc_config = default_rc_config,
            HCCompactionConfig hc_compaction_config = HCCompactionConfig())
  : hot_store{ hot_log_table_size, hot_log_mem_size, hot_log_filename, hot_log_mutable_perc, rc_config }
  , cold_store{ cold_log_table_size, cold_log_mem_size, cold_log_filename, cold_log_mutable_perc }
  , hc_compaction_config_{ hc_compaction_config }
  {
    hot_store.SetOtherStore(&cold_store);
    cold_store.SetOtherStore(&hot_store);

    if (hc_compaction_config.hot_store.enabled || hc_compaction_config.cold_store.enabled) {
      //
      if (hc_compaction_config.hot_store.hlog_size_budget < 64_MiB) {
        throw std::runtime_error{ "HCComapctionConfig: Hot log size too small (<64 MB)" };
      }
      if (hc_compaction_config.cold_store.hlog_size_budget < 64_MiB) {
        throw std::runtime_error{ "HCComapctionConfig: Cold log size too small (<64 MB)" };
      }
      // Launch background compaction check thread
      is_compaction_active_.store(true);
      compaction_thread_ = std::move(std::thread(&FasterKvHC::CheckInternalLogsSize, this));
    } else {
      log_warn("Automatic HC compaction is disabled");
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
                      int n_threads = hot_faster_store_t::kNumCompactionThreads);
  bool CompactColdLog(uint64_t until_address, bool shift_begin_address,
                      int n_threads = cold_faster_store_t::kNumCompactionThreads);

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
  hot_faster_store_t hot_store;
  cold_faster_store_t cold_store;

  // retry queue
  concurrent_queue<async_hc_rmw_context_t*> retry_rmw_requests;
 private:
  static void AsyncContinuePendingRead(IAsyncContext* ctxt, Status result);

  template<class C>
  Status InternalRmw(C& pending_context);
  static void AsyncContinuePendingRmw(IAsyncContext* ctxt, Status result);
  static void AsyncContinuePendingRmwRead(IAsyncContext* ctxt, Status result);
  static void AsyncContinuePendingRmwConditionalInsert(IAsyncContext* ctxt, Status result);

  void CheckInternalLogsSize();
  void CompleteRmwRetryRequests();

  uint64_t hot_log_disk_size_;
  uint64_t cold_log_disk_size_;

  // compaction-related
  /*
  const std::chrono::milliseconds compaction_check_interval_ = 250ms;
  uint64_t hot_log_disk_size_limit_;
  uint64_t cold_log_disk_size_limit_;
  double hot_log_compaction_log_perc_;
  double cold_log_compaction_log_perc_;
  */
  HCCompactionConfig hc_compaction_config_;
  std::thread compaction_thread_;
  std::atomic<bool> is_compaction_active_;
};

template<class K, class V, class D>
inline Guid FasterKvHC<K, V, D>::StartSession() {
  Guid guid = Guid::Create();
  Guid ret_guid = hot_store.StartSession(guid);
  assert(ret_guid == guid);
  ret_guid = cold_store.StartSession(guid);
  assert(ret_guid == guid);
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
inline void FasterKvHC<K, V, D>::Refresh() {
  hot_store.Refresh();
  cold_store.Refresh();
}

template<class K, class V, class D>
inline bool FasterKvHC<K, V, D>::CompletePending(bool wait) {
  bool hot_store_done, cold_store_done;
  do {
    // Complete pending requests on both stores
    hot_store_done = hot_store.CompletePending(false);
    cold_store_done = cold_store.CompletePending(false);

    CompleteRmwRetryRequests();

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
  typedef HotColdIndexContext<hc_read_context_t> hc_index_context_t;

  hc_read_context_t hc_context{ this, ReadOperationStage::HOT_LOG_READ,
                              context, callback, monotonic_serial_num };

  // Store latest hash index entry before initiating Read op
  hc_index_context_t index_context{ &hc_context };
  Status index_status = hot_store.hash_index_.template FindEntry<hc_index_context_t>(
                              hot_store.thread_ctx(), index_context);
  assert(index_status == Status::Ok || index_status == Status::NotFound);
  // index_context.entry can be either HashBucketEntry::kInvalidEntry or a valid one (either hlog or read cache)
  hc_context.entry = index_context.entry;
  hc_context.atomic_entry = index_context.atomic_entry;

  // Issue request to hot log
  Status status = hot_store.Read(hc_context, AsyncContinuePendingRead, monotonic_serial_num, true);
  if (status != Status::NotFound) {
    if (status == Status::Aborted) {
      // found a tombstone -- no need to check cold log
      return Status::NotFound;
    }
    return status;
  }
  // Issue request on cold log
  hc_context.stage = ReadOperationStage::COLD_LOG_READ;
  status = cold_store.Read(hc_context, AsyncContinuePendingRead, monotonic_serial_num);
  assert(status != Status::Aborted); // impossible when !abort_if_tombstone

  if (status == Status::Ok && hot_store.UseReadCache()) {
    // Try to insert cold log-resident record to read cache
    auto record = reinterpret_cast<typename hot_faster_store_t::record_t*>(hc_context.record);
    Status rc_status = hot_store.read_cache_->Insert(hot_store.thread_ctx(), hc_context, record, true);
    assert(rc_status == Status::Ok || rc_status == Status::Aborted);
  }
  // OK (no read-cache), not_found, pending or error status
  return status;
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::AsyncContinuePendingRead(IAsyncContext* ctxt, Status result) {
  CallbackContext<async_hc_read_context_t> context{ ctxt };
  faster_hc_t* faster_hc = static_cast<faster_hc_t*>(context->faster_hc);

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
    context->stage = ReadOperationStage::COLD_LOG_READ;
    Status status = faster_hc->cold_store.Read(*context.get(),
                                              AsyncContinuePendingRead,
                                              context->serial_num);
    if (status == Status::Pending) {
      context.async = true;
      return;
    }
    result = status;
  }

  if (context->stage == ReadOperationStage::COLD_LOG_READ) {
    if (faster_hc->hot_store.UseReadCache() && result == Status::Ok) {
      // try to insert to read cache
      auto record = reinterpret_cast<typename hot_faster_store_t::record_t*>(context->record);
      auto& hot_store = faster_hc->hot_store;
      Status rc_status = hot_store.read_cache_->Insert(hot_store.thread_ctx(), *context.get(), record, true);
      assert(rc_status == Status::Ok || rc_status == Status::Aborted);
    }
    // call user-provided callback
    context->caller_callback(context->caller_context, result);
    return;
  }

  assert(false); // not reachable
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
  typedef HotColdIndexContext<hc_rmw_context_t> hc_index_context_t;

  // Keep hash bucket entry
  hc_rmw_context_t hc_rmw_context{ this, RmwOperationStage::HOT_LOG_RMW,
                                  HashBucketEntry::kInvalidEntry,
                                  hot_store.hlog.GetTailAddress(),
                                  context, callback, monotonic_serial_num };

  hc_index_context_t index_context{ &hc_rmw_context };
  Status status = hot_store.hash_index_.template FindOrCreateEntry<hc_index_context_t>(
                        hot_store.thread_ctx(), index_context);
  assert(status == Status::Ok);
  assert(index_context.entry.address() >= Address::kInvalidAddress);

  // Store index latest hash index hlog address at the time
  hc_rmw_context.expected_hlog_address = (hot_store.UseReadCache())
      ? hot_store.read_cache_->Skip(index_context)
      : index_context.entry.address();

  return InternalRmw(hc_rmw_context);
}

template <class K, class V, class D>
template <class C>
inline Status FasterKvHC<K, V, D>::InternalRmw(C& hc_rmw_context) {
  uint64_t monotonic_serial_num = hc_rmw_context.serial_num;

  // Issue RMW request on hot log; do not create a new record if exists!
  Status status = hot_store.Rmw(hc_rmw_context, AsyncContinuePendingRmw, monotonic_serial_num, false);
  if (status != Status::NotFound) {
    return status;
  }
  // Entry not found in hot log

  // Issue read request to cold log
  hc_rmw_context.stage = RmwOperationStage::COLD_LOG_READ;
  hc_rmw_read_context_t rmw_read_context{ &hc_rmw_context };
  Status read_status = cold_store.Read(rmw_read_context, AsyncContinuePendingRmwRead,
                                        monotonic_serial_num);

  if (read_status == Status::Ok || read_status == Status::NotFound) {
    // Conditional insert to hot log
    hc_rmw_context.read_context = &rmw_read_context;
    hc_rmw_context.stage = RmwOperationStage::HOT_LOG_CONDITIONAL_INSERT;

    bool rmw_rcu = (read_status == Status::Ok);
    hc_rmw_ci_context_t ci_context{ &hc_rmw_context, rmw_rcu };
    hc_rmw_context.ci_context = &ci_context;
    Status ci_status = hot_store.ConditionalInsert(ci_context, AsyncContinuePendingRmwConditionalInsert,
                                                  hc_rmw_context.expected_hlog_address, false);
    if (ci_status == Status::Aborted || ci_status == Status::NotFound) {
      // add to retry rmw queue
      hc_rmw_context.prepare_for_retry();
      // deep copy context
      IAsyncContext* hc_rmw_context_copy;
      RETURN_NOT_OK(hc_rmw_context.DeepCopy(hc_rmw_context_copy));
      retry_rmw_requests.push(
        static_cast<async_hc_rmw_context_t*>(hc_rmw_context_copy));
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
  CallbackContext<async_hc_rmw_context_t> context{ ctxt };
  faster_hc_t* faster_hc = static_cast<faster_hc_t*>(context->faster_hc);

  hc_rmw_read_context_t rmw_read_context { context.get() };
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
      hc_rmw_ci_context_t ci_context{ context.get(), rmw_rcu };
      Status ci_status = faster_hc->hot_store.ConditionalInsert(ci_context, AsyncContinuePendingRmwConditionalInsert,
                                                                context->expected_hlog_address, false);
      if (ci_status == Status::Pending) {
        context.async = true;
        return;
      }
      result = ci_status;
    }
    else {
      // call user-provided callback -- error status
      context->caller_callback(context->caller_context, result);
      context->free_aux_contexts();
      return;
    }
  }

  if (context->stage == RmwOperationStage::HOT_LOG_CONDITIONAL_INSERT) {
    // result corresponds to hot log Conditional Insert op status
    if (result != Status::Aborted && result != Status::NotFound) {
      context->caller_callback(context->caller_context, result); // Status::Ok or error
      context->free_aux_contexts();
      return;
    }
    // add to retry queue
    context.async = true; // do not free this context
    context->prepare_for_retry();
    faster_hc->retry_rmw_requests.push(context.get());
    return;
  }

  assert(false); // not reachable
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::AsyncContinuePendingRmwRead(IAsyncContext* ctxt, Status result) {
  CallbackContext<hc_rmw_read_context_t> rmw_read_context{ ctxt };
  rmw_read_context.async = true;
  // Validation
  async_hc_rmw_context_t* rmw_context = static_cast<async_hc_rmw_context_t*>(rmw_read_context->hc_rmw_context);
  assert(rmw_context->stage == RmwOperationStage::COLD_LOG_READ);
  AsyncContinuePendingRmw(static_cast<IAsyncContext*>(rmw_context), result);
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::AsyncContinuePendingRmwConditionalInsert(IAsyncContext* ctxt, Status result) {
  CallbackContext<hc_rmw_ci_context_t> rmw_ci_context{ ctxt };
  rmw_ci_context.async = true;
  // Validation
  async_hc_rmw_context_t* rmw_context = static_cast<async_hc_rmw_context_t*>(rmw_ci_context->rmw_context);
  assert(rmw_context->stage == RmwOperationStage::HOT_LOG_CONDITIONAL_INSERT);
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
  uint64_t tail_address = hot_store.hlog.GetTailAddress().control();
  log_error("Auto-compact HOT: {%.2lf GB} [%lu %lu] -> [%lu %lu]",
            static_cast<double>(hot_store.Size()) / (1 << 30),
            hot_store.hlog.begin_address.control(), tail_address,
            until_address, tail_address);

  bool success = hot_store.CompactWithLookup(until_address, shift_begin_address, n_threads, true);
  log_error("Auto-compact HOT: {%.2lf GB} Done!",
            static_cast<double>(hot_store.Size()) / (1 << 30));
  return success;
}

template<class K, class V, class D>
inline bool FasterKvHC<K, V, D>::CompactColdLog(uint64_t until_address, bool shift_begin_address, int n_threads) {
  uint64_t tail_address = cold_store.hlog.GetTailAddress().control();
  log_error("Auto-compact COLD: {%.2lf GB} [%lu %lu] -> [%lu %lu]",
            static_cast<double>(cold_store.Size()) / (1 << 30),
            cold_store.hlog.begin_address.control(), tail_address,
            until_address, until_address);

  bool success = cold_store.CompactWithLookup(until_address, shift_begin_address, n_threads);
  log_error("Auto-compact COLD: {%.2lf GB} Done!",
            static_cast<double>(cold_store.Size()) / (1 << 30));
  return success;
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::CheckInternalLogsSize() {

  HlogCompactionConfig& hot_compaction_config = hc_compaction_config_.hot_store;
  HlogCompactionConfig& cold_compaction_config = hc_compaction_config_.cold_store;

  uint64_t hot_log_size_threshold = static_cast<uint64_t>(
    hot_compaction_config.hlog_size_budget * hot_compaction_config.trigger_perc);
  uint64_t cold_log_size_threshold = static_cast<uint64_t>(
    cold_compaction_config.hlog_size_budget * cold_compaction_config.trigger_perc);

  std::chrono::milliseconds check_interval = std::min(
    hot_compaction_config.check_interval,
    cold_compaction_config.check_interval
  );

  while(is_compaction_active_.load()) {
    if (hot_store.Size() < hot_log_size_threshold &&
        cold_store.Size() < cold_log_size_threshold) {
      std::this_thread::sleep_for(check_interval);
      continue;
    }

    if (hot_store.Size() >= hot_log_size_threshold) {
      uint64_t begin_address = hot_store.hlog.begin_address.control();
      // calculate until address
      uint64_t until_address = begin_address + (
                  static_cast<uint64_t>(hot_store.Size() * hot_compaction_config.compact_perc));
      // respect user-imposed limit
      until_address = std::min(until_address, begin_address + hot_compaction_config.max_compacted_size);
      // do not compact in-memory regions
      until_address = std::min(until_address, hot_store.hlog.safe_head_address.control());
      // round down address to page bounds
      until_address = until_address - Address(until_address).offset() + Address::kMaxOffset + 1;
      assert(until_address <= hot_store.hlog.safe_read_only_address.control());
      assert(until_address % hot_store.hlog.kPageSize == 0);

      // perform hot-cold compaction
      StartSession();
      if (!CompactHotLog(until_address, true, hot_compaction_config.num_threads)) {
        log_error("Compact HOT: *not* successful! :(");
      }
      StopSession();
    }

    if (cold_store.Size() >= cold_log_size_threshold) {
      uint64_t begin_address = cold_store.hlog.begin_address.control();
      // calculate until address
      uint64_t until_address = begin_address + (
                    static_cast<uint64_t>(cold_store.Size() * cold_compaction_config.compact_perc));
      // respect user-imposed limit
      until_address = std::min(until_address, begin_address + cold_compaction_config.max_compacted_size);
      // do not compact in-memory regions
      until_address = std::min(until_address, cold_store.hlog.safe_head_address.control());
      // round down address to page bounds
      until_address = until_address - Address(until_address).offset() + Address::kMaxOffset + 1;
      assert(until_address <= cold_store.hlog.safe_read_only_address.control());
      assert(until_address % cold_store.hlog.kPageSize == 0);

      // perform cold-cold compaction
      cold_store.StartSession();
      if (!CompactColdLog(until_address, true, cold_compaction_config.num_threads)) {
        log_error("Compact COLD: *not* successful! :(");
      }
      cold_store.StopSession();
    }
  }
}

template<class K, class V, class D>
inline void FasterKvHC<K, V, D>::CompleteRmwRetryRequests() {
  typedef HotColdIndexContext<async_hc_rmw_context_t> hc_index_context_t;

  async_hc_rmw_context_t* ctxt;
  while (retry_rmw_requests.try_pop(ctxt)) {
    log_debug("RMW RETRY");
    CallbackContext<async_hc_rmw_context_t> context{ ctxt };
    // Get hash bucket entry
    hc_index_context_t index_context{ context.get() };
    Status index_status = hot_store.hash_index_.template FindOrCreateEntry<hc_index_context_t>(
                                  hot_store.thread_ctx(), index_context);
    assert(index_status == Status::Ok); // Non-pending status
    assert(index_context.entry.address() >= Address::kInvalidAddress);

    // Initialize rmw context vars
    // Store index latest hash index hlog address at the time
    context->expected_hlog_address = (hot_store.UseReadCache())
        ? hot_store.read_cache_->Skip(index_context)
        : index_context.entry.address();

    context->stage = RmwOperationStage::HOT_LOG_RMW;
    // Re-issue RMW request
    Status status = InternalRmw(*context.get());
    if (status == Status::Pending) {
      context.async = true;
      continue;
    }
    // if done, issue user callback & free context
    context->caller_callback(context->caller_context, status);
  }
}

}
} // namespace FASTER::core