// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "config.h"
#include "faster.h"
#include "guid.h"

#include "checkpoint_state_f2.h"
#include "internal_contexts.h"
#include "internal_contexts_f2.h"

#include "index/cold_index.h"

namespace FASTER {
namespace core {

template<class K, class V, class D, class HHI = MemHashIndex<D>, class CHI = ColdIndex<D>>
class F2Kv {
 public:
  typedef FasterKv<K, V, D, HHI, CHI> hot_faster_store_t;
  typedef FasterKv<K, V, D, CHI, HHI> cold_faster_store_t;

  typedef typename hot_faster_store_t::IndexConfig HotIndexConfig;
  typedef typename cold_faster_store_t::IndexConfig ColdIndexConfig;

  typedef FasterStoreConfig<HHI> hot_faster_store_config_t;
  typedef FasterStoreConfig<CHI> cold_faster_store_config_t;

  typedef F2Kv<K, V, D, HHI, CHI> f2_t;
  typedef AsyncF2ReadContext<K, V> async_f2_read_context_t;
  typedef AsyncF2RmwContext<K, V> async_f2_rmw_context_t;
  typedef F2RmwReadContext<K, V> f2_rmw_read_context_t;
  typedef F2RmwConditionalInsertContext<K, V> f2_rmw_ci_context_t;

  typedef K key_t;
  typedef V value_t;

  constexpr static auto kLazyCompactionMaxWait = 2s;

  F2Kv(const HotIndexConfig& hot_index_config, uint64_t hot_log_mem_size, const std::string& hot_log_filename,
       const ColdIndexConfig& cold_index_config, uint64_t cold_log_mem_size, const std::string& cold_log_filename,
       double hot_log_mutable_pct = 0.6, double cold_log_mutable_pct = 0,
       ReadCacheConfig rc_config = DEFAULT_READ_CACHE_CONFIG,
       F2CompactionConfig compaction_config = DEFAULT_F2_COMPACTION_CONFIG)
  : hot_store{ hot_index_config, hot_log_mem_size, hot_log_filename, hot_log_mutable_pct, rc_config }  // FasterKv auto-compaction is disabled
  , cold_store{ cold_index_config, cold_log_mem_size, cold_log_filename, cold_log_mutable_pct }        // FasterKv auto-compaction is disabled
  , compaction_config_{ compaction_config }
  , background_worker_active_{ false }
  , compaction_scheduled_{ false }
  {
    hot_store.SetOtherStore(&cold_store);
    cold_store.SetOtherStore(&hot_store);


    InitializeCompaction();

    // launch background thread
    background_worker_active_.store(true);
    background_worker_thread_ = std::move(std::thread(&F2Kv::CheckSystemState, this));
  }

  F2Kv(const hot_faster_store_config_t& hot_config_, const cold_faster_store_config_t& cold_config_)
  : F2Kv(hot_config_.index_config, hot_config_.hlog_config.in_mem_size, hot_config_.filepath,
         cold_config_.index_config, cold_config_.hlog_config.in_mem_size, cold_config_.filepath,
         hot_config_.hlog_config.mutable_fraction, cold_config_.hlog_config.mutable_fraction,
         hot_config_.rc_config, { hot_config_.hlog_compaction_config, cold_config_.hlog_compaction_config }) {
  }

  // No copy constructor.
  F2Kv(const F2Kv& other) = delete;

  ~F2Kv() {
    while (hot_store.system_state_.phase() != Phase::REST ||
          cold_store.system_state_.phase() != Phase::REST ) {
      std::this_thread::yield();
    }
    if (background_worker_thread_.joinable()) {
      // shut down compaction thread
      background_worker_active_.store(false);
      background_worker_thread_.join();
    }
  }

 private:
  void InitializeCompaction();

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

  void CompletePendingCompactions();

  /// Checkpoint/recovery operations.
  bool Checkpoint(HybridLogPersistenceCallback hybrid_log_persistence_callback,
                  Guid& token, bool lazy = true);

  Status Recover(const Guid& token, uint32_t& version, std::vector<Guid>& session_ids);

  static void HotStoreCheckpointedCallback(void* ctxt, Status result, uint64_t persistent_serial_num);

  bool CompactHotLog(uint64_t until_address, bool shift_begin_address,
                      int n_threads = hot_faster_store_t::kNumCompactionThreads);
  bool CompactColdLog(uint64_t until_address, bool shift_begin_address,
                      int n_threads = cold_faster_store_t::kNumCompactionThreads);

  #ifdef TOML_CONFIG
  static F2Kv<K, V, D, HHI, CHI> FromConfigString(const std::string& config);
  static F2Kv<K, V, D, HHI, CHI> FromConfigFile(const std::string& filepath);
  #endif

  /// Statistics
  inline uint64_t Size() const {
    return hot_store.Size() + cold_store.Size();
  }
  inline void DumpDistribution() {
    fprintf(stderr, "\n\nHOT  LOG\n==========");
    hot_store.DumpDistribution();
    fprintf(stderr, "\n\nCOLD LOG\n==========");
    cold_store.DumpDistribution();
  }
  inline uint32_t NumActiveSessions() const {
    return std::max(hot_store.NumActiveSessions(), cold_store.NumActiveSessions());
  }
  inline bool AutoCompactionScheduled() const {
    return compaction_scheduled_.load();
  }

#ifdef STATISTICS
  void EnableStatsCollection() {
    hot_store.EnableStatsCollection();
    cold_store.EnableStatsCollection();
  }
  void DisableStatsCollection() {
    hot_store.DisableStatsCollection();
    cold_store.DisableStatsCollection();
  }
  inline void PrintStats() {
    printf("\n**************** HOT  LOG ****************\n");
    hot_store.PrintStats();
    printf("\n**************** -------- ****************\n");
    printf("\n**************** COLD LOG ****************\n");
    cold_store.PrintStats();
    printf("\n**************** -------- ****************\n");
  }
#endif

 public:
  hot_faster_store_t hot_store;
  cold_faster_store_t cold_store;

  // retry queue
  concurrent_queue<async_f2_rmw_context_t*> retry_rmw_requests;

 private:
  enum StoreType : uint8_t {
    HOT,
    COLD
  };
  static void AsyncContinuePendingRead(IAsyncContext* ctxt, Status result);

  template<class C>
  Status InternalRmw(C& pending_context);
  void CompleteRmwRetryRequests();

  static void AsyncContinuePendingRmw(IAsyncContext* ctxt, Status result);
  static void AsyncContinuePendingRmwRead(IAsyncContext* ctxt, Status result);
  static void AsyncContinuePendingRmwConditionalInsert(IAsyncContext* ctxt, Status result);

  void HeavyEnter();

  void CheckSystemState();

  template<class S>
  bool ShouldCompactHlog(const S& store, StoreType store_type,
                        const HlogCompactionConfig& compaction_config, uint64_t& until_address);
  template<class S>
  bool CompactLog(S& store, StoreType store_type, uint64_t until_address,
                  bool shift_begin_address, int n_threads, bool checkpoint);

 private:
  // background thread
  std::thread background_worker_thread_;
  std::atomic<bool> background_worker_active_;

  // compaction-related
  F2CompactionConfig compaction_config_;
  std::atomic<bool> compaction_scheduled_;

  // checkpoint-related
  HotColdCheckpointState checkpoint_;
};

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::InitializeCompaction() {
  if (compaction_config_.hot_store.enabled || compaction_config_.cold_store.enabled) {
    if (compaction_config_.hot_store.hlog_size_budget < 64_MiB) {
      throw std::runtime_error{ "F2CompactionConfig: Hot log size too small (<64 MB)" };
    }
    if (compaction_config_.cold_store.hlog_size_budget < 64_MiB) {
      throw std::runtime_error{ "F2CompactionConfig: Cold log size too small (<64 MB)" };
    }
  } else {
    log_info("Automatic F2 compaction is disabled");
  }
}

template<class K, class V, class D, class HHI, class CHI>
inline Guid F2Kv<K, V, D, HHI, CHI>::StartSession() {
  if (checkpoint_.phase.load() != CheckpointPhase::REST) {
    throw std::runtime_error{ "Can start new session only in REST phase!" };
  }
  Guid guid = Guid::Create();
  Guid ret_guid = hot_store.StartSession(guid);
  assert(ret_guid == guid);
  ret_guid = cold_store.StartSession(guid);
  assert(ret_guid == guid);
  return guid;
}

template <class K, class V, class D, class HHI, class CHI>
inline uint64_t F2Kv<K, V, D, HHI, CHI>::ContinueSession(const Guid& session_id) {
  if (checkpoint_.phase.load() != CheckpointPhase::REST) {
    throw std::runtime_error{ "Can continue session only in REST phase!" };
  }
  uint64_t serial_num = hot_store.ContinueSession(session_id);
  cold_store.ContinueSession(session_id);
  return serial_num;
}

template <class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::StopSession() {
  // Finish pending ops and finish checkpointing before stopping session
  CheckpointPhase phase;
  while(!CompletePending(false) ||
      ((phase = checkpoint_.phase.load()) != CheckpointPhase::REST)) {
    std::this_thread::yield();
  }
  hot_store.StopSession();
  cold_store.StopSession();
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::Refresh() {
  if (checkpoint_.phase.load() != CheckpointPhase::REST) {
    HeavyEnter();
  }
  hot_store.Refresh();
  cold_store.Refresh();
}

template<class K, class V, class D, class HHI, class CHI>
inline bool F2Kv<K, V, D, HHI, CHI>::CompletePending(bool wait) {
  bool hot_store_done, cold_store_done;
  do {
    // Complete pending requests on both stores
    hot_store_done = hot_store.CompletePending(false);
    cold_store_done = cold_store.CompletePending(false);

    CompleteRmwRetryRequests();
    Refresh();

    if(hot_store_done && cold_store_done &&
        retry_rmw_requests.empty()) {
      return true;
    }
  } while(wait);
  return false;
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::CompletePendingCompactions() {
  while (compaction_scheduled_.load()) {
    if (hot_store.epoch_.IsProtected()) {
      CompletePending();
    }
    std::this_thread::yield();
  }
}

template <class K, class V, class D, class HHI, class CHI>
template <class RC>
inline Status F2Kv<K, V, D, HHI, CHI>::Read(RC& context, AsyncCallback callback,
                                            uint64_t monotonic_serial_num) {
  typedef RC read_context_t;
  typedef F2ReadContext<read_context_t> f2_read_context_t;
  typedef F2IndexContext<f2_read_context_t> f2_index_context_t;

  f2_read_context_t f2_context{ this, ReadOperationStage::HOT_LOG_READ,
                                context, callback, monotonic_serial_num };

  // Store latest hash index entry before initiating Read op
  f2_index_context_t index_context{ &f2_context };
  Status index_status = hot_store.hash_index_.template FindEntry<f2_index_context_t>(
                              hot_store.thread_ctx(), index_context);
  assert(index_status == Status::Ok || index_status == Status::NotFound);
  // index_context.entry can be either HashBucketEntry::kInvalidEntry or a valid one (either hlog or read cache)
  f2_context.entry = index_context.entry;
  f2_context.atomic_entry = index_context.atomic_entry;

  // Issue request to hot log
  Status status = hot_store.Read(f2_context, AsyncContinuePendingRead, monotonic_serial_num, true);
  if (status != Status::NotFound) {
    if (status == Status::Aborted) {
      // found a tombstone -- no need to check cold log
      return Status::NotFound;
    }
    return status;
  }
  // Issue request on cold log
  f2_context.stage = ReadOperationStage::COLD_LOG_READ;
  status = cold_store.Read(f2_context, AsyncContinuePendingRead, monotonic_serial_num);
  assert(status != Status::Aborted); // impossible when !abort_if_tombstone

  if (status == Status::Ok && hot_store.UseReadCache()) {
    // Try to insert cold log-resident record to read cache
    auto record = reinterpret_cast<typename hot_faster_store_t::record_t*>(f2_context.record);
    Status rc_status = hot_store.read_cache_->Insert(hot_store.thread_ctx(), f2_context, record, true);
    assert(rc_status == Status::Ok || rc_status == Status::Aborted);
  }
  // OK (no read-cache), not_found, pending or error status
  return status;
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::AsyncContinuePendingRead(IAsyncContext* ctxt, Status result) {
  CallbackContext<async_f2_read_context_t> context{ ctxt };
  f2_t* f2 = static_cast<f2_t*>(context->f2);

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
    Status status = f2->cold_store.Read(*context.get(),
                                        AsyncContinuePendingRead,
                                        context->serial_num);
    if (status == Status::Pending) {
      context.async = true;
      return;
    }
    result = status;
  }

  if (context->stage == ReadOperationStage::COLD_LOG_READ) {
    if (f2->hot_store.UseReadCache() && result == Status::Ok) {
      // try to insert to read cache
      auto record = reinterpret_cast<typename hot_faster_store_t::record_t*>(context->record);
      auto& hot_store = f2->hot_store;
      Status rc_status = hot_store.read_cache_->Insert(hot_store.thread_ctx(), *context.get(), record, true);
      assert(rc_status == Status::Ok || rc_status == Status::Aborted);
    }
    // call user-provided callback
    context->caller_callback(context->caller_context, result);
    return;
  }

  assert(false); // not reachable
}


template<class K, class V, class D, class HHI, class CHI>
template <class UC>
inline Status F2Kv<K, V, D, HHI, CHI>::Upsert(UC& context, AsyncCallback callback,
                                              uint64_t monotonic_serial_num) {
  return hot_store.Upsert(context, callback, monotonic_serial_num);
}

template<class K, class V, class D, class HHI, class CHI>
template <class MC>
inline Status F2Kv<K, V, D, HHI, CHI>::Rmw(MC& context, AsyncCallback callback,
                                           uint64_t monotonic_serial_num) {
  typedef MC rmw_context_t;
  typedef F2RmwContext<rmw_context_t> f2_rmw_context_t;
  typedef F2IndexContext<f2_rmw_context_t> f2_index_context_t;

  // Keep hash bucket entry
  f2_rmw_context_t f2_rmw_context{ this, RmwOperationStage::HOT_LOG_RMW,
                                  HashBucketEntry::kInvalidEntry,
                                  hot_store.hlog.GetTailAddress(),
                                  context, callback, monotonic_serial_num };

  f2_index_context_t index_context{ &f2_rmw_context };
  Status status = hot_store.hash_index_.template FindOrCreateEntry<f2_index_context_t>(
                        hot_store.thread_ctx(), index_context);
  assert(status == Status::Ok);
  assert(index_context.entry.address() >= Address::kInvalidAddress);

  // Store index latest hash index hlog address at the time
  f2_rmw_context.expected_hlog_address = (hot_store.UseReadCache())
      ? hot_store.read_cache_->Skip(index_context)
      : index_context.entry.address();

  return InternalRmw(f2_rmw_context);
}

template<class K, class V, class D, class HHI, class CHI>
template <class C>
inline Status F2Kv<K, V, D, HHI, CHI>::InternalRmw(C& f2_rmw_context) {
  uint64_t monotonic_serial_num = f2_rmw_context.serial_num;

  // Issue RMW request on hot log; do not create a new record if exists!
  Status status = hot_store.Rmw(f2_rmw_context, AsyncContinuePendingRmw, monotonic_serial_num, false);
  if (status != Status::NotFound) {
    return status;
  }
  // Entry not found in hot log

  // Issue read request to cold log
  f2_rmw_context.stage = RmwOperationStage::COLD_LOG_READ;
  f2_rmw_read_context_t rmw_read_context{ &f2_rmw_context };
  Status read_status = cold_store.Read(rmw_read_context, AsyncContinuePendingRmwRead,
                                        monotonic_serial_num);

  if (read_status == Status::Ok || read_status == Status::NotFound) {
    // Conditional insert to hot log
    f2_rmw_context.read_context = &rmw_read_context;
    f2_rmw_context.stage = RmwOperationStage::HOT_LOG_CONDITIONAL_INSERT;

    bool rmw_rcu = (read_status == Status::Ok);
    f2_rmw_ci_context_t ci_context{ &f2_rmw_context, rmw_rcu };
    f2_rmw_context.ci_context = &ci_context;
    Status ci_status = hot_store.ConditionalInsert(ci_context, AsyncContinuePendingRmwConditionalInsert,
                                                   f2_rmw_context.expected_hlog_address, false);
    if (ci_status == Status::Aborted || ci_status == Status::NotFound) {
      // add to retry rmw queue
      f2_rmw_context.prepare_for_retry();
      // deep copy context
      IAsyncContext* f2_rmw_context_copy;
      RETURN_NOT_OK(f2_rmw_context.DeepCopy(f2_rmw_context_copy));
      retry_rmw_requests.push( static_cast<async_f2_rmw_context_t*>(f2_rmw_context_copy) );
      return Status::Pending;
    }
    // return to user
    assert (ci_status == Status::Ok || ci_status == Status::Pending);
    return ci_status;
  }

  // pending or error status
  return read_status;
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::AsyncContinuePendingRmw(IAsyncContext* ctxt, Status result) {
  CallbackContext<async_f2_rmw_context_t> context{ ctxt };
  f2_t* f2 = static_cast<f2_t*>(context->f2);

  f2_rmw_read_context_t rmw_read_context { context.get() };
  if (context->stage == RmwOperationStage::HOT_LOG_RMW) {
    if (result != Status::NotFound) {
      // call user-provided callback -- most common case
      context->caller_callback(context->caller_context, result);
      return;
    }
    // issue read request to cold log
    context->stage = RmwOperationStage::COLD_LOG_READ;
    Status read_status = f2->cold_store.Read(rmw_read_context, AsyncContinuePendingRmwRead,
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
      f2_rmw_ci_context_t ci_context{ context.get(), rmw_rcu };
      Status ci_status = f2->hot_store.ConditionalInsert(ci_context,
                                                         AsyncContinuePendingRmwConditionalInsert,
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
    f2->retry_rmw_requests.push(context.get());
    return;
  }

  assert(false); // not reachable
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::AsyncContinuePendingRmwRead(IAsyncContext* ctxt, Status result) {
  CallbackContext<f2_rmw_read_context_t> rmw_read_context{ ctxt };
  rmw_read_context.async = true;
  // Validation
  async_f2_rmw_context_t* rmw_context = static_cast<async_f2_rmw_context_t*>(rmw_read_context->f2_rmw_context);
  assert(rmw_context->stage == RmwOperationStage::COLD_LOG_READ);
  AsyncContinuePendingRmw(static_cast<IAsyncContext*>(rmw_context), result);
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::AsyncContinuePendingRmwConditionalInsert(IAsyncContext* ctxt, Status result) {
  CallbackContext<f2_rmw_ci_context_t> rmw_ci_context{ ctxt };
  rmw_ci_context.async = true;
  // Validation
  async_f2_rmw_context_t* rmw_context = static_cast<async_f2_rmw_context_t*>(rmw_ci_context->rmw_context);
  assert(rmw_context->stage == RmwOperationStage::HOT_LOG_CONDITIONAL_INSERT);
  AsyncContinuePendingRmw(static_cast<IAsyncContext*>(rmw_context), result);
}


template<class K, class V, class D, class HHI, class CHI>
template <class DC>
inline Status F2Kv<K, V, D, HHI, CHI>::Delete(DC& context, AsyncCallback callback,
                                              uint64_t monotonic_serial_num) {
  // Force adding a tombstone entry in the tail of the log
  return hot_store.Delete(context, callback, monotonic_serial_num, true);
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::HeavyEnter() {
  if (checkpoint_.phase.load() == CheckpointPhase::COLD_STORE_CHECKPOINT) {
    StoreCheckpointStatus status = checkpoint_.cold_store_status.load();
    if (status != StoreCheckpointStatus::FINISHED && status != StoreCheckpointStatus::FAILED) {
      return;
    }

    // call user-provided callback for this active session (if not already)
    uint64_t psn = checkpoint_.persistent_serial_nums[Thread::id()].load();
    if (psn == 0) {
      return;
    }
    Status result = (checkpoint_.hot_store_status.load() == StoreCheckpointStatus::FINISHED &&
                      checkpoint_.cold_store_status.load() == StoreCheckpointStatus::FINISHED)
                        ? Status::Ok : Status::IOError;
    checkpoint_.store_persistence_callback(result, psn);

    checkpoint_.persistent_serial_nums[Thread::id()].store(0);
    if (--checkpoint_.threads_pending_issue_callback == 0) {
      // all threads issued their callback -- atomically move to REST phase
      CheckpointPhase phase;
      if ((phase = checkpoint_.phase.load()) != CheckpointPhase::COLD_STORE_CHECKPOINT) {
        log_error("Unexpected checkpoint phase [expected: COLD_STORE_CHECKPOINT, actual: %s]",
                  CHECKPOINT_PHASE_STR[static_cast<int>(phase)]);
        assert(false);
      }

      log_debug("Hot-cold checkpoint finished! Moving to REST phase");
      checkpoint_.Reset();
      checkpoint_.phase.store(CheckpointPhase::REST);
    }
  }
}

template<class K, class V, class D, class HHI, class CHI>
inline bool F2Kv<K, V, D, HHI, CHI>::Checkpoint(HybridLogPersistenceCallback hybrid_log_persistence_callback,
                                                Guid& token, bool lazy) {
  CheckpointPhase expected_phase = CheckpointPhase::REST;
  if (!checkpoint_.phase.compare_exchange_strong(expected_phase, CheckpointPhase::HOT_STORE_CHECKPOINT)) {
    throw std::runtime_error{ "Can start checkpoint only when no other concurrent op" };
  }

  token = Guid::Create();
  auto lazy_checkpoint_expired = HotColdCheckpointState::time_point_t::clock::now() + (lazy ? kLazyCompactionMaxWait : 0s);
  checkpoint_.Initialize(hybrid_log_persistence_callback, token, lazy_checkpoint_expired);

  // For now we just ask the background thread to do the checkpoint, on the next possible moment
  checkpoint_.hot_store_status.store(StoreCheckpointStatus::REQUESTED);
  return true;
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::HotStoreCheckpointedCallback(void* ctxt, Status result, uint64_t persistent_serial_num) {
  // This will be called multiple times (i.e., once for each active session)
  auto checkpoint = static_cast<HotColdCheckpointState*>(ctxt);
  assert(checkpoint->phase.load() == CheckpointPhase::HOT_STORE_CHECKPOINT);
  assert(checkpoint->hot_store_status.load() == StoreCheckpointStatus::ACTIVE);

  // store persistent serial number for this thread
  uint64_t psn = checkpoint->persistent_serial_nums[Thread::id()].load();
  if (psn > 0) {
    log_warn("Persistent serial num for thread %u was already set [to %lu]. "
            "Was callback called twice for the same thread?", Thread::id(), psn);
    assert(false);
  }
  checkpoint->persistent_serial_nums[Thread::id()].store(persistent_serial_num);
  log_debug("Checkpoint callback for hot store [tid=%u] called! [psn: %lu, result: %s]",
            Thread::id(), psn, STATUS_STR[static_cast<int>(result)]);

  // Update global result if (1) first thread to arrive, or (2) non-ok result
  while (1) {
    Status global_result = checkpoint->hot_store_checkpoint_result.load();
    if (global_result == Status::Corruption || result != Status::Ok) {
      if (checkpoint->hot_store_checkpoint_result.compare_exchange_strong(global_result, result)) {
        break; // success on updated global result
      }
    }
  }

  if (--checkpoint->threads_pending_hot_store_persist == 0) {
    // All threads finished checkpointing hot store
    // Last thread responsible for moving to next phase (i.e., cold-store checkpointing)
    StoreCheckpointStatus status;

    // Mark hot store checkpoint status to finished (or failed)
    Status global_result = checkpoint->hot_store_checkpoint_result.load();
    log_debug("Hot store checkpoint result: %s", STATUS_STR[static_cast<int>(global_result)]);

    status = (global_result == Status::Ok) ? StoreCheckpointStatus::FINISHED : StoreCheckpointStatus::FAILED;
    checkpoint->hot_store_status.store(status);

    // Request cold store checkpointing
    CheckpointPhase phase;
    if ((phase = checkpoint->phase.load()) != CheckpointPhase::HOT_STORE_CHECKPOINT) {
      log_error("Unexpected checkpoint phase [expected: HOT_STORE_CHECKPOINT, actual: %s]",
                CHECKPOINT_PHASE_STR[static_cast<int>(phase)]);
    }
    log_debug("Moving to cold-store checkpoint phase");
    checkpoint->phase.store(CheckpointPhase::COLD_STORE_CHECKPOINT);

    if ((status = checkpoint->cold_store_status.load()) != StoreCheckpointStatus::IDLE) {
      log_error("Unexpected checkpoint status for COLD store [expected: IDLE, actual: %s]",
                STORE_CHECKPOINT_STATUS_STR[static_cast<int>(status)]);
      assert(false);
    }
    log_debug("Requesting cold store checkpoint...");
    checkpoint->cold_store_status.store(StoreCheckpointStatus::REQUESTED);
  }
}

template<class K, class V, class D, class HHI, class CHI>
inline Status F2Kv<K, V, D, HHI, CHI>::Recover(const Guid& token, uint32_t& version, std::vector<Guid>& session_ids) {
  CheckpointPhase phase = CheckpointPhase::REST;
  if (!checkpoint_.phase.compare_exchange_strong(phase, CheckpointPhase::RECOVER)) {
    log_error("Unexpected checkpoint phase during recovery [expected: REST, actual: %s]",
              CHECKPOINT_PHASE_STR[static_cast<int>(phase)]);
    return Status::Aborted;
  }

  uint32_t hot_store_version, cold_store_version;
  if (hot_store.Recover(token, token, hot_store_version, session_ids) != Status::Ok) {
    log_error("Failed to recover hot store!");
    return Status::Aborted;
  }

  std::vector<Guid> temp_vector;
  if (cold_store.Recover(token, token, cold_store_version, temp_vector) != Status::Ok) {
    log_error("Failed to recover cold store!");
    return Status::Aborted;
  }

  if (hot_store_version != cold_store_version) {
    log_warn("Version of stores differ [hot: %u, cold %u]", hot_store_version, cold_store_version);
  }

  phase = CheckpointPhase::RECOVER;
  if (!checkpoint_.phase.compare_exchange_strong(phase, CheckpointPhase::REST)) {
    log_error("Unexpected checkpoint phase during recovery [expected: RECOVER, actual: %s]",
              CHECKPOINT_PHASE_STR[static_cast<int>(phase)]);
    return Status::Aborted;
  }
  return Status::Ok;
}

template<class K, class V, class D, class HHI, class CHI>
inline bool F2Kv<K, V, D, HHI, CHI>::CompactHotLog(uint64_t until_address, bool shift_begin_address, int n_threads) {
  return CompactLog(hot_store, StoreType::HOT, until_address, shift_begin_address, n_threads, false);
}

template<class K, class V, class D, class HHI, class CHI>
inline bool F2Kv<K, V, D, HHI, CHI>::CompactColdLog(uint64_t until_address, bool shift_begin_address, int n_threads) {
  return CompactLog(cold_store, StoreType::COLD, until_address, shift_begin_address, n_threads, false);
}

template<class K, class V, class D, class HHI, class CHI>
template <class S>
inline bool F2Kv<K, V, D, HHI, CHI>::CompactLog(S& store, StoreType store_type, uint64_t until_address,
                                                bool shift_begin_address, int n_threads, bool checkpoint) {
  const bool is_hot_store = (store_type == StoreType::HOT);

  uint64_t tail_address = store.hlog.GetTailAddress().control();
  log_debug("Compact %s: {%.2lf GB} {Goal %.2lf GB} [%lu %lu] -> [%lu %lu]",
            is_hot_store ? "HOT" : "COLD",
            static_cast<double>(store.Size()) / (1 << 30),
            static_cast<double>(tail_address - until_address) / (1 << 30),
            store.hlog.begin_address.control(), tail_address,
            until_address, tail_address);
  if (until_address > store.hlog.safe_read_only_address.control()) {
    throw std::invalid_argument{ "Can only compact until safe read-only region" };
  }

  StoreCheckpointStatus status;
  if (checkpoint) {
    assert(checkpoint_.phase.load() == CheckpointPhase::COLD_STORE_CHECKPOINT);
    if ((status = checkpoint_.cold_store_status.load()) != StoreCheckpointStatus::REQUESTED) {
      log_error("Unexpected checkpoint status [%s] for COLD store [expected: %s]",
                STORE_CHECKPOINT_STATUS_STR[static_cast<int>(status)],
                STORE_CHECKPOINT_STATUS_STR[static_cast<int>(StoreCheckpointStatus::REQUESTED)]);
      assert(false);
    }
    checkpoint_.cold_store_status.store(StoreCheckpointStatus::ACTIVE);
  }
  Guid token = checkpoint ? checkpoint_.token : Guid::Create();
  bool success = store.InternalCompactWithLookup(until_address, shift_begin_address,
                                                n_threads, is_hot_store, checkpoint, token);
  log_debug("Compact %s [success=%d]: {Goal %.2lf} {Actual %.2lf GB} Done!",
            is_hot_store ? "HOT": "COLD", success,
            static_cast<double>(tail_address - until_address) / (1 << 30),
            static_cast<double>(store.Size()) / (1 << 30));

  if (checkpoint) {
    assert(checkpoint_.phase.load() == CheckpointPhase::COLD_STORE_CHECKPOINT);
    if ((status = checkpoint_.cold_store_status.load()) != StoreCheckpointStatus::ACTIVE) {
      log_error("Unexpected checkpoint status [%s] for COLD store [expected: %s]",
                STORE_CHECKPOINT_STATUS_STR[static_cast<int>(status)],
                STORE_CHECKPOINT_STATUS_STR[static_cast<int>(StoreCheckpointStatus::ACTIVE)]);
      assert(false);
    }
    // TODO: get checkpoint result from store
    checkpoint_.cold_store_status.store(StoreCheckpointStatus::FINISHED);
  }
  return success;
}

template<class K, class V, class D, class HHI, class CHI>
template <class S>
inline bool F2Kv<K, V, D, HHI, CHI>::ShouldCompactHlog(const S& store, StoreType store_type,
                                                       const HlogCompactionConfig& compaction_config, uint64_t& until_address) {
  until_address = Address::kInvalidAddress;
  if (!compaction_config.enabled) {
    return false;
  }

  uint64_t hlog_size_threshold = static_cast<uint64_t>(
    compaction_config.hlog_size_budget * compaction_config.trigger_pct);
  if (store.Size() < hlog_size_threshold) {
    return false; // hlog smaller than limit
  }

  CheckpointPhase phase = checkpoint_.phase.load();
  switch (phase) {
    case CheckpointPhase::REST:
      break;
    case CheckpointPhase::HOT_STORE_CHECKPOINT:
      if (store_type == StoreType::HOT) {
        // Cannot start hot-cold compaction when hot store is being checkpointed
        // This is primarily due to the (destructive) SBA step of hot-cold compaction
        // ------------------------------------------------------------------------------------------
        // In theory, one could execute the compaction part w/ hot log checkpointing concurrently
        // (with proper care for starting/stopping compaction threads), and then explicitly wait for
        // hot-store checkpoint to finish, before issuing the final SBE part of hot-cold compaction
        return false;
      }
      break;
    case CheckpointPhase::COLD_STORE_CHECKPOINT:
      // Cold store checkpoint started, without waiting for next compaction process (i.e., high-priority)
      if (checkpoint_.cold_store_status.load() == StoreCheckpointStatus::ACTIVE) {
        // StoreType::HOT
        // ==============
        // When we want to start a hot-cold compaction, we need to start sessions on both store.
        // Yet, if a cold-store checkpoint is currently active, we cannot do so for the cold-store.
        // ----------------------------------------------------------------------------------------------
        // StoreType::COLD
        // ===============
        // Again, due to the (destructive) SBA step of the cold-cold compaction process, we need to wait..
        return false;
      }
      break;
    case CheckpointPhase::RECOVER:
      // Recovery in-progress! Should wait until it finishes!
      return false;
    default:
      assert(false); // not-reachable!
  }

  uint64_t begin_address = store.hlog.begin_address.control();
  uint64_t safe_head_address = store.hlog.safe_head_address.control();
  // calculate until address
  until_address = begin_address + (
              static_cast<uint64_t>(store.Size() * compaction_config.compact_pct));
  // respect user-imposed limit
  until_address = std::min(until_address, begin_address + compaction_config.max_compacted_size);
  // do not compact in-memory regions
  until_address = std::min(until_address, safe_head_address);
  // round down address to page bounds
  until_address = until_address - Address(until_address).offset() + Address::kMaxOffset + 1;
  assert(until_address <= store.hlog.safe_read_only_address.control());
  assert(until_address % store.hlog.kPageSize == 0);

  return (until_address < safe_head_address);
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::CheckSystemState() {
  const HlogCompactionConfig& hot_log_compaction_config = compaction_config_.hot_store;
  const HlogCompactionConfig& cold_log_compaction_config = compaction_config_.cold_store;

  // Used for throttling incoming user writes when respective max hlog size has been reached
  hot_store.max_hlog_size_ = hot_log_compaction_config.hlog_size_budget;
  cold_store.max_hlog_size_ = cold_log_compaction_config.hlog_size_budget;

  const std::chrono::milliseconds check_interval = std::min(
    hot_log_compaction_config.check_interval,
    cold_log_compaction_config.check_interval
  );
  bool wait;
  auto should_checkpoint_cold_store = [&cold_store_status = checkpoint_.cold_store_status]() {
    return (cold_store_status == StoreCheckpointStatus::REQUESTED);
  };
  uint64_t until_address;

  do {
    compaction_scheduled_.store(true);

    // Hot log checkpoint
    if (checkpoint_.hot_store_status == StoreCheckpointStatus::REQUESTED) {
      checkpoint_.SetNumActiveThreads(hot_store.NumActiveSessions()); // cannot be set previously due to other background ops
      log_debug("Issuing hot store checkpoint... (active sessions: %u)", hot_store.NumActiveSessions());
      bool success = hot_store.InternalCheckpoint(nullptr, F2Kv<K, V, D>::HotStoreCheckpointedCallback,
                                                  checkpoint_.token, static_cast<void*>(&checkpoint_));
      if (!success) {
        log_error("Hot store checkpoint failed!");
      }

      auto desired = success ? StoreCheckpointStatus::ACTIVE : StoreCheckpointStatus::FAILED;
      checkpoint_.hot_store_status.store(desired);
      log_debug("Hot store checkpoint issued! (status: %s)",
                STORE_CHECKPOINT_STATUS_STR[static_cast<int>(checkpoint_.hot_store_status)]);
    }

    // Cold log checkpoint (high-priority)
    if (should_checkpoint_cold_store() && checkpoint_.IsLazyCheckpointExpired()) {
      auto callback = [](void* ctxt, Status result, uint64_t persistent_serial_num) {
        auto checkpoint = static_cast<HotColdCheckpointState*>(ctxt);
        assert(checkpoint->phase.load() == CheckpointPhase::COLD_STORE_CHECKPOINT);
        assert(checkpoint->cold_store_status.load() == StoreCheckpointStatus::ACTIVE);

        StoreCheckpointStatus status;
        if ((status = checkpoint->cold_store_status.load()) != StoreCheckpointStatus::ACTIVE) {
          log_error("Unexpected checkpoint status for COLD store [expected: %s, actual: %s]",
                    STORE_CHECKPOINT_STATUS_STR[static_cast<int>(StoreCheckpointStatus::ACTIVE)],
                    STORE_CHECKPOINT_STATUS_STR[static_cast<int>(status)]);
          assert(false);
        }

        // Set checkpoint status for cold-log
        status = (result == Status::Ok) ? StoreCheckpointStatus::FINISHED : StoreCheckpointStatus::FAILED;
        checkpoint->cold_store_status.store(status);
        log_debug("Cold store checkpoint result: %s", STATUS_STR[static_cast<int>(result)]);
      };

      log_debug("Issuing cold store checkpoint... (active sessions: %u)", cold_store.NumActiveSessions());
      bool success = cold_store.InternalCheckpoint(nullptr, callback, checkpoint_.token,
                                                  static_cast<void*>(&checkpoint_));
      if (!success) {
        log_error("Cold store checkpoint failed!");
      }

      auto desired = success ? StoreCheckpointStatus::ACTIVE : StoreCheckpointStatus::FAILED;
      checkpoint_.cold_store_status.store(desired);
      log_debug("Cold store checkpoint issued! (status: %s)",
                STORE_CHECKPOINT_STATUS_STR[static_cast<int>(checkpoint_.cold_store_status)]);
    }

    // Hot-Cold compaction (optional cold-log checkpoint)
    if (ShouldCompactHlog(hot_store, StoreType::HOT, hot_log_compaction_config, until_address)) {
      hot_store.StartSession();
      cold_store.StartSession();
      if (!CompactLog(hot_store, StoreType::HOT, until_address, true,
                      hot_log_compaction_config.num_threads, should_checkpoint_cold_store())) {
        log_error("Compact HOT log failed! :(");
      }
      cold_store.StopSession();
      hot_store.StopSession();
    }

    // Cold-Cold compaction (optional cold-log checkpoint)
    if (ShouldCompactHlog(cold_store, StoreType::COLD, cold_log_compaction_config, until_address)) {
      cold_store.StartSession();
      if (!CompactLog(cold_store, StoreType::COLD, until_address, true,
                      cold_log_compaction_config.num_threads, should_checkpoint_cold_store())) {
        log_error("Compact COLD log failed!  :(");
      }
      cold_store.StopSession();
    }

    wait = (checkpoint_.hot_store_status != StoreCheckpointStatus::REQUESTED &&
            !ShouldCompactHlog(hot_store, StoreType::HOT, hot_log_compaction_config, until_address) &&
            !ShouldCompactHlog(cold_store, StoreType::COLD, cold_log_compaction_config, until_address));
    if (wait) {
      compaction_scheduled_.store(false);
      std::this_thread::sleep_for(check_interval);
    }

  } while(background_worker_active_.load());

  compaction_scheduled_.store(false);
}

template<class K, class V, class D, class HHI, class CHI>
inline void F2Kv<K, V, D, HHI, CHI>::CompleteRmwRetryRequests() {
  typedef F2IndexContext<async_f2_rmw_context_t> f2_index_context_t;

  async_f2_rmw_context_t* ctxt;
  while (retry_rmw_requests.try_pop(ctxt)) {
    CallbackContext<async_f2_rmw_context_t> context{ ctxt };
    // Get hash bucket entry
    f2_index_context_t index_context{ context.get() };
    Status index_status = hot_store.hash_index_.template FindOrCreateEntry<f2_index_context_t>(
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

#ifdef TOML_CONFIG
template<class K, class V, class D, class HHI, class CHI>
inline F2Kv<K, V, D, HHI, CHI> F2Kv<K, V, D, HHI, CHI>::FromConfigString(const std::string& config) {
  hot_faster_store_config_t hot_store_config = hot_faster_store_t::Config::FromConfigString(config, "f2", "hot");
  cold_faster_store_config_t cold_store_config = cold_faster_store_t::Config::FromConfigString(config, "f2", "cold");

  // NOTE: Compactions config are handled by constructor(s)
  return { hot_store_config, cold_store_config };
}

template<class K, class V, class D, class HHI, class CHI>
inline F2Kv<K, V, D, HHI, CHI> F2Kv<K, V, D, HHI, CHI>::FromConfigFile(const std::string& filepath) {
  std::ifstream t(filepath);
  std::string config(
    (std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
  return F2Kv<K, V, D>::FromConfigString(config);
}
#endif

}
} // namespace FASTER::core