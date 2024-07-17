// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING

#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <type_traits>
#include <algorithm>
#include <unordered_set>

#ifdef STATISTICS
#include <chrono>
#endif

#include "common/log.h"
#include "device/file_system_disk.h"
#include "index/mem_index.h"
#include "index/faster_index.h"

#include "address.h"
#include "alloc.h"
#include "checkpoint_locks.h"
#include "checkpoint_state.h"
#include "constants.h"
#include "compact.h"
#include "config.h"
#include "gc_state.h"
#include "grow_state.h"
#include "guid.h"
#include "internal_contexts.h"
#include "key_hash.h"
#include "log_scan.h"
#include "malloc_fixed_page_size.h"
#include "persistent_memory_malloc.h"
#include "read_cache.h"
#include "record.h"
#include "recovery_status.h"
#include "state_transitions.h"
#include "status.h"
#include "utility.h"

using namespace std::chrono_literals;
using namespace FASTER::index;

/// The FASTER key-value store, and related classes.

namespace FASTER {
namespace core {

// Forward class declaration
template<class K, class V, class D>
class FasterKvHC;

class alignas(Constants::kCacheLineBytes) ThreadContext {
 public:
  ThreadContext()
    : contexts_{}
    , cur_{ 0 } {
  }

  inline const ExecutionContext& cur() const {
    return contexts_[cur_];
  }
  inline ExecutionContext& cur() {
    return contexts_[cur_];
  }

  inline const ExecutionContext& prev() const {
    return contexts_[(cur_ + 1) % 2];
  }
  inline ExecutionContext& prev() {
    return contexts_[(cur_ + 1) % 2];
  }

  inline void swap() {
    cur_ = (cur_ + 1) % 2;
  }

 private:
  ExecutionContext contexts_[2];
  uint8_t cur_;
};
static_assert(sizeof(ThreadContext) == 448, "sizeof(ThreadContext) != 448");

/// The FASTER key-value store.
template <class K, class V, class D, class H = HashIndex<D>, class OH = H>
class FasterKv {
 public:
  typedef FasterKv<K, V, D, H, OH> faster_t;
  typedef FasterKv<K, V, D, OH, H> other_faster_t;

  friend class FasterKvHC<K, V, D>;
  friend class FASTER::index::FasterIndex<D, typename H::hash_index_definition_t>;


  // Make friend all templated instances of this class
  template <class Ki, class Vi, class Di, class Hi, class OHi>
  friend class FasterKv;

  /// Keys and values stored in this key-value store.
  typedef K key_t;
  typedef V value_t;

  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef typename D::log_file_t log_file_t;

  typedef PersistentMemoryMalloc<disk_t> hlog_t;
  typedef ConcurrentLogPage<key_t, value_t> concurrent_log_page_t;

  typedef H hash_index_t;
  typedef typename H::key_hash_t key_hash_t;
  typedef typename H::Config IndexConfig;
  typedef FASTER::index::HashBucketEntry HashBucketEntry;

  typedef FasterStoreConfig<hash_index_t> Config;

  typedef ReadCache<K, V, D, H> read_cache_t;

  /// Contexts that have been deep-copied, for async continuations, and must be accessed via
  /// virtual function calls.
  typedef AsyncPendingReadContext<key_t> async_pending_read_context_t;
  typedef AsyncPendingUpsertContext<key_t> async_pending_upsert_context_t;
  typedef AsyncPendingRmwContext<key_t> async_pending_rmw_context_t;
  typedef AsyncPendingDeleteContext<key_t> async_pending_delete_context_t;
  typedef AsyncPendingConditionalInsertContext<key_t> async_pending_ci_context_t;

  static constexpr uint64_t kMaxPendingIOs = 128;

  FasterKv(IndexConfig index_config, uint64_t hlog_mem_size, const std::string& filepath,
           double hlog_mutable_fraction = DEFAULT_HLOG_MUTABLE_FRACTION,
           ReadCacheConfig rc_config = DEFAULT_READ_CACHE_CONFIG,
           HlogCompactionConfig hlog_compaction_config = DEFAULT_HLOG_COMPACTION_CONFIG,
           bool pre_allocate_log = false, const std::string& config = "")
    : min_index_table_size_{ index_config.table_size }
    , disk{ filepath, epoch_, config }
    , hlog{ filepath.empty() /*hasNoBackingStorage*/, hlog_mem_size, epoch_, disk, disk.log(), hlog_mutable_fraction, pre_allocate_log }
    , system_state_{ Action::None, Phase::REST, 1 }
    , grow_state_{ &hlog }
    , hash_index_{ filepath, disk, epoch_, gc_state_, grow_state_ }
    , read_cache_{ nullptr }
    , num_pending_ios_{ 0 }
    , num_compaction_truncations_{ 0 }
    , next_hlog_begin_address_{ Address::kInvalidAddress }
    , num_active_sessions_{ 0 }
    , other_store_{ nullptr }
    , refresh_callback_store_{ nullptr }
    , refresh_callback_{ nullptr }
    , hlog_compaction_config_{ hlog_compaction_config }
    , auto_compaction_active_{ false }
    , auto_compaction_scheduled_{ false } {

    log_debug("Hash Index Size: %lu", index_config.table_size);
    hash_index_.Initialize(index_config);
    if (!hash_index_.IsSync()) {
      hash_index_.SetRefreshCallback(static_cast<void*>(this), DoRefreshCallback);
    }

    log_debug("Read cache is %s", rc_config.enabled ? "ENABLED" : "DISABLED");
    if (rc_config.enabled) {
      read_cache_ = std::make_unique<read_cache_t>(epoch_, hash_index_, hlog,
                                                  BlockAllocateReadCacheCallback, rc_config);
      read_cache_->SetFasterInstance(static_cast<void*>(this));
    }

    log_debug("Auto compaction is %s", hlog_compaction_config.enabled ? "ENABLED" : "DISABLED");
    if (hlog_compaction_config.enabled) {
      auto_compaction_active_.store(true);
      compaction_thread_ = std::move(std::thread(&FasterKv::AutoCompactHlog, this));
    }

    #ifdef STATISTICS
    InitializeStats();
    #endif
  }

  FasterKv(const Config& config)
    : FasterKv{ config.index_config, config.hlog_config.in_mem_size, config.filepath,
                config.hlog_config.mutable_fraction, config.rc_config,
                config.hlog_compaction_config, config.hlog_config.pre_allocate } {
  }

  ~FasterKv() {
    while (system_state_.phase() != Phase::REST) {
      std::this_thread::yield();
    }
    if (compaction_thread_.joinable()) {
      // shut down compaction thread
      auto_compaction_active_.store(false);
      compaction_thread_.join();
    }
  }

  // No copy constructor.
  FasterKv(const FasterKv& other) = delete;

 public:
  /// Thread-related operations
  Guid StartSession(Guid guid = Guid());
  uint64_t ContinueSession(const Guid& guid);
  void StopSession();
  void Refresh(bool from_callback = false);

  /// Store interface
  template <class RC>
  inline Status Read(RC& context, AsyncCallback callback, uint64_t monotonic_serial_num,
                      bool abort_if_tombstone = false);

  template <class UC>
  inline Status Upsert(UC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class MC>
  inline Status Rmw(MC& context, AsyncCallback callback, uint64_t monotonic_serial_num,
                      bool create_if_not_exists = true);

  template <class DC>
  inline Status Delete(DC& context, AsyncCallback callback, uint64_t monotonic_serial_num,
                        bool force_tombstone = false);

  inline bool CompletePending(bool wait = false);

  /// Checkpoint/recovery operations.
  bool Checkpoint(IndexPersistenceCallback index_persistence_callback,
                  HybridLogPersistenceCallback hybrid_log_persistence_callback,
                  Guid& token);
  bool CheckpointIndex(IndexPersistenceCallback index_persistence_callback, Guid& token);
  bool CheckpointHybridLog(HybridLogPersistenceCallback hybrid_log_persistence_callback,
                            Guid& token);
  Status Recover(const Guid& index_token, const Guid& hybrid_log_token, uint32_t& version,
                 std::vector<Guid>& session_ids);

  /// Log compaction entry method.
  bool Compact(uint64_t untilAddress);
  bool CompactWithLookup(uint64_t until_address, bool shift_begin_address,
                          int n_threads = kNumCompactionThreads,
                          bool to_other_store = false,
                          bool checkpoint = false);

  /// Truncating the head of the log.
  bool ShiftBeginAddress(Address address, GcState::truncate_callback_t truncate_callback,
                         GcState::complete_callback_t complete_callback);

  /// Make the hash table larger.
  bool GrowIndex(GrowCompleteCallback caller_callback);

  #ifdef TOML_CONFIG
  static faster_t FromConfigString(const std::string& config);
  static faster_t FromConfigFile(const std::string& filepath);
  #endif

  /// Statistics
  inline uint64_t Size() const {
    return hlog.GetTailAddress().control() - hlog.begin_address.control();
  }
  inline void DumpDistribution() {
    hash_index_.DumpDistribution();
  }
  inline uint32_t NumActiveSessions() const {
    return num_active_sessions_.load();
  }
  inline bool AutoCompactionScheduled() const {
    return auto_compaction_scheduled_.load();
  }


 private:
  typedef Record<key_t, value_t> record_t;
  typedef PendingContext<key_t> pending_context_t;

 public:
  template <class C>
  inline OperationStatus InternalRead(C& pending_context) const;

  template <class C>
  inline OperationStatus InternalUpsert(C& pending_context);

  template <class C>
  inline OperationStatus InternalRmw(C& pending_context, bool retrying);

  inline OperationStatus InternalRetryPendingRmw(async_pending_rmw_context_t& pending_context);

  template<class C>
  inline OperationStatus InternalDelete(C& pending_context, bool force_tombstone);

 private:
  void InternalContinuePendingRequest(ExecutionContext& ctx, AsyncIOContext& io_context);
  OperationStatus InternalContinuePendingRead(ExecutionContext& ctx,
      AsyncIOContext& io_context, bool& log_truncated);
  OperationStatus InternalContinuePendingRmw(ExecutionContext& ctx,
      AsyncIOContext& io_context);
  OperationStatus InternalContinuePendingConditionalInsert(ExecutionContext& ctx,
      AsyncIOContext& io_context);

  inline bool InternalCompactWithLookup(uint64_t until_address, bool shift_begin_address,
                                  int n_threads, bool to_other_store, bool checkpoint, Guid& token);

  bool InternalCheckpoint(InternalIndexPersistenceCallback index_persistence_callback,
                  InternalHybridLogPersistenceCallback hybrid_log_persistence_callback,
                  Guid& token, void* callback_context);

  bool InternalShiftBeginAddress(Address address, GcState::internal_truncate_callback_t truncate_callback,
                                 GcState::internal_complete_callback_t complete_callback,
                                 IAsyncContext* callback_context, bool after_compaction = false);

  template<class F>
  inline void InternalCompact(int thread_id, uint32_t max_records);

  template<class C>
  inline Address TraceBackForKeyMatchCtxt(const C& ctxt, Address from_address,
                                      Address min_offset) const;
  inline Address TraceBackForKeyMatch(const key_t& key, Address from_address,
                                      Address min_offset) const;
  Address TraceBackForOtherChainStart(uint64_t old_size,  uint64_t new_size, Address from_address,
                                      Address min_address, uint8_t side);

  inline Address BlockAllocate(uint32_t record_size);
  static Address BlockAllocateReadCacheCallback(void* faster, uint32_t record_size);
  inline Address BlockAllocateReadCache(uint32_t record_size);

  static void DoRefreshCallback(void* faster);

  inline Status HandleOperationStatus(ExecutionContext& ctx,
                                      pending_context_t& pending_context,
                                      OperationStatus internal_status, bool& async);
  inline Status PivotAndRetry(ExecutionContext& ctx, pending_context_t& pending_context,
                              bool& async);
  inline Status RetryLater(ExecutionContext& ctx, pending_context_t& pending_context,
                           bool& async);
  inline Status IssueAsyncIoRequest(ExecutionContext& ctx, pending_context_t& pending_context,
                                    bool& async);
  void DoThrottling();
  void AsyncGetFromDisk(Address address, uint32_t num_records, AsyncIOCallback callback,
                        AsyncIOContext& context);
  static void AsyncGetFromDiskCallback(IAsyncContext* ctxt, Status result,
                                       size_t bytes_transferred);

  void CompleteIoPendingRequests(ExecutionContext& context);
  void CompleteIndexPendingRequests(ExecutionContext& context);
  void CompleteRetryRequests(ExecutionContext& context);

  template<class CIC>
  inline Status ConditionalInsert(CIC& context, AsyncCallback callback,
                                  Address min_start_offset, bool to_other_store);

  template<class C>
  inline OperationStatus InternalConditionalInsert(C& pending_context);

  /// Checkpoint/recovery methods.
  void InitializeCheckpointLocks();
  void HandleSpecialPhases();
  bool GlobalMoveToNextState(SystemState current_state);

  Status WriteCprMetadata();
  Status ReadCprMetadata(const Guid& token);
  Status WriteCprContext();
  Status ReadCprContexts(const Guid& token, const Guid* guids);

  Status RecoverHybridLog();
  Status RecoverHybridLogFromSnapshotFile();
  Status RecoverFromPage(Address from_address, Address to_address);
  Status RestoreHybridLog();

  void MarkAllPendingRequests();

  /// Grow Index methods
  inline void HeavyEnter();
  void GrowIndexBlocking();

  /// Compaction/Garbage collect methods
  Address LogScanForValidity(Address from, faster_t* temp);
  bool ContainsKeyInMemory(IndexContext<key_t, value_t> key, Address offset);

  // Auto-compaction daemon
  void AutoCompactHlog();

  /// Access the current and previous (thread-local) execution contexts.
  const ExecutionContext& thread_ctx() const {
    return thread_contexts_[Thread::id()].cur();
  }
  ExecutionContext& thread_ctx() {
    return thread_contexts_[Thread::id()].cur();
  }
  ExecutionContext& prev_thread_ctx() {
    return thread_contexts_[Thread::id()].prev();
  }

  inline constexpr uint32_t MinIoRequestSize() const {
    return static_cast<uint32_t>(
            sizeof(value_t) + pad_alignment(record_t::min_disk_key_size(),
                alignof(value_t)));
  }

  bool UseReadCache() const {
    return (read_cache_.get() != nullptr);
  }
  void SetOtherStore(other_faster_t* other_store) {
    assert(other_store_ == nullptr); // set only once
    other_store_ = other_store;
  }
 public:
  void SetRefreshCallback(void* faster, RefreshCallback refresh_callback) {
    assert(refresh_callback_store_ == nullptr);
    refresh_callback_ = refresh_callback;
    refresh_callback_store_ = faster;
  }

 private:
  LightEpoch epoch_;

 public:
  disk_t disk;
  hlog_t hlog;

  hash_index_t hash_index_;
  std::unique_ptr<read_cache_t> read_cache_;

 private:
  static constexpr bool kCopyReadsToTail = false;
  static constexpr int kNumCompactionThreads = 8;

  bool fold_over_snapshot = true;

  /// Initial size of the index table
  uint64_t min_index_table_size_;

  CheckpointLocks<key_hash_t> checkpoint_locks_;

  AtomicSystemState system_state_;

  /// Checkpoint/recovery state.
  CheckpointState<file_t> checkpoint_;
  /// Garbage collection state.
  typename hash_index_t::gc_state_t gc_state_;
  /// Grow (hash table) state.
  typename hash_index_t::grow_state_t grow_state_;

  /// Global count of pending I/Os, used for throttling.
  std::atomic<uint64_t> num_pending_ios_;

  /// Global count of number of truncations after compaction
  std::atomic<uint64_t> num_compaction_truncations_;

  // Context of threads participating in the hybrid log compaction
  //CompactionThreadsContext<faster_t> compaction_context_;
  ConcurrentCompactionThreadsContext<faster_t> compaction_context_;

  /// Hlog begin address after the compaction & log trimming is finished
  AtomicAddress next_hlog_begin_address_;

  /// Number of active sessions
  std::atomic<size_t> num_active_sessions_;

  // Pointer to other FasterKv instance (if hot-cold); else nullptr
  // Required for the Epoch framework to work properly
  other_faster_t* other_store_;

  void* refresh_callback_store_;
  RefreshCallback refresh_callback_;

  /// Space for two contexts per thread, stored inline.
  ThreadContext thread_contexts_[Thread::kMaxNumThreads];

  /// Hlog auto-compaction
  HlogCompactionConfig hlog_compaction_config_;

  std::thread compaction_thread_;
  std::atomic<bool> auto_compaction_active_;
  std::atomic<bool> auto_compaction_scheduled_;

  // Hard limit of log size
  uint64_t max_hlog_size_;


#ifdef STATISTICS
 public:
  void EnableStatsCollection() {
    hash_index_.EnableStatsCollection();
    collect_stats_ = true;
  }
  void DisableStatsCollection() {
    hash_index_.DisableStatsCollection();
    collect_stats_ = false;
  }

  void PrintStats() const {
    // Reads
    fprintf(stderr, "==== READS ====\n");
    fprintf(stderr, "Total Requests Served\t: %lu\n", reads_total_.load());
    if (reads_total_.load() > 0) {
      fprintf(stderr, "Sync Requests (%%)\t: %.2lf\n",
        (static_cast<double>(reads_sync_.load()) / reads_total_.load()) * 100.0);
      fprintf(stderr, "\nTotal I/O operations\t: %lu\n", reads_iops_.load());
      fprintf(stderr, "I/O Op Per Request\t: %.2lf\n",
        static_cast<double>(reads_iops_.load()) / reads_total_.load());
      pprint_per_req_stats(reads_per_req_iops_, "I/O ops");
    }
    // Upserts
    fprintf(stderr, "\n==== UPSERTS ====\n");
    fprintf(stderr, "Total Requests Served\t: %lu\n", upserts_total_.load());
    if (upserts_total_.load() > 0) {
      fprintf(stderr, "Sync Requests (%%)\t: %.2lf\n",
        (static_cast<double>(upserts_sync_.load()) / upserts_total_.load()) * 100.0);
      fprintf(stderr, "\nTotal I/O operations\t: %lu\n", upserts_iops_.load());
      fprintf(stderr, "I/O Op Per Request\t: %.2lf\n",
        static_cast<double>(upserts_iops_.load()) / upserts_total_.load());
      pprint_per_req_stats(upserts_per_req_iops_, "I/O ops");
      pprint_per_req_stats(upserts_per_req_record_invalidations_, "Record Invalidations");
    }
    // RMWs
    fprintf(stderr, "\n==== RMWs ====\n");
    fprintf(stderr, "Total Requests Served\t: %lu\n", rmw_total_.load());
    if (rmw_total_.load() > 0) {
      fprintf(stderr, "Sync Requests (%%)\t: %.2lf\n",
        (static_cast<double>(rmw_sync_.load()) / rmw_total_.load()) * 100.0);
      fprintf(stderr, "Total I/O operations\t: %lu\n", rmw_iops_.load());
      fprintf(stderr, "I/O Op Per Request\t: %.2lf\n",
        static_cast<double>(rmw_iops_.load()) / rmw_total_.load());
      pprint_per_req_stats(rmw_per_req_iops_, "I/O ops");
      pprint_per_req_stats(rmw_per_req_record_invalidations_, "Record Invalidations");
    }
    // Deletes
    fprintf(stderr, "\n==== DELETES ====\n");
    fprintf(stderr, "Total Requests Served\t: %lu\n", deletes_total_.load());
    if (deletes_total_.load() > 0) {
      fprintf(stderr, "Sync Requests (%%)\t: %.2lf\n",
        (static_cast<double>(deletes_sync_.load()) / deletes_total_.load()) * 100.0);
      fprintf(stderr, "Total I/O operations\t: %lu\n", deletes_iops_.load());
      fprintf(stderr, "I/O Op Per Request\t: %.2lf\n",
        static_cast<double>(deletes_iops_.load()) / deletes_total_.load());
      pprint_per_req_stats(deletes_per_req_iops_, "I/O ops");
      pprint_per_req_stats(deletes_per_req_record_invalidations_, "Record Invalidations");
    }
    // Conditional Inserts
    fprintf(stderr, "\n=== Conditional Inserts ===\n");
    fprintf(stderr, "Total Requests Served\t: %lu\n", ci_total_.load());
    if (ci_total_.load() > 0) {
      fprintf(stderr, "Sync Requests (%%)\t: %.2lf\n",
        (static_cast<double>(ci_sync_.load()) / ci_total_.load()) * 100.0);
      fprintf(stderr, "Total I/O operations \t: %lu\n", ci_iops_.load());
      fprintf(stderr, "I/O Op Per Request\t: %.2lf\n",
        static_cast<double>(ci_iops_.load()) / ci_total_.load());
      fprintf(stderr, "Records Copied (%%)\t: %.2lf\n",
        static_cast<double>(ci_copied_.load()) / ci_total_.load() * 100.0);
      pprint_per_req_stats(ci_per_req_iops_, "I/O ops");
      pprint_per_req_stats(ci_per_req_record_invalidations_, "Record Invalidations");
    }
    // Misc
    fprintf(stderr, "\n=== Misc ===\n");
    fprintf(stderr, "Refresh()\t\tTime Spent\t: %ld ms\n", refresh_func_time_spent_.load() / 1000);
    fprintf(stderr, "HandleSpecialPhases() Time Spent\t: %ld ms\n", special_phases_time_spent_.load() / 1000);
    fprintf(stderr, "Throttling\t\tTime Spent\t: %ld ms\n", throttling_time_spent_.load() / 1000);

    // Print hash index stats
    fprintf(stderr, "\n ########## HASH INDEX #########\n");
    hash_index_.PrintStats();
    fprintf(stderr, "\n ########## ---------- #########\n");
  }

 private:
  void InitializeStats() {
    for (int i = 0; i < kPerReqResolution; i++) {
      // iops
      reads_per_req_iops_[i].store(0);
      upserts_per_req_iops_[i].store(0);
      rmw_per_req_iops_[i].store(0);
      deletes_per_req_iops_[i].store(0);
      ci_per_req_iops_[i].store(0);
      // record invalidations
      upserts_per_req_record_invalidations_[i].store(0);
      rmw_per_req_record_invalidations_[i].store(0);
      deletes_per_req_record_invalidations_[i].store(0);
      ci_per_req_record_invalidations_[i].store(0);
    }
  }

  void UpdatePerReqStats(pending_context_t* pending_context) {
    if (collect_stats_) {
      auto num_iops = std::min(pending_context->num_iops, 5U);
      auto num_record_invalidations = std::min(pending_context->num_record_invalidations, 5U);
      switch(pending_context->type) {
        case OperationType::Read:
          ++reads_per_req_iops_[num_iops];
          break;
        case OperationType::Upsert:
          // possible due to cold-index reqs
          ++upserts_per_req_iops_[num_iops];
          ++upserts_per_req_record_invalidations_[num_record_invalidations];
          break;
        case OperationType::RMW:
          ++rmw_per_req_iops_[num_iops];
          ++rmw_per_req_record_invalidations_[num_record_invalidations];
          break;
        case OperationType::Delete:
          // possible due to cold-index reqs
          ++deletes_per_req_iops_[num_iops];
          ++deletes_per_req_record_invalidations_[num_record_invalidations];
          break;
        case OperationType::ConditionalInsert:
          ++ci_per_req_iops_[num_iops];
          ++ci_per_req_record_invalidations_[num_record_invalidations];
          break;
        default:
          assert(false);
          break;
      }
    }
  }

  void pprint_per_req_stats(const std::atomic<uint64_t>* iops_arr, std::string prefix) const {
    static const unsigned allDots = 60;

    uint64_t sum = 0;
    for (int i = 0; i < kPerReqResolution; i++) {
      sum += iops_arr[i].load();
    }
    if (sum == 0) return;

    for (int i = 0; i < kPerReqResolution; i++) {
      auto num_iops = iops_arr[i].load();
      fprintf(stderr, "%d%c  %s [%'10lu] {%6.2lf%%}: ", i,
        (i != kPerReqResolution - 1) ? ' ' : '+', prefix.c_str(),
        num_iops, static_cast<double>(num_iops) / sum * 100.0);
      auto dots = (iops_arr[i].load() * allDots) / sum;
      fprintf(stderr, "%s\n", std::string(dots, '*').c_str());
    }
  }

  const double kSketchAccuracy = 0.01;
  static constexpr unsigned kPerReqResolution = 6;
  bool collect_stats_{ true };
  // Reads
  std::atomic<uint64_t> reads_total_{ 0 };
  std::atomic<uint64_t> reads_sync_{ 0 };
  std::atomic<uint64_t> reads_iops_{ 0 };
  std::atomic<uint64_t> reads_per_req_record_invalidations_[kPerReqResolution];
  std::atomic<uint64_t> reads_per_req_iops_[kPerReqResolution];
  // Upserts
  std::atomic<uint64_t> upserts_total_{ 0 };
  std::atomic<uint64_t> upserts_sync_{ 0 };
  std::atomic<uint64_t> upserts_iops_{ 0 };
  std::atomic<uint64_t> upserts_per_req_record_invalidations_[kPerReqResolution];
  std::atomic<uint64_t> upserts_per_req_iops_[kPerReqResolution];
  // RMWs
  std::atomic<uint64_t> rmw_total_{ 0 };
  std::atomic<uint64_t> rmw_sync_{ 0 };
  std::atomic<uint64_t> rmw_iops_{ 0 };
  std::atomic<uint64_t> rmw_per_req_record_invalidations_[kPerReqResolution];
  std::atomic<uint64_t> rmw_per_req_iops_[kPerReqResolution];
  // Deletes
  std::atomic<uint64_t> deletes_total_{ 0 };
  std::atomic<uint64_t> deletes_sync_{ 0 };
  std::atomic<uint64_t> deletes_iops_{ 0 };
  std::atomic<uint64_t> deletes_per_req_record_invalidations_[kPerReqResolution];
  std::atomic<uint64_t> deletes_per_req_iops_[kPerReqResolution];
  // Conditional Inserts
  std::atomic<uint64_t> ci_total_{ 0 };
  std::atomic<uint64_t> ci_sync_{ 0 };
  std::atomic<uint64_t> ci_iops_{ 0 };
  std::atomic<uint64_t> ci_copied_{ 0 };
  std::atomic<uint64_t> ci_per_req_record_invalidations_[kPerReqResolution];
  std::atomic<uint64_t> ci_per_req_iops_[kPerReqResolution];
  // Misc
  std::atomic<int64_t> refresh_func_time_spent_{ 0 };
  std::atomic<int64_t> throttling_time_spent_{ 0 };
  std::atomic<int64_t> special_phases_time_spent_{ 0 };
#endif
};

// Implementations.
template <class K, class V, class D, class H, class OH>
inline Guid FasterKv<K, V, D, H, OH>::StartSession(Guid guid) {
  hash_index_.StartSession();
  SystemState state = system_state_.load();
  if(state.phase != Phase::REST) {
    throw std::runtime_error{ "Can acquire only in REST phase!" };
  }
  if (Guid::IsNull(guid)) {
    guid = Guid::Create();
  }
  thread_ctx().Initialize(state.phase, state.version, guid, 0);
  epoch_.ProtectAndDrain();
  Refresh();
  ++num_active_sessions_;
  return thread_ctx().guid;
}

template <class K, class V, class D, class H, class OH>
inline uint64_t FasterKv<K, V, D, H, OH>::ContinueSession(const Guid& session_id) {
  // TODO: Look into whether we can have a mapping of log session ids -> hash index session ids
  hash_index_.StartSession();

  auto iter = checkpoint_.continue_tokens.find(session_id);
  if(iter == checkpoint_.continue_tokens.end()) {
    throw std::invalid_argument{ "Unknown session ID" };
  }

  SystemState state = system_state_.load();
  if(state.phase != Phase::REST) {
    throw std::runtime_error{ "Can continue only in REST phase!" };
  }
  thread_ctx().Initialize(state.phase, state.version, session_id, iter->second);
  epoch_.ProtectAndDrain();
  Refresh();
  ++num_active_sessions_;
  return iter->second;
}

template <class K, class V, class D, class H, class OH>
inline void FasterKv<K, V, D, H, OH>::Refresh(bool from_callback) {
  #ifdef STATISTICS
  auto start_time = std::chrono::high_resolution_clock::now();
  #endif

  if (!epoch_.IsProtected()) {
    log_warn("[FasterKv=%p] Thread [%lu] called Refresh(), with no active session",
              static_cast<void*>(this), Thread::id());
  }
  epoch_.ProtectAndDrain();
  if (from_callback) {
    if (other_store_ && other_store_->epoch_.IsProtected()) {
      other_store_->Refresh();
    }
  } else {
    // avoid inf recursion
    hash_index_.Refresh();
  }
  // We check if we are in normal mode
  SystemState new_state = system_state_.load();
  if(thread_ctx().phase == Phase::REST && new_state.phase == Phase::REST) {
    return;
  }

  #ifdef STATISTICS
  std::chrono::nanoseconds duration = std::chrono::high_resolution_clock::now() - start_time;
  refresh_func_time_spent_ += duration.count();
  #endif

  HandleSpecialPhases();
}

template <class K, class V, class D, class H, class OH>
inline void FasterKv<K, V, D, H, OH>::StopSession() {
  // If this thread is still involved in some activity, wait until it finishes.
  while(thread_ctx().phase != Phase::REST ||
        !thread_ctx().pending_ios.empty() ||
        !thread_ctx().retry_requests.empty()) {
    CompletePending(false);
    std::this_thread::yield();
  }

  assert(thread_ctx().retry_requests.empty());
  assert(thread_ctx().pending_ios.empty());
  assert(thread_ctx().io_responses.empty());

  assert(prev_thread_ctx().retry_requests.empty());
  assert(prev_thread_ctx().pending_ios.empty());
  assert(prev_thread_ctx().io_responses.empty());

  assert(thread_ctx().phase == Phase::REST);
  hash_index_.StopSession();

  epoch_.Unprotect();
  --num_active_sessions_;
}

template <class K, class V, class D, class H, class OH>
template <class RC>
inline Status FasterKv<K, V, D, H, OH>::Read(RC& context, AsyncCallback callback,
                                      uint64_t monotonic_serial_num, bool abort_if_tombstone) {
  typedef RC read_context_t;
  typedef PendingReadContext<RC> pending_read_context_t;
  static_assert(std::is_base_of<value_t, typename read_context_t::value_t>::value,
                "value_t is not a base class of read_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename read_context_t::value_t),
                "alignof(value_t) != alignof(typename read_context_t::value_t)");

  pending_read_context_t pending_context{ context, callback, abort_if_tombstone,
                                          Address::kInvalidAddress, num_compaction_truncations_.load() };
  OperationStatus internal_status = InternalRead(pending_context);

  #ifdef STATISTICS
  if (collect_stats_) {
    ++reads_total_;
    if (internal_status == OperationStatus::SUCCESS ||
        internal_status == OperationStatus::NOT_FOUND ||
        internal_status == OperationStatus::ABORTED) {
      ++reads_sync_;
      ++reads_per_req_iops_[0];
    }
  }
  #endif

  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else if(internal_status == OperationStatus::NOT_FOUND) {
    status = Status::NotFound;
  } else if (internal_status == OperationStatus::ABORTED) {
    assert(abort_if_tombstone);
    status = Status::Aborted;
  } else {
    assert(internal_status == OperationStatus::RECORD_ON_DISK ||
          internal_status == OperationStatus::INDEX_ENTRY_ON_DISK);
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  return status;
}

template <class K, class V, class D, class H, class OH>
template <class UC>
inline Status FasterKv<K, V, D, H, OH>::Upsert(UC& context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num) {
  typedef UC upsert_context_t;
  typedef PendingUpsertContext<UC> pending_upsert_context_t;
  static_assert(std::is_base_of<value_t, typename upsert_context_t::value_t>::value,
                "value_t is not a base class of upsert_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename upsert_context_t::value_t),
                "alignof(value_t) != alignof(typename upsert_context_t::value_t)");

  pending_upsert_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalUpsert(pending_context);

  #ifdef STATISTICS
  if (collect_stats_) {
    ++upserts_total_;
    if (internal_status == OperationStatus::SUCCESS) {
      ++upserts_sync_;
      ++upserts_per_req_iops_[0];
    }
  }
  #endif

  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else {
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  return status;
}

template <class K, class V, class D, class H, class OH>
template <class MC>
inline Status FasterKv<K, V, D, H, OH>::Rmw(MC& context, AsyncCallback callback,
                                     uint64_t monotonic_serial_num, bool create_if_not_exists) {
  typedef MC rmw_context_t;
  typedef PendingRmwContext<MC> pending_rmw_context_t;
  static_assert(std::is_base_of<value_t, typename rmw_context_t::value_t>::value,
                "value_t is not a base class of rmw_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename rmw_context_t::value_t),
                "alignof(value_t) != alignof(typename rmw_context_t::value_t)");

  pending_rmw_context_t pending_context{ context, callback, create_if_not_exists };
  OperationStatus internal_status = InternalRmw(pending_context, false);

  #ifdef STATISTICS
  if (collect_stats_) {
    ++rmw_total_;
    if (internal_status == OperationStatus::SUCCESS ||
        internal_status == OperationStatus::NOT_FOUND) {
      ++rmw_sync_;
      ++rmw_per_req_iops_[0];
    }
  }
  #endif

  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else if (internal_status == OperationStatus::NOT_FOUND) {
    // Can be returned iff create_if_not_exists = false
    assert(!create_if_not_exists);
    status = Status::NotFound;
  } else {
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  return status;
}

template <class K, class V, class D, class H, class OH>
template <class DC>
inline Status FasterKv<K, V, D, H, OH>::Delete(DC& context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num, bool force_tombstone) {
  typedef DC delete_context_t;
  typedef PendingDeleteContext<DC> pending_delete_context_t;
  static_assert(std::is_base_of<value_t, typename delete_context_t::value_t>::value,
                "value_t is not a base class of delete_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename delete_context_t::value_t),
                "alignof(value_t) != alignof(typename delete_context_t::value_t)");

  pending_delete_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalDelete(pending_context, force_tombstone);

  #ifdef STATISTICS
  if (collect_stats_) {
    ++deletes_total_;
    if (internal_status == OperationStatus::SUCCESS) {
      ++deletes_sync_;
      ++deletes_per_req_iops_[0];
    }
  }
  #endif

  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else if(internal_status == OperationStatus::NOT_FOUND) {
    status = Status::NotFound;
  } else {
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  return status;
}

template <class K, class V, class D, class H, class OH>
inline bool FasterKv<K, V, D, H, OH>::CompletePending(bool wait) {
  bool hash_index_done;
  do {
    disk.TryComplete();
    hash_index_done = hash_index_.CompletePending();

    bool done = true;
    if(thread_ctx().phase != Phase::WAIT_PENDING && thread_ctx().phase != Phase::IN_PROGRESS) {
      CompleteIndexPendingRequests(thread_ctx());
      CompleteIoPendingRequests(thread_ctx());
    }
    Refresh();
    CompleteRetryRequests(thread_ctx());

    done = (thread_ctx().pending_ios.empty() && thread_ctx().retry_requests.empty() && hash_index_done);

    if(thread_ctx().phase != Phase::REST) {
      CompleteIndexPendingRequests(prev_thread_ctx());
      CompleteIoPendingRequests(prev_thread_ctx());
      Refresh();
      CompleteRetryRequests(prev_thread_ctx());
      done = false;
    }
    if(done) {
      return true;
    }
  } while(wait);
  return false;
}

template <class K, class V, class D, class H, class OH>
inline void FasterKv<K, V, D, H, OH>::CompleteIoPendingRequests(ExecutionContext& context) {
  AsyncIOContext* ctxt;
  // Clear this thread's I/O response queue. (Does not clear I/Os issued by this thread that have
  // not yet completed.)
  while(context.io_responses.try_pop(ctxt)) {
    CallbackContext<AsyncIOContext> io_context{ ctxt };
    CallbackContext<pending_context_t> pending_context{ io_context->caller_context };
    // This I/O is no longer pending, since we popped its response off the queue.
    auto pending_io = context.pending_ios.find(io_context->io_id);
    assert(pending_io != context.pending_ios.end());
    context.pending_ios.erase(pending_io);

    // Issue the continue command
    bool log_truncated = false;
    OperationStatus internal_status;
    if(pending_context->type == OperationType::Read) {
      internal_status = InternalContinuePendingRead(context, *io_context.get(), log_truncated);
      assert(internal_status != OperationStatus::INDEX_ENTRY_ON_DISK); // continue reads do not access index
    } else if (pending_context->type == OperationType::RMW) {
      internal_status = InternalContinuePendingRmw(context, *io_context.get());
      assert(internal_status != OperationStatus::INDEX_ENTRY_ON_DISK); // RMWs not used in cold log
    } else {
      assert(pending_context->type == OperationType::ConditionalInsert);
      internal_status = InternalContinuePendingConditionalInsert(context, *io_context.get());
    }

    Status result;
    if(internal_status == OperationStatus::SUCCESS) {
      result = Status::Ok;
    } else if(internal_status == OperationStatus::NOT_FOUND) {
      result = Status::NotFound;
    } else if (internal_status == OperationStatus::ABORTED) {
      assert(pending_context->type == OperationType::Read ||
            pending_context->type == OperationType::ConditionalInsert);
      result = Status::Aborted;
    } else {
      result = HandleOperationStatus(context, *pending_context.get(), internal_status,
                                     pending_context.async);
    }

    if (log_truncated && result == Status::NotFound) {
      // Logic for avoiding the false NOT_FOUND when performing Reads concurrent to CompactionLookup
      async_pending_read_context_t* read_pending_context = static_cast<async_pending_read_context_t*>(
          io_context->caller_context);

      // Re-scan newly introduced log range (i.e. [expected_entry->address, tailAddress])
      assert(!read_pending_context->expected_hlog_address.in_readcache());
      read_pending_context->min_search_offset = read_pending_context->expected_hlog_address;
      read_pending_context->num_compaction_truncations = num_compaction_truncations_.load();
      // Retry request with updated info
      result = HandleOperationStatus(context, *pending_context.get(), OperationStatus::RETRY_NOW,
                                     pending_context.async);
    }

    if(!pending_context.async) {
      #ifdef STATISTICS
      if (collect_stats_) {
        UpdatePerReqStats(pending_context.get());
      }
      #endif
      if (pending_context->caller_callback) {
        pending_context->caller_callback(pending_context->caller_context, result);
      }
    }
  }
}

template <class K, class V, class D, class H, class OH>
inline void FasterKv<K, V, D, H, OH>::CompleteIndexPendingRequests(ExecutionContext& context) {
  AsyncIndexIOContext* ctxt;
  // Clear this thread's index I/O response queue.
  // NOTE: Does not clear I/Os issued by this thread that have not yet completed.
  while(context.index_io_responses.try_pop(ctxt)) {
    CallbackContext<AsyncIndexIOContext> index_io_context{ ctxt };
    CallbackContext<pending_context_t> pending_context{ index_io_context->caller_context };
    // This I/O is no longer pending, since we popped its response off the queue.
    auto pending_io = context.pending_ios.find(index_io_context->io_id);
    assert(pending_io != context.pending_ios.end());
    context.pending_ios.erase(pending_io);

    // Update index entry from finished cold index (on-disk) request
    pending_context->set_index_entry(index_io_context->entry, nullptr);
    pending_context->index_op_result = index_io_context->result;

    //fprintf(stderr, "%d %d %d\n", pending_context->type, pending_context->index_op_type,
    //                              pending_context->index_op_result);
    assert(pending_context->index_op_type != IndexOperationType::None);

    Status result;
    switch(pending_context->type) {
      case OperationType::Read:
        // Only Retrieve index op is possible
        assert(pending_context->index_op_type == IndexOperationType::Retrieve);
        assert(pending_context->index_op_result == Status::Ok ||
                pending_context->index_op_result == Status::NotFound);
        // Resume request with the retrieved index hash bucket
        result = HandleOperationStatus(context, *pending_context.get(), OperationStatus::RETRY_NOW,
                                        pending_context.async);
        break;
      case OperationType::Upsert:
      case OperationType::Delete:
        if (pending_context->index_op_type == IndexOperationType::Retrieve) {
          // Resume request with the retrieved index hash bucket
          result = HandleOperationStatus(context, *pending_context.get(), OperationStatus::RETRY_NOW,
                                          pending_context.async);
        } else if (pending_context->index_op_type == IndexOperationType::Update) {
          if (pending_context->index_op_result == Status::Ok) {
            // Request was successfully completed!
            pending_context.async = false;
            result = Status::Ok;
          } else {
            assert(pending_context->index_op_result == Status::Aborted);
            // Request was aborted: try to mark record as invalid
            assert(index_io_context->record_address != Address::kInvalidAddress);
            Address record_address = index_io_context->record_address;
            if (record_address >= hlog.head_address.load()) {
              record_t* record = reinterpret_cast<record_t*>(hlog.Get(record_address));
              record->header.invalid = true;
            }
            // if not marked here, it will be ignored by conditional insert during compaction

            #ifdef STATISTICS
            if (collect_stats_) {
              ++pending_context->num_record_invalidations;
            }
            #endif

            // Retry request
            pending_context->clear_index_op();
            result = HandleOperationStatus(context, *pending_context.get(), OperationStatus::RETRY_NOW,
                                          pending_context.async);
          }
        } else {
          assert(false); // not reachable
        }
        break;
      case OperationType::ConditionalInsert:
        if (pending_context->index_op_type == IndexOperationType::Retrieve) {
          // Resume request with retrieved index hash bucket
          {
            assert(pending_context->index_op_result == Status::Ok);

            result = HandleOperationStatus(context, *pending_context.get(), OperationStatus::RETRY_NOW,
                                          pending_context.async);
          }
        } else if (pending_context->index_op_type == IndexOperationType::Update) {
          //fprintf(stderr, "UPDATE: %d\n", pending_context->index_op_result);
          if (pending_context->index_op_result == Status::Ok) {
            // Request was successfully completed!
            pending_context.async = false;
            result = Status::Ok;
          } else {
            assert(pending_context->index_op_result == Status::Aborted);
            // Request was aborted: try to mark record as invalid
            // NOTE: this record is stray: i.e., *not* part of the hash chain.
            Address record_address = index_io_context->record_address;
            if (record_address >= hlog.head_address.load()) {
              record_t* record = reinterpret_cast<record_t*>(hlog.Get(record_address));
              record->header.invalid = true;
            }
            // if not marked here, it will be ignored by conditional insert during compaction

            #ifdef STATISTICS
            if (collect_stats_) {
              ++pending_context->num_record_invalidations;
            }
            #endif

            // Retry request
            pending_context->clear_index_op();
            result = HandleOperationStatus(context, *pending_context.get(), OperationStatus::RETRY_NOW,
                                          pending_context.async);
          }
        } else {
          assert(false); // not reachable
        }
        break;
      case OperationType::RMW:
        // Not implemented; not needed as no RMW requests are used in the cold log
        // NOTE: If needed in the future, implementation is similar to ConditionalInsert
        // but care should be taken wrt the race conditions related to atomic_entry.
        assert(false);
        break;
      case OperationType::Recovery:
        // Recovery process of cold-log (i.e., when using cold-index, where index requests go pending)
        // NOTE: Recovery is performed sequentially, and any synchronization is done explicitly
        assert(pending_context->index_op_type == IndexOperationType::Update);
        // no concurrent updates, thus no way for op to have been aborted
        assert(pending_context->index_op_result == Status::Ok);
        pending_context.async = false;
        result = Status::Ok;
        break;
      default:
        // not reachable
        assert(false);
        break;
    }

    if(!pending_context.async) {
      #ifdef STATISTICS
      if (collect_stats_) {
        UpdatePerReqStats(pending_context.get());
      }
      #endif
      if (pending_context->caller_callback) {
        pending_context->caller_callback(pending_context->caller_context, result);
      }
    }
  }
}

template <class K, class V, class D, class H, class OH>
inline void FasterKv<K, V, D, H, OH>::CompleteRetryRequests(ExecutionContext& context) {
  // If we can't complete a request, it will be pushed back onto the deque. Retry each request
  // only once.
  size_t size = context.retry_requests.size();
  for(size_t idx = 0; idx < size; ++idx) {
    CallbackContext<pending_context_t> pending_context{ context.retry_requests.front() };
    context.retry_requests.pop_front();
    // Issue retry command
    OperationStatus internal_status;
    switch(pending_context->type) {
    case OperationType::RMW:
      internal_status = InternalRetryPendingRmw(
                          *static_cast<async_pending_rmw_context_t*>(pending_context.get()));
      break;
    case OperationType::Upsert:
      internal_status = InternalUpsert(
                          *static_cast<async_pending_upsert_context_t*>(pending_context.get()));
      break;
    default:
      assert(false);
      throw std::runtime_error{ "Cannot happen!" };
    }
    // Handle operation status
    Status result;
    if(internal_status == OperationStatus::SUCCESS) {
      result = Status::Ok;
    } else if (internal_status == OperationStatus::NOT_FOUND) {
      result = Status::NotFound;
    } else {
      result = HandleOperationStatus(context, *pending_context.get(), internal_status,
                                     pending_context.async);
    }

    // If done, callback user code.
    if(!pending_context.async) {
      #ifdef STATISTICS
      UpdatePerReqStats(pending_context.get());
      #endif
      if (pending_context->caller_callback) {
        pending_context->caller_callback(pending_context->caller_context, result);
      }
    }
  }
}

template <class K, class V, class D, class H, class OH>
template <class C>
inline OperationStatus FasterKv<K, V, D, H, OH>::InternalRead(C& pending_context) const {
  typedef C pending_read_context_t;

  assert(pending_context.index_op_type != IndexOperationType::Update);

  if(thread_ctx().phase != Phase::REST) {
    const_cast<faster_t*>(this)->HeavyEnter();
  }
  // Stamp request (if not stamped already)
  pending_context.try_stamp_request(thread_ctx().phase, thread_ctx().version);

  Status index_status;
  if (pending_context.index_op_type == IndexOperationType::None) {
    index_status = hash_index_.FindEntry(const_cast<ExecutionContext&>(thread_ctx()), pending_context);
    if (index_status == Status::Pending) {
      return OperationStatus::INDEX_ENTRY_ON_DISK;
    }
  } else {
    assert(!hash_index_.IsSync());
    assert(pending_context.index_op_type == IndexOperationType::Retrieve);
    // retrieve result of async index operation
    index_status = pending_context.index_op_result;
    pending_context.clear_index_op();
  }

  if(index_status == Status::NotFound) {
    // no record found
    assert(!pending_context.atomic_entry);
    return OperationStatus::NOT_FOUND;
  }
  assert(index_status == Status::Ok);

  Address address = pending_context.entry.address();
  if (UseReadCache()) {
    Status rc_status = read_cache_->Read(pending_context, address);
    if (rc_status == Status::Ok) {
      return OperationStatus::SUCCESS;
    }
    assert(rc_status == Status::NotFound);
    // address should have been modified to point to hlog
  }
  pending_context.expected_hlog_address = address;
  assert(!address.in_readcache());

  Address begin_address = hlog.begin_address.load();
  Address head_address = hlog.head_address.load();
  Address safe_read_only_address = hlog.safe_read_only_address.load();
  Address read_only_address = hlog.read_only_address.load();
  uint64_t latest_record_version = 0;

  if(address >= head_address) {
    // Look through the in-memory portion of the log, to find the first record (if any) whose key
    // matches.
    const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(address));
    latest_record_version = record->header.checkpoint_version;
    if(!pending_context.is_key_equal(record->key())) {
      address = TraceBackForKeyMatchCtxt(pending_context, record->header.previous_address(), head_address);
    }
  }

  if (pending_context.min_search_offset != Address::kInvalidAddress &&
      pending_context.min_search_offset > address) {
    // Found an record before the designated start of search range
    return OperationStatus::NOT_FOUND;
  }

  switch(thread_ctx().phase) {
  case Phase::PREPARE:
    // Reading old version (v).
    if(latest_record_version > thread_ctx().version) {
      // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
      // what we've seen.
      pending_context.go_async(address);
      return OperationStatus::CPR_SHIFT_DETECTED;
    }
    break;
  default:
    break;
  }

  if(address >= safe_read_only_address) {
    // Mutable or fuzzy region
    // concurrent read
    if (reinterpret_cast<const record_t*>(hlog.Get(address))->header.tombstone) {
      return (pending_context.abort_if_tombstone)
                ? OperationStatus::ABORTED
                : OperationStatus::NOT_FOUND;
    }
    pending_context.GetAtomic(hlog.Get(address));
    return OperationStatus::SUCCESS;
  } else if(address >= head_address) {
    // Immutable region
    // single-thread read
    if (reinterpret_cast<const record_t*>(hlog.Get(address))->header.tombstone) {
      return (pending_context.abort_if_tombstone)
                ? OperationStatus::ABORTED
                : OperationStatus::NOT_FOUND;
    }
    pending_context.Get(hlog.Get(address));
    return OperationStatus::SUCCESS;
  } else if(address >= begin_address) {
    // Record not available in-memory
    pending_context.go_async(address);
    return OperationStatus::RECORD_ON_DISK;
  } else {
    // No record found
    return OperationStatus::NOT_FOUND;
  }
}

template <class K, class V, class D, class H, class OH>
template <class C>
inline OperationStatus FasterKv<K, V, D, H, OH>::InternalUpsert(C& pending_context) {
  typedef C pending_upsert_context_t;

  if(thread_ctx().phase != Phase::REST) {
    HeavyEnter();
  }

  // Stamp request (if not stamped already)
  pending_context.try_stamp_request(thread_ctx().phase, thread_ctx().version);

  Status index_status;
  if (pending_context.index_op_type == IndexOperationType::None) {
    index_status = hash_index_.FindOrCreateEntry(thread_ctx(), pending_context);
    if (index_status == Status::Pending) {
      return OperationStatus::INDEX_ENTRY_ON_DISK;
    }
  } else {
    assert(!hash_index_.IsSync());
    assert(pending_context.index_op_type == IndexOperationType::Retrieve);
    // retrieve result of async index operation
    index_status = pending_context.index_op_result;
    pending_context.clear_index_op();
  }
  assert(index_status == Status::Ok);

  HashBucketEntry expected_entry{ pending_context.entry };
  AtomicHashBucketEntry* atomic_entry{ pending_context.atomic_entry };
  // (Note that address will be Address::kInvalidAddress, if the atomic_entry was created.)
  Address address = expected_entry.address();

  if (UseReadCache()) {
    address = read_cache_->SkipAndInvalidate(pending_context);
  }
  Address hlog_address = address;
  assert(!address.in_readcache());

  Address head_address = hlog.head_address.load();
  Address read_only_address = hlog.read_only_address.load();
  uint64_t latest_record_version = 0;

  if(address >= head_address) {
    // Multiple keys may share the same hash. Try to find the most recent record with a matching
    // key that we might be able to update in place.
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    latest_record_version = record->header.checkpoint_version;
    if(!pending_context.is_key_equal(record->key())) {
      address = TraceBackForKeyMatchCtxt(pending_context, record->header.previous_address(), head_address);
    }
  }

  CheckpointLockGuard<key_hash_t> lock_guard{ checkpoint_locks_, pending_context.get_key_hash() };

  // The common case
  if(thread_ctx().phase == Phase::REST && address >= read_only_address) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!record->header.tombstone && pending_context.PutAtomic(record)) {
      return OperationStatus::SUCCESS;
    } else {
      // Must retry as RCU.
      goto create_record;
    }
  }

  // Acquire necessary locks.
  switch(thread_ctx().phase) {
  case Phase::PREPARE:
    // Working on old version (v).
    if(!lock_guard.try_lock_old()) {
      pending_context.go_async(address);
      return OperationStatus::CPR_SHIFT_DETECTED;
    } else {
      if(latest_record_version > thread_ctx().version) {
        // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
        // what we've seen.
        pending_context.go_async(address);
        return OperationStatus::CPR_SHIFT_DETECTED;
      }
    }
    break;
  case Phase::IN_PROGRESS:
    // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
    if(latest_record_version < thread_ctx().version) {
      // Will create new record or update existing record to new version (v+1).
      if(!lock_guard.try_lock_new()) {
        pending_context.go_async(address);
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_PENDING:
    // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
    if(latest_record_version < thread_ctx().version) {
      if(lock_guard.old_locked()) {
        pending_context.go_async(address);
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_FLUSH:
    // All other threads are in phase {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
    if(latest_record_version < thread_ctx().version) {
      goto create_record;
    }
    break;
  default:
    break;
  }

  if(address >= read_only_address) {
    // Mutable region; try to update in place.
    // TODO: fix this in the cold log; UPDATE: it might just be the case that there is no need to fix anything here.
    // NOTE: TL;DR; cold log index cannot experience the lost-update anomaly.
    //       Cold log only receives upserts during hot-cold compaction
    //       During a single hot-cold compaction, a record with a given key, can be updated AT MOST once!
    //       This is guaranteed by the lookup compaction algorithm, as only a single hot-cold compaction is active at any given time.
    //       In case of hash collisions, there should be no problem, as different threads are (up)inserting records with different keys.
    //       Even when there is a concurrent cold-cold compaction occurring, these records are only being inserted *conditionally*.
    //       This means that there will be inserted at the tail, ONLY IF there is *no* other record present anywhere in the cold log.
    //       And since these records to be compacted are NOT located in the mutable region, then entering this if is impossible :)
    //
    assert(hash_index_.IsSync() && atomic_entry != nullptr);
    if(atomic_entry->load() != expected_entry) {
      // Some other thread may have RCUed the record before we locked it; try again.
      return OperationStatus::RETRY_NOW;
    }
    // We acquired the necessary locks, so we can update the record's bucket atomically.
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!record->header.tombstone && pending_context.PutAtomic(record)) {
      // Host successfully replaced record, atomically.
      return OperationStatus::SUCCESS;
    } else {
      // Must retry as RCU.
      goto create_record;
    }
  }

  // Create a record and attempt RCU.
create_record:
  uint32_t record_size = record_t::size(pending_context.key_size(), pending_context.value_size());
  Address new_address = BlockAllocate(record_size);
  record_t* record = reinterpret_cast<record_t*>(hlog.Get(new_address));
  assert(!hlog_address.in_readcache()); // hlog entries do *not* point to read cache
  new(record) record_t{
    RecordInfo{
      static_cast<uint16_t>(thread_ctx().version), true, false, false,
      hlog_address },
  };
  pending_context.write_deep_key_at(const_cast<key_t*>(&record->key()));
  pending_context.Put(record);

  index_status = hash_index_.TryUpdateEntry(thread_ctx(), pending_context, new_address);
  if (index_status == Status::Ok) {
    // Installed the new record in the hash table.
    return OperationStatus::SUCCESS;
  } else if (index_status == Status::Aborted) {
    assert(index_status == Status::Aborted);
    // Try again.
    record->header.invalid = true;
    pending_context.clear_index_op();

    #ifdef STATISTICS
    if (collect_stats_) {
      ++pending_context.num_record_invalidations;
    }
    #endif

    return InternalUpsert(pending_context);
  } else {
    assert(index_status == Status::Pending);
    assert(!hash_index_.IsSync());
    return OperationStatus::INDEX_ENTRY_ON_DISK;
  }
}

template <class K, class V, class D, class H, class OH>
template <class C>
inline OperationStatus FasterKv<K, V, D, H, OH>::InternalRmw(C& pending_context, bool retrying) {
  typedef C pending_rmw_context_t;
  assert(hash_index_.IsSync()); // cold log does not need to support RMWs requests

  Phase phase = retrying ? pending_context.phase : thread_ctx().phase;
  uint32_t version = retrying ? pending_context.version : thread_ctx().version;

  if(phase != Phase::REST) {
    HeavyEnter();
  }

  // Stamp request (if not stamped already)
  pending_context.try_stamp_request(thread_ctx().phase, thread_ctx().version);

  Status index_status = hash_index_.FindOrCreateEntry(thread_ctx(), pending_context);
  assert(index_status == Status::Ok);

  HashBucketEntry expected_entry{ pending_context.entry };
  AtomicHashBucketEntry* atomic_entry{ pending_context.atomic_entry };

  // (Note that address will be Address::kInvalidAddress, if the entry was created.)
  Address address = expected_entry.address();

  if (UseReadCache()) {
    address = read_cache_->Skip(pending_context);
  }
  pending_context.expected_hlog_address = address;
  assert(!address.in_readcache());

  Address begin_address = hlog.begin_address.load();
  Address head_address = hlog.head_address.load();
  Address read_only_address = hlog.read_only_address.load();
  Address safe_read_only_address = hlog.safe_read_only_address.load();
  uint64_t latest_record_version = 0;

  if(address >= head_address) {
    // Multiple keys may share the same hash. Try to find the most recent record with a matching
    // key that we might be able to update in place.
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    latest_record_version = record->header.checkpoint_version;
    if(!pending_context.is_key_equal(record->key())) {
      address = TraceBackForKeyMatchCtxt(pending_context, record->header.previous_address(), head_address);
    }
  }

  CheckpointLockGuard<key_hash_t> lock_guard{ checkpoint_locks_, pending_context.get_key_hash() };

  // The common case.
  if(phase == Phase::REST && address >= read_only_address) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!record->header.tombstone && pending_context.RmwAtomic(record)) {
      // In-place RMW succeeded.
      return OperationStatus::SUCCESS;
    } else {
      // Must retry as RCU.
      goto create_record;
    }
  }

  // Acquire necessary locks.
  switch(phase) {
  case Phase::PREPARE:
    // Working on old version (v).
    if(!lock_guard.try_lock_old()) {
      // If we're retrying the operation, then we already have an old lock, so we'll always
      // succeed in obtaining a second. Otherwise, another thread has acquired the new lock, so
      // a CPR shift has occurred.
      assert(!retrying);
      pending_context.go_async(address);
      return OperationStatus::CPR_SHIFT_DETECTED;
    } else {
      if(latest_record_version > version) {
        // CPR shift detected: we are in the "PREPARE" phase, and a mutable record has a version
        // later than what we've seen.
        assert(!retrying);
        pending_context.go_async(address);
        return OperationStatus::CPR_SHIFT_DETECTED;
      }
    }
    break;
  case Phase::IN_PROGRESS:
    // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
    if(latest_record_version < version) {
      // Will create new record or update existing record to new version (v+1).
      if(!lock_guard.try_lock_new()) {
        pending_context.go_async(address);
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_PENDING:
    // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
    if(latest_record_version < version) {
      if(lock_guard.old_locked()) {
        pending_context.go_async(address);
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_FLUSH:
    // All other threads are in phase {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
    if(latest_record_version < version) {
      goto create_record;
    }
    break;
  default:
    break;
  }

  if(address >= read_only_address) {
    // TODO: check
    // Mutable region. Try to update in place.
    if(atomic_entry->load() != expected_entry) {
      // Some other thread may have RCUed the record before we locked it; try again.
      return OperationStatus::RETRY_NOW;
    }
    // We acquired the necessary locks, so so we can update the record's bucket atomically.
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!record->header.tombstone && pending_context.RmwAtomic(record)) {
      // In-place RMW succeeded.
      return OperationStatus::SUCCESS;
    } else {
      // Must retry as RCU.
      goto create_record;
    }
  } else if(address >= safe_read_only_address && !reinterpret_cast<record_t*>(hlog.Get(address))->header.tombstone) {
    // Fuzzy Region: Must go pending due to lost-update anomaly
    pending_context.go_async(address);
    return OperationStatus::RETRY_LATER;
  } else if(address >= head_address) {
    goto create_record;
  } else if(address >= begin_address) {
    // Need to obtain old record from disk.
    pending_context.go_async(address);
    return OperationStatus::RECORD_ON_DISK;
  } else {
    // No record exists in the log
    if (!pending_context.create_if_not_exists) {
      return OperationStatus::NOT_FOUND;
    }
    // Create a new record.
    goto create_record;
  }

  // Create a record and attempt RCU.
create_record:
  if (UseReadCache()) {
    // invalidate read cache entry before trying to insert new entry
    read_cache_->SkipAndInvalidate(pending_context);
  }
  assert(!address.in_readcache());

  const record_t* old_record = nullptr;
  if(address >= head_address) {
    old_record = reinterpret_cast<const record_t*>(hlog.Get(address));
    if(old_record->header.tombstone) {
      old_record = nullptr;
    }
  }
  uint32_t record_size = old_record != nullptr ?
    record_t::size(pending_context.key_size(), pending_context.value_size(old_record)) :
    record_t::size(pending_context.key_size(), pending_context.value_size());

  Address new_address = BlockAllocate(record_size);
  record_t* new_record = reinterpret_cast<record_t*>(hlog.Get(new_address));

  // Allocating a block may have the side effect of advancing the head address.
  head_address = hlog.head_address.load();
  // Allocating a block may have the side effect of advancing the thread context's version and phase
  if(!retrying) {
    // Re-stamp request with new version/phase
    pending_context.stamp_request(thread_ctx().phase, thread_ctx().version);
  }

  assert(!pending_context.expected_hlog_address.in_readcache()); // hlog entries do *not* point to read cache
  new(new_record) record_t{
    RecordInfo{
      static_cast<uint16_t>(pending_context.version), true, false, false,
      pending_context.expected_hlog_address },
  };
  pending_context.write_deep_key_at(const_cast<key_t*>(&new_record->key()));

  if(old_record == nullptr || address < hlog.begin_address.load()) {
    if (!pending_context.create_if_not_exists) {
      return OperationStatus::NOT_FOUND;
    }
    pending_context.RmwInitial(new_record);
  } else if(address >= head_address) {
    assert(old_record != nullptr);
    pending_context.RmwCopy(old_record, new_record);
  } else {
    // The block we allocated for the new record caused the head address to advance beyond
    // the old record. Need to obtain the old record from disk.
    new_record->header.invalid = true;
    pending_context.go_async(address);
    return OperationStatus::RECORD_ON_DISK;
  }

  index_status = hash_index_.TryUpdateEntry(thread_ctx(), pending_context, new_address);
  if (index_status == Status::Ok) {
    return OperationStatus::SUCCESS;
  } else {
    assert(index_status == Status::Aborted);
    // Try again.
    new_record->header.invalid = true;

    #ifdef STATISTICS
    if (collect_stats_) {
      ++pending_context.num_record_invalidations;
    }
    #endif

    return OperationStatus::RETRY_NOW;
  }
}

template <class K, class V, class D, class H, class OH>
inline OperationStatus FasterKv<K, V, D, H, OH>::InternalRetryPendingRmw(
  async_pending_rmw_context_t& pending_context) {
  OperationStatus status = InternalRmw(pending_context, true);
  if(status == OperationStatus::SUCCESS && pending_context.version != thread_ctx().version) {
    status = OperationStatus::SUCCESS_UNMARK;
  }
  return status;
}

template <class K, class V, class D, class H, class OH>
template<class C>
inline OperationStatus FasterKv<K, V, D, H, OH>::InternalDelete(C& pending_context, bool force_tombstone) {
  typedef C pending_delete_context_t;
  assert(pending_context.index_op_type != IndexOperationType::Update);

  if(thread_ctx().phase != Phase::REST) {
    HeavyEnter();
  }
  // Stamp request (if not stamped already)
  pending_context.try_stamp_request(thread_ctx().phase, thread_ctx().version);

  Address address;
  Address hlog_address{ 0 };
  Address head_address = hlog.head_address.load();
  Address read_only_address = hlog.read_only_address.load();
  Address begin_address = hlog.begin_address.load();
  uint64_t latest_record_version = 0;

  Status index_status;
  if (pending_context.index_op_type == IndexOperationType::None) {
    if (!force_tombstone) {
      index_status = hash_index_.FindEntry(thread_ctx(), pending_context);
    } else {
      index_status = hash_index_.FindOrCreateEntry(thread_ctx(), pending_context);
    }
    if (index_status == Status::Pending) {
      return OperationStatus::INDEX_ENTRY_ON_DISK;
    }
  } else {
    assert(!hash_index_.IsSync());
    assert(pending_context.index_op_type == IndexOperationType::Retrieve);
    // retrieve result of async index operation
    index_status = pending_context.index_op_result;
    pending_context.clear_index_op();
  }

  HashBucketEntry expected_entry{ pending_context.entry };

  if(index_status == Status::NotFound) {
    // no record found
    if (!force_tombstone) {
      assert(!pending_context.atomic_entry);
      return OperationStatus::NOT_FOUND;
    } else {
      goto create_record;
    }
  }
  assert(index_status == Status::Ok);

  address = expected_entry.address();

  if (UseReadCache()) {
    address = read_cache_->SkipAndInvalidate(pending_context);
  }
  hlog_address = address;
  assert(!address.in_readcache());

  if(address >= head_address) {
    const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(address));
    latest_record_version = record->header.checkpoint_version;
    if(!pending_context.is_key_equal(record->key())) {
      address = TraceBackForKeyMatchCtxt(pending_context, record->header.previous_address(), head_address);
    }
  }

  {
    CheckpointLockGuard<key_hash_t> lock_guard{ checkpoint_locks_, pending_context.get_key_hash() };
    // NO optimization for most common case

    // Acquire necessary locks.
    switch (thread_ctx().phase) {
    case Phase::PREPARE:
      // Working on old version (v).
      if(!lock_guard.try_lock_old()) {
        pending_context.go_async(address);
        return OperationStatus::CPR_SHIFT_DETECTED;
      } else if(latest_record_version > thread_ctx().version) {
        // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
        // what we've seen.
        pending_context.go_async(address);
        return OperationStatus::CPR_SHIFT_DETECTED;
      }
      break;
    case Phase::IN_PROGRESS:
      // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
      if(latest_record_version < thread_ctx().version) {
        // Will create new record or update existing record to new version (v+1).
        if(!lock_guard.try_lock_new()) {
          pending_context.go_async(address);
          return OperationStatus::RETRY_LATER;
        } else {
          // Update to new version (v+1) requires RCU.
          goto create_record;
        }
      }
      break;
    case Phase::WAIT_PENDING:
      // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
      if(latest_record_version < thread_ctx().version) {
        if(lock_guard.old_locked()) {
          pending_context.go_async(address);
          return OperationStatus::RETRY_LATER;
        } else {
          // Update to new version (v+1) requires RCU.
          goto create_record;
        }
      }
      break;
    case Phase::WAIT_FLUSH:
      // All other threads are in phase {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
      if(latest_record_version < thread_ctx().version) {
        goto create_record;
      }
      break;
    default:
      break;
    }
  }

  // Mutable Region: Update the record in-place
  if(address >= read_only_address) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    // If the record is the head of the hash chain, try to update the hash chain and completely
    // elide record only if the previous address points to invalid address
    // NOTE: this is an optimization enabled for hot log; disabled when cold index is used.
    if(hash_index_.IsSync() && expected_entry.address() == address) {
      Address previous_address = record->header.previous_address();
      if (previous_address < begin_address) {
        hash_index_.TryUpdateEntry(thread_ctx(), pending_context, HashBucketEntry::kInvalidEntry);
      }
    }
    record->header.tombstone = true;
    return OperationStatus::SUCCESS;
  }

create_record:
  uint32_t record_size = record_t::size(pending_context.key_size(), pending_context.value_size());
  Address new_address = BlockAllocate(record_size);
  record_t* record = reinterpret_cast<record_t*>(hlog.Get(new_address));
  assert(!hlog_address.in_readcache()); // hlog entries do *not* point to read cache
  new(record) record_t{
    RecordInfo{
      static_cast<uint16_t>(thread_ctx().version), true, true, false,
      hlog_address },
  };
  pending_context.write_deep_key_at(const_cast<key_t*>(&record->key()));

  index_status = hash_index_.TryUpdateEntry(thread_ctx(), pending_context, new_address);
  if (index_status == Status::Ok) {
    return OperationStatus::SUCCESS;
  } else if (index_status == Status::Aborted) {
    // Try again.
    record->header.invalid = true;
    pending_context.clear_index_op();

    #ifdef STATISTICS
    if (collect_stats_) {
      ++pending_context.num_record_invalidations;
    }
    #endif

    return InternalDelete(pending_context, force_tombstone);
  } else {
    assert(!hash_index_.IsSync());
    assert(index_status == Status::Pending);
    return OperationStatus::INDEX_ENTRY_ON_DISK;
  }
}

template <class K, class V, class D, class H, class OH>
template<class C>
inline Address FasterKv<K, V, D, H, OH>::TraceBackForKeyMatchCtxt(const C& ctxt, Address from_address,
    Address min_offset) const {
  while(from_address >= min_offset) {
    const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(from_address));
    if(ctxt.is_key_equal(record->key())) {
      return from_address;
    } else {
      from_address = record->header.previous_address();
      assert(!from_address.in_readcache());
      continue;
    }
  }
  return from_address;
}

template <class K, class V, class D, class H, class OH>
inline Address FasterKv<K, V, D, H, OH>::TraceBackForKeyMatch(const key_t& key, Address from_address,
                                                       Address min_offset) const {
  while(from_address >= min_offset) {
    const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(from_address));
    if(key == record->key()) {
      return from_address;
    } else {
      from_address = record->header.previous_address();
      assert(!from_address.in_readcache());
      continue;
    }
  }
  return from_address;
}

template <class K, class V, class D, class H, class OH>
inline Status FasterKv<K, V, D, H, OH>::HandleOperationStatus(ExecutionContext& ctx,
    pending_context_t& pending_context, OperationStatus internal_status, bool& async) {
  async = false;
  switch(internal_status) {
  case OperationStatus::RETRY_NOW:
    switch(pending_context.type) {
    case OperationType::Read: {
      async_pending_read_context_t& read_context =
        *static_cast<async_pending_read_context_t*>(&pending_context);
      internal_status = InternalRead(read_context);
      break;
    }
    case OperationType::Upsert: {
      async_pending_upsert_context_t& upsert_context =
        *static_cast<async_pending_upsert_context_t*>(&pending_context);
      internal_status = InternalUpsert(upsert_context);
      break;
    }
    case OperationType::RMW: {
      async_pending_rmw_context_t& rmw_context =
        *static_cast<async_pending_rmw_context_t*>(&pending_context);
      internal_status = InternalRmw(rmw_context, false);
      break;
    }
    case OperationType::Delete: {
      async_pending_delete_context_t& delete_context =
        *static_cast<async_pending_delete_context_t*>(&pending_context);
      internal_status = InternalDelete(delete_context, true);
      break;
    }
    case OperationType::ConditionalInsert: {
      async_pending_ci_context_t& conditional_insert_context =
        *static_cast<async_pending_ci_context_t*>(&pending_context);
      internal_status = InternalConditionalInsert(conditional_insert_context);
    }
    }

    if(internal_status == OperationStatus::SUCCESS) {
      return Status::Ok;
    } else if(internal_status == OperationStatus::NOT_FOUND) {
      return Status::NotFound;
    } else if (internal_status == OperationStatus::ABORTED) {
      assert(pending_context.type == OperationType::ConditionalInsert);
      return Status::Aborted;
    } else {
      return HandleOperationStatus(ctx, pending_context, internal_status, async);
    }
  case OperationStatus::RETRY_LATER:
    if(thread_ctx().phase == Phase::PREPARE) {
      assert(pending_context.type == OperationType::RMW);
      // Can I be marking an operation again and again?
      if(!checkpoint_locks_.get_lock(pending_context.get_key_hash()).try_lock_old()) {
        return PivotAndRetry(ctx, pending_context, async);
      }
    }
    return RetryLater(ctx, pending_context, async);
  case OperationStatus::RECORD_ON_DISK:
    if(thread_ctx().phase == Phase::PREPARE && pending_context.type != OperationType::ConditionalInsert) {
      assert(pending_context.type == OperationType::Read ||
             pending_context.type == OperationType::RMW);
      // Can I be marking an operation again and again?
      if(!checkpoint_locks_.get_lock(pending_context.get_key_hash()).try_lock_old()) {
        return PivotAndRetry(ctx, pending_context, async);
      }
    }
    return IssueAsyncIoRequest(ctx, pending_context, async);
  case OperationStatus::INDEX_ENTRY_ON_DISK:
    // Keep track of pending requests due to async index retrievals
    async = true;
    return Status::Pending;
  case OperationStatus::ASYNC_TO_COLD_STORE:
    // hot-cold only: live record (up)inserted/deleted at cold store
    assert(pending_context.type == OperationType::ConditionalInsert);
    async = false;
    // callback will be called when op finishes on other store
    pending_context.caller_callback = nullptr;
    return Status::Pending;
  case OperationStatus::SUCCESS_UNMARK:
    checkpoint_locks_.get_lock(pending_context.get_key_hash()).unlock_old();
    return Status::Ok;
  case OperationStatus::NOT_FOUND_UNMARK:
    checkpoint_locks_.get_lock(pending_context.get_key_hash()).unlock_old();
    return Status::NotFound;
  case OperationStatus::ABORTED_UNMARK:
    assert(pending_context.type == OperationType::Read);
    checkpoint_locks_.get_lock(pending_context.get_key_hash()).unlock_old();
    return Status::Aborted;
  case OperationStatus::CPR_SHIFT_DETECTED:
    return PivotAndRetry(ctx, pending_context, async);
  }
  // not reached
  assert(false);
  return Status::Corruption;
}

template <class K, class V, class D, class H, class OH>
inline Status FasterKv<K, V, D, H, OH>::PivotAndRetry(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  // Some invariants
  assert(ctx.version == thread_ctx().version);
  assert(thread_ctx().phase == Phase::PREPARE);
  Refresh();
  // thread must have moved to IN_PROGRESS phase
  assert(thread_ctx().version == ctx.version + 1);
  // update request phase & version
  pending_context.stamp_request(thread_ctx().phase, thread_ctx().version);
  // retry with new contexts
  return HandleOperationStatus(thread_ctx(), pending_context, OperationStatus::RETRY_NOW, async);
}

template <class K, class V, class D, class H, class OH>
inline Status FasterKv<K, V, D, H, OH>::RetryLater(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  IAsyncContext* context_copy;
  Status result = pending_context.DeepCopy(context_copy);
  if(result == Status::Ok) {
    async = true;
    ctx.retry_requests.push_back(context_copy);
    return Status::Pending;
  } else {
    async = false;
    return result;
  }
}

template <class K, class V, class D, class H, class OH>
inline Status FasterKv<K, V, D, H, OH>::IssueAsyncIoRequest(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  // Issue asynchronous I/O request
  uint64_t io_id = thread_ctx().io_id++;
  thread_ctx().pending_ios.insert({ io_id, pending_context.get_key_hash() });
  async = true;
  AsyncIOContext io_request{ this, pending_context.address, &pending_context,
                             &thread_ctx().io_responses, io_id };
  AsyncGetFromDisk(pending_context.address, MinIoRequestSize(), AsyncGetFromDiskCallback,
                   io_request);
  return Status::Pending;
}

template <class K, class V, class D, class H, class OH>
inline Address FasterKv<K, V, D, H, OH>::BlockAllocate(uint32_t record_size) {
  uint32_t page;
  Address retval = hlog.Allocate(record_size, page);
  while(retval < hlog.read_only_address.load()) {
    Refresh();
    if (other_store_ && other_store_->epoch_.IsProtected()) other_store_->Refresh();
    if (refresh_callback_store_) refresh_callback_(refresh_callback_store_);
    // Don't overrun the hlog's tail offset.
    bool page_closed = (retval == Address::kInvalidAddress);
    while(page_closed) {
      page_closed = !hlog.NewPage(page);
      Refresh();
      if (other_store_ && other_store_->epoch_.IsProtected()) other_store_->Refresh();
      if (refresh_callback_store_) refresh_callback_(refresh_callback_store_);
    }
    retval = hlog.Allocate(record_size, page);
  }
  return retval;
}

template <class K, class V, class D, class H, class OH>
Address FasterKv<K, V, D, H, OH>::BlockAllocateReadCacheCallback(void* faster, uint32_t record_size) {
  faster_t* self = reinterpret_cast<faster_t*>(faster);
  return self->BlockAllocateReadCache(record_size);
}

template <class K, class V, class D, class H, class OH>
inline Address FasterKv<K, V, D, H, OH>::BlockAllocateReadCache(uint32_t record_size) {
  uint32_t page;
  Address retval = read_cache_->read_cache_.Allocate(record_size, page);
  while (retval < read_cache_->read_cache_.read_only_address.load()) {
    Refresh();
    if (other_store_ && other_store_->epoch_.IsProtected()) other_store_->Refresh();
    if (refresh_callback_) refresh_callback_(refresh_callback_store_);
    bool page_closed = (retval == Address::kInvalidAddress);
    while (page_closed) {
      page_closed = !read_cache_->read_cache_.NewPage(page);
      Refresh();
      if (other_store_ && other_store_->epoch_.IsProtected()) other_store_->Refresh();
      if (refresh_callback_) refresh_callback_(refresh_callback_store_);
    }
    retval = read_cache_->read_cache_.Allocate(record_size, page);
  }
  return retval;
}

template <class K, class V, class D, class H, class OH>
inline void FasterKv<K, V, D, H, OH>::DoRefreshCallback(void* faster) {
  faster_t* self = reinterpret_cast<faster_t*>(faster);
  self->Refresh(true);
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::DoThrottling() {
  if(epoch_.IsProtected()) {
    /// User threads wait for user-issued requests
    bool other_store_protected = (other_store_ != nullptr) && other_store_->epoch_.IsProtected();
    while(num_pending_ios_.load() > kMaxPendingIOs) {
      disk.TryComplete();
      hash_index_.CompletePending();
      std::this_thread::yield();
      epoch_.ProtectAndDrain();
      if (other_store_protected) {
        other_store_->epoch_.ProtectAndDrain();
      }
    }
  }
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::AsyncGetFromDisk(Address address, uint32_t num_records,
    AsyncIOCallback callback, AsyncIOContext& context) {
  #ifdef STATISTICS
  auto start_time = std::chrono::high_resolution_clock::now();
  #endif

  DoThrottling();

  #ifdef STATISTICS
  std::chrono::nanoseconds duration = std::chrono::high_resolution_clock::now() - start_time;
  throttling_time_spent_ += duration.count();
  #endif

  ++num_pending_ios_;

  #ifdef STATISTICS
  if (collect_stats_) {
    auto* pending_context = static_cast<pending_context_t*>(context.caller_context);
    ++pending_context->num_iops;
    switch (pending_context->type) {
      case OperationType::Read:
        ++reads_iops_;
        break;
      case OperationType::RMW:
        ++rmw_iops_;
        break;
      case OperationType::ConditionalInsert:
        ++ci_iops_;
        break;
      default:
        // not reached
        assert(false);
        break;
    }
  }
  #endif

  hlog.AsyncGetFromDisk(address, num_records, callback, context);
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::AsyncGetFromDiskCallback(IAsyncContext* ctxt, Status result,
    size_t bytes_transferred) {
  CallbackContext<AsyncIOContext> context{ ctxt };
  faster_t* faster = reinterpret_cast<faster_t*>(context->faster);
  /// Context stack is: AsyncIOContext, PendingContext.
  pending_context_t* pending_context = static_cast<pending_context_t*>(context->caller_context);

  /// This I/O is finished.
  --faster->num_pending_ios_;
  /// Always "goes async": context is freed by the issuing thread, when processing thread I/O
  /// responses.
  context.async = true;

  pending_context->result = result;
  if(result == Status::Ok) {
    record_t* record = reinterpret_cast<record_t*>(context->record.GetValidPointer());
    // Size of the record we read from disk (might not have read the entire record, yet).
    size_t record_size = context->record.available_bytes;
    if(record->min_disk_key_size() > record_size) {
      // Haven't read the full record in yet; I/O is not complete!
      faster->AsyncGetFromDisk(context->address, record->min_disk_key_size(),
                               AsyncGetFromDiskCallback, *context.get());
      context.async = true;
    } else if(record->min_disk_value_size() > record_size) {
      // Haven't read the full record in yet; I/O is not complete!
      faster->AsyncGetFromDisk(context->address, record->min_disk_value_size(),
                               AsyncGetFromDiskCallback, *context.get());
      context.async = true;
    } else if(record->disk_size() > record_size) {
      // Haven't read the full record in yet; I/O is not complete!
      faster->AsyncGetFromDisk(context->address, record->disk_size(),
                               AsyncGetFromDiskCallback, *context.get());
      context.async = true;
    } else if(pending_context->is_key_equal(record->key())) {
      //The keys are same, so I/O is complete
      context->thread_io_responses->push(context.get());
    } else {
      //keys are not same. I/O is not complete

      // check for min_address in pending_context to avoid unecessary I/Os
      Address min_search_offset = faster->hlog.begin_address.load();
      switch (pending_context->type) {
        case OperationType::Read: {
          auto read_pending_context = static_cast<async_pending_read_context_t*>(pending_context);
          if (read_pending_context->min_search_offset > min_search_offset) {
            min_search_offset = read_pending_context->min_search_offset;
          }
        }
        break;
        case OperationType::ConditionalInsert: {
          auto ci_pending_context = static_cast<async_pending_ci_context_t*>(pending_context);
          if (ci_pending_context->min_search_offset > min_search_offset) {
            min_search_offset = ci_pending_context->min_search_offset;
          }
        }
        break;
        default:
          break;
      }

      context->address = record->header.previous_address();
      if(context->address >= min_search_offset) {
        faster->AsyncGetFromDisk(context->address, faster->MinIoRequestSize(),
                                 AsyncGetFromDiskCallback, *context.get());
        context.async = true;
      } else {
        // Record not found, so I/O is complete.
        context->thread_io_responses->push(context.get());
      }
    }
  } else if (result == Status::IOError) {
    log_warn("AsyncGetFromDiskCallback: received *IOError* status from request callback");
    pending_context->caller_callback(pending_context->caller_context, Status::IOError);
  } else {
    // not reached
    assert(false);
  }
}


template <class K, class V, class D, class H, class OH>
OperationStatus FasterKv<K, V, D, H, OH>::InternalContinuePendingRead(ExecutionContext& context,
    AsyncIOContext& io_context, bool& log_truncated) {

  async_pending_read_context_t* pending_context = static_cast<async_pending_read_context_t*>(
        io_context.caller_context);
  log_truncated = pending_context->num_compaction_truncations < num_compaction_truncations_.load();

  OperationStatus op_status;
  if (pending_context->min_search_offset != Address::kInvalidAddress &&
      pending_context->min_search_offset > io_context.address) {
        op_status = OperationStatus::NOT_FOUND;
  }
  else if (io_context.address >= hlog.begin_address.load()) {
    // Address points to valid record
    record_t* record = reinterpret_cast<record_t*>(io_context.record.GetValidPointer());
    if (record->header.tombstone) {
      op_status = (pending_context->abort_if_tombstone)
                    ? OperationStatus::ABORTED
                    : OperationStatus::NOT_FOUND;
    }
    else {
      pending_context->Get(record);

      // Try to insert record to read cache
      // NOTE: Insert only if record will be valid after any potential concurrent log trimming operation
      if (UseReadCache() && io_context.address >= next_hlog_begin_address_.load()) {
        Status rc_status = read_cache_->Insert(context, *pending_context, record);
        assert(rc_status == Status::Ok || rc_status == Status::Aborted);
      }

      assert(!kCopyReadsToTail);
      op_status = OperationStatus::SUCCESS;
    }
  }
  else {
    op_status = OperationStatus::NOT_FOUND; // Invalid address
  }

  if (op_status == OperationStatus::SUCCESS) {
    return (thread_ctx().version > context.version) ? OperationStatus::SUCCESS_UNMARK :
           OperationStatus::SUCCESS;
  }
  else if (op_status == OperationStatus::NOT_FOUND) {
    return (thread_ctx().version > context.version) ? OperationStatus::NOT_FOUND_UNMARK :
           OperationStatus::NOT_FOUND;
  }
  else {
    assert(op_status == OperationStatus::ABORTED);
    return (thread_ctx().version > context.version) ? OperationStatus::ABORTED_UNMARK :
          OperationStatus::ABORTED;
  }
}

template <class K, class V, class D, class H, class OH>
OperationStatus FasterKv<K, V, D, H, OH>::InternalContinuePendingConditionalInsert(ExecutionContext& context,
    AsyncIOContext& io_context) {

  async_pending_ci_context_t* pending_context = (
    static_cast<async_pending_ci_context_t*>(io_context.caller_context));

  assert(pending_context->min_search_offset != Address::kInvalidAddress &&
          pending_context->expected_hlog_address != Address::kInvalidAddress);
  // assert(pending_context->min_search_offset <= pending_context->start_search_entry.address());

  Address begin_address = hlog.begin_address.load();
  Address head_address = hlog.head_address.load();

  if (pending_context->min_search_offset < begin_address) {
    // we cannot scan the entire region because log was truncated
    // request should be re-tried at a higher level!
    return OperationStatus::NOT_FOUND;
  }

  if (io_context.address > pending_context->min_search_offset) {
    // found newer version for this record -- abort insert!
    return OperationStatus::ABORTED;
  } else if (io_context.address < pending_context->orig_min_search_offset) {
    // record not part of the same hash chain: i.e., invalid record; skip record
    // NOTE: this check mainly protects from copying "invalid" records that were not marked invalid
    // due to an index operation going pending, and the record being flushed to disk before being marked.
    // (flush-before-marked-invalid race condition).
    // Cross-validating that a record is part of the same hash chain is a relatively cheap way to identify
    // those records.
    return OperationStatus::ABORTED;
  }
  assert(io_context.address >= pending_context->orig_min_search_offset);
  // Searched entire region, but did not find newer version for record

  // Update search range to [prev start search addr, latest hash entry addr]
  pending_context->min_search_offset = pending_context->expected_hlog_address;

  return OperationStatus::RETRY_NOW;
}


template <class K, class V, class D, class H, class OH>
OperationStatus FasterKv<K, V, D, H, OH>::InternalContinuePendingRmw(ExecutionContext& context,
                                                              AsyncIOContext& io_context) {
  assert(hash_index_.IsSync()); // RMWs are not supported when cold index is used
  async_pending_rmw_context_t* pending_context = static_cast<async_pending_rmw_context_t*>(
        io_context.caller_context);
  bool create_if_not_exists = pending_context->create_if_not_exists;

  Address expected_hlog_address = pending_context->expected_hlog_address;
  Address begin_address = hlog.begin_address.load();

  // This is an optimization for when index is in-memory (and thus index ops are sync).
  Address head_address = hlog.head_address.load();

  // Find a hash bucket entry to store the updated value in.
  // (Note that address will be Address::kInvalidAddress, if the atomic_entry was created.)
  Status status = hash_index_.FindOrCreateEntry(thread_ctx(), *pending_context);
  assert(status == Status::Ok);
  // Address from new hash index entry
  Address address = pending_context->entry.address();

  if (UseReadCache()) {
    address = read_cache_->Skip(*pending_context);
  }
  Address hlog_address = address;
  assert(!address.in_readcache());

  // Make sure that no other record has been inserted in the meantime
  if(address >= head_address && address != expected_hlog_address) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!pending_context->is_key_equal(record->key())) {
      Address min_offset = std::max(expected_hlog_address + 1, head_address);
      address = TraceBackForKeyMatchCtxt(*pending_context, record->header.previous_address(), min_offset);
    }
  }
  // part of the same hash chain or entry deleted
  assert(address < begin_address || address >= expected_hlog_address);

  if(address > expected_hlog_address) {
    // This handles two mutually exclusive cases. In both cases InternalRmw will be called immediately:
    //  1) Found a newer record in the in-memory region (i.e. address >= head_address)
    //     Calling InternalRmw will result in taking into account the newer version,
    //     instead of the record we've just read from disk.
    //  2) We can't trace the current hash bucket entry back to the record we've read (i.e. address < head_address)
    //     This is because part of the hash chain now extends to disk, thus we cannot check it right away
    //     Calling InternalRmw will result in launching a new search in the hash chain, by reading
    //     the newly introduced entries in the chain, so we won't miss any potential entries with same key.
    //
    pending_context->go_async(address);
    return OperationStatus::RETRY_NOW;
  }
  assert(address < begin_address || address == expected_hlog_address);

  // invalidate read cache entry
  if (UseReadCache()) {
    read_cache_->SkipAndInvalidate(*pending_context);
  }

  // We have to do RCU and write the updated value to the tail of the log.
  Address new_address;
  record_t* new_record;
  if(io_context.address < begin_address) {
    // The on-disk trace back failed to find a key match.
    if (!create_if_not_exists) {
      return OperationStatus::NOT_FOUND;
    }

    uint32_t record_size = record_t::size(pending_context->key_size(), pending_context->value_size());
    new_address = BlockAllocate(record_size);
    new_record = reinterpret_cast<record_t*>(hlog.Get(new_address));

    assert(!hlog_address.in_readcache()); // hlog entries do *not* point to read cache
    new(new_record) record_t{
      RecordInfo{
        static_cast<uint16_t>(context.version), true, false, false,
        hlog_address },
    };
    pending_context->write_deep_key_at(const_cast<key_t*>(&new_record->key()));
    pending_context->RmwInitial(new_record);
  } else {
    // The record we read from disk.
    const record_t* disk_record = reinterpret_cast<const record_t*>(
                                    io_context.record.GetValidPointer());
    bool is_tombstone = disk_record->header.tombstone;
    uint32_t record_size = record_t::size(pending_context->key_size(), pending_context->value_size(disk_record));
    new_address = BlockAllocate(record_size);
    new_record = reinterpret_cast<record_t*>(hlog.Get(new_address));

    assert(!hlog_address.in_readcache()); // hlog entries do *not* point to read cache
    new(new_record) record_t{
      RecordInfo{
        static_cast<uint16_t>(context.version), true, false, false,
        hlog_address },
    };
    pending_context->write_deep_key_at(const_cast<key_t*>(&new_record->key()));
    if (!is_tombstone) {
      pending_context->RmwCopy(disk_record, new_record);
    } else {
      pending_context->RmwInitial(new_record);
    }
  }

  Status index_status = hash_index_.TryUpdateEntry(thread_ctx(), *pending_context, new_address);
  if (index_status == Status::Ok) {
    assert(thread_ctx().version >= context.version);
    return (thread_ctx().version == context.version) ? OperationStatus::SUCCESS
                                                     : OperationStatus::SUCCESS_UNMARK;
  } else {
    assert(index_status == Status::Aborted);
    // CAS failed; try again.
    new_record->header.invalid = true;
    pending_context->go_async(address);

    #ifdef STATISTICS
    if (collect_stats_) {
      ++pending_context->num_record_invalidations;
    }
    #endif

    return OperationStatus::RETRY_NOW;
  }
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::InitializeCheckpointLocks() {
  checkpoint_locks_.Initialize(hash_index_.size());
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::WriteCprMetadata() {
  std::string filename = disk.cpr_checkpoint_path(checkpoint_.hybrid_log_token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "wb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fwrite(&checkpoint_.log_metadata, sizeof(checkpoint_.log_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::ReadCprMetadata(const Guid& token) {
  std::string filename = disk.cpr_checkpoint_path(token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "rb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fread(&checkpoint_.log_metadata, sizeof(checkpoint_.log_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::WriteCprContext() {
  std::string filename = disk.cpr_checkpoint_path(checkpoint_.hybrid_log_token);
  const Guid& guid = prev_thread_ctx().guid;
  filename += guid.ToString();
  filename += ".dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "wb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fwrite(static_cast<PersistentExecContext*>(&prev_thread_ctx()),
                 sizeof(PersistentExecContext), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::ReadCprContexts(const Guid& token, const Guid* guids) {
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    const Guid& guid = guids[idx];
    if(guid == Guid{}) {
      continue;
    }
    std::string filename = disk.cpr_checkpoint_path(token);
    filename += guid.ToString();
    filename += ".dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE* file = std::fopen(filename.c_str(), "rb");
    if(!file) {
      return Status::IOError;
    }
    PersistentExecContext context{};
    if(std::fread(&context, sizeof(PersistentExecContext), 1, file) != 1) {
      std::fclose(file);
      return Status::IOError;
    }
    if(std::fclose(file) != 0) {
      return Status::IOError;
    }
    auto result = checkpoint_.continue_tokens.insert({ context.guid, context.serial_num });
    assert(result.second);
  }
  if(checkpoint_.continue_tokens.size() != checkpoint_.log_metadata.num_threads) {
    return Status::Corruption;
  } else {
    return Status::Ok;
  }
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::RecoverHybridLog() {
  class Context : public IAsyncContext {
   public:
    Context(hlog_t& hlog_, uint32_t page_, RecoveryStatus& recovery_status_)
      : hlog{ &hlog_}
      , page{ page_ }
      , recovery_status{ &recovery_status_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other)
      : hlog{ other.hlog }
      , page{ other.page }
      , recovery_status{ other.recovery_status } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    hlog_t* hlog;
    uint32_t page;
    RecoveryStatus* recovery_status;
  };

  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<Context> context{ ctxt };
    result = context->hlog->AsyncReadPagesFromLog(context->page, 1, *context->recovery_status);
  };

  Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
  Address to_address = checkpoint_.log_metadata.final_address;

  uint32_t start_page = from_address.page();
  uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
  uint32_t capacity = hlog.buffer_size();
  RecoveryStatus recovery_status{ start_page, end_page };
  // Initially issue read request for all pages that can be held in memory
  uint32_t total_pages_to_read = end_page - start_page;
  uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
  RETURN_NOT_OK(hlog.AsyncReadPagesFromLog(start_page, pages_to_read_first, recovery_status));

  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }

    // handle start and end at non-page boundaries
    RETURN_NOT_OK(RecoverFromPage(page == start_page ? from_address : Address{ page, 0 },
                                  page + 1 == end_page ? to_address :
                                  Address{ page, Address::kMaxOffset }));

    // OS thread flushes current page and issues a read request if necessary
    if(page + capacity < end_page) {
      Context context{ hlog, page + capacity, recovery_status };
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, callback, &context));
    } else {
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, nullptr, nullptr));
    }
  }
  // Wait until all pages have been flushed
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  return Status::Ok;
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::RecoverHybridLogFromSnapshotFile() {
  class Context : public IAsyncContext {
   public:
    Context(hlog_t& hlog_, file_t& file_, uint32_t file_start_page_, uint32_t page_,
            RecoveryStatus& recovery_status_)
      : hlog{ &hlog_ }
      , file{ &file_ }
      , file_start_page{ file_start_page_ }
      , page{ page_ }
      , recovery_status{ &recovery_status_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other)
      : hlog{ other.hlog }
      , file{ other.file }
      , file_start_page{ other.file_start_page }
      , page{ other.page }
      , recovery_status{ other.recovery_status } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    hlog_t* hlog;
    file_t* file;
    uint32_t file_start_page;
    uint32_t page;
    RecoveryStatus* recovery_status;
  };

  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<Context> context{ ctxt };
    result = context->hlog->AsyncReadPagesFromSnapshot(*context->file,
             context->file_start_page, context->page, 1, *context->recovery_status);
  };

  Address file_start_address = checkpoint_.log_metadata.flushed_address;
  Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
  Address to_address = checkpoint_.log_metadata.final_address;

  uint32_t start_page = file_start_address.page();
  uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
  uint32_t capacity = hlog.buffer_size();
  RecoveryStatus recovery_status{ start_page, end_page };
  checkpoint_.snapshot_file = disk.NewFile(disk.relative_cpr_checkpoint_path(
                                checkpoint_.hybrid_log_token) + "snapshot.dat");
  RETURN_NOT_OK(checkpoint_.snapshot_file.Open(&disk.handler()));

  // Initially issue read request for all pages that can be held in memory
  uint32_t total_pages_to_read = end_page - start_page;
  uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
  RETURN_NOT_OK(hlog.AsyncReadPagesFromSnapshot(checkpoint_.snapshot_file, start_page, start_page,
                pages_to_read_first, recovery_status));

  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }

    // Perform recovery if page in fuzzy portion of the log
    if(Address{ page + 1, 0 } > from_address) {
      // handle start and end at non-page boundaries
      RETURN_NOT_OK(RecoverFromPage(page == from_address.page() ? from_address :
                                    Address{ page, 0 },
                                    page + 1 == end_page ? to_address :
                                    Address{ page, Address::kMaxOffset }));
    }

    // OS thread flushes current page and issues a read request if necessary
    if(page + capacity < end_page) {
      Context context{ hlog, checkpoint_.snapshot_file, start_page, page + capacity,
                       recovery_status };
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, callback, &context));
    } else {
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, nullptr, nullptr));
    }
  }
  // Wait until all pages have been flushed
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  return Status::Ok;
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::RecoverFromPage(Address from_address, Address to_address) {
  typedef RecoveryContext<K, V> recovery_context_t;
  typedef PendingRecoveryContext<recovery_context_t> pending_recovery_context_t;

  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<recovery_context_t> context{ ctxt };
    assert(result == Status::Ok);
    // set update flag to true
    context->flag->store(true);
  };

  std::atomic<bool> ready;

  assert(from_address.page() == to_address.page());
  for(Address address = from_address; address < to_address;) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(record->header.IsNull()) {
      address += sizeof(record->header);
      continue;
    }
    if(record->header.invalid) {
      address += record->size();
      continue;
    }

    recovery_context_t recovery_context{ record, &ready };
    pending_recovery_context_t pending_context{ recovery_context, callback };
    Status status = hash_index_.FindOrCreateEntry(thread_ctx(), pending_context);
    assert(status == Status::Ok || status == Status::Pending);

    if (status == Status::Pending) {
      while (!ready.load()) {
        CompletePending(false);
        std::this_thread::yield();
      }
    }

    HashBucketEntry expected_entry{ pending_context.entry };
    KeyHash hash = record->key().GetHash();

    if(record->header.checkpoint_version <= checkpoint_.log_metadata.version) {
      hash_index_.UpdateEntry(thread_ctx(), pending_context, address);
    } else {
      record->header.invalid = true;
      if(record->header.previous_address() < checkpoint_.index_metadata.checkpoint_start_address) {
        hash_index_.UpdateEntry(thread_ctx(), pending_context, record->header.previous_address());
      }
    }

    if (status == Status::Pending) {
      while (!ready.load()) {
        CompletePending(false);
        std::this_thread::yield();
      }
    }

    address += record->size();
  }

  return Status::Ok;
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::RestoreHybridLog() {
  Address tail_address = checkpoint_.log_metadata.final_address;
  uint32_t end_page = tail_address.offset() > 0 ? tail_address.page() + 1 : tail_address.page();
  uint32_t capacity = hlog.buffer_size();
  // Restore as much of the log as will fit in memory.
  uint32_t start_page;
  if(end_page < capacity - hlog.kNumHeadPages) {
    start_page = 0;
  } else {
    start_page = end_page - (capacity - hlog.kNumHeadPages);
  }
  RecoveryStatus recovery_status{ start_page, end_page };

  uint32_t num_pages = end_page - start_page;
  RETURN_NOT_OK(hlog.AsyncReadPagesFromLog(start_page, num_pages, recovery_status));

  // Wait until all pages have been read.
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  // Skip the null page.
  Address head_address = start_page == 0 ? Address{ 0, Constants::kCacheLineBytes } :
                         Address{ start_page, 0 };
  hlog.RecoveryReset(checkpoint_.index_metadata.log_begin_address, head_address, tail_address);
  return Status::Ok;
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::HeavyEnter() {
  if(thread_ctx().phase == Phase::GC_IO_PENDING || thread_ctx().phase == Phase::GC_IN_PROGRESS) {
    hash_index_.GarbageCollect(read_cache_.get());
    return;
  }
  while(thread_ctx().phase == Phase::GROW_PREPARE) {
    // We spin-wait as a simplification
    // Could instead do a "heavy operation" here
    std::this_thread::yield();
    Refresh();
  }
  if(thread_ctx().phase == Phase::GROW_IN_PROGRESS) {
    GrowIndexBlocking();
  }
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::GrowIndexBlocking() {
  hash_index_.template Grow<record_t>();

  // Thread has finished growing its part of the hash table.
  thread_ctx().phase = Phase::REST;
  // Thread ack that it has finished growing the hash table.
  if(epoch_.FinishThreadPhase(Phase::GROW_IN_PROGRESS)) {
    // Let other threads know that they can use the new hash table now.
    GlobalMoveToNextState(SystemState{ Action::GrowIndex, Phase::GROW_IN_PROGRESS,
                                       thread_ctx().version });
  } else {
    while(system_state_.load().phase == Phase::GROW_IN_PROGRESS) {
      // Spin until all other threads have finished splitting their chunks.
      std::this_thread::yield();
    }
  }
}

template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::GlobalMoveToNextState(SystemState current_state) {
  SystemState next_state = current_state.GetNextState();
  if(!system_state_.compare_exchange_strong(current_state, next_state)) {
    return false;
  }

  switch(next_state.action) {
  case Action::CheckpointFull:
  case Action::CheckpointIndex:
  case Action::CheckpointHybridLog:
    switch(next_state.phase) {
    case Phase::PREP_INDEX_CHKPT:
      // This case is handled directly inside Checkpoint[Index]().
      assert(false);
      break;
    case Phase::INDEX_CHKPT:
      assert(next_state.action != Action::CheckpointHybridLog);
      // Issue async request for fuzzy checkpoint
      assert(!checkpoint_.failed);
      if(hash_index_.Checkpoint(checkpoint_, read_cache_.get()) != Status::Ok) {
        checkpoint_.failed = true;
      }
      /*if (UseReadCache()) {
        if (read_cache_->Checkpoint(checkpoint_) != Status::Ok) {
          checkpoint_.failed = true;
        }
      }*/
      break;
    case Phase::PREPARE:
      // Index checkpoint will never reach this state; and CheckpointHybridLog() will handle this
      // case directly.
      assert(next_state.action == Action::CheckpointFull);
      // INDEX_CHKPT -> PREPARE
      // Write index meta data on disk
      if(hash_index_.WriteCheckpointMetadata(checkpoint_) != Status::Ok) {
        checkpoint_.failed = true;
      }
      /*if (UseReadCache()) {
        if (read_cache_->WriteCheckpointMetadata(checkpoint_) != Status::Ok) {
          checkpoint_.failed = true;
        }
      }*/

      // Notify the host that the index checkpoint has completed.
      checkpoint_.IssueIndexPersistenceCallback();
      break;
    case Phase::IN_PROGRESS: {
      assert(next_state.action != Action::CheckpointIndex);
      // PREPARE -> IN_PROGRESS
      // Do nothing
      break;
    }
    case Phase::WAIT_PENDING:
      assert(next_state.action != Action::CheckpointIndex);
      // IN_PROGRESS -> WAIT_PENDING
      // Do nothing
      break;
    case Phase::WAIT_FLUSH:
      assert(next_state.action != Action::CheckpointIndex);
      // WAIT_PENDING -> WAIT_FLUSH
      if(fold_over_snapshot) {
        // Move read-only to tail
        Address tail_address = hlog.ShiftReadOnlyToTail();
        // Get final address for CPR
        checkpoint_.log_metadata.final_address = tail_address;
      } else {
        Address tail_address = hlog.GetTailAddress();
        // Get final address for CPR
        checkpoint_.log_metadata.final_address = tail_address;
        checkpoint_.snapshot_file = disk.NewFile(disk.relative_cpr_checkpoint_path(
                                      checkpoint_.hybrid_log_token) + "snapshot.dat");
        if(checkpoint_.snapshot_file.Open(&disk.handler()) != Status::Ok) {
          checkpoint_.failed = true;
        }
        // Flush the log to a snapshot.
        hlog.AsyncFlushPagesToFile(checkpoint_.log_metadata.flushed_address.page(),
                                   checkpoint_.log_metadata.final_address, checkpoint_.snapshot_file,
                                   checkpoint_.flush_pending);
      }
      // Write CPR meta data file
      if(WriteCprMetadata() != Status::Ok) {
        checkpoint_.failed = true;
      }
      break;
    case Phase::PERSISTENCE_CALLBACK:
      assert(next_state.action != Action::CheckpointIndex);
      // WAIT_FLUSH -> PERSISTENCE_CALLBACK
      break;
    case Phase::REST:
      // PERSISTENCE_CALLBACK -> REST or INDEX_CHKPT -> REST
      if(next_state.action != Action::CheckpointIndex) {
        // The checkpoint is done; we can reset the contexts now. (Have to reset contexts before
        // another checkpoint can be started.)
        checkpoint_.CheckpointDone();
        // Free checkpoint locks!
        checkpoint_locks_.Free();
        // Checkpoint is done--no more work for threads to do.
        system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      } else {
        // Write index meta data on disk
        if(hash_index_.WriteCheckpointMetadata(checkpoint_) != Status::Ok) {
          checkpoint_.failed = true;
        }
        //auto index_persistence_callback = checkpoint_.index_persistence_callback;
        checkpoint_.IssueIndexPersistenceCallback();
        // The checkpoint is done; we can reset the contexts now. (Have to reset contexts before
        // another checkpoint can be started.)
        checkpoint_.CheckpointDone();
        // Checkpoint is done--no more work for threads to do.
        system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
        //if(index_persistence_callback) {
          // Notify the host that the index checkpoint has completed.
          //index_persistence_callback(Status::Ok);
        //}
      }
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;
  case Action::GC:
    switch(next_state.phase) {
    case Phase::GC_IO_PENDING:
      // This case is handled directly inside ShiftBeginAddress().
      assert(false);
      break;
    case Phase::GC_IN_PROGRESS:
      // GC_IO_PENDING -> GC_IN_PROGRESS
      // Tell the disk to truncate the log.
      hlog.Truncate(gc_state_);
      break;
    case Phase::REST:
      // GC_IN_PROGRESS -> REST
      // GC is done--no more work for threads to do.
      gc_state_.IssueCompleteCallback();
      system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;
  case Action::GrowIndex:
    switch(next_state.phase) {
    case Phase::GROW_PREPARE:
      // This case is handled directly inside GrowIndex().
      assert(false);
      break;
    case Phase::GROW_IN_PROGRESS:
      // Swap hash table versions so that all threads will use the new version after populating it.
      hash_index_.resize_info.version = grow_state_.new_version;
      break;
    case Phase::REST:
      if(grow_state_.callback != nullptr) {
        grow_state_.callback(hash_index_.new_size());
      }
      system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;
  default:
    // not reached
    assert(false);
    break;
  }
  return true;
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::MarkAllPendingRequests() {
  for(const IAsyncContext* ctxt : thread_ctx().retry_requests) {
    const pending_context_t* context = static_cast<const pending_context_t*>(ctxt);
    // We will succeed, since no other thread can currently advance the entry's version, since this
    // thread hasn't acked "PENDING" phase completion yet.
    bool result = checkpoint_locks_.get_lock(context->get_key_hash()).try_lock_old();
    assert(result);
  }
  for(const auto& pending_io : thread_ctx().pending_ios) {
    // We will succeed, since no other thread can currently advance the entry's version, since this
    // thread hasn't acked "PENDING" phase completion yet.
    bool result = checkpoint_locks_.get_lock(pending_io.second).try_lock_old();
    assert(result);
  }
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::HandleSpecialPhases() {
  #ifdef STATISTICS
  auto start_time = std::chrono::high_resolution_clock::now();
  #endif

  SystemState final_state = system_state_.load();
  if(final_state.phase == Phase::REST) {
    // Nothing to do; just reset thread context.
    thread_ctx().phase = Phase::REST;
    thread_ctx().version = final_state.version;
    return;
  }
  SystemState previous_state{ final_state.action, thread_ctx().phase, thread_ctx().version };
  do {
    // Identify the transition (currentState -> nextState)
    SystemState current_state = (previous_state == final_state) ? final_state :
                                previous_state.GetNextState();
    switch(current_state.action) {
    case Action::CheckpointFull:
    case Action::CheckpointIndex:
    case Action::CheckpointHybridLog:
      switch(current_state.phase) {
      case Phase::PREP_INDEX_CHKPT:
        assert(current_state.action != Action::CheckpointHybridLog);
        // Both from REST -> PREP_INDEX_CHKPT and PREP_INDEX_CHKPT -> PREP_INDEX_CHKPT
        if(previous_state.phase == Phase::REST) {
          // Thread ack that we're performing a checkpoint.
          if(epoch_.FinishThreadPhase(Phase::PREP_INDEX_CHKPT)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::INDEX_CHKPT: {
        assert(current_state.action != Action::CheckpointHybridLog);
        // Both from PREP_INDEX_CHKPT -> INDEX_CHKPT and INDEX_CHKPT -> INDEX_CHKPT
        if (checkpoint_.index_checkpoint_started) {
          Status result = hash_index_.CheckpointComplete();
          if(result != Status::Pending && result != Status::Ok) {
            checkpoint_.failed = true;
          }
          if(result != Status::Pending) {
            if(current_state.action == Action::CheckpointIndex) {
              // This thread is done now.
              thread_ctx().phase = Phase::REST;
              // Thread ack that it is done.
              if(epoch_.FinishThreadPhase(Phase::INDEX_CHKPT)) {
                GlobalMoveToNextState(current_state);
              }
            } else {
              // Index checkpoint is done; move on to PREPARE phase.
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      }
      case Phase::PREPARE:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle (INDEX_CHKPT -> PREPARE or REST -> PREPARE) and PREPARE -> PREPARE
        if(previous_state.phase != Phase::PREPARE) {
          // mark pending requests
          MarkAllPendingRequests();
          // keep a count of number of threads
          ++checkpoint_.log_metadata.num_threads;
          // set the thread index
          checkpoint_.log_metadata.guids[Thread::id()] = thread_ctx().guid;
          // Thread ack that it has finished marking its pending requests.
          if(epoch_.FinishThreadPhase(Phase::PREPARE)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::IN_PROGRESS:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle PREPARE -> IN_PROGRESS and IN_PROGRESS -> IN_PROGRESS
        if(previous_state.phase == Phase::PREPARE) {
          assert(prev_thread_ctx().retry_requests.empty());
          assert(prev_thread_ctx().pending_ios.empty());
          assert(prev_thread_ctx().io_responses.empty());

          // Get a new thread context; keep track of the old one as "previous."
          thread_contexts_[Thread::id()].swap();
          // initialize a new local context
          thread_ctx().Initialize(Phase::IN_PROGRESS, current_state.version,
                                  prev_thread_ctx().guid, prev_thread_ctx().serial_num);
          // Thread ack that it has swapped contexts.
          if(epoch_.FinishThreadPhase(Phase::IN_PROGRESS)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::WAIT_PENDING:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle IN_PROGRESS -> WAIT_PENDING and WAIT_PENDING -> WAIT_PENDING
        if(!epoch_.HasThreadFinishedPhase(Phase::WAIT_PENDING)) {
          if(prev_thread_ctx().pending_ios.empty() &&
              prev_thread_ctx().retry_requests.empty()) {
            // Thread ack that it has completed its pending I/Os.
            if(epoch_.FinishThreadPhase(Phase::WAIT_PENDING)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::WAIT_FLUSH:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle WAIT_PENDING -> WAIT_FLUSH and WAIT_FLUSH -> WAIT_FLUSH
        if(!epoch_.HasThreadFinishedPhase(Phase::WAIT_FLUSH)) {
          bool flushed;
          if(fold_over_snapshot) {
            flushed = hlog.flushed_until_address.load() >= checkpoint_.log_metadata.final_address;
          } else {
            flushed = checkpoint_.flush_pending.load() == 0;
          }
          if(flushed) {
            // write context info
            WriteCprContext();
            // Thread ack that it has written its CPU context.
            if(epoch_.FinishThreadPhase(Phase::WAIT_FLUSH)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::PERSISTENCE_CALLBACK:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle WAIT_FLUSH -> PERSISTENCE_CALLBACK and PERSISTENCE_CALLBACK -> PERSISTENCE_CALLBACK
        if(previous_state.phase == Phase::WAIT_FLUSH) {
          // Persistence callback
          checkpoint_.IssueHybridLogPersistenceCallback(prev_thread_ctx().serial_num);
          // Thread has finished checkpointing.
          thread_ctx().phase = Phase::REST;
          // Thread ack that it has finished checkpointing.
          if(epoch_.FinishThreadPhase(Phase::PERSISTENCE_CALLBACK)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      default:
        // nothing to do.
        break;
      }
      break;
    case Action::GC:
      switch(current_state.phase) {
      case Phase::GC_IO_PENDING:
        // Handle REST -> GC_IO_PENDING and GC_IO_PENDING -> GC_IO_PENDING.
        if(previous_state.phase == Phase::REST) {
          assert(prev_thread_ctx().retry_requests.empty());
          assert(prev_thread_ctx().pending_ios.empty());
          assert(prev_thread_ctx().io_responses.empty());
          // Get a new thread context; keep track of the old one as "previous."
          thread_contexts_[Thread::id()].swap();
          // initialize a new local context
          thread_ctx().Initialize(Phase::GC_IO_PENDING, current_state.version,
                                  prev_thread_ctx().guid, prev_thread_ctx().serial_num);
        }

        // See if the old thread context has completed its pending I/Os.
        if(!epoch_.HasThreadFinishedPhase(Phase::GC_IO_PENDING)) {
          if(prev_thread_ctx().pending_ios.empty() &&
              prev_thread_ctx().retry_requests.empty()) {
            // Thread ack that it has completed its pending I/Os.
            if(epoch_.FinishThreadPhase(Phase::GC_IO_PENDING)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::GC_IN_PROGRESS:
        // Handle GC_IO_PENDING -> GC_IN_PROGRESS and GC_IN_PROGRESS -> GC_IN_PROGRESS.
        if(!epoch_.HasThreadFinishedPhase(Phase::GC_IN_PROGRESS)) {
          if(!hash_index_.GarbageCollect(read_cache_.get())) {
            // No more buckets for this thread to clean; thread has finished GC.
            thread_ctx().phase = Phase::REST;
            // Thread ack that it has finished GC.
            if(epoch_.FinishThreadPhase(Phase::GC_IN_PROGRESS)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      default:
        assert(false); // not reached
        break;
      }
      break;
    case Action::GrowIndex:
      switch(current_state.phase) {
      case Phase::GROW_PREPARE:
        if(previous_state.phase == Phase::REST) {
          // Thread ack that we're going to grow the hash table.
          if(epoch_.FinishThreadPhase(Phase::GROW_PREPARE)) {
            GlobalMoveToNextState(current_state);
          }
        } else {
          // Wait for all other threads to finish their outstanding (synchronous) hash table
          // operations.
          std::this_thread::yield();
        }
        break;
      case Phase::GROW_IN_PROGRESS:
        GrowIndexBlocking();
        break;
      }
      break;
    }
    thread_ctx().phase = current_state.phase;
    thread_ctx().version = current_state.version;
    previous_state = current_state;
  } while(previous_state != final_state);

  #ifdef STATISTICS
  std::chrono::nanoseconds duration = std::chrono::high_resolution_clock::now() - start_time;
  special_phases_time_spent_ += duration.count();
  #endif
}

template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::Checkpoint(IndexPersistenceCallback index_persistence_callback,
                                    HybridLogPersistenceCallback hybrid_log_persistence_callback,
                                    Guid& token) {
  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointFull, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateIndexCheckpointDirectory(token);
  disk.CreateCprCheckpointDirectory(token);
  // Obtain tail address for fuzzy index checkpoint
  if(!fold_over_snapshot) {
    checkpoint_.InitializeCheckpoint(token, desired.version, hash_index_.size(),
                                     hlog.begin_address.load(),  hlog.GetTailAddress(), true,
                                     hlog.flushed_until_address.load(),
                                     index_persistence_callback,
                                     hybrid_log_persistence_callback);
  } else {
    checkpoint_.InitializeCheckpoint(token, desired.version, hash_index_.size(),
                                     hlog.begin_address.load(),  hlog.GetTailAddress(), false,
                                     Address::kInvalidAddress, index_persistence_callback,
                                     hybrid_log_persistence_callback);

  }
  InitializeCheckpointLocks();
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::InternalCheckpoint(InternalIndexPersistenceCallback index_persistence_callback,
                                    InternalHybridLogPersistenceCallback hybrid_log_persistence_callback,
                                    Guid& token, void* callback_context) {

  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointFull, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  if (Guid::IsNull(token)) {
    token = Guid::Create();
  }
  disk.CreateIndexCheckpointDirectory(token);
  disk.CreateCprCheckpointDirectory(token);
  // Obtain tail address for fuzzy index checkpoint
  if(!fold_over_snapshot) {
    checkpoint_.InitializeCheckpoint(token, desired.version, hash_index_.size(),
                                     hlog.begin_address.load(),  hlog.GetTailAddress(), true,
                                     hlog.flushed_until_address.load(),
                                     index_persistence_callback,
                                     hybrid_log_persistence_callback, callback_context);
  } else {
    checkpoint_.InitializeCheckpoint(token, desired.version, hash_index_.size(),
                                     hlog.begin_address.load(),  hlog.GetTailAddress(), false,
                                     Address::kInvalidAddress, index_persistence_callback,
                                     hybrid_log_persistence_callback, callback_context);

  }
  InitializeCheckpointLocks();
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::CheckpointIndex(IndexPersistenceCallback index_persistence_callback,
                                        Guid& token) {
  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointIndex, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateIndexCheckpointDirectory(token);
  checkpoint_.InitializeIndexCheckpoint(token, desired.version, hash_index_.size(),
                                        hlog.begin_address.load(), hlog.GetTailAddress(),
                                        index_persistence_callback);
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::CheckpointHybridLog(HybridLogPersistenceCallback hybrid_log_persistence_callback,
                                            Guid& token) {
  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointHybridLog, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateCprCheckpointDirectory(token);
  // Obtain tail address for fuzzy index checkpoint
  if(!fold_over_snapshot) {
    checkpoint_.InitializeHybridLogCheckpoint(token, desired.version, true,
        hlog.flushed_until_address.load(), hybrid_log_persistence_callback);
  } else {
    checkpoint_.InitializeHybridLogCheckpoint(token, desired.version, false,
        Address::kInvalidAddress, hybrid_log_persistence_callback);
  }
  InitializeCheckpointLocks();
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class K, class V, class D, class H, class OH>
Status FasterKv<K, V, D, H, OH>::Recover(const Guid& index_token, const Guid& hybrid_log_token,
                                  uint32_t& version,
                                  std::vector<Guid>& session_ids) {
  version = 0;
  session_ids.clear();
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::Recover, Phase::REST, expected.version })) {
    return Status::Aborted;
  }
  checkpoint_.InitializeRecover(index_token, hybrid_log_token);
  Status status;
#define BREAK_NOT_OK(s) \
    status = (s); \
    if (status != Status::Ok) break

  do {
    // Index and log metadata.
    BREAK_NOT_OK(hash_index_.ReadCheckpointMetadata(index_token, checkpoint_));
    BREAK_NOT_OK(ReadCprMetadata(hybrid_log_token));
    if(checkpoint_.index_metadata.version != checkpoint_.log_metadata.version) {
      // Index and hybrid-log checkpoints should have the same version.
      status = Status::Corruption;
      break;
    }

    system_state_.store(SystemState{ Action::Recover, Phase::REST,
                                     checkpoint_.log_metadata.version + 1 });

    BREAK_NOT_OK(ReadCprContexts(hybrid_log_token, checkpoint_.log_metadata.guids));
    // The index itself (including overflow buckets).
    BREAK_NOT_OK(hash_index_.Recover(checkpoint_));
    BREAK_NOT_OK(hash_index_.RecoverComplete());
    // Any changes made to the log while the index was being fuzzy-checkpointed.
    if(fold_over_snapshot) {
      BREAK_NOT_OK(RecoverHybridLog());
    } else {
      BREAK_NOT_OK(RecoverHybridLogFromSnapshotFile());
    }
    BREAK_NOT_OK(RestoreHybridLog());
  } while(false);
  if(status == Status::Ok) {
    for(const auto& token : checkpoint_.continue_tokens) {
      session_ids.push_back(token.first);
    }
    version = checkpoint_.log_metadata.version;
  }
  checkpoint_.RecoverDone();
  system_state_.store(SystemState{ Action::None, Phase::REST,
                                   checkpoint_.log_metadata.version + 1 });
  return status;
#undef BREAK_NOT_OK
}

template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::ShiftBeginAddress(Address address, GcState::truncate_callback_t truncate_callback,
                                                GcState::complete_callback_t complete_callback) {
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::GC, Phase::REST, expected.version })) {
    return false; // Can't start a GC while an action is already in progress.
  }
  hlog.begin_address.store(address);
  log_debug("ShiftBeginAddress: log truncated to %lu", address.control());

  // Each active thread will notify the epoch when all pending I/Os have completed.
  epoch_.ResetPhaseFinished();
  // Prepare for index garbage collection
  hash_index_.GarbageCollectSetup(hlog.begin_address.load(),
                                  truncate_callback, complete_callback);
  // Let other threads know to complete their pending I/Os, so that the log can be truncated.
  system_state_.store(SystemState{ Action::GC, Phase::GC_IO_PENDING, expected.version });
  return true;
}

template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::InternalShiftBeginAddress(Address address,
                                                         GcState::internal_truncate_callback_t truncate_callback,
                                                         GcState::internal_complete_callback_t complete_callback,
                                                         IAsyncContext* callback_context,
                                                         bool after_compaction) {
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::GC, Phase::REST, expected.version })) {
    return false; // Can't start a GC while an action is already in progress.
  }
  if (after_compaction) {
    ++num_compaction_truncations_;
  }
  hlog.begin_address.store(address);
  log_debug("ShiftBeginAddress: [after-compaction=%d] log truncated to %lu",
            after_compaction, address.control());

  if (after_compaction) {
    next_hlog_begin_address_.store(Address::kInvalidAddress);
  }
  // Each active thread will notify the epoch when all pending I/Os have completed.
  epoch_.ResetPhaseFinished();
  // Prepare for index garbage collection
  hash_index_.GarbageCollectSetup(hlog.begin_address.load(),
                                  truncate_callback, complete_callback,
                                  callback_context);
  // Let other threads know to complete their pending I/Os, so that the log can be truncated.
  system_state_.store(SystemState{ Action::GC, Phase::GC_IO_PENDING, expected.version });
  return true;
}


template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::GrowIndex(GrowCompleteCallback caller_callback) {
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::GrowIndex, Phase::REST, expected.version })) {
    // An action is already in progress.
    return false;
  }
  epoch_.ResetPhaseFinished();

  // Prepare index for index growing
  hash_index_.GrowSetup(caller_callback);
  // Switch system state to GrowIndex
  SystemState next = SystemState{ Action::GrowIndex, Phase::GROW_PREPARE, expected.version };
  system_state_.store(next);

  // Let this thread know it should be growing the index.
  Refresh();
  return true;
}

// Some printing support for testing
inline std::ostream& operator << (std::ostream& out, const Status s) {
  return out << (uint8_t)s;
}

inline std::ostream& operator << (std::ostream& out, const Guid guid) {
  return out << guid.ToString();
}

inline std::ostream& operator << (std::ostream& out, const FixedPageAddress address) {
  return out << address.control();
}

/// Compacts the hybrid log between the begin address and the specified
/// address, moving active records to the tail of log.
///
/// It identifies live records by looking up the in-memory hash index
/// and by potentially going through the hash chains, rather than
/// scanning the entire log from head to tail.
template <class K, class V, class D, class H, class OH>
inline bool FasterKv<K, V, D, H, OH>::CompactWithLookup(uint64_t until_address, bool shift_begin_address, int n_threads,
                                                  bool to_other_store, bool checkpoint) {
  Guid token;
  return InternalCompactWithLookup(until_address, shift_begin_address, n_threads, to_other_store, checkpoint, token);
}

template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::InternalCompactWithLookup(uint64_t until_address, bool shift_begin_address, int n_threads,
                                                        bool to_other_store, bool checkpoint, Guid& checkpoint_token) {
  // TODO: maybe switch to an initial phase for GC to avoid concurrent actions (e.g. checkpoint, grow index, etc.)

  if (hlog.begin_address.load() > until_address) {
    throw std::invalid_argument {"Invalid until address; should be larger than hlog.begin_address"};
  }
  if (until_address > hlog.safe_read_only_address.control()) {
    throw std::invalid_argument {"Can compact only until hlog.safe_read_only_address"};
  }
  assert(!to_other_store || (other_store_ != nullptr));

  if (!epoch_.IsProtected() || (to_other_store && !other_store_->epoch_.IsProtected())) {
    throw std::invalid_argument {"Thread should have an active FASTER session"};
  }

  if (shift_begin_address) {
    // used to avoid inserting soon-to-be-compacted records to Read Cache
    next_hlog_begin_address_.store(until_address);
  }

  std::deque<std::thread> threads;

  ConcurrentLogPageIterator<faster_t> iter(&hlog, &disk, &epoch_, hlog.begin_address.load(), Address(until_address));
  compaction_context_.Initialize(&iter, n_threads-1, to_other_store);

  // Spawn the threads first
  for (size_t idx = 0; idx < n_threads-1; ++idx) {
    threads.emplace_back(
      &FasterKv<K, V, D, H, OH>::InternalCompact<faster_t>, this, idx, std::numeric_limits<uint32_t>::max());
  }
  InternalCompact<faster_t>(-1, std::numeric_limits<uint32_t>::max()); // participate in the compaction

  // Wait for threads
  // NOTE: since we have an active session, we *must* periodically refresh
  //       in order to allow progress for remaining active threads
  //       Therefore, we cannot utilize the *blocking* `thread.join`
  //while (compaction_context_.n_threads.load() > 0) {
  int remaining = n_threads - 1;
  while (remaining > 0 || compaction_context_.active_threads.load() > 0) {
    for (size_t idx = 0; idx < n_threads-1; ++idx) {
      if (compaction_context_.thread_finished[idx].load() && threads[idx].joinable()) {
        threads[idx].join();
        --remaining;
      }
    }

    Refresh();
    if (to_other_store) {
      other_store_->Refresh();
    }
    std::this_thread::yield();
  }
  assert(remaining == 0 && compaction_context_.active_threads.load() == 0);


  if (checkpoint) {
    // index checkpoint
    CompactionCheckpointContext cc_context;

    auto index_persist_callback = [](void* ctxt, Status status) {
      auto* context = static_cast<CompactionCheckpointContext*>(ctxt);

      // callback should not have been called before (i.e., called only once!)
      Status expected = Status::Corruption;
      if (!context->index_persisted_status.compare_exchange_strong(expected, status)) {
        log_warn("Unexpected index persist status: expected = %s, actual = %s",
                STATUS_STR[static_cast<int>(Status::Corruption)], STATUS_STR[static_cast<int>(expected)]);
        assert(false);
        context->index_persisted_status.store(status);
      }
      // assert index checkpoint was successful
      assert(status == Status::Ok);
      if (status != Status::Ok) {
        log_warn("Checkpoint: Index persist process finished with non-ok status -> %s",
                STATUS_STR[static_cast<int>(status)]);
      }
    };

    auto hlog_persist_callback = [](void* ctxt, Status status, uint64_t persistent_serial_number) {
      auto* context = static_cast<CompactionCheckpointContext*>(ctxt);

      bool expected = false;
      if (!context->hlog_threads_persisted[Thread::id()].compare_exchange_strong(expected, true)) {
        log_warn("Expected checkpoint thread-specific entry to *not* have been set! "
                  "Was callback called *twice* for the same thread?");
        assert(false);
      }
      context->hlog_num_threads_persisted++;

      // Update global checkpoint status, if error or first thread to finish
      bool success;
      do {
        success = true;
        Status global_status = context->hlog_persisted_status.load();
        if (global_status == Status::Corruption || status != Status::Ok) {
          success = context->hlog_persisted_status.compare_exchange_strong(global_status, status);
        }
      } while (!success);
    };

    // Init checkpointing
    if (!to_other_store) {
      Refresh();
    } else {
      other_store_->Refresh();
    }
    bool success;
    do {
      if (!to_other_store) {
        success = InternalCheckpoint(index_persist_callback, hlog_persist_callback,
                                    checkpoint_token, static_cast<void*>(&cc_context));
      } else {
        success = other_store_->InternalCheckpoint(index_persist_callback, hlog_persist_callback,
                                                  checkpoint_token, static_cast<void*>(&cc_context));
      }
      if (!success) {
        Refresh();
        if (to_other_store) other_store_->Refresh();
        std::this_thread::yield();
      }
    } while (!success);

    // wait until checkpoint has finished
    // NOTE: sessions cannot be started/stopped during checkpointing
    size_t num_threads_active = num_active_sessions_.load();
    while (cc_context.hlog_num_threads_persisted.load() < num_threads_active) {
      CompletePending(false);
      if (to_other_store) {
        other_store_->CompletePending(false);
      }
      std::this_thread::yield();
    }

    auto index_persisted_status = cc_context.index_persisted_status.load();
    auto hlog_persisted_status = cc_context.hlog_persisted_status.load();
    assert(index_persisted_status != Status::Corruption && hlog_persisted_status != Status::Corruption);
    if (index_persisted_status != Status::Ok || hlog_persisted_status != Status::Ok) {
      log_warn("Checkpoint failed [index_persisted_status: %s, hlog_persisted_status=%s]",
                STATUS_STR[static_cast<int>(index_persisted_status)],
                STATUS_STR[static_cast<int>(hlog_persisted_status)]);
      return false;
    }

    if (!to_other_store && shift_begin_address) {
      // Wait until all threads move to REST phase
      // This ensures that the imminent log truncation operation will be successful
      auto callback = [](IAsyncContext* ctxt) {
        auto* context = static_cast<CompactionCheckpointContext*>(ctxt);
        bool success, expected = false;
        success = context->ready_for_truncation.compare_exchange_strong(expected, true);
        assert(success && expected == false);
      };
      epoch_.BumpCurrentEpoch(callback, &cc_context);

      do {
        Refresh();
        std::this_thread::yield();
      } while(!cc_context.ready_for_truncation.load());
    }
  }

  if (shift_begin_address) {
    // Truncate log & update hash index
    CompactionShiftBeginAddressContext context;

    auto truncate_callback = [](IAsyncContext* ctxt, uint64_t offset) {
      auto* context = reinterpret_cast<CompactionShiftBeginAddressContext*>(ctxt);
      bool expected = false;
      if (!context->log_truncated.compare_exchange_strong(expected, true)) {
        log_warn("Unexpected log_truncated value! [expected: FALSE, actual: %s]",
                  expected ? "TRUE" : "FALSE");
      }
    };
    auto complete_callback = [](IAsyncContext* ctxt) {
      auto* context = reinterpret_cast<CompactionShiftBeginAddressContext*>(ctxt);
      bool expected = false;
      if (!context->gc_completed.compare_exchange_strong(expected, true)) {
        log_warn("Unexpected gc_completed value! [expected: FALSE, actual: %s]",
                  expected ? "TRUE" : "FALSE");
      }
    };

    // try shift begin address
    bool success;
    do {
      success = InternalShiftBeginAddress(Address(until_address), truncate_callback,
                                          complete_callback, static_cast<IAsyncContext*>(&context), true);
      if (!success) {
        Refresh();
        if (to_other_store) other_store_->Refresh();
        std::this_thread::yield();
      }
    } while(!success);

    // block until truncation & hash index update finishes
    while (!context.log_truncated.load() || !context.gc_completed.load()) {
      CompletePending(false);
      if (to_other_store) {
        other_store_->CompletePending(false);
      }
      // Participate in GCing the hash index
      if(thread_ctx().phase != Phase::REST) {
        HeavyEnter();
      }
      std::this_thread::yield();
    }
  }

  return true;
}


template <class K, class V, class D, class H, class OH>
template <class F>
inline void FasterKv<K, V, D, H, OH>::InternalCompact(int thread_id, uint32_t max_records) {
  static constexpr uint32_t kRefreshInterval = 64;
  static constexpr uint32_t kCompletePendingInterval = 512;

  typedef CompactionConditionalInsertContext<K, V> ci_context_t;
  typedef std::unordered_set<uint64_t> pending_records_t;

  // Callback for ConditionalInsert request
  auto ci_callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<ci_context_t> context(ctxt);
    assert(result == Status::Ok || result == Status::Aborted);

    // Remove info for this record, since it has been processed
    uint64_t address = context->record_address().control();
    auto records_info = static_cast<pending_records_t*>(context->records_info());
    records_info->erase(address);
  };

  ++compaction_context_.active_threads; // one more thread taking part in compaction
  bool to_other_store = compaction_context_.to_other_store;

  pending_records_t pending_records;

  // used locally when iterating log
  Address record_address;
  record_t* record;

  // Start session for each thread that has not started one already
  bool was_epoch_protected = epoch_.IsProtected();
  if (!was_epoch_protected) {
    StartSession();
    if (to_other_store) {
      other_store_->StartSession();
    }
  }

  ConcurrentLogPage<key_t, value_t>* page{ nullptr };
  uint32_t old_page = Address::kMaxPage;

  size_t num_iter = 0;
  uint32_t records_processed = 0;
  bool pages_available = true;
  while(true) {
    bool finished = !pages_available || (records_processed >= max_records);
    if (finished) {
      goto complete_pending;
    }

    // get next record from hybrid log
    if (page != nullptr) {
      record = page->GetNextRecord(record_address);
    }

    if (record == nullptr || page == nullptr) {
      if(!compaction_context_.pages_available.load()) {
        pages_available = false;
        continue;
      }

      // Try to get next page
      while (pages_available) {
        Refresh();
        if (other_store_ && other_store_->epoch_.IsProtected()) other_store_->Refresh();
        if (refresh_callback_store_) refresh_callback_(refresh_callback_store_);

        // Try to get next page
        page = compaction_context_.iter->GetNextPage(old_page, pages_available);
        if (page != nullptr) {
          old_page = page->id();
          break;
        }
        std::this_thread::yield();
      }
      if (!pages_available) {
        compaction_context_.pages_available.store(false);
      }

      continue;
    }

    if (record->header.tombstone && !to_other_store)  {
      goto complete_pending; // do not compact tombstones
    }

    {
      assert(record_address != Address::kInvalidAddress);

      // add to pending records
      assert(pending_records.find(record_address.control()) == pending_records.end());
      pending_records.insert(record_address.control());

      // Insert record if not exists already in (recordAddress, tailAddress] range
      ++records_processed;
      ci_context_t ci_context{ record, record_address, &pending_records };
      Status status = ConditionalInsert(ci_context, ci_callback,
                                        record_address, to_other_store);
      assert(status == Status::Ok || status == Status::Aborted || status == Status::Pending);

      if (status == Status::Ok || status == Status::Aborted) {
        // clear record info, since request it has been processed
        // (i.e. either inserted at the end, or didn't)
        pending_records.erase(record_address.control());
      } else if (status == Status::NotFound) {
        throw std::runtime_error{ "not possible" };
      }
    }

complete_pending:
    bool try_complete_pending = \
      (num_iter % kCompletePendingInterval == 0) ||
      (!pending_records.empty() ) || (record == nullptr);
    if (try_complete_pending) {
      // Try to complete pending requests
      CompletePending(false);
      if (to_other_store) {
        other_store_->CompletePending(false);
      }
      if (record == nullptr) {
        std::this_thread::yield();
      }
    }
    if (num_iter % kRefreshInterval == 0) {
      // Periodically refresh epoch
      Refresh();
      if (to_other_store) other_store_->Refresh();
    }

    ++num_iter;
    if (finished && pending_records.empty()) {
      // no more records to compact && no callbacks pending
      break;
    }
  }
  assert(pending_records.empty());

  if (!was_epoch_protected) {
    // This thread was spawned *only* for compaction and should have finished all activity
    if (to_other_store) { // hot-cold compaction
      assert(other_store_->thread_ctx().retry_requests.empty());
      assert(other_store_->thread_ctx().pending_ios.empty());
      assert(other_store_->thread_ctx().io_responses.empty());
    } else { // single log / cold-cold compaction
      assert(thread_ctx().retry_requests.empty());
      assert(thread_ctx().pending_ios.empty());
      assert(thread_ctx().io_responses.empty());
    }
  }

  // Stop session for each spawned thread
  if (!was_epoch_protected) {
    StopSession();
    if (to_other_store) {
      other_store_->StopSession();
    }
  }
  if (thread_id >= 0) {
    assert(!was_epoch_protected);
    // Mark thread as finished
    compaction_context_.thread_finished[thread_id].store(true);
  }
  --compaction_context_.active_threads;
}


template <class K, class V, class D, class H, class OH>
template <class CIC>
inline Status FasterKv<K, V, D, H, OH>::ConditionalInsert(CIC& context, AsyncCallback callback,
                                                  Address min_search_offset, bool to_other_store) {
  typedef CIC conditional_insert_context_t;
  typedef PendingConditionalInsertContext<CIC> pending_ci_context_t;
  static_assert(std::is_base_of<value_t, typename pending_ci_context_t::value_t>::value,
                "value_t is not a base class of read_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename pending_ci_context_t::value_t),
                "alignof(value_t) != alignof(typename read_context_t::value_t)");

  pending_ci_context_t pending_context{ context, callback, min_search_offset, to_other_store };
  OperationStatus internal_status = InternalConditionalInsert(pending_context);

  #ifdef STATISTICS
  if (collect_stats_) {
    ++ci_total_;
    if (internal_status == OperationStatus::SUCCESS ||
        internal_status == OperationStatus::NOT_FOUND ||
        internal_status == OperationStatus::ABORTED ||
        internal_status == OperationStatus::ASYNC_TO_COLD_STORE) {
      ++ci_sync_;
      ++ci_per_req_iops_[0];
    }
  }
  #endif

  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    // insert performed successfully
    status = Status::Ok;
  } else if (internal_status == OperationStatus::ABORTED) {
    // insert aborted due to the existence of newer record
    status = Status::Aborted;
  } else if (internal_status == OperationStatus::NOT_FOUND) {
    // was not able to properly search up to min_search_offset
    status = Status::NotFound;
  } else {
    assert(internal_status == OperationStatus::RECORD_ON_DISK ||
          internal_status == OperationStatus::INDEX_ENTRY_ON_DISK ||
          internal_status == OperationStatus::ASYNC_TO_COLD_STORE);
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }

  // TODO FIX THIS
  //thread_ctx().serial_num = monotonic_serial_num;
  return status;
}


template <class K, class V, class D, class H, class OH>
template <class C>
inline OperationStatus FasterKv<K, V, D, H, OH>::InternalConditionalInsert(C& pending_context) {
  bool to_other_store = pending_context.to_other_store;

  Address min_search_offset = pending_context.min_search_offset;
  assert(!min_search_offset.in_readcache());

  // Handle hash index GC operation
  if(thread_ctx().phase != Phase::REST) {
    HeavyEnter();
  }
  if(to_other_store && other_store_->thread_ctx().phase != Phase::REST) {
    other_store_->HeavyEnter();
  }

  Address begin_address = hlog.begin_address.load();
  Address head_address = hlog.head_address.load();

  // Retrieve or create hash index bucket for this entry
  Status index_status;
  if (pending_context.index_op_type == IndexOperationType::None) {
    index_status = hash_index_.FindOrCreateEntry(thread_ctx(), pending_context);
    if (index_status == Status::Pending) {
      return OperationStatus::INDEX_ENTRY_ON_DISK;
    }
  } else {
    // retrieve result of async index operation
    index_status = pending_context.index_op_result;
    pending_context.clear_index_op();
  }
  assert(index_status == Status::Ok);

  Address address = pending_context.entry.address();

  if (UseReadCache()) {
    // entries in readcache do not count as "active"
    address = read_cache_->Skip(pending_context);
  }
  pending_context.expected_hlog_address = address;
  assert(!address.in_readcache());

  // (Note that address will be Address::kInvalidAddress, if the entry was created.)
  if (min_search_offset == Address::kInvalidAddress) {
    // == Only possible in HC-RMW Copy case ==
    // no other record exists for the same key in this log
    if (address == Address::kInvalidAddress) {
      if (pending_context.orig_hlog_tail_address() >= begin_address) {
        //log_info("%llu %llu", pending_context.orig_hlog_tail_address().control(), begin_address.control());
        // last condition ensure that we did *not* lose any records
        goto create_record;
      }
    }
    // cannot determine where the min_search_offset should be
    // request needs to be retried at a higher level!
    return OperationStatus::NOT_FOUND;
  }
  else if (address == Address::kInvalidAddress) {
    // == Only possible in HC-RMW Copy case ==
    // record pointed by min_search_offset compacted to cold log
    // it was certainly a hash-collision, because otherwise the initial
    // hot-log RMW would have worked.
    if (pending_context.orig_hlog_tail_address() >= begin_address) {
      // last condition ensure that we did *not* lose any records
      goto create_record;
    }
  }
  else if (min_search_offset < begin_address) {
    // == Only possible in HC-RMW Copy case ==
    // result of log truncation -- cannot happen during hot-cold/cold-cold compaction
    // request should be re-tried at a higher level!
    return OperationStatus::NOT_FOUND;
  }
  assert(address != Address::kInvalidAddress && min_search_offset != Address::kInvalidAddress);

  if(address >= head_address && min_search_offset != pending_context.expected_hlog_address) {
    const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(address));
    Address search_until = std::max(min_search_offset + 1, head_address);
    if(!pending_context.is_key_equal(record->key())) {
      address = TraceBackForKeyMatchCtxt(pending_context, record->header.previous_address(), search_until);
    }
  }

  if (address < min_search_offset) {
    // == Only possible in cold-cold compaction, when using *async* cold-index ==
    // not part of the same hash chain due to the flush-before-marked-invalid race condition
    return OperationStatus::ABORTED;
  }
  assert(address >= min_search_offset); // part of the same hash chain

  if(address >= head_address) {
    // Mutable, fuzzy or immutable region [in-memory]
    if (address > min_search_offset) {
      // Found newer record with same key
      return OperationStatus::ABORTED;
    }
    assert(address == min_search_offset);
  }
  else if(address >= begin_address) {
    // Stable region [on-disk]
    if (address > min_search_offset) {
      // Record not available in-memory -- issue disk I/O request
      pending_context.go_async(address);
      return OperationStatus::RECORD_ON_DISK;
    }
    assert(address == min_search_offset);
  }
  else {
    // Cannot reach min address because it has been truncated
    // Request needs to be retried at a higher level!
    assert(min_search_offset < begin_address);
    return OperationStatus::NOT_FOUND;
  }

  // Update search range
  pending_context.min_search_offset = pending_context.expected_hlog_address;

create_record:
  if (UseReadCache()) {
    // Record will be moved either to the tail, or to another log
    // Thus, make any potential read cache record invalid
    read_cache_->SkipAndInvalidate(pending_context);
  }

  // Append entry to the log
  if (!to_other_store) {
    // Case: Single log compaction, or Hot-Cold RMW conditional insert
    assert(!pending_context.is_tombstone());
    // Create record
    uint32_t record_size = record_t::size(pending_context.key_size(), pending_context.value_size());
    Address new_address = BlockAllocate(record_size);
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(new_address));
    // Copy record
    bool success = pending_context.Insert(record, record_size);
    assert(success);
    // Replace record header content
    new(record) record_t{
      RecordInfo{
        static_cast<uint16_t>(thread_ctx().version),
        true, false, false, pending_context.expected_hlog_address }
    };
    // Try to update hash bucket address
    index_status = hash_index_.TryUpdateEntry(thread_ctx(), pending_context, new_address);

    if (index_status == Status::Ok) {
      // Installed the new record in the hash table.
      #ifdef STATISTICS
      if (collect_stats_) {
        ++ci_copied_;
      }
      #endif

      return OperationStatus::SUCCESS;
    } else if (index_status == Status::Pending) {
      return OperationStatus::INDEX_ENTRY_ON_DISK;
    } else {
      assert(index_status == Status::Aborted);
      // Hash-chain changed since last time -- retry!
      record->header.invalid = true;

      #ifdef STATISTICS
      if (collect_stats_) {
        ++pending_context.num_record_invalidations;
      }
      #endif

      return InternalConditionalInsert(pending_context);
    }
  }
  else {
    // Case: hot-cold compaction
    // Upsert/Deletes on cold log should not go pending since upserts only go pending when during checkpointing
    // ---> And no two hot-cold compactions can take place simultaneously
    if (!pending_context.is_tombstone()) {
      async_pending_upsert_context_t* upsert_context = pending_context.ConvertToUpsertContext();
      OperationStatus op_status = other_store_->InternalUpsert(*upsert_context);
      assert(op_status == OperationStatus::SUCCESS || op_status == OperationStatus::INDEX_ENTRY_ON_DISK);

      #ifdef STATISTICS
      if (other_store_->collect_stats_) {
        // Update stats of `other_store_'
        ++(other_store_->upserts_total_);
        if (op_status == OperationStatus::SUCCESS) {
          ++(other_store_->upserts_sync_);
          ++(other_store_->upserts_per_req_iops_[0]);
        }
      }
      #endif

      if (op_status == OperationStatus::SUCCESS) {
        // free upsert context(s) before returning
        CallbackContext<async_pending_upsert_context_t> upsert_ctxt{ upsert_context };
        if (!pending_context.from_deep_copy()) {
          // free if ConditionalInsert did not go pending; otherwise it will be freed by CompleteIoPendingRequests
          CallbackContext<IAsyncContext> upsert_caller_ctxt{ upsert_context->caller_context };
        }

        #ifdef STATISTICS
        if (collect_stats_) {
          ++ci_copied_;
        }
        #endif

        return op_status;
      } else {
        return OperationStatus::ASYNC_TO_COLD_STORE;
      }
    } else {
      async_pending_delete_context_t* delete_context = pending_context.ConvertToDeleteContext();
      OperationStatus op_status = other_store_->InternalDelete(*delete_context, true);
      assert(op_status == OperationStatus::SUCCESS || op_status == OperationStatus::INDEX_ENTRY_ON_DISK);

      #ifdef STATISTICS
      if (other_store_->collect_stats_) {
        // Update stats of `other_store_'
        ++(other_store_->deletes_total_);
        if (op_status == OperationStatus::SUCCESS) {
          ++(other_store_->deletes_sync_);
          ++(other_store_->deletes_per_req_iops_[0]);
        }
      }
      #endif

      if (op_status == OperationStatus::SUCCESS) {
        // free delete context(s) before returning
        CallbackContext<async_pending_delete_context_t> delete_ctxt{ delete_context };
        if (!pending_context.from_deep_copy()) {
          // free if ConditionalInsert did not go pending; otherwise it will be freed by CompleteIoPendingRequests
          CallbackContext<IAsyncContext> delete_caller_ctxt{ delete_context->caller_context };
        }

        #ifdef STATISTICS
        if (collect_stats_) {
          ++ci_copied_;
        }
        #endif

        return op_status;
      } else {
        return OperationStatus::ASYNC_TO_COLD_STORE;
      }
    }
  }
}

/// When invoked, compacts the hybrid-log between the begin address and a
/// passed in offset (`untilAddress`).
template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::Compact(uint64_t untilAddress)
{
  typedef IndexContext<K, V> index_context_t;

  // First, initialize a mini FASTER that will store all live records in
  // the range [beginAddress, untilAddress).
  Address begin = hlog.begin_address.load();
  auto size = 2 * (untilAddress - begin.control());
  if (size < 0) return false;

  auto pSize = PersistentMemoryMalloc<D>::kPageSize;
  size = std::max(8 * pSize, size);

  if (size % pSize != 0) size += pSize - (size % pSize);

  faster_t tempKv(min_index_table_size_, size, "");
  tempKv.StartSession();

  // In the first phase of compaction, scan the hybrid-log between addresses
  // [beginAddress, untilAddress), adding all live records to the mini FASTER.
  // On encountering a tombstone, we try to delete the record from the mini
  // instance of FASTER.
  int numOps = 0;
  LogRecordIterator<faster_t> iter(&hlog, Buffering::DOUBLE_PAGE, begin,
                              Address(untilAddress), &disk);
  while (true) {
    auto r = iter.GetNext();
    if (r == nullptr) break;

    if (!r->header.tombstone) {
      CompactionUpsert<K, V> ctxt(r);
      auto cb = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<CompactionUpsert<K, V>> context(ctxt);
        assert(result == Status::Ok);
      };
      tempKv.Upsert(ctxt, cb, 0);
    } else {
      CompactionDelete<K, V> ctxt(r);
      auto cb = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<CompactionDelete<K, V>> context(ctxt);
        assert(result == Status::Ok);
      };
      tempKv.Delete(ctxt, cb, 0);
    }

    if (++numOps % 1000 == 0) {
      tempKv.Refresh();
      Refresh();
    }
  }

  // Scan the remainder of the hybrid log, deleting all encountered records
  // from the temporary/mini FASTER instance.
  auto upto = LogScanForValidity(Address(untilAddress), &tempKv);

  // Finally, scan through all records within the temporary FASTER instance,
  // inserting those that don't already exist within FASTER's mutable region.
  numOps = 0;
  LogRecordIterator<faster_t> iter2(&tempKv.hlog, Buffering::DOUBLE_PAGE,
                               tempKv.hlog.begin_address.load(),
                               tempKv.hlog.GetTailAddress(), &tempKv.disk);
  while (true) {
    auto r = iter2.GetNext();
    if (r == nullptr) break;

    index_context_t index_context{ r };

    if (!r->header.tombstone && !ContainsKeyInMemory(index_context, upto)) {
      CompactionUpsert<K, V> ctxt(r);
      auto cb = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<CompactionUpsert<K, V>> context(ctxt);
        assert(result == Status::Ok);
      };

      Upsert(ctxt, cb, 0);
    }

    if (++numOps % 1000 == 0) {
      tempKv.Refresh();
      Refresh();
    }

    // The safe-read-only region might have moved forward since the previous
    // log scan. If it has, perform another validity scan over the delta.
    if (upto < hlog.safe_read_only_address.load()) {
      upto = LogScanForValidity(upto, &tempKv);
    }
  }

  tempKv.StopSession();
  return true;
}

/// Scans the hybrid log starting at `from` until the safe-read-only address,
/// deleting all encountered records from within a passed in temporary FASTER
/// instance.
///
/// Useful for log compaction where the temporary instance contains potentially
/// live records that were found before `from` on the log. This method will then
/// delete all records from within that instance that are dead because they exist
/// in the safe-read-only region of the main FASTER instance.
///
/// Returns the address upto which the scan was performed.
template <class K, class V, class D, class H, class OH>
Address FasterKv<K, V, D, H, OH>::LogScanForValidity(Address from, faster_t* temp)
{
  // Scan upto the safe read only region of the log, deleting all encountered
  // records from the temporary instance of FASTER. Since the safe-read-only
  // offset can advance while we're scanning, we repeat this operation until
  // we converge.
  Address sRO = hlog.safe_read_only_address.load();
  while (from < sRO) {
    int numOps = 0;
    LogRecordIterator<faster_t> iter(&hlog, Buffering::DOUBLE_PAGE, from,
                                sRO, &disk);
    while (true) {
      auto r = iter.GetNext();
      if (r == nullptr) break;

      CompactionDelete<K, V> ctxt(r);
      auto cb = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<CompactionDelete<K, V>> context(ctxt);
        assert(result == Status::Ok);
      };
      temp->Delete(ctxt, cb, 0);

      if (++numOps % 1000 == 0) {
        temp->Refresh();
        Refresh();
      }
    }

    // Refresh Faster, updating our start and end addresses for the convergence
    // check in the while loop above.
    Refresh();
    from = sRO;
    sRO = hlog.safe_read_only_address.load();
  }

  return sRO;
}

/// Checks if a key exists between a passed in address (`offset`) and the
/// current tail of the hybrid log.
template <class K, class V, class D, class H, class OH>
bool FasterKv<K, V, D, H, OH>::ContainsKeyInMemory(IndexContext<key_t, value_t> context, Address offset)
{
  // First, retrieve the hash table entry corresponding to this key.
  Status status = hash_index_.FindEntry(thread_ctx(), context);
  assert(status == Status::Ok || status == Status::NotFound);
  if (!context.atomic_entry) return false;

  Address address = context.entry.address();

  if (address >= offset) {
    // Look through the in-memory portion of the log, to find the first record
    // (if any) whose key matches.
    const record_t* record =
              reinterpret_cast<const record_t*>(hlog.Get(address));
    if(context.key() != record->key()) {
      address = TraceBackForKeyMatch(context.key(), record->header.previous_address(),
                                     offset);
    }
  }

  // If we found a record after the passed in address then we succeeded.
  // Otherwise, we failed and so return false.
  if (address >= offset) return true;
  return false;
}

template <class K, class V, class D, class H, class OH>
void FasterKv<K, V, D, H, OH>::AutoCompactHlog() {
  uint64_t hlog_size_threshold = static_cast<uint64_t>(
    hlog_compaction_config_.hlog_size_budget * hlog_compaction_config_.trigger_pct);

  max_hlog_size_ = hlog_compaction_config_.hlog_size_budget;

  while (auto_compaction_active_.load()) {
    if (Size() < hlog_size_threshold) {
      auto_compaction_scheduled_.store(false);
      std::this_thread::sleep_for(hlog_compaction_config_.check_interval);
      continue;
    }
    auto_compaction_scheduled_.store(true);

    uint64_t begin_address = hlog.begin_address.control();
    uint64_t tail_address = hlog.GetTailAddress().control();

    /// calculate until address
    // start with compaction percentage
    uint64_t until_address = begin_address + static_cast<uint64_t>(Size() * hlog_compaction_config_.compact_pct);
    // respect user-imposed limit
    until_address = std::min(until_address, begin_address + hlog_compaction_config_.max_compacted_size);
    // do not compact in-memory region
    until_address = std::min(until_address, hlog.safe_head_address.control());
    // round down address to page bounds
    until_address = until_address - Address(until_address).offset() + Address::kMaxOffset + 1;
    assert(until_address <= hlog.safe_read_only_address.control());
    assert(until_address % hlog.kPageSize == 0);

    // perform log compaction
    log_error("Auto-compaction: [%lu %lu] -> [%lu %lu] {%lu}",
              begin_address, tail_address, until_address, tail_address, Size());
    StartSession();
    bool success = CompactWithLookup(until_address, true, hlog_compaction_config_.num_threads);
    StopSession();

    log_error("Auto-compaction: Size: %.3f GB", static_cast<double>(Size()) / (1 << 30));
    if (!success) {
      log_error("Hlog compaction was not successful :( -- retry!");
    }
  }
  // no more auto-compactions
  auto_compaction_scheduled_.store(false);
}

#ifdef TOML_CONFIG
template <class K, class V, class D, class H, class OH>
inline FasterKv<K, V, D, H, OH> FasterKv<K, V, D, H, OH>::FromConfigString(const std::string& config) {
  return Config::FromConfigString(config, "faster");
}

template <class K, class V, class D, class H, class OH>
inline FasterKv<K, V, D, H, OH> FasterKv<K, V, D, H, OH>::FromConfigFile(const std::string& filepath) {
  std::ifstream t(filepath);
  std::string config(
    (std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
  return FasterKv<K, V, D, H, OH>::FromConfigString(config);
}
#endif


}
} // namespace FASTER::core
