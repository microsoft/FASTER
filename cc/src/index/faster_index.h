// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "../core/async.h"
#include "../core/async_result_types.h"
#include "../core/checkpoint_state.h"
#include "../core/faster.h"
#include "../core/gc_state.h"
#include "../core/grow_state.h"
#include "../core/internal_contexts.h"
#include "../core/light_epoch.h"
#include "../core/persistent_memory_malloc.h"
#include "../core/state_transitions.h"
#include "../core/status.h"

#include "faster_index_contexts.h"
#include "mem_index.h"
#include "hash_bucket.h"
#include "hash_table.h"
#include "index.h"

using namespace FASTER::core;

namespace FASTER {
namespace core {
  // Forward Declaration
  template<class K, class V, class D, class H, class OH>
  class FasterKv;
}
}

namespace FASTER {
namespace index {

// 32 entries -- 4 buckets in chunk w/ 8 entries per chunk bucket [256 bytes]
constexpr static uint8_t DEFAULT_NUM_ENTRIES = 32;

template<class D, class HID = ColdLogHashIndexDefinition<DEFAULT_NUM_ENTRIES>>
class FasterIndex : public IHashIndex<D> {
 public:
  typedef FasterIndex<D, HID> faster_index_t;

  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  typedef HID hash_index_definition_t;
  typedef typename HID::key_hash_t key_hash_t;
  typedef typename HID::hash_bucket_t hash_bucket_t;
  typedef typename HID::hash_bucket_entry_t hash_bucket_entry_t;
  typedef typename HID::hash_bucket_chunk_entry_t hash_bucket_chunk_entry_t;

  typedef FasterIndexReadContext<HID> read_context_t;
  typedef FasterIndexRmwContext<HID> rmw_context_t;

  typedef HashIndexChunkKey<key_hash_t> hash_index_chunk_key_t;
  typedef HashIndexChunkEntry<HID::kNumBuckets> hash_index_chunk_entry_t;

  typedef HashIndex<D, HID> hash_index_t;
  typedef FasterKv<hash_index_chunk_key_t, hash_index_chunk_entry_t, disk_t, hash_index_t, hash_index_t> store_t;

  typedef GcStateFasterIndex gc_state_t;
  typedef GrowState<hlog_t> grow_state_t;

  FasterIndex(const std::string& root_path, disk_t& disk, LightEpoch& epoch,
              gc_state_t& gc_state, grow_state_t& grow_state)
    : IHashIndex<D>(root_path, disk, epoch)
    , gc_state_{ &gc_state }
    , grow_state_{ &grow_state }
    , table_size_{ 0 }
    , serial_num_{ 0 } {
      // create faster index root path
      this->root_path_ = FASTER::environment::NormalizePath(this->root_path_ + "faster-index");
      log_info("FasterIndex will be stored @ %s", this->root_path_.c_str());
      std::experimental::filesystem::create_directories(this->root_path_);

      // checkpoint-related vars
      checkpoint_pending_.store(false);
      checkpoint_failed_.store(false);
  }

  struct Config {
    Config(uint64_t table_size_, uint64_t in_mem_size_, double mutable_fraction_)
      : table_size{ table_size_ }
      , in_mem_size{ in_mem_size_ }
      , mutable_fraction{ mutable_fraction_ } {
    }

    #ifdef TOML_CONFIG
    explicit Config(const toml::value& table) {
      table_size = toml::find<uint64_t>(table, "table_size");
      in_mem_size = toml::find<uint64_t>(table, "in_mem_size_mb") * (1 << 20);
      mutable_fraction = toml::find<double>(table, "mutable_fraction");

      // Warn if unexpected fields are found
      const std::vector<std::string> VALID_INDEX_FIELDS = { "table_size", "in_mem_size_mb", "mutable_fraction" };
      for (auto& it : toml::get<toml::table>(table)) {
        if (std::find(VALID_INDEX_FIELDS.begin(), VALID_INDEX_FIELDS.end(), it.first) == VALID_INDEX_FIELDS.end()) {
          fprintf(stderr, "WARNING: Ignoring invalid *index* field '%s'\n", it.first.c_str());
        }
      }
    }
    #endif

    uint64_t table_size;      // Size of the hash index table
    uint64_t in_mem_size;     // In-memory size of the hlog
    double mutable_fraction;  // Hlog mutable region fraction
  };

  void Initialize(const Config& config) {
    if(!Utility::IsPowerOfTwo(config.table_size)) {
      throw std::invalid_argument{ "Index size is not a power of 2" };
    }
    if(config.table_size > INT32_MAX) {
      throw std::invalid_argument{ "Cannot allocate such a large hash table" };
    }
    this->resize_info.version = 0;

    assert(table_size_ == 0);
    table_size_ = config.table_size;

    log_info("FasterIndex: %lu HT entries -> %lu MiB in-memory [~%lu MiB mutable]",
              table_size_, config.in_mem_size / (1 << 20UL),
              static_cast<uint64_t>(config.in_mem_size * config.mutable_fraction) / (1 << 20UL));

    typename store_t::IndexConfig store_config{ table_size_ };
    store_ = std::make_unique<store_t>(store_config, config.in_mem_size,
                                      this->root_path_, config.mutable_fraction);
  }
  void SetRefreshCallback(void* faster, RefreshCallback callback) {
    store_->SetRefreshCallback(faster, callback);
  }

  // Find the hash bucket entry, if any, corresponding to the specified hash.
  // The caller can use the "expected_entry" to CAS its desired address into the entry.
  template <class C>
  Status FindEntry(ExecutionContext& exec_context, C& pending_context) const;

  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // create a new entry. The caller can use the "expected_entry" to CAS its desired address into
  // the entry.
  template <class C>
  Status FindOrCreateEntry(ExecutionContext& exec_context, C& pending_context);

  template <class C>
  Status TryUpdateEntry(ExecutionContext& exec_context, C& pending_context,
                        Address new_address, bool readcache = false);

  template <class C>
  Status UpdateEntry(ExecutionContext& exec_context, C& pending_context, Address new_address);

  // Garbage Collect methods
  template <class TC, class CC>
  void GarbageCollectSetup(Address new_begin_address,
                          TC truncate_callback,
                          CC complete_callback,
                          IAsyncContext* callback_context = nullptr);

  template <class RC>
  bool GarbageCollect(RC* read_cache);

  // Index grow related methods
  void GrowSetup(GrowCompleteCallback callback) {
    throw std::runtime_error{ "Not Implemented!" };
  }

  template <class R>
  void Grow() {
    throw std::runtime_error{ "Not Implemented!" };
  }

  void StartSession() {
    store_->StartSession();
  }
  void StopSession() {
    store_->StopSession();
  }
  bool CompletePending() {
    return store_->CompletePending(false);
  }
  void Refresh() {
    store_->Refresh();
  }

  // Checkpointing methods
  template <class RC>
  Status Checkpoint(CheckpointState<file_t>& checkpoint, const RC* read_cache) {
    if (read_cache != nullptr) {
      log_warn("Cold-index should not be possible to be combined with read-cache. Ignoring...");
    }
    auto num_active_sessions = store_->NumActiveSessions();
    if (num_active_sessions != 1) {
      log_warn("Initiating cold-index checkpoint w/ %lu active sessions "
              "(expected 1)", num_active_sessions);
    }

    // Callback called when hash chunk FasterKv checkpointing finishes
    auto callback = [](void* ctxt, Status result, uint64_t persistent_serial_num) {
      auto* faster_index = static_cast<faster_index_t*>(ctxt);

      if (result != Status::Ok) {
        log_warn("cold-index: checkpointing of hash chunk FasterKv store failed! [status: %s]",
                  STATUS_STR[static_cast<int>(result)]);
      }
      faster_index->checkpoint_failed_.store(result != Status::Ok);
      faster_index->checkpoint_pending_.store(false);
    };
    checkpoint_pending_.store(true);
    checkpoint_failed_.store(false);

    // Begin hash chunk FasterKv checkpointing...
    if (!store_->InternalCheckpoint(nullptr, callback, checkpoint.index_token,
                                    static_cast<faster_index_t*>(this))) {
      return Status::Aborted;
    }
    checkpoint.index_checkpoint_started = true;
    return Status::Ok;
  }
  Status CheckpointComplete() {
    if (checkpoint_pending_) {
      return Status::Pending;
    }
    return checkpoint_failed_ ? Status::IOError : Status::Ok;
  }
  Status WriteCheckpointMetadata(CheckpointState<file_t>& checkpoint) {
    return IHashIndex<D>::WriteCheckpointMetadata(checkpoint);
  }

  // Recovery methods
  Status Recover(CheckpointState<file_t>& checkpoint) {
    std::vector<Guid> session_ids; // do not need these, since only (ephemeral) compaction threads are used
    return store_->Recover(checkpoint.index_token, checkpoint.index_token,
                          checkpoint.index_metadata.version, session_ids);
  }
  Status RecoverComplete() {
    return Status::Ok; // Recovery is already completed (i.e., when Recovery function returns)
  }
  Status ReadCheckpointMetadata(const Guid& token, CheckpointState<file_t>& checkpoint) {
    return IHashIndex<D>::ReadCheckpointMetadata(token, checkpoint);
  }

  // Number of in-memory table entries
  inline uint64_t size() const {
    return store_->hash_index_.size();
  }
  inline uint64_t new_size() const {
    return store_->hash_index_.new_size();
  }
  constexpr static bool IsSync() {
    return false;
  }

  void DumpDistribution() const {
    store_->hash_index_.DumpDistribution();
  }

 private:
  static void AsyncEntryOperationDiskCallback(IAsyncContext* ctxt, Status result);

  template <class C>
  Status ReadIndexEntry(ExecutionContext& exec_context, C& pending_context) const;

  template <class C>
  Status RmwIndexEntry(ExecutionContext& exec_context, C& pending_context,
                      Address new_address, HashBucketEntry expected_entry, bool force_update);


  inline void MarkRequestAsPending(ExecutionContext& exec_context, key_hash_t hash) {
    uint64_t io_id = exec_context.io_id++;
    exec_context.pending_ios.insert({ io_id, hash });
  }

  gc_state_t* gc_state_;
  grow_state_t* grow_state_;

 public:
  std::unique_ptr<store_t> store_;

  std::atomic<bool> checkpoint_pending_;
  std::atomic<bool> checkpoint_failed_;

 private:
  uint64_t table_size_;
  std::atomic<uint64_t> serial_num_;

#ifdef STATISTICS
 public:
  void EnableStatsCollection() {
    collect_stats_ = true;
  }
  void DisableStatsCollection() {
    collect_stats_ = false;
  }
  // implementation at the end of this file
  void PrintStats() const;

 private:
  //static bool collect_stats_;
  bool collect_stats_{ true };

  // FindEntry
  mutable std::atomic<uint64_t> find_entry_calls_{ 0 };
  mutable std::atomic<uint64_t> find_entry_sync_calls_{ 0 };
  mutable std::atomic<uint64_t> find_entry_success_count_{ 0 };
  // FindOrCreateEntry
  std::atomic<uint64_t> find_or_create_entry_calls_{ 0 };
  std::atomic<uint64_t> find_or_create_entry_sync_calls_{ 0 };
  // TryUpdateEntry
  std::atomic<uint64_t> try_update_entry_calls_{ 0 };
  std::atomic<uint64_t> try_update_entry_sync_calls_{ 0 };
  std::atomic<uint64_t> try_update_entry_success_count_{ 0 };
  // UpdateEntry
  std::atomic<uint64_t> update_entry_calls_{ 0 };
  std::atomic<uint64_t> update_entry_sync_calls_{ 0 };
#endif

};

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::FindEntry(ExecutionContext& exec_context,
                                            C& pending_context) const {
  pending_context.index_op_type = IndexOperationType::Retrieve;
  Status status = ReadIndexEntry(exec_context, pending_context);

#ifdef STATISTICS
  if (collect_stats_) {
    ++find_entry_calls_;
    switch(status) {
      case Status::Ok:
        ++find_entry_success_count_;
      case Status::NotFound:
        ++find_entry_sync_calls_;
        break;
      case Status::Pending:
      default:
        break;
    }
  }
#endif

  return status;
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::ReadIndexEntry(ExecutionContext& exec_context, C& pending_context) const {
  key_hash_t hash{ pending_context.get_key_hash() };
  // Use the hash from the original key to form the hash for the cold-index
  hash_index_chunk_key_t key{ hash.chunk_id(table_size_), hash.tag() };
  HashIndexChunkPos pos{ hash.index_in_chunk(), hash.tag_in_chunk() };

  uint64_t io_id = exec_context.io_id++;
  read_context_t context{ OperationType::Read, pending_context, io_id,
                          &exec_context.index_io_responses, key, pos };
#ifdef STATISTICS
  context.set_hash_index((void*)this);
#endif

  // TODO: replace serial_num with thread-local (possibly inside )
  Status status = store_->Read(context, AsyncEntryOperationDiskCallback, 0);
  assert(status == Status::Ok ||
        status == Status::NotFound ||
        status == Status::Pending);

  if (status == Status::Pending) {
    // Request went pending
    exec_context.pending_ios.insert({ io_id, hash.to_keyhash() });
    return Status::Pending;
  }

  // if hash chunk not found, then entry is also not found
  // if found, then entry might have been found or not!
  status = (status == Status::NotFound) ? Status::NotFound : context.result;
  pending_context.set_index_entry(context.entry, nullptr);
  pending_context.clear_index_op();
  return status;
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::FindOrCreateEntry(ExecutionContext& exec_context, C& pending_context) {
  pending_context.index_op_type = IndexOperationType::Retrieve;
  // replace with desired entry (i.e., Address::kInvalidAddress) *only* if entry is expected to be empty
  Status status = RmwIndexEntry(exec_context, pending_context, Address::kInvalidAddress,
                              HashBucketEntry::kInvalidEntry, false);
#ifdef STATISTICS
  if (collect_stats_) {
    ++find_or_create_entry_calls_;
    switch(status) {
      case Status::Ok:
        ++find_or_create_entry_sync_calls_;
        break;
      case Status::Pending:
      default:
        break;
    }
  }
#endif

  return status;
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::TryUpdateEntry(ExecutionContext& exec_context, C& pending_context,
                                                Address new_address, bool readcache) {
  pending_context.index_op_type = IndexOperationType::Update;
  Status status = RmwIndexEntry(exec_context, pending_context, new_address, pending_context.entry, false);

#ifdef STATISTICS
  if (collect_stats_) {
    ++try_update_entry_calls_;
    switch(status) {
      case Status::Ok:
        ++try_update_entry_success_count_;
      case Status::NotFound:
        ++try_update_entry_sync_calls_;
        break;
      case Status::Pending:
      default:
        break;
    }
  }
#endif

  return status;
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::UpdateEntry(ExecutionContext& exec_context, C& pending_context,
                                              Address new_address) {
  pending_context.index_op_type = IndexOperationType::Update;
  Status status = RmwIndexEntry(exec_context, pending_context, new_address, pending_context.entry, true);

  #ifdef STATISTICS
    if (collect_stats_) {
      ++update_entry_calls_;
      switch(status) {
        case Status::Ok:
          ++update_entry_sync_calls_;
          break;
        case Status::Pending:
        default:
          break;
      }
    }
  #endif

  return status;
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::RmwIndexEntry(ExecutionContext& exec_context, C& pending_context,
                                                Address new_address, HashBucketEntry expected_entry,
                                                bool force_update) {
  key_hash_t hash{ pending_context.get_key_hash() };
  // Use the hash from the original key to form the hash for the cold-index
  hash_index_chunk_key_t key{ hash.chunk_id(table_size_), hash.tag() };
  HashIndexChunkPos pos{ hash.index_in_chunk(), hash.tag_in_chunk() };

  // FIXME: avoid incrementing io_id for every request if possible
  uint64_t io_id = exec_context.io_id++;

  // TODO: check usefulness of tentative flag here
  hash_bucket_chunk_entry_t desired_entry{ new_address, pos.tag, false };
  rmw_context_t context{ OperationType::RMW, pending_context, io_id,
                        &exec_context.index_io_responses,
                        key, pos, force_update, expected_entry, desired_entry };
#ifdef STATISTICS
  context.set_hash_index((void*)this);
#endif

  Status status = store_->Rmw(context, AsyncEntryOperationDiskCallback, ++serial_num_);
  assert(status == Status::Ok || status == Status::Pending);

  if (status == Status::Pending) {
    exec_context.pending_ios.insert({ io_id, hash.to_keyhash() });
    return Status::Pending;
  }

  assert(context.result == Status::Ok || context.result == Status::Aborted);
  // if FindOrCreateEntry, entry was found or created; either way it's a success
  // if TryUpdateEntry, entry might have changed (Aborted), or now updated to desired entry (Ok)
  status = (pending_context.index_op_type == IndexOperationType::Retrieve)
              ? status : context.result;
  pending_context.set_index_entry(context.entry, nullptr);
  pending_context.clear_index_op();
  return status;
}

template <class D, class HID>
void FasterIndex<D, HID>::AsyncEntryOperationDiskCallback(IAsyncContext* ctxt, Status result) {
  CallbackContext<FasterIndexContext<HID>> context{ ctxt };
  // Result here is wrt to FASTER operation (i.e., Read / Rmw) result
  assert(result == Status::Ok || result == Status::NotFound);
  if (result == Status::Ok) {
  assert(context->result == Status::Ok ||
        context->result == Status::NotFound ||
        context->result == Status::Aborted);
  } else {
    // Only in Reads: if no hash chunk exists, context->result was not set
    assert(context->result == Status::Corruption);
    context->result = Status::NotFound;
  }

#ifdef STATISTICS
  typedef FasterIndex<D, HID> faster_hash_index_t;
  faster_hash_index_t* hash_index = static_cast<faster_hash_index_t*>(context->hash_index);

  if (hash_index->collect_stats_ && context->result == Status::Ok) {
    switch(context->hash_index_op) {
      case HashIndexOp::FIND_ENTRY:
        ++(hash_index->find_entry_success_count_);
        break;
      case HashIndexOp::TRY_UPDATE_ENTRY:
        ++(hash_index->try_update_entry_success_count_);
        break;
      case HashIndexOp::FIND_OR_CREATE_ENTRY:
      case HashIndexOp::UPDATE_ENTRY:
      default:
        break;
    }
  }
#endif

  // FIXME: store this inside FasterIndexReadContext
  AsyncIndexIOContext index_io_context{ context->caller_context, context->thread_io_responses,
                                        context->io_id};
  index_io_context.entry = context->entry;
  // TODO: cross-validate result from here with internal context result
  index_io_context.result = context->result;

  if (context->op_type == OperationType::RMW) {
    index_io_context.record_address = static_cast<rmw_context_t*>(ctxt)->record_address();
    // NOTE: record address can be Address::kInvalidAddress if FindOrCreateEntry was called
  }

  IAsyncContext* index_io_context_copy;
  Status copy_status = index_io_context.DeepCopy(index_io_context_copy);
  assert(copy_status == Status::Ok);

  context->thread_io_responses->push(reinterpret_cast<AsyncIndexIOContext*>(index_io_context_copy));
}

template <class D, class HID>
template <class TC, class CC>
void FasterIndex<D, HID>::GarbageCollectSetup(Address new_begin_address, TC truncate_callback,
                                            CC complete_callback, IAsyncContext* callback_context) {
  uint64_t num_chunks = std::max(store_->hash_index_.size() / gc_state_t::kHashTableChunkSize, ((uint64_t)1));
  gc_state_->Initialize(new_begin_address, truncate_callback, complete_callback, callback_context, num_chunks);
}

template <class D, class HID>
template <class RC>
bool FasterIndex<D, HID>::GarbageCollect(RC* read_cache) {
  if (read_cache != nullptr) {
    throw std::runtime_error{ "FasterIndex should *not* store read-cached entries" };
  }

  uint64_t chunk = gc_state_->next_chunk++;
  if(chunk >= gc_state_->num_chunks) {
    return false;  // No chunk left to clean.
  }
  ++gc_state_->thread_count;

  log_debug("FasterIndex-GC: %lu/%lu [START...]", chunk + 1, gc_state_->num_chunks);
  log_debug("FasterIndex-GC: begin-address: %lu", gc_state_->new_begin_address.control());
  log_debug("FasterIndex-GC: global-min-address: %lu", gc_state_->min_address.load());

  uint8_t version = store_->hash_index_.resize_info.version;
  auto& current_table = store_->hash_index_.table_[version];

  uint64_t upper_bound;
  if(chunk + 1 < gc_state_->num_chunks) {
    // All chunks but the last chunk contain gc.kHashTableChunkSize elements.
    upper_bound = gc_state_t::kHashTableChunkSize;
  } else {
    // Last chunk might contain more or fewer elements.
    upper_bound = current_table.size() - (chunk * gc_state_t::kHashTableChunkSize);
  }

  // find smaller address in the hash table
  uint64_t min_address{ gc_state_->min_address.load() };
  for(uint64_t idx = 0; idx < upper_bound; ++idx) {
    hash_bucket_t* bucket = &current_table.bucket(chunk * gc_state_t::kHashTableChunkSize + idx);
    for(uint32_t entry_idx = 0; entry_idx < hash_bucket_t::kNumEntries; ++entry_idx) {
      AtomicHashBucketEntry& atomic_entry = bucket->entries[entry_idx];
      hash_bucket_entry_t expected_entry{ atomic_entry.load() };

      if (expected_entry.unused() ||
          expected_entry.address() == Address::kInvalidAddress ||
          min_address < expected_entry.address().control()) {
        continue;
      }
      min_address = expected_entry.address().control();
    }
  }
  log_debug("FasterIndex-GC: %lu/%lu [local_min_addr=%lu] [DONE!]",
            chunk + 1, gc_state_->num_chunks, min_address);

  // Update global min address if local min address is smaller
  uint64_t global_min_address = gc_state_->min_address.load();
  while (global_min_address > min_address) {
    gc_state_->min_address.compare_exchange_strong(global_min_address, min_address);
  }

  --gc_state_->thread_count;
  if (gc_state_->next_chunk.load() >= gc_state_->num_chunks && gc_state_->thread_count == 0) {
    // last thread after garbage collection is finished
    // truncate log -- no need to define/wait for callbacks
    Address begin_address = store_->hlog.begin_address.load();
    Address read_only_address = store_->hlog.read_only_address.load();

    Address new_begin_address = gc_state_->min_address.load();
    log_debug("FasterIndex-GC: Min address [%lu]", new_begin_address.control());
    assert(new_begin_address >= store_->hlog.begin_address.load());

    log_debug("FasterIndex-GC: [BA=%lu] [HA=%lu] [ROA=%lu]",
      begin_address.control(), store_->hlog.read_only_address.load().control(), read_only_address.control());

    // Truncate log, if possible
    if (new_begin_address > begin_address && new_begin_address < read_only_address) {
      log_debug("FasterIndex-GC: Truncating to [%lu]", new_begin_address.control());
      store_->ShiftBeginAddress(new_begin_address, nullptr, nullptr);
    } else if (new_begin_address >= read_only_address) {
      log_debug("FasterIndex-GC: Skipped truncation due to min-address being inside IPU region");
    } else {
      log_debug("FasterIndex-GC: Skipped truncation due to min-address being less-or-equal to begin_address");
    }
  }

  return true;
}

#ifdef STATISTICS
template <class D, class HID>
void FasterIndex<D, HID>::PrintStats() const {
  // FindEntry
  fprintf(stderr, "FindEntry Calls\t: %lu\n", find_entry_calls_.load());
  if (find_entry_calls_.load() > 0) {
    fprintf(stderr, "Status::Ok (%%)\t: %.2lf\n",
      (static_cast<double>(find_entry_success_count_.load()) / find_entry_calls_.load()) * 100.0);
  }
  // FindOrCreateEntry
  fprintf(stderr, "FindOrCreateEntry Calls\t: %lu\n", find_or_create_entry_calls_.load());
  // TryUpdateEntry
  fprintf(stderr, "TryUpdateEntry Calls\t: %lu\n", try_update_entry_calls_.load());
  if (try_update_entry_calls_.load() > 0) {
    fprintf(stderr, "Status::Ok (%%)\t: %.2lf\n",
      (static_cast<double>(try_update_entry_success_count_.load()) / try_update_entry_calls_.load()) * 100.0);
  }
  // UpdateEntry
  fprintf(stderr, "UpdateEntry Calls\t: %lu\n\n", update_entry_calls_.load());

  fprintf(stderr, "==== COLD-INDEX ====\n");
  store_->PrintStats();
}
#endif

}
} // namespace FASTER::index
