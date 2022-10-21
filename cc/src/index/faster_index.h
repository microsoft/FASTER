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

template<class D, class HID = ColdLogHashIndexDefinition>
class FasterIndex : public IHashIndex<D> {
 public:
  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  typedef HID hash_index_definition_t;
  typedef typename HID::key_hash_t key_hash_t;
  typedef typename HID::hash_bucket_t hash_bucket_t;
  typedef typename HID::hash_bucket_entry_t hash_bucket_entry_t;

  typedef HashIndex<D, HID> hash_index_t;
  typedef FasterKv<HashIndexChunkKey, HashIndexChunkEntry, disk_t, hash_index_t, hash_index_t> store_t;

  typedef GcStateFasterIndex gc_state_t;
  typedef GrowState<hlog_t> grow_state_t;

  // 256k rows x 8 entries each = 2M entries
  // 16MB in-mem index (w/o overflow) [2M x 8 bytes each]
  const uint64_t kHashIndexNumEntries = 256 * 1024;
  // Stick to 768 MB for now -- can use remaining (~256 MB) for read cache.
  //const uint64_t kInMemSize = 768_MiB;
  const uint64_t kInMemSize = 16_GiB;
  // 90% of 768MB (~691 MB) constitute the mutable region
  const double dMutablePercentage = 0.75;

  FasterIndex(disk_t& disk, LightEpoch& epoch, gc_state_t& gc_state, grow_state_t& grow_state)
    : disk_{ disk }
    , epoch_{ epoch }
    , gc_state_{ &gc_state }
    , grow_state_{ &grow_state }
    , table_size_{ 0 }
    , serial_num_{ 0 }
    , index_root_path_{ disk.root_path } {

      if (index_root_path_.back() == FASTER::environment::kPathSeparator[0]) {
        index_root_path_.pop_back();
      }
      index_root_path_ += "index_";
      log_error("FasterIndex will be stored @ %s", index_root_path_.c_str());
  }

  void Initialize(uint64_t new_size) {
    this->resize_info.version = 0;

    assert(table_size_ == 0);
    table_size_ = new_size;

    if(table_size_ != kHashIndexNumEntries) {
      log_warn("FasterIndex # entries (%lu) is different than default (%lu)",
                table_size_, kHashIndexNumEntries);
    }
    store_ = std::make_unique<store_t>(table_size_, kInMemSize,
                                      index_root_path_, dMutablePercentage);
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
  void GarbageCollectSetup(Address new_begin_address,
                          GcState::truncate_callback_t truncate_callback,
                          GcState::complete_callback_t complete_callback);

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
  void CompletePending() {
    store_->CompletePending(false);
  }
  void Refresh() {
    store_->Refresh();
  }

  // Checkpointing methods
  Status Checkpoint(CheckpointState<file_t>& checkpoint) {
    throw std::runtime_error{ "Not Implemented!" };
  }
  Status CheckpointComplete() {
    throw std::runtime_error{ "Not Implemented!" };
  }
  Status WriteCheckpointMetadata(CheckpointState<file_t>& checkpoint) {
    throw std::runtime_error{ "Not Implemented!" };
  }

  // Recovery methods
  Status Recover(CheckpointState<file_t>& checkpoint) {
    throw std::runtime_error{ "Not Implemented!" };
  }
  Status RecoverComplete() {
    throw std::runtime_error{ "Not Implemented!" };
  }
  Status ReadCheckpointMetadata(const Guid& token, CheckpointState<file_t>& checkpoint) {
    throw std::runtime_error{ "Not Implemented!" };
  }

  uint64_t size() const {
    return store_->Size() + store_->hash_index_.size();
  }
  uint64_t new_size() const {
    return store_->Size() + store_->hash_index_.new_size();
  }
  constexpr static bool IsSync() {
    return false;
  }

 private:
  static void AsyncEntryOperationDiskCallback(IAsyncContext* ctxt, Status result);

  template <class C>
  Status ReadIndexEntry(ExecutionContext& exec_context, C& pending_context) const;

  template <class C>
  Status RmwIndexEntry(ExecutionContext& exec_context, C& pending_context,
                      Address new_address, bool force_update);


  inline void MarkRequestAsPending(ExecutionContext& exec_context, key_hash_t hash) {
    uint64_t io_id = exec_context.io_id++;
    exec_context.pending_ios.insert({ io_id, hash });
  }

  disk_t& disk_;
  LightEpoch& epoch_;
  gc_state_t* gc_state_;
  grow_state_t* grow_state_;

  uint64_t table_size_;

  std::unique_ptr<store_t> store_;
  std::atomic<uint64_t> serial_num_;
  std::string index_root_path_;
};

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::FindEntry(ExecutionContext& exec_context,
                                            C& pending_context) const {
  pending_context.index_op_type = IndexOperationType::Retrieve;
  return ReadIndexEntry(exec_context, pending_context);
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::ReadIndexEntry(ExecutionContext& exec_context, C& pending_context) const {
  key_hash_t hash{ pending_context.get_key_hash() };
  HashIndexChunkKey key{ hash.chunk_id(table_size_) };
  HashIndexChunkPos pos{ hash.index_in_chunk(), hash.tag_in_chunk() };

  // TODO: avoid incrementing io_id for every request if possible
  uint64_t io_id = exec_context.io_id++;
  FasterIndexReadContext context{ OperationType::Read, pending_context, io_id,
                                  &exec_context.index_io_responses, key, pos };

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
  //pending_context.entry = HashBucketEntry::kInvalidEntry;
  return RmwIndexEntry(exec_context, pending_context, Address::kInvalidAddress, false);
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::TryUpdateEntry(ExecutionContext& exec_context, C& pending_context,
                                                Address new_address, bool readcache) {
  pending_context.index_op_type = IndexOperationType::Update;
  return RmwIndexEntry(exec_context, pending_context, new_address, false);
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::UpdateEntry(ExecutionContext& exec_context, C& pending_context,
                                              Address new_address) {
  pending_context.index_op_type = IndexOperationType::Update;
  return RmwIndexEntry(exec_context, pending_context, new_address, true);
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::RmwIndexEntry(ExecutionContext& exec_context, C& pending_context,
                                                Address new_address, bool force_update) {
  key_hash_t hash{ pending_context.get_key_hash() };
  HashIndexChunkKey key{ hash.chunk_id(table_size_) };
  HashIndexChunkPos pos{ hash.index_in_chunk(), hash.tag_in_chunk() };

  // FIXME: avoid incrementing io_id for every request if possible
  uint64_t io_id = exec_context.io_id++;

  // TODO: check usefullness of tentative flag here
  hash_bucket_entry_t desired_entry{ new_address, pos.tag, false };
  //fprintf(stderr, "DESIRED = %llu\n", HashBucketEntry{desired_entry}.control_);
  FasterIndexRmwContext context{ OperationType::RMW, pending_context, io_id,
                                &exec_context.index_io_responses,
                                key, pos, force_update, pending_context.entry, desired_entry };

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
  CallbackContext<FasterIndexContext> context{ ctxt };
  // Result here is wrt to FASTER operation (i.e., Read / Rmw) result
  assert(result == Status::Ok || result == Status::NotFound);
  assert(context->result == Status::Ok ||
        context->result == Status::NotFound ||
        context->result == Status::Aborted);

  // FIXME: store this inside FasterIndexReadContext
  AsyncIndexIOContext index_io_context{ context->caller_context, context->thread_io_responses,
                                        context->io_id};
  index_io_context.entry = context->entry;
  // TODO: cross-validate result from here with internal context result
  index_io_context.result = context->result;

  if (context->op_type == OperationType::RMW) {
    index_io_context.record_address = static_cast<FasterIndexRmwContext*>(ctxt)->record_address();
    // NOTE: record address can be Address::kInvalidAddress if FindOrCreateEntry was called
  }

  IAsyncContext* index_io_context_copy;
  Status copy_status = index_io_context.DeepCopy(index_io_context_copy);
  assert(copy_status == Status::Ok);

  context->thread_io_responses->push(reinterpret_cast<AsyncIndexIOContext*>(index_io_context_copy));
}

template <class D, class HID>
void FasterIndex<D, HID>::GarbageCollectSetup(Address new_begin_address,
                                            GcState::truncate_callback_t truncate_callback,
                                            GcState::complete_callback_t complete_callback) {

  uint64_t num_chunks = std::max(store_->hash_index_.size() / gc_state_t::kHashTableChunkSize, (uint64_t)1);
  gc_state_->Initialize(new_begin_address, truncate_callback, complete_callback, num_chunks);
}

template <class D, class HID>
template <class RC>
bool FasterIndex<D, HID>::GarbageCollect(RC* read_cache) {
  if (read_cache != nullptr) {
    throw std::runtime_error{ "FasterIndex should *not* store read-cach'ed entries" };
  }

  uint64_t chunk = gc_state_->next_chunk++;
  if(chunk >= gc_state_->num_chunks) {
    // No chunk left to clean.
    return false;
  }
  ++gc_state_->thread_count;

  log_debug("FasterIndex-GC: %lu/%lu [START...]", chunk + 1, gc_state_->num_chunks);
  log_debug("FasterIndex-GC: begin-address: %lu", gc_state_->new_begin_address.control());

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

      if (!expected_entry.unused() && expected_entry.address() != Address::kInvalidAddress) {
        while (min_address > expected_entry.address().control()) {
          // try updating min address with smaller address
          if (gc_state_->min_address.compare_exchange_strong(min_address, expected_entry.address().control())) {
            break;
          }
          // retry with new min_address value
        }
      }
    }
  }

  log_debug("FasterIndex-GC: %lu/%lu [DONE!]", chunk + 1, gc_state_->num_chunks);

  --gc_state_->thread_count;
  if (gc_state_->next_chunk.load() >= gc_state_->num_chunks && gc_state_->thread_count == 0) {
    // last thread after garbage collection is finished
    // truncate log -- no need to define/wait for callbacks
    Address new_begin_address = gc_state_->min_address.load();
    log_debug("FasterIndex-GC: Min address [%lu]", new_begin_address.control());
    assert(new_begin_address >= store_->hlog.begin_address.load());
    if (new_begin_address > store_->hlog.begin_address.load()) {
      store_->ShiftBeginAddress(new_begin_address, nullptr, nullptr);
    }
  }

  return true;
}


}
} // namespace FASTER::index
