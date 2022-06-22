#pragma once

#include <experimental/filesystem>

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
  template<class K, class V, class D, class H>
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

  typedef typename HID::key_hash_t key_hash_t;
  typedef typename HID::hash_bucket_entry_t hash_bucket_entry_t;

  typedef HashIndex<D, HID> hash_index_t;
  typedef FasterKv<HashIndexChunkKey, HashIndexChunkEntry, disk_t, hash_index_t> store_t;

  typedef GcStateWithIndex gc_state_t;
  typedef GrowState<hlog_t> grow_state_t;

  // 256k rows x 8 entries each = 2M entries
  // 16MB in-mem index (w/o overflow) [2M x 8 bytes each]
  int kHashIndexNumEntries = 2048 * (1 << 10);

  FasterIndex(disk_t& disk, LightEpoch& epoch, gc_state_t& gc_state, grow_state_t& grow_state)
    : disk_{ disk }
    , epoch_{ epoch }
    , gc_state_{ &gc_state }
    , grow_state_{ &grow_state }
    , serial_num_{ 0 } {

      std::string& root_path = disk.root_path;
      if (!root_path.empty()) {
        root_path += "cold_index";
        assert(std::experimental::filesystem::create_directories(root_path));
      }
      store_ = std::make_unique<store_t>(kHashIndexNumEntries, 192 * (1 << 20), root_path, 0.4);
  }

  void Initialize(uint64_t new_size) {
    this->resize_info.version = 0;
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
  Status TryUpdateEntry(ExecutionContext& exec_context, C& pending_context, Address new_address);

  template <class C>
  Status UpdateEntry(ExecutionContext& exec_context, C& pending_context, Address new_address);

  // Garbage Collect methods
  void GarbageCollectSetup(Address new_begin_address,
                          GcState::truncate_callback_t truncate_callback,
                          GcState::complete_callback_t complete_callback) {
    throw std::runtime_error{ "Not Implemented!" };
  }
  bool GarbageCollect() {
    throw std::runtime_error{ "Not Implemented!" };
  }

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
  bool IsSync() const {
    return false;
  }

 private:
  static void AsyncEntryOperationDiskCallback(IAsyncContext* ctxt, Status result);

  template <class C>
  Status ReadIndexEntry(ExecutionContext& exec_context, C& pending_context, bool create_entry) const;

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

  std::unique_ptr<store_t> store_;
  std::atomic<uint64_t> serial_num_;
};

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::FindEntry(ExecutionContext& exec_context,
                                            C& pending_context) const {
  return ReadIndexEntry(exec_context, pending_context, false);
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::FindOrCreateEntry(ExecutionContext& exec_context, C& pending_context) {
  // FIXME: replace read with RMW
  Status status = ReadIndexEntry(exec_context, pending_context, true);
  return (status == Status::NotFound) ? Status::Ok : status;
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::ReadIndexEntry(ExecutionContext& exec_context, C& pending_context,
                                                bool create_entry) const {
  pending_context.index_op_type = IndexOperationType::Retrieve;

  key_hash_t hash{ pending_context.get_key_hash() };
  HashIndexChunkKey key{ hash.chunk_id(kHashIndexNumEntries) };
  HashIndexChunkPos pos{ hash.index_in_chunk(), hash.tag_in_chunk() };

  // TODO: avoid incrementing io_id for every request if possible
  uint64_t io_id = exec_context.io_id++;
  FasterIndexReadContext context{ OperationType::Read, pending_context, io_id,
                                  &exec_context.index_io_responses,
                                  key, pos, create_entry };

  // TODO: replace serial_num with thread-local (possibly inside )
  // FIXME: replace nullptr with appropriate callback
  Status status = store_->Read(context, AsyncEntryOperationDiskCallback, 0);
  if (status != Status::Pending) {
    pending_context.index_op_result = context.result;
    pending_context.set_index_entry(context.entry, nullptr);
    return status;
  }
  // Request went pending
  exec_context.pending_ios.insert({ io_id, hash.to_keyhash() });
  return Status::Pending;
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::TryUpdateEntry(ExecutionContext& exec_context, C& pending_context,
                                                Address new_address) {
  return RmwIndexEntry(exec_context, pending_context, new_address, false);
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::UpdateEntry(ExecutionContext& exec_context, C& pending_context,
                                              Address new_address) {
  return RmwIndexEntry(exec_context, pending_context, new_address, true);
}

template <class D, class HID>
template <class C>
inline Status FasterIndex<D, HID>::RmwIndexEntry(ExecutionContext& exec_context, C& pending_context,
                                                Address new_address, bool force_update) {
  pending_context.index_op_type = IndexOperationType::Update;

  key_hash_t hash{ pending_context.get_key_hash() };
  HashIndexChunkKey key{ hash.chunk_id(kHashIndexNumEntries) };
  //fprintf(stderr, "%llu\n", hash.chunk_id(kHashIndexNumEntries));
  HashIndexChunkPos pos{ hash.index_in_chunk(), hash.tag_in_chunk() };

  // TODO: avoid incrementing io_id for every request if possible
  uint64_t io_id = exec_context.io_id++;

  // TODO: check usefullness of tentative flag here
  hash_bucket_entry_t desired_entry{ new_address, pos.tag, false };
  /*if (new_address == HashBucketEntry::kInvalidEntry) {
    desired_entry = HashBucketEntry::kInvalidEntry; // Invalidate hash bucket entry
  }*/
  FasterIndexRmwContext context{ OperationType::RMW, pending_context, io_id,
                                &exec_context.index_io_responses,
                                key, pos, force_update, pending_context.entry, desired_entry };

  Status status = store_->Rmw(context, AsyncEntryOperationDiskCallback, ++serial_num_);
  if (status != Status::Pending) {
    pending_context.index_op_result = context.result;
    //pending_context.set_index_entry(context.entry, hash.control());
    pending_context.set_index_entry(context.entry, nullptr);
    return status;
  }
  // Request went pending
  exec_context.pending_ios.insert({ io_id, hash.to_keyhash() });
  return Status::Pending;
}

template <class D, class HID>
void FasterIndex<D, HID>::AsyncEntryOperationDiskCallback(IAsyncContext* ctxt, Status result) {
  CallbackContext<FasterIndexContext> context{ ctxt };
  // Result here is wrt to FASTER operation (i.e., Read / Rmw) result
  assert(result == Status::Ok || result == Status::NotFound);

  // FIXME: store this inside FasterIndexReadContext
  AsyncIndexIOContext index_io_context{ context->caller_context, context->thread_io_responses,
                                        context->io_id};

  index_io_context.entry = context->entry;
  // TODO: cross-validate result from here with internal context result
  index_io_context.result = context->result;
  if (context->op_type == OperationType::RMW) {
    index_io_context.record_address = static_cast<FasterIndexRmwContext*>(ctxt)->record_address();
    assert(index_io_context.record_address != Address::kInvalidAddress);
  }

  IAsyncContext* index_io_context_copy;
  assert(index_io_context.DeepCopy(index_io_context_copy) == Status::Ok);

  context->thread_io_responses->push(reinterpret_cast<AsyncIndexIOContext*>(index_io_context_copy));
}

}
} // namespace FASTER::index
