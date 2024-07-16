#pragma once

#include "hash_bucket.h"
#include "hash_table.h"

#include "../core/checkpoint_state.h"
#include "../core/internal_contexts.h"
#include "../core/light_epoch.h"
#include "../core/persistent_memory_malloc.h"

using namespace FASTER::core;

namespace FASTER {
namespace index {

typedef void(*RefreshCallback)(void* faster);

// Checkpointing and recovery context
class IndexCheckpointMetadataIoContext : public IAsyncContext {
  public:
  IndexCheckpointMetadataIoContext(Status* result_, std::atomic<bool>* completed_)
    : result{ result_ }
    , completed{ completed_ } {
  }
  /// The deep-copy constructor
  IndexCheckpointMetadataIoContext(IndexCheckpointMetadataIoContext& other)
    : result{ other.result }
    , completed{ other.completed } {
  }
  protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }
  public:
  Status* result;
  std::atomic<bool>* completed;
};

template<class D>
class IHashIndex {
 public:
  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  IHashIndex(const std::string& root_path, disk_t& disk, LightEpoch& epoch)
    : root_path_{ FASTER::environment::NormalizePath(root_path) }
    , disk_{ disk }
    , epoch_{ epoch } {
  }

  void SetRefreshCallback(void* faster, RefreshCallback callback);

  template <class C>
  Status FindEntry(ExecutionContext& exec_context, C& pending_context) const;

  template <class C>
  Status FindOrCreateEntry(ExecutionContext& exec_context, C& pending_context);

  template <class C>
  Status TryUpdateEntry(ExecutionContext& context, C& pending_context, Address new_address);

  template <class C>
  Status UpdateEntry(ExecutionContext& context, C& pending_context, Address new_address);

  // Garbage Collect methods
  void GarbageCollectSetup(Address new_begin_address,
                          GcState::truncate_callback_t truncate_callback,
                          GcState::complete_callback_t complete_callback);

  template<class RC>
  bool GarbageCollect(RC* read_cache);

  // Index grow methods
  void GrowSetup(GrowCompleteCallback callback);

  template<class R>
  void Grow();

  // Helper methods to support the FASTER-like interface
  // used in the FASTER index (i.e., cold index).
  void StartSession();
  void StopSession();
  bool CompletePending();
  void Refresh();

  // Checkpointing methods
  template <class RC>
  Status Checkpoint(CheckpointState<file_t>& checkpoint, const RC* read_cache);

  Status CheckpointComplete();
  Status WriteCheckpointMetadata(CheckpointState<file_t>& checkpoint);

  // Recovery methods
  Status Recover(CheckpointState<file_t>& checkpoint);
  Status RecoverComplete();
  Status ReadCheckpointMetadata(const Guid& token, CheckpointState<file_t>& checkpoint);

  // Helper functions
  uint64_t size() const;
  uint64_t new_size() const;
  constexpr static bool IsSync();

 public:
#ifdef STATISTICS
  void PrintStats() const;
#endif

  ResizeInfo resize_info;

 protected:
  std::string root_path_;
  disk_t& disk_;
  LightEpoch& epoch_;
};

template<class D>
Status IHashIndex<D>::ReadCheckpointMetadata(const Guid& token, CheckpointState<file_t>& checkpoint) {
  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<IndexCheckpointMetadataIoContext> context{ ctxt };
    // callback is called only once
    assert(*(context->result) == Status::Corruption && context->completed->load() == false);
    // mark request completed
    *(context->result) = result;
    context->completed->store(true);
  };

  Status read_result{ Status::Corruption };
  std::atomic<bool> read_completed{ false };
  IndexCheckpointMetadataIoContext context{ &read_result, &read_completed };

  // Read from file
  auto filepath = disk_.relative_index_checkpoint_path(token) + "info.dat";
  file_t file = disk_.NewFile(filepath);
  RETURN_NOT_OK(file.Open(&disk_.handler()));

  // Create aligned buffer used in read async
  uint32_t read_size = sizeof(checkpoint.index_metadata);
  assert(read_size <= file.alignment());      // less than file alignment
  read_size += file.alignment() - read_size;  // pad to file alignment
  assert(read_size % file.alignment() == 0);
  uint8_t* buffer = reinterpret_cast<uint8_t*>(core::aligned_alloc(file.alignment(), read_size));
  memset(buffer, 0, read_size);
  // Write to file
  RETURN_NOT_OK(file.ReadAsync(0, buffer, read_size, callback, context));

  // Wait until disk I/O completes
  while(!read_completed) {
    disk_.TryComplete();
    std::this_thread::yield();
  }
  // Copy from buffer to struct
  memcpy(&checkpoint.index_metadata, buffer, sizeof(checkpoint.index_metadata));
  core::aligned_free(reinterpret_cast<uint8_t*>(buffer));

  return read_result;
}

template<class D>
Status IHashIndex<D>::WriteCheckpointMetadata(CheckpointState<file_t>& checkpoint) {
  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<IndexCheckpointMetadataIoContext> context{ ctxt };
    // callback is called only once
    assert(*(context->result) == Status::Corruption && context->completed->load() == false);
    // mark request completed
    *(context->result) = result;
    context->completed->store(true);
  };

  Status write_result{ Status::Corruption };
  std::atomic<bool> write_completed{ false };
  IndexCheckpointMetadataIoContext context{ &write_result, &write_completed };

  // Create file
  auto filepath = disk_.relative_index_checkpoint_path(checkpoint.index_token) + "info.dat";
  file_t file = disk_.NewFile(filepath);
  RETURN_NOT_OK(file.Open(&disk_.handler()));

  // Create aligned buffer used in write async
  uint32_t write_size = sizeof(checkpoint.index_metadata);
  assert(write_size <= file.alignment());       // less than file alignment
  write_size += file.alignment() - write_size;  // pad to file alignment
  assert(write_size % file.alignment() == 0);
  uint8_t* buffer = reinterpret_cast<uint8_t*>(core::aligned_alloc(file.alignment(), write_size));
  memset(buffer, 0, write_size);
  memcpy(buffer, &checkpoint.index_metadata, sizeof(checkpoint.index_metadata));
  // Write to file
  RETURN_NOT_OK(file.WriteAsync(buffer, 0, write_size, callback, context));

  // Wait until disk I/O completes
  while(!write_completed) {
    disk_.TryComplete();
    std::this_thread::yield();
  }
  core::aligned_free(reinterpret_cast<uint8_t*>(buffer));
  return write_result;
}

}
} // namespace FASTER::index
