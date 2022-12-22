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

template<class D>
class IHashIndex {
 public:
  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  IHashIndex() { }
  ~IHashIndex() { }

  void Initialize(uint64_t new_size);
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
  bool GarbageCollect();

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
  Status Checkpoint(CheckpointState<file_t>& checkpoint);
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
};

}
} // namespace FASTER::index
