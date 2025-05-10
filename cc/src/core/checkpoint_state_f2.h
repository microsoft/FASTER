// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <unordered_map>

#include "checkpoint_state.h"
#include "guid.h"
#include "thread.h"

namespace FASTER {
namespace core {

enum StoreCheckpointStatus : uint8_t {
  IDLE = 0,
  REQUESTED,
  ACTIVE,
  FINISHED,
  FAILED,
};
static const char* STORE_CHECKPOINT_STATUS_STR[] = { "Idle", "Requested", "Active", "Finished" };

enum CheckpointPhase : uint8_t {
  REST = 0,
  HOT_STORE_CHECKPOINT,
  COLD_STORE_CHECKPOINT,
  RECOVER,
};
static const char* CHECKPOINT_PHASE_STR[] = { "REST", "HOT_STORE_CHECKPOINT", "COLD_STORE_CHECKPOINT" };

class HotColdCheckpointState {
 public:
  typedef std::chrono::time_point<std::chrono::system_clock> time_point_t;

  HotColdCheckpointState()
    : phase{ CheckpointPhase::REST }
    , store_persistence_callback{ nullptr }
    , lazy_checkpoint_expired{ time_point_t::min() }
    , hot_store_status{ StoreCheckpointStatus::IDLE }
    , hot_store_checkpoint_result{ Status::Corruption }
    , cold_store_status{ StoreCheckpointStatus::IDLE }
    , threads_pending_hot_store_persist{ 0 }
    , threads_pending_issue_callback{ 0 } {
      token.Clear();
  }

  void Initialize(HybridLogPersistenceCallback store_persistence_callback_,
                  Guid token_, time_point_t lazy_checkpoint_expired_) {
    store_persistence_callback = store_persistence_callback_;
    token = token_;
    lazy_checkpoint_expired = lazy_checkpoint_expired_;

    hot_store_status.store(StoreCheckpointStatus::IDLE);
    hot_store_checkpoint_result.store(Status::Corruption);
    cold_store_status.store(StoreCheckpointStatus::IDLE);

    for (size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
      persistent_serial_nums[idx].store(0);
    }
  }

  void SetNumActiveThreads(uint32_t active_sessions) {
    threads_pending_hot_store_persist.store(active_sessions);
    threads_pending_issue_callback.store(active_sessions);
  }

  bool IsLazyCheckpointExpired() {
    return lazy_checkpoint_expired <= time_point_t::clock::now();
  }

  void Reset() {
    store_persistence_callback = nullptr;
    token.Clear();
    lazy_checkpoint_expired = time_point_t::min();

    hot_store_status.store(StoreCheckpointStatus::IDLE);
    hot_store_checkpoint_result.store(Status::Corruption);
    cold_store_status.store(StoreCheckpointStatus::IDLE);

    threads_pending_hot_store_persist.store(0);
    threads_pending_issue_callback.store(0);
    for (size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
      persistent_serial_nums[idx].store(0);
    }
    phase.store(CheckpointPhase::REST);
  }

  std::atomic<CheckpointPhase> phase;

  //
  HybridLogPersistenceCallback store_persistence_callback;
  Guid token;
  time_point_t lazy_checkpoint_expired;

  std::atomic<StoreCheckpointStatus> hot_store_status;
  std::atomic<Status> hot_store_checkpoint_result;

  std::atomic<StoreCheckpointStatus> cold_store_status;

  // metadata used for hot-store checkpointing
  std::atomic<uint32_t> threads_pending_hot_store_persist;
  std::atomic<uint32_t> threads_pending_issue_callback;
  std::atomic<uint64_t> persistent_serial_nums[Thread::kMaxNumThreads];  // thread_id -> persistent_serial_num
};

}
} // namespace FASTER::core