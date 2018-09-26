// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include "phase.h"

namespace FASTER {
namespace core {

struct ResizeInfo {
  uint8_t version;
};

/// Each FASTER store can perform only one action at a time (checkpoint, recovery, garbage
// collect, or grow index).
enum class Action : uint8_t {
  None = 0,
  CheckpointFull,
  CheckpointIndex,
  CheckpointHybridLog,
  Recover,
  GC,
  GrowIndex
};

struct SystemState {
  SystemState(Action action_, Phase phase_, uint32_t version_)
    : control_{ 0 } {
    action = action_;
    phase = phase_;
    version = version_;
  }
  SystemState(uint64_t control)
    : control_{ control } {
  }
  SystemState(const SystemState& other)
    : control_{ other.control_ } {
  }

  inline SystemState& operator=(const SystemState& other) {
    control_ = other.control_;
    return *this;
  }
  inline bool operator==(const SystemState& other) {
    return control_ == other.control_;
  }
  inline bool operator!=(const SystemState& other) {
    return control_ != other.control_;
  }

  /// The state transitions.
  inline SystemState GetNextState() const {
    switch(action) {
    case Action::CheckpointFull:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::CheckpointFull, Phase::PREP_INDEX_CHKPT, version };
      case Phase::PREP_INDEX_CHKPT:
        return SystemState{ Action::CheckpointFull, Phase::INDEX_CHKPT, version };
      case Phase::INDEX_CHKPT:
        return SystemState{ Action::CheckpointFull, Phase::PREPARE, version };
      case Phase::PREPARE:
        return SystemState{ Action::CheckpointFull, Phase::IN_PROGRESS, version + 1 };
      case Phase::IN_PROGRESS:
        return SystemState{ Action::CheckpointFull, Phase::WAIT_PENDING, version };
      case Phase::WAIT_PENDING:
        return SystemState{ Action::CheckpointFull, Phase::WAIT_FLUSH, version };
      case Phase::WAIT_FLUSH:
        return SystemState{ Action::CheckpointFull, Phase::PERSISTENCE_CALLBACK, version };
      case Phase::PERSISTENCE_CALLBACK:
        return SystemState{ Action::CheckpointFull, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;
    case Action::CheckpointIndex:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::CheckpointIndex, Phase::PREP_INDEX_CHKPT, version };
      case Phase::PREP_INDEX_CHKPT:
        return SystemState{ Action::CheckpointIndex, Phase::INDEX_CHKPT, version };
      case Phase::INDEX_CHKPT:
        return SystemState{ Action::CheckpointIndex, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;
    case Action::CheckpointHybridLog:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::CheckpointHybridLog, Phase::PREPARE, version };
      case Phase::PREPARE:
        return SystemState{ Action::CheckpointHybridLog, Phase::IN_PROGRESS, version + 1 };
      case Phase::IN_PROGRESS:
        return SystemState{ Action::CheckpointHybridLog, Phase::WAIT_PENDING, version };
      case Phase::WAIT_PENDING:
        return SystemState{ Action::CheckpointHybridLog, Phase::WAIT_FLUSH, version };
      case Phase::WAIT_FLUSH:
        return SystemState{ Action::CheckpointHybridLog, Phase::PERSISTENCE_CALLBACK, version };
      case Phase::PERSISTENCE_CALLBACK:
        return SystemState{ Action::CheckpointHybridLog, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;
    case Action::GC:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::GC, Phase::GC_IO_PENDING, version };
      case Phase::GC_IO_PENDING:
        return SystemState{ Action::GC, Phase::GC_IN_PROGRESS, version };
      case Phase::GC_IN_PROGRESS:
        return SystemState{ Action::GC, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;
    case Action::GrowIndex:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::GrowIndex, Phase::GROW_PREPARE, version };
      case Phase::GROW_PREPARE:
        return SystemState{ Action::GrowIndex, Phase::GROW_IN_PROGRESS, version };
      case Phase::GROW_IN_PROGRESS:
        return SystemState{ Action::GrowIndex, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
    default:
      // not reached
      assert(false);
      return SystemState(UINT64_MAX);
    }
  }

  union {
      struct {
        /// Action being performed (checkpoint, recover, or gc).
        Action action;
        /// Phase of that action currently being executed.
        Phase phase;
        /// Checkpoint version (used for CPR).
        uint32_t version;
      };
      uint64_t control_;
    };
};
static_assert(sizeof(SystemState) == 8, "sizeof(SystemState) != 8");

class AtomicSystemState {
 public:
  AtomicSystemState(Action action_, Phase phase_, uint32_t version_) {
    SystemState state{ action_, phase_, version_ };
    control_.store(state.control_);
  }

  /// Atomic access.
  inline SystemState load() const {
    return SystemState{ control_.load() };
  }
  inline void store(SystemState value) {
    control_.store(value.control_);
  }
  inline bool compare_exchange_strong(SystemState& expected, SystemState desired) {
    uint64_t expected_control = expected.control_;
    bool result = control_.compare_exchange_strong(expected_control, desired.control_);
    expected = SystemState{ expected_control };
    return result;
  }

  /// Accessors.
  inline Phase phase() const {
    return load().phase;
  }
  inline uint32_t version() const {
    return load().version;
  }

 private:
  /// Atomic access to the system state.
  std::atomic<uint64_t> control_;
};

}
} // namespace FASTER::core
