// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include "phase.h"
#include "value_control.h"

namespace FASTER {
namespace core {

struct ResizeInfo {
  uint8_t version;
};

/// Each FASTER store can perform only one action at a time (checkpoint, recovery, garbage
/// collect, or grow index).
enum class Action : uint8_t {
  None = 0,
  CheckpointFull,
  CheckpointIndex,
  CheckpointHybridLog,
  Recover,
  GC,
  GrowIndex
};

struct SystemStateLayout
{
  /// Action being performed (checkpoint, recover, or gc).
  Action action_;
  /// Phase of that action currently being executed.
  Phase phase_;
  /// otherwise control value of two SystemState object constructed from (action, phase, version) is undefined
  uint16_t always_zero_placeholder_ = 0;
  /// Checkpoint version (used for CPR).
  uint32_t version_;

  SystemStateLayout(Action action, Phase phase, uint32_t version) noexcept
	  : action_{action}
	  , phase_{phase}
	  , always_zero_placeholder_{}
	  , version_{version}
  {}
};

struct SystemState
  : public value_control<SystemStateLayout, uint64_t, 0>
{
  using value_control::value_control;

  SystemState(Action action, Phase phase, uint32_t version)
    : value_control{SystemStateLayout{action, phase, version}}
  {}

  Action action() const noexcept
  {
      return value().action_;
  }
  Phase phase() const noexcept
  {
      return value().phase_;
  }
  uint32_t version() const noexcept
  {
      return value().version_;
  }

  /// The state transitions.
  inline SystemState GetNextState() const
  {
    switch (action())
    {
    case Action::CheckpointFull:
      switch (phase())
      {
      case Phase::REST:
        return SystemState{ Action::CheckpointFull, Phase::PREP_INDEX_CHKPT, version() };
      case Phase::PREP_INDEX_CHKPT:
        return SystemState{ Action::CheckpointFull, Phase::INDEX_CHKPT, version() };
      case Phase::INDEX_CHKPT:
        return SystemState{ Action::CheckpointFull, Phase::PREPARE, version() };
      case Phase::PREPARE:
        return SystemState{ Action::CheckpointFull, Phase::IN_PROGRESS, version() + 1 };
      case Phase::IN_PROGRESS:
        return SystemState{ Action::CheckpointFull, Phase::WAIT_PENDING, version() };
      case Phase::WAIT_PENDING:
        return SystemState{ Action::CheckpointFull, Phase::WAIT_FLUSH, version() };
      case Phase::WAIT_FLUSH:
        return SystemState{ Action::CheckpointFull, Phase::PERSISTENCE_CALLBACK, version() };
      case Phase::PERSISTENCE_CALLBACK:
        return SystemState{ Action::CheckpointFull, Phase::REST, version() };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;
    case Action::CheckpointIndex:
      switch (phase())
      {
      case Phase::REST:
        return SystemState{ Action::CheckpointIndex, Phase::PREP_INDEX_CHKPT, version() };
      case Phase::PREP_INDEX_CHKPT:
        return SystemState{ Action::CheckpointIndex, Phase::INDEX_CHKPT, version() };
      case Phase::INDEX_CHKPT:
        return SystemState{ Action::CheckpointIndex, Phase::REST, version() };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;
    case Action::CheckpointHybridLog:
      switch (phase())
      {
      case Phase::REST:
        return SystemState{ Action::CheckpointHybridLog, Phase::PREPARE, version() };
      case Phase::PREPARE:
        return SystemState{ Action::CheckpointHybridLog, Phase::IN_PROGRESS, version() + 1 };
      case Phase::IN_PROGRESS:
        return SystemState{ Action::CheckpointHybridLog, Phase::WAIT_PENDING, version() };
      case Phase::WAIT_PENDING:
        return SystemState{ Action::CheckpointHybridLog, Phase::WAIT_FLUSH, version() };
      case Phase::WAIT_FLUSH:
        return SystemState{ Action::CheckpointHybridLog, Phase::PERSISTENCE_CALLBACK, version() };
      case Phase::PERSISTENCE_CALLBACK:
        return SystemState{ Action::CheckpointHybridLog, Phase::REST, version() };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;
    case Action::GC:
      switch (phase())
      {
      case Phase::REST:
        return SystemState{ Action::GC, Phase::GC_IO_PENDING, version() };
      case Phase::GC_IO_PENDING:
        return SystemState{ Action::GC, Phase::GC_IN_PROGRESS, version() };
      case Phase::GC_IN_PROGRESS:
        return SystemState{ Action::GC, Phase::REST, version() };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;
    case Action::GrowIndex:
      switch (phase())
      {
      case Phase::REST:
        return SystemState{ Action::GrowIndex, Phase::GROW_PREPARE, version() };
      case Phase::GROW_PREPARE:
        return SystemState{ Action::GrowIndex, Phase::GROW_IN_PROGRESS, version() };
      case Phase::GROW_IN_PROGRESS:
        return SystemState{ Action::GrowIndex, Phase::REST, version() };
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
};
static_assert(sizeof(SystemState) == 8, "sizeof(SystemState) != 8");

class AtomicSystemState
  : public value_atomic_control<SystemState, uint64_t, 0>
{
  public:
    AtomicSystemState() = delete; // is not used

    AtomicSystemState(Action action, Phase phase, uint32_t version)
    {
      store(SystemState{ action, phase, version });
    }

    /// Accessors.
    Phase phase() const noexcept
    {
      return load().value().phase_;
    }
    uint32_t version() const noexcept
    {
      return load().value().version_;
    }
};
static_assert(sizeof(AtomicSystemState) == 8, "sizeof(AtomicSystemState) != 8");

}
} // namespace FASTER::core
