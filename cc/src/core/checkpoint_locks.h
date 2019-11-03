// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <thread>

#include "alloc.h"
#include "constants.h"
#include "key_hash.h"
#include "value_control.h"

namespace FASTER {
namespace core {

struct CheckpointLockLayout
{
  uint32_t old_lock_count_;
  uint32_t new_lock_count_;
};

struct CheckpointLock
  : public value_control<CheckpointLockLayout, uint64_t, 0>
{
  using value_control::value_control;
  
  //CheckpointLock() = default;

  CheckpointLock(uint32_t old_lock_count, uint32_t new_lock_count) noexcept
    : value_control{CheckpointLockLayout{old_lock_count, new_lock_count}}
  {}

  uint32_t oldLockCount() const noexcept { return value().old_lock_count_; }
  uint32_t newLockCount() const noexcept { return value().new_lock_count_; }
};
static_assert(sizeof(CheckpointLock) == 8, "sizeof(CheckpointLock) != 8");

class AtomicCheckpointLock
  : public value_atomic_control<CheckpointLock, uint64_t, 0>
{
 public:
   using value_atomic_control::value_atomic_control;
   
   //AtomicCheckpointLock() = default;

  /// Try to lock the old version of a record.
  bool try_lock_old() noexcept
  {
    CheckpointLock expected{ load() };
    while (expected.newLockCount() == 0)
    {
      CheckpointLock desired{ expected.oldLockCount() + 1, 0 };
      if (compare_exchange_strong(expected, desired))
      {
        return true;
      }
    }
    return false;
  }
  void unlock_old() noexcept
  {
    value_.control_ -= CheckpointLock{ 1, 0 }.control(); // TODO: is it actualliy faster than --value_.value_control_.old_lock_count_;?
  }

  /// Try to lock the new version of a record.
  bool try_lock_new() noexcept
  {
    CheckpointLock expected{ value_.control_.load() };
    while (expected.oldLockCount() == 0)
    {
      if (compare_exchange_strong(expected, CheckpointLock{ 0, expected.newLockCount() + 1 }))
      {
        return true;
      }
    }
    return false;
  }
  void unlock_new() noexcept
  {
      value_.control_ -= CheckpointLock{ 0, 1 }.control();
  }

  bool old_locked() const noexcept
  {
    CheckpointLock result{ value_.control_ };
    return result.oldLockCount() > 0;
  }
  bool new_locked() const noexcept
  {
    CheckpointLock result{ value_.control_ };
    return result.newLockCount() > 0;
  }
};
static_assert(sizeof(AtomicCheckpointLock) == 8, "sizeof(AtomicCheckpointLock) != 8");

class CheckpointLocks {
 public:
  CheckpointLocks()
    : size_{ 0 }
    , locks_{ nullptr } {
  }

  ~CheckpointLocks() {
    if(locks_) {
      aligned_free(locks_);
    }
  }

  void Initialize(uint64_t size) {
    assert(size < INT32_MAX);
    assert(Utility::IsPowerOfTwo(size));
    if(locks_) {
      aligned_free(locks_);
    }
    size_ = size;
    locks_ = reinterpret_cast<AtomicCheckpointLock*>(aligned_alloc(Constants::kCacheLineBytes,
             size_ * sizeof(AtomicCheckpointLock)));
    std::memset(locks_, 0, size_ * sizeof(AtomicCheckpointLock));
  }

  void Free() {
    assert(locks_);
#ifdef _DEBUG
    for(uint64_t idx = 0; idx < size_; ++idx) {
      assert(!locks_[idx].old_locked());
      assert(!locks_[idx].new_locked());
    }
#endif
    aligned_free(locks_);
    size_ = 0;
    locks_ = nullptr;
  }

  inline uint64_t size() const {
    return size_;
  }

  inline AtomicCheckpointLock& get_lock(KeyHash hash) {
    return locks_[hash.idx(size_)];
  }

 private:
  uint64_t size_;
  AtomicCheckpointLock* locks_;
};

class CheckpointLockGuard {
 public:
  CheckpointLockGuard(CheckpointLocks& locks, KeyHash hash)
    : lock_{ nullptr }
    , locked_old_{ false }
    , locked_new_{ false } {
    if(locks.size() > 0) {
      lock_ = &locks.get_lock(hash);
    }
  }
  ~CheckpointLockGuard() {
    if(lock_) {
      if(locked_old_) {
        lock_->unlock_old();
      }
      if(locked_new_) {
        lock_->unlock_new();
      }
    }
  }
  inline bool try_lock_old() {
    assert(lock_);
    assert(!locked_old_);
    locked_old_ = lock_->try_lock_old();
    return locked_old_;
  }
  inline bool try_lock_new() {
    assert(lock_);
    assert(!locked_new_);
    locked_new_ = lock_->try_lock_new();
    return locked_new_;
  }

  inline bool old_locked() const {
    assert(lock_);
    return lock_->old_locked();
  }
  inline bool new_locked() const {
    assert(lock_);
    return lock_->new_locked();
  }

 private:
  AtomicCheckpointLock* lock_;
  bool locked_old_;
  bool locked_new_;
};

}
} // namespace FASTER::core
