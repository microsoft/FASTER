// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include "core/faster.h"
#include "core/utility.h"
#include "device/file_system_disk.h"

using namespace FASTER::core;

namespace sum_store {

// Sum store's key type.
class AdId {
 public:
  AdId(uint64_t key)
    : key_{ key } {
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(AdId));
  }
  inline KeyHash GetHash() const {
    return KeyHash{ Utility::GetHashCode(key_) };
  }

  /// Comparison operators.
  inline bool operator==(const AdId& other) const {
    return key_ == other.key_;
  }
  inline bool operator!=(const AdId& other) const {
    return key_ != other.key_;
  }

 private:
  uint64_t key_;
};
static_assert(sizeof(AdId) == 8, "sizeof(AdId) != 8)");

// Sum store's value type.
class NumClicks {
 public:
  NumClicks()
    : num_clicks{ 0 } {
  }
  NumClicks(const NumClicks& other)
    : num_clicks{ other.num_clicks } {
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(NumClicks));
  }

  union {
      uint64_t num_clicks;
      std::atomic<uint64_t> atomic_num_clicks;
    };
};

/// Key is an 8-byte advertising ID.
typedef AdId key_t;

/// Value is an 8-byte count of clicks.
typedef NumClicks value_t;

/// Context to update the sum store (via read-modify-write).
class RmwContext : public IAsyncContext {
 public:
  typedef sum_store::key_t key_t;
  typedef sum_store::value_t value_t;

  RmwContext(const AdId& key, uint64_t increment)
    : key_{ key }
    , increment_{ increment } {
  }

  /// Copy (and deep-copy) constructor.
  RmwContext(const RmwContext& other)
    : key_{ other.key_ }
    , increment_{ other.increment_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const AdId& key() const {
    return key_;
  }

  inline void RmwInitial(NumClicks& value) {
    value.num_clicks = increment_;
  }
  inline void RmwCopy(const NumClicks& old_value, NumClicks& value) {
    value.num_clicks = old_value.num_clicks + increment_;
  }
  inline bool RmwAtomic(NumClicks& value) {
    value.atomic_num_clicks.fetch_add(increment_);
    return true;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  inline static constexpr uint32_t value_size(const NumClicks& old_value) {
    return sizeof(value_t);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  AdId key_;
  uint64_t increment_;
};

/// Context to read the store (after recovery).
class ReadContext : public IAsyncContext {
 public:
  typedef sum_store::key_t key_t;
  typedef sum_store::value_t value_t;

  ReadContext(const AdId& key, uint64_t* result)
    : key_{ key }
    , result_{ result } {
  }

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ }
    , result_{ other.result_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const AdId& key() const {
    return key_;
  }

  inline void Get(const value_t& value) {
    *result_ = value.num_clicks;
  }
  inline void GetAtomic(const value_t& value) {
    *result_ = value.atomic_num_clicks;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  AdId key_;
  uint64_t* result_;
};

typedef FasterKv<key_t, value_t, FASTER::device::FileSystemDisk<
FASTER::environment::QueueIoHandler, 1073741824L>> store_t;

} // namespace sum_store
