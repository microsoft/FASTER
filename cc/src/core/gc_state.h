// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <limits>

#include "address.h"
#include "async.h"
#include "utility.h"

namespace FASTER {
namespace core {

/// State of the active garbage-collection call.
class GcState {
 public:
  // Used when no callback context was provided (i.e., ShiftBeginAddress())
  typedef void(*truncate_callback_t)(uint64_t offset);
  typedef void(*complete_callback_t)(void);
  // Used when callback context was provided (i.e., InternalShiftBeginAddress())
  typedef void(*internal_truncate_callback_t)(IAsyncContext* context, uint64_t offset);
  typedef void(*internal_complete_callback_t)(IAsyncContext* context);

  GcState()
    : new_begin_address{ 0 }
    , truncate_callback{ nullptr }
    , complete_callback{ nullptr }
    , callback_context{ nullptr } {
  }

  template <class TC, class CC>
  void Initialize(Address new_begin_address_, TC truncate_callback_,
                  CC complete_callback_, IAsyncContext* callback_context_ = nullptr) {
    // static check of callback supported types
    static_assert((std::is_same<TC, truncate_callback_t>::value && std::is_same<CC, complete_callback_t>::value) ||
                  (std::is_same<TC, internal_truncate_callback_t>::value && std::is_same<CC, internal_complete_callback_t>::value));

    Initialize(new_begin_address_,
               reinterpret_cast<func_ptr_t>(truncate_callback_),
               reinterpret_cast<func_ptr_t>(complete_callback_),
               callback_context_);
  }

  void IssueTruncateCallback(uint64_t new_begin_offset) const {
    if (!truncate_callback) {
      return;
    }
    if (callback_context) {
      const auto& callback = reinterpret_cast<internal_truncate_callback_t>(truncate_callback);
      callback(callback_context, new_begin_offset);
    } else {
      const auto& callback = reinterpret_cast<truncate_callback_t>(truncate_callback);
      callback(new_begin_offset);
    }
  }

  void IssueCompleteCallback() const {
    if (!complete_callback) {
      return;
    }
    if (callback_context) {
      const auto& callback = reinterpret_cast<internal_complete_callback_t>(complete_callback);
      callback(callback_context);
    } else {
      const auto& callback = reinterpret_cast<complete_callback_t>(complete_callback);
      callback();
    }
  }

 protected:
  void Initialize(Address new_begin_address_, func_ptr_t truncate_callback_,
                  func_ptr_t complete_callback_, IAsyncContext* callback_context_) {
    new_begin_address = new_begin_address_;
    truncate_callback = truncate_callback_;
    complete_callback = complete_callback_;
    callback_context = callback_context_;
  }

 public:
  Address new_begin_address;
  func_ptr_t truncate_callback;
  func_ptr_t complete_callback;
  IAsyncContext* callback_context;
};


class GcStateInMemIndex: public GcState {
 public:
  static constexpr uint64_t kHashTableChunkSize = 16384;

  GcStateInMemIndex()
    : GcState()
    , num_chunks{ 0 }
    , next_chunk{ 0 } {
  }

  template <class T_CB, class C_CB>
  void Initialize(Address new_begin_address_, T_CB truncate_callback_,
                  C_CB complete_callback_, IAsyncContext* callback_context_, uint64_t num_chunks_) {
    GcState::Initialize(new_begin_address_, truncate_callback_, complete_callback_, callback_context_);
    num_chunks = num_chunks_;
    next_chunk = 0;
  }

  uint64_t num_chunks;
  std::atomic<uint64_t> next_chunk;
};

class GcStateColdIndex: public GcStateInMemIndex {
 public:
  static constexpr uint64_t kHashTableChunkSize = GcStateInMemIndex::kHashTableChunkSize;

  GcStateColdIndex()
    : GcStateInMemIndex()
    , min_address{ std::numeric_limits<uint64_t>::max() }
    , thread_count{ 0 } {
  }

  template <class T_CB, class C_CB>
  void Initialize(Address new_begin_address_, T_CB truncate_callback_,
                  C_CB complete_callback_, IAsyncContext* callback_context_, uint64_t num_chunks_) {
    GcStateInMemIndex::Initialize(new_begin_address_, truncate_callback_, complete_callback_, callback_context_, num_chunks_);
    min_address = std::numeric_limits<uint64_t>::max();
    thread_count = 0;
  }

  std::atomic<uint64_t> min_address;
  std::atomic<uint16_t> thread_count;
};


}
} // namespace FASTER::core
