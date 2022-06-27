// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <limits>

#include "address.h"

namespace FASTER {
namespace core {

/// State of the active garbage-collection call.
class GcState {
 public:
  typedef void(*truncate_callback_t)(uint64_t offset);
  typedef void(*complete_callback_t)(void);

  GcState()
    : new_begin_address{ 0 }
    , truncate_callback{ nullptr }
    , complete_callback{ nullptr } {
  }

  void Initialize(Address new_begin_address_, truncate_callback_t truncate_callback_,
                  complete_callback_t complete_callback_) {
    new_begin_address = new_begin_address_;
    truncate_callback = truncate_callback_;
    complete_callback = complete_callback_;
  }

  Address new_begin_address;
  truncate_callback_t truncate_callback;
  complete_callback_t complete_callback;
};


class GcStateInMemIndex: public GcState {
 public:
  static constexpr uint64_t kHashTableChunkSize = 16384;

  GcStateInMemIndex()
    : GcState()
    , num_chunks{ 0 }
    , next_chunk{ 0 } {
  }

  void Initialize(Address new_begin_address_, truncate_callback_t truncate_callback_,
                  complete_callback_t complete_callback_, uint64_t num_chunks_) {
    GcState::Initialize(new_begin_address_, truncate_callback_, complete_callback_);
    num_chunks = num_chunks_;
    next_chunk = 0;
  }

  uint64_t num_chunks;
  std::atomic<uint64_t> next_chunk;
};

class GcStateFasterIndex: public GcStateInMemIndex {
 public:
  static constexpr uint64_t kHashTableChunkSize = GcStateInMemIndex::kHashTableChunkSize;

  GcStateFasterIndex()
    : GcStateInMemIndex()
    , min_address{ std::numeric_limits<uint64_t>::max() }
    , thread_count{ 0 } {
  }

  void Initialize(Address new_begin_address_, truncate_callback_t truncate_callback_,
                  complete_callback_t complete_callback_, uint64_t num_chunks_) {
    GcStateInMemIndex::Initialize(new_begin_address_, truncate_callback_, complete_callback_, num_chunks_);
    min_address = std::numeric_limits<uint64_t>::max();
    thread_count = 0;
  }

  std::atomic<uint64_t> min_address;
  std::atomic<uint16_t> thread_count;
};


}
} // namespace FASTER::core
