// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>

namespace FASTER {
namespace core {

/// State of the active grow-index call.
class GrowState {
 public:
  typedef void(*callback_t)(uint64_t new_size);

  GrowState()
    : callback{ nullptr }
    , num_pending_chunks{ 0 }
    , old_version{ UINT8_MAX }
    , new_version{ UINT8_MAX } {
  }

  void Initialize(callback_t callback_, uint8_t current_version, uint64_t num_chunks_) {
    callback = callback_;
    assert(current_version == 0 || current_version == 1);
    old_version = current_version;
    new_version = 1 - current_version;
    num_chunks = num_chunks_;
    num_pending_chunks = num_chunks_;
    next_chunk = 0;
  }

  callback_t callback;
  uint8_t old_version;
  uint8_t new_version;
  uint64_t num_chunks;
  std::atomic<uint64_t> num_pending_chunks;
  std::atomic<uint64_t> next_chunk;
};

}
} // namespace FASTER::core
