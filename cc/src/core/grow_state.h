// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>

#include "persistent_memory_malloc.h"

namespace FASTER {
namespace core {

typedef void(*GrowCompleteCallback)(uint64_t new_size);

/// State of the active grow-index call.
template<class T>
class GrowStateBase {
 public:
  typedef T hlog_t;

  GrowStateBase(hlog_t* hlog_)
    : hlog{ hlog_ }
    , callback{ nullptr } {
      assert(hlog != nullptr);
  }

  void Initialize(GrowCompleteCallback callback_) {
    callback = callback_;
  }

  hlog_t* hlog;
  GrowCompleteCallback callback;
};

template<class T>
class GrowState : public GrowStateBase<T> {
 public:
  typedef T hlog_t;
  static constexpr uint64_t kHashTableChunkSize = 16384;

  GrowState(hlog_t* hlog_)
    : GrowStateBase<T>(hlog_)
    , num_pending_chunks{ 0 }
    , old_version{ UINT8_MAX }
    , new_version{ UINT8_MAX } {
  }

  void Initialize(GrowCompleteCallback callback_, uint8_t current_version,
                  uint64_t num_chunks_) {
    GrowStateBase<T>::Initialize(callback_);
    assert(current_version == 0 || current_version == 1);
    old_version = current_version;
    new_version = 1 - current_version;
    num_chunks = num_chunks_;
    num_pending_chunks = num_chunks_;
    next_chunk = 0;
  }

  uint8_t old_version;
  uint8_t new_version;
  uint64_t num_chunks;
  std::atomic<uint64_t> num_pending_chunks;
  std::atomic<uint64_t> next_chunk;
};

}
} // namespace FASTER::core
