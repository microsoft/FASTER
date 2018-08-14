// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <unordered_map>
#include "address.h"
#include "guid.h"
#include "malloc_fixed_page_size.h"
#include "thread.h"

namespace FASTER {
namespace core {

/// Checkpoint metadata for the index itself.
class IndexMetadata {
 public:
  IndexMetadata()
    : version{ 0 }
    , table_size{ 0 }
    , num_ht_bytes{ 0 }
    , num_ofb_bytes{ 0 }
    , ofb_count{ FixedPageAddress::kInvalidAddress }
    , log_begin_address{ Address::kInvalidAddress }
    , checkpoint_start_address{ Address::kInvalidAddress } {
  }

  inline void Initialize(uint32_t version_, uint64_t size_, Address log_begin_address_,
                         Address checkpoint_start_address_) {
    version = version_;
    table_size = size_;
    log_begin_address = log_begin_address_;
    checkpoint_start_address = checkpoint_start_address_;
    num_ht_bytes = 0;
    num_ofb_bytes = 0;
    ofb_count = FixedPageAddress::kInvalidAddress;
  }
  inline void Reset() {
    version = 0;
    table_size = 0;
    num_ht_bytes = 0;
    num_ofb_bytes = 0;
    ofb_count = FixedPageAddress::kInvalidAddress;
    log_begin_address = Address::kInvalidAddress;
    checkpoint_start_address = Address::kInvalidAddress;
  }

  uint32_t version;
  uint64_t table_size;
  uint64_t num_ht_bytes;
  uint64_t num_ofb_bytes;
  FixedPageAddress ofb_count;
  /// Earliest address that is valid for the log.
  Address log_begin_address;
  /// Address as of which this checkpoint was taken.
  Address checkpoint_start_address;
};
static_assert(sizeof(IndexMetadata) == 56, "sizeof(IndexMetadata) != 56");

/// Checkpoint metadata, for the log.
class LogMetadata {
 public:
  LogMetadata()
    : use_snapshot_file{ false }
    , version{ UINT32_MAX }
    , num_threads{ 0 }
    , flushed_address{ Address::kInvalidAddress }
    , final_address{ Address::kMaxAddress } {
    std::memset(guids, 0, sizeof(guids));
    std::memset(monotonic_serial_nums, 0, sizeof(monotonic_serial_nums));
  }

  inline void Initialize(bool use_snapshot_file_, uint32_t version_, Address flushed_address_) {
    use_snapshot_file = use_snapshot_file_;
    version = version_;
    num_threads = 0;
    flushed_address = flushed_address_;
    final_address = Address::kMaxAddress;
    std::memset(guids, 0, sizeof(guids));
    std::memset(monotonic_serial_nums, 0, sizeof(monotonic_serial_nums));
  }
  inline void Reset() {
    Initialize(false, UINT32_MAX, Address::kInvalidAddress);
  }

  bool use_snapshot_file;
  uint32_t version;
  std::atomic<uint32_t> num_threads;
  Address flushed_address;
  Address final_address;
  uint64_t monotonic_serial_nums[Thread::kMaxNumThreads];
  Guid guids[Thread::kMaxNumThreads];
};
static_assert(sizeof(LogMetadata) == 32 + (24 * Thread::kMaxNumThreads),
              "sizeof(LogMetadata) != 32 + (24 * Thread::kMaxNumThreads)");

/// State of the active Checkpoint()/Recover() call, including metadata written to disk.
template <class F>
class CheckpointState {
 public:
  typedef F file_t;
  typedef void(*persistence_callback_t)(uint64_t persistent_serial_num);

  CheckpointState()
    : index_checkpoint_started{ false }
    , failed{ false }
    , flush_pending{ UINT32_MAX }
    , persistence_callback{ nullptr } {
  }

  void InitializeCheckpoint(uint32_t version, uint64_t table_size, Address log_begin_address,
                            Address checkpoint_start_address, bool use_snapshot_file,
                            Address flushed_until_address,
                            persistence_callback_t persistence_callback_) {
    failed = false;
    index_checkpoint_started = false;
    continue_tokens.clear();
    index_metadata.Initialize(version, table_size, log_begin_address, checkpoint_start_address);
    log_metadata.Initialize(use_snapshot_file, version, flushed_until_address);
    if(use_snapshot_file) {
      flush_pending = UINT32_MAX;
    } else {
      flush_pending = 0;
    }
    persistence_callback = persistence_callback_;
  }

  void CheckpointDone() {
    assert(!failed);
    assert(index_checkpoint_started);
    assert(continue_tokens.empty());
    assert(flush_pending == 0);
    index_metadata.Reset();
    log_metadata.Reset();
    snapshot_file.Close();
    persistence_callback = nullptr;
  }

  inline void InitializeRecover() {
    failed = false;
  }

  void RecoverDone() {
    assert(!failed);
    index_metadata.Reset();
    log_metadata.Reset();
    snapshot_file.Close();
  }

  std::atomic<bool> index_checkpoint_started;
  std::atomic<bool> failed;
  IndexMetadata index_metadata;
  LogMetadata log_metadata;
  /// State used when fold_over_snapshot = false.
  file_t snapshot_file;
  std::atomic<uint32_t> flush_pending;

  persistence_callback_t persistence_callback;
  std::unordered_map<Guid, uint64_t> continue_tokens;
};

}
} // namespace FASTER::core

