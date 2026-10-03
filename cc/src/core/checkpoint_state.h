// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <unordered_map>

#include "address.h"
#include "guid.h"
#include "malloc_fixed_page_size.h"
#include "status.h"
#include "thread.h"
#include "utility.h"

namespace FASTER {
namespace core {

// Used when no callback context was provided (i.e., Checkpoint())
typedef void(*IndexPersistenceCallback)(Status result);
typedef void(*HybridLogPersistenceCallback)(Status result, uint64_t persistent_serial_num);

// Used when callback context was provided (i.e., InternalCheckpoint())
typedef void(*InternalIndexPersistenceCallback)(void* ctxt, Status result);
typedef void(*InternalHybridLogPersistenceCallback)(void* ctxt, Status result, uint64_t persistent_serial_num);

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
    std::memset(reinterpret_cast<void*>(guids), 0, sizeof(guids));
    std::memset(monotonic_serial_nums, 0, sizeof(monotonic_serial_nums));
  }

  inline void Initialize(bool use_snapshot_file_, uint32_t version_, Address flushed_address_) {
    use_snapshot_file = use_snapshot_file_;
    version = version_;
    num_threads = 0;
    flushed_address = flushed_address_;
    final_address = Address::kMaxAddress;
    std::memset(reinterpret_cast<void*>(guids), 0, sizeof(guids));
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

  CheckpointState()
    : index_checkpoint_started{ false }
    , failed{ false }
    , flush_pending{ UINT32_MAX }
    , index_persistence_callback{ nullptr }
    , hybrid_log_persistence_callback{ nullptr }
    , callback_context{ nullptr } {
  }

  void InitializeIndexCheckpoint(const Guid& token, uint32_t version, uint64_t table_size,
                                 Address log_begin_address, Address checkpoint_start_address,
                                 IndexPersistenceCallback callback) {
    failed = false;
    index_checkpoint_started = false;
    continue_tokens.clear();
    index_token = token;
    hybrid_log_token = Guid{};
    index_metadata.Initialize(version, table_size, log_begin_address, checkpoint_start_address);
    log_metadata.Reset();
    flush_pending = 0;
    index_persistence_callback = reinterpret_cast<func_ptr_t>(callback);
    hybrid_log_persistence_callback = nullptr;
    callback_context = nullptr;
  }

  void InitializeHybridLogCheckpoint(const Guid& token, uint32_t version, bool use_snapshot_file,
                                     Address flushed_until_address,
                                     HybridLogPersistenceCallback callback) {
    failed = false;
    index_checkpoint_started = false;
    continue_tokens.clear();
    index_token = Guid{};
    hybrid_log_token = token;
    index_metadata.Reset();
    log_metadata.Initialize(use_snapshot_file, version, flushed_until_address);
    if(use_snapshot_file) {
      flush_pending = UINT32_MAX;
    } else {
      flush_pending = 0;
    }
    index_persistence_callback = nullptr;
    hybrid_log_persistence_callback = reinterpret_cast<func_ptr_t>(callback);
    callback_context = nullptr;
  }

  template <class IPC, class LPC>
  void InitializeCheckpoint(const Guid& token, uint32_t version, uint64_t table_size,
                            Address log_begin_address, Address checkpoint_start_address,
                            bool use_snapshot_file, Address flushed_until_address,
                            IPC index_persistence_callback_,
                            LPC hybrid_log_persistence_callback_,
                            void* callback_context_ = nullptr) {
    // static check of callback supported types
    static_assert((std::is_same<IPC, IndexPersistenceCallback>::value &&
                  std::is_same<LPC, HybridLogPersistenceCallback>::value) ||
                  (std::is_same<IPC, InternalIndexPersistenceCallback>::value &&
                  std::is_same<LPC, InternalHybridLogPersistenceCallback>::value));

    InitializeCheckpoint(token, version, table_size, log_begin_address, checkpoint_start_address,
                            use_snapshot_file, flushed_until_address,
                            reinterpret_cast<func_ptr_t>(index_persistence_callback_),
                            reinterpret_cast<func_ptr_t>(hybrid_log_persistence_callback_),
                            callback_context_);
  }

 private:
  void InitializeCheckpoint(const Guid& token, uint32_t version, uint64_t table_size,
                            Address log_begin_address, Address checkpoint_start_address,
                            bool use_snapshot_file, Address flushed_until_address,
                            func_ptr_t index_persistence_callback_,
                            func_ptr_t hybrid_log_persistence_callback_,
                            void* callback_context_) {
    failed = false;
    index_checkpoint_started = false;
    continue_tokens.clear();
    index_token = token;
    hybrid_log_token = token;
    index_metadata.Initialize(version, table_size, log_begin_address, checkpoint_start_address);
    log_metadata.Initialize(use_snapshot_file, version, flushed_until_address);
    if(use_snapshot_file) {
      flush_pending = UINT32_MAX;
    } else {
      flush_pending = 0;
    }
    index_persistence_callback = index_persistence_callback_;
    hybrid_log_persistence_callback = hybrid_log_persistence_callback_;
    callback_context = callback_context_;
  }

 public:
  auto GetIndexPersistenceCallback() {
    return [cb = this->index_persistence_callback, ctxt = this->callback_context]() {
      if (!cb) {
        return; // no callback was provided
      }

      // TODO: Status was always set to Status::Ok on the original FASTER code.
      //       Ideally, we should check if `failed` member is true
      if (ctxt) {
        const auto& callback = reinterpret_cast<InternalIndexPersistenceCallback>(cb);
        callback(ctxt, Status::Ok);
      } else {
        const auto& callback = reinterpret_cast<IndexPersistenceCallback>(cb);
        callback(Status::Ok);
      }
    };
  }

  auto GetHybridLogPersistenceCallback() {
    return [cb = this->hybrid_log_persistence_callback, ctxt = this->callback_context](uint64_t serial_num) {
      if (!cb) {
        return; // no callback was provided
      }

      // TODO: Status was always set to Status::Ok on the original FASTER code.
      //       Ideally, we should check if `failed` member is true
      if (ctxt) {
        const auto& callback = reinterpret_cast<InternalHybridLogPersistenceCallback>(cb);
        callback(ctxt, Status::Ok, serial_num);
      } else {
        const auto& callback = reinterpret_cast<HybridLogPersistenceCallback>(cb);
        callback(Status::Ok, serial_num);
      }
    };
  }

  void CheckpointDone() {
    assert(!failed);
    assert(index_token == Guid{} || index_checkpoint_started);
    assert(continue_tokens.empty());
    assert(flush_pending == 0);
    index_metadata.Reset();
    log_metadata.Reset();
    snapshot_file.Close();
    index_persistence_callback = nullptr;
    hybrid_log_persistence_callback = nullptr;
    callback_context = nullptr;
  }

  inline void InitializeRecover(const Guid& index_token_, const Guid& hybrid_log_token_) {
    failed = false;
    index_token = index_token_;
    hybrid_log_token = hybrid_log_token_;
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

  Guid index_token;
  Guid hybrid_log_token;

  /// State used when fold_over_snapshot = false.
  file_t snapshot_file;
  std::atomic<uint32_t> flush_pending;

  func_ptr_t index_persistence_callback;
  func_ptr_t hybrid_log_persistence_callback;
  void* callback_context;

  std::unordered_map<Guid, uint64_t> continue_tokens;
};

}
} // namespace FASTER::core

