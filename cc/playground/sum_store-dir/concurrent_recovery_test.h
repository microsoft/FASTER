// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "core/auto_ptr.h"
#include "core/faster.h"
#include "core/thread.h"
#include "sum_store.h"

namespace sum_store {

class ConcurrentRecoveryTest {
 public:
  static constexpr uint64_t kNumUniqueKeys = (1L << 22);
  static constexpr uint64_t kKeySpace = (1L << 14);
  static constexpr uint64_t kNumOps = (1L << 25);
  static constexpr uint64_t kRefreshInterval = (1L << 8);
  static constexpr uint64_t kCompletePendingInterval = (1L << 12);
  static constexpr uint64_t kCheckpointInterval = (1L << 22);

  ConcurrentRecoveryTest(store_t& store_, size_t num_threads_)
    : store{ store_ }
    , num_threads{ num_threads_ }
    , num_active_threads{ 0 }
    , num_checkpoints{ 0 } {
  }

 private:
  static void PopulateWorker(store_t* store, size_t thread_idx,
                             std::atomic<size_t>* num_active_threads, size_t num_threads,
                             std::atomic<uint32_t>* num_checkpoints) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext> context{ ctxt };
      assert(result == Status::Ok);
    };

    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
      if(result != Status::Ok) {
        printf("Thread %" PRIu32 " reports checkpoint failed.\n",
               Thread::id());
      } else {
        printf("Thread %" PRIu32 " reports persistence until %" PRIu64 "\n",
               Thread::id(), persistent_serial_num);
      }
    };

    // Register thread with the store
    store->StartSession();

    ++(*num_active_threads);

    // Process the batch of input data
    for(size_t idx = 0; idx < kNumOps; ++idx) {
      RmwContext context{ idx % kNumUniqueKeys, 1 };
      store->Rmw(context, callback, idx);
      if(idx % kCheckpointInterval == 0 && *num_active_threads == num_threads) {
        Guid token;
        if(store->Checkpoint(nullptr, hybrid_log_persistence_callback, token)) {
          printf("Thread %" PRIu32 " calling Checkpoint(), version = %" PRIu32 ", token = %s\n",
                 Thread::id(), ++(*num_checkpoints), token.ToString().c_str());
        }
      }
      if(idx % kCompletePendingInterval == 0) {
        store->CompletePending(false);
      } else if(idx % kRefreshInterval == 0) {
        store->Refresh();
      }
    }

    // Make sure operations are completed
    store->CompletePending(true);

    // Deregister thread from FASTER
    store->StopSession();

    printf("Populate successful on thread %" PRIu32 ".\n", Thread::id());
  }

 public:
  void Populate() {
    std::deque<std::thread> threads;
    for(size_t idx = 0; idx < num_threads; ++idx) {
      threads.emplace_back(&PopulateWorker, &store, idx, &num_active_threads, num_threads,
                           &num_checkpoints);
    }
    for(auto& thread : threads) {
      thread.join();
    }
    // Verify the records.
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context{ ctxt };
      assert(result == Status::Ok);
    };
    // Create array for reading
    auto read_results = alloc_aligned<uint64_t>(64, sizeof(uint64_t) * kNumUniqueKeys);
    std::memset(read_results.get(), 0, sizeof(uint64_t) * kNumUniqueKeys);

    // Register with thread
    store.StartSession();

    // Issue read requests
    for(uint64_t idx = 0; idx < kNumUniqueKeys; ++idx) {
      ReadContext context{ AdId{ idx }, read_results.get() + idx };
      store.Read(context, callback, idx);
    }

    // Complete all pending requests
    store.CompletePending(true);

    // Release
    store.StopSession();
    for(uint64_t idx = 0; idx < kNumUniqueKeys; ++idx) {
      uint64_t expected_result = (num_threads * kNumOps) / kNumUniqueKeys;
      if(read_results.get()[idx] != expected_result) {
        printf("Debug error for AdId %" PRIu64 ": Expected (%" PRIu64 "), Found(%" PRIu64 ")\n",
               idx,
               expected_result,
               read_results.get()[idx]);
      }
    }
  }

  void RecoverAndTest(const Guid& index_token, const Guid& hybrid_log_token) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context{ ctxt };
      assert(result == Status::Ok);
    };

    // Recover
    uint32_t version;
    std::vector<Guid> session_ids;
    FASTER::core::Status result = store.Recover(index_token, hybrid_log_token, version,
                                  session_ids);
    if(result != FASTER::core::Status::Ok) {
      printf("Recovery failed with error %u\n", static_cast<uint8_t>(result));
      exit(1);
    }

    std::vector<uint64_t> serial_nums;
    for(const auto& session_id : session_ids) {
      serial_nums.push_back(store.ContinueSession(session_id));
      store.StopSession();
    }

    // Create array for reading
    auto read_results = alloc_aligned<uint64_t>(64, sizeof(uint64_t) * kNumUniqueKeys);
    std::memset(read_results.get(), 0, sizeof(uint64_t) * kNumUniqueKeys);

    // Register with thread
    store.StartSession();

    // Issue read requests
    for(uint64_t idx = 0; idx < kNumUniqueKeys; ++idx) {
      ReadContext context{ AdId{ idx}, read_results.get() + idx };
      store.Read(context, callback, idx);
    }

    // Complete all pending requests
    store.CompletePending(true);

    // Release
    store.StopSession();

    // Test outputs
    // Compute expected array
    auto expected_results = alloc_aligned<uint64_t>(64,
                            sizeof(uint64_t) * kNumUniqueKeys);
    std::memset(expected_results.get(), 0, sizeof(uint64_t) * kNumUniqueKeys);

    // Sessions that were active during checkpoint:
    for(uint64_t serial_num : serial_nums) {
      for(uint64_t idx = 0; idx <= serial_num; ++idx) {
        ++expected_results.get()[idx % kNumUniqueKeys];
      }
    }
    // Sessions that were finished at time of checkpoint.
    size_t num_completed = num_threads - serial_nums.size();
    for(size_t thread_idx = 0; thread_idx < num_completed; ++thread_idx) {
      uint64_t serial_num = kNumOps;
      for(uint64_t idx = 0; idx < serial_num; ++idx) {
        ++expected_results.get()[idx % kNumUniqueKeys];
      }
    }

    // Assert if expected is same as found
    for(uint64_t idx = 0; idx < kNumUniqueKeys; ++idx) {
      if(expected_results.get()[idx] != read_results.get()[idx]) {
        printf("Debug error for AdId %" PRIu64 ": Expected (%" PRIu64 "), Found(%" PRIu64 ")\n",
               idx,
               expected_results.get()[idx],
               read_results.get()[idx]);
      }
    }
    printf("Test successful\n");
  }

  static void ContinueWorker(store_t* store, size_t thread_idx,
                             std::atomic<size_t>* num_active_threads, size_t num_threads,
                             std::atomic<uint32_t>* num_checkpoints, Guid guid) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext> context{ ctxt };
      assert(result == Status::Ok);
    };


    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
      if(result != Status::Ok) {
        printf("Thread %" PRIu32 " reports checkpoint failed.\n",
               Thread::id());
      } else {
        printf("Thread %" PRIu32 " reports persistence until %" PRIu64 "\n",
               Thread::id(), persistent_serial_num);
      }
    };

    // Register thread with the store
    uint64_t start_num = store->ContinueSession(guid);

    ++(*num_active_threads);

    // Process the batch of input data
    for(size_t idx = start_num + 1; idx < kNumOps; ++idx) {
      RmwContext context{ idx % kNumUniqueKeys, 1 };
      store->Rmw(context, callback, idx);
      if(idx % kCheckpointInterval == 0 && *num_active_threads == num_threads) {
        Guid token;
        if(store->Checkpoint(nullptr, hybrid_log_persistence_callback, token)) {
          printf("Thread %" PRIu32 " calling Checkpoint(), version = %" PRIu32 ", token = %s\n",
                 Thread::id(), ++(*num_checkpoints), token.ToString().c_str());
        }
      }
      if(idx % kCompletePendingInterval == 0) {
        store->CompletePending(false);
      } else if(idx % kRefreshInterval == 0) {
        store->Refresh();
      }
    }

    // Make sure operations are completed
    store->CompletePending(true);

    // Deregister thread from FASTER
    store->StopSession();

    printf("Populate successful on thread %" PRIu32 ".\n", Thread::id());
  }

  void Continue(const Guid& index_token, const Guid& hybrid_log_token) {
    // Recover
    printf("Recovering version (index_token = %s, hybrid_log_token = %s)\n",
           index_token.ToString().c_str(), hybrid_log_token.ToString().c_str());
    uint32_t version;
    std::vector<Guid> session_ids;
    FASTER::core::Status result = store.Recover(index_token, hybrid_log_token, version,
                                  session_ids);
    if(result != FASTER::core::Status::Ok) {
      printf("Recovery failed with error %u\n", static_cast<uint8_t>(result));
      exit(1);
    } else {
      printf("Recovery Done!\n");
    }

    num_checkpoints.store(version);
    // Some threads may have already completed.
    num_threads = session_ids.size();

    std::deque<std::thread> threads;
    for(size_t idx = 0; idx < num_threads; ++idx) {
      threads.emplace_back(&ContinueWorker, &store, idx, &num_active_threads, num_threads,
                           &num_checkpoints, session_ids[idx]);
    }
    for(auto& thread : threads) {
      thread.join();
    }
  }

  store_t& store;
  size_t num_threads;
  std::atomic<size_t> num_active_threads;
  std::atomic<uint32_t> num_checkpoints;
};

} // namespace sum_store
