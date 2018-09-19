// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>

#include "core/auto_ptr.h"
#include "core/faster.h"
#include "sum_store.h"

using namespace FASTER;

namespace sum_store {

class SingleThreadedRecoveryTest {
 public:
  static constexpr uint64_t kNumUniqueKeys = (1L << 23);
  static constexpr uint64_t kNumOps = (1L << 25);
  static constexpr uint64_t kRefreshInterval = (1L << 8);
  static constexpr uint64_t kCompletePendingInterval = (1L << 12);
  static constexpr uint64_t kCheckpointInterval = (1L << 20);

  SingleThreadedRecoveryTest(store_t& store_)
    : store{ store_ } {
  }

 private:

 public:
  void Populate() {
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

    // Register thread with FASTER
    store.StartSession();

    // Process the batch of input data
    for(uint64_t idx = 0; idx < kNumOps; ++idx) {
      RmwContext context{ AdId{ idx % kNumUniqueKeys}, 1 };
      store.Rmw(context, callback, idx);

      if(idx % kCheckpointInterval == 0) {
        Guid token;
        store.Checkpoint(nullptr, hybrid_log_persistence_callback, token);
        printf("Calling Checkpoint(), token = %s\n", token.ToString().c_str());
      }
      if(idx % kCompletePendingInterval == 0) {
        store.CompletePending(false);
      } else if(idx % kRefreshInterval == 0) {
        store.Refresh();
      }
    }
    // Make sure operations are completed
    store.CompletePending(true);

    // Deregister thread from FASTER
    store.StopSession();

    printf("Populate successful\n");

    std::string discard;
    std::getline(std::cin, discard);
  }

  void RecoverAndTest(const Guid& index_token, const Guid& hybrid_log_token) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context{ ctxt };
      assert(result == Status::Ok);
    };

    // Recover
    uint32_t version;
    std::vector<Guid> session_ids;
    store.Recover(index_token, hybrid_log_token, version, session_ids);

    // Create array for reading
    auto read_results = alloc_aligned<uint64_t>(64, sizeof(uint64_t) * kNumUniqueKeys);
    std::memset(read_results.get(), 0, sizeof(uint64_t) * kNumUniqueKeys);

    Guid session_id = session_ids[0];

    // Register with thread
    uint64_t sno = store.ContinueSession(session_id);

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

    for(uint64_t idx = 0; idx <= sno; ++idx) {
      ++expected_results.get()[idx % kNumUniqueKeys];
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

    std::string discard;
    std::getline(std::cin, discard);
  }

  void Continue() {
    // Not implemented.
    assert(false);
  }

  store_t& store;
};

} // namespace sum_store
