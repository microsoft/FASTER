// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <experimental/filesystem>
#include <string>

#include "concurrent_recovery_test.h"
#include "sum_store.h"
#include "single_threaded_recovery_test.h"

int main(int argc, char* argv[]) {
  if(argc < 3) {
    printf("Usage: sum_store.exe single <operation>\n");
    printf("Where <operation> is one of \"populate\", \"recover <version>\", or \"continue\".\n");
    exit(0);
  }

  std::experimental::filesystem::create_directory("sum_storage");

  static constexpr uint64_t kKeySpace = (1L << 15);

  sum_store::store_t store{ kKeySpace, 17179869184, "sum_storage" };


  std::string type{ argv[1] };
  if(type == "single") {
    sum_store::SingleThreadedRecoveryTest test{ store };

    std::string task{ argv[2] };
    if(task == "populate") {
      test.Populate();
    } else if(task == "recover") {
      if(argc != 4) {
        printf("Must specify version to recover to.\n");
        exit(1);
      }
      uint32_t version = std::atoi(argv[3]);
      test.RecoverAndTest(version, version);
    }
  } else if(type == "concurrent") {
    if(argc < 4) {
      printf("Must specify number of threads to execute concurrently.\n");
      exit(1);
    }

    size_t num_threads = std::atoi(argv[2]);

    sum_store::ConcurrentRecoveryTest test{ store, num_threads };

    std::string task{ argv[3] };
    if(task == "populate") {
      test.Populate();
    } else if(task == "recover") {
      if(argc != 5) {
        printf("Must specify version to recover to.\n");
        exit(1);
      }
      uint32_t version = std::atoi(argv[4]);
      test.RecoverAndTest(version, version);
    } else if(task == "continue") {
      if(argc != 5) {
        printf("Must specify version to continue from.\n");
        exit(1);
      }
      uint32_t version = std::atoi(argv[4]);
      test.Continue(version, version);
    }

  }


  return 0;
}

