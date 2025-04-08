// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "benchmark.h"

using namespace FASTER::core;

using store_t = FasterKv<Key, Value, disk_t>;

int main(int argc, char* argv[]) {
  constexpr size_t kNumArgs = 4;
  if(argc != kNumArgs + 1) {
    printf("Usage: benchmark.exe <workload> <# threads> <load_filename> <run_filename>\n");
    exit(0);
  }

  Workload workload = static_cast<Workload>(std::atol(argv[1]));
  size_t num_threads = ::atol(argv[2]);
  std::string load_filename{ argv[3] };
  std::string run_filename{ argv[4] };

  load_files(load_filename, run_filename);

  // FASTER store has a hash table with approx. kInitCount / 2 entries and a log of size 16 GB
  size_t init_size = next_power_of_two(kInitCount / 2);
  store_t store{ init_size, 17179869184, "storage" };

  run(&store, workload, num_threads);

  return 0;
}
