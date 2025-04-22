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

  // FASTER store has a hash table with approx. kInitCount / 4 entries
  size_t init_size = next_power_of_two(kInitCount / 4);

  // PERF: Uncomment one of the two below for in-memory versus larger-than-memory benchmark
  store_t store{ init_size, 17179869184, "E:\\storage" }; // data served from memory, log of size 16 GB
  // store_t store{ init_size, 1048576 * 192ULL, "E:\\storage", 0.4 }; // data served from disk, log size of 192MB

  run(&store, workload, num_threads);

  return 0;
}
