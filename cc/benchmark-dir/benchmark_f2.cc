// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "benchmark.h"

#include "core/faster_hc.h"

using namespace FASTER::core;

using hot_index_t = HashIndex<disk_t>;
// Using 32 hash bucket entries per hash chunk (i.e., 32 * 8 = 256 Bytes / hash chunk)
using cold_index_t = FasterIndex<disk_t, ColdLogHashIndexDefinition<32>>;
using store_t = FasterKvHC<Key, Value, disk_t, hot_index_t, cold_index_t>;

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

  // F2 store has a hot hash table with approx. kInitCount / 2 entries, a hot log of 6 GiB and a cold log of 10 GiB.
  // NOTE: This is an I/O intensive benchmark -- for in-memory only, configure hot log to 8 GiB and cold log to 8 GiB.
  size_t hot_table_size = next_power_of_two(kInitCount / 2);
  size_t cold_table_size = 1'048'576; // 1M in-memory hash index entries * 64 Bytes each -> 64 MiB

  ReadCacheConfig rc_config { .enabled = false };
  store_t::HotIndexConfig hot_index_config{ hot_table_size };
  store_t::ColdIndexConfig cold_index_config{ cold_table_size, 256_MiB, 0.6 };

  store_t store{
    hot_index_config, 4294967296, "storage-hc/hot_",
    cold_index_config, 12884901888, "storage-hc/cold_",
    0.9, 0, rc_config
  };

  run(&store, workload, num_threads);

  return 0;
}
