// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <chrono>
#include <stdint.h>

#include "common/utils.h"

using namespace std::chrono_literals;

namespace FASTER {
namespace core {

struct ReadCacheConfig {
  uint64_t mem_size = 256_MiB;
  double mutable_fraction = 0.5;
  bool pre_allocate_log = false;

  bool enabled = false;
};
static_assert(sizeof(ReadCacheConfig) == 24);

struct HlogCompactionConfig {
  std::chrono::milliseconds check_interval = 250ms;
  double trigger_perc = 0.8;
  double compact_perc = 0.2;
  uint64_t max_compacted_size;
  uint64_t hlog_size_budget;
  uint8_t num_threads = 4;

  bool enabled = false;
};

struct HCCompactionConfig {
  HlogCompactionConfig hot_store{
    250ms, 0.8, 0.2, 256_MiB, 1_GiB, 4, true };
  HlogCompactionConfig cold_store{
    250ms, 0.9, 0.1, 1_GiB, 8_GiB, 4, true };
};

}
} // namespace FASTER::core
