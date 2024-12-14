// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <chrono>
#include <stdint.h>

#include "core/utility.h"
#include "index/mem_index.h"

#ifdef TOML_CONFIG
#include "toml.hpp"
#endif

using namespace std::chrono_literals;

namespace FASTER {
namespace core {

// Hlog Config
constexpr double DEFAULT_HLOG_MUTABLE_FRACTION = 0.9;

struct HlogConfig {
  uint64_t in_mem_size;
  double mutable_fraction;
  bool pre_allocate;
};

// Read-cache Config
struct ReadCacheConfig {
  uint64_t mem_size;
  double mutable_fraction;
  bool pre_allocate;
  bool enabled;
};

constexpr ReadCacheConfig DEFAULT_READ_CACHE_CONFIG {
  //.mem_size = 256_MiB,
  //.mutable_fraction = 0.5,
  //.pre_allocate = false,

  .enabled = false
};

// Hlog Compaction Config

struct HlogCompactionConfig {
  std::chrono::milliseconds check_interval;
  double trigger_pct;
  double compact_pct;
  uint64_t max_compacted_size;
  uint64_t hlog_size_budget;
  uint8_t num_threads;

  bool enabled;
};

constexpr HlogCompactionConfig DEFAULT_HLOG_COMPACTION_CONFIG {
  .check_interval = 250ms,
  .trigger_pct = 0.8,
  .compact_pct = 0.2,
  .max_compacted_size = 512_MiB,
  .num_threads = 4,

  .enabled = false
};

// F2 Compaction Config

struct F2CompactionConfig {
  HlogCompactionConfig hot_store;
  HlogCompactionConfig cold_store;
};

constexpr static F2CompactionConfig DEFAULT_F2_COMPACTION_CONFIG{
  // hot-log compaction policy
  // check every 250ms, trigger at 80% of 1GB, compact min(20% of total size, 256MB), 4 threads
  .hot_store{
    .check_interval = 250ms,
    .trigger_pct = 0.8,
    .compact_pct = 0.2,
    .max_compacted_size = 256_MiB,
    .hlog_size_budget = 1_GiB,
    .num_threads = 4,

    .enabled = true
  },
  // cold-log compaction policy
  // check every 250ms, trigger at 90% of 8GB, compact min(10% of total size, 1GB), 4 threads
  .cold_store{
    .check_interval = 250ms,
    .trigger_pct = 0.9,
    .compact_pct = 0.1,
    .max_compacted_size = 1_GiB,
    .hlog_size_budget = 8_GiB,
    .num_threads = 4,

    .enabled = true
  }
};


// FASTER store config

#ifdef TOML_CONFIG
HlogConfig PopulateHlogConfig(const toml::value& hlog_table) {
  // hlog
  const uint64_t in_mem_size = toml::find<uint64_t>(hlog_table, "in_mem_size_mb") * (1 << 20);
  // toml11 does not parse ints if 'double' type is specified; the following implements the intended behavior
  double mutable_fraction = toml::find_or<int>(hlog_table, "mutable_fraction", -1);  // -1 "means" not found with int type
  mutable_fraction = (mutable_fraction < 0) ? toml::find_or<double>(hlog_table, "mutable_fraction", DEFAULT_HLOG_MUTABLE_FRACTION) : mutable_fraction;
  const bool pre_allocate = toml::find_or<bool>(hlog_table, "pre_allocate", false);

  const std::vector<std::string> VALID_HLOG_FIELDS = { "in_mem_size_mb", "mutable_fraction", "pre_allocate", "compaction" };
  for (auto& it : toml::get<toml::table>(hlog_table)) {
    if (std::find(VALID_HLOG_FIELDS.begin(), VALID_HLOG_FIELDS.end(), it.first) == VALID_HLOG_FIELDS.end()) {
      fprintf(stderr, "WARNING: Ignoring invalid *hlog* field '%s'\n", it.first.c_str());
    }
  }

  return { in_mem_size, mutable_fraction, pre_allocate };
}

HlogCompactionConfig PopulateHlogCompactionConfig(const toml::value& hlog_table) {
  // hlog.compaction
  HlogCompactionConfig hlog_compaction_config{ .enabled = false };
  if (hlog_table.contains("compaction")) {
    const auto compaction_table = toml::find(hlog_table, "compaction");
    hlog_compaction_config.enabled = toml::find_or<bool>(compaction_table, "enabled", hlog_compaction_config.enabled);
    hlog_compaction_config.hlog_size_budget = toml::find<uint64_t>(compaction_table, "hlog_size_budget_mb") * (1 << 20);
    hlog_compaction_config.max_compacted_size = compaction_table.contains("max_compacted_size_mb")
                                                  ? toml::find<uint64_t>(compaction_table, "max_compacted_size_mb") * (1 << 20)
                                                  : hlog_compaction_config.max_compacted_size;
    hlog_compaction_config.trigger_pct = toml::find_or<double>(compaction_table, "trigger_pct", hlog_compaction_config.trigger_pct);
    hlog_compaction_config.compact_pct = toml::find_or<double>(compaction_table, "compact_pct", hlog_compaction_config.compact_pct);
    hlog_compaction_config.check_interval = compaction_table.contains("check_interval_ms")
                                              ? std::chrono::milliseconds(toml::find<size_t>(compaction_table, "check_interval_ms"))
                                              : hlog_compaction_config.check_interval;
    hlog_compaction_config.num_threads = static_cast<uint8_t>(toml::find_or<int>(compaction_table, "num_threads", hlog_compaction_config.num_threads));

    const std::vector<std::string> VALID_HLOG_COMPACTION_FIELDS = {
      "enabled", "hlog_size_budget_mb", "max_compacted_size_mb", "trigger_pct", "compact_pct", "check_interval_ms", "num_threads" };
    for (auto& it : toml::get<toml::table>(compaction_table)) {
      if (std::find(VALID_HLOG_COMPACTION_FIELDS.begin(), VALID_HLOG_COMPACTION_FIELDS.end(), it.first) == VALID_HLOG_COMPACTION_FIELDS.end()) {
        fprintf(stderr, "WARNING: Ignoring invalid *hlog-compaction* field '%s'\n", it.first.c_str());
      }
    }
  }

  return hlog_compaction_config;
}

template<bool IsColdStore>
ReadCacheConfig PopulateReadCacheConfig(const toml::value& top_table);

template<>
ReadCacheConfig PopulateReadCacheConfig<true>(const toml::value& top_table) {
  ReadCacheConfig rc_config{ .enabled = false };
  if (top_table.contains("read_cache")) {
    throw std::runtime_error{ "Found invalid 'read_cache' definition for cold store." };
  }
  return rc_config;
}

template<>
ReadCacheConfig PopulateReadCacheConfig<false>(const toml::value& top_table) {
  ReadCacheConfig rc_config{ .enabled = false };
  if (top_table.contains("read_cache")) {
    const auto rc_table = toml::find(top_table, "read_cache");
    rc_config.enabled = toml::find_or<bool>(rc_table, "enabled", rc_config.enabled);
    rc_config.mem_size = rc_table.contains("in_mem_size_mb") ?
                            toml::find<uint64_t>(rc_table, "in_mem_size_mb") * (1 << 20) : rc_config.mem_size;

    // toml11 does not parse ints if 'double' type is specified; the following implements the intended behavior
    double mutable_fraction = toml::find_or<int>(rc_table, "mutable_fraction", -1); // -1 "means" not found with int type
    rc_config.mutable_fraction = (mutable_fraction < 0) ? toml::find_or<double>(rc_table, "mutable_fraction", rc_config.mutable_fraction) : mutable_fraction;

    rc_config.pre_allocate = toml::find_or(rc_table, "pre_allocate", rc_config.pre_allocate);

    const std::vector<std::string> valid_read_cache_fields = { "enabled", "in_mem_size_mb", "mutable_fraction", "pre_allocate" };
    for (auto& it : toml::get<toml::table>(rc_table)) {
      if (std::find(valid_read_cache_fields.begin(), valid_read_cache_fields.end(), it.first) == valid_read_cache_fields.end()) {
        fprintf(stderr, "warning: ignoring invalid *read-cache* field '%s'\n", it.first.c_str());
      }
    }
  }
  return rc_config;
}

#endif

// FasterKv Store config
template<class H>
struct FasterKvConfig{
  typedef H hash_index_t;
  typedef typename H::Config IndexConfig;

  constexpr static bool IsColdStore = !H::IsSync();

  IndexConfig index_config;
  HlogConfig hlog_config;
  HlogCompactionConfig hlog_compaction_config;
  ReadCacheConfig rc_config;
  std::string filepath;

  #ifdef TOML_CONFIG
  template<typename... Ts>
  static FasterKvConfig FromConfigString(const std::string& config, Ts... table_key_path) {
    std::istringstream is(config, std::ios_base::binary | std::ios_base::in);
    const auto data = toml::parse(is, "std::string");

    // parse TOML config & validate top-level fields
    const auto top_table = toml::find(data, table_key_path...);

    const std::vector<std::string> VALID_TOP_LEVEL_FIELDS = { "filepath", "hlog", "index", "read_cache" };
    for (auto& it : toml::get<toml::table>(top_table)) {
      if (std::find(VALID_TOP_LEVEL_FIELDS.begin(), VALID_TOP_LEVEL_FIELDS.end(), it.first) == VALID_TOP_LEVEL_FIELDS.end()) {
        fprintf(stderr, "WARNING: Ignoring invalid top-level field '%s'\n", it.first.c_str());
      }
    }

    // top-level
    std::string filepath = toml::find<std::string>(top_table, "filepath");

    // hlog
    const auto hlog_table = toml::find(top_table, "hlog");
    HlogConfig hlog_config = PopulateHlogConfig(hlog_table);
    // hlog.compaction
    HlogCompactionConfig hlog_compaction_config = PopulateHlogCompactionConfig(hlog_table);

    // index
    const auto index_table = toml::find(top_table, "index");
    IndexConfig index_config{ index_table };

    // Read cache
    ReadCacheConfig rc_config = PopulateReadCacheConfig<IsColdStore>(top_table);

    return { index_config, hlog_config, hlog_compaction_config, rc_config, filepath };
  }
  #endif
};

}
} // namespace FASTER::core
