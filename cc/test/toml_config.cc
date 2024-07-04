// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"

#include "core/faster.h"
#include "core/faster_hc.h"
#include "device/null_disk.h"

#include "test_types.h"

using namespace FASTER::core;
using FASTER::test::FixedSizeKey;
using FASTER::test::SimpleAtomicValue;

using Key = FixedSizeKey<uint64_t>;
using Value = SimpleAtomicValue<uint64_t>;

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif

/// Upsert context required to insert data for unit testing.
class UpsertContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  UpsertContext(uint64_t key)
    : key_{ key }
  {}

  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_{ other.key_ }
  {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  /// Non-atomic and atomic Put() methods.
  inline void Put(Value& value) {
    value.value = 23;
  }
  inline bool PutAtomic(Value& value) {
    value.atomic_value.store(42);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
};

const char* FASTER_TEST_CONFIG = R"(
  [faster]
  filepath = ""

  [faster.hlog]
  in_mem_size_mb = 1024
  mutable_fraction = 0.85
  pre_allocate = false

  [faster.hlog.compaction]
  enabled = false
  hlog_size_budget_mb = 2048
  trigger_pct = 0.8
  compact_pct = 0.2
  max_compacted_size_mb = 512
  check_interval_ms = 250
  num_threads = 4

  [faster.index]
  table_size = 128

  [faster.read_cache]
  enabled = false
  in_mem_size_mb = 512
  mutable_fraction = 0.81
  pre_allocate = false
)";

const char* F2_TEST_CONFIG = R"(
  [f2]
  filepath = "hc_store"

  [f2.hot]
  filepath = "hot"

  [f2.hot.hlog]
  in_mem_size_mb = 1024
  mutable_fraction = 0.85
  pre_allocate = false

  [f2.hot.index]
  table_size = 128

  [f2.hot.read_cache]
  enabled = false
  in_mem_size_mb = 512
  mutable_fraction = 0.81
  pre_allocate = false

  [f2.cold]
  filepath = "cold"

  [f2.cold.hlog]
  in_mem_size_mb = 192
  mutable_fraction = 0

  [f2.cold.index]
  table_size = 524288
  in_mem_size_mb = 192
  mutable_fraction = 0.0
)";


TEST(TomlConfig, FasterFromConfig) {
  typedef FasterKv<Key, Value, FASTER::device::NullDisk> faster_t;

  std::string config_string_{ FASTER_TEST_CONFIG };
  faster_t store = faster_t::FromConfigString(config_string_);
}

TEST(TomlConfig, FasterFromConfigFile) {
  typedef FasterKv<Key, Value, FASTER::device::NullDisk> faster_t;
  const std::string filepath = "tmp-config.toml";

  FILE* f = fopen(filepath.c_str(), "w");
  fprintf(f, "%s", FASTER_TEST_CONFIG);
  fclose(f);

  faster_t store = faster_t::FromConfigFile(filepath);
}

TEST(TomlConfig, F2FromConfig) {
  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef FasterKvHC<Key, Value, disk_t> faster_hc_t;

  std::string config_{ F2_TEST_CONFIG };
  faster_hc_t store = faster_hc_t::FromConfigString(config_);
}

TEST(TomlConfig, F2FromConfigFile) {
  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef FasterKvHC<Key, Value, disk_t> faster_hc_t;

  const std::string filepath = "tmp-config.toml";

  FILE* f = fopen(filepath.c_str(), "w");
  fprintf(f, "%s", F2_TEST_CONFIG);
  fclose(f);

  faster_hc_t store = faster_hc_t::FromConfigFile(filepath);
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
