// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <algorithm>
#include <numeric>
#include <random>
#include <vector>

#include "gtest/gtest.h"

#include "core/f2.h"
#include "core/log_scan.h"

#include "device/null_disk.h"

#include "test_types.h"
#include "utils.h"

using namespace FASTER::core;

// Keys
using FASTER::test::FixedSizeKey;
using FASTER::test::VariableSizeKey;
using FASTER::test::VariableSizeShallowKey;
// Values
using FASTER::test::SimpleAtomicLargeValue;
using FASTER::test::SimpleAtomicMediumValue;
using MediumValue = SimpleAtomicMediumValue<uint64_t>;
using LargeValue = SimpleAtomicLargeValue<uint64_t>;
// Used by variable length key contexts
using FASTER::test::GenLock;
using FASTER::test::AtomicGenLock;

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif

// Parameterized test definition
// <# hash index buckets, auto compaction, read-cache>
using param_types = std::tuple<uint32_t, bool, bool>;

class HotColdParameterizedTestParam : public ::testing::TestWithParam<param_types> {
};
INSTANTIATE_TEST_CASE_P(
  HotColdTests,
  HotColdParameterizedTestParam,
  ::testing::Values(
    // ==================================================================
    // NOTE: Some tests are disabled for CI -- enable locally if needed
    // ==================================================================

    // Small hash hot & cold indices
    //std::make_tuple(8192, false, false),
    //std::make_tuple(8192, true, false),
    //std::make_tuple(8192, false, true),
    std::make_tuple(8192, true, true),
    // Large hot & cold indices
    //std::make_tuple((1 << 20), false, false),
    //std::make_tuple((1 << 20), true, false),
    //std::make_tuple((1 << 20), false, true),
    std::make_tuple((1 << 20), true, true)
  )
);

#ifdef _WIN32
static std::string root_path{ "test_f2_store" };
#else
static std::string root_path{ "test_f2_store/" };
#endif

static constexpr uint64_t kCompletePendingInterval = 128;
static constexpr uint8_t kNumCompactionThreads = 2;

/// Upsert context required to insert data for unit testing.
template <class K, class V>
class UpsertContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  UpsertContext(key_t key, value_t value)
    : key_{ key }
    , value_{ value }
  {}
  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_{ other.key_ }
    , value_{ other.value_ }
  {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const key_t& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  /// Non-atomic and atomic Put() methods.
  inline void Put(value_t& value) {
    value.value = value_.value;
  }
  inline bool PutAtomic(value_t& value) {
    value.atomic_value.store(value_.value);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  key_t key_;
  value_t value_;
};

/// Context to read a key when unit testing.
template <class K, class V>
class ReadContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  ReadContext(key_t key)
    : key_{ key }
    , num_records{ 0 }
  {}
  /// To have access to the number of records in the callback
  ReadContext(key_t key, uint64_t num_records_)
    : key_{ key }
    , num_records{ num_records_ }
  {}

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ }
    , num_records { other.num_records }
  {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const key_t& key() const {
    return key_;
  }

  inline void Get(const value_t& value) {
    output = value.value;
  }
  inline void GetAtomic(const value_t& value) {
    output = value.atomic_value.load();
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  key_t key_;
 public:
  value_t output;
  uint64_t num_records;
};

/// Context to RMW a key when unit testing.
template<class K, class V>
class RmwContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  RmwContext(key_t key, value_t incr)
    : key_{ key }
    , incr_{ incr } {
  }

  /// Copy (and deep-copy) constructor.
  RmwContext(const RmwContext& other)
    : key_{ other.key_ }
    , incr_{ other.incr_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const key_t& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  inline static constexpr uint32_t value_size(const value_t& old_value) {
    return sizeof(value_t);
  }
  inline void RmwInitial(value_t& value) {
    value.value = incr_.value;
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    value.value = old_value.value + incr_.value;
  }
  inline bool RmwAtomic(value_t& value) {
    value.atomic_value.fetch_add(incr_.value);
    return true;
  }

  protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  key_t key_;
  value_t incr_;
};

/// Context to delete a key when unit testing.
template<class K, class V>
class DeleteContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  explicit DeleteContext(const key_t& key)
    : key_(key)
  {}

  inline const key_t& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  key_t key_;
};


TEST_P(HotColdParameterizedTestParam, UpsertRead) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  std::string hot_fp, cold_fp;
  CreateNewLogDir(root_path, hot_fp, cold_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  bool auto_compaction = std::get<1>(args);
  bool rc_enabled = std::get<2>(args);

  log_info("\nTEST: Index size: %lu\tAuto compaction: %d\tUse Read Cache: %d",
            table_size, auto_compaction, rc_enabled);

  ReadCacheConfig rc_config {
    .mem_size = 256_MiB,
    .mutable_fraction = 0.5,
    .pre_allocate = false,
    .enabled = rc_enabled,
  };

  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, kNumCompactionThreads, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 768_MiB, kNumCompactionThreads, auto_compaction };

  f2_t::ColdIndexConfig cold_index_config{ table_size, 256_MiB, 0.6 };
  f2_t store{ table_size, 192_MiB, hot_fp,
              cold_index_config, 192_MiB, cold_fp,
              0.4, 0, rc_config, f2_compaction_config };

  uint32_t num_records = 100000; // ~800 MB of data
  store.StartSession();

  // Insert.
  for(size_t idx = 1; idx <= num_records; idx += 2) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(idx), Value(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }

  // Read.
  for(size_t idx = 1; idx <= num_records; idx += 2) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(context->key().key, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(idx, context.output.value);
    }
    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Upsert more records
  for(size_t idx = 2; idx <= num_records; idx += 2) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(idx), Value(idx * 2) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true, 4);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }

  // Read existing again (in random order), plus non-existing ones
  std::vector<uint64_t> keys (num_records + num_records / 4);
  std::iota(keys.begin(), keys.end(), 1);
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(42));

  for(size_t idx = 0; idx < keys.size(); ++idx) {
    uint64_t key = keys[idx];

    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      if (context->key().key > context->num_records) {
        ASSERT_EQ(Status::NotFound, result);
        return;
      }
      ASSERT_EQ(Status::Ok, result);
      if (context->key().key % 2 == 1)
        ASSERT_EQ(context->key().key, context->output.value );
      else
        ASSERT_EQ(context->key().key * 2, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(key), num_records };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::NotFound || result == Status::Pending);
    if (result == Status::Ok) {
      if (key % 2 == 1) ASSERT_EQ(key, context.output.value);
      else ASSERT_EQ(key * 2, context.output.value);
    }
    else if (result != Status::Pending) {
      ASSERT_EQ(Status::NotFound, result);
      ASSERT_TRUE(key > num_records);
    }

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }

  if (!auto_compaction) {
    // perform cold-cold compaction
    store.CompactColdLog(store.cold_store.hlog.safe_read_only_address.control(), true, 4);
  }

  store.StopSession();
}

TEST_P(HotColdParameterizedTestParam, HotColdCompaction) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = MediumValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  std::string hot_fp, cold_fp;
  CreateNewLogDir(root_path, hot_fp, cold_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  bool auto_compaction = std::get<1>(args);
  bool rc_enabled = std::get<2>(args);

  log_info("\nTEST: Index size: %lu\tAuto compaction: %d\tUse Read Cache: %d",
            table_size, auto_compaction, rc_enabled);

  ReadCacheConfig rc_config {
    .mem_size = 256_MiB,
    .mutable_fraction = 0.5,
    .pre_allocate = false,
    .enabled = rc_enabled,
  };

  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, kNumCompactionThreads, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 2_GiB, kNumCompactionThreads, auto_compaction };

  f2_t::ColdIndexConfig cold_index_config{ 8192, 256_MiB, 0.6 };
  f2_t store{ table_size, 192_MiB, hot_fp,
              cold_index_config, 192_MiB, cold_fp,
              0.4, 0, rc_config, f2_compaction_config };

  uint32_t num_records = 500'000; // ~500 MB of data
  store.StartSession();

  // Insert.
  for(size_t idx = 1; idx <= num_records; idx++) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(idx), Value(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }

  // Read existing again (in random order), plus non-existing ones
  std::vector<uint64_t> keys (num_records + num_records / 2);
  std::iota(keys.begin(), keys.end(), 1);
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(42));

  for(size_t idx = 0; idx < keys.size(); ++idx) {
    uint64_t key = keys[idx];

    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      if (context->key().key > context->num_records) {
        ASSERT_EQ(Status::NotFound, result);
        return;
      }
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(context->key().key, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(key), num_records };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::NotFound || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(key, context.output.value);
    }
    else if (result != Status::Pending) {
      ASSERT_EQ(Status::NotFound, result);
      ASSERT_TRUE(key > num_records);
    }

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }

  if (!auto_compaction) {
    // perform cold-cold compaction
    store.CompactColdLog(store.cold_store.hlog.safe_read_only_address.control(), true, 4);
  }

  store.StopSession();
}

TEST_P(HotColdParameterizedTestParam, UpsertDelete) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  std::string hot_fp, cold_fp;
  CreateNewLogDir(root_path, hot_fp, cold_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  bool auto_compaction = std::get<1>(args);
  bool rc_enabled = std::get<2>(args);

  log_info("\nTEST: Index size: %lu\tAuto compaction: %d\tUse Read Cache: %d",
            table_size, auto_compaction, rc_enabled);

  ReadCacheConfig rc_config {
    .mem_size = 256_MiB,
    .mutable_fraction = 0.5,
    .pre_allocate = false,
    .enabled = rc_enabled,
  };

  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, kNumCompactionThreads, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 768_MiB, kNumCompactionThreads, auto_compaction };

  f2_t::ColdIndexConfig cold_index_config{ table_size, 256_MiB, 0.6 };
  f2_t store{ table_size, 192_MiB, hot_fp,
              cold_index_config, 192_MiB, cold_fp,
              0.4, 0, rc_config, f2_compaction_config };

  uint32_t num_records = 100000; // ~800 MB of data
  store.StartSession();

  // Insert.
  for(size_t idx = 1; idx <= num_records; idx += 2) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(idx), Value(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true, 1);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }
  // Read both existent and non-existent keys
  for(size_t idx = 1; idx <= num_records; idx++) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      if (context->key().key % 2 == 0) {
        ASSERT_EQ(Status::NotFound, result);
      } else {
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->key().key, context->output.value );
      }
    };
    ReadContext<Key, Value> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::NotFound ||
                  result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_TRUE(idx % 2 == 1);
      ASSERT_EQ(idx, context.output.value);
    } else if (result != Status::Pending) {
      ASSERT_TRUE(idx % 2 == 0);
      ASSERT_EQ(Status::NotFound, result);
    }
    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Delete all inserted records
  for(size_t idx = 1; idx <= num_records; idx += 2) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Deletes do not go pending (in normal op)
    };
    DeleteContext<Key, Value> context{ Key(idx) };
    Status result = store.Delete(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true, 1);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }

  // Read all keys -- all should return NOT_FOUND
  for(size_t idx = 1; idx <= num_records; idx++) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(Status::NotFound, result);
    };
    ReadContext<Key, Value> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::NotFound || result == Status::Pending);
    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  if (!auto_compaction) {
    // perform cold-cold compaction
    store.CompactColdLog(store.cold_store.hlog.safe_read_only_address.control(), true, 1);
  }

  store.StopSession();
}

TEST_P(HotColdParameterizedTestParam, Rmw) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  std::string hot_fp, cold_fp;
  CreateNewLogDir(root_path, hot_fp, cold_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  bool auto_compaction = std::get<1>(args);
  bool rc_enabled = std::get<2>(args);

  log_info("\nTEST: Index size: %lu\tAuto compaction: %d\tUse Read Cache: %d",
            table_size, auto_compaction, rc_enabled);

  ReadCacheConfig rc_config {
    .mem_size = 256_MiB,
    .mutable_fraction = 0.5,
    .pre_allocate = false,
    .enabled = rc_enabled,
  };

  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, kNumCompactionThreads, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 768_MiB, kNumCompactionThreads, auto_compaction };

  f2_t::ColdIndexConfig cold_index_config{ table_size, 256_MiB, 0.6 };
  f2_t store{ table_size, 256_MiB, hot_fp,
              cold_index_config, 192_MiB, cold_fp,
              0.4, 0, rc_config, f2_compaction_config };

  uint32_t num_records = 20000; // ~160 MB of data
  store.StartSession();

  // Rmw initial (all records fit in-memory)
  for(size_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    RmwContext<Key, Value> context{ Key(idx), Value(5) };
    Status result = store.Rmw(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read. (all records in-memory)
  for(size_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    ReadContext<Key, Value> context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    ASSERT_EQ(context.output.value, 5);
  }

  // Rmw, increment by 1, 4 times (in random order)
  std::vector<uint64_t> keys;
  for (size_t idx = 1; idx <= num_records; idx++) {
    for (int t = 0; t < 4; ++t) {
      keys.push_back(idx);
    }
  }
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(42));

  for(size_t idx = 0; idx < 4 * num_records; ++idx) {
    uint64_t key = keys[idx];
    assert(1 <= key && key <= num_records);

    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
    };
    RmwContext<Key, Value> context{ Key(key), Value(1) };
    Status result = store.Rmw(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Read.
  for(size_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(result, Status::Ok);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(context->output.value, 9);
    };
    ReadContext<Key, Value> context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(context.output.value, 9);
    }
    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }

  // Rmw, decrement by 1, 8 times -- random order
  keys.clear();
  for (size_t idx = 1; idx <= num_records; idx++) {
    for (int t = 0; t < 8; ++t) {
      keys.push_back(idx);
    }
  }
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(42));

  for(size_t idx = 0; idx < 8 * num_records; ++idx) {
    uint64_t key = keys[idx];
    assert(1 <= key && key <= num_records);

    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
    };
    RmwContext<Key, Value> context{ Key(key), Value(-1) };
    Status result = store.Rmw(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Append non-existent keys and reshuffle
  for (size_t idx = 1; idx <= num_records; idx++) {
    keys.push_back(num_records + idx);
  }
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(42));
  // Read.
  for(size_t idx = 0; idx < keys.size(); ++idx) {
    uint64_t key = keys[idx];
    assert(1 <= key && key <= 2 * num_records);

    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      if (context->key().key > context->num_records) {
        ASSERT_EQ(Status::NotFound, result);
        return;
      }
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(context->output.value, 1);
    };
    ReadContext<Key, Value> context{ Key(key), num_records };
    Status result = store.Read(context, callback, 1);

    ASSERT_TRUE(result == Status::Ok || result == Status::Pending ||
                result == Status::NotFound);
    if (result == Status::Ok) {
      ASSERT_TRUE(1 <= key && key <= num_records);
      ASSERT_EQ(context.output.value, 1);
    } else if (result != Status::Pending) {
      ASSERT_EQ(Status::NotFound, result);
      ASSERT_TRUE(key > num_records);
    }

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }

  store.StopSession();
}


/// Inserts a bunch of records into a FASTER instance, and invokes the
/// compaction algorithm. Concurrent to the compaction, Upserts, RMWs, and Deletes
/// are performed in 1/4 of the keys, respectively. After compaction, it
/// checks that updated/RMW-ed keys have the new value, while deleted keys do not exist.
TEST_P(HotColdParameterizedTestParam, ConcurrentOps) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  std::string hot_fp, cold_fp;
  CreateNewLogDir(root_path, hot_fp, cold_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  bool auto_compaction = std::get<1>(args);
  bool rc_enabled = std::get<2>(args);

  log_info("\nTEST: Index size: %lu\tAuto compaction: %d\tUse Read Cache: %d",
            table_size, auto_compaction, rc_enabled);

  ReadCacheConfig rc_config {
    .mem_size = 256_MiB,
    .mutable_fraction = 0.5,
    .pre_allocate = false,
    .enabled = rc_enabled,
  };

  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, kNumCompactionThreads, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 768_MiB, kNumCompactionThreads, auto_compaction };

  f2_t::ColdIndexConfig cold_index_config{ table_size, 256_MiB, 0.6 };
  f2_t store{ table_size, 192_MiB, hot_fp,
              cold_index_config, 192_MiB, cold_fp,
              0.4, 0, rc_config, f2_compaction_config };

  static constexpr int num_records = 100000;

  store.StartSession();
  // Populate initial keys
  for (size_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, Value> context{Key(idx), Value(idx)};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  store.StopSession();

  // Thread that performs Upsert ops
  auto upsert_worker_func = [&store]() {
    store.StartSession();
    for (size_t idx = 4; idx <= num_records; idx += 4) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_TRUE(false);
      };
      UpsertContext<Key, Value> context{ Key(idx), Value(2 * idx) };
      Status result = store.Upsert(context, callback, idx / 4);
      ASSERT_EQ(Status::Ok, result);
    }
    store.CompletePending(true);
    store.StopSession();
  };
  // Thread that performs RMW ops
  auto rmw_worker_func = [&store]() {
    store.StartSession();
    for (size_t idx = 1; idx <= num_records; idx += 4) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwContext<Key, Value>> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
      };
      RmwContext<Key, Value> context{ Key(idx), Value(1) };
      Status result = store.Rmw(context, callback, idx / 4);
      ASSERT_TRUE(result == Status::Ok || result == Status::Pending);

      if (idx % kCompletePendingInterval == 0) {
        store.CompletePending(false);
      }
    }
    store.CompletePending(true);
    store.StopSession();
  };
  // Thread that performs Delete ops
  auto delete_worker_func = [&store]() {
    store.StartSession();
    for (size_t idx = 2; idx <= num_records; idx += 4) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_TRUE(false);
      };
      DeleteContext<Key, Value> context{ Key(idx) };
      Status result = store.Delete(context, callback, idx / 4);
      ASSERT_EQ(Status::Ok, result);
    }
    store.CompletePending(true);
    store.StopSession();
  };
  // launch threads
  std::thread upset_worker (upsert_worker_func);
  std::thread rmw_worker (rmw_worker_func);
  std::thread delete_worker (delete_worker_func);
  upset_worker.join();
  rmw_worker.join();
  delete_worker.join();

  store.StartSession();

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }

  // Perform reads for all keys (and more non-existent ones) in random order
  std::vector<uint64_t> keys (num_records + num_records / 4);
  std::iota(keys.begin(), keys.end(), 1);
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(42));

  for (size_t idx = 0; idx < keys.size(); ++idx) {
    uint64_t key = keys[idx];

    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext<Key, Value>> context(ctxt);
      if (context->key().key > num_records) {
        // non-existent key
        ASSERT_EQ(Status::NotFound, result);
        return;
      }
      ASSERT_TRUE(context->key().key > 0 &&
                  context->key().key <= context->num_records);
      if (context->key().key % 4 == 0) {
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->key().key, context->output.value / 2);
      } else if (context->key().key % 4 == 1) {
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->key().key + 1, context->output.value);
      } else if (context->key().key % 4 == 2) {
        ASSERT_EQ(Status::NotFound, result);
      } else { // key % 4 == 3
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->key().key, context->output.value);
      }
    };
    ReadContext<Key, Value> context{ Key(key), num_records };
    Status result = store.Read(context, callback, 1);
    EXPECT_TRUE(result == Status::Ok || result == Status::NotFound ||
                result == Status::Pending);

    if (result == Status::Ok) {
      assert(key >= 1 && key <= num_records);
      if (key % 4 == 0) { // upsert-ed
        ASSERT_EQ(key, context.output.value / 2);
      } else if (key % 4 == 1) { // RMWed
        ASSERT_EQ(key + 1, context.output.value);
      } else if (key % 4 == 3) { // unmodified
        ASSERT_EQ(key, context.output.value);
      } else {
        ASSERT_TRUE(false);
      }
    } else if (result == Status::NotFound) {
      ASSERT_TRUE(key % 4 == 2 || key > num_records); // deleted or non-existing
    }

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.StopSession();

}

TEST_P(HotColdParameterizedTestParam, VariableLengthKey) {
  using Key = VariableSizeKey;
  using ShallowKey = VariableSizeShallowKey;
  using Value = LargeValue;

  class UpsertContext : public IAsyncContext {
   public:
    // Typedef required for *PendingContext instances
    // but compiler throws warnings
    [[maybe_unused]] typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint32_t* key, uint32_t key_length, value_t value)
      : key_{ key, key_length }
      , value_{ value } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext& other)
      : key_{ other.key_ }
      , value_{ other.value_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const ShallowKey& key() const {
      return key_;
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.value = value_.value;
    }
    inline bool PutAtomic(Value& value) {
      value.atomic_value.store(value_.value);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    ShallowKey key_;
    Value value_;
  };

  class ReadContext : public IAsyncContext {
   public:
    // Typedef required for *PendingContext instances
    // but compiler throws warnings
    [[maybe_unused]] typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint32_t* key, uint32_t key_length)
            : key_{ key, key_length } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
            : key_{ other.key_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const ShallowKey& key() const {
      return key_;
    }

    inline void Get(const value_t& value) {
      output.value = value.value;
    }
    inline void GetAtomic(const value_t& value) {
      output.value = value.atomic_value.load();
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    ShallowKey key_;
   public:
    value_t output;
  };

  class RmwContext : public IAsyncContext {
   public:
    // Typedef required for *PendingContext instances
    // but compiler throws warnings
    [[maybe_unused]] typedef Key key_t;
    typedef Value value_t;

    RmwContext(uint32_t* key, uint32_t key_length, value_t incr)
      : key_{ key, key_length }
      , incr_{ incr } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , incr_{ other.incr_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const ShallowKey& key() const {
      return key_;
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }
    inline static constexpr uint32_t value_size(const value_t& old_value) {
      return sizeof(value_t);
    }
    inline void RmwInitial(value_t& value) {
      value.value = incr_.value;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.value = old_value.value + incr_.value;
    }
    inline bool RmwAtomic(value_t& value) {
      value.atomic_value.fetch_add(incr_.value);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    ShallowKey key_;
    value_t incr_;
  };

  class DeleteContext : public IAsyncContext {
   public:
    // Typedef required for *PendingContext instances
    // but compiler throws warnings
    [[maybe_unused]] typedef Key key_t;
    typedef Value value_t;

    explicit DeleteContext(uint32_t* key, uint32_t key_length)
      : key_{ key, key_length }
    {}

    /// Copy (and deep-copy) constructor.
    DeleteContext(const DeleteContext& other)
            : key_{ other.key_ } {
    }
    /// The implicit and explicit interfaces require a key() accessor.
    inline const ShallowKey& key() const {
      return key_;
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   private:
    ShallowKey key_;
  };

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  std::string hot_fp, cold_fp;
  CreateNewLogDir(root_path, hot_fp, cold_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  bool auto_compaction = std::get<1>(args);
  bool rc_enabled = std::get<2>(args);

  log_info("\nTEST: Index size: %lu\tAuto compaction: %d\tUse Read Cache: %d",
            table_size, auto_compaction, rc_enabled);

  ReadCacheConfig rc_config {
    .mem_size = 256_MiB,
    .mutable_fraction = 0.5,
    .pre_allocate = false,
    .enabled = rc_enabled,
  };

  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, kNumCompactionThreads, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 768_MiB, kNumCompactionThreads, auto_compaction };

  f2_t::ColdIndexConfig cold_index_config{ table_size, 256_MiB, 0.6 };
  f2_t store{ table_size, 192_MiB, hot_fp,
              cold_index_config, 192_MiB, cold_fp,
              0.4, 0, rc_config, f2_compaction_config };

  uint32_t num_records = 17500;

  store.StartSession();

  // Insert.
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // Writes do not go pending in normal operation
      ASSERT_TRUE(false);
    };
    // Create the key as a variable length array
    uint32_t* key = (uint32_t*) malloc(idx * sizeof(uint32_t));
    for (uint32_t j = 0; j < idx; ++j) {
      key[j] = j;
    }

    UpsertContext context{ key, idx, idx};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    free(key);
  }
  // Read.
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext> context{ ctxt };

      ASSERT_EQ(context->output.value, context->key().key_length_);
      for (size_t j = 0; j < context->key().key_length_; ++j) {
        ASSERT_EQ(context->key().key_data_[j], j);
      }
      free(context->key().key_data_);
    };
    // Create the key as a variable length array
    uint32_t* key = (uint32_t*) malloc(idx * sizeof(uint32_t));
    for (uint32_t j = 0; j < idx; ++j) {
      key[j] = j;
    }

    ReadContext context{ key, idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(idx, context.output.value);
      for (uint32_t j = 0; j < context.output.value; ++j) {
        ASSERT_EQ(context.key().key_data_[j], j);
      }
      free(key);
    }
  }

  if (auto_compaction) {
    store.CompletePending(true);
  }

  // Update one fourth of the entries
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    if (idx % 4 == 0) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // Writes do not go pending in normal operation
        ASSERT_TRUE(false);
      };
      // Create the key as a variable length array
      uint32_t* key = (uint32_t*) malloc(idx * sizeof(uint32_t));
      for (uint32_t j = 0; j < idx; ++j) {
        key[j] = j;
      }

      UpsertContext context{ key, idx, 2*idx };
      Status result = store.Upsert(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
      free(key);
    }
  }
  // Delete another one fourth
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    if (idx % 4 == 1) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_TRUE(false); // deletes do not go pending
      };
      // Create the key as a variable length array
      uint32_t* key = (uint32_t*) malloc(idx * sizeof(uint32_t));
      for (uint32_t j = 0; j < idx; ++j) {
        key[j] = j;
      }

      DeleteContext context{ key, idx };
      Status result = store.Delete(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
      free(key);
    }
  }
  // RMW another one fourth
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    if (idx % 4 == 2) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_EQ(result, Status::Ok);
        // free memory
        CallbackContext<RmwContext> context{ ctxt };
        free(context->key().key_data_);
      };
      // Create the key as a variable length array
      uint32_t* key = (uint32_t*) malloc(idx * sizeof(uint32_t));
      for (uint32_t j = 0; j < idx; ++j) {
        key[j] = j;
      }

      RmwContext context{ key, idx, idx };
      Status result = store.Rmw(context, callback, 1);
      ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
      if (result == Status::Ok) {
        free(key);
      }
    }
  }
  store.CompletePending(true);

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }

  // Read again.
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context{ ctxt };
      // check request result & value
      if (context->key().key_length_ % 4 == 0) { // Upsert
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->output.value, 2 * context->key().key_length_);
      } else if (context->key().key_length_ % 4 == 1) { // Delete
        ASSERT_EQ(Status::NotFound, result);
      } else if (context->key().key_length_ % 4 == 2) { // Rmw
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->output.value, 2 * context->key().key_length_);
      } else { // key_length_ % 4 == 3 (Intact)
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->output.value, context->key().key_length_);
      }
      // verify that key match the requested key
      for (uint32_t j = 0; j < context->key().key_length_; ++j) {
        ASSERT_EQ(context->key().key_data_[j], j);
      }
      free(context->key().key_data_);
    };
    // Create the key as a variable length array
    uint32_t* key = (uint32_t*) malloc(idx * sizeof(uint32_t));
    for (uint32_t j = 0; j < idx; ++j) {
      key[j] = j;
    }

    ReadContext context{ key, idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending ||
                result == Status::NotFound);
    if (result == Status::Ok) {
      if (idx % 4 == 0) { // Upsert
        ASSERT_EQ(2 * idx, context.output.value);
      } else if (idx % 4 == 2) { // RMW
        ASSERT_EQ(2 * idx, context.output.value);
      } else if (idx % 4 == 3) { // Intact
        ASSERT_EQ(idx, context.output.value);
      }
      else ASSERT_TRUE(false);
      for (uint32_t j = 0; j < context.key().key_length_; ++j) {
        ASSERT_EQ(context.key().key_data_[j], j);
      }
      free(key);
    }
    else if (result == Status::NotFound) { // Deleted
      ASSERT_TRUE(idx % 4 == 1);
      free(key);
    }
  }
  store.CompletePending(true);
  store.StopSession();

}

TEST_P(HotColdParameterizedTestParam, VariableLengthValue) {
  using Key = FixedSizeKey<uint32_t>;

  class UpsertContextVLV;
  class ReadContextVLV;

  class Value {
   public:
    Value()
      : gen_lock_{ 0 }
      , size_{ 0 }
      , length_{ 0 } {
    }

    inline uint32_t size() const {
      return size_;
    }

    friend class UpsertContextVLV;
    friend class ReadContextVLV;

   private:
    AtomicGenLock gen_lock_;
    uint32_t size_;
    uint32_t length_;

    inline const uint8_t* buffer() const {
      return reinterpret_cast<const uint8_t*>(this + 1);
    }
    inline uint8_t* buffer() {
      return reinterpret_cast<uint8_t*>(this + 1);
    }
  };

  class UpsertContextVLV : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContextVLV(uint32_t key, uint32_t *value, uint32_t value_length)
      : key_{ key }
      , value_{ value }
      , value_length_{ value_length } {
    }
    /// Copy (and deep-copy) constructor.
    UpsertContextVLV(const UpsertContextVLV& other)
      : key_{ other.key_ }
      , value_{ other.value_ }
      , value_length_{ other.value_length_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + value_length_ * sizeof(uint32_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t& value) {
      value.gen_lock_.store(0);
      value.size_ = value_size();
      value.length_ = value_length_;
      std::memcpy(value.buffer(), value_, value_length_ * sizeof(uint32_t));
    }
    inline bool PutAtomic(value_t& value) {
      bool replaced;
      while(!value.gen_lock_.try_lock(replaced) && !replaced) {
        std::this_thread::yield();
      }
      if(replaced) {
        // Some other thread replaced this record.
        return false;
      }
      if(value.size_ < value_size()) {
        // Current value is too small for in-place update.
        value.gen_lock_.unlock(true);
        return false;
      }
      // In-place update overwrites length and buffer, but not size.
      value.length_ = value_length_;
      std::memcpy(value.buffer(), value_, value_length_ * sizeof(uint32_t));
      value.gen_lock_.unlock(false);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    key_t key_;
    uint32_t* value_;
    uint32_t value_length_;
  };

  class ReadContextVLV : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContextVLV(uint32_t key)
      : key_{ key }
      , output{ nullptr }
      , output_length{ 0 } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContextVLV(const ReadContextVLV& other)
      : key_{ other.key_ }
      , output{ other.output }
      , output_length{ other.output_length }
    { }

    ~ReadContextVLV() {
      if (output != nullptr) {
        free(output);
      }
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline void Get(const value_t& value) {
      output_length = value.length_;
      if (output == nullptr) {
        output = (uint32_t*) malloc(output_length * sizeof(uint32_t));
      }
      std::memcpy(output, value.buffer(), output_length * sizeof(uint32_t));
    }
    inline void GetAtomic(const value_t& value) {
      GenLock before, after;
      do {
        before = value.gen_lock_.load();
        output_length = value.length_;
        if (output == nullptr) {
          output = (uint32_t*) malloc(output_length * sizeof(uint32_t));
        }
        std::memcpy(output, value.buffer(), output_length * sizeof(uint32_t));
        after = value.gen_lock_.load();
      } while(before.gen_number != after.gen_number);
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    key_t key_;
   public:
    uint32_t *output;
    uint32_t output_length;
  };

  using UpsertContext = UpsertContextVLV;
  using ReadContext = ReadContextVLV;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  std::string hot_fp, cold_fp;
  CreateNewLogDir(root_path, hot_fp, cold_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  bool auto_compaction = std::get<1>(args);
  bool rc_enabled = std::get<2>(args);

  log_info("\nTEST: Index size: %lu\tAuto compaction: %d\tUse Read Cache: %d",
            table_size, auto_compaction, rc_enabled);

  ReadCacheConfig rc_config {
    .mem_size = 256_MiB,
    .mutable_fraction = 0.5,
    .pre_allocate = false,
    .enabled = rc_enabled,
  };

  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, kNumCompactionThreads, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 768_MiB, kNumCompactionThreads, auto_compaction };

  f2_t::ColdIndexConfig cold_index_config{ table_size, 256_MiB, 0.6 };
  f2_t store{ table_size, 192_MiB, hot_fp,
              cold_index_config, 192_MiB, cold_fp,
              0.4, 0, rc_config, f2_compaction_config };

  uint32_t num_records = 17500;

  store.StartSession();

  // Insert.
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // upserts do not go pending
    };
    // Create the value as a variable length array
    uint32_t* value = (uint32_t*) malloc(idx * sizeof(uint32_t));
    for (uint32_t j = 0; j < idx; ++j) {
      value[j] = idx;
    }

    UpsertContext context{ idx, value, idx};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    free(value);
  }
  // Read.
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);

      ASSERT_EQ(context->output_length, context->key().key);
      for (uint32_t j = 0; j < context->output_length; ++j) {
        if (context->key().key % 2 == 0) {
          // Either old or updated record
          ASSERT_TRUE(
            context->output[j] == context->key().key ||
            context->output[j] == 2 * context->key().key);
        }
        else {
          // Only old record
          ASSERT_EQ(context->output[j], context->key().key);
        }
      }
    };

    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      for (uint32_t j = 0; j < idx; ++j) {
        ASSERT_EQ(context.output[j], idx);
      }
    }
  }
  // Update half
  for(uint32_t idx = 1; idx <= num_records; ++idx) {
    if (idx % 2 == 0) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_TRUE(false); // upserts do not go pending
      };
      // Create the value as a variable length array
      uint32_t* value = (uint32_t*) malloc(idx * sizeof(uint32_t));
      for (uint32_t j = 0; j < idx; ++j) {
        value[j] = 2 * idx;
      }

      UpsertContext context{ idx, value, idx }; // double the value_id
      Status result = store.Upsert(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
      free(value);
    }
  }
  store.CompletePending(true);

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }

  // Read again.
  for(uint32_t idx = 1; idx <= num_records ; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext> context{ ctxt };

      ASSERT_EQ(context->output_length, context->key().key);
      uint32_t value_id = (context->key().key % 2 == 0)
                              ? 2 * context->key().key
                              : context->key().key;
      for (uint32_t j = 0; j < context->output_length; ++j) {
        ASSERT_EQ(context->output[j], value_id);
      }
    };

    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      uint32_t value_id = (idx % 2 == 0) ? 2*idx : idx;
      for (uint32_t j = 0; j < idx; ++j) {
        ASSERT_EQ(context.output[j], value_id);
      }
    }
  }
  store.StopSession();

}


int main(int argc, char** argv) {
#ifndef _WIN32
  rlim_t new_stack_size = 64_MiB;
  if (!set_stack_size(new_stack_size)) {
    log_warn("Could not set stack size to %lu bytes", new_stack_size);
  }
#endif
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  if (ret == 0) {
    RemoveDir(root_path);
  }
  return ret;
}
