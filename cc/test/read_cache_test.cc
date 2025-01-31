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

// FASTER: <# hash index buckets, num_threads, random_seed>
using faster_param_types = std::tuple<uint32_t, int, uint64_t>;
// F2: <# hash index buckets, num_threads, random_seed, auto_compaction>
using f2_param_types = std::tuple<uint32_t, int, uint64_t, bool>;

// ============================================================================
/// UpsertReadTest
// ============================================================================
class FASTERUpsertReadParameterizedTestParam : public ::testing::TestWithParam<faster_param_types> {
};
INSTANTIATE_TEST_CASE_P(
  FASTERReadCacheTests,
  FASTERUpsertReadParameterizedTestParam,
  ::testing::Values(
    // Small hash index
    std::make_tuple(2048, 1, 42),
    std::make_tuple(2048, 4, 42),
    // Large hash index
    std::make_tuple((1 << 22), 1, 42),
    std::make_tuple((1 << 22), 4, 42)
  )
);
class F2UpsertReadParameterizedTestParam : public ::testing::TestWithParam<f2_param_types> {
};
INSTANTIATE_TEST_CASE_P(
  F2ReadCacheTests,
  F2UpsertReadParameterizedTestParam,
  ::testing::Values(
    // Small hash index
    std::make_tuple(2048, 1, 42, false),
    std::make_tuple(2048, 4, 42, false),
    // NOTE: skip for now -- Hash index GC, invalidates RC entries
    //std::make_tuple(2048, 1, 42, true),
    //std::make_tuple(2048, 4, 42, true),
    // Large hash index
    std::make_tuple((1 << 22), 1, 42, false),
    std::make_tuple((1 << 22), 4, 42, false)
    // NOTE: skip for now -- Hash index GC, invalidates RC entries
    //std::make_tuple((1 << 22), 1, 42, true),
    //std::make_tuple((1 << 22), 4, 42, true)
  )
);

// ============================================================================
/// InsertAbortTest
// ============================================================================
/*
class FASTERInsertAbortParameterizedTestParam : public ::testing::TestWithParam<faster_param_types> {
};
INSTANTIATE_TEST_CASE_P(
  FASTERReadCacheTests,
  FASTERInsertAbortParameterizedTestParam,
  ::testing::Values(
    std::make_tuple(1, 1, 42),
    std::make_tuple(1, 4, 42)
  )
);
*/

static std::string root_path{ "test_store/" };
static constexpr uint64_t kCompletePendingInterval = 128;

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

template<class S, bool HasColdLog>
struct StoreCompactionHelper {
  static inline void CompactHotLog(S& store) {
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
    ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
  }
};
template<class S>
struct StoreCompactionHelper<S, false> {
  static inline void CompactHotLog(S& store) {
    // do nothing for FASTER
  }
};

// ============================================================================
/// UpsertReadTest
// ============================================================================
template <class S, class K, class V, bool HasColdLog>
void UpsertReadTest(S& store, uint32_t num_threads,
                    uint32_t random_seed, bool auto_compaction) {
  using Key = K;
  using Value = V;
  store.StartSession();

  // Two non-overlapping set of keys
  uint32_t start_key_a = 1337;
  uint32_t num_records_a = 50'000; // ~400 MB of data

  uint32_t start_key_b = num_records_a * 5;
  uint32_t num_records_b = 100'000; // ~800 MB of data
  assert(start_key_a + num_records_a < start_key_b);

  std::vector<uint64_t> key_set_a(num_records_a);
  std::vector<uint64_t> key_set_b(num_records_b);
  std::iota(key_set_a.begin(), key_set_a.end(), start_key_a);
  std::iota(key_set_b.begin(), key_set_b.end(), start_key_b);

  // Insert set A
  std::shuffle(key_set_a.begin(), key_set_a.end(), std::default_random_engine(random_seed));
  log_debug("Inserting set A...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(key), Value(key) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  store.CompletePending(true);

  // Insert set B -- this evicts set A to disk
  std::shuffle(key_set_b.begin(), key_set_b.end(), std::default_random_engine(random_seed));
  log_debug("Inserting set B (this evicts set A)...");
  for(auto& key : key_set_b) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(key), Value(key) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  store.CompletePending(true);

  if (!auto_compaction) {
    // perform hot-cold compaction for F2
    StoreCompactionHelper<S, HasColdLog>::CompactHotLog(store);
  }

  // Read set A
  //  -> This should trigger all disk accesses
  //  -> It should also insert all set A records to read-cache
  std::shuffle(key_set_a.begin(), key_set_a.end(),
               std::default_random_engine(random_seed));

  log_debug("Reading set A (set A should be on-disk)...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(context->key().key, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(key) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Pending
                || (auto_compaction && result == Status::Ok)); // for F2
    if (result == Status::Ok) {
      ASSERT_EQ(key, context.output.value);
    }

    if (key % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Re-Read set A
  //  -> This should trigger all reads from read-cache
  // NOTE: RC is best-effort so not all entries would be in RC
  std::shuffle(key_set_a.begin(), key_set_a.end(),
               std::default_random_engine(random_seed));

  uint64_t num_rc_entries = 0;
  log_debug("Re-reading set A (most of set A should be in RC)...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(context->key().key, context->output.value );
    };

    ReadContext<Key, Value> context{ Key(key) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(key, context.output.value);
      ++num_rc_entries;
    }
    if (key % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);
  // >= 90% of entries in RC
  ASSERT_TRUE((num_records_a - num_rc_entries) < (num_rc_entries / 10));

  // Read set B
  //   -> Read-cache entries should *not* have altered set B values
  std::shuffle(key_set_b.begin(), key_set_b.end(),
               std::default_random_engine(random_seed));

  log_debug("Reading set B (set B should be unaltered)...");
  for(auto& key : key_set_b) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(context->key().key, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(key) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(key, context.output.value);
    }
    if (key % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Update set A with new values
  std::shuffle(key_set_a.begin(), key_set_a.end(),
               std::default_random_engine(random_seed));

  log_debug("Updating set A (w/ new values)...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(key), Value(2 * key) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  store.CompletePending(true);

  if (!auto_compaction) {
    // perform hot-cold compaction for F2
    StoreCompactionHelper<S, HasColdLog>::CompactHotLog(store);
  }

  // Re-Read set A
  //  -> This should read *new* values from memory or disk
  //  -> Keep track the disk-resident records; these should be inserted to read-cache
  std::shuffle(key_set_a.begin(), key_set_a.end(),
               std::default_random_engine(random_seed));
  std::unordered_set<uint64_t> rc_records_a;

  log_debug("Re-reading set A (should have new values)...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(2 * context->key().key, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(key) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(2 * key, context.output.value);
    } else {
      rc_records_a.insert(key);
    }
    if (key % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Read read cache-resident subset A
  //  -> This should read *new* values from read-cache
  num_rc_entries = 0;

  log_debug("Re-reading disk-resident set A (should now be in RC)...");
  for(auto& key : rc_records_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(2 * context->key().key, context->output.value );
    };

    ReadContext<Key, Value> context{ Key(key) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(2 * key, context.output.value);
      ++num_rc_entries;
    }
    if (key % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);
  // >= 90% of entries in RC
  ASSERT_TRUE((rc_records_a.size() - num_rc_entries) < (rc_records_a.size() / 10));

  store.StopSession();
}

TEST_P(FASTERUpsertReadParameterizedTestParam, UpsertRead) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef FasterKv<Key, Value, disk_t> faster_t;

  std::string log_fp;
  CreateNewLogDir(root_path, log_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  uint32_t num_threads = std::get<1>(args);
  uint64_t random_seed = std::get<2>(args);
  bool auto_compaction = false;
  log_info("\nTEST: [FASTER]\t Index size: %lu\t Num-threads: %lu\tRandom Seed: %lu",
           table_size, num_threads, random_seed);

  ReadCacheConfig rc_config {
    .mem_size = 512_MiB,
    .mutable_fraction = 0.81,
    .pre_allocate = false,
    .enabled = true,
  };
  HlogCompactionConfig compaction_config{
    250ms, 0.9, 0.1, 256_MiB, 1_GiB, 4, auto_compaction };
  faster_t store{ table_size, 192_MiB, log_fp, 0.4,
                  rc_config, compaction_config };

  // Run test
  UpsertReadTest<faster_t, Key, Value, false>(store, num_threads, random_seed, false);

  RemoveDir(root_path);
}

TEST_P(F2UpsertReadParameterizedTestParam, UpsertRead) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  std::string hot_fp, cold_fp;
  CreateNewLogDir(root_path, hot_fp, cold_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  uint32_t num_threads = std::get<1>(args);
  uint64_t random_seed = std::get<2>(args);
  bool auto_compaction = std::get<3>(args);
  log_info("\nTEST: [F2]\t Index size: %lu\t Num-threads: %lu\tRandom Seed: %lu"
           "\tAuto-compaction: %s", table_size, num_threads, random_seed, auto_compaction ? "TRUE": "FALSE");

  ReadCacheConfig rc_config {
    .mem_size = 512_MiB,
    .mutable_fraction = 0.81,
    .pre_allocate = false,
    .enabled = true,
  };
  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, 4, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 768_MiB, 4, false };

  f2_t::ColdIndexConfig cold_index_config{ table_size, 256_MiB, 0.6 };
  f2_t store{ table_size, 192_MiB, hot_fp,
              cold_index_config, 192_MiB, cold_fp,
              0.4, 0, rc_config, f2_compaction_config };

  // Run test
  UpsertReadTest<f2_t, Key, Value, true>(store, num_threads, random_seed, auto_compaction);

  RemoveDir(root_path);
}

/* Comment out test for -- test is flanky */
// ============================================================================
/// InsertAbortTest
// ============================================================================
/*
template <class S, class K, class V, bool HasColdLog>
void InsertAbortTest(S& store, uint32_t num_threads,
                    uint32_t random_seed, bool auto_compaction) {
  using Key = K;
  using Value = V;
  store.StartSession();

  // Two non-overlaping set of keys
  uint32_t start_key_a = 1;
  uint32_t num_records_a = 10'000; // ~80 MB of data

  uint32_t start_key_b = start_key_a + num_records_a + 1;
  uint32_t num_records_b = 20'000; // ~160 MB of data
  assert(start_key_a + num_records_a < start_key_b);

  uint32_t start_key_c = start_key_b + num_records_b + 1;
  uint32_t num_records_c = 100'000; // ~800 MB of data
  assert(start_key_b + num_records_b < start_key_c);

  std::vector<uint64_t> key_set_a(num_records_a);
  std::vector<uint64_t> key_set_b(num_records_b);
  std::vector<uint64_t> key_set_c(num_records_c);
  std::iota(key_set_a.begin(), key_set_a.end(), start_key_a);
  std::iota(key_set_b.begin(), key_set_b.end(), start_key_b);
  std::iota(key_set_c.begin(), key_set_c.end(), start_key_c);

  // Insert set A
  std::shuffle(key_set_a.begin(), key_set_a.end(), std::default_random_engine(random_seed));
  log_debug("Inserting set A...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(key), Value(key) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  store.CompletePending(true);

  // Insert set B -- this moves set A to disk (or to cold-log disk in F2)
  std::shuffle(key_set_b.begin(), key_set_b.end(), std::default_random_engine(random_seed));
  log_debug("Inserting set B (this forces set A to disk)...");
  for(auto& key : key_set_b) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(key), Value(key) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  store.CompletePending(true);

  if (!auto_compaction) {
    // perform hot-cold compaction for F2
    StoreCompactionHelper<S, HasColdLog>::CompactHotLog(store);
  }

  // Issue Read requests for set A
  //  -> This should trigger all disk accesses
  //  -> It does *not* complete reads; just populates the contexts with HI expected entries
  std::shuffle(key_set_a.begin(), key_set_a.end(),
               std::default_random_engine(random_seed));

  log_debug("Issuing Read reqs for set A (set A should be on-disk)...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(context->key().key, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(key) };
    Status result = store.Read(context, callback, 1);
    assert(result == Status::Pending);
    ASSERT_TRUE(result == Status::Pending);
  }

  // Insert set C -- this moves set B to disk
  //   -> This modifies all HI entries with new values
  std::shuffle(key_set_c.begin(), key_set_c.end(), std::default_random_engine(random_seed));
  log_debug("Inserting set C (this forces set B to disk)...");
  for(auto& key : key_set_c) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false); // Upsert do not go pending (in normal op)
    };
    UpsertContext<Key, Value> context{ Key(key), Value(key) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  if (!auto_compaction) {
    // perform hot-cold compaction for F2
    StoreCompactionHelper<S, HasColdLog>::CompactHotLog(store);
  }

  // Complete all pending Read requests for set A
  //   -> No entry should be included in read-cache because HI expected entries were modified
  log_debug("Complete all pending Read requests for set A....");
  store.CompletePending(true);

  // Read-cache size should 0
  // [TODO]

  // Re-read set A
  //  -> This should trigger all reads from disk (!!!)
  //  -> Most entries will be added to RC
  std::shuffle(key_set_a.begin(), key_set_a.end(),
               std::default_random_engine(random_seed));
  std::unordered_map<uint16_t, uint64_t> rc_entries;

  log_debug("Reading set A (set A should *not* be in RC)...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(context->key().key, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(key) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Pending);
    store.CompletePending(true);

    uint16_t tag = HotLogHashIndexDefinition::key_hash_t{ context.key().GetHash() }.tag();
    rc_entries[tag] = key;
  }

  // Re-Read set A (same order as before!)
  //  -> This should read orig values from RC now (or disk for hash-collisions)
  log_debug("Re-reading set A (set A should be in RC)...");
  for(auto& key : key_set_a) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext<Key, Value>> context{ ctxt };
      ASSERT_EQ(context->key().key, context->output.value );
    };
    ReadContext<Key, Value> context{ Key(key) };
    Status result = store.Read(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);

    uint16_t tag = HotLogHashIndexDefinition::key_hash_t{ context.key().GetHash() }.tag();
    auto it = rc_entries.find(tag);
    bool present_in_rc = (it != rc_entries.end() && it->second == key);
    store.CompletePending(true);

    if (result == Status::Ok) {
      ASSERT_EQ(key, context.output.value);
      ASSERT_TRUE(present_in_rc);
    } else {
      ASSERT_TRUE(!present_in_rc);
      rc_entries[tag] = key;
    }
  }

  store.StopSession();
}

TEST_P(FASTERInsertAbortParameterizedTestParam, InsertAbort) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef FasterKv<Key, Value, disk_t> faster_t;

  std::string log_fp;
  CreateNewLogDir(root_path, log_fp);

  auto args = GetParam();
  uint32_t table_size  = std::get<0>(args);
  uint32_t num_threads = std::get<1>(args);
  uint64_t random_seed = std::get<2>(args);
  bool auto_compaction = false;
  log_info("\nTEST: [FASTER]\t Index size: %lu\t Num-threads: %lu\tRandom Seed: %lu",
           table_size, num_threads, random_seed);

  ReadCacheConfig rc_config {
    .mem_size = 512_MiB,
    .mutable_fraction = 0.81,
    .pre_allocate = false,
    .enabled = true,
  };
  HlogCompactionConfig compaction_config{
    250ms, 0.9, 0.1, 256_MiB, 1_GiB, 4, auto_compaction };
  faster_t store{ table_size, 192_MiB, log_fp, 0.4,
                  rc_config, compaction_config };

  // Run test
  InsertAbortTest<faster_t, Key, Value, false>(store, num_threads, random_seed, false);

  RemoveDir(root_path);
}
*/

int main(int argc, char** argv) {
  rlim_t new_stack_size = 64_MiB;
  if (!set_stack_size(new_stack_size)) {
    log_warn("Could not set stack size to %lu bytes", new_stack_size);
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
