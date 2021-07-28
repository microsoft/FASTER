// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <algorithm>
#include <numeric>
#include <random>
#include <vector>

#include "gtest/gtest.h"

#include "core/faster_hc.h"
#include "core/log_scan.h"

#include "device/null_disk.h"

#include "test_types.h"

using namespace FASTER::core;

// Keys
using FASTER::test::FixedSizeKey;
using FASTER::test::VariableSizeKey;
using FASTER::test::VariableSizeShallowKey;
// Values
using FASTER::test::SimpleAtomicValue;
using FASTER::test::SimpleAtomicMediumValue;
using FASTER::test::SimpleAtomicLargeValue;
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
// bool value indicates whether or not to perform automatic or manual compaction
class HotColdParameterizedTestFixture : public ::testing::TestWithParam<bool> {
};
INSTANTIATE_TEST_CASE_P(
  HotColdTests,
  HotColdParameterizedTestFixture,
  ::testing::Values(false, true)
);


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

// Megabyte & Gigabyte literal helpers
constexpr uint64_t operator""_MiB(unsigned long long const x ) {
  return static_cast<uint64_t>(x * (1 << 20));
}
constexpr uint64_t operator""_GiB(unsigned long long const x ) {
  return static_cast<uint64_t>(x * (1 << 30));
}
// Dir creation / deletion helpers
void CreateLogDirs (std::string root, std::string& hot_log_dir, std::string& cold_log_dir) {
  assert(!root.empty());
  std::experimental::filesystem::remove_all(root);
  std::experimental::filesystem::create_directories(root);

  if (root.back() != FASTER::environment::kPathSeparator[0]) {
    root += FASTER::environment::kPathSeparator;
  }
  hot_log_dir = root + "hot_log";
  cold_log_dir = root + "cold_log";

  assert(std::experimental::filesystem::create_directories(hot_log_dir));
  assert(std::experimental::filesystem::create_directories(cold_log_dir));
}
void RemoveDirs (const std::string& root, const std::string& hot_log_dir, const std::string& cold_log_dir) {
  assert(std::experimental::filesystem::remove_all(hot_log_dir));
  assert(std::experimental::filesystem::remove_all(cold_log_dir));
  assert(std::experimental::filesystem::remove_all(root));
}


TEST_P(HotColdParameterizedTestFixture, UpsertRead) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef FasterKvHC<Key, Value, disk_t> faster_hc_t;

  std::string hot_fp, cold_fp;
  CreateLogDirs("temp_store", hot_fp, cold_fp);

  bool auto_compaction = GetParam();
  faster_hc_t store { 1_GiB, 0.25,            // 256 MB hot log, 768 cold log
                      192_MiB, 1024, hot_fp,  // [hot]  192 MB mem size, 512 entries in hash index
                      192_MiB, 2048, cold_fp, // [cold] 192 MB mem size, 512 entries in hash index
                      0.4, 0,                 // 64 MB mutable hot log, minimum mutable cold (i.e. 64 MB)
                      auto_compaction };      // automatic or manual compaction

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
    if (idx % 25 == 0) {
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
  }

  if (!auto_compaction) {
    // perform hot-cold compaction
    uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
    store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
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
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      if (key % 2 == 1) ASSERT_EQ(key, context.output.value);
      else ASSERT_EQ(key * 2, context.output.value);
    }
    else if (result != Status::Pending) {
      ASSERT_EQ(Status::NotFound, result);
      ASSERT_TRUE(key > num_records);
    }

    if (idx % 25 == 0) {
      store.CompletePending(false);
    }
  }

  store.StopSession();
  RemoveDirs("temp_store", hot_fp, cold_fp);
}

TEST_P(HotColdParameterizedTestFixture, Rmw) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef FasterKvHC<Key, Value, disk_t> faster_hc_t;

  std::string hot_fp, cold_fp;
  CreateLogDirs("temp_store", hot_fp, cold_fp);

  bool auto_compaction = GetParam();
  faster_hc_t store { 1_GiB, 0.25,            // 256 MB hot log, 768 cold log
                      192_MiB, 2048, hot_fp,  // [hot]  192 MB mem size, 512 entries in hash index
                      192_MiB, 2048, cold_fp, // [cold] 192 MB mem size, 512 entries in hash index
                      0.4, 0,                 // 64 MB mutable hot log, minimum mutable cold (i.e. 64 MB)
                      auto_compaction };      // automatic or manual compaction

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
      ASSERT_EQ(Status::Ok, result);
    };
    RmwContext<Key, Value> context{ Key(key), Value(1) };
    Status result = store.Rmw(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
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
    if (idx % 25 == 0) {
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
      ASSERT_EQ(Status::Ok, result);
    };
    RmwContext<Key, Value> context{ Key(key), Value(-1) };
    Status result = store.Rmw(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
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
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_TRUE(1 <= key && key <= num_records);
      ASSERT_EQ(context.output.value, 1);
    } else if (result != Status::Pending) {
      ASSERT_EQ(Status::NotFound, result);
      ASSERT_TRUE(key > num_records);
    }

    if (idx % 25 == 0) {
      store.CompletePending(false);
    }
  }

  store.StopSession();
  RemoveDirs("temp_store", hot_fp, cold_fp);
}


/// Inserts a bunch of records into a FASTER instance, and invokes the
/// compaction algorithm. Concurrent to the compaction, upserts, RMWs, and deletes
/// are performed in 1/4 of the keys, respectively. After compaction, it
/// checks that updated/RMW-ed keys have the new value, while deleted keys do not exist.
TEST_P(HotColdParameterizedTestFixture, ConcurrentOps) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef FasterKvHC<Key, Value, disk_t> faster_hc_t;

  std::string hot_fp, cold_fp;
  CreateLogDirs("temp_store", hot_fp, cold_fp);

  auto auto_compaction = GetParam();
  faster_hc_t store { 1_GiB, 0.25,            // 256 MB hot log, 768 cold log
                      192_MiB, 2048, hot_fp,  // [hot]  192 MB mem size, 512 entries in hash index
                      192_MiB, 2048, cold_fp, // [cold] 192 MB mem size, 512 entries in hash index
                      0.4, 0,                 // 64 MB mutable hot log, minimum mutable cold (i.e. 64 MB)
                      auto_compaction };      // automatic or manual compaction
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
    for (size_t idx = 1; idx <= num_records; ++idx) {
      if (idx % 4 != 0) continue;

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
    for (size_t idx = 1; idx <= num_records; ++idx) {
      if (idx % 4 != 1) continue;

      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_EQ(Status::Ok, result);
      };
      RmwContext<Key, Value> context{ Key(idx), Value(1) };
      Status result = store.Rmw(context, callback, idx / 4);
      ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
    }
    store.CompletePending(true);
    store.StopSession();
  };
  // Thread that performs Delete ops
  auto delete_worker_func = [&store]() {
    store.StartSession();
    for (size_t idx = 1; idx <= num_records; ++idx) {
      if (idx % 4 != 2) continue;

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
      if (key % 4 == 0) { // upserted
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

    if (idx % 25 == 0) {
      store.CompletePending(false);
    }
  }
  store.StopSession();
  RemoveDirs("temp_store", hot_fp, cold_fp);
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}