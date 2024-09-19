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

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif

// Parameterized test definition
// <# hash index buckets, auto compaction, lazy-compaction, read-cache enabled>
using param_types = std::tuple<uint32_t, bool, bool, bool>;

class HotColdRecoveryTestParam : public ::testing::TestWithParam<param_types> {
};
INSTANTIATE_TEST_CASE_P(
  HotColdRecoveryTests,
  HotColdRecoveryTestParam,
  ::testing::Values(
    ///== w/o read-cache
    // Small hash hot & cold indices
    std::make_tuple(2048, false, false, false),
    std::make_tuple(2048, false, true, false),
    std::make_tuple(2048, true, false, false),
    std::make_tuple(2048, true, true, false),
    // Large hot & cold indices
    std::make_tuple((1 << 22), false, false, false),
    std::make_tuple((1 << 22), false, true, false),
    std::make_tuple((1 << 22), true, false, false),
    std::make_tuple((1 << 22), true, true, false),
    ///== w/ read-cache
    // Small hash hot & cold indices
    std::make_tuple(2048, false, false, true),
    std::make_tuple(2048, false, true, true),
    std::make_tuple(2048, true, false, true),
    std::make_tuple(2048, true, true, true),
    // Large hot & cold indices
    std::make_tuple((1 << 22), false, false, true),
    std::make_tuple((1 << 22), false, true, true),
    std::make_tuple((1 << 22), true, false, true),
    std::make_tuple((1 << 22), true, true, true)

  )
);

static std::string ROOT_PATH{ "test_store/" };
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


TEST_P(HotColdRecoveryTestParam, CheckpointAndRecoverySerial) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = LargeValue;

  typedef UpsertContext<Key, Value> upsert_context_t;
  typedef ReadContext<Key, Value> read_context_t;

  typedef FASTER::device::FileSystemDisk<handler_t, 1_GiB> disk_t; // 1GB file segments
  typedef F2Kv<Key, Value, disk_t> f2_t;

  auto args = GetParam();
  uint64_t table_size = std::get<0>(args);
  bool auto_compaction = std::get<1>(args);
  bool lazy_checkpoint = std::get<2>(args);
  bool rc_enabled = std::get<3>(args);

  log_info("\nTEST: Index size: %lu\tAuto compaction: %d\tWait for Next Compaction: %d\tRead-Cache: %s",
            table_size, auto_compaction, lazy_checkpoint, rc_enabled ? "ENABLED" : "DISABLED");

  ReadCacheConfig rc_config {
    .mem_size = 256_MiB,
    .mutable_fraction = 0.5,
    .pre_allocate_log = false,
    .enabled = rc_enabled,
  };

  F2CompactionConfig f2_compaction_config;
  f2_compaction_config.hot_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 256_MiB, 4, auto_compaction };
  f2_compaction_config.cold_store = HlogCompactionConfig{
    250ms, 0.9, 0.1, 128_MiB, 768_MiB, 4, auto_compaction };

  f2_t::ColdIndexConfig cold_index_config{ table_size, 256_MiB, 0.6 };

  Guid session_id;
  Guid token;

  uint32_t numRecords = 100'000;
  static std::atomic<uint64_t> records_read{ 0 };

  std::string hot_fp, cold_fp;
  CreateNewLogDir(ROOT_PATH, hot_fp, cold_fp);

  // Test checkpointing
  {
    f2_t store{ table_size, 192_MiB, hot_fp,
                cold_index_config, 192_MiB, cold_fp,
                0.4, 0, rc_config, f2_compaction_config };

    // Populate the store
    session_id = store.StartSession();
    for (size_t idx = 1; idx <= numRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<upsert_context_t> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
      };
      upsert_context_t context{ Key{ idx }, Value{idx % 256} };
      Status result = store.Upsert(context, callback, 1);
      ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

      if (idx % kCompletePendingInterval == 0) {
        store.CompletePending(false);
      }
    }

    if (!auto_compaction) {
      // perform hot-cold compaction
      uint64_t hot_size = store.hot_store.Size(), cold_size = store.cold_store.Size();
      store.CompactHotLog(store.hot_store.hlog.safe_read_only_address.control(), true);
      ASSERT_TRUE(store.hot_store.Size() < hot_size && store.cold_store.Size() > cold_size);
    }

    // Verify inserted entries
    records_read.store(0);
    for(size_t idx = 1; idx <= numRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_EQ(Status::Ok, result);
        CallbackContext<read_context_t> context{ ctxt };
        ASSERT_EQ(context->key().key % 256, context->output.value );
        ++records_read;
      };

      if(idx % kCompletePendingInterval == 0) {
        store.CompletePending(false);
      }

      read_context_t context{ Key{ idx } };
      Status result = store.Read(context, callback, 1);
      if(result == Status::Ok) {
        ASSERT_EQ(idx % 256, context.output.value);
        ++records_read;
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    store.CompletePending(true);

    // Checkpoint the store
    static std::atomic<size_t> num_threads_persistent{ 0 };
    static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads];
    for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
      threads_persistent[idx] = false;
    }

    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
      bool expected = false;
      ASSERT_EQ(Status::Ok, result);
      log_debug("Hot-cold store checkpoint callback called! [result: %s, psn: %lu]",
                STATUS_STR[static_cast<int>(result)], persistent_serial_num);
      ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected, true));
      ++num_threads_persistent;
    };

    // checkpoint (transition from REST to INDEX_CHKPT)
    log_debug("Requesting checkpoint...");
    ASSERT_TRUE(store.Checkpoint(hybrid_log_persistence_callback, token, lazy_checkpoint));
    log_debug("Checkpoint requested!");

    if (!auto_compaction) {
      // perform cold-cold compaction
      uint64_t cold_size = store.cold_store.Size();
      store.CompactColdLog(store.cold_store.hlog.safe_read_only_address.control(), true);
    }

    while(num_threads_persistent < 1) {
      store.CompletePending(false);
    }
    bool result = store.CompletePending(true);
    ASSERT_TRUE(result);
    ASSERT_EQ(numRecords, records_read.load());

    store.StopSession();
    log_debug("Checkpoint finished!");
  }

  // Test recovery
  {
    f2_t store{ table_size, 192_MiB, hot_fp,
                cold_index_config, 192_MiB, cold_fp,
                0.4, 0, rc_config, f2_compaction_config };

    uint32_t version;
    std::vector<Guid> session_ids;

    log_debug("Issuing recover...");
    Status status = store.Recover(token, version, session_ids);
    log_debug("Recover done!");

    ASSERT_EQ(Status::Ok, status);
    ASSERT_EQ(1, session_ids.size());
    ASSERT_EQ(session_id, session_ids[0]);
    ASSERT_EQ(1, store.ContinueSession(session_id));

    // Verify inserted entries
    log_debug("Verifying previously inserted entries...");
    records_read.store(0);
    for(size_t idx = 1; idx <= numRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_EQ(Status::Ok, result);
        CallbackContext<read_context_t> context{ ctxt };
        ASSERT_EQ(context->key().key % 256, context->output.value );
        ++records_read;
      };

      if(idx % kCompletePendingInterval == 0) {
        store.CompletePending(false);
      }

      read_context_t context{ Key{ idx } };
      Status result = store.Read(context, callback, 1);
      if(result == Status::Ok) {
        ASSERT_EQ(idx % 256, context.output.value);
        ++records_read;
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    store.CompletePending(true);
    ASSERT_EQ(records_read.load(), numRecords);
    store.StopSession();

    log_debug("Success!");
  }

  RemoveDir(ROOT_PATH);
}

int main(int argc, char** argv) {
  rlim_t new_stack_size = 64_MiB;
  if (!set_stack_size(new_stack_size)) {
    log_warn("Could not set stack size to %lu bytes", new_stack_size);
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
