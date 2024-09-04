// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <atomic>
#include <cstdint>
#include <cstring>
#include <deque>
#include <experimental/filesystem>
#include <functional>
#include <thread>

#include "gtest/gtest.h"
#include "core/faster.h"
#include "device/file_system_disk.h"

#include "index/index.h"
#include "index/mem_index.h"
#include "index/faster_index.h"
#include "utils.h"

using namespace FASTER::core;

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif

// Parameterized test definition
using param_types = std::tuple<uint64_t, uint64_t, double, bool>;
class ColdIndexTestParams : public ::testing::TestWithParam<param_types> {
};
INSTANTIATE_TEST_CASE_P(
  ColdIndexTests,
  ColdIndexTestParams,
  ::testing::Values(
    // w/o compaction
    std::make_tuple(2048, 192_MiB, 0.4, false),
    std::make_tuple(2048, 192_MiB, 0.0, false),
    std::make_tuple((1 << 20), 192_MiB, 0.4, false),
    std::make_tuple((1 << 20), 192_MiB, 0.0, false),
    // \w atomic hash chunk updates
    std::make_tuple(2048, 1_GiB, 0.9, false),
    std::make_tuple((1 << 20), 1_GiB, 0.9, false),
    // \w compaction
    std::make_tuple(2048, 192_MiB, 0.4, true),
    std::make_tuple(2048, 192_MiB, 0.0, true),
    std::make_tuple((1 << 20), 192_MiB, 0.4, true),
    std::make_tuple((1 << 20), 192_MiB, 0.0, true),
    // \w compaction & atomic hash chunk updates
    std::make_tuple(2048, 1_GiB, 0.9, true),
    std::make_tuple((1 << 20), 1_GiB, 0.9, true)
  )
);

// Parameterized recovery test definition
using recovery_param_types = std::tuple<uint64_t, uint64_t, double>;
class ColdIndexRecoveryTestParams : public ::testing::TestWithParam<recovery_param_types> {
};
INSTANTIATE_TEST_CASE_P(
  ColdIndexRecoveryTests,
  ColdIndexRecoveryTestParams,
  ::testing::Values(
    std::make_tuple(2048, 192_MiB, 0.4),
    std::make_tuple(2048, 192_MiB, 0.0),
    std::make_tuple((1 << 20), 192_MiB, 0.4),
    std::make_tuple((1 << 20), 192_MiB, 0.0)
  )
);


// Forward declaration
class UpsertContext;
class ReadContext;
class DeleteContext;

class Key {
 public:
  Key(uint64_t key)
    : key_{ key } {
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Key));
  }
  inline KeyHash GetHash() const {
    //std::hash<uint64_t> hash_fn;
    FasterHashHelper<uint64_t> hash_fn;
    return KeyHash{ hash_fn(key_) };
  }

  /// Comparison operators.
  inline bool operator==(const Key& other) const {
    return key_ == other.key_;
  }
  inline bool operator!=(const Key& other) const {
    return key_ != other.key_;
  }

  friend class UpsertContext;
  friend class ReadContext;
  friend class DeleteContext;

 public:
  uint64_t key_;
};

class Value {
 public:
  Value()
    : gen_{ 0 }
    , value_{ 0 }
    , length_{ 0 } {
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Value));
  }

  friend class UpsertContext;
  friend class ReadContext;
  friend class DeleteContext;

 public:
  uint8_t value_[1014];
  uint16_t length_;

  std::atomic<uint64_t> gen_;
};
static_assert(sizeof(Value) == 1024, "sizeof(Value) != 1024");
static_assert(alignof(Value) == 8, "alignof(Value) != 8");

class UpsertContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  UpsertContext(const Key& key, uint8_t val)
    : key_{ key }
    , val_{ val } {
  }

  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_{ other.key_ }
    , val_{ other.val_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  inline static constexpr uint32_t value_size(const Value& old_value) {
    return sizeof(value_t);
  }
  /// Non-atomic and atomic Put() methods.
  inline void Put(Value& value) {
    value.gen_ = 0;
    value.length_ = val_ + 1; // avoid 0 length value
    std::memset(value.value_, val_, value.length_);
  }
  inline bool PutAtomic(Value& value) {
    // Get the lock on the value.
    uint64_t expected_gen;
    bool success;
    do {
      do {
        // Spin until other the thread releases the lock.
        expected_gen = value.gen_.load();
      } while(expected_gen == UINT64_MAX);
      // Try to get the lock.
      success = value.gen_.compare_exchange_weak(expected_gen, UINT64_MAX);
    } while(!success);

    value.length_ = val_ + 1; // avoid 0 length value
    std::memset(value.value_, val_, value.length_);
    // Increment the value's generation number.
    value.gen_.store(expected_gen + 1);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  uint8_t val_;
};

class ReadContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  ReadContext(Key key) // uint8_t expected)
    : key_{ key } {
  }

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }

  inline void Get(const Value& value) {
    // This is a paging test, so we expect to read stuff from disk.
    std::memcpy(&value_, &value, sizeof(Value));
  }
  inline void GetAtomic(const Value& value) {
    uint64_t post_gen = value.gen_.load();
    uint64_t pre_gen;
    do {
      // Pre- gen # for this read is last read's post- gen #.
      pre_gen = post_gen;
      std::memcpy(&value_, &value, sizeof(Value));
      post_gen = value.gen_.load();
    } while(pre_gen != post_gen);
  }

  inline void CheckValue() {
    CheckValue(static_cast<uint8_t>(key_.key_ % 256));
  }

  inline void CheckValue(uint8_t expected_) {
    ASSERT_EQ(expected_, value_.length_ - 1);
    ASSERT_EQ(expected_, value_.value_[expected_]);
  }

  protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  Value value_;
};

class DeleteContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  explicit DeleteContext(const Key& key)
    : key_(key) {
  }

  inline const Key& key() const {
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
  Key key_;
};

/// Disk's log uses 64 MB segments.
typedef FASTER::device::FileSystemDisk<handler_t, 64_MiB> disk_t;
typedef FasterKv<Key, Value, disk_t, FasterIndex<disk_t>> faster_t;

static std::string ROOT_PATH{ "test_store/" };
constexpr size_t kCompletePendingInterval = 64;

TEST_P(ColdIndexTestParams, UpsertRead_Serial) {
  CreateNewLogDir(ROOT_PATH);

  auto args = GetParam();
  uint64_t table_size = std::get<0>(args);
  uint64_t log_mem_size = std::get<1>(args);
  double log_mutable_frac = std::get<2>(args);
  bool auto_compaction = std::get<3>(args);

  ReadCacheConfig rc_config;
  rc_config.enabled = false;

  HlogCompactionConfig compaction_config{
    250ms, 0.8, 0.1, 128_MiB, 768_MiB, 4, auto_compaction };

  faster_t::IndexConfig index_config{ table_size, 256_MiB, 0.6 };
  faster_t store{ index_config, log_mem_size, ROOT_PATH, log_mutable_frac,
                  rc_config, compaction_config };

  constexpr size_t kNumRecords = 250'000;
  static std::atomic<uint64_t> records_read{ 0 };
  static std::atomic<uint64_t> records_updated{ 0 };

  Guid session_id = store.StartSession();

  // Insert.
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<UpsertContext> context{ ctxt };
      ASSERT_EQ(result, Status::Ok);
    };

    if(idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }

    UpsertContext context{ Key{ idx }, static_cast<uint8_t>(idx % 256) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
  }
  store.CompletePending(true);

  // Read.
  records_read = 0;
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext> context{ ctxt };
      context->CheckValue();
      ++records_read;
    };

    if(idx % kCompletePendingInterval == 0) {
      store.Refresh();
    }

    ReadContext context{ Key{ idx } };
    Status result = store.Read(context, callback, 1);
    if(result == Status::Ok) {
      context.CheckValue();
      ++records_read;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  ASSERT_LE(records_read.load(), kNumRecords);
  bool result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_read.load());

  // Update.
  records_updated = 0;
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<UpsertContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_updated;
    };

    if(idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }

    UpsertContext context{ Key{ idx }, static_cast<uint8_t>(idx % 256) };
    Status result = store.Upsert(context, callback, 1);
    if(result == Status::Ok) {
      ++records_updated;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  result = store.CompletePending(true);
  ASSERT_EQ(kNumRecords, records_updated.load());
  ASSERT_TRUE(result);

  // Read again.
  records_read = 0;
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext> context{ ctxt };
      context->CheckValue();
      ++records_read;
    };

    if(idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }

    ReadContext context{ Key{ idx } };
    Status result = store.Read(context, callback, 1);
    if(result == Status::Ok) {
      context.CheckValue();
      ++records_read;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  ASSERT_LE(records_read.load(), kNumRecords);
  result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_read.load());

  store.StopSession();
  RemoveDir(ROOT_PATH);
}

TEST_P(ColdIndexTestParams, ConcurrentUpsertAndRead) {
  CreateNewLogDir(ROOT_PATH);

  auto args = GetParam();
  uint64_t table_size = std::get<0>(args);
  uint64_t log_mem_size = std::get<1>(args);
  double log_mutable_frac = std::get<2>(args);
  bool do_compaction = std::get<3>(args);

  faster_t::IndexConfig index_config{ table_size, 256_MiB, 0.6 };
  faster_t store{ index_config, log_mem_size, ROOT_PATH, log_mutable_frac };

  static constexpr size_t kNumRecords = 250'000;
  static constexpr size_t kNumThreads = 8;

  static std::atomic<uint64_t> records_read{ 0 };
  static std::atomic<uint64_t> records_updated{ 0 };

  auto upsert_worker = [](faster_t* store_, size_t thread_idx) {
    Guid session_id = store_->StartSession();

    for(size_t idx = 0; idx < kNumRecords / kNumThreads; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ++records_updated;
      };

      if(idx % kCompletePendingInterval == 0) {
        store_->CompletePending(false);
      }

      uint64_t key_component = thread_idx * (kNumRecords / kNumThreads) + idx;
      UpsertContext context{ Key{ key_component },
                            static_cast<uint8_t>(key_component % 256) };
      Status result = store_->Upsert(context, callback, 1);
      if (result == Status::Ok) {
        ++records_updated;
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }

    store_->CompletePending(true);
    store_->StopSession();
  };

  // Insert.
  records_updated = 0;

  std::deque<std::thread> threads{};
  for(size_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(upsert_worker, &store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }
  threads.clear();

  ASSERT_EQ(kNumRecords, records_updated.load());

  // Read.
  Guid session_id = store.StartSession();

  records_read = 0;
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext> context{ ctxt };
      context->CheckValue();
      ++records_read;
    };

    if(idx % kCompletePendingInterval == 0) {
      store.Refresh();
    }

    ReadContext context{ Key{ idx } };
    Status result = store.Read(context, callback, 1);
    if(result == Status::Ok) {
      context.CheckValue();
      ++records_read;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  ASSERT_LE(records_read.load(), kNumRecords);
  bool result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_read.load());

  // Stop session as we are going to wait for threads
  store.StopSession();

  // Update.
  records_updated = 0;

  for(size_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(upsert_worker, &store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }
  threads.clear();

  ASSERT_EQ(kNumRecords, records_updated.load());

  // Restart session
  store.StartSession();

  // Read again.
  records_read = 0;;
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext> context{ ctxt };
      context->CheckValue();
      ++records_read;
    };

    if(idx % kCompletePendingInterval == 0) {
      store.Refresh();
    }

    ReadContext context{ Key{ idx } };
    Status result = store.Read(context, callback, 1);
    if(result == Status::Ok) {
      context.CheckValue();
      ++records_read;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  ASSERT_LE(records_read.load(), kNumRecords);
  result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_read.load());

  store.StopSession();
  RemoveDir(ROOT_PATH);
}

TEST_P(ColdIndexTestParams, UpsertDeleteHalfRead) {
  CreateNewLogDir(ROOT_PATH);

  auto args = GetParam();
  uint64_t table_size = std::get<0>(args);
  uint64_t log_mem_size = std::get<1>(args);
  double log_mutable_frac = std::get<2>(args);
  bool do_compaction = std::get<3>(args);
  bool shift_begin_address = true;

  faster_t::IndexConfig index_config{ table_size, 256_MiB, 0.6 };
  faster_t store{ index_config, log_mem_size, ROOT_PATH, log_mutable_frac };

  int numRecords = 100'000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<UpsertContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
    };
    UpsertContext context{ Key{ idx }, static_cast<uint8_t>(idx % 256) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Delete every alternate key here.
  for (size_t idx = 1; idx <= numRecords; idx += 2) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<DeleteContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
    };
    DeleteContext context{ Key(idx) };
    Status result = store.Delete(context, callback, 1);
    ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Insert fresh entries for the alternate keys
  for (size_t idx = 1; idx <= numRecords; idx += 2) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<UpsertContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
    };
    UpsertContext context{ Key(idx), static_cast<uint8_t>((2 * idx) % 256) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // perform compaction
  if (do_compaction) {
    uint64_t until_address = store.hlog.safe_read_only_address.control();
    if (until_address > store.hlog.begin_address.control()) {
      ASSERT_TRUE(
        store.CompactWithLookup(until_address, shift_begin_address));
      ASSERT_EQ(until_address, store.hlog.begin_address.control());
    }
  }

  // All entries should exist
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context(ctxt);
      ASSERT_EQ(Status::Ok, result);
    };
    ReadContext context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok) {
      uint8_t expected_ = (idx % 2 == 0)  ? static_cast<uint8_t>(idx % 256)
                                          : static_cast<uint8_t>((idx * 2) % 256);
      context.CheckValue(expected_);
    } else {
      ASSERT_EQ(Status::Pending, result);
    }

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }

  store.CompletePending(true);
  store.StopSession();

  RemoveDir(ROOT_PATH);
}

TEST_P(ColdIndexTestParams, UpsertUpdateAll) {
  CreateNewLogDir(ROOT_PATH);

  auto args = GetParam();
  uint64_t table_size = std::get<0>(args);
  uint64_t log_mem_size = std::get<1>(args);
  double log_mutable_frac = std::get<2>(args);
  bool do_compaction = std::get<3>(args);
  bool shift_begin_address = true;

  faster_t::IndexConfig index_config{ table_size, 256_MiB, 0.6 };
  faster_t store{ index_config, log_mem_size, ROOT_PATH, log_mutable_frac };

  int numRecords = 100'000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<UpsertContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
    };
    UpsertContext context{ Key{ idx }, static_cast<uint8_t>(idx % 256) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  // Insert fresh entries for all keys
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<UpsertContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
    };
    UpsertContext context{ Key(idx), static_cast<uint8_t>((2 * idx) % 256) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);
  store.Refresh();

  // perform compaction
  if (do_compaction) {
    uint64_t until_address = store.hlog.safe_read_only_address.control();
    if (until_address > store.hlog.begin_address.control()) {
      ASSERT_TRUE(
        store.CompactWithLookup(until_address, shift_begin_address, 1));
      ASSERT_EQ(until_address, store.hlog.begin_address.control());
    }
  }

  // All entries should have updated values
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context(ctxt);
      ASSERT_EQ(Status::Ok, result);
    };
    ReadContext context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok) {
      uint8_t expected_ = static_cast<uint8_t>((idx * 2) % 256);
      context.CheckValue(expected_);
    } else {
      ASSERT_EQ(Status::Pending, result);
    }

    if (idx % kCompletePendingInterval == 0) {
      store.CompletePending(false);
    }
  }

  store.CompletePending(true);
  store.StopSession();

  RemoveDir(ROOT_PATH);
}

TEST_P(ColdIndexRecoveryTestParams, Serial) {
  CreateNewLogDir(ROOT_PATH);

  auto args = GetParam();
  uint64_t table_size = std::get<0>(args);
  uint64_t log_mem_size = std::get<1>(args);
  double log_mutable_frac = std::get<2>(args);

  faster_t::IndexConfig index_config{ table_size, 256_MiB, 0.6 };

  int numRecords = 100'000;

  Guid session_id;
  Guid token;

  static std::atomic<uint64_t> records_read{ 0 };

  // Test checkpointing
  {
    faster_t store{ index_config, log_mem_size, ROOT_PATH, log_mutable_frac };

    // Populate the store
    session_id = store.StartSession();
    for (size_t idx = 1; idx <= numRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
      };
      UpsertContext context{ Key{ idx }, static_cast<uint8_t>(idx % 256) };
      Status result = store.Upsert(context, callback, 1);
      ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

      if (idx % kCompletePendingInterval == 0) {
        store.CompletePending(false);
      }
    }
    store.CompletePending(true);

    // Verify inserted entries
    records_read.store(0);
    for(size_t idx = 1; idx <= numRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        ASSERT_EQ(Status::Ok, result);
        CallbackContext<ReadContext> context{ ctxt };
        context->CheckValue();
        ++records_read;
      };

      if(idx % kCompletePendingInterval == 0) {
        store.CompletePending(false);
      }

      ReadContext context{ Key{ idx } };
      Status result = store.Read(context, callback, 1);
      if(result == Status::Ok) {
        context.CheckValue();
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
      ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected, true));
      ++num_threads_persistent;
    };

    // checkpoint (transition from REST to INDEX_CHKPT)
    log_debug("Issuing checkpoint...");
    ASSERT_TRUE(store.Checkpoint(nullptr, hybrid_log_persistence_callback, token));
    log_debug("Checkpoint issued!");

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
    faster_t store{ index_config, log_mem_size, ROOT_PATH, log_mutable_frac };

    uint32_t version;
    std::vector<Guid> session_ids;

    log_debug("Issuing recover...");
    Status status = store.Recover(token, token, version, session_ids);
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
        CallbackContext<ReadContext> context{ ctxt };
        context->CheckValue();
        ++records_read;
      };

      if(idx % kCompletePendingInterval == 0) {
        store.CompletePending(false);
      }

      ReadContext context{ Key{ idx } };
      Status result = store.Read(context, callback, 1);
      if(result == Status::Ok) {
        context.CheckValue();
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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
