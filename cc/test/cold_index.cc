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

using namespace FASTER::core;

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif

// Parameterized test definition
// bool value indicates whether or not to perform lookup compaction
class ColdIndexCompactionTestFixture : public ::testing::TestWithParam<bool> {
};
INSTANTIATE_TEST_CASE_P(
  ColdIndexCompactionTests,
  ColdIndexCompactionTestFixture,
  ::testing::Values(false, true)
);

// Megabyte & Gigabyte literal helpers
constexpr uint64_t operator""_MiB(unsigned long long const x ) {
  return static_cast<uint64_t>(x * (1 << 20));
}
constexpr uint64_t operator""_GiB(unsigned long long const x ) {
  return static_cast<uint64_t>(x * (1 << 30));
}

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
    std::hash<uint64_t> hash_fn;
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

 private:
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

 private:
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
    //, expected_{ expected } {
  }

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ } {
    //, expected_{ other.expected_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }

  inline void Get(const Value& value) {
    // This is a paging test, so we expect to read stuff from disk.
    std::memcpy(&value_, &value, sizeof(Value));

    //ASSERT_EQ(expected_, value.length_ - 1);
    //ASSERT_EQ(expected_, value.value_[expected_]);
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

    //ASSERT_EQ(expected_, static_cast<uint8_t>(len - 1));
    //ASSERT_EQ(expected_, val);
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
  //uint8_t expected_;
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

constexpr size_t REFRESH_INTERVAL = 29;

TEST(ColdIndex, UpsertRead_Serial) {
  std::string root_path{ "logs" };
  std::experimental::filesystem::remove_all(root_path);
  ASSERT_TRUE(std::experimental::filesystem::create_directories(root_path));

  FasterKv<Key, Value, disk_t, FasterIndex<disk_t>> store{ 16384, 192_MiB, root_path, 0.4 };
  //FasterKv<Key, Value, disk_t> store{ 16384, 192_MiB, root_path, 0.4 };
  constexpr size_t kNumRecords = 250000;

  Guid session_id = store.StartSession();
  // Insert.
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // Upserts don't go to disk.
      ASSERT_TRUE(false);
    };

    if(idx % REFRESH_INTERVAL == 0) {
      store.CompletePending(false);
    }

    UpsertContext context{ Key{ idx }, static_cast<uint8_t>(idx % 256) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // Read.
  static std::atomic<uint64_t> records_read{ 0 };
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext> context{ ctxt };
      context->CheckValue();
      ++records_read;
    };

    if(idx % REFRESH_INTERVAL == 0) {
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

  ASSERT_LT(records_read.load(), kNumRecords);
  bool result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_read.load());

  // Update.
  static std::atomic<uint64_t> records_updated{ 0 };
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<UpsertContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_updated;
    };

    if(idx % REFRESH_INTERVAL == 0) {
      store.CompletePending(false);
    }

    UpsertContext context{ Key{ idx }, static_cast<uint8_t>(idx % 256) };
    Status result = store.Upsert(context, callback, 1);
    if(result == Status::Ok) {
      ++records_updated;
    } else {
      //fprintf(stderr, "%lu\n", idx);
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

    if(idx % REFRESH_INTERVAL == 0) {
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

  ASSERT_LT(records_read.load(), kNumRecords);
  result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_read.load());

  store.StopSession();

  std::experimental::filesystem::remove_all(root_path);
}

TEST(ColdIndex, ConcurrentUpsertAndRead) {
  std::string root_path{ "logs" };
  std::experimental::filesystem::remove_all(root_path);
  ASSERT_TRUE(std::experimental::filesystem::create_directories(root_path));

  // 8 pages!
  FasterKv<Key, Value, disk_t> store{ 16384, 192_MiB, "logs", 0.4 };

  static constexpr size_t kNumRecords = 250000;
  static constexpr size_t kNumThreads = 8;

  static std::atomic<uint64_t> num_writes{ 0 };

  auto upsert_worker = [](FasterKv<Key, Value, disk_t>* store_, size_t thread_idx) {
    Guid session_id = store_->StartSession();

    for(size_t idx = 0; idx < kNumRecords / kNumThreads; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ++num_writes;
      };

      if(idx % REFRESH_INTERVAL == 0) {
        store_->CompletePending(false);
      }

      uint64_t key_component = thread_idx * (kNumRecords / kNumThreads) + idx;
      UpsertContext context{ Key{ key_component },
                            static_cast<uint8_t>(key_component % 256) };
      Status result = store_->Upsert(context, callback, 1);
      if (result == Status::Ok) {
        ++num_writes;
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }

    store_->CompletePending(true);
    store_->StopSession();
  };

  // Insert.
  std::deque<std::thread> threads{};
  for(size_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(upsert_worker, &store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_EQ(kNumRecords, num_writes.load());

  // Read.
  Guid session_id = store.StartSession();

  static std::atomic<uint64_t> records_read{ 0 };
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      CallbackContext<ReadContext> context{ ctxt };
      context->CheckValue();
      ++records_read;
    };

    if(idx % REFRESH_INTERVAL == 0) {
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

  ASSERT_LT(records_read.load(), kNumRecords);
  bool result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_read.load());

  // Stop session as we are going to wait for threads
  store.StopSession();

  //// Update.
  num_writes = 0;
  threads.clear();
  for(size_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(upsert_worker, &store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_EQ(kNumRecords, num_writes.load());

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

    if(idx % REFRESH_INTERVAL == 0) {
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

  ASSERT_LT(records_read.load(), kNumRecords);
  result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_read.load());

  store.StopSession();

  std::experimental::filesystem::remove_all(root_path);
}

TEST_P(ColdIndexCompactionTestFixture, UpsertDeleteHalfRead) {
  typedef FasterKv<Key, Value, disk_t, FasterIndex<disk_t>> faster_t;

  std::string root_path{ "logs" };
  std::experimental::filesystem::remove_all(root_path);
  ASSERT_TRUE(std::experimental::filesystem::create_directories(root_path));
  // NOTE: deliberatly keeping the hash index small to test hash-chain chasing correctness
  faster_t store { 2048, (1 << 20) * 192, "logs", 0.4 };
  int numRecords = 100000;

  bool shift_begin_address = true;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
    };
    UpsertContext context{ Key{ idx }, static_cast<uint8_t>(idx % 256) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

    if (idx % REFRESH_INTERVAL == 0) {
      store.CompletePending(false);
    }
  }

  // Delete every alternate key here.
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    if (idx % 2 == 0) continue;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
    };
    DeleteContext context{ Key(idx) };
    Status result = store.Delete(context, callback, 1);
    ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

    if (idx % REFRESH_INTERVAL == 0) {
      store.CompletePending(false);
    }
  }

  // Insert fresh entries for the alternate keys
  for (size_t idx = 1; idx <= numRecords; idx += 2) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
    };
    UpsertContext context{ Key(idx), static_cast<uint8_t>((2 * idx) % 256) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_TRUE(Status::Ok == result || Status::Pending == result);

    if (idx % REFRESH_INTERVAL == 0) {
      store.CompletePending(false);
    }
  }

  store.CompletePending(true);

  // perform compaction (with or without shift begin address)
  bool perform_compaction = GetParam();

  if (perform_compaction) {
    uint64_t until_address = store.hlog.safe_read_only_address.control();
    ASSERT_TRUE(
      store.CompactWithLookup(until_address, true));
    ASSERT_EQ(until_address, store.hlog.begin_address.control());
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

    if (idx % REFRESH_INTERVAL == 0) {
      store.CompletePending(false);
    }
  }

  store.CompletePending(true);
  store.StopSession();

  std::experimental::filesystem::remove_all(root_path);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
