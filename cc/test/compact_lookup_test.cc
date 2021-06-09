// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"

#include "core/faster.h"

#include "device/null_disk.h"

#include "test_types.h"

using namespace FASTER::core;
using FASTER::test::FixedSizeKey;
using FASTER::test::SimpleAtomicMediumValue;
using FASTER::test::SimpleAtomicLargeValue;

using Key = FixedSizeKey<uint64_t>;
using MediumValue = SimpleAtomicMediumValue<uint64_t>;
using LargeValue = SimpleAtomicLargeValue<uint64_t>;

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif

class CompactLookupParametrizedTestFixture : public ::testing::TestWithParam<bool> {
};

INSTANTIATE_TEST_CASE_P(
        CompactLookupTests,
        CompactLookupParametrizedTestFixture,
        ::testing::Values(false, true)
      );

/// Upsert context required to insert data for unit testing.
template <class K, class V>
class UpsertContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  UpsertContext(K key, V value)
    : key_(key)
    , value_(value)
  {}

  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_(other.key_)
    , value_(other.value_)
  {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const K& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return V::size();
  }
  /// Non-atomic and atomic Put() methods.
  inline void Put(V& value) {
    value.value = value_.value;
  }
  inline bool PutAtomic(V& value) {
    value.atomic_value.store(value_.value);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  K key_;
  V value_;
};

/// Context to read a key when unit testing.
template <class K, class V>
class ReadContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  ReadContext(K key)
    : key_(key)
  {}

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_(other.key_)
  {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const K& key() const {
    return key_;
  }

  inline void Get(const V& value) {
    output = value.value;
  }
  inline void GetAtomic(const V& value) {
    output = value.atomic_value.load();
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  K key_;
 public:
  V output;
};

/// Context to delete a key when unit testing.
template<class K, class V>
class DeleteContext : public IAsyncContext {
 private:
  K key_;

 public:
  typedef K key_t;
  typedef V value_t;

  explicit DeleteContext(const K& key)
    : key_(key)
  {}

  inline const K& key() const {
    return key_;
  }

  inline static constexpr uint32_t value_size() {
    return V::size();
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }
};

// ****************************************************************************
// IN-MEMORY TESTS
// ****************************************************************************

/// Inserts a bunch of records into a FASTER instance and invokes the
/// compaction algorithm. Since all records are still live, checks if
/// they remain so after the algorithm completes/returns.
TEST_P(CompactLookupParametrizedTestFixture, InMemAllLive) {
  // In memory hybrid log
  typedef FasterKv<Key, MediumValue, FASTER::device::NullDisk> faster_t;
  // 512 MB log size -- 64 MB mutable region (min possible)
  faster_t store { 1024, (1 << 20) * 512, "", 0.125 };
  int numRecords = 200000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback won't be called
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, MediumValue> context{ Key(idx), MediumValue(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // perform compaction (with or without shift begin address)
  uint64_t until_address = store.hlog.safe_read_only_address.control();
  bool shift_begin_address = GetParam();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address, shift_begin_address));
  if (shift_begin_address)
    ASSERT_EQ(until_address, store.hlog.begin_address.control());

  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback should won't be called
      ASSERT_TRUE(false);
    };
    ReadContext<Key, MediumValue> context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    ASSERT_EQ(idx, context.output.value);
  }

  store.StopSession();
}

/// Inserts a bunch of records into a FASTER instance, deletes half of them
/// and invokes the compaction algorithm. Checks that the ones that should
/// be alive are alive and the ones that should be dead stay dead.
TEST_P(CompactLookupParametrizedTestFixture, InMemHalfLive) {
  // In memory hybrid log
  typedef FasterKv<Key, MediumValue, FASTER::device::NullDisk> faster_t;
  // 512 MB log size -- 64 MB mutable region (min possible)
  faster_t store { 1024, (1 << 20) * 512, "", 0.125 };
  int numRecords = 200000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback won't be called
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, MediumValue> context{ Key(idx), MediumValue(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // Delete every alternate key here.
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    if (idx % 2 == 0) continue;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    DeleteContext<Key, MediumValue> context{ Key(idx) };
    Status result = store.Delete(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // perform compaction (with or without shift begin address)
  uint64_t until_address = store.hlog.safe_read_only_address.control();
  bool shift_begin_address = GetParam();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address, shift_begin_address));
  if (shift_begin_address)
    ASSERT_EQ(until_address, store.hlog.begin_address.control());

  // After compaction, deleted keys stay deleted.
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback should won't be called
      ASSERT_TRUE(false);
    };
    ReadContext<Key, MediumValue> context{ idx };
    Status result = store.Read(context, callback, 1);
    Status expect = idx % 2 == 0 ? Status::Ok : Status::NotFound;
    ASSERT_EQ(expect, result);
    if (idx % 2 == 0) ASSERT_EQ(idx, context.output.value);
  }

  store.StopSession();
}

/// Inserts a bunch of records into a FASTER instance, updates half of them
/// with new values and invokes the compaction algorithm. Checks that the
/// updated ones have the new value, and the others the old one.
TEST_P(CompactLookupParametrizedTestFixture, InMemAllLiveNewEntries) {
  // In memory hybrid log
  typedef FasterKv<Key, MediumValue, FASTER::device::NullDisk> faster_t;
  // 512 MB log size -- 64 MB mutable region (min possible)
  faster_t store { 1024, (1 << 20) * 512, "", 0.125 };
  int numRecords = 200000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, MediumValue> context{Key(idx), MediumValue(idx)};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // Insert fresh entries for half the records
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    if (idx % 2 == 0) continue;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, MediumValue> context{ Key(idx), MediumValue(2 * idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // perform compaction (with or without shift begin address)
  uint64_t until_address = store.hlog.safe_read_only_address.control();
  bool shift_begin_address = GetParam();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address, shift_begin_address));
  if (shift_begin_address)
    ASSERT_EQ(until_address, store.hlog.begin_address.control());

  // After compaction, reads should return newer values
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback won't be called
      ASSERT_TRUE(false);
    };
    ReadContext<Key, MediumValue> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(result, Status::Ok);
    Key expected_key {(idx % 2 == 0)
                          ? context.output.value
                          : context.output.value / 2};
    ASSERT_EQ(idx, expected_key.key);
  }

  store.StopSession();
}

/// Inserts a bunch of records into a FASTER instance, and invokes the
/// compaction algorithm. Concurrent to the compaction, upserts and deletes
/// are performed in 1/3 of the keys, respectively. After compaction, it
/// checks that updated keys have the new value, while deleted keys do not exist.
TEST_P(CompactLookupParametrizedTestFixture, InMemConcurrentOps) {
  // In memory hybrid log
  typedef FASTER::device::NullDisk disk_t;
  typedef FasterKv<Key, MediumValue, disk_t> faster_t;
  // 512 MB log size -- 64 MB mutable region (min possible)
  faster_t store { 1024, (1 << 20) * 512, "", 0.125 };
  static constexpr int numRecords = 200000;

  store.StartSession();
  // Populate initial keys
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, MediumValue> context{Key(idx), MediumValue(idx)};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  store.StopSession();

  auto upsert_worker_func = [&store] {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // Insert fresh entries for half the records
    store.StartSession();
    for (size_t idx = 1; idx <= numRecords; ++idx) {
      if (idx % 3 == 0) {
        auto callback = [](IAsyncContext* ctxt, Status result) {
          ASSERT_TRUE(false);
        };
        UpsertContext<Key, MediumValue> context{ Key(idx), MediumValue(2 * idx) };
        Status result = store.Upsert(context, callback, idx / 3);
        ASSERT_EQ(Status::Ok, result);
      }
    }
    store.StopSession();
  };

  auto delete_worker_func = [&store] {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // Delete every alternate key here.
    store.StartSession();
    for (size_t idx = 1; idx <= numRecords; ++idx) {
      if (idx % 3 == 1) {
        auto callback = [](IAsyncContext* ctxt, Status result) {
          ASSERT_TRUE(false);
        };
        DeleteContext<Key, MediumValue> context{ Key(idx) };
        Status result = store.Delete(context, callback, idx / 3);
        ASSERT_EQ(Status::Ok, result);
      }
      store.StopSession();
    }
  };

  std::thread upset_worker (upsert_worker_func);
  std::thread delete_worker (delete_worker_func);

  // perform compaction concurrently
  store.StartSession();
  // perform compaction (with or without shift begin address)
  uint64_t until_address = store.hlog.safe_read_only_address.control();
  bool shift_begin_address = false; //GetParam();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address, shift_begin_address));
  if (shift_begin_address)
    ASSERT_EQ(until_address, store.hlog.begin_address.control());
  store.StopSession();

  upset_worker.join();
  delete_worker.join();

  // Reads should return newer values for non-deleted entries
  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback won't be called
      ASSERT_TRUE(false);
    };
    ReadContext<Key, MediumValue> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);

    if (idx % 3 == 0) {
      ASSERT_EQ(result, Status::Ok);
      ASSERT_EQ(idx, context.output.value / 2);
    } else if (idx % 3 == 1) {
      ASSERT_EQ(result, Status::NotFound);
    }
    else {
      ASSERT_EQ(result, Status::Ok);
      ASSERT_EQ(idx, context.output.value);
    }
  }
  store.StopSession();
}

/*TEST(CompactLookup, InMemAllLiveWithShiftAddress) {
  // In memory hybrid log
  typedef FasterKv<Key, MediumValue, FASTER::device::NullDisk> faster_t;
  // 512 MB log size -- 64 MB mutable region (min possible)
  faster_t store { 1024, (1 << 20) * 512, "", 0.125 };
  int numRecords = 200000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback won't be called
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, MediumValue> context{ Key(idx), MediumValue(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  Address until_address = store.hlog.safe_read_only_address.control();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address.control(), true));
  ASSERT_EQ(until_address, store.hlog.begin_address.control());

  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback should won't be called
      ASSERT_TRUE(false);
    };
    ReadContext<Key, MediumValue> context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    ASSERT_EQ(idx, context.output.value);
  }
  store.StopSession();
}*/

// ****************************************************************************
// PERSISTENCE STORAGE TESTS
// ****************************************************************************

/// Inserts a bunch of records into a FASTER instance and invokes the
/// compaction algorithm. Since all records are still live, checks if
/// they remain so after the algorithm completes/returns.
TEST_P(CompactLookupParametrizedTestFixture, AllLive) {
  typedef FASTER::device::FileSystemDisk<handler_t, (1 << 30)> disk_t; // 1GB file segments
  typedef FasterKv<Key, LargeValue, disk_t> faster_t;

  std::experimental::filesystem::create_directories("tmp_store");
  // NOTE: deliberatly keeping the hash index small to test hash-chain chasing correctness
  faster_t store { 1024, (1 << 20) * 192, "tmp_store", 0.4 };
  int numRecords = 50000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback won't be called
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, LargeValue> context{Key(idx), LargeValue(idx)};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // perform compaction (with or without shift begin address)
  uint64_t until_address = store.hlog.safe_read_only_address.control();
  bool shift_begin_address = GetParam();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address, shift_begin_address));
  if (shift_begin_address)
    ASSERT_EQ(until_address, store.hlog.begin_address.control());

  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);

      CallbackContext<ReadContext<Key, LargeValue>> context(ctxt);
      ASSERT_TRUE(context->key().key > 0);
      ASSERT_EQ(context->key(), context->output.value);
    };
    ReadContext<Key, LargeValue> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    EXPECT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      ASSERT_EQ(idx, context.output.value);
    }

    if (idx % 20 == 0) {
      // occasionally complete pending I/O requests
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);
  store.StopSession();

  std::experimental::filesystem::remove_all("tmp_store");
}

/// Inserts a bunch of records into a FASTER instance, deletes half of them
/// and invokes the compaction algorithm. Checks that the ones that should
/// be alive are alive and the ones that should be dead stay dead.
TEST_P(CompactLookupParametrizedTestFixture, HalfLive) {
  typedef FASTER::device::FileSystemDisk<handler_t, (1 << 30)> disk_t; // 1GB file segments
  typedef FasterKv<Key, LargeValue, disk_t> faster_t;

  std::experimental::filesystem::create_directories("tmp_store");
  // NOTE: deliberatly keeping the hash index small to test hash-chain chasing correctness
  faster_t store { 1024, (1 << 20) * 192, "tmp_store", 0.4 };
  int numRecords = 50000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, LargeValue> context{Key(idx), LargeValue(idx)};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // Delete every alternate key here.
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    if (idx % 2 == 0) continue;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    DeleteContext<Key, LargeValue> context{ Key(idx) };
    Status result = store.Delete(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // perform compaction (with or without shift begin address)
  uint64_t until_address = store.hlog.safe_read_only_address.control();
  bool shift_begin_address = GetParam();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address, shift_begin_address));
  if (shift_begin_address)
    ASSERT_EQ(until_address, store.hlog.begin_address.control());

  // After compaction, deleted keys stay deleted.
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext<Key, LargeValue>> context(ctxt);
      ASSERT_TRUE(context->key().key > 0);
      Status expected_status = (context->key().key % 2 == 0) ? Status::Ok
                                                             : Status::NotFound;
      ASSERT_EQ(expected_status, result);
      if (expected_status == Status::Ok) {
        ASSERT_EQ(context->key().key, context->output.value);
      }
    };
    ReadContext<Key, LargeValue> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    if (idx % 2 == 0) {
      EXPECT_TRUE(result == Status::Ok || result == Status::Pending);
    }
    else {
      EXPECT_TRUE(result == Status::NotFound || result == Status::Pending);
    }
    if (result == Status::Ok) {
      ASSERT_EQ(idx, context.output.value);
    }

    if (idx % 20 == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);
  store.StopSession();

  std::experimental::filesystem::remove_all("tmp_store");
}

/// Inserts a bunch of records into a FASTER instance, updates half of them
/// with new values, deletes the other half, and invokes the compaction algorithm.
/// Checks that the updated ones have the new value, and the rest remain deleted.
TEST_P(CompactLookupParametrizedTestFixture, AllLiveDeleteAndReInsert) {
  typedef FASTER::device::FileSystemDisk<handler_t, (1 << 30)> disk_t; // 1GB file segments
  typedef FasterKv<Key, LargeValue, disk_t> faster_t;

  std::experimental::filesystem::create_directories("tmp_store");
  // NOTE: deliberatly keeping the hash index small to test hash-chain chasing correctness
  faster_t store { 1024, (1 << 20) * 192, "tmp_store", 0.4 };
  int numRecords = 50000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, LargeValue> context{Key(idx), LargeValue(idx)};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // Delete every alternate key here.
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    if (idx % 2 == 0) continue;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    DeleteContext<Key, LargeValue> context{ Key(idx) };
    Status result = store.Delete(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // Insert fresh entries for the alternate keys
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    if (idx % 2 == 0) continue;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, LargeValue> context{ Key(idx), LargeValue(2 * idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // perform compaction (with or without shift begin address)
  uint64_t until_address = store.hlog.safe_read_only_address.control();
  bool shift_begin_address = GetParam();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address, shift_begin_address));
  if (shift_begin_address)
    ASSERT_EQ(until_address, store.hlog.begin_address.control());

  // After compaction, all entries should exist
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);

      CallbackContext<ReadContext<Key, LargeValue>> context(ctxt);
      ASSERT_TRUE(context->key().key > 0);
      Key expected_key {(context->key().key % 2 == 0)
                            ? context->output.value
                            : context->output.value / 2};
      ASSERT_EQ(context->key().key, expected_key.key);
    };
    ReadContext<Key, LargeValue> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    EXPECT_TRUE(result == Status::Ok || result == Status::Pending);
    if (result == Status::Ok) {
      Key expected_key {(idx % 2 == 0)
                            ? context.output.value
                            : context.output.value / 2};
      ASSERT_EQ(idx, expected_key.key);
    }

    if (idx % 20 == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);
  store.StopSession();

  std::experimental::filesystem::remove_all("tmp_store");
}

/// Inserts a bunch of records into a FASTER instance, and invokes the
/// compaction algorithm. Concurrent to the compaction, upserts and deletes
/// are performed in 1/3 of the keys, respectively. After compaction, it
/// checks that updated keys have the new value, while deleted keys do not exist.
TEST_P(CompactLookupParametrizedTestFixture, ConcurrentOps) {
  typedef FASTER::device::FileSystemDisk<handler_t, (1 << 30)> disk_t; // 1GB file segments
  typedef FasterKv<Key, LargeValue, disk_t> faster_t;

  std::experimental::filesystem::create_directories("tmp_store");
  // NOTE: deliberatly keeping the hash index small to test hash-chain chasing correctness
  faster_t store { 1024, (1 << 20) * 192, "tmp_store", 0.4 };
  constexpr int numRecords = 50000;

  store.StartSession();
  // Populate initial keys
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, LargeValue> context{Key(idx), LargeValue(idx)};
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  store.StopSession();

  auto upsert_worker_func = [&store]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    store.StartSession();
    // Insert fresh entries for half the records
    for (size_t idx = 1; idx <= numRecords; ++idx) {
      if (idx % 3 == 0) {
        auto callback = [](IAsyncContext* ctxt, Status result) {
          ASSERT_TRUE(false);
        };
        UpsertContext<Key, LargeValue> context{ Key(idx), LargeValue(2 * idx) };
        Status result = store.Upsert(context, callback, idx / 3);
        ASSERT_EQ(Status::Ok, result);
      }
    }
    store.CompletePending(true);
    store.StopSession();
  };

  auto delete_worker_func = [&store]() {
    // Delete every alternate key here.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    store.StartSession();
    for (size_t idx = 1; idx <= numRecords; ++idx) {
      if (idx % 3 == 1) {
        auto callback = [](IAsyncContext* ctxt, Status result) {
          ASSERT_TRUE(false);
        };
        DeleteContext<Key, LargeValue> context{ Key(idx) };
        Status result = store.Delete(context, callback, idx / 3);
        ASSERT_EQ(Status::Ok, result);
      }
    }
    store.CompletePending(true);
    store.StopSession();
  };
  // launch threads
  std::thread upset_worker (upsert_worker_func);
  std::thread delete_worker (delete_worker_func);

  store.StartSession();
  // perform compaction concurrently
  uint64_t until_address = store.hlog.safe_read_only_address.control();
  bool shift_begin_address = GetParam();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address, shift_begin_address));
  if (shift_begin_address)
    ASSERT_EQ(until_address, store.hlog.begin_address.control());
  store.StopSession();

  upset_worker.join();
  delete_worker.join();

  store.StartSession();
  // Reads should return newer values for non-deleted entries
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext<Key, LargeValue>> context(ctxt);
      ASSERT_TRUE(context->key().key > 0);
      if (context->key().key % 3 == 0) {
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->key().key, context->output.value / 2);
      } else if (context->key().key % 3 == 1) {
        ASSERT_EQ(Status::NotFound, result);
      } else {
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->key().key, context->output.value);
      }
    };
    ReadContext<Key, LargeValue> context{ Key(idx) };
    Status result = store.Read(context, callback, 1);
    EXPECT_TRUE(result == Status::Ok || result == Status::NotFound || result == Status::Pending);

    if (result == Status::Ok) {
      if (idx % 3 == 0) { // upserted
        ASSERT_EQ(idx, context.output.value / 2);
      } else if (idx % 3 == 2) { // unmodified
        ASSERT_EQ(idx, context.output.value);
      } else {
        ASSERT_TRUE(false);
      }
    } else if (result == Status::NotFound) {
      ASSERT_TRUE(idx % 3 == 1); // deleted
    }

    if (idx % 20 == 0) {
      store.CompletePending(false);
    }
  }
  store.CompletePending(true);

  std::experimental::filesystem::remove_all("tmp_store");
}

/*TEST(CompactLookup, InMemAllLiveWithShiftAddress) {
  // In memory hybrid log
  typedef FasterKv<Key, Value, FASTER::device::NullDisk> faster_t;
  // 1GB log size
  faster_t store { 1024, (1 << 20) * 1024, "", 0.4 };
  int numRecords = 100000;

  store.StartSession();
  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback won't be called
      ASSERT_TRUE(false);
    };
    UpsertContext<Key, Value> context{ Key(idx), Value(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  Address until_address = store.hlog.GetTailAddress();
  ASSERT_TRUE(
    store.CompactWithLookup(until_address.control(), true));
  ASSERT_EQ(until_address, store.hlog.begin_address.control());

  for (size_t idx = 1; idx <= numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // request will be sync -- callback should won't be called
      ASSERT_TRUE(false);
    };
    ReadContext<Key, Value> context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    ASSERT_EQ(idx, context.output.value);
  }

  store.StopSession();
}*/

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
