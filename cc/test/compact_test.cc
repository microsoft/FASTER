// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"

#include "core/faster.h"

#include "device/null_disk.h"

#include "test_types.h"

using namespace FASTER::core;
using FASTER::test::FixedSizeKey;
using FASTER::test::SimpleAtomicValue;

using Key = FixedSizeKey<uint64_t>;
using Value = SimpleAtomicValue<uint64_t>;

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
    value.value = key_.key;
  }
  inline bool PutAtomic(Value& value) {
    value.atomic_value.store(key_.key);
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

/// Context to read a key when unit testing.
class ReadContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  ReadContext(uint64_t key)
    : key_{ key }
  {}

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ }
  {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }

  inline void Get(const Value& value) {
    output = value.value;
  }
  inline void GetAtomic(const Value& value) {
    output = value.atomic_value.load();
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
 public:
  uint64_t output;
};

/// Context to delete a key when unit testing.
class DeleteContext : public IAsyncContext {
 private:
  Key key_;

 public:
  typedef Key key_t;
  typedef Value value_t;

  explicit DeleteContext(const Key& key)
    : key_{ key }
  {}

  inline const Key& key() const {
    return key_;
  }

  inline static constexpr uint32_t value_size() {
    return Value::size();
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }
};

/// Inserts a bunch of records into a FASTER instance and invokes the
/// compaction algorithm. Since all records are still live, checks if
/// they remain so after the algorithm completes/returns.
TEST(Compact, AllLive) {
  typedef FasterKv<Key, Value, FASTER::device::NullDisk> faster_t;

  faster_t store { 128, 1073741824, "" };

  store.StartSession();

  int numRecords = 256;
  for (size_t idx = 0; idx < numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext context{ static_cast<uint64_t>(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  store.Compact(store.hlog.GetTailAddress().control());

  for (size_t idx = 0; idx < numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    ASSERT_EQ(idx, context.output);
  }

  store.StopSession();
}

/// Inserts a bunch of records into a FASTER instance, deletes half of them
/// and invokes the compaction algorithm. Checks that the ones that should
/// be alive are alive and the ones that should be dead stay dead.
TEST(Compact, HalfLive) {
  typedef FasterKv<Key, Value, FASTER::device::NullDisk> faster_t;

  faster_t store { 128, 1073741824, "" };

  store.StartSession();

  int numRecords = 256;
  for (size_t idx = 0; idx < numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    UpsertContext context{ static_cast<uint64_t>(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  // Delete every alternate key here.
  for (size_t idx = 0; idx < numRecords; ++idx) {
    if (idx % 2 == 0) continue;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    Key key{ idx };
    DeleteContext context{ key };
    Status result = store.Delete(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  store.Compact(store.hlog.GetTailAddress().control());

  // After compaction, deleted keys stay deleted.
  for (size_t idx = 0; idx < numRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_TRUE(false);
    };
    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    Status expect = idx % 2 == 0 ? Status::Ok : Status::NotFound;
    ASSERT_EQ(expect, result);
    if (idx % 2 == 0) ASSERT_EQ(idx, context.output);
  }

  store.StopSession();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
