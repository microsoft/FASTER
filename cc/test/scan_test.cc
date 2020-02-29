// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"

#include "core/faster.h"
#include "core/log_scan.h"

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

/// Inserts 256 8 Byte keys into FASTER and tests that the scan iterator
/// scans over and returns all of them correctly.
TEST(ScanIter, InMem) {
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

  ScanIterator<faster_t> iter(&(store.hlog), Buffering::UN_BUFFERED,
                              store.hlog.begin_address.load(),
                              store.hlog.GetTailAddress(), &(store.disk));

  int num = 0;
  while (true) {
    auto r = iter.GetNext();
    if (r == nullptr) break;
    ASSERT_EQ(num, r->key().key);
    num++;
  }

  ASSERT_EQ(numRecords, num);

  store.StopSession();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
