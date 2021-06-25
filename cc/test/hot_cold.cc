// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"

#include "core/faster_hc.h"
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

class ReadContext : public IAsyncContext {
  public:
  typedef Key key_t;
  typedef Value value_t;

  ReadContext(uint8_t key)
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
    // All reads should be atomic (from the mutable tail).
    ASSERT_TRUE(false);
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

TEST(HotCold, Init) {
  typedef FasterKvHC<Key, Value, FASTER::device::NullDisk> faster_hc_t;

  size_t log_size = 192 * (1 << 20);
  faster_hc_t store { 128, log_size, "", 128, log_size, ""};

  store.StartSession();

  // Insert.
  for(size_t idx = 0; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    UpsertContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read.
  for(size_t idx = 0; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // All upserts should have inserts (non-atomic).
    ASSERT_EQ(23, context.output);
  }
  // Update.
  for(size_t idx = 0; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    UpsertContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read again.
  for(size_t idx = 0; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // All upserts should have updates (atomic).
    ASSERT_EQ(42, context.output);
  }

  store.StopSession();
}

TEST(HotCold, Rmw) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = SimpleAtomicValue<uint32_t>;

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(uint64_t key, int32_t incr)
      : key_{ key }
      , incr_{ incr } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , incr_{ other.incr_ } {
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
    inline void RmwInitial(Value& value) {
      value.value = incr_;
    }
    inline void RmwCopy(const Value& old_value, Value& value) {
      value.value = old_value.value + incr_;
    }
    inline bool RmwAtomic(Value& value) {
      value.atomic_value.fetch_add(incr_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    int32_t incr_;
    Key key_;
  };

  class ReadContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint64_t key)
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
      // All reads should be atomic (from the mutable tail).
      ASSERT_TRUE(false);
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
    int32_t output;
  };

  typedef FasterKvHC<Key, Value, FASTER::device::NullDisk> faster_hc_t;
  size_t log_size = 1024 * (1 << 20);
  faster_hc_t store { 128, log_size, "", 128, log_size, ""};

  store.StartSession();

  // Rmw, increment by 1.
  for(size_t idx = 0; idx < 2048; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    RmwContext context{ idx % 512, 1 };
    Status result = store.Rmw(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read.
  for(size_t idx = 0; idx < 512; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result) << idx;
    // Should have performed 4 RMWs.
    ASSERT_EQ(4, context.output);
  }
  // Rmw, decrement by 1.
  for(size_t idx = 0; idx < 2048; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    RmwContext context{ idx % 512, -1 };
    Status result = store.Rmw(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read again.
  for(size_t idx = 0; idx < 512; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // All upserts should have inserts (non-atomic).
    ASSERT_EQ(0, context.output);
  }

  store.StopSession();
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}