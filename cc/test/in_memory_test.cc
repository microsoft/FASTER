// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdint>
#include <cstring>
#include <deque>
#include <thread>
#include <utility>
#include "gtest/gtest.h"

#include "core/faster.h"
#include "device/null_disk.h"

#include "test_types.h"

using namespace FASTER::core;
using FASTER::test::NonMovable;
using FASTER::test::NonCopyable;
using FASTER::test::FixedSizeKey;
using FASTER::test::SimpleAtomicValue;

class Latch {
 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool triggered_ = false;

 public:
  void Wait() {
    std::unique_lock<std::mutex> lock{ mutex_ };
    while (!triggered_) {
      cv_.wait(lock);
    }
  }

  void Trigger() {
    std::unique_lock<std::mutex> lock{ mutex_ };
    triggered_ = true;
    cv_.notify_all();
  }

  void Reset() {
    triggered_ = false;
  }
};

template <typename Callable, typename... Args>
void run_threads(size_t num_threads, Callable worker, Args... args) {
  Latch latch;
  auto run_thread = [&latch, &worker, &args...](size_t idx) {
    latch.Wait();
    worker(idx, args...);
  };

  std::deque<std::thread> threads{};
  for(size_t idx = 0; idx < num_threads; ++idx) {
    threads.emplace_back(run_thread, idx);
  }

  latch.Trigger();
  for(auto& thread : threads) {
    thread.join();
  }
}

TEST(InMemFaster, UpsertRead) {
  using Key = FixedSizeKey<uint8_t>;
  using Value = SimpleAtomicValue<uint8_t>;

  class UpsertContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint8_t key)
      : key_{ key } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext& other)
      : key_{ other.key_ } {
    }

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
    uint8_t output;
  };

  FasterKv<Key, Value, FASTER::device::NullDisk> store { 128, 1073741824, "" };

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

/// The hash always returns "0," so the FASTER store devolves into a linked list.
TEST(InMemFaster, UpsertRead_DummyHash) {
  class DummyHash {
   public:
    inline size_t operator()(const uint16_t& key) const {
      return 42;
    }
  };

  using Key = FixedSizeKey<uint16_t, DummyHash>;
  using Value = SimpleAtomicValue<uint16_t>;

  class UpsertContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint16_t key)
      : key_{ key } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext& other)
      : key_{ other.key_ } {
    }

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

  class ReadContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint16_t key)
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
    uint16_t output;
  };

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 128, 1073741824, "" };

  store.StartSession();

  // Insert.
  for(uint16_t idx = 0; idx < 10000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    UpsertContext context{ idx };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read.
  for(uint16_t idx = 0; idx < 10000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // All upserts should have inserts (non-atomic).
    ASSERT_EQ(idx, context.output);
  }

  store.StopSession();
}

TEST(InMemFaster, UpsertRead_Concurrent) {
  using Key = FixedSizeKey<uint32_t>;

  class UpsertContext;
  class ReadContext;

  class alignas(16) Value {
   public:
    Value()
      : length_{ 0 }
      , value_{ 0 } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Value));
    }

    friend class UpsertContext;
    friend class ReadContext;

   private:
    uint8_t value_[31];
    std::atomic<uint8_t> length_;
  };

  class UpsertContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint32_t key)
      : key_{ key } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext& other)
      : key_{ other.key_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.length_ = 5;
      std::memset(value.value_, 23, 5);
    }
    inline bool PutAtomic(Value& value) {
      // Get the lock on the value.
      bool success;
      do {
        uint8_t expected_length;
        do {
          // Spin until other the thread releases the lock.
          expected_length = value.length_.load();
        } while(expected_length == UINT8_MAX);
        // Try to get the lock.
        success = value.length_.compare_exchange_weak(expected_length, UINT8_MAX);
      } while(!success);

      std::memset(value.value_, 42, 7);
      value.length_.store(7);
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

    ReadContext(uint32_t key)
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
      do {
        output_length = value.length_.load();
        ASSERT_EQ(0, reinterpret_cast<size_t>(value.value_) % 16);
        output_pt1 = *reinterpret_cast<const uint64_t*>(value.value_);
        output_pt2 = *reinterpret_cast<const uint64_t*>(value.value_ + 8);
      } while(output_length != value.length_.load());
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
   public:
    uint8_t output_length;
    uint64_t output_pt1;
    uint64_t output_pt2;
  };

  static constexpr size_t kNumOps = 1024;
  static constexpr size_t kNumThreads = 2;

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 128, 1073741824, "" };

  auto upsert_worker = [&store](size_t thread_idx) {
    store.StartSession();

    for(size_t idx = 0; idx < kNumOps; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      UpsertContext context{ static_cast<uint32_t>((thread_idx * kNumOps) + idx) };
      Status result = store.Upsert(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }

    store.StopSession();
  };

  auto read_worker = [&store](size_t thread_idx, uint64_t expected_value) {
    store.StartSession();

    for(size_t idx = 0; idx < kNumOps; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      ReadContext context{ static_cast<uint32_t>((thread_idx * kNumOps) + idx) };
      Status result = store.Read(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(expected_value, context.output_pt1);
    }

    store.StopSession();
  };

  // Insert.
  run_threads(kNumThreads, upsert_worker);

  // Read.
  run_threads(kNumThreads, read_worker, 0x1717171717);

  // Update.
  run_threads(kNumThreads, upsert_worker);

  // Read again.
  run_threads(kNumThreads, read_worker, 0x2a2a2a2a2a2a2a);
}

TEST(InMemFaster, UpsertRead_ResizeValue_Concurrent) {
  using Key = FixedSizeKey<uint32_t>;

  class UpsertContext;
  class ReadContext;

  class GenLock {
   public:
    GenLock()
      : control_{ 0 } {
    }
    GenLock(uint64_t control)
      : control_{ control } {
    }
    inline GenLock& operator=(const GenLock& other) {
      control_ = other.control_;
      return *this;
    }

    union {
        struct {
          uint64_t gen_number : 62;
          uint64_t locked : 1;
          uint64_t replaced : 1;
        };
        uint64_t control_;
      };
  };
  static_assert(sizeof(GenLock) == 8, "sizeof(GenLock) != 8");

  class AtomicGenLock {
   public:
    AtomicGenLock()
      : control_{ 0 } {
    }
    AtomicGenLock(uint64_t control)
      : control_{ control } {
    }

    inline GenLock load() const {
      return GenLock{ control_.load() };
    }
    inline void store(GenLock desired) {
      control_.store(desired.control_);
    }

    inline bool try_lock(bool& replaced) {
      replaced = false;
      GenLock expected{ control_.load() };
      expected.locked = 0;
      expected.replaced = 0;
      GenLock desired{ expected.control_ };
      desired.locked = 1;

      if(control_.compare_exchange_strong(expected.control_, desired.control_)) {
        return true;
      }
      if(expected.replaced) {
        replaced = true;
      }
      return false;
    }
    inline void unlock(bool replaced) {
      if(!replaced) {
        // Just turn off "locked" bit and increase gen number.
        uint64_t sub_delta = ((uint64_t)1 << 62) - 1;
        control_.fetch_sub(sub_delta);
      } else {
        // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
        uint64_t add_delta = ((uint64_t)1 << 63) - ((uint64_t)1 << 62) + 1;
        control_.fetch_add(add_delta);
      }
    }

   private:
    std::atomic<uint64_t> control_;
  };
  static_assert(sizeof(AtomicGenLock) == 8, "sizeof(AtomicGenLock) != 8");

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

    friend class UpsertContext;
    friend class ReadContext;

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

  class UpsertContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint32_t key, uint32_t length)
      : key_{ key }
      , length_{ length } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext& other)
      : key_{ other.key_ }
      , length_{ other.length_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(Value) + length_;
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.gen_lock_.store(0);
      value.size_ = sizeof(Value) + length_;
      value.length_ = length_;
      std::memset(value.buffer(), 88, length_);
    }
    inline bool PutAtomic(Value& value) {
      bool replaced;
      while(!value.gen_lock_.try_lock(replaced) && !replaced) {
        std::this_thread::yield();
      }
      if(replaced) {
        // Some other thread replaced this record.
        return false;
      }
      if(value.size_ < sizeof(Value) + length_) {
        // Current value is too small for in-place update.
        value.gen_lock_.unlock(true);
        return false;
      }
      // In-place update overwrites length and buffer, but not size.
      value.length_ = length_;
      std::memset(value.buffer(), 88, length_);
      value.gen_lock_.unlock(false);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t length_;
  };

  class ReadContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint32_t key)
      : key_{ key }
      , output_length{ 0 } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
      : key_{ other.key_ }
      , output_length{ 0 } {
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
      GenLock before, after;
      do {
        before = value.gen_lock_.load();
        output_length = value.length_;
        output_bytes[0] = value.buffer()[0];
        output_bytes[1] = value.buffer()[value.length_ - 1];
        after = value.gen_lock_.load();
      } while(before.gen_number != after.gen_number);
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
   public:
    uint8_t output_length;
    // Extract two bytes of output.
    uint8_t output_bytes[2];
  };

  static constexpr size_t kNumOps = 1024;
  static constexpr size_t kNumThreads = 2;

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 128, 1073741824, "" };

  auto upsert_worker = [&store](size_t thread_idx, uint32_t value_length) {
    store.StartSession();

    for(size_t idx = 0; idx < kNumOps; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      UpsertContext context{ static_cast<uint32_t>((thread_idx * kNumOps) + idx), value_length };
      Status result = store.Upsert(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }

    store.StopSession();
  };

  auto read_worker = [&store](size_t thread_idx, uint8_t expected_value) {
    store.StartSession();

    for(size_t idx = 0; idx < kNumOps; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      ReadContext context{ static_cast<uint32_t>((thread_idx * kNumOps) + idx) };
      Status result = store.Read(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(expected_value, context.output_bytes[0]);
      ASSERT_EQ(expected_value, context.output_bytes[1]);
    }

    store.StopSession();
  };

  // Insert.
  run_threads(kNumThreads, upsert_worker, 7);

  // Read.
  run_threads(kNumThreads, read_worker, 88);

  // Update.
  run_threads(kNumThreads, upsert_worker, 11);

  // Read again.
  run_threads(kNumThreads, read_worker, 88);
}

TEST(InMemFaster, Rmw) {
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

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 256, 1073741824, "" };

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

TEST(InMemFaster, Rmw_Concurrent) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = SimpleAtomicValue<int64_t>;

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(uint64_t key, int64_t incr)
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
    int64_t incr_;
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
    int64_t output;
  };

  static constexpr size_t kNumThreads = 2;
  static constexpr size_t kNumRmws = 2048;
  static constexpr size_t kRange = 512;

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 256, 1073741824, "" };

  auto rmw_worker = [&store](size_t thread_idx, int64_t multiplier) {
    store.StartSession();

    int64_t incr{ (int64_t) thread_idx * multiplier };
    for(size_t idx = 0; idx < kNumRmws; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      RmwContext context{ idx % kRange, incr };
      Status result = store.Rmw(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }

    store.StopSession();
  };

  // Rmw, increment by 2 * idx.
  run_threads(kNumThreads, rmw_worker, 2);

  // Read.
  store.StartSession();

  for(size_t idx = 0; idx < kRange; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result) << idx;
    // Should have performed 4 RMWs.
    ASSERT_EQ((kNumThreads * (kNumThreads - 1)) * (kNumRmws / kRange), context.output);
  }

  store.StopSession();

  // Rmw, decrement by idx.
  run_threads(kNumThreads, rmw_worker, -1);

  // Read again.
  store.StartSession();

  for(size_t idx = 0; idx < kRange; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // All upserts should have inserts (non-atomic).
    ASSERT_EQ(((kNumThreads * (kNumThreads - 1)) / 2) * (kNumRmws / kRange), context.output);
  }

  store.StopSession();
}

TEST(InMemFaster, Rmw_ResizeValue_Concurrent) {
  using Key = FixedSizeKey<uint64_t>;

  class RmwContext;
  class ReadContext;

  class GenLock {
   public:
    GenLock()
      : control_{ 0 } {
    }
    GenLock(uint64_t control)
      : control_{ control } {
    }
    inline GenLock& operator=(const GenLock& other) {
      control_ = other.control_;
      return *this;
    }

    union {
        struct {
          uint64_t gen_number : 62;
          uint64_t locked : 1;
          uint64_t replaced : 1;
        };
        uint64_t control_;
      };
  };
  static_assert(sizeof(GenLock) == 8, "sizeof(GenLock) != 8");

  class AtomicGenLock {
   public:
    AtomicGenLock()
      : control_{ 0 } {
    }
    AtomicGenLock(uint64_t control)
      : control_{ control } {
    }

    inline GenLock load() const {
      return GenLock{ control_.load() };
    }
    inline void store(GenLock desired) {
      control_.store(desired.control_);
    }

    inline bool try_lock(bool& replaced) {
      replaced = false;
      GenLock expected{ control_.load() };
      expected.locked = 0;
      expected.replaced = 0;
      GenLock desired{ expected.control_ };
      desired.locked = 1;

      if(control_.compare_exchange_strong(expected.control_, desired.control_)) {
        return true;
      }
      if(expected.replaced) {
        replaced = true;
      }
      return false;
    }
    inline void unlock(bool replaced) {
      if(!replaced) {
        // Just turn off "locked" bit and increase gen number.
        uint64_t sub_delta = ((uint64_t)1 << 62) - 1;
        control_.fetch_sub(sub_delta);
      } else {
        // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
        uint64_t add_delta = ((uint64_t)1 << 63) - ((uint64_t)1 << 62) + 1;
        control_.fetch_add(add_delta);
      }
    }

   private:
    std::atomic<uint64_t> control_;
  };
  static_assert(sizeof(AtomicGenLock) == 8, "sizeof(AtomicGenLock) != 8");

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

    friend class RmwContext;
    friend class ReadContext;

   private:
    AtomicGenLock gen_lock_;
    uint32_t size_;
    uint32_t length_;

    inline const int8_t* buffer() const {
      return reinterpret_cast<const int8_t*>(this + 1);
    }
    inline int8_t* buffer() {
      return reinterpret_cast<int8_t*>(this + 1);
    }
  };

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(uint64_t key, int8_t incr, uint32_t length)
      : key_{ key }
      , incr_{ incr }
      , length_{ length } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , incr_{ other.incr_ }
      , length_{ other.length_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + length_;
    }
    inline uint32_t value_size(const Value& old_value) const {
      return sizeof(value_t) + length_;
    }

    inline void RmwInitial(Value& value) {
      value.gen_lock_.store(GenLock{});
      value.size_ = sizeof(Value) + length_;
      value.length_ = length_;
      std::memset(value.buffer(), incr_, length_);
    }
    inline void RmwCopy(const Value& old_value, Value& value) {
      value.gen_lock_.store(GenLock{});
      value.size_ = sizeof(Value) + length_;
      value.length_ = length_;
      std::memset(value.buffer(), incr_, length_);
      for(uint32_t idx = 0; idx < std::min(old_value.length_, length_); ++idx) {
        value.buffer()[idx] = old_value.buffer()[idx] + incr_;
      }
    }
    inline bool RmwAtomic(Value& value) {
      bool replaced;
      while(!value.gen_lock_.try_lock(replaced) && !replaced) {
        std::this_thread::yield();
      }
      if(replaced) {
        // Some other thread replaced this record.
        return false;
      }
      if(value.size_ < sizeof(Value) + length_) {
        // Current value is too small for in-place update.
        value.gen_lock_.unlock(true);
        return false;
      }
      // In-place update overwrites length and buffer, but not size.
      value.length_ = length_;
      for(uint32_t idx = 0; idx < length_; ++idx) {
        value.buffer()[idx] += incr_;
      }
      value.gen_lock_.unlock(false);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    int8_t incr_;
    uint32_t length_;
    Key key_;
  };

  class ReadContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint64_t key)
      : key_{ key }
      , output_length{ 0 } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
      : key_{ other.key_ }
      , output_length{ 0 } {
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
      GenLock before, after;
      do {
        before = value.gen_lock_.load();
        output_length = value.length_;
        output_bytes[0] = value.buffer()[0];
        output_bytes[1] = value.buffer()[value.length_ - 1];
        after = value.gen_lock_.load();
      } while(before.gen_number != after.gen_number);
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
   public:
    uint8_t output_length;
    // Extract two bytes of output.
    int8_t output_bytes[2];
  };

  static constexpr int8_t kNumThreads = 2;
  static constexpr size_t kNumRmws = 2048;
  static constexpr size_t kRange = 512;

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 256, 1073741824, "" };

  auto rmw_worker = [&store](size_t thread_idx, int8_t incr, uint32_t value_length) {
    store.StartSession();

    for(size_t idx = 0; idx < kNumRmws; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      RmwContext context{ idx % kRange, incr, value_length };
      Status result = store.Rmw(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }

    store.StopSession();
  };

  // Rmw, increment by 3.
  run_threads(kNumThreads, rmw_worker, 3, 5);

  // Read.
  store.StartSession();

  for(size_t idx = 0; idx < kRange; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result) << idx;
    // Should have performed 4 RMWs.
    ASSERT_EQ(5, context.output_length);
    ASSERT_EQ(kNumThreads * 4 * 3, context.output_bytes[0]);
    ASSERT_EQ(kNumThreads * 4 * 3, context.output_bytes[1]);
  }

  store.StopSession();

  // Rmw, decrement by 4.
  run_threads(kNumThreads, rmw_worker, -4, 8);

  // Read again.
  store.StartSession();

  for(size_t idx = 0; idx < kRange; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // Should have performed 4 RMWs.
    ASSERT_EQ(8, context.output_length);
    ASSERT_EQ(kNumThreads * -4, context.output_bytes[0]);
    ASSERT_EQ(kNumThreads * -16, context.output_bytes[1]);
  }

  store.StopSession();
}

TEST(InMemFaster, Rmw_GrowString_Concurrent) {
  using Key = FixedSizeKey<uint64_t>;

  class RmwContext;
  class ReadContext;

  class Value {
   public:
    Value()
            : length_{ 0 } {
    }

    inline uint32_t size() const {
      return length_;
    }

    friend class RmwContext;
    friend class ReadContext;

   private:
    uint32_t length_;

    const char* buffer() const {
      return reinterpret_cast<const char*>(this + 1);
    }
    char* buffer() {
      return reinterpret_cast<char*>(this + 1);
    }
  };

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(uint64_t key, char letter)
            : key_{ key }
            , letter_{ letter } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
            : key_{ other.key_ }
            , letter_{ other.letter_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + sizeof(char);
    }
    inline uint32_t value_size(const Value& old_value) const {
      return sizeof(value_t) + old_value.length_ + sizeof(char);
    }

    inline void RmwInitial(Value& value) {
      value.length_ = sizeof(char);
      value.buffer()[0] = letter_;
    }
    inline void RmwCopy(const Value& old_value, Value& value) {
      value.length_ = old_value.length_ + sizeof(char);
      std::memcpy(value.buffer(), old_value.buffer(), old_value.length_);
      value.buffer()[old_value.length_] = letter_;
    }
    inline bool RmwAtomic(Value& value) {
      // All RMW operations use Read-Copy-Update
      return false;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    char letter_;
    Key key_;
  };

  class ReadContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint64_t key)
            : key_{ key }
            , output_length{ 0 } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
            : key_{ other.key_ }
            , output_length{ 0 } {
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
      // There are no concurrent updates
      output_length = value.length_;
      output_letters[0] = value.buffer()[0];
      output_letters[1] = value.buffer()[value.length_ - 1];
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
   public:
    uint8_t output_length;
    // Extract two letters of output.
    char output_letters[2];
  };

  static constexpr int8_t kNumThreads = 2;
  static constexpr size_t kNumRmws = 2048;
  static constexpr size_t kRange = 512;

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 256, 1073741824, "" };

  auto rmw_worker = [&store](size_t _, char start_letter){
    store.StartSession();

    for(size_t idx = 0; idx < kNumRmws; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
          // In-memory test.
          ASSERT_TRUE(false);
      };
      char letter = static_cast<char>(start_letter + idx / kRange);
      RmwContext context{ idx % kRange, letter };
      Status result = store.Rmw(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }

    store.StopSession();
  };

  // Rmw.
  run_threads(kNumThreads, rmw_worker, 'A');

  // Read.
  store.StartSession();

  for(size_t idx = 0; idx < kRange; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };
    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result) << idx;
    ASSERT_EQ(kNumThreads * kNumRmws / kRange, context.output_length);
    ASSERT_EQ('A', context.output_letters[0]);
    ASSERT_EQ('D', context.output_letters[1]);
  }

  store.StopSession();

  // Rmw.
  run_threads(kNumThreads, rmw_worker, 'E');

  // Read again.
  store.StartSession();

  for(size_t idx = 0; idx < kRange; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };
    ReadContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    ASSERT_EQ(2 * kNumThreads * kNumRmws / kRange, context.output_length);
    ASSERT_EQ('A', context.output_letters[0]);
    ASSERT_EQ('H', context.output_letters[1]);
  }

  store.StopSession();
}

TEST(InMemFaster, ConcurrentDelete) {
  using KeyData = std::pair<uint64_t, uint64_t>;
  struct HashFn {
    inline size_t operator()(const KeyData& key) const {
      std::hash<uint64_t> hash_fn;
      return hash_fn(key.first);
    }
  };

  using Key = FixedSizeKey<KeyData, HashFn>;
  using Value = SimpleAtomicValue<int64_t>;

  class RmwContext : public IAsyncContext {
   private:
    Key key_;
   public:
    typedef Key key_t;
    typedef Value value_t;

    explicit RmwContext(const Key& key)
      : key_{ key }
    {}

    inline const Key& key() const {
      return key_;
    }

    inline static constexpr uint32_t value_size() {
      return Value::size();
    }

    inline static constexpr uint32_t value_size(const Value& old_value) {
      return Value::size();
    }

    inline void RmwInitial(Value& value) {
      value.value = 1;
    }

    inline void RmwCopy(const Value& old_value, Value& value) {
      value.value = old_value.value * 2 + 1;
    }

    inline bool RmwAtomic(Value& value) {
      // Not supported: so that operation would allocate a new entry for the update.
      return false;
    }
   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
  };

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

  class ReadContext : public IAsyncContext {
   private:
    Key key_;
   public:
    typedef Key key_t;
    typedef Value value_t;

    int64_t output;

    explicit ReadContext(const Key& key)
      : key_{ key }
    {}

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
  };

  static constexpr size_t kNumOps = 1024;
  static constexpr size_t kNumThreads = 2;

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 128, 1073741824, "" };

  // Rmw.
  run_threads(kNumThreads, [&store](size_t thread_idx) {
    store.StartSession();

    // Update each entry 2 times (1st is insert, 2nd is rmw).
    for(size_t i = 0; i < 2; ++i) {
      for(size_t idx = 0; idx < kNumOps; ++idx) {
        auto callback = [](IAsyncContext* ctxt, Status result) {
          // In-memory test.
          ASSERT_TRUE(false);
        };
        Key key{ std::make_pair(idx % 7, thread_idx * kNumOps + idx) };
        RmwContext context{ key };
        Status result = store.Rmw(context, callback, 1);
        ASSERT_EQ(Status::Ok, result);
      }
    }

    store.StopSession();
  });

  // Delete.
  run_threads(kNumThreads, [&store](size_t thread_idx) {
    store.StartSession();

    for(size_t idx = 0; idx < kNumOps; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      Key key{ std::make_pair(idx % 7, thread_idx * kNumOps + idx) };
      DeleteContext context{ key };
      Status result = store.Delete(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }

    store.StopSession();
  });

  // Read.
  run_threads(kNumThreads, [&store](size_t thread_idx) {
    store.StartSession();

    for(size_t idx = 0; idx < kNumOps; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      Key key{ std::make_pair(idx % 7, thread_idx * kNumOps + idx) };
      ReadContext context{ key };
      Status result = store.Read(context, callback, 1);
      ASSERT_EQ(Status::NotFound, result);
    }

    store.StopSession();
  });
}

TEST(InMemFaster, GrowHashTable) {
  using Key = FixedSizeKey<uint64_t>;
  using Value = SimpleAtomicValue<int64_t>;

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(uint64_t key, int64_t incr)
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
    int64_t incr_;
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
    int64_t output;
  };

  static constexpr size_t kNumThreads = 2;
  static constexpr size_t kNumRmws = 32768;
  static constexpr size_t kRange = 8192;

  FasterKv<Key, Value, FASTER::device::NullDisk> store{ 256, 1073741824, "" };
  static std::atomic<bool> grow_done{ false };

  auto rmw_worker = [&store](size_t thread_idx, int64_t multiplier) {
    store.StartSession();

    int64_t incr{ (int64_t) thread_idx * multiplier };
    for(size_t idx = 0; idx < kNumRmws; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };
      RmwContext context{ idx % kRange, incr };
      Status result = store.Rmw(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }

    if(thread_idx == 0) {
      // Double the size of the index.
      store.GrowIndex([](uint64_t new_size) {
        grow_done = true;
      });
    }

    while(!grow_done) {
      store.Refresh();
      std::this_thread::yield();
    }

    store.StopSession();
  };

  // Rmw, increment by 2 * idx.
  run_threads(kNumThreads, rmw_worker, 2);

  // Read.
  store.StartSession();

  for(size_t idx = 0; idx < kRange; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result) << idx;
    // Should have performed 4 RMWs.
    ASSERT_EQ((kNumThreads * (kNumThreads - 1)) * (kNumRmws / kRange), context.output);
  }

  store.StopSession();

  // Rmw, decrement by idx.
  grow_done = false;
  run_threads(kNumThreads, rmw_worker, -1);

  // Read again.
  store.StartSession();

  for(size_t idx = 0; idx < kRange; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // In-memory test.
      ASSERT_TRUE(false);
    };
    ReadContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // All upserts should have inserts (non-atomic).
    ASSERT_EQ(((kNumThreads * (kNumThreads - 1)) / 2) * (kNumRmws / kRange), context.output);
  }

  store.StopSession();
}

TEST(InMemFaster, UpsertRead_VariableLengthKey) {
  class Key : NonCopyable, NonMovable {
  public:
      static uint32_t size(uint32_t key_length) {
        return static_cast<uint32_t>(sizeof(Key) + key_length);
      }

      static void Create(Key* dst, uint32_t key_length, uint8_t* key_data) {
        dst->key_length_ = key_length;
        memcpy(dst->buffer(), key_data, key_length);
      }

      /// Methods and operators required by the (implicit) interface:
      inline uint32_t size() const {
        return static_cast<uint32_t>(sizeof(Key) + key_length_);
      }
      inline KeyHash GetHash() const {
        return KeyHash(Utility::HashBytesUint8(buffer(), key_length_));
      }

      /// Comparison operators.
      inline bool operator==(const Key& other) const {
        if (this->key_length_ != other.key_length_) return false;
        return memcmp(buffer(), other.buffer(), key_length_) == 0;
      }
      inline bool operator!=(const Key& other) const {
        return !(*this == other);
      }

      uint32_t key_length_;

      inline const uint8_t* buffer() const {
        return reinterpret_cast<const uint8_t*>(this + 1);
      }
      inline uint8_t* buffer() {
        return reinterpret_cast<uint8_t*>(this + 1);
      }
  };

  class ShallowKey {
  public:
      ShallowKey(uint8_t* key_data, uint32_t key_length)
          : key_length_(key_length), key_data_(key_data)
      { }

      inline uint32_t size() const {
        return Key::size(key_length_);
      }
      inline KeyHash GetHash() const {
        return KeyHash(Utility::HashBytesUint8(key_data_, key_length_));
      }
      inline void write_deep_key_at(Key* dst) const {
        Key::Create(dst, key_length_, key_data_);
      }
      /// Comparison operators.
      inline bool operator==(const Key& other) const {
        if (this->key_length_ != other.key_length_) return false;
        return memcmp(key_data_, other.buffer(), key_length_) == 0;
      }
      inline bool operator!=(const Key& other) const {
        return !(*this == other);
      }

      uint32_t key_length_;
      uint8_t* key_data_;
  };

  using Value = SimpleAtomicValue<uint8_t>;

  class UpsertContext : public IAsyncContext {
  public:
      typedef Key key_t;
      typedef Value value_t;

      UpsertContext(uint8_t* key, uint32_t key_length)
              : key_{ key, key_length } {
      }

      /// Copy (and deep-copy) constructor.
      UpsertContext(const UpsertContext& other)
          : key_{ other.key_ } {
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
        value.value = 23;
      }
      inline bool PutAtomic(Value& value) {
        value.atomic_value.store(42);
        return true;
      }

  protected:
      /// The explicit interface requires a DeepCopy_Internal() implementation.
      Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        // In this particular test, the key content is always on the heap and always available,
        // so we don't need to copy the key content. If the key content were on the stack,
        // we would need to copy the key content to the heap as well
        //
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }

  private:
      ShallowKey key_;
  };

  class ReadContext : public IAsyncContext {
  public:
      typedef Key key_t;
      typedef Value value_t;

      ReadContext(uint8_t* key, uint32_t key_length)
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
      ShallowKey key_;
  public:
      uint8_t output;
  };

  FasterKv<Key, Value, FASTER::device::NullDisk> store { 128, 1073741824, "" };

  store.StartSession();

  // Insert.
  for(uint32_t idx = 1; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };

    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    UpsertContext context{ key, idx };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read.
  for(uint32_t idx = 1; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };

    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    ReadContext context{ key, idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // All upserts should have inserts (non-atomic).
    ASSERT_EQ(23, context.output);
  }
  // Update.
  for(uint32_t idx = 1; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };

    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    UpsertContext context{ key, idx };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read again.
  for(uint32_t idx = 1; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };

    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    ReadContext context{ key, idx };
    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    // All upserts should have updates (atomic).
    ASSERT_EQ(42, context.output);
  }

  store.StopSession();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
