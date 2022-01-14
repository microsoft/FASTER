// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <experimental/filesystem>

using namespace FASTER;

/// Disk's log uses 64 MB segments.
typedef FASTER::device::FileSystemDisk<handler_t, 67108864L> disk_t;

TEST(CLASS, UpsertRead_Serial) {
  class Key {
   public:
    Key(uint64_t pt1, uint64_t pt2)
      : pt1_{ pt1 }
      , pt2_{ pt2 } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Key));
    }
    inline KeyHash GetHash() const {
      std::hash<uint64_t> hash_fn;
      return KeyHash{ hash_fn(pt1_) };
    }

    /// Comparison operators.
    inline bool operator==(const Key& other) const {
      return pt1_ == other.pt1_ &&
             pt2_ == other.pt2_;
    }
    inline bool operator!=(const Key& other) const {
      return pt1_ != other.pt1_ ||
             pt2_ != other.pt2_;
    }

   private:
    uint64_t pt1_;
    uint64_t pt2_;
  };

  class UpsertContext;
  class ReadContext;

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

   private:
    std::atomic<uint64_t> gen_;
    uint8_t value_[1014];
    uint16_t length_;
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
      std::memset(value.value_, val_, val_);
      value.length_ = val_;
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

      std::memset(value.value_, val_, val_);
      value.length_ = val_;
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

    ReadContext(Key key, uint8_t expected)
      : key_{ key }
      , expected_{ expected } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
      : key_{ other.key_ }
      , expected_{ other.expected_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      // This is a paging test, so we expect to read stuff from disk.
      ASSERT_EQ(expected_, value.length_);
      ASSERT_EQ(expected_, value.value_[expected_ - 5]);
    }
    inline void GetAtomic(const Value& value) {
      uint64_t post_gen = value.gen_.load();
      uint64_t pre_gen;
      uint16_t len;
      uint8_t val;
      do {
        // Pre- gen # for this read is last read's post- gen #.
        pre_gen = post_gen;
        len = value.length_;
        val = value.value_[len - 5];
        post_gen = value.gen_.load();
      } while(pre_gen != post_gen);
      ASSERT_EQ(expected_, static_cast<uint8_t>(len));
      ASSERT_EQ(expected_, val);
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint8_t expected_;
  };

  std::experimental::filesystem::create_directories("logs");

  // 8 pages!
  FasterKv<Key, Value, disk_t> store{ 262144, 268435456, "logs", 0.5 };

  Guid session_id = store.StartSession();

  constexpr size_t kNumRecords = 250000;

  // Insert.
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // Upserts don't go to disk.
      ASSERT_TRUE(false);
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    UpsertContext context{ Key{idx, idx}, 25 };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read.
  static std::atomic<uint64_t> records_read{ 0 };
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_read;
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    ReadContext context{ Key{ idx, idx}, 25 };
    Status result = store.Read(context, callback, 1);
    if(result == Status::Ok) {
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
      // Upserts don't go to disk.
      ASSERT_TRUE(false);
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    UpsertContext context{ Key{ idx, idx }, 87 };
    Status result = store.Upsert(context, callback, 1);
    if(result == Status::Ok) {
      ++records_updated;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  ASSERT_EQ(kNumRecords, records_updated.load());
  result = store.CompletePending(true);
  ASSERT_TRUE(result);

  // Read again.
  records_read = 0;;
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_read;
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    ReadContext context{ Key{ idx, idx }, 87 };
    Status result = store.Read(context, callback, 1);
    if(result == Status::Ok) {
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
}

TEST(CLASS, UpsertRead_Concurrent) {
  class UpsertContext;
  class ReadContext;

  class Key {
   public:
    Key(uint64_t pt1, uint64_t pt2)
      : pt1_{ pt1 }
      , pt2_{ pt2 } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Key));
    }
    inline KeyHash GetHash() const {
      std::hash<uint64_t> hash_fn;
      return KeyHash{ hash_fn(pt1_) };
    }

    /// Comparison operators.
    inline bool operator==(const Key& other) const {
      return pt1_ == other.pt1_ &&
             pt2_ == other.pt2_;
    }
    inline bool operator!=(const Key& other) const {
      return pt1_ != other.pt1_ ||
             pt2_ != other.pt2_;
    }

    friend class UpsertContext;
    friend class ReadContext;

   private:
    uint64_t pt1_;
    uint64_t pt2_;
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

   private:
    std::atomic<uint64_t> gen_;
    uint8_t value_[1014];
    uint16_t length_;
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
      std::memset(value.value_, val_, val_);
      value.length_ = val_;
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

      std::memset(value.value_, val_, val_);
      value.length_ = val_;
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

    ReadContext(Key key, uint8_t expected)
      : key_{ key }
      , expected_{ expected } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
      : key_{ other.key_ }
      , expected_{ other.expected_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      // This is a paging test, so we expect to read stuff from disk.
      ASSERT_EQ(expected_, value.length_);
      ASSERT_EQ(expected_, value.value_[expected_ - 5]);
    }
    inline void GetAtomic(const Value& value) {
      uint64_t post_gen = value.gen_.load();
      uint64_t pre_gen;
      uint16_t len;
      uint8_t val;
      do {
        // Pre- gen # for this read is last read's post- gen #.
        pre_gen = post_gen;
        len = value.length_;
        val = value.value_[len - 5];
        post_gen = value.gen_.load();
      } while(pre_gen != post_gen);
      ASSERT_EQ(expected_, val);
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint8_t expected_;
  };

  std::experimental::filesystem::create_directories("logs");

  // 8 pages!
  FasterKv<Key, Value, disk_t> store{ 262144, 268435456, "logs", 0.5 };

  static constexpr size_t kNumRecords = 250000;
  static constexpr size_t kNumThreads = 2;

  static std::atomic<uint64_t> num_writes{ 0 };

  auto upsert_worker = [](FasterKv<Key, Value, disk_t>* store_,
  size_t thread_idx, uint8_t val) {
    Guid session_id = store_->StartSession();

    for(size_t idx = 0; idx < kNumRecords / kNumThreads; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
      };

      if(idx % 256 == 0) {
        store_->Refresh();
      }

      uint64_t key_component = thread_idx * (kNumRecords / kNumThreads) + idx;
      UpsertContext context{ Key{ key_component, key_component }, val };
      Status result = store_->Upsert(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
      ++num_writes;
    }

    store_->StopSession();
  };

  // Insert.
  std::deque<std::thread> threads{};
  for(size_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(upsert_worker, &store, idx, 25);
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
      CallbackContext<ReadContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_read;
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    ReadContext context{ Key{ idx, idx }, 25 };
    Status result = store.Read(context, callback, 1);
    if(result == Status::Ok) {
      ++records_read;
    } else {
      ASSERT_EQ(Status::Pending, result) << idx;
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
    threads.emplace_back(upsert_worker, &store, idx, 87);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_EQ(kNumRecords, num_writes.load());

  // Restart session
  store.StartSession();

  // Delete some old copies of records (160 MB) that we no longer need.
  static constexpr uint64_t kNewBeginAddress{ 167772160L };
  static std::atomic<bool> truncated{ false };
  static std::atomic<bool> complete{ false };
  auto truncate_callback = [](uint64_t offset) {
    ASSERT_LE(offset, kNewBeginAddress);
    truncated = true;
  };
  auto complete_callback = []() {
    complete = true;
  };

  result = store.ShiftBeginAddress(Address{ kNewBeginAddress }, truncate_callback, complete_callback);
  ASSERT_TRUE(result);

  while(!truncated || !complete) {
    store.CompletePending(false);
  }

  // Read again.
  records_read = 0;;
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_read;
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    ReadContext context{ Key{ idx, idx }, 87 };
    Status result = store.Read(context, callback, 1);
    if(result == Status::Ok) {
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
}

TEST(CLASS, Rmw) {
  class Key {
   public:
    Key(uint64_t key)
      : key_{ key } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Key));
    }
    inline KeyHash GetHash() const {
      return KeyHash{ Utility::GetHashCode(key_) };
    }

    /// Comparison operators.
    inline bool operator==(const Key& other) const {
      return key_ == other.key_;
    }
    inline bool operator!=(const Key& other) const {
      return key_ != other.key_;
    }

   private:
    uint64_t key_;
  };

  class RmwContext;

  class Value {
   public:
    Value()
      : counter_{ 0 }
      , junk_{ 1 } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Value));
    }

    friend class RmwContext;

   private:
    std::atomic<uint64_t> counter_;
    uint8_t junk_[1016];
  };
  static_assert(sizeof(Value) == 1024, "sizeof(Value) != 1024");
  static_assert(alignof(Value) == 8, "alignof(Value) != 8");

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(Key key, uint64_t incr)
      : key_{ key }
      , incr_{ incr }
      , val_{ 0 } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , incr_{ other.incr_ }
      , val_{ other.val_ } {
    }

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
      value.counter_ = incr_;
      val_ = value.counter_;
    }
    inline void RmwCopy(const Value& old_value, Value& value) {
      value.counter_ = old_value.counter_ + incr_;
      val_ = value.counter_;
    }
    inline bool RmwAtomic(Value& value) {
      val_ = value.counter_.fetch_add(incr_) + incr_;
      return true;
    }

    inline uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint64_t incr_;

    uint64_t val_;
  };

  std::experimental::filesystem::create_directories("logs");

  // 8 pages!
  FasterKv<Key, Value, disk_t> store{ 262144, 268435456, "logs", 0.5 };

  Guid session_id = store.StartSession();

  constexpr size_t kNumRecords = 200000;

  // Initial RMW.
  static std::atomic<uint64_t> records_touched{ 0 };
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(3, context->val());
      ++records_touched;
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    RmwContext context{ Key{ idx }, 3 };
    Status result = store.Rmw(context, callback, 1);
    if(result == Status::Ok) {
      ASSERT_EQ(3, context.val());
      ++records_touched;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  bool result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_touched.load());

  // Second RMW.
  records_touched = 0;
  for(size_t idx = kNumRecords; idx > 0; --idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(8, context->val());
      ++records_touched;
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    RmwContext context{ Key{ idx - 1 }, 5 };
    Status result = store.Rmw(context, callback, 1);
    if(result == Status::Ok) {
      ASSERT_EQ(8, context.val()) << idx - 1;
      ++records_touched;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  ASSERT_LT(records_touched.load(), kNumRecords);
  result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_touched.load());

  store.StopSession();
}

TEST(CLASS, Rmw_Large) {
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

   private:
    uint64_t key_;
  };

  class RmwContext;

  class Value {
   public:
    Value()
      : counter_{ 0 }
      , junk_{ 1 } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Value));
    }

    friend class RmwContext;

   private:
    std::atomic<uint64_t> counter_;
    uint8_t junk_[8016];
  };
  static_assert(sizeof(Value) == 8024, "sizeof(Value) != 8024");
  static_assert(alignof(Value) == 8, "alignof(Value) != 8");

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(Key key, uint64_t incr)
      : key_{ key }
      , incr_{ incr }
      , val_{ 0 } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , incr_{ other.incr_ }
      , val_{ other.val_ } {
    }

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
      value.counter_ = incr_;
      val_ = value.counter_;
    }
    inline void RmwCopy(const Value& old_value, Value& value) {
      value.counter_ = old_value.counter_ + incr_;
      val_ = value.counter_;
    }
    inline bool RmwAtomic(Value& value) {
      val_ = value.counter_.fetch_add(incr_) + incr_;
      return true;
    }

    inline uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint64_t incr_;

    uint64_t val_;
  };

  std::experimental::filesystem::create_directories("logs");

  typedef FASTER::device::FileSystemDisk<handler_t, (1 << 30)> disk_t;
  FasterKv<Key, Value, disk_t> store { 2048, (1 << 20) * 192, "logs", 0.4 };

  Guid session_id = store.StartSession();

  constexpr size_t kNumRecords = 50000;

  // Initial RMW.
  static std::atomic<uint64_t> records_touched{ 0 };
  for(size_t idx = 0; idx < kNumRecords; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(3, context->val());
      ++records_touched;
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    RmwContext context{ Key{ idx }, 3 };
    Status result = store.Rmw(context, callback, 1);
    if(result == Status::Ok) {
      ASSERT_EQ(3, context.val());
      ++records_touched;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  bool result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_touched.load());

  // Second RMW.
  records_touched = 0;
  for(size_t idx = kNumRecords; idx > 0; --idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(8, context->val());
      ++records_touched;
    };

    if(idx % 256 == 0) {
      store.Refresh();
    }

    RmwContext context{ Key{ idx - 1 }, 5 };
    Status result = store.Rmw(context, callback, 1);
    if(result == Status::Ok) {
      ASSERT_EQ(8, context.val()) << idx - 1;
      ++records_touched;
    } else {
      ASSERT_EQ(Status::Pending, result);
    }
  }

  ASSERT_LT(records_touched.load(), kNumRecords);
  result = store.CompletePending(true);
  ASSERT_TRUE(result);
  ASSERT_EQ(kNumRecords, records_touched.load());

  store.StopSession();
}

TEST(CLASS, Rmw_Concurrent) {
  class Key {
   public:
    Key(uint64_t key)
      : key_{ key } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Key));
    }
    inline KeyHash GetHash() const {
      return KeyHash{ Utility::GetHashCode(key_) };
    }

    /// Comparison operators.
    inline bool operator==(const Key& other) const {
      return key_ == other.key_;
    }
    inline bool operator!=(const Key& other) const {
      return key_ != other.key_;
    }

   private:
    uint64_t key_;
  };

  class RmwContext;
  class ReadContext;

  class Value {
   public:
    Value()
      : counter_{ 0 }
      , junk_{ 1 } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Value));
    }

    friend class RmwContext;
    friend class ReadContext;

   private:
    std::atomic<uint64_t> counter_;
    uint8_t junk_[1016];
  };
  static_assert(sizeof(Value) == 1024, "sizeof(Value) != 1024");
  static_assert(alignof(Value) == 8, "alignof(Value) != 8");

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(Key key, uint64_t incr)
      : key_{ key }
      , incr_{ incr } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , incr_{ other.incr_ } {
    }

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
      value.counter_ = incr_;
    }
    inline void RmwCopy(const Value& old_value, Value& value) {
      value.counter_ = old_value.counter_ + incr_;
    }
    inline bool RmwAtomic(Value& value) {
      value.counter_.fetch_add(incr_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint64_t incr_;
  };

  class ReadContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(Key key)
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
      counter = value.counter_.load(std::memory_order_acquire);
    }
    inline void GetAtomic(const Value& value) {
      counter = value.counter_.load();
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
   public:
    uint64_t counter;
  };

  static constexpr size_t kNumRecords = 150000;
  static constexpr size_t kNumThreads = 2;

  auto rmw_worker = [](FasterKv<Key, Value, disk_t>* store_, uint64_t incr) {
    Guid session_id = store_->StartSession();
    for(size_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
      };

      if(idx % 256 == 0) {
        store_->Refresh();
      }

      RmwContext context{ Key{ idx }, incr };
      Status result = store_->Rmw(context, callback, 1);
      if(result != Status::Ok) {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    bool result = store_->CompletePending(true);
    ASSERT_TRUE(result);
    store_->StopSession();
  };

  auto read_worker1 = [](FasterKv<Key, Value, disk_t>* store_, size_t thread_idx) {
    Guid session_id = store_->StartSession();
    for(size_t idx = 0; idx < kNumRecords / kNumThreads; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(7 * kNumThreads, context->counter);
      };

      if(idx % 256 == 0) {
        store_->Refresh();
      }

      ReadContext context{ Key{ thread_idx* (kNumRecords / kNumThreads) + idx } };
      Status result = store_->Read(context, callback, 1);
      if(result == Status::Ok) {
        ASSERT_EQ(7 * kNumThreads, context.counter);
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    bool result = store_->CompletePending(true);
    ASSERT_TRUE(result);
    store_->StopSession();
  };

  auto read_worker2 = [](FasterKv<Key, Value, disk_t>* store_, size_t thread_idx) {
    Guid session_id = store_->StartSession();
    for(size_t idx = 0; idx < kNumRecords / kNumThreads; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(13 * kNumThreads, context->counter);
      };

      if(idx % 256 == 0) {
        store_->Refresh();
      }

      ReadContext context{ Key{ thread_idx* (kNumRecords / kNumThreads) + idx } };
      Status result = store_->Read(context, callback, 1);
      if(result == Status::Ok) {
        ASSERT_EQ(13 * kNumThreads, context.counter);
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    bool result = store_->CompletePending(true);
    ASSERT_TRUE(result);
    store_->StopSession();
  };

  std::experimental::filesystem::create_directories("logs");

  // 8 pages!
  FasterKv<Key, Value, disk_t> store{ 262144, 268435456, "logs", 0.5 };

  // Initial RMW.
  std::deque<std::thread> threads{};
  for(int64_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(rmw_worker, &store, 7);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  // Read.
  threads.clear();
  for(int64_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker1, &store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  // Second RMW.
  threads.clear();
  for(int64_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(rmw_worker, &store, 6);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  // Read again.
  threads.clear();
  for(int64_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker2, &store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }
}

TEST(CLASS, Rmw_Concurrent_Large) {
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

   private:
    uint64_t key_;
  };

  class RmwContext;
  class ReadContext;

  class Value {
   public:
    Value()
      : counter_{ 0 }
      , junk_{ 1 } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Value));
    }

    friend class RmwContext;
    friend class ReadContext;

   private:
    std::atomic<uint64_t> counter_;
    uint8_t junk_[8016];
  };
  static_assert(sizeof(Value) == 8024, "sizeof(Value) != 8024");
  static_assert(alignof(Value) == 8, "alignof(Value) != 8");

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(Key key, uint64_t incr)
      : key_{ key }
      , incr_{ incr } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , incr_{ other.incr_ } {
    }

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
      value.counter_ = incr_;
    }
    inline void RmwCopy(const Value& old_value, Value& value) {
      value.counter_ = old_value.counter_ + incr_;
    }
    inline bool RmwAtomic(Value& value) {
      value.counter_.fetch_add(incr_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint64_t incr_;
  };

  class ReadContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(Key key)
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
      counter = value.counter_.load(std::memory_order_acquire);
    }
    inline void GetAtomic(const Value& value) {
      counter = value.counter_.load();
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
   public:
    uint64_t counter;
  };

  typedef FASTER::device::FileSystemDisk<handler_t, (1 << 30)> disk_t;
  static constexpr size_t kNumRecords = 50000;
  static constexpr size_t kNumThreads = 2;

  auto rmw_worker = [](FasterKv<Key, Value, disk_t>* store_, uint64_t incr) {
    Guid session_id = store_->StartSession();
    for(size_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
      };

      if(idx % 256 == 0) {
        store_->Refresh();
      }

      RmwContext context{ Key{ idx }, incr };
      Status result = store_->Rmw(context, callback, 1);
      if(result != Status::Ok) {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    bool result = store_->CompletePending(true);
    ASSERT_TRUE(result);
    store_->StopSession();
  };

  auto read_worker1 = [](FasterKv<Key, Value, disk_t>* store_, size_t thread_idx) {
    Guid session_id = store_->StartSession();
    for(size_t idx = 0; idx < kNumRecords / kNumThreads; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(7 * kNumThreads, context->counter);
      };

      if(idx % 256 == 0) {
        store_->Refresh();
      }

      ReadContext context{ Key{ thread_idx* (kNumRecords / kNumThreads) + idx } };
      Status result = store_->Read(context, callback, 1);
      if(result == Status::Ok) {
        ASSERT_EQ(7 * kNumThreads, context.counter);
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    bool result = store_->CompletePending(true);
    ASSERT_TRUE(result);
    store_->StopSession();
  };

  auto read_worker2 = [](FasterKv<Key, Value, disk_t>* store_, size_t thread_idx) {
    Guid session_id = store_->StartSession();
    for(size_t idx = 0; idx < kNumRecords / kNumThreads; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(13 * kNumThreads, context->counter);
      };

      if(idx % 256 == 0) {
        store_->Refresh();
      }

      ReadContext context{ Key{ thread_idx* (kNumRecords / kNumThreads) + idx } };
      Status result = store_->Read(context, callback, 1);
      if(result == Status::Ok) {
        ASSERT_EQ(13 * kNumThreads, context.counter);
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    bool result = store_->CompletePending(true);
    ASSERT_TRUE(result);
    store_->StopSession();
  };

  std::experimental::filesystem::create_directories("logs");

  // 192 MB in memory -- rest on disk
  FasterKv<Key, Value, disk_t> store { 2048, (1 << 20) * 192, "logs", 0.4 };

  // Initial RMW.
  std::deque<std::thread> threads{};
  for(int64_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(rmw_worker, &store, 7);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  // Read.
  threads.clear();
  for(int64_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker1, &store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  // Second RMW.
  threads.clear();
  for(int64_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(rmw_worker, &store, 6);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  // Read again.
  threads.clear();
  for(int64_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker2, &store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }
}
