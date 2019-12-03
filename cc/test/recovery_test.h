// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <experimental/filesystem>

#include "test_types.h"

using namespace FASTER;
using FASTER::test::FixedSizeKey;
using FASTER::test::SimpleAtomicValue;

/// Disk's log uses 32 MB segments.
typedef FASTER::device::FileSystemDisk<handler_t, 33554432L> disk_t;
typedef FASTER::device::FileSystemFile<handler_t> file_t;

TEST(CLASS, MallocFixedPageSize) {
  typedef MallocFixedPageSize<HashBucket, disk_t> alloc_t;

  // Test copied from C#, RecoveryTest.cs.
  std::random_device rd{};
  uint32_t seed = rd();
  std::mt19937_64 rng{ seed };
  std::experimental::filesystem::create_directories("test_ofb");

  size_t num_bytes_written;

  LightEpoch epoch;
  alloc_t allocator{};
  allocator.Initialize(512, epoch);

  size_t num_buckets_to_add = 2 * FixedPage<HashBucket>::kPageSize + 5;

  FixedPageAddress* buckets = new FixedPageAddress[num_buckets_to_add];

  {
    disk_t checkpoint_disk{ "test_ofb", epoch };
    file_t checkpoint_file = checkpoint_disk.NewFile("test_ofb.dat");
    Status result = checkpoint_file.Open(&checkpoint_disk.handler());
    ASSERT_EQ(Status::Ok, result);

    //do something
    for(size_t bucket_idx = 0; bucket_idx < num_buckets_to_add; ++bucket_idx) {
      buckets[bucket_idx] = allocator.Allocate();
      HashBucket& bucket = allocator.Get(buckets[bucket_idx]);
      for(size_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
        HashBucketEntry expected{ 0 };
        uint64_t random_num = rng();
        bool success = bucket.entries[entry_idx].compare_exchange_strong(expected, random_num);
        ASSERT_TRUE(success);
      }
      HashBucketOverflowEntry expected{ 0 };
      uint64_t random_num = rng();
      bool success = bucket.overflow_entry.compare_exchange_strong(expected, random_num);
      ASSERT_TRUE(success);
    }
    //issue call to checkpoint
    result = allocator.Checkpoint(checkpoint_disk, std::move(checkpoint_file), num_bytes_written);
    ASSERT_EQ(Status::Ok, result);
    // (All the bucket we allocated, + the null page.)
    ASSERT_EQ((num_buckets_to_add + 1) * sizeof(HashBucket), num_bytes_written);
    //wait until complete
    result = allocator.CheckpointComplete(true);
    ASSERT_EQ(Status::Ok, result);
  }

  LightEpoch recover_epoch;
  alloc_t recover_allocator{};
  recover_allocator.Initialize(512, recover_epoch);
  disk_t recover_disk{ "test_ofb", recover_epoch };
  file_t recover_file = recover_disk.NewFile("test_ofb.dat");
  Status result = recover_file.Open(&recover_disk.handler());
  ASSERT_EQ(Status::Ok, result);

  //issue call to recover
  result = recover_allocator.Recover(recover_disk, std::move(recover_file), num_bytes_written,
                                     num_bytes_written / sizeof(typename alloc_t::item_t));
  ASSERT_EQ(Status::Ok, result);
  //wait until complete
  result = recover_allocator.RecoverComplete(true);
  ASSERT_EQ(Status::Ok, result);

  //verify that something
  std::mt19937_64 rng2{ seed };
  for(size_t bucket_idx = 0; bucket_idx < num_buckets_to_add; ++bucket_idx) {
    HashBucket& bucket = allocator.Get(buckets[bucket_idx]);
    for(size_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      uint64_t random_num = rng2();
      ASSERT_EQ(random_num, bucket.entries[entry_idx].load().control_);
    }
    uint64_t random_num = rng2();
    ASSERT_EQ(random_num, bucket.overflow_entry.load().control_);
  }

  FixedPageAddress address = recover_allocator.Allocate();
  ASSERT_EQ(FixedPageAddress{ num_buckets_to_add + 1 }, address);

  delete[] buckets;
}

TEST(CLASS, InternalHashTable) {
  // (Just the hash table itself--no overflow buckets.)
  std::random_device rd{};
  uint32_t seed = rd();
  std::mt19937_64 rng{ seed };
  std::experimental::filesystem::create_directories("test_ht");

  constexpr uint64_t kNumBuckets = 8388608/8;
  size_t num_bytes_written;
  {
    LightEpoch epoch;
    disk_t checkpoint_disk{ "test_ht", epoch };
    file_t checkpoint_file = checkpoint_disk.NewFile("test_ht.dat");
    Status result = checkpoint_file.Open(&checkpoint_disk.handler());
    ASSERT_EQ(Status::Ok, result);

    InternalHashTable<disk_t> table{};
    table.Initialize(kNumBuckets, checkpoint_file.alignment());

    //do something
    for(size_t bucket_idx = 0; bucket_idx < kNumBuckets; ++bucket_idx) {
      for(size_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
        HashBucketEntry expected{ 0 };
        bool success = table.bucket(bucket_idx).entries[entry_idx].compare_exchange_strong(
                         expected, rng());
        ASSERT_TRUE(success);
      }
      HashBucketOverflowEntry expected{ 0 };
      bool success = table.bucket(bucket_idx).overflow_entry.compare_exchange_strong(expected,
                     rng());
      ASSERT_TRUE(success);
    }

    //issue call to checkpoint
    result = table.Checkpoint(checkpoint_disk, std::move(checkpoint_file), num_bytes_written);
    ASSERT_EQ(Status::Ok, result);
    // (All the bucket we allocated, + the null page.)
    ASSERT_EQ(kNumBuckets * sizeof(HashBucket), num_bytes_written);
    //wait until complete
    result = table.CheckpointComplete(true);
    ASSERT_EQ(Status::Ok, result);
  }

  LightEpoch epoch;
  disk_t recover_disk{ "test_ht", epoch };
  file_t recover_file = recover_disk.NewFile("test_ht.dat");
  Status result = recover_file.Open(&recover_disk.handler());
  ASSERT_EQ(Status::Ok, result);

  InternalHashTable<disk_t> recover_table{};
  //issue call to recover
  result = recover_table.Recover(recover_disk, std::move(recover_file), num_bytes_written);
  ASSERT_EQ(Status::Ok, result);
  //wait until complete
  result = recover_table.RecoverComplete(true);
  ASSERT_EQ(Status::Ok, result);

  //verify that something
  std::mt19937_64 rng2{ seed };
  for(size_t bucket_idx = 0; bucket_idx < kNumBuckets; ++bucket_idx) {
    for(size_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      uint64_t random_num = rng2();
      ASSERT_EQ(random_num, recover_table.bucket(bucket_idx).entries[entry_idx].load().control_);
    }
    uint64_t random_num = rng2();
    ASSERT_EQ(random_num, recover_table.bucket(bucket_idx).overflow_entry.load().control_);
  }
}

TEST(CLASS, Serial) {
  class Key {
   public:
    Key(uint32_t key)
      : key_{ key } {
    }

    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Key));
    }
    inline KeyHash GetHash() const {
      std::hash<uint32_t> hash_fn{};
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
    uint32_t key_;
  };
  static_assert(sizeof(Key) == 4, "sizeof(Key) != 4");
  static_assert(alignof(Key) == 4, "alignof(Key) != 4");

  class UpsertContext1;
  class UpsertContext2;
  class ReadContext1;
  class ReadContext2;

  class Value1 {
   public:
    inline uint32_t size() const {
      return size_;
    }

    friend class UpsertContext1;
    friend class UpsertContext2;
    friend class ReadContext1;

   private:
    uint16_t size_;
    union {
      std::atomic<uint32_t> atomic_val1_;
      uint32_t val1_;
    };
  };
  static_assert(sizeof(Value1) == 8, "sizeof(Value1) != 8");
  static_assert(alignof(Value1) == 4, "alignof(Value1) != 4");

  class Value2 : public Value1 {
   public:
    friend class UpsertContext2;
    friend class ReadContext2;

   private:
    union {
      std::atomic<uint16_t> atomic_val2_;
      uint16_t val2_;
    };
    uint8_t wasted_space[3];
  };
  static_assert(sizeof(Value2) == 16, "sizeof(Value2) != 16");
  static_assert(alignof(Value2) == 4, "alignof(Value2) != 4");

  class UpsertContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value1 value_t;

    UpsertContext1(const Key& key, uint32_t val)
      : key_{ key }
      , val_{ val } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext1(const UpsertContext1& other)
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
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value1& value) {
      value.size_ = sizeof(value);
      value.val1_ = val_;
    }
    inline bool PutAtomic(Value1& value) {
      EXPECT_EQ(value.size_, sizeof(value));
      value.atomic_val1_.store(val_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
  };

  class UpsertContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value2 value_t;

    UpsertContext2(const Key& key, uint16_t val)
      : key_{ key }
      , val_{ val } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext2(const UpsertContext2& other)
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
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value2& value) {
      value.size_ = sizeof(value);
      value.val2_ = val_;
    }
    inline bool PutAtomic(Value2& value) {
      EXPECT_EQ(value.size_, sizeof(value));
      value.atomic_val2_.store(val_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint16_t val_;
  };

  class ReadContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value1 value_t;

    ReadContext1(Key key, uint32_t expected_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext1(const ReadContext1& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value1& value) {
      val_ = value.val1_;
    }
    inline void GetAtomic(const Value1& value) {
      val_ = value.atomic_val1_.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
  };

  class ReadContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value2 value_t;

    ReadContext2(Key key, uint16_t expected_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext2(const ReadContext2& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value2& value) {
      val_ = value.val2_;
    }
    inline void GetAtomic(const Value2& value) {
      val_ = value.atomic_val2_.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint16_t val_;
   public:
    const uint16_t expected;
  };

  auto upsert_callback = [](IAsyncContext* context, Status result) {
    // Upserts don't go to disk.
    ASSERT_TRUE(false);
  };

  std::experimental::filesystem::create_directories("storage");

  static constexpr size_t kNumRecords = 600000;

  Guid session_id;
  Guid token;

  {
    // Populate and checkpoint the store.
    // 6 pages!
    FasterKv<Key, Value1, disk_t> store{ 524288, 201326592, "storage", 0.4 };

    session_id = store.StartSession();

    // upsert some records
    assert(kNumRecords % 2 == 0);
    for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
      {
        UpsertContext1 context{ Key{ idx }, idx + 7 };
        Status result = store.Upsert(context, upsert_callback, 1);
        ASSERT_EQ(Status::Ok, result);
      }
      {
        UpsertContext2 context{ Key{ idx + 1 }, 55 };
        Status result = store.Upsert(context, upsert_callback, 1);
        ASSERT_EQ(Status::Ok, result);
      }
    }
    // verify them
    static std::atomic<uint64_t> records_read;
    records_read = 0;
    for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
      auto callback1 = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext1> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ++records_read;
        ASSERT_EQ(context->expected, context->val());
      };
      auto callback2 = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext2> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ++records_read;
        ASSERT_EQ(context->expected, context->val());
      };

      if(idx % 256 == 0) {
        store.Refresh();
        store.CompletePending(false);
      }

      {
        ReadContext1 context{ Key{ idx }, idx + 7 };
        Status result = store.Read(context, callback1, 1);
        if(result == Status::Ok) {
          ++records_read;
          ASSERT_EQ(context.expected, context.val());
        } else {
          ASSERT_EQ(Status::Pending, result);
        }
      }
      {
        ReadContext2 context{ Key{ idx + 1 }, 55 };
        Status result = store.Read(context, callback2, 1);
        if(result == Status::Ok) {
          ++records_read;
          ASSERT_EQ(context.expected, context.val());
        } else {
          ASSERT_EQ(Status::Pending, result);
        }
      }
    }

    static std::atomic<size_t> num_threads_persistent;
    num_threads_persistent = 0;
    static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads];
    for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
      threads_persistent[idx] = false;
    }

    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
      bool expected = false;
      ASSERT_EQ(Status::Ok, result);
      ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected,
                  true));
      ++num_threads_persistent;
    };

    // checkpoint (transition from REST to INDEX_CHKPT)
    ASSERT_TRUE(store.Checkpoint(nullptr, hybrid_log_persistence_callback, token));

    while(num_threads_persistent < 1) {
      store.CompletePending(false);
    }

    bool result = store.CompletePending(true);
    ASSERT_TRUE(result);
    ASSERT_EQ(kNumRecords, records_read.load());

    store.StopSession();
  }

  // Test recovery.
  FasterKv<Key, Value1, disk_t> new_store{ 524288, 201326592, "storage", 0.4 };

  uint32_t version;
  std::vector<Guid> session_ids;
  Status status = new_store.Recover(token, token, version, session_ids);
  ASSERT_EQ(Status::Ok, status);
  ASSERT_EQ(1, session_ids.size());
  ASSERT_EQ(session_id, session_ids[0]);
  ASSERT_EQ(1, new_store.ContinueSession(session_id));

  // Verify the recovered store.
  static std::atomic<uint64_t> records_read;
  records_read = 0;
  for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
    auto callback1 = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext1> context{ ctxt };
      ASSERT_EQ(Status::Ok, result) << *reinterpret_cast<const uint32_t*>(&context->key());
      ++records_read;
      ASSERT_EQ(context->expected, context->val());
    };
    auto callback2 = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      ASSERT_EQ(Status::Ok, result) << *reinterpret_cast<const uint32_t*>(&context->key());
      ++records_read;
      ASSERT_EQ(context->expected, context->val());
    };

    if(idx % 256 == 0) {
      new_store.Refresh();
      new_store.CompletePending(false);
    }

    {
      ReadContext1 context{ Key{ idx }, idx + 7 };
      Status result = new_store.Read(context, callback1, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    {
      ReadContext2 context{ Key{ idx + 1 }, 55 };
      Status result = new_store.Read(context, callback2, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
  }

  new_store.CompletePending(true);
  ASSERT_EQ(records_read.load(), kNumRecords);
  new_store.StopSession();

  session_id = new_store.StartSession();

  // Upsert some changes and verify them.
  for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
    {
      UpsertContext1 context{ Key{ idx }, idx + 55 };
      Status result = new_store.Upsert(context, upsert_callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }
    {
      UpsertContext2 context{ Key{ idx + 1 }, 77 };
      Status result = new_store.Upsert(context, upsert_callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }
  }
  records_read = 0;
  for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
    auto callback1 = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext1> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_read;
      ASSERT_EQ(context->expected, context->val());
    };
    auto callback2 = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_read;
      ASSERT_EQ(context->expected, context->val());
    };

    if(idx % 256 == 0) {
      new_store.Refresh();
      new_store.CompletePending(false);
    }

    {
      ReadContext1 context{ Key{ idx }, idx + 55 };
      Status result = new_store.Read(context, callback1, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    {
      ReadContext2 context{ Key{ idx + 1 }, 77 };
      Status result = new_store.Read(context, callback2, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
  }

  new_store.CompletePending(true);
  ASSERT_EQ(records_read.load(), kNumRecords);
  new_store.StopSession();
}

TEST(CLASS, Serial_VariableLengthKey) {
  class alignas(4) Key {
   public:
    Key(uint8_t len, uint32_t fill)
      : len_{ len } {
      for(uint8_t idx = 0; idx < len_; ++idx) {
        buffer()[idx] = fill;
      }
    }

    /// Copy constructor.
    Key(const Key& other)
      : len_{ other.len_ } {
      std::memcpy(buffer(), other.buffer(), len_ * sizeof(uint32_t));
    }

    inline uint32_t size() const {
      return sizeof(*this) + (len_ * sizeof(uint32_t));
    }
   private:
    inline uint32_t* buffer() {
      return reinterpret_cast<uint32_t*>(this + 1);
    }
   public:
    inline const uint32_t* buffer() const {
      return reinterpret_cast<const uint32_t*>(this + 1);
    }
    inline KeyHash GetHash() const {
      return KeyHash{ Utility::HashBytes(
                        reinterpret_cast<const uint16_t*>(buffer()), len_ * 2) };
    }

    /// Comparison operators.
    inline bool operator==(const Key& other) const {
      return len_ == other.len_ &&
             std::memcmp(buffer(), other.buffer(), len_ * sizeof(uint32_t)) == 0;
    }
    inline bool operator!=(const Key& other) const {
      return len_ != other.len_ ||
             std::memcmp(buffer(), other.buffer(), len_ * sizeof(uint32_t)) != 0;
    }

   private:
    uint8_t len_;

  };
  static_assert(sizeof(Key) == 4, "sizeof(Key) != 4");
  static_assert(alignof(Key) == 4, "alignof(Key) != 4");

  class UpsertContext1;
  class UpsertContext2;
  class ReadContext1;
  class ReadContext2;

  class Value1 {
   public:
    inline uint32_t size() const {
      return size_;
    }

    friend class UpsertContext1;
    friend class UpsertContext2;
    friend class ReadContext1;

   private:
    uint16_t size_;
    union {
      std::atomic<uint32_t> atomic_val1_;
      uint32_t val1_;
    };
  };
  static_assert(sizeof(Value1) == 8, "sizeof(Value1) != 8");
  static_assert(alignof(Value1) == 4, "alignof(Value1) != 4");

  class Value2 : public Value1 {
   public:
    friend class UpsertContext2;
    friend class ReadContext2;

   private:
    union {
      std::atomic<uint16_t> atomic_val2_;
      uint16_t val2_;
    };
    uint8_t wasted_space[3];
  };
  static_assert(sizeof(Value2) == 16, "sizeof(Value2) != 16");
  static_assert(alignof(Value2) == 4, "alignof(Value2) != 4");

  class UpsertContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value1 value_t;

    UpsertContext1(uint32_t key, uint32_t val)
      : val_{ val } {
      uint8_t len = (key % 16) + 1;
      key_ = alloc_context<key_t>(sizeof(key_t) + (len * sizeof(uint32_t)));
      new(key_.get()) key_t{ len, key };
    }

    /// Deep-copy constructor.
    UpsertContext1(UpsertContext1& other)
      : key_{ std::move(other.key_) }
      , val_{ other.val_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return *key_.get();
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value1& value) {
      value.size_ = sizeof(value);
      value.val1_ = val_;
    }
    inline bool PutAtomic(Value1& value) {
      EXPECT_EQ(value.size_, sizeof(value));
      value.atomic_val1_.store(val_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    context_unique_ptr_t<key_t> key_;
    uint32_t val_;
  };

  class UpsertContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value2 value_t;

    UpsertContext2(uint32_t key, uint16_t val)
      : val_{ val } {
      uint8_t len = (key % 16) + 1;
      key_ = alloc_context<key_t>(sizeof(key_t) + (len * sizeof(uint32_t)));
      new(key_.get()) key_t{ len, key };
    }

    /// Deep-copy constructor.
    UpsertContext2(UpsertContext2& other)
      : key_{ std::move(other.key_) }
      , val_{ other.val_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return *key_.get();
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value2& value) {
      value.size_ = sizeof(value);
      value.val2_ = val_;
    }
    inline bool PutAtomic(Value2& value) {
      EXPECT_EQ(value.size_, sizeof(value));
      value.atomic_val2_.store(val_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    context_unique_ptr_t<key_t> key_;
    uint16_t val_;
  };

  class ReadContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value1 value_t;

    ReadContext1(uint32_t key, uint32_t expected_)
      : val_{ 0 }
      , expected{ expected_ } {
      uint8_t len = (key % 16) + 1;
      key_ = alloc_context<key_t>(sizeof(key_t) + (len * sizeof(uint32_t)));
      new(key_.get()) key_t{ len, key };
    }

    /// Deep-copy constructor.
    ReadContext1(ReadContext1& other)
      : key_{ std::move(other.key_) }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return *key_.get();
    }

    inline void Get(const Value1& value) {
      val_ = value.val1_;
    }
    inline void GetAtomic(const Value1& value) {
      val_ = value.atomic_val1_.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    context_unique_ptr_t<key_t> key_;
    uint32_t val_;
   public:
    const uint32_t expected;
  };

  class ReadContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value2 value_t;

    ReadContext2(uint32_t key, uint16_t expected_)
      : val_{ 0 }
      , expected{ expected_ } {
      uint8_t len = (key % 16) + 1;
      key_ = alloc_context<key_t>(sizeof(key_t) + (len * sizeof(uint32_t)));
      new(key_.get()) key_t{ len, key };
    }

    /// Deep-copy constructor.
    ReadContext2(ReadContext2& other)
      : key_{ std::move(other.key_) }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return *key_.get();
    }

    inline void Get(const Value2& value) {
      val_ = value.val2_;
    }
    inline void GetAtomic(const Value2& value) {
      val_ = value.atomic_val2_.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    context_unique_ptr_t<key_t> key_;
    uint16_t val_;
   public:
    const uint16_t expected;
  };

  auto upsert_callback = [](IAsyncContext* context, Status result) {
    // Upserts don't go to disk.
    ASSERT_TRUE(false);
  };

  std::experimental::filesystem::create_directories("storage");

  static constexpr size_t kNumRecords = 600000;

  Guid session_id;
  Guid token;

  {
    // Populate and checkpoint the store.
    // 6 pages!
    FasterKv<Key, Value1, disk_t> store{ 524288, 201326592, "storage", 0.4 };

    session_id = store.StartSession();

    // upsert some records
    assert(kNumRecords % 2 == 0);
    for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
      {
        UpsertContext1 context{ idx, idx + 7 };
        Status result = store.Upsert(context, upsert_callback, 1);
        ASSERT_EQ(Status::Ok, result);
      }
      {
        UpsertContext2 context{ idx + 1, 55 };
        Status result = store.Upsert(context, upsert_callback, 1);
        ASSERT_EQ(Status::Ok, result);
      }
    }
    // verify them
    static std::atomic<uint64_t> records_read;
    records_read = 0;
    for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
      auto callback1 = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext1> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ++records_read;
        ASSERT_EQ(context->expected, context->val());
      };
      auto callback2 = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext2> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ++records_read;
        ASSERT_EQ(context->expected, context->val());
      };

      if(idx % 256 == 0) {
        store.Refresh();
        store.CompletePending(false);
      }

      {
        ReadContext1 context{ idx, idx + 7 };
        Status result = store.Read(context, callback1, 1);
        if(result == Status::Ok) {
          ++records_read;
          ASSERT_EQ(context.expected, context.val());
        } else {
          ASSERT_EQ(Status::Pending, result);
        }
      }
      {
        ReadContext2 context{ idx + 1, 55 };
        Status result = store.Read(context, callback2, 1);
        if(result == Status::Ok) {
          ++records_read;
          ASSERT_EQ(context.expected, context.val());
        } else {
          ASSERT_EQ(Status::Pending, result);
        }
      }
    }

    static std::atomic<size_t> num_threads_persistent;
    num_threads_persistent = 0;
    static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads];
    for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
      threads_persistent[idx] = false;
    }

    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
      bool expected = false;
      ASSERT_EQ(Status::Ok, result);
      ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected,
                  true));
      ++num_threads_persistent;
    };

    // checkpoint (transition from REST to INDEX_CHKPT)
    ASSERT_TRUE(store.Checkpoint(nullptr, hybrid_log_persistence_callback, token));

    while(num_threads_persistent < 1) {
      store.CompletePending(false);
    }

    bool result = store.CompletePending(true);
    ASSERT_TRUE(result);
    ASSERT_EQ(kNumRecords, records_read.load());

    store.StopSession();
  }

  // Test recovery.
  FasterKv<Key, Value1, disk_t> new_store{ 524288, 201326592, "storage", 0.4 };

  uint32_t version;
  std::vector<Guid> session_ids;
  Status status = new_store.Recover(token, token, version, session_ids);
  ASSERT_EQ(Status::Ok, status);
  ASSERT_EQ(1, session_ids.size());
  ASSERT_EQ(session_id, session_ids[0]);
  ASSERT_EQ(1, new_store.ContinueSession(session_id));

  // Verify the recovered store.
  static std::atomic<uint64_t> records_read;
  records_read = 0;
  for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
    auto callback1 = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext1> context{ ctxt };
      ASSERT_EQ(Status::Ok, result) << *reinterpret_cast<const uint32_t*>(&context->key());
      ++records_read;
      ASSERT_EQ(context->expected, context->val());
    };
    auto callback2 = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      ASSERT_EQ(Status::Ok, result) << *reinterpret_cast<const uint32_t*>(&context->key());
      ++records_read;
      ASSERT_EQ(context->expected, context->val());
    };

    if(idx % 256 == 0) {
      new_store.Refresh();
      new_store.CompletePending(false);
    }

    {
      ReadContext1 context{ idx, idx + 7 };
      Status result = new_store.Read(context, callback1, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    {
      ReadContext2 context{ idx + 1, 55 };
      Status result = new_store.Read(context, callback2, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
  }

  new_store.CompletePending(true);
  ASSERT_EQ(records_read.load(), kNumRecords);
  new_store.StopSession();

  session_id = new_store.StartSession();

  // Upsert some changes and verify them.
  for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
    {
      UpsertContext1 context{ idx, idx + 55 };
      Status result = new_store.Upsert(context, upsert_callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }
    {
      UpsertContext2 context{ idx + 1, 77 };
      Status result = new_store.Upsert(context, upsert_callback, 1);
      ASSERT_EQ(Status::Ok, result);
    }
  }
  records_read = 0;
  for(uint32_t idx = 0; idx < kNumRecords; idx += 2) {
    auto callback1 = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext1> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_read;
      ASSERT_EQ(context->expected, context->val());
    };
    auto callback2 = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      ++records_read;
      ASSERT_EQ(context->expected, context->val());
    };

    if(idx % 256 == 0) {
      new_store.Refresh();
      new_store.CompletePending(false);
    }

    {
      ReadContext1 context{ idx, idx + 55 };
      Status result = new_store.Read(context, callback1, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    {
      ReadContext2 context{ idx + 1, 77 };
      Status result = new_store.Read(context, callback2, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
    }
  }

  new_store.CompletePending(true);
  ASSERT_EQ(records_read.load(), kNumRecords);
  new_store.StopSession();
}

TEST(CLASS, Concurrent_Insert_Small) {
  using Key = FixedSizeKey<uint32_t>;
  using Value = SimpleAtomicValue<uint32_t>;

  class UpsertContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(const Key& key, uint32_t val)
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
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.value = val_;
    }
    inline bool PutAtomic(Value& value) {
      value.atomic_value.store(val_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
  };

  static auto upsert_callback = [](IAsyncContext* context, Status result) {
    // Upserts don't go to disk.
    ASSERT_TRUE(false);
  };

  std::experimental::filesystem::create_directories("storage");

  static constexpr uint32_t kNumRecords = 500000;
  static constexpr uint32_t kNumThreads = 2;
  static constexpr uint32_t kNumRecordsPerThread = kNumRecords / kNumThreads;

  static Guid session_ids[kNumThreads];
  std::memset(session_ids, 0, sizeof(session_ids));
  static Guid token;

  static std::atomic<uint32_t> num_threads_persistent;
  num_threads_persistent = 0;
  static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads];
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    threads_persistent[idx] = false;
  }

  static std::atomic<uint32_t> num_threads_started;
  num_threads_started = 0;

  static auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
    bool expected = false;
    ASSERT_EQ(Status::Ok, result);
    ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected, true));
    ++num_threads_persistent;
  };

  typedef FasterKv<Key, Value, disk_t> store_t;

  class ReadContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext1(Key key, uint32_t expected_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext1(const ReadContext1& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
  };

  {
    // Populate and checkpoint the store.

    // 6 pages!
    store_t store{ 8192, 201326592, "storage", 0.4 };

    auto upsert_checkpoint_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id == 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // upsert some records
      for(uint32_t idx =  kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        UpsertContext context{ Key{ idx }, idx + 7 };

        Status result = store->Upsert(context, upsert_callback, 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
        }
      }

      while(num_threads_started < kNumThreads) {
        std::this_thread::yield();
      }
      // checkpoint (transition from REST to INDEX_CHKPT)
      ASSERT_TRUE(store->Checkpoint(nullptr, hybrid_log_persistence_callback, token));

      // Ensure that the checkpoint completes.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    auto upsert_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id != 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // upsert some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        UpsertContext context{ Key{ idx }, idx + 7 };
        Status result = store->Upsert(context, upsert_callback, 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
        }
      }

      // Don't exit this session until the checkpoint has completed.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    std::deque<std::thread> threads{};
    threads.emplace_back(upsert_checkpoint_worker, &store, 0);
    for(uint32_t idx = 1; idx < kNumThreads; ++idx) {
      threads.emplace_back(upsert_worker, &store, idx);
    }
    for(auto& thread : threads) {
      thread.join();
    }

    // Verify the store.
    store.StartSession();

    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext1> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->expected, context->val());
      };

      ReadContext1 context{ Key{ idx }, idx + 7 };
      Status result = store.Read(context, callback, 1);
      if(result != Status::Ok) {
        ASSERT_EQ(Status::Pending, result);
      }
    }

    store.StopSession();
  }

  // Test recovery.
  store_t new_store{ 8192, 201326592, "storage", 0.4 };

  uint32_t version;
  std::vector<Guid> recovered_session_ids;
  Status status = new_store.Recover(token, token, version, recovered_session_ids);
  ASSERT_EQ(recovered_session_ids.size(), kNumThreads);
  ASSERT_EQ(Status::Ok, status);

  static std::atomic<uint32_t> records_read;
  records_read = 0;

  class ReadContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext2(Key key, uint32_t expected_, uint32_t idx_, std::atomic<bool>* found_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ }
      , idx{ idx_ }
      , found{ found_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext2(const ReadContext2& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected }
      , idx{ other.idx }
      , found{ other.found } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
    const uint32_t idx;
    std::atomic<bool>* found;
  };

  auto read_worker = [](store_t* store, uint32_t thread_id) {
    uint64_t serial_num = store->ContinueSession(session_ids[thread_id]);
    ASSERT_EQ(1, serial_num);

    std::unique_ptr<std::atomic<bool>> found{ new std::atomic<bool>[kNumRecordsPerThread] };
    std::memset(found.get(), 0, sizeof(found.get()[0]) * kNumRecordsPerThread);

    // verify records
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context->expected, context->val());
        bool expected = false;
        ASSERT_TRUE(context->found[context->idx].compare_exchange_strong(expected, true));
      } else {
        ASSERT_EQ(Status::NotFound, result);
        ASSERT_FALSE(context->found[context->idx].load());
      }
    };
    for(uint32_t idx = kNumRecordsPerThread * thread_id;
        idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
      ReadContext2 context{ Key{ idx }, idx + 7, idx - (kNumRecordsPerThread * thread_id),
                            found.get() };
      Status result = store->Read(context, callback, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
        bool expected = false;
        ASSERT_TRUE(found.get()[context.idx].compare_exchange_strong(expected, true));
      } else {
        ASSERT_TRUE(result == Status::Pending || result == Status::NotFound);
        if(result == Status::NotFound) {
          ASSERT_FALSE(found.get()[context.idx].load());
        }
      }

      if(idx % 256 == 0) {
        store->Refresh();
        store->CompletePending(false);
      }
    }
    store->CompletePending(true);
    store->StopSession();

    bool found_all = true;
    for(uint32_t idx = 0; idx < kNumRecordsPerThread; ++idx) {
      if(found_all != found.get()[idx]) {
        // Consistent-point recovery implies that after one record isn't found, all subsequent
        // records will not be found.
        Key key{ kNumRecordsPerThread* thread_id + idx };
        KeyHash hash = key.GetHash();
        std::string error;
        error += "key = ";
        error += std::to_string(kNumRecordsPerThread* thread_id + idx);
        error += ", idx = ";
        error += std::to_string(hash.idx(8192));
        error += ", tag = ";
        error += std::to_string(hash.tag());
        ASSERT_TRUE(found_all) << error;
        found_all = false;
      }
    }
  };

  std::deque<std::thread> threads{};
  for(uint32_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker, &new_store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_GT(records_read, (uint32_t)0);
  ASSERT_LE(records_read, kNumRecords);
}

TEST(CLASS, Concurrent_Insert_Large) {
  using Key = FixedSizeKey<uint32_t>;
  using Value = SimpleAtomicValue<uint32_t>;

  class UpsertContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(const Key& key, uint32_t val)
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
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.value = val_;
    }
    inline bool PutAtomic(Value& value) {
      value.atomic_value.store(val_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
  };

  static auto upsert_callback = [](IAsyncContext* context, Status result) {
    // Upserts don't go to disk.
    ASSERT_TRUE(false);
  };

  std::experimental::filesystem::create_directories("storage");

  static constexpr uint32_t kNumRecords = 1000000;
  static constexpr uint32_t kNumThreads = 2;
  static constexpr uint32_t kNumRecordsPerThread = kNumRecords / kNumThreads;

  static Guid session_ids[kNumThreads];
  std::memset(session_ids, 0, sizeof(session_ids));
  static Guid token;

  static std::atomic<uint32_t> num_threads_persistent;
  num_threads_persistent = 0;
  static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads];
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    threads_persistent[idx] = false;
  }

  static std::atomic<uint32_t> num_threads_started;
  num_threads_started = 0;

  static auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
    bool expected = false;
    ASSERT_EQ(Status::Ok, result);
    ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected, true));
    ++num_threads_persistent;
  };

  typedef FasterKv<Key, Value, disk_t> store_t;

  class ReadContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext1(Key key, uint32_t expected_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext1(const ReadContext1& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
  };

  {
    // Populate and checkpoint the store.

    // 6 pages!
    store_t store{ 524288, 201326592, "storage", 0.4 };

    auto upsert_checkpoint_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id == 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // upsert some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        UpsertContext context{ Key{ idx }, idx + 7 };

        Status result = store->Upsert(context, upsert_callback, 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
        }
      }

      while(num_threads_started < kNumThreads) {
        std::this_thread::yield();
      }
      // checkpoint (transition from REST to INDEX_CHKPT)
      ASSERT_TRUE(store->Checkpoint(nullptr, hybrid_log_persistence_callback, token));

      // Ensure that the checkpoint completes.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    auto upsert_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id != 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // upsert some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        UpsertContext context{ Key{ idx }, idx + 7 };
        Status result = store->Upsert(context, upsert_callback, 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
        }
      }

      // Don't exit this session until the checkpoint has completed.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    std::deque<std::thread> threads{};
    threads.emplace_back(upsert_checkpoint_worker, &store, 0);
    for(uint32_t idx = 1; idx < kNumThreads; ++idx) {
      threads.emplace_back(upsert_worker, &store, idx);
    }
    for(auto& thread : threads) {
      thread.join();
    }

    // Verify the store.
    store.StartSession();
    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext1> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->expected, context->val());
      };

      ReadContext1 context{ Key{ idx }, idx + 7 };
      Status result = store.Read(context, callback, 1);
      if(result != Status::Ok) {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    store.StopSession();
  }

  // Test recovery.
  store_t new_store{ 524288, 201326592, "storage", 0.4 };

  uint32_t version;
  std::vector<Guid> recovered_session_ids;
  Status status = new_store.Recover(token, token, version, recovered_session_ids);
  ASSERT_EQ(recovered_session_ids.size(), kNumThreads);
  ASSERT_EQ(Status::Ok, status);

  static std::atomic<uint32_t> records_read;
  records_read = 0;

  class ReadContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext2(Key key, uint32_t expected_, uint32_t idx_, std::atomic<bool>* found_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ }
      , idx{ idx_ }
      , found{ found_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext2(const ReadContext2& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected }
      , idx{ other.idx }
      , found{ other.found } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
    const uint32_t idx;
    std::atomic<bool>* found;
  };

  auto read_worker = [](store_t* store, uint32_t thread_id) {
    uint64_t serial_num = store->ContinueSession(session_ids[thread_id]);
    ASSERT_EQ(1, serial_num);

    std::unique_ptr<std::atomic<bool>> found{ new std::atomic<bool>[kNumRecordsPerThread] };
    std::memset(found.get(), 0, sizeof(found.get()[0]) * kNumRecordsPerThread);

    // verify records
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context->expected, context->val());
        bool expected = false;
        ASSERT_TRUE(context->found[context->idx].compare_exchange_strong(expected, true));
      } else {
        ASSERT_EQ(Status::NotFound, result);
        ASSERT_FALSE(context->found[context->idx].load());
      }
    };
    for(uint32_t idx = kNumRecordsPerThread * thread_id;
        idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
      ReadContext2 context{ Key{ idx }, idx + 7, idx - (kNumRecordsPerThread * thread_id),
                            found.get() };
      Status result = store->Read(context, callback, 1);
      if(result == Status::Ok) {
        ++records_read;
        ASSERT_EQ(context.expected, context.val());
        bool expected = false;
        ASSERT_TRUE(found.get()[context.idx].compare_exchange_strong(expected, true));
      } else {
        ASSERT_TRUE(result == Status::Pending || result == Status::NotFound);
        if(result == Status::NotFound) {
          ASSERT_FALSE(found.get()[context.idx].load());
        }
      }

      if(idx % 256 == 0) {
        store->Refresh();
        store->CompletePending(false);
      }
    }
    store->CompletePending(true);
    store->StopSession();

    bool found_all = true;
    for(uint32_t idx = 0; idx < kNumRecordsPerThread; ++idx) {
      if(found_all != found.get()[idx]) {
        // Consistent-point recovery implies that after one record isn't found, all subsequent
        // records will not be found.
        Key key{ kNumRecordsPerThread* thread_id + idx };
        KeyHash hash = key.GetHash();
        std::string error;
        error += "key = ";
        error += std::to_string(kNumRecordsPerThread* thread_id + idx);
        error += ", idx = ";
        error += std::to_string(hash.idx(8192));
        error += ", tag = ";
        error += std::to_string(hash.tag());
        ASSERT_TRUE(found_all) << error;
        found_all = false;
      }
    }
  };

  std::deque<std::thread> threads{};
  for(uint32_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker, &new_store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_GT(records_read, (uint32_t)0);
  ASSERT_LE(records_read, kNumRecords);
}

TEST(CLASS, Concurrent_Update_Small) {
  using Key = FixedSizeKey<uint32_t>;
  using Value = SimpleAtomicValue<uint32_t>;

  class UpsertContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(const Key& key, uint32_t val)
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
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.value = val_;
    }
    inline bool PutAtomic(Value& value) {
      value.atomic_value.store(val_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
  };

  static auto upsert_callback = [](IAsyncContext* context, Status result) {
    // Upserts don't go to disk.
    ASSERT_TRUE(false);
  };

  std::experimental::filesystem::create_directories("storage");

  static constexpr uint32_t kNumRecords = 200000;
  static constexpr uint32_t kNumThreads = 2;
  static constexpr uint32_t kNumRecordsPerThread = kNumRecords / kNumThreads;

  static Guid session_ids[kNumThreads];
  std::memset(session_ids, 0, sizeof(session_ids));
  static Guid token;

  static std::atomic<uint32_t> num_threads_persistent;
  num_threads_persistent = 0;
  static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads];
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    threads_persistent[idx] = false;
  }

  static std::atomic<uint32_t> num_threads_started;
  num_threads_started = 0;

  static auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
    bool expected = false;
    ASSERT_EQ(Status::Ok, result);
    ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected, true));
    ++num_threads_persistent;
  };

  typedef FasterKv<Key, Value, disk_t> store_t;

  class ReadContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext1(Key key, uint32_t expected_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext1(const ReadContext1& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
  };

  {
    // 6 pages!
    store_t store{ 8192, 201326592, "storage", 0.4 };

    // Populate the store.
    store.StartSession();
    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      UpsertContext context{ Key{ idx }, 999 };
      Status result = store.Upsert(context, upsert_callback, 1);
      ASSERT_EQ(Status::Ok, result);
      if(idx % 256 == 0) {
        store.Refresh();
        store.CompletePending(false);
      }
    }
    store.StopSession();

    /// Update and checkpoint the store.
    auto upsert_checkpoint_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id == 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // update some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        UpsertContext context{ Key{ idx }, idx + 1 };

        Status result = store->Upsert(context, upsert_callback, idx + 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
        }
      }

      while(num_threads_started < kNumThreads) {
        std::this_thread::yield();
      }
      // checkpoint (transition from REST to INDEX_CHKPT)
      ASSERT_TRUE(store->Checkpoint(nullptr, hybrid_log_persistence_callback, token));

      // Ensure that the checkpoint completes.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    auto upsert_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id != 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // update some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        UpsertContext context{ Key{ idx }, idx + 1 };
        Status result = store->Upsert(context, upsert_callback, idx + 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
        }
      }

      // Don't exit this session until the checkpoint has completed.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    std::deque<std::thread> threads{};
    threads.emplace_back(upsert_checkpoint_worker, &store, 0);
    for(uint32_t idx = 1; idx < kNumThreads; ++idx) {
      threads.emplace_back(upsert_worker, &store, idx);
    }
    for(auto& thread : threads) {
      thread.join();
    }

    // Verify the store.
    store.StartSession();
    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext1> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->expected, context->val());
      };

      ReadContext1 context{ Key{ idx }, idx + 1 };
      Status result = store.Read(context, callback, 1);
      if(result != Status::Ok) {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    store.StopSession();
  }

  // Test recovery.
  store_t new_store{ 8192, 201326592, "storage", 0.4 };

  uint32_t version;
  std::vector<Guid> recovered_session_ids;
  Status status = new_store.Recover(token, token, version, recovered_session_ids);
  ASSERT_EQ(recovered_session_ids.size(), kNumThreads);
  ASSERT_EQ(Status::Ok, status);

  static std::atomic<uint32_t> records_read;
  records_read = 0;

  class ReadContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext2(Key key, uint32_t expected_, uint32_t idx_, std::atomic<bool>* found_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ }
      , idx{ idx_ }
      , found{ found_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext2(const ReadContext2& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected }
      , idx{ other.idx }
      , found{ other.found } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
    const uint32_t idx;
    std::atomic<bool>* found;
  };

  auto read_worker = [](store_t* store, uint32_t thread_id) {
    uint64_t serial_num = store->ContinueSession(session_ids[thread_id]);
    ASSERT_GE(serial_num, 1);

    std::unique_ptr<std::atomic<bool>> found{ new std::atomic<bool>[kNumRecordsPerThread] };
    std::memset(found.get(), 0, sizeof(found.get()[0]) * kNumRecordsPerThread);

    // verify records
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      if(context->expected == context->val()) {
        bool expected = false;
        ASSERT_TRUE(context->found[context->idx].compare_exchange_strong(expected, true));
      } else {
        ASSERT_EQ(999, context->val());
        bool expected = false;
        ASSERT_FALSE(context->found[context->idx].load());
      }
    };
    for(uint32_t idx = kNumRecordsPerThread * thread_id;
        idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
      ReadContext2 context{ Key{ idx }, idx + 1, idx - (kNumRecordsPerThread * thread_id),
                            found.get() };
      Status result = store->Read(context, callback, 1);
      if(result == Status::Ok) {
        ++records_read;
        if(context.expected == context.val()) {
          bool expected = false;
          ASSERT_TRUE(found.get()[context.idx].compare_exchange_strong(expected, true));
        } else {
          ASSERT_EQ(999, context.val());
          bool expected = false;
          ASSERT_FALSE(found.get()[context.idx].load());
        }
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
      if(idx % 256 == 0) {
        store->Refresh();
        store->CompletePending(false);
      }
    }
    store->CompletePending(true);
    store->StopSession();

    bool found_all = true;
    for(uint32_t idx = 0; idx < kNumRecordsPerThread; ++idx) {
      if(found_all != found.get()[idx]) {
        // Consistent-point recovery implies that after one record isn't found, all subsequent
        // records will not be found.
        Key key{ kNumRecordsPerThread* thread_id + idx };
        KeyHash hash = key.GetHash();
        std::string error;
        error += "key = ";
        error += std::to_string(kNumRecordsPerThread* thread_id + idx);
        error += ", idx = ";
        error += std::to_string(hash.idx(8192));
        error += ", tag = ";
        error += std::to_string(hash.tag());
        ASSERT_TRUE(found_all) << error;
        found_all = false;
      }
    }
  };

  std::deque<std::thread> threads{};
  for(uint32_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker, &new_store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_GT(records_read, (uint32_t)0);
  ASSERT_LE(records_read, kNumRecords);
}

TEST(CLASS, Concurrent_Update_Large) {
  using Key = FixedSizeKey<uint32_t>;
  using Value = SimpleAtomicValue<uint32_t>;

  class UpsertContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(const Key& key, uint32_t val)
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
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.value = val_;
    }
    inline bool PutAtomic(Value& value) {
      value.atomic_value.store(val_);
      return true;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
  };

  static auto upsert_callback = [](IAsyncContext* context, Status result) {
    // Upserts don't go to disk.
    ASSERT_TRUE(false);
  };

  std::experimental::filesystem::create_directories("storage");

  static constexpr uint32_t kNumRecords = 1000000;
  static constexpr uint32_t kNumThreads = 2;
  static constexpr uint32_t kNumRecordsPerThread = kNumRecords / kNumThreads;

  static Guid session_ids[kNumThreads];
  std::memset(session_ids, 0, sizeof(session_ids));
  static Guid index_token;
  static Guid hybrid_log_token;

  static std::atomic<bool> index_checkpoint_completed;
  index_checkpoint_completed = false;
  static std::atomic<uint32_t> num_threads_persistent;
  num_threads_persistent = 0;
  static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads];
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    threads_persistent[idx] = false;
  }

  static std::atomic<uint32_t> num_threads_started;
  num_threads_started = 0;

  static auto index_persistence_callback = [](Status result) {
    ASSERT_EQ(Status::Ok, result);
    index_checkpoint_completed = true;
  };

  static auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
    bool expected = false;
    ASSERT_EQ(Status::Ok, result);
    ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected, true));
    ++num_threads_persistent;
  };

  typedef FasterKv<Key, Value, disk_t> store_t;

  class ReadContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext1(Key key, uint32_t expected_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext1(const ReadContext1& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
  };

  {
    // 6 pages!
    store_t store{ 524288, 201326592, "storage", 0.4 };

    // Populate the store.
    store.StartSession();
    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      UpsertContext context{ Key{ idx }, 999 };
      Status result = store.Upsert(context, upsert_callback, 1);
      ASSERT_EQ(Status::Ok, result);
      if(idx % 256 == 0) {
        store.Refresh();
        store.CompletePending(false);
      }
    }

    store.StopSession();

    /// Update and checkpoint the store.
    auto upsert_checkpoint_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id == 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // update some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        UpsertContext context{ Key{ idx }, idx + 1 };

        Status result = store->Upsert(context, upsert_callback, idx + 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
        }
      }

      while(num_threads_started < kNumThreads) {
        std::this_thread::yield();
      }
      // checkpoint the index (transition from REST to INDEX_CHKPT)
      ASSERT_TRUE(store->CheckpointIndex(index_persistence_callback, index_token));
      // Ensure that the index checkpoint completes.
      while(!index_checkpoint_completed) {
        store->CompletePending(false);
      }
	  store->CompletePending(false);

      // checkpoint the hybrid log (transition from REST to PREPARE)
      ASSERT_TRUE(store->CheckpointHybridLog(hybrid_log_persistence_callback, hybrid_log_token));
      // Ensure that the hybrid-log checkpoint completes.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    auto upsert_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id != 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // update some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        UpsertContext context{ Key{ idx }, idx + 1 };
        Status result = store->Upsert(context, upsert_callback, idx + 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
        }
      }

      // Don't exit this session until the checkpoint has completed.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    std::deque<std::thread> threads{};
    threads.emplace_back(upsert_checkpoint_worker, &store, 0);
    for(uint32_t idx = 1; idx < kNumThreads; ++idx) {
      threads.emplace_back(upsert_worker, &store, idx);
    }
    for(auto& thread : threads) {
      thread.join();
    }

    // Verify the store.
    store.StartSession();
    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext1> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->expected, context->val());
      };

      ReadContext1 context{ Key{ idx }, idx + 1 };
      Status result = store.Read(context, callback, 1);
      if(result != Status::Ok) {
        ASSERT_EQ(Status::Pending, result);
      }
      if(idx % 256 == 0) {
        store.Refresh();
        store.CompletePending(false);
      }
    }

    bool result = store.CompletePending(true);
    ASSERT_TRUE(result);
    store.StopSession();
  }

  // Test recovery.
  store_t new_store{ 524288, 201326592, "storage", 0.4 };

  uint32_t version;
  std::vector<Guid> recovered_session_ids;
  Status status = new_store.Recover(index_token, hybrid_log_token, version, recovered_session_ids);
  ASSERT_EQ(recovered_session_ids.size(), kNumThreads);
  ASSERT_EQ(Status::Ok, status);

  static std::atomic<uint32_t> records_read;
  records_read = 0;

  class ReadContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext2(Key key, uint32_t expected_, uint32_t idx_, std::atomic<bool>* found_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ }
      , idx{ idx_ }
      , found{ found_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext2(const ReadContext2& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected }
      , idx{ other.idx }
      , found{ other.found } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
    const uint32_t idx;
    std::atomic<bool>* found;
  };

  auto read_worker = [](store_t* store, uint32_t thread_id) {
    uint64_t serial_num = store->ContinueSession(session_ids[thread_id]);
    ASSERT_GE(serial_num, 1);

    std::unique_ptr<std::atomic<bool>> found{ new std::atomic<bool>[kNumRecordsPerThread] };
    std::memset(found.get(), 0, sizeof(found.get()[0]) * kNumRecordsPerThread);

    // verify records
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      if(context->expected == context->val()) {
        bool expected = false;
        ASSERT_TRUE(context->found[context->idx].compare_exchange_strong(expected, true));
      } else {
        ASSERT_EQ(999, context->val());
        bool expected = false;
        ASSERT_FALSE(context->found[context->idx].load());
      }
    };
    for(uint32_t idx = kNumRecordsPerThread * thread_id;
        idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
      ReadContext2 context{ Key{ idx }, idx + 1, idx - (kNumRecordsPerThread * thread_id),
                            found.get() };
      Status result = store->Read(context, callback, 1);
      if(result == Status::Ok) {
        ++records_read;
        if(context.expected == context.val()) {
          bool expected = false;
          ASSERT_TRUE(found.get()[context.idx].compare_exchange_strong(expected, true));
        } else {
          ASSERT_EQ(999, context.val());
          bool expected = false;
          ASSERT_FALSE(found.get()[context.idx].load());
        }
      } else {
        ASSERT_EQ(Status::Pending, result) << idx;
      }
      if(idx % 256 == 0) {
        store->Refresh();
        store->CompletePending(false);
      }
    }
    store->CompletePending(true);
    store->StopSession();

    bool found_all = true;
    for(uint32_t idx = 0; idx < kNumRecordsPerThread; ++idx) {
      if(found_all != found.get()[idx]) {
        // Consistent-point recovery implies that after one record isn't found, all subsequent
        // records will not be found.
        Key key{ kNumRecordsPerThread* thread_id + idx };
        KeyHash hash = key.GetHash();
        std::string error;
        error += "key = ";
        error += std::to_string(kNumRecordsPerThread* thread_id + idx);
        error += ", idx = ";
        error += std::to_string(hash.idx(8192));
        error += ", tag = ";
        error += std::to_string(hash.tag());
        ASSERT_TRUE(found_all) << error;
        found_all = false;
      }
    }
  };

  std::deque<std::thread> threads{};
  for(uint32_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker, &new_store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_GT(records_read, (uint32_t)0);
  ASSERT_LE(records_read, kNumRecords);
}

TEST(CLASS, Concurrent_Rmw_Small) {
  using Key = FixedSizeKey<uint32_t>;
  using Value = SimpleAtomicValue<uint32_t>;

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(const Key& key, uint32_t delta)
      : key_{ key }
      , delta_{ delta } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , delta_{ other.delta_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }
    inline static constexpr uint32_t value_size(const value_t& old_value) {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void RmwInitial(Value& value) {
      value.value = key_.key;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.value = old_value.value + delta_;
    }
    inline bool RmwAtomic(value_t& value) {
      value.atomic_value += delta_;
      return true;
    }
   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t delta_;
  };

  std::experimental::filesystem::create_directories("storage");

  static constexpr uint32_t kNumRecords = 200000;
  static constexpr uint32_t kNumThreads = 2;
  static constexpr uint32_t kNumRecordsPerThread = kNumRecords / kNumThreads;

  static Guid session_ids[kNumThreads];
  std::memset(session_ids, 0, sizeof(session_ids));
  static Guid token;

  static std::atomic<uint32_t> num_threads_persistent;
  num_threads_persistent = 0;
  static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads] = {};
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    threads_persistent[idx] = false;
  }

  static std::atomic<uint32_t> num_threads_started;
  num_threads_started = 0;

  static auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
    bool expected = false;
    ASSERT_EQ(Status::Ok, result);
    ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected, true));
    ++num_threads_persistent;
  };

  typedef FasterKv<Key, Value, disk_t> store_t;

  class ReadContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext1(Key key, uint32_t expected_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext1(const ReadContext1& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
  };

  {
    // 6 pages!
    store_t store{ 8192, 402653184, "storage", 0.4 };

    // Populate the store.
    store.StartSession();
    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* context, Status result) {
        ASSERT_EQ(Status::Ok, result);
      };

      RmwContext context{ Key{ idx }, 230 };
      Status result = store.Rmw(context, callback, 1);
      ASSERT_EQ(Status::Ok, result);
      if(idx % 256 == 0) {
        store.Refresh();
        store.CompletePending(false);
      }
    }
    store.StopSession();

    /// Read-modify-write and checkpoint the store.
    auto rmw_checkpoint_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id == 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // read-modify-write some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        auto callback = [](IAsyncContext* context, Status result) {
          ASSERT_EQ(Status::Ok, result);
        };
        RmwContext context{ Key{ idx }, 230 };
        Status result = store->Rmw(context, callback, idx + 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
          store->CompletePending(false);
        }
      }

      while(num_threads_started < kNumThreads) {
        std::this_thread::yield();
      }
      // checkpoint (transition from REST to INDEX_CHKPT)
      ASSERT_TRUE(store->Checkpoint(nullptr, hybrid_log_persistence_callback, token));

      // Ensure that the checkpoint completes.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    auto rmw_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id != 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // update some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        auto callback = [](IAsyncContext* context, Status result) {
          ASSERT_EQ(Status::Ok, result);
        };
        RmwContext context{ Key{ idx }, 230 };
        Status result = store->Rmw(context, callback, idx + 1);
        ASSERT_EQ(Status::Ok, result);

        if(idx % 256 == 0) {
          store->Refresh();
          store->CompletePending(false);
        }
      }

      // Don't exit this session until the checkpoint has completed.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    std::deque<std::thread> threads{};
    threads.emplace_back(rmw_checkpoint_worker, &store, 0);
    for(uint32_t idx = 1; idx < kNumThreads; ++idx) {
      threads.emplace_back(rmw_worker, &store, idx);
    }
    for(auto& thread : threads) {
      thread.join();
    }

    // Verify the store.
    store.StartSession();
    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext1> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->expected, context->val());
      };

      ReadContext1 context{ Key{ idx }, idx + 230 };
      Status result = store.Read(context, callback, 1);
      if(result != Status::Ok) {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    store.StopSession();
  }

  // Test recovery.
  store_t new_store{ 8192, 402653184, "storage", 0.4 };

  uint32_t version;
  std::vector<Guid> recovered_session_ids;
  Status status = new_store.Recover(token, token, version, recovered_session_ids);
  ASSERT_EQ(recovered_session_ids.size(), kNumThreads);
  ASSERT_EQ(Status::Ok, status);

  static std::atomic<uint32_t> records_read;
  records_read = 0;

  class ReadContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext2(Key key, uint32_t expected_, uint32_t idx_, std::atomic<bool>* found_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ }
      , idx{ idx_ }
      , found{ found_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext2(const ReadContext2& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected }
      , idx{ other.idx }
      , found{ other.found } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
    const uint32_t idx;
    std::atomic<bool>* found;
  };

  auto read_worker = [](store_t* store, uint32_t thread_id) {
    uint64_t serial_num = store->ContinueSession(session_ids[thread_id]);
    ASSERT_GE(serial_num, 1);

    std::unique_ptr<std::atomic<bool>> found{ new std::atomic<bool>[kNumRecordsPerThread] };
    std::memset(found.get(), 0, sizeof(found.get()[0]) * kNumRecordsPerThread);

    // verify records
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      if(context->expected == context->val()) {
        bool expected = false;
        ASSERT_TRUE(context->found[context->idx].compare_exchange_strong(expected, true));
      } else {
        ASSERT_EQ(context->expected - 230, context->val());
        bool expected = false;
        ASSERT_FALSE(context->found[context->idx].load());
      }
    };
    for(uint32_t idx = kNumRecordsPerThread * thread_id;
        idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
      ReadContext2 context{ Key{ idx }, idx + 230, idx - (kNumRecordsPerThread * thread_id),
                            found.get() };
      Status result = store->Read(context, callback, 1);
      if(result == Status::Ok) {
        ++records_read;
        if(context.expected == context.val()) {
          bool expected = false;
          ASSERT_TRUE(found.get()[context.idx].compare_exchange_strong(expected, true));
        } else {
          ASSERT_EQ(idx, context.val());
          bool expected = false;
          ASSERT_FALSE(found.get()[context.idx].load());
        }
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
      if(idx % 256 == 0) {
        store->Refresh();
        store->CompletePending(false);
      }
    }
    store->CompletePending(true);
    store->StopSession();

    bool found_all = true;
    for(uint32_t idx = 0; idx < kNumRecordsPerThread; ++idx) {
      if(found_all != found.get()[idx]) {
        // Consistent-point recovery implies that after one record isn't found, all subsequent
        // records will not be found.
        Key key{ kNumRecordsPerThread* thread_id + idx };
        KeyHash hash = key.GetHash();
        std::string error;
        error += "key = ";
        error += std::to_string(kNumRecordsPerThread* thread_id + idx);
        error += ", idx = ";
        error += std::to_string(hash.idx(8192));
        error += ", tag = ";
        error += std::to_string(hash.tag());
        ASSERT_TRUE(found_all) << error;
        found_all = false;
      }
    }
  };

  std::deque<std::thread> threads{};
  for(uint32_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker, &new_store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_GT(records_read, (uint32_t)0);
  ASSERT_LE(records_read, kNumRecords);
}

TEST(CLASS, Concurrent_Rmw_Large) {
  using Key = FixedSizeKey<uint32_t>;
  using Value = SimpleAtomicValue<uint32_t>;

  class RmwContext : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(const Key& key, uint32_t delta)
      : key_{ key }
      , delta_{ delta } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , delta_{ other.delta_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }
    inline static constexpr uint32_t value_size(const value_t& old_value) {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void RmwInitial(Value& value) {
      value.value = key_.key;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.value = old_value.value + delta_;
    }
    inline bool RmwAtomic(value_t& value) {
      value.atomic_value += delta_;
      return true;
    }
   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t delta_;
  };

  std::experimental::filesystem::create_directories("storage");

  static constexpr uint32_t kNumRecords = 1000000;
  static constexpr uint32_t kNumThreads = 2;
  static_assert(kNumRecords % kNumThreads == 0, "kNumRecords % kNumThreads != 0");
  static constexpr uint32_t kNumRecordsPerThread = kNumRecords / kNumThreads;

  static Guid session_ids[kNumThreads];
  std::memset(session_ids, 0, sizeof(session_ids));
  static Guid token;

  static std::atomic<uint32_t> num_threads_persistent;
  num_threads_persistent = 0;
  static std::atomic<bool> threads_persistent[Thread::kMaxNumThreads];
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    threads_persistent[idx] = false;
  }

  static std::atomic<uint32_t> num_threads_started;
  num_threads_started = 0;

  static auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
    bool expected = false;
    ASSERT_EQ(Status::Ok, result);
    ASSERT_TRUE(threads_persistent[Thread::id()].compare_exchange_strong(expected, true));
    ++num_threads_persistent;
  };

  typedef FasterKv<Key, Value, disk_t> store_t;

  class ReadContext1 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext1(Key key, uint32_t expected_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext1(const ReadContext1& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
  };

  {
    // 6 pages!
    store_t store{ 524288, 402653184, "storage", 0.4 };

    // Populate the store.
    auto populate_worker0 = [](store_t* store, uint32_t thread_id) {
      store->StartSession();
      auto callback = [](IAsyncContext* context, Status result) {
        ASSERT_EQ(Status::Ok, result);
      };
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        RmwContext context{ Key{ idx }, 230 };
        Status result = store->Rmw(context, callback, 1);
        ASSERT_EQ(Status::Ok, result);
        if(idx % 256 == 0) {
          store->Refresh();
          store->CompletePending(false);
        }
      }
      store->GrowIndex(nullptr);
      store->StopSession();
    };
    auto populate_worker = [](store_t* store, uint32_t thread_id) {
      store->StartSession();
      auto callback = [](IAsyncContext* context, Status result) {
        ASSERT_EQ(Status::Ok, result);
      };
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        RmwContext context{ Key{ idx }, 230 };
        Status result = store->Rmw(context, callback, 1);
        ASSERT_EQ(Status::Ok, result);
        if(idx % 256 == 0) {
          store->Refresh();
          store->CompletePending(false);
        }
      }
      store->StopSession();
    };

    std::deque<std::thread> threads{};
    threads.emplace_back(populate_worker0, &store, 0);
    for(uint32_t idx = 1; idx < kNumThreads; ++idx) {
      threads.emplace_back(populate_worker, &store, idx);
    }
    for(auto& thread : threads) {
      thread.join();
    }

    /// Read-modify-write and checkpoint the store.
    auto rmw_checkpoint_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id == 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // read-modify-write some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        auto callback = [](IAsyncContext* context, Status result) {
          ASSERT_EQ(Status::Ok, result);
        };
        RmwContext context{ Key{ idx }, 230 };
        Status result = store->Rmw(context, callback, idx + 1);
        ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
        if(idx % 256 == 0) {
          store->Refresh();
          store->CompletePending(false);
        }
      }

      while(num_threads_started < kNumThreads) {
        std::this_thread::yield();
      }
      // checkpoint (transition from REST to INDEX_CHKPT)
      ASSERT_TRUE(store->Checkpoint(nullptr, hybrid_log_persistence_callback, token));

      // Ensure that the checkpoint completes.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    auto rmw_worker = [](store_t* store, uint32_t thread_id) {
      assert(thread_id != 0);
      session_ids[thread_id] = store->StartSession();
      ++num_threads_started;

      // update some records
      for(uint32_t idx = kNumRecordsPerThread * thread_id;
          idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
        auto callback = [](IAsyncContext* context, Status result) {
          ASSERT_EQ(Status::Ok, result);
        };
        RmwContext context{ Key{ idx }, 230 };
        Status result = store->Rmw(context, callback, idx + 1);
        ASSERT_TRUE(result == Status::Ok || result == Status::Pending);
        if(idx % 256 == 0) {
          store->Refresh();
          store->CompletePending(false);
        }
      }

      // Don't exit this session until the checkpoint has completed.
      while(num_threads_persistent < kNumThreads) {
        store->CompletePending(false);
      }

      bool result = store->CompletePending(true);
      ASSERT_TRUE(result);
      store->StopSession();
    };

    threads.clear();
    threads.emplace_back(rmw_checkpoint_worker, &store, 0);
    for(uint32_t idx = 1; idx < kNumThreads; ++idx) {
      threads.emplace_back(rmw_worker, &store, idx);
    }
    for(auto& thread : threads) {
      thread.join();
    }

    // Verify the store.
    store.StartSession();
    for(uint32_t idx = 0; idx < kNumRecords; ++idx) {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext1> context{ ctxt };
        ASSERT_EQ(Status::Ok, result);
        ASSERT_EQ(context->expected, context->val());
      };

      ReadContext1 context{ Key{ idx }, idx + 230 };
      Status result = store.Read(context, callback, 1);
      if(result != Status::Ok) {
        ASSERT_EQ(Status::Pending, result);
      }
    }
    store.StopSession();
  }

  // Test recovery.
  store_t new_store{ 524288 * 2, 402653184, "storage", 0.4 };

  uint32_t version;
  std::vector<Guid> recovered_session_ids;
  Status status = new_store.Recover(token, token, version, recovered_session_ids);
  ASSERT_EQ(recovered_session_ids.size(), kNumThreads);
  ASSERT_EQ(Status::Ok, status);

  static std::atomic<uint32_t> records_read;
  records_read = 0;

  class ReadContext2 : public IAsyncContext {
   public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext2(Key key, uint32_t expected_, uint32_t idx_, std::atomic<bool>* found_)
      : key_{ key }
      , val_{ 0 }
      , expected{ expected_ }
      , idx{ idx_ }
      , found{ found_ } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext2(const ReadContext2& other)
      : key_{ other.key_ }
      , val_{ other.val_ }
      , expected{ other.expected }
      , idx{ other.idx }
      , found{ other.found } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      val_ = value.value;
    }
    inline void GetAtomic(const Value& value) {
      val_ = value.atomic_value.load();
    }

    uint64_t val() const {
      return val_;
    }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    Key key_;
    uint32_t val_;
   public:
    const uint32_t expected;
    const uint32_t idx;
    std::atomic<bool>* found;
  };

  auto read_worker = [](store_t* store, uint32_t thread_id) {
    uint64_t serial_num = store->ContinueSession(session_ids[thread_id]);
    ASSERT_GE(serial_num, 1);

    std::unique_ptr<std::atomic<bool>> found{ new std::atomic<bool>[kNumRecordsPerThread] };
    std::memset(found.get(), 0, sizeof(found.get()[0]) * kNumRecordsPerThread);

    // verify records
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext2> context{ ctxt };
      ASSERT_EQ(Status::Ok, result);
      if(context->expected == context->val()) {
        bool expected = false;
        ASSERT_TRUE(context->found[context->idx].compare_exchange_strong(expected, true));
      } else {
        ASSERT_EQ(context->expected - 230, context->val());
        bool expected = false;
        ASSERT_FALSE(context->found[context->idx].load());
      }
    };
    for(uint32_t idx = kNumRecordsPerThread * thread_id;
        idx < kNumRecordsPerThread * (thread_id + 1); ++idx) {
      ReadContext2 context{ Key{ idx }, idx + 230, idx - (kNumRecordsPerThread * thread_id),
                            found.get() };
      Status result = store->Read(context, callback, 1);
      if(result == Status::Ok) {
        ++records_read;
        if(context.expected == context.val()) {
          bool expected = false;
          ASSERT_TRUE(found.get()[context.idx].compare_exchange_strong(expected, true));
        } else {
          ASSERT_EQ(idx, context.val());
          bool expected = false;
          ASSERT_FALSE(found.get()[context.idx].load());
        }
      } else {
        ASSERT_EQ(Status::Pending, result);
      }
      if(idx % 256 == 0) {
        store->Refresh();
        store->CompletePending(false);
      }
    }
    store->CompletePending(true);
    store->StopSession();

    bool found_all = true;
    for(uint32_t idx = 0; idx < kNumRecordsPerThread; ++idx) {
      if(found_all != found.get()[idx]) {
        // Consistent-point recovery implies that after one record isn't found, all subsequent
        // records will not be found.
        Key key{ kNumRecordsPerThread* thread_id + idx };
        KeyHash hash = key.GetHash();
        std::string error;
        error += "key = ";
        error += std::to_string(kNumRecordsPerThread* thread_id + idx);
        error += ", idx = ";
        error += std::to_string(hash.idx(8192));
        error += ", tag = ";
        error += std::to_string(hash.tag());
        ASSERT_TRUE(found_all) << error;
        found_all = false;
      }
    }
  };

  std::deque<std::thread> threads{};
  for(uint32_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(read_worker, &new_store, idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  ASSERT_GT(records_read, (uint32_t)0);
  ASSERT_LE(records_read, kNumRecords);
}
