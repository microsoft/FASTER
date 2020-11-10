// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"

#include "core/alloc.h"
#include "core/faster.h"
#include "core/light_epoch.h"

#include "device/azure.h"
#include "device/file_system_disk.h"
#include "device/storage.h"

#include "environment/file.h"

using namespace FASTER::device;
using namespace FASTER::environment;

/// Tests if a StorageFile can be opened and closed.
TEST(StorageTest, OpenClose) {
  // Typedef on the async IO handler for convenience.
  typedef QueueIoHandler handler_t;

  // Typedef on the remote tier for convenience.
  typedef BlobFile remote_t;

  // Create a disk. The hybrid log file is implicitly opened here.
  LightEpoch epoch;
  StorageDevice<handler_t, remote_t> disk(std::string(".") + kPathSeparator,
                                          epoch,
                                          "UseDevelopmentStorage=true;");

  // Assert that the hybrid log closes fine.
  auto hLog = std::move(disk.log());
  ASSERT_EQ(Status::Ok, hLog.Close());
}

/// Tests if data once written can be read from the Storage interface.
TEST(StorageTest, ReadWrite) {
  // Typedef on the async IO handler for convenience.
  typedef QueueIoHandler handler_t;

  // Typedef on the remote tier for convenience.
  typedef BlobFile remote_t;

  // Create a disk. The hybrid log file is implicitly opened here.
  LightEpoch epoch;
  StorageDevice<handler_t, remote_t> disk(std::string(".") + kPathSeparator,
                                          epoch,
                                          "UseDevelopmentStorage=true;");

  // Handle to the StorageFile holding the log.
  auto hLog = std::move(disk.log());

  // Skeleton context required to issue writes and reads to StorageFile.
  class Context : public IAsyncContext {
   public:
    Context() {}

    /// Copy (and deep-copy) constructor.
    Context(const Context& other) {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const int& key() const {
      return _t;
    }

    inline void Get(const int& value) {
      // All reads should be atomic (from the mutable tail).
      ASSERT_TRUE(false);
    }

    inline void GetAtomic(const int& value) {}

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    int _t;
  };

  // Skeleton callback required for when async writes and reads complete.
  auto acallback = [](IAsyncContext* context, Status result, size_t n) {
    return;
  };

  // Create a context. Will be shared between both reads and writes because
  // this is just a simple unit test.
  Context ctxt;

  // Data to be written to StorageFile. A simple byte array will suffice.
  const uint64_t num = 512;
  uint8_t* bytes =
        reinterpret_cast<uint8_t*>(FASTER::core::aligned_alloc(1024, num));
  for (int i = 0; i < num; i++) bytes[i] = 45;

  // Issue a write to the storage file.
  ASSERT_EQ(Status::Ok,
            hLog.WriteAsync(bytes, 0, num, acallback, ctxt));
  while (!disk.TryComplete());
  disk.CompletePending();

  // Prepare a buffer to receive data into.
  uint8_t* rBuff =
        reinterpret_cast<uint8_t*>(FASTER::core::aligned_alloc(1024, num));
  AsyncIOContext context(nullptr, Address(0), &ctxt, nullptr, 1);
  context.record = SectorAlignedMemory(rBuff, 0, nullptr);
  context.record.valid_offset = 0;
  context.record.available_bytes = num;
  context.record.required_bytes = num;

  // Read what was written earlier.
  ASSERT_EQ(Status::Ok,
            hLog.ReadAsync(0, reinterpret_cast<void*>(rBuff), num,
                           acallback, context));
  while (!disk.TryComplete());
  disk.CompletePending();

  /// Assert that what was read is the same as what was written.
  ASSERT_EQ(std::string((char *) bytes, num),
            std::string((char *) rBuff, num));

  // Assert that the hybrid log closes fine.
  ASSERT_EQ(Status::Ok, hLog.Close());
}

/// Tests the flushing and eviction mechanism. Writes are issued on
/// multiple files. This causes a few files to be evicted from local
/// storage. Reads are then issued and return values are matched
/// against what was written.
TEST(StorageTest, MultipleReadWrite) {
  // Typedef on the async IO handler for convenience.
  typedef QueueIoHandler handler_t;

  // Typedef on the remote tier for convenience.
  typedef BlobFile remote_t;

  // Create a disk. The hybrid log file is implicitly opened here.
  LightEpoch epoch(1);
  StorageDevice<handler_t, remote_t> disk(std::string(".") + kPathSeparator,
                                          epoch,
                                          "UseDevelopmentStorage=true;");

  // Handle to the StorageFile holding the log.
  auto hLog = std::move(disk.log());

  // Skeleton context required to issue writes and reads to StorageFile.
  class Context : public IAsyncContext {
   public:
    Context() {}

    /// Copy (and deep-copy) constructor.
    Context(const Context& other) {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const int& key() const {
      return _t;
    }

    inline void Get(const int& value) {
      // All reads should be atomic (from the mutable tail).
      ASSERT_TRUE(false);
    }

    inline void GetAtomic(const int& value) {}

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    int _t;
  };

  // Skeleton callback required for when async writes and reads complete.
  auto acallback = [](IAsyncContext* context, Status result, size_t n) {
    return;
  };

  // Create a context. Will be shared between both reads and writes because
  // this is just a simple unit test.
  Context ctxt;

  // Number of bytes for each read/write operation.
  const uint64_t num = 512;

  // Issue 12 back to back writes.
  for (uint64_t addr = 0; addr < 12; addr++) {
    uint8_t* bytes =
        reinterpret_cast<uint8_t*>(FASTER::core::aligned_alloc(1024, num));
    for (int i = 0; i < num; i++) bytes[i] = 45 + (uint8_t)addr;

    // Issue a write to the storage file.
    ASSERT_EQ(Status::Ok, hLog.WriteAsync(bytes, addr * 512, num,
                                          acallback, ctxt));
    while (!disk.TryComplete());
    disk.CompletePending();
  }

  // Run any enqueued epoch actions. This should move the remote offset
  // on the StorageFile.
  epoch.ProtectAndDrain();

  // Issue 12 back to back reads on the addresses written above.
  for (uint64_t addr = 0; addr < 12; addr++) {
    // Prepare a receive buffer for the data.
    uint64_t src = addr * 512;
    uint8_t* rBuff =
        reinterpret_cast<uint8_t*>(FASTER::core::aligned_alloc(1024, num));
    AsyncIOContext context(nullptr, Address(src), &ctxt, nullptr, 1);
    context.record = SectorAlignedMemory(rBuff, 0, nullptr);
    context.record.valid_offset = 0;
    context.record.available_bytes = num;
    context.record.required_bytes = num;

    // Read what was written earlier.
    ASSERT_EQ(Status::Ok,
              hLog.ReadAsync(src, reinterpret_cast<void*>(rBuff), num,
                             acallback, context));
    while (!disk.TryComplete());
    disk.CompletePending();

    /// Assert that what was read is the same as what was written earlier.
    uint8_t* written =
          reinterpret_cast<uint8_t*>(FASTER::core::aligned_alloc(1024, num));
    for (int i = 0; i < num; i++) written[i] = 45 + (uint8_t)addr;

    ASSERT_EQ(std::string((char *) written, num),
              std::string((char *) rBuff, num));
  }

  // Close the StorageFile holding the log.
  ASSERT_EQ(Status::Ok, hLog.Close());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
