// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"

#include "core/async.h"
#include "core/faster.h"
#include "core/status.h"

#include "device/azure.h"
#include "device/null_disk.h"

#ifdef _WIN32

#include <concurrent_queue.h>
template <typename T>
using concurrent_queue = concurrency::concurrent_queue<T>;

#else

#include "tbb/concurrent_queue.h"
template <typename T>
using concurrent_queue = tbb::concurrent_queue<T>;

#endif

using namespace FASTER::device;

/// Tests if the blob file can be opened and closed successfully.
TEST(AzureBlobs, OpenClose) {
  BlobFile remoteFile;

  ASSERT_EQ(Status::Ok, remoteFile.Open());
  ASSERT_EQ(Status::Ok, remoteFile.Close());
}

/// Tests if a bunch of bytes once written can be read from blob
/// storage. Issues a write to a BlobFile followed by a read and
/// asserts that both values match.
TEST(AzureBlobs, ReadWrite) {
  /// Skeleton context required to issue writes and reads to blob storage.
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

  /// Skeleton callback required for when async writes and reads complete.
  auto acallback = [](IAsyncContext* context, Status result, size_t n) {
    return;
  };

  /// Create a context. Will be shared between both reads and writes because
  /// this is just a simple unit test.
  Context ctxt;

  /// BlobFile to be tested.
  concurrent_queue<BlobFile::Task> tasks;
  BlobFile remoteFile(tasks);

  /// Data to be written to blob storage. A simple byte array will suffice.
  const uint64_t num = 512;
  uint8_t bytes[num];
  for (int i = 0; i < num; i++) bytes[i] = 145;

  /// Logical address to access on blob storage.
  uint64_t addr = (uint64_t)3 << 30;

  /// Assert that the file opened successfully.
  ASSERT_EQ(Status::Ok, remoteFile.Open());

  /// Issue the write to blob storage.
  ASSERT_EQ(Status::Ok,
            remoteFile.WriteAsync(bytes, num, addr, acallback, ctxt));

  /// Wait for the write to complete.
  BlobFile::Task task;
  tasks.try_pop(task);
  task.wait();

  /// Prepare pointer to hold data read from blob storage.
  uint8_t* rBuff = new uint8_t[num];
  AsyncIOContext context(nullptr, Address(addr), &ctxt, nullptr, 1);
  context.record = SectorAlignedMemory(rBuff, 0, nullptr);
  context.record.valid_offset = 0;
  context.record.available_bytes = num;
  context.record.required_bytes = num;

  /// Issue the read to blob storage.
  ASSERT_EQ(Status::Ok,
            remoteFile.ReadAsync(addr, num, rBuff, acallback, context));

  /// Wait for the read to complete.
  tasks.try_pop(task);
  task.wait();

  /// Assert that what was read is the same as what was written.
  ASSERT_EQ(std::string((char *) bytes, num),
            std::string((char *) rBuff, num));

  /// Assert that the file closes successfully.
  ASSERT_EQ(Status::Ok, remoteFile.Close());

  delete(rBuff);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
