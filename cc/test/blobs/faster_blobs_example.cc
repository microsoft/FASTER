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

#include "../test_types.h"

using namespace FASTER::core;
using FASTER::test::FixedSizeKey;
using FASTER::test::SimpleAtomicValue;

using namespace FASTER::device;
using namespace FASTER::environment;

/// Tests if we can construct an instance of FASTER that uses the blob
/// device underneath.
TEST(FasterBlobs, Example) {
  typedef QueueIoHandler handler_t;
  typedef BlobFile remote_t;
  typedef FASTER::device::StorageDevice<handler_t, remote_t> disk_t;

  using K = FixedSizeKey<uint8_t>;
  using V = SimpleAtomicValue<uint8_t>;
  typedef FASTER::core::FasterKv<K, V, disk_t> faster_t;

  std::string path = std::string(".") + kPathSeparator; // Filesystem path under which log will be stored.
  std::string conn = "UseDevelopmentStorage=true;"; // Azure Blobs connection str.
  faster_t store(128, 1073741824, path, 0.9, false, conn);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
