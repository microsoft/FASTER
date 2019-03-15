// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdint>
#include <deque>
#include <thread>
#include "gtest/gtest.h"

#include "core/light_epoch.h"
#include "core/malloc_fixed_page_size.h"
#include "device/null_disk.h"

struct alignas(32) Item {
  uint8_t buffer[32];
};

using namespace FASTER::core;

typedef MallocFixedPageSize<Item, FASTER::device::NullDisk> alloc_t;

TEST(MallocFixedPageSize, AllocFree) {
  LightEpoch epoch;
  alloc_t allocator{};
  allocator.Initialize(256, epoch);
  for(size_t idx = 0; idx < 1000000; ++idx) {
    FixedPageAddress address = allocator.Allocate();
    Item* item = &allocator.Get(address);
    ASSERT_EQ(0, reinterpret_cast<size_t>(item) % alignof(Item));
    allocator.FreeAtEpoch(address, 0);
  }
  ASSERT_EQ(1, allocator.free_list().size());
}

TEST(MallocFixedPageSize, Alloc) {
  LightEpoch epoch;
  alloc_t allocator{};
  allocator.Initialize(128, epoch);
  for(size_t idx = 0; idx < 3200000; ++idx) {
    FixedPageAddress address = allocator.Allocate();
    Item* item = &allocator.Get(address);
    ASSERT_EQ(0, reinterpret_cast<size_t>(item) % alignof(Item));
  }
  ASSERT_EQ(0, allocator.free_list().size());
}


static void MultiThread_Worker(alloc_t* allocator) {
  constexpr size_t kAllocCount = 1600000;
  FixedPageAddress* addresses = new FixedPageAddress[kAllocCount];

  for(size_t idx = 0; idx < kAllocCount; ++idx) {
    addresses[idx] = allocator->Allocate();
    Item* item = &allocator->Get(addresses[idx]);
    ASSERT_EQ(0, reinterpret_cast<size_t>(item) % alignof(Item));
  }
  for(size_t idx = 0; idx < kAllocCount; ++idx) {
    allocator->FreeAtEpoch(addresses[idx], idx);
  }
  ASSERT_EQ(kAllocCount, allocator->free_list().size());

  delete[] addresses;
}

TEST(MallocFixedPageSize, Concurrent) {
  constexpr size_t kNumThreads = 2;
  LightEpoch epoch;
  alloc_t allocator{};
  allocator.Initialize(64, epoch);
  std::deque<std::thread> threads{};
  for(size_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(MultiThread_Worker, &allocator);
  }
  for(auto& thread : threads) {
    thread.join();
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}