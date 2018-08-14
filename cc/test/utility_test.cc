// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdint>
#include "gtest/gtest.h"

#include "core/auto_ptr.h"

using namespace FASTER::core;

TEST(UtilityTest, NextPowerOfTwo) {
  EXPECT_EQ(1, next_power_of_two(1));
  EXPECT_EQ(2, next_power_of_two(2));
  EXPECT_EQ(4, next_power_of_two(3));
  EXPECT_EQ(4, next_power_of_two(4));
  EXPECT_EQ(8, next_power_of_two(5));
  EXPECT_EQ(8, next_power_of_two(6));
  EXPECT_EQ(8, next_power_of_two(7));
  EXPECT_EQ(8, next_power_of_two(8));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}