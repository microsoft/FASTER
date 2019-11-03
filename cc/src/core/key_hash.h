// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include "utility.h"
#include "value_control.h"

namespace FASTER {
namespace core {

struct KeyHashLayout
{
  uint64_t address_   : 48;
  uint64_t tag_       : 14;
  uint64_t not_used_  : 2;

  KeyHashLayout(uint64_t address, uint64_t tag)
    : address_{address}
    , tag_{tag}
    , not_used_{}
  {}
};

/// Hash of a key is 8 bytes, compatible with hash bucket entry.
struct KeyHash
  : public value_control<KeyHashLayout, uint64_t, 0>
{
  using value_control::value_control;

  /// Truncate the key hash's address to get the page_index into a hash table of specified size.
  uint64_t idx(uint64_t size) const noexcept
  {
    assert(Utility::IsPowerOfTwo(size));
    return value().address_ & (size - 1);
  }

  /// The tag (14 bits) serves as a discriminator inside a hash bucket. (Hash buckets use 2 bits
  /// for control and 48 bits for log-structured store offset; the remaining 14 bits discriminate
  /// between different key hashes stored in the same bucket.)
  uint16_t tag() const noexcept
  {
    return static_cast<uint16_t>(value().tag_);
  }
};
static_assert(sizeof(KeyHash) == 8, "sizeof(KeyHash) != 8");

}
} // namespace FASTER::core
