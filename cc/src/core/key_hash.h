// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include "utility.h"

namespace FASTER {
namespace core {

/// Hash of a key is 8 bytes, compatible with hash bucket entry.
struct KeyHash {
  KeyHash()
    : control_{ 0 } {
  }
  explicit KeyHash(uint64_t code)
    : control_{ code } {
  }
  KeyHash(const KeyHash& other)
    : control_{ other.control_ } {
  }

  KeyHash& operator=(const KeyHash& other) {
    control_ = other.control_;
    return *this;
  }

  /// Truncate the key hash's address to get the page_index into a hash table of specified size.
  inline uint64_t idx(uint64_t size) const {
    assert(Utility::IsPowerOfTwo(size));
    return address_ & (size - 1);
  }

  /// The tag (14 bits) serves as a discriminator inside a hash bucket. (Hash buckets use 2 bits
  /// for control and 48 bits for log-structured store offset; the remaining 14 bits discriminate
  /// between different key hashes stored in the same bucket.)
  inline uint16_t tag() const {
    return static_cast<uint16_t>(tag_);
  }

 private:
  union {
      struct {
        uint64_t address_ : 48;
        uint64_t tag_ : 14;
        uint64_t not_used_ : 2;
      };
      uint64_t control_;
    };
};
static_assert(sizeof(KeyHash) == 8, "sizeof(KeyHash) != 8");

}
} // namespace FASTER::core
