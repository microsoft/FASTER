// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>

#include "../index/hash_bucket.h"

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

 public:
  uint64_t control_;
};
static_assert(sizeof(KeyHash) == 8, "sizeof(KeyHash) != 8");

template <class U, uint8_t kInChunkBits>
struct IndexKeyHash {
  // Bits used for in-chunk entries should be less than bits used for the entire address
  static_assert(kInChunkBits < Address::kAddressBits);
  static constexpr uint64_t kMaxInChunk = ((uint64_t)1 << kInChunkBits) - 1;

  IndexKeyHash()
    : entry_{ 0 } {
  }
  explicit IndexKeyHash(uint64_t code)
    : entry_{ code } {
  }
  IndexKeyHash(const IndexKeyHash& other)
    : entry_{ other.entry_} {
  }
  // Copy constructor from KeyHash class
  IndexKeyHash(const KeyHash& other)
    : entry_{ other.control_ } {
  }

  IndexKeyHash& operator=(const IndexKeyHash& other) {
    entry_ = other.entry_;
    return *this;
  }

  /// Truncate the key hash's address to get the page_index into a hash table of specified size.
  inline size_t hash_table_index(uint64_t hash_table_size) const {
    assert(Utility::IsPowerOfTwo(hash_table_size));
    return (entry_.address >> kInChunkBits) & (hash_table_size - 1);
  }

  /// Truncate the key hash's address to get the page_index into a hash table of specified size.
  inline size_t in_chunk_index() const {
    assert(kInChunkBits != 0); // should not be called if there are no in-chunks bits
    return entry_.address & kInChunkBits;
  }

  /// The tag serves as a discriminator inside a hash bucket. (Hash buckets use 2 bits
  /// for control and 48 bits for log-structured store offset; the remaining tag bits discriminate
  /// between different key hashes stored in the same bucket.)
  inline uint16_t tag() const {
    return static_cast<uint16_t>(entry_.tag);
  }

 private:
  U entry_;
};

// Hot index hash bucket definition -- no chunks
typedef IndexKeyHash<index::HotLogIndexBucketEntryDef, 0> HotLogIndexKeyHash;
// Cold index hash bucket definition -- 4kB chunks
static_assert(Constants::kCacheLineBytes == 64);
typedef IndexKeyHash<index::ColdLogIndexBucketEntryDef, 6> ColdLogIndexKeyHash;

}
} // namespace FASTER::core
