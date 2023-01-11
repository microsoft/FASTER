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

template <class U, uint8_t kInChunkIndexBits, uint8_t kInChunkTagBits>
struct IndexKeyHash {
  static_assert(kInChunkTagBits >= 0 &&  kInChunkTagBits <= 8,
              "Chunk tag bits can cause overflow of uint8_t");
  static_assert(kInChunkIndexBits >= 0 && kInChunkIndexBits <= 8,
              "Chunk index bits can cause overflow of uint8_t");

  static constexpr uint8_t kInChunkTotalBits = kInChunkIndexBits + kInChunkTagBits;
  static_assert(kInChunkTotalBits <= U::kReservedBits);

  static constexpr uint64_t kInChunkIndexSize = ((uint64_t)1 << kInChunkIndexBits);
  static constexpr uint64_t kInChunkTagSize = ((uint64_t)1 << kInChunkTagBits);

  IndexKeyHash()
    : entry_{ 0 } {
  }
  explicit IndexKeyHash(uint64_t code)
    : entry_{ code } {
  }
  IndexKeyHash(uint64_t address, uint16_t tag)
    : entry_{ 0 } {
      entry_.address = address;
      entry_.tag = tag;
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
    return entry_.address & (hash_table_size - 1);
  }
  /// The tag serves as a discriminator inside a hash bucket. (Hash buckets use 2 bits
  /// for control and 48 bits for log-structured store offset; the remaining tag bits discriminate
  /// between different key hashes stored in the same bucket.)
  inline uint16_t tag() const {
    return static_cast<uint16_t>(entry_.tag);
  }

  inline size_t chunk_id(uint64_t hash_table_size) const {
    assert(kInChunkTotalBits != 0); // should not be called if there are no in-chunks bits
    return hash_table_index(hash_table_size);
  }
  inline uint8_t index_in_chunk() const {
    assert(kInChunkTotalBits != 0); // should not be called if there are no in-chunks bits
    return (entry_.reserved >> kInChunkTagBits) & (kInChunkIndexSize - 1);
  }
  inline uint8_t tag_in_chunk() const {
    assert(kInChunkTotalBits != 0); // should not be called if there are no in-chunks bits
    return entry_.reserved & (kInChunkTagSize - 1);
  }
  inline uint64_t control() const {
    return entry_.control;
  }
  inline KeyHash to_keyhash() const {
    return KeyHash{ control() };
  }

 private:
  U entry_;
};

// Hot index hash bucket definition -- no chunks
typedef IndexKeyHash<index::HotLogIndexBucketEntryDef, 0, 0> HotLogIndexKeyHash;
// Cold index hash bucket definition -- 64 buckets in chunk, 8 entries per chunk bucket
static_assert(Constants::kCacheLineBytes == 64);
typedef IndexKeyHash<index::ColdLogIndexBucketEntryDef, 6, 3> ColdLogIndexKeyHash;

}
} // namespace FASTER::core
