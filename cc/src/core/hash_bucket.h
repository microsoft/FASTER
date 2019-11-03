// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <thread>

#include "address.h"
#include "constants.h"
#include "malloc_fixed_page_size.h"
#include "value_control.h"

namespace FASTER {
namespace core {

static_assert(Address::kAddressBits == 48, "Address::kAddressBits != 48");

struct HashBucketEntryLayout
{
  /// Invalid value in the hash table
  static constexpr uint64_t kInvalidEntry = 0;

  uint64_t address_   : 48; // corresponds to logical address
  uint64_t tag_       : 14;
  uint64_t reserved_  :  1;
  uint64_t tentative_ :  1;

  HashBucketEntryLayout(Address address, uint16_t tag, bool tentative) noexcept
    : address_{address.control()}
    , tag_{tag}
    , reserved_{}
    , tentative_{tentative}
  {}
};

/// Entry stored in a hash bucket. Packed into 8 bytes.
struct HashBucketEntry
  : public value_control<HashBucketEntryLayout, uint64_t, HashBucketEntryLayout::kInvalidEntry>
{
  static constexpr uint64_t kInvalidEntry = HashBucketEntryLayout::kInvalidEntry;

  using value_control::value_control;

  HashBucketEntry() = default;

  HashBucketEntry(Address address, uint16_t tag, bool tentative) noexcept
    : value_control{HashBucketEntryLayout{ address, tag, tentative }}
  {}

  bool unused() const noexcept
  {
    return control() == HashBucketEntryLayout::kInvalidEntry;
  }
  bool valid() const noexcept
  {
      return control() != HashBucketEntryLayout::kInvalidEntry;
  }
  Address address() const noexcept
  {
    return Address{ value().address_ };
  }
  uint16_t tag() const noexcept
  {
    return static_cast<uint16_t>(value().tag_);
  }
  bool tentative() const noexcept
  {
    return static_cast<bool>(value().tentative_);
  }
  void set_tentative(bool desired) noexcept
  {
    value().tentative_ = desired;
  }
};
static_assert(sizeof(HashBucketEntry) == 8, "sizeof(HashBucketEntry) != 8");

/// Atomic hash-bucket entry.
using AtomicHashBucketEntry = value_atomic_control<HashBucketEntry, uint64_t, HashBucketEntryLayout::kInvalidEntry>;

struct HashBucketOverflowEntryLayout
{
  /// Invalid value in the hash table
  static constexpr uint64_t kInvalidEntry = 0;

  uint64_t address_ : 48; // corresponds to logical address
  uint64_t unused_  : 16;

  explicit HashBucketOverflowEntryLayout(uint64_t address) noexcept
    : address_{address}
    , unused_{}
  {}
};

/// Entry stored in a hash bucket that points to the next overflow bucket (if any).
struct HashBucketOverflowEntry
  : public value_control<HashBucketOverflowEntryLayout, uint64_t, HashBucketOverflowEntryLayout::kInvalidEntry>
{
  using value_control::value_control;

  HashBucketOverflowEntry() = default;

  explicit HashBucketOverflowEntry(FixedPageAddress address) noexcept
    : value_control{HashBucketOverflowEntryLayout{address.control()}}
  {}
  bool unused() const noexcept
  {
    return control() == HashBucketOverflowEntryLayout::kInvalidEntry;
  }
  FixedPageAddress address() const noexcept
  {
    return FixedPageAddress{ value().address_ };
  }
};
static_assert(sizeof(HashBucketOverflowEntry) == 8, "sizeof(HashBucketOverflowEntry) != 8");

/// Atomic hash-bucket overflow entry.
using AtomicHashBucketOverflowEntry = value_atomic_control<HashBucketOverflowEntry, uint64_t, HashBucketOverflowEntryLayout::kInvalidEntry>;

/// A bucket consisting of 7 hash bucket entries, plus one hash bucket overflow entry. Fits in
/// a cache line.
struct alignas(Constants::kCacheLineBytes) HashBucket {
  /// Number of entries per bucket (excluding overflow entry).
  static constexpr uint32_t kNumEntries = 7;
  /// The entries.
  AtomicHashBucketEntry entries[kNumEntries];
  /// Overflow entry points to next overflow bucket, if any.
  AtomicHashBucketOverflowEntry overflow_entry;
};
static_assert(sizeof(HashBucket) == Constants::kCacheLineBytes,
              "sizeof(HashBucket) != Constants::kCacheLineBytes");

}
} // namespace FASTER::core
