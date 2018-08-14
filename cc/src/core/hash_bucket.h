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

namespace FASTER {
namespace core {

static_assert(Address::kAddressBits == 48, "Address::kAddressBits != 48");

/// Entry stored in a hash bucket. Packed into 8 bytes.
struct HashBucketEntry {
  /// Invalid value in the hash table
  static constexpr uint64_t kInvalidEntry = 0;

  HashBucketEntry()
    : control_{ 0 } {
  }
  HashBucketEntry(Address address, uint16_t tag, bool tentative)
    : address_{ address.control() }
    , tag_{ tag }
    , reserved_{ 0 }
    , tentative_{ tentative } {
  }
  HashBucketEntry(uint64_t code)
    : control_{ code } {
  }
  HashBucketEntry(const HashBucketEntry& other)
    : control_{ other.control_ } {
  }

  inline HashBucketEntry& operator=(const HashBucketEntry& other) {
    control_ = other.control_;
    return *this;
  }
  inline bool operator ==(const HashBucketEntry& other) const {
    return control_ == other.control_;
  }
  inline bool operator !=(const HashBucketEntry& other) const {
    return control_ != other.control_;
  }
  inline bool unused() const {
    return control_ == 0;
  }
  inline Address address() const {
    return Address{ address_ };
  }
  inline uint16_t tag() const {
    return static_cast<uint16_t>(tag_);
  }
  inline bool tentative() const {
    return static_cast<bool>(tentative_);
  }
  inline void set_tentative(bool desired) {
    tentative_ = desired;
  }

  union {
      struct {
        uint64_t address_ : 48; // corresponds to logical address
        uint64_t tag_ : 14;
        uint64_t reserved_ : 1;
        uint64_t tentative_ : 1;
      };
      uint64_t control_;
    };
};
static_assert(sizeof(HashBucketEntry) == 8, "sizeof(HashBucketEntry) != 8");

/// Atomic hash-bucket entry.
class AtomicHashBucketEntry {
 public:
  AtomicHashBucketEntry(const HashBucketEntry& entry)
    : control_{ entry.control_ } {
  }
  /// Default constructor
  AtomicHashBucketEntry()
    : control_{ HashBucketEntry::kInvalidEntry } {
  }

  /// Atomic access.
  inline HashBucketEntry load() const {
    return HashBucketEntry{ control_.load() };
  }
  inline void store(const HashBucketEntry& desired) {
    control_.store(desired.control_);
  }
  inline bool compare_exchange_strong(HashBucketEntry& expected, HashBucketEntry desired) {
    uint64_t expected_control = expected.control_;
    bool result = control_.compare_exchange_strong(expected_control, desired.control_);
    expected = HashBucketEntry{ expected_control };
    return result;
  }

 private:
  /// Atomic address to the hash bucket entry.
  std::atomic<uint64_t> control_;
};

/// Entry stored in a hash bucket that points to the next overflow bucket (if any).
struct HashBucketOverflowEntry {
  HashBucketOverflowEntry()
    : control_{ 0 } {
  }
  HashBucketOverflowEntry(FixedPageAddress address)
    : address_{ address.control() }
    , unused_{ 0 } {
  }
  HashBucketOverflowEntry(const HashBucketOverflowEntry& other)
    : control_{ other.control_ } {
  }
  HashBucketOverflowEntry(uint64_t code)
    : control_{ code } {
  }

  inline HashBucketOverflowEntry& operator=(const HashBucketOverflowEntry& other) {
    control_ = other.control_;
    return *this;
  }
  inline bool operator ==(const HashBucketOverflowEntry& other) const {
    return control_ == other.control_;
  }
  inline bool operator !=(const HashBucketOverflowEntry& other) const {
    return control_ != other.control_;
  }
  inline bool unused() const {
    return address_ == 0;
  }
  inline FixedPageAddress address() const {
    return FixedPageAddress{ address_ };
  }

  union {
      struct {
        uint64_t address_ : 48; // corresponds to logical address
        uint64_t unused_ : 16;
      };
      uint64_t control_;
    };
};
static_assert(sizeof(HashBucketOverflowEntry) == 8, "sizeof(HashBucketOverflowEntry) != 8");

/// Atomic hash-bucket overflow entry.
class AtomicHashBucketOverflowEntry {
 private:
  static constexpr uint64_t kPinIncrement = (uint64_t)1 << 48;
  static constexpr uint64_t kLocked = (uint64_t)1 << 63;

 public:
  AtomicHashBucketOverflowEntry(const HashBucketOverflowEntry& entry)
    : control_{ entry.control_ } {
  }
  /// Default constructor
  AtomicHashBucketOverflowEntry()
    : control_{ HashBucketEntry::kInvalidEntry } {
  }

  /// Atomic access.
  inline HashBucketOverflowEntry load() const {
    return HashBucketOverflowEntry{ control_.load() };
  }
  inline void store(const HashBucketOverflowEntry& desired) {
    control_.store(desired.control_);
  }
  inline bool compare_exchange_strong(HashBucketOverflowEntry& expected,
                                      HashBucketOverflowEntry desired) {
    uint64_t expected_control = expected.control_;
    bool result = control_.compare_exchange_strong(expected_control, desired.control_);
    expected = HashBucketOverflowEntry{ expected_control };
    return result;
  }

 private:
  /// Atomic address to the hash bucket entry.
  std::atomic<uint64_t> control_;
};

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
