// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <thread>

#include "../core/address.h"
#include "../core/constants.h"
#include "../core/malloc_fixed_page_size.h"

using namespace FASTER::core;

namespace FASTER {
namespace index {

static_assert(Address::kAddressBits == 48, "Address::kAddressBits != 48");

template <class U>
class IndexHashBucketEntry;

/// Entry stored in a hash bucket. Packed into 8 bytes.
struct HashBucketEntry {
  /// Invalid value in the hash table
  static constexpr uint64_t kInvalidEntry = 0;

  HashBucketEntry()
    : control_{ kInvalidEntry } {
  }
  HashBucketEntry(Address address)
    : address_{ address.control() } {
  }
  HashBucketEntry(uint64_t code)
    : control_{ code } {
  }
  HashBucketEntry(const HashBucketEntry& other)
    : control_{ other.control_ } {
  }
  template <class U>
  HashBucketEntry(IndexHashBucketEntry<U>& other) {
    control_ = other.entry_.control;
  }

  inline HashBucketEntry& operator=(const HashBucketEntry& other) {
    control_ = other.control_;
    return *this;
  }
  template <class U>
  inline HashBucketEntry& operator=(const IndexHashBucketEntry<U>& other) {
    control_ = other.entry_.control;
    return *this;
  }

  inline bool operator ==(const HashBucketEntry& other) const {
    return control_ == other.control_;
  }
  inline bool operator !=(const HashBucketEntry& other) const {
    return control_ != other.control_;
  }
  inline Address address() const {
    return Address{ address_ };
  }

  union {
    struct {
      uint64_t address_ : 48;   // corresponds to logical address
      uint64_t reserved_ : 16;  // reserved for internal index use
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

template <class U>
struct IndexHashBucketEntry {
  friend class HashBucketEntry;

  IndexHashBucketEntry()
    : entry_{ HashBucketEntry::kInvalidEntry } {
  }
  IndexHashBucketEntry(Address address, uint16_t tag, bool tentative)
    : entry_{ address.control(), tag, tentative } {
  }
  IndexHashBucketEntry(uint64_t code)
    : entry_{ code } {
  }
  IndexHashBucketEntry(const IndexHashBucketEntry& other)
    : entry_{ other.entry_ } {
  }
  IndexHashBucketEntry(const HashBucketEntry& other)
    : entry_{ other.control_ } {
  }

  inline IndexHashBucketEntry& operator=(const IndexHashBucketEntry& other) {
    entry_ = other.entry_;
    return *this;
  }
  inline bool operator ==(const IndexHashBucketEntry& other) const {
    return entry_.control == other.entry_.control;
  }
  inline bool operator !=(const IndexHashBucketEntry& other) const {
    return entry_.control != other.entry_.control;
  }

  inline bool unused() const {
    return entry_.control == HashBucketEntry::kInvalidEntry;
  }
  inline Address address() const {
    return Address{ entry_.address };
  }
  inline uint16_t tag() const {
    return static_cast<uint16_t>(entry_.tag);
  }
  inline bool tentative() const {
    return static_cast<bool>(entry_.tentative);
  }

 private:
  U entry_;
};

union HotLogIndexBucketEntryDef {
  HotLogIndexBucketEntryDef(uint64_t code)
    : control{ code } {
  }
  HotLogIndexBucketEntryDef(uint64_t address_, uint64_t tag_, uint64_t tentative_)
    : address{ address_ }
    , tag{ tag_ }
    , reserved{ 0 }
    , tentative{ tentative_ } {
  }

  struct {
    uint64_t address : 48;   // corresponds to logical address
    uint64_t tag : 14;       // used to distinguish among different entries in same hash table position
    uint64_t reserved : 1;   // not used
    uint64_t tentative : 1;  // used (internally) to handle concurrent updates to the hash table
  };
  uint64_t control;
};
typedef IndexHashBucketEntry<HotLogIndexBucketEntryDef> HotLogIndexHashBucketEntry;
static_assert(sizeof(HotLogIndexHashBucketEntry) == 8,
              "sizeof(HotLogIndexHashBucketEntry) != 8");

union ColdLogIndexBucketEntryDef {
  ColdLogIndexBucketEntryDef(uint64_t code)
    : control{ code } {
  }
  ColdLogIndexBucketEntryDef(uint64_t address_, uint64_t tag_, uint64_t tentative_)
    : address{ address_ }
    , tag{ tag_ }
    , reserved{ 0 }
    , tentative{ tentative_ } {
  }

  struct {
    uint64_t address : 48;  // corresponds to logical address
    uint64_t tag : 3;       // used to distinguish among different entries in same hash table position
    uint64_t reserved : 12;  // not used
    uint64_t tentative : 1; // used (internally) to handle concurrent updates to the hash table
  };
  uint64_t control;
};
typedef IndexHashBucketEntry<ColdLogIndexBucketEntryDef> ColdLogIndexHashBucketEntry;
static_assert(sizeof(ColdLogIndexHashBucketEntry) == 8,
              "sizeof(ColdLogIndexHashBucketEntry) != 8");

/// Entry stored in a hash bucket that points to the next overflow bucket (if any).
struct HashBucketOverflowEntry {
  /// Invalid value in the hash table
  static constexpr uint64_t kInvalidEntry = 0;

  HashBucketOverflowEntry()
    : control_{ kInvalidEntry } {
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
    return address_ == kInvalidEntry;
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
    : control_{ HashBucketOverflowEntry::kInvalidEntry } {
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

/// A bucket consisting of 7 hash bucket entries, plus one hash bucket overflow entry.
/// Fits in a cache line.
struct alignas(Constants::kCacheLineBytes) HotLogIndexHashBucket {
  /// Number of entries per bucket (excluding overflow entry).
  static constexpr uint32_t kNumEntries = 7;
  /// The entries.
  AtomicHashBucketEntry entries[kNumEntries];
  /// Overflow entry points to next overflow bucket, if any.
  AtomicHashBucketOverflowEntry overflow_entry;
};
static_assert(sizeof(HotLogIndexHashBucket) == Constants::kCacheLineBytes,
              "sizeof(HashBucket) != Constants::kCacheLineBytes");

/// A bucket consisting of 8 hash bucket entries (no overflow buckets)
/// Fits in a cache line.
struct alignas(Constants::kCacheLineBytes) ColdLogIndexHashBucket {
  /// Number of entries per bucket
  static constexpr uint32_t kNumEntries = 8;
  /// The entries.
  AtomicHashBucketEntry entries[kNumEntries];
};
static_assert(sizeof(ColdLogIndexHashBucket) == Constants::kCacheLineBytes,
              "sizeof(ColdHashBucket) != Constants::kCacheLineBytes");


// Taken from: https://fekir.info/post/detect-member-variables/
template <typename T, typename = void>
struct has_overflow_entry : std::false_type{};

template <typename T>
struct has_overflow_entry<T, decltype((void)T::overflow_entry, void())> : std::true_type {};

}
} // namespace FASTER::index
