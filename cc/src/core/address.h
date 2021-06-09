// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>

namespace FASTER {
namespace core {

class PageOffset;

/// (Logical) address into persistent memory. Identifies a page and an offset within that page.
/// Uses 48 bits: 25 bits for the offset and 23 bits for the page. (The remaining 16 bits are
/// reserved for use by the hash table.)
/// Address
class Address {
 public:
  friend class PageOffset;

  /// An invalid address, used when you need to initialize an address but you don't have a valid
  /// value for it yet. NOTE: set to 1, not 0, to distinguish an invalid hash bucket entry
  /// (initialized to all zeros) from a valid hash bucket entry that points to an invalid address.
  static constexpr uint64_t kInvalidAddress = 1;

  /// A logical address is 8 bytes.
  /// --of which 48 bits are used for the address. (The remaining 16 bits are used by the hash
  /// table, for control bits and the tag.)
  static constexpr uint64_t kAddressBits = 48;
  static constexpr uint64_t kMaxAddress = ((uint64_t)1 << kAddressBits) - 1;
  /// --of which 25 bits are used for offsets into a page, of size 2^25 = 32 MB.
  static constexpr uint64_t kOffsetBits = 25;
  static constexpr uint32_t kMaxOffset = ((uint32_t)1 << kOffsetBits) - 1;
  /// --and the remaining 23 bits are used for the page index, allowing for approximately 8 million
  /// pages.
  static constexpr uint64_t kPageBits = kAddressBits - kOffsetBits;
  static constexpr uint32_t kMaxPage = ((uint32_t)1 << kPageBits) - 1;

  /// Default constructor.
  Address()
    : control_{ 0 } {
  }
  Address(uint32_t page, uint32_t offset)
    : reserved_{ 0 }
    , page_{ page }
    , offset_{ offset } {
  }
  /// Copy constructor.
  Address(const Address& other)
    : control_{ other.control_ } {
  }
  Address(uint64_t control)
    : control_{ control } {
    assert(reserved_ == 0);
  }

  inline Address& operator=(const Address& other) {
    control_ = other.control_;
    return *this;
  }
  inline Address& operator+=(uint64_t delta) {
    assert(delta < UINT32_MAX);
    control_ += delta;
    return *this;
  }
  inline Address operator+(const uint64_t delta) {
    Address addr (*this);
    addr += delta;
    return addr;
  }
  inline Address operator-(const Address& other) {
    return control_ - other.control_;
  }

  /// Comparison operators.
  inline bool operator<(const Address& other) const {
    assert(reserved_ == 0);
    assert(other.reserved_ == 0);
    return control_ < other.control_;
  }
  inline bool operator<=(const Address& other) const {
    assert(reserved_ == 0);
    assert(other.reserved_ == 0);
    return control_ <= other.control_;
  }
  inline bool operator>(const Address& other) const {
    assert(reserved_ == 0);
    assert(other.reserved_ == 0);
    return control_ > other.control_;
  }
  inline bool operator>=(const Address& other) const {
    assert(reserved_ == 0);
    assert(other.reserved_ == 0);
    return control_ >= other.control_;
  }
  inline bool operator==(const Address& other) const {
    return control_ == other.control_;
  }
  inline bool operator!=(const Address& other) const {
    return control_ != other.control_;
  }

  /// Accessors.
  inline uint32_t page() const {
    return static_cast<uint32_t>(page_);
  }
  inline uint32_t offset() const {
    return static_cast<uint32_t>(offset_);
  }
  inline uint64_t control() const {
    return control_;
  }

 private:
  union {
      struct {
        uint64_t offset_ : kOffsetBits;         // 25 bits
        uint64_t page_ : kPageBits;  // 23 bits
        uint64_t reserved_ : 64 - kAddressBits; // 16 bits
      };
      uint64_t control_;
    };
};
static_assert(sizeof(Address) == 8, "sizeof(Address) != 8");

}
} // namespace FASTER::core

/// Implement std::min() for Address type.
namespace std {
template <>
inline const FASTER::core::Address& min(const FASTER::core::Address& a,
                                        const FASTER::core::Address& b) {
  return (b < a) ? b : a;
}
}

namespace FASTER {
namespace core {

/// Atomic (logical) address.
class AtomicAddress {
 public:
  AtomicAddress(const Address& address)
    : control_{ address.control() } {
  }

  /// Atomic access.
  inline Address load() const {
    return Address{ control_.load() };
  }
  inline void store(Address value) {
    control_.store(value.control());
  }
  inline bool compare_exchange_strong(Address& expected, Address desired) {
    uint64_t expected_control = expected.control();
    bool result = control_.compare_exchange_strong(expected_control, desired.control());
    expected = Address{ expected_control };
    return result;
  }

  /// Accessors.
  inline uint32_t page() const {
    return load().page();
  }
  inline uint32_t offset() const {
    return load().offset();
  }
  inline uint64_t control() const {
    return load().control();
  }

 private:
  /// Atomic access to the address.
  std::atomic<uint64_t> control_;
};

}
} // namespace FASTER::core
