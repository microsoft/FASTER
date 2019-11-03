// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include "value_control.h"

namespace FASTER {
namespace core {

class PageOffset;

struct AddressLayout
{
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

  uint64_t offset_   : kOffsetBits;       // 25 bits
  uint64_t page_     : kPageBits;         // 23 bits
  uint64_t reserved_ : 64 - kAddressBits; // 16 bits
};

/// (Logical) address into persistent memory. Identifies a page and an offset within that page.
/// Uses 48 bits: 25 bits for the offset and 23 bits for the page. (The remaining 16 bits are
/// reserved for use by the hash table.)
/// Address
class Address
  : public value_control<AddressLayout, uint64_t, AddressLayout::kInvalidAddress>
{
 public:
  friend class PageOffset;

  // temp
  static constexpr uint64_t kInvalidAddress = AddressLayout::kInvalidAddress;
  static constexpr uint64_t kAddressBits = AddressLayout::kAddressBits;
  static constexpr uint64_t kMaxAddress = AddressLayout::kMaxAddress;
  static constexpr uint64_t kOffsetBits = AddressLayout::kOffsetBits;
  static constexpr uint32_t kMaxOffset = AddressLayout::kMaxOffset;
  static constexpr uint64_t kPageBits = AddressLayout::kPageBits;
  static constexpr uint32_t kMaxPage = AddressLayout::kMaxPage;

  using value_control::value_control;

  Address() = default;

  Address(uint32_t page, uint32_t offset) noexcept
    : value_control{AddressLayout{offset, page, 0}}
  {}
  /*explicit */Address(uint64_t control) noexcept
    : value_control{control}
  {
    assert(value().reserved_ == 0);  // ! Sometimes assertion is fired
  }

  Address& operator=(const Address& other) noexcept
  {
    control_ref() = other.control();
    assert(value().reserved_ == 0);
    return *this;
  }
  Address& operator=(uint64_t control) noexcept
  {
    control_ref() = control;
    assert(value().reserved_ == 0);
    return *this;
  }
  Address& operator+=(uint64_t delta) noexcept
  {
    assert(delta < UINT32_MAX);
    control_ref() += delta;
    assert(value().reserved_ == 0);  // ! Sometimes assertion is fired
    return *this;
  }
  Address operator-(const Address& other) noexcept
  {
    return Address{control() - other.control()};
  }

  /// Comparison operators.
  bool operator<(const Address& other) const noexcept
  {
    assert(value().reserved_ == 0);
    assert(other.value().reserved_ == 0);
    return control() < other.control();
  }
  bool operator<=(const Address& other) const noexcept
  {
    assert(value().reserved_ == 0);
    assert(other.value().reserved_ == 0);
    return control() <= other.control();
  }
  bool operator>(const Address& other) const noexcept
  {
    assert(value().reserved_ == 0);
    assert(other.value().reserved_ == 0);
    return control() > other.control();
  }
  bool operator>=(const Address& other) const noexcept
  {
    assert(value().reserved_ == 0);
    assert(other.value().reserved_ == 0);
    return control() >= other.control();
  }

  /// Accessors.
  uint32_t page() const noexcept
  {
    return static_cast<uint32_t>(value().page_);
  }
  uint32_t offset() const noexcept
  {
    return static_cast<uint32_t>(value().offset_);
  }
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
class AtomicAddress
  : public value_atomic_control<Address, uint64_t, AddressLayout::kInvalidAddress>
{
 public:
  using value_atomic_control::value_atomic_control;

  /// Accessors.
  uint32_t page() const noexcept
  {
    return load().page();
  }
  uint32_t offset() const noexcept
  {
    return load().offset();
  }
  uint64_t control() const noexcept
  {
    return load().control();
  }
};

}
} // namespace FASTER::core
