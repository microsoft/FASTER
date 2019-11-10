// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstdint>
#include "address.h"
#include "auto_ptr.h"
#include "value_control.h"

namespace FASTER {
namespace core {

struct RecordInfoLayout
{
  uint64_t previous_address_    : 48;
  uint64_t checkpoint_version_  : 13;
  uint64_t invalid_             : 1;
  uint64_t tombstone_           : 1;
  uint64_t final_bit_           : 1;

  RecordInfoLayout(uint16_t checkpoint_version, bool final_bit, bool tombstone, bool invalid, Address previous_address) noexcept
    : checkpoint_version_{checkpoint_version}
    , final_bit_{final_bit}
    , tombstone_{tombstone}
    , invalid_{invalid}
    , previous_address_{previous_address.control()}
  {}
};

/// Record header, internal to FASTER.
class RecordInfo
  : public value_control<RecordInfoLayout, uint64_t, 0>
{
public:
  using value_control::value_control;
   
  RecordInfo(uint16_t checkpoint_version, bool final_bit, bool tombstone, bool invalid, Address previous_address) noexcept
    : value_control{RecordInfoLayout{ checkpoint_version, final_bit, tombstone, invalid, previous_address }}
  {}
  bool IsNull() const noexcept
  {
    return control() == 0;
  }
  Address previous_address() const noexcept
  {
    return Address{ value().previous_address_ };
  }
  uint16_t checkpoint_version() const noexcept
  {
    return static_cast<uint16_t>(value().checkpoint_version_);
  }
  bool invalid() const noexcept
  {
    return static_cast<bool>(value().invalid_);
  }
  void set_invalid() noexcept
  {
    value().invalid_ = true;
  }
  bool tombstone() const noexcept
  {
    return static_cast<bool>(value().tombstone_);
  }
  void set_tombstone(bool val) noexcept
  {
	  value().tombstone_ = val;
  }
  bool final_bit() const noexcept
  {
    return static_cast<bool>(value().final_bit_);
  }
};
static_assert(sizeof(RecordInfo) == 8, "sizeof(RecordInfo) != 8");

/// A record stored in the log. The log starts at 0 (mod 64), and consists of Records, one after
/// the other. Each record's header is 8 bytes.
template <class key_t, class value_t>
struct Record {
  // To support records with alignment > 64, modify the persistent-memory allocator to allocate
  // a larger NULL page on startup.
  static_assert(alignof(key_t) <= Constants::kCacheLineBytes,
                "alignof(key_t) > Constants::kCacheLineBytes)");
  static_assert(alignof(value_t) <= Constants::kCacheLineBytes,
                "alignof(value_t) > Constants::kCacheLineBytes)");

  /// For placement new() operator. Can't set value, since it might be set by value = input (for
  /// upsert), or rmw_initial(...) (for RMW).
  Record(RecordInfo header_, const key_t& key_)
    : header{ header_ } {
    void* buffer = const_cast<key_t*>(&key());
    new(buffer)key_t{ key_ };
  }

  /// Key appears immediately after record header (subject to alignment padding). Keys are
  /// immutable.
  inline constexpr const key_t& key() const {
    const uint8_t* head = reinterpret_cast<const uint8_t*>(this);
    size_t offset = pad_alignment(sizeof(RecordInfo), alignof(key_t));
    return *reinterpret_cast<const key_t*>(head + offset);
  }

  /// Value appears immediately after key (subject to alignment padding). Values can be modified.
  inline constexpr const value_t& value() const {
    const uint8_t* head = reinterpret_cast<const uint8_t*>(this);
    size_t offset = pad_alignment(key().size() +
                                  pad_alignment(sizeof(RecordInfo), alignof(key_t)),
                                  alignof(value_t));
    return *reinterpret_cast<const value_t*>(head + offset);
  }
  inline constexpr value_t& value() {
    uint8_t* head = reinterpret_cast<uint8_t*>(this);
    size_t offset = pad_alignment(key().size() +
                                  pad_alignment(sizeof(RecordInfo), alignof(key_t)),
                                  alignof(value_t));
    return *reinterpret_cast<value_t*>(head + offset);
  }

  /// Size of a record to be created, in memory. (Includes padding, if any, after the value, so
  /// that the next record stored in the log is properly aligned.)
  static inline constexpr uint32_t size(const key_t& key_, uint32_t value_size) {
    return static_cast<uint32_t>(
             // --plus Value size, all padded to Header alignment.
             pad_alignment(value_size +
                           // --plus Key size, all padded to Value alignment.
                           pad_alignment(key_.size() +
                                         // Header, padded to Key alignment.
                                         pad_alignment(sizeof(RecordInfo), alignof(key_t)),
                                         alignof(value_t)),
                           alignof(RecordInfo)));
  }
  /// Size of the existing record, in memory. (Includes padding, if any, after the value.)
  inline constexpr uint32_t size() const {
    return size(key(), value().size());
  }

  /// Minimum size of a read from disk that is guaranteed to include the record's header + whatever
  /// information class key_t needs to determine its key size.
  static inline constexpr uint32_t min_disk_key_size() {
    return static_cast<uint32_t>(
             // -- plus sizeof(key_t).
             sizeof(key_t) +
             // Header size, padded to Key alignment.
             pad_alignment(sizeof(RecordInfo), alignof(key_t)));
  }

  /// Minimum size of a read from disk that is guaranteed to include the record's header, key,
  /// and whatever information the host needs to determine the value size.
  inline constexpr uint32_t min_disk_value_size() const {
    return static_cast<uint32_t>(
             // -- plus size of the Value's header.
             sizeof(value_t) +
             // --plus Key size, padded to Base Value alignment.
             pad_alignment(key().size() +
                           // Header, padded to Key alignment.
                           pad_alignment(sizeof(RecordInfo), alignof(key_t)),
                           alignof(value_t))
           );
  }

  /// Size of a record, on disk. (Excludes padding, if any, after the value.)
  inline constexpr uint32_t disk_size() const {
    return static_cast<uint32_t>(value().size() +
                                 pad_alignment(key().size() +
                                     // Header, padded to Key alignment.
                                     pad_alignment(sizeof(RecordInfo), alignof(key_t)),
                                     alignof(value_t)));
  }

 public:
  RecordInfo header;
};

}
} // namespace FASTER::core
