// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstdint>
#include "address.h"
#include "auto_ptr.h"

namespace FASTER {
namespace core {

/// Record header, internal to FASTER.
class RecordInfo {
 public:
  RecordInfo(uint16_t checkpoint_version_, bool final_bit_, bool tombstone_, bool invalid_,
             Address previous_address)
    : checkpoint_version{ checkpoint_version_ }
    , final_bit{ final_bit_ }
    , tombstone{ tombstone_ }
    , invalid{ invalid_ }
    , previous_address_{ previous_address.control() } {
  }

  RecordInfo(const RecordInfo& other)
    : control_{ other.control_ } {
  }

  inline bool IsNull() const {
    return control_ == 0;
  }
  inline Address previous_address() const {
    return Address{ previous_address_ };
  }

  union {
      struct {
        uint64_t previous_address_ : 48;
        uint64_t checkpoint_version : 13;
        uint64_t invalid : 1;
        uint64_t tombstone : 1;
        uint64_t final_bit : 1;
      };

      uint64_t control_;
    };
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

  Record(RecordInfo header_)
      : header{ header_ } {
  }

  /// For placement new() operator. Can't set value, since it might be set by value = input (for
  /// upsert), or rmw_initial(...) (for RMW).
  /*
  Record(RecordInfo header_, const key_t& key_)
    : header{ header_ } {
    void* buffer = const_cast<key_t*>(&key());
    new(buffer)key_t{ key_ };
  }
  */

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
  static inline constexpr uint32_t size(uint32_t key_size, uint32_t value_size) {
    return static_cast<uint32_t>(
             // --plus Value size, all padded to Header alignment.
             pad_alignment(value_size +
                           // --plus Key size, all padded to Value alignment.
                           pad_alignment(key_size +
                                         // Header, padded to Key alignment.
                                         pad_alignment(sizeof(RecordInfo), alignof(key_t)),
                                         alignof(value_t)),
                           alignof(RecordInfo)));
  }
  /// Size of the existing record, in memory. (Includes padding, if any, after the value.)
  inline constexpr uint32_t size() const {
    return size(key().size(), value().size());
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
  // and whatever information the host needs to determine the value size.
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
