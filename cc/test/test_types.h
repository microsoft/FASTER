// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <functional>

#include "core/key_hash.h"

namespace FASTER {
namespace test {

using namespace FASTER::core;

template<class T, class HashFn = std::hash<T>>
class FixedSizeKey {
 public:
  FixedSizeKey(T value)
    : key{ value }
  {}

  FixedSizeKey(const FixedSizeKey&) = default;

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(FixedSizeKey));
  }

  inline core::KeyHash GetHash() const {
    HashFn hash_fn;
    return core::KeyHash{ hash_fn(key) };
  }

  inline bool operator==(const FixedSizeKey& other) const {
    return key == other.key;
  }
  inline bool operator!=(const FixedSizeKey& other) const {
    return key != other.key;
  }

  T key;
};

template<class T>
class SimpleAtomicValue {
 public:
  SimpleAtomicValue()
    : value{}
  {}

  SimpleAtomicValue(T value_)
    : value{ value_ }
  {}

  SimpleAtomicValue(const SimpleAtomicValue& other)
    : value{ other.value }
  {}

  void operator=(const SimpleAtomicValue &other) {
    value = other.value;
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(SimpleAtomicValue));
  }

  union {
    T value;
    std::atomic<T> atomic_value;
  };
};

// For 8-byte T (i.e. uint64_t), its size would be ~1KB
template<class T>
class SimpleAtomicMediumValue {
 public:
  SimpleAtomicMediumValue()
    : value{}
  {}

  SimpleAtomicMediumValue(T value_)
    : value(value_)
  {}
  SimpleAtomicMediumValue(const SimpleAtomicMediumValue& other)
    : value(other.value)
  {}

  void operator=(const SimpleAtomicMediumValue &other) {
    value = other.value;
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(SimpleAtomicMediumValue));
  }

  union {
    T value;
    std::atomic<T> atomic_value;
  };

 private:
  T extra[126];
};


// For 8-byte T (i.e. uint64_t), its size would be ~8KB
// NOTE: need to keep size less than max file segment (e.g. 8000 in Linux)
template<class T>
class SimpleAtomicLargeValue {
 public:
  SimpleAtomicLargeValue()
    : value{}
  {}

  SimpleAtomicLargeValue(T value_)
    : value(value_)
  {}
  SimpleAtomicLargeValue(const SimpleAtomicLargeValue& other)
    : value(other.value)
  {}

  void operator=(const SimpleAtomicLargeValue &other) {
    value = other.value;
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(SimpleAtomicLargeValue));
  }

  union {
    T value;
    std::atomic<T> atomic_value;
  };

 private:
  T extra[992];
};

// Variable size keys / values

class NonCopyable
{
  public:
  NonCopyable(const NonCopyable&) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;

  protected:
  NonCopyable() = default;
  ~NonCopyable() = default;
};

class NonMovable
{
  public:
  NonMovable(NonMovable&&) = delete;
  NonMovable& operator=(NonMovable&&) = delete;

  protected:
  NonMovable() = default;
  ~NonMovable() = default;
};

class VariableSizeKey : NonCopyable, NonMovable {
 public:

  static uint32_t size(uint32_t key_length) {
    return static_cast<uint32_t>(sizeof(VariableSizeKey) + key_length * sizeof(uint32_t));
  }

  static void Create(VariableSizeKey* dst, uint32_t key_length, const uint32_t* key_data) {
    dst->key_length_ = key_length;
    memcpy(dst->buffer(), key_data, key_length * sizeof(uint32_t));
  }

  /// Methods and operators required by the (implicit) interface:
  inline uint32_t size() const {
    return static_cast<uint32_t>(sizeof(VariableSizeKey) + key_length_ * sizeof(uint32_t));
  }
  inline KeyHash GetHash() const {
    return KeyHash(Utility::HashBytes(
      reinterpret_cast<const uint16_t*>(buffer()), key_length_ * 2));
  }

  /// Comparison operators.
  inline bool operator==(const VariableSizeKey& other) const {
    if (this->key_length_ != other.key_length_) return false;
    return memcmp(buffer(), other.buffer(), key_length_) == 0;
  }
  inline bool operator!=(const VariableSizeKey& other) const {
    return !(*this == other);
  }

  uint32_t key_length_;

  inline const uint32_t* buffer() const {
    return reinterpret_cast<const uint32_t*>(this + 1);
  }
  inline uint32_t* buffer() {
    return reinterpret_cast<uint32_t*>(this + 1);
  }
};

class VariableSizeShallowKey {
 public:
  VariableSizeShallowKey(uint32_t* key_data, uint32_t key_length)
      : key_length_(key_length), key_data_(key_data)
  { }

  inline uint32_t size() const {
    return VariableSizeKey::size(key_length_);
  }
  inline KeyHash GetHash() const {
    return KeyHash(Utility::HashBytes(
      reinterpret_cast<const uint16_t*>(key_data_), key_length_ * 2));
  }
  inline void write_deep_key_at(VariableSizeKey* dst) const {
    VariableSizeKey::Create(dst, key_length_, key_data_);
  }
  /// Comparison operators.
  inline bool operator==(const VariableSizeKey& other) const {
    if (this->key_length_ != other.key_length_) return false;
    return memcmp(key_data_, other.buffer(), key_length_) == 0;
  }
  inline bool operator!=(const VariableSizeKey& other) const {
    return !(*this == other);
  }

  uint32_t key_length_;
  uint32_t* key_data_;
};


}
} // namespace FASTER::test
