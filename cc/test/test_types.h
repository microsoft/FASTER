// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <functional>

#include "core/key_hash.h"

namespace FASTER {
namespace test {

using namespace FASTER::core;

template<class T, class HashFn = FasterHashHelper<T>>
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
    FasterHashHelper<uint16_t> hash_fn;
    return KeyHash{ hash_fn(
      reinterpret_cast<const uint16_t*>(buffer()), key_length_ * 2) };
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
    FasterHashHelper<uint16_t> hash_fn;
    return KeyHash{ hash_fn(
      reinterpret_cast<const uint16_t*>(key_data_), key_length_ * 2) };
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

// Used with variable length values
class GenLock {
  public:
  GenLock()
    : control_{ 0 } {
  }
  GenLock(uint64_t control)
    : control_{ control } {
  }
  inline GenLock& operator=(const GenLock& other) {
    control_ = other.control_;
    return *this;
  }

  union {
      struct {
        uint64_t gen_number : 62;
        uint64_t locked : 1;
        uint64_t replaced : 1;
      };
      uint64_t control_;
    };
};
static_assert(sizeof(GenLock) == 8, "sizeof(GenLock) != 8");

class AtomicGenLock {
  public:
  AtomicGenLock()
    : control_{ 0 } {
  }
  AtomicGenLock(uint64_t control)
    : control_{ control } {
  }

  inline GenLock load() const {
    return GenLock{ control_.load() };
  }
  inline void store(GenLock desired) {
    control_.store(desired.control_);
  }

  inline bool try_lock(bool& replaced) {
    replaced = false;
    GenLock expected{ control_.load() };
    expected.locked = 0;
    expected.replaced = 0;
    GenLock desired{ expected.control_ };
    desired.locked = 1;

    if(control_.compare_exchange_strong(expected.control_, desired.control_)) {
      return true;
    }
    if(expected.replaced) {
      replaced = true;
    }
    return false;
  }
  inline void unlock(bool replaced) {
    if(!replaced) {
      // Just turn off "locked" bit and increase gen number.
      uint64_t sub_delta = ((uint64_t)1 << 62) - 1;
      control_.fetch_sub(sub_delta);
    } else {
      // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
      uint64_t add_delta = ((uint64_t)1 << 63) - ((uint64_t)1 << 62) + 1;
      control_.fetch_add(add_delta);
    }
  }

  private:
  std::atomic<uint64_t> control_;
};
static_assert(sizeof(AtomicGenLock) == 8, "sizeof(AtomicGenLock) != 8");

}
} // namespace FASTER::test
