// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <functional>

namespace FASTER {
namespace test {

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

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(SimpleAtomicValue));
  }

  union {
    T value;
    std::atomic<T> atomic_value;
  };
};

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

}
} // namespace FASTER::test
