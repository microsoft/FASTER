// Copyright (c) University of Utah. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>

#include "hash.h"

#include "core/thread.h"
#include "core/utility.h"
#include "core/key_hash.h"

namespace SoFASTER {

using namespace FASTER::core;

/// This class defines the type on the Key indexed by FASTER.
class Key {
 public:
  /// Constructs a key given an 8 byte integer.
  Key(uint64_t key)
   : key(key)
  {}

  /// Equality operator. Returns true if the argument matches this key.
  inline bool operator==(const Key& other) const {
    return key == other.key;
  }

  /// Inequality operator. Returns true if there is a mismatch between the
  /// argument and the key.
  inline bool operator!=(const Key& other) const {
    return key != other.key;
  }

  /// Returns the size of the key in bytes.
  inline static constexpr uint32_t size() {
    return sizeof(Key);
  }

  /// Returns the hash of the underlying key.
  inline FASTER::core::KeyHash GetHash() const {
    uint64_t o[2];
    MurmurHash3_x86_128(&key, 8, 1, o);
    return FASTER::core::KeyHash(o[0]);
  }

 private:
  /// The actual underlying 8 byte key.
  uint64_t key;
};

/// This class defines the type on the Value indexed by FASTER.
class Value {
 public:
  /// Constructs a value out of an 8 byte integer.
  Value(uint64_t value)
   : value(value)
  {}

  /// Default constructor that sets the underlying value to zero.
  Value()
   : value(0)
  {}

  /// Copy constructor.
  Value(const Value& from)
   : value(from.value)
  {}

  /// Copy-assign operator.
  Value& operator=(const Value& from) {
    value = from.value;
    return *this;
  }

  /// Addition operator.
  Value& operator+=(const Value& other) {
    value += other.value;
    return *this;
  }

  /// Returns the size of the value in bytes.
  inline static constexpr uint32_t size() {
    return sizeof(Value);
  }

  /// Copies the value into a passed in argument.
  inline void copy(Value* val) const {
    val->value = value;
  }

  /// Atomically copies the value into a passed in argument.
  inline void copyAtomic(Value* val) const {
    val->value = atomicValue.load();
  }

  /// Stores a passed in value into this value.
  inline void store(Value& val) {
    value = val.value;
  }

  /// Atomically stores a passed in value into this value.
  inline void storeAtomic(Value& val) {
    atomicValue.store(val.value);
  }

  /// Adds up two passed in values and stores the result into this value.
  inline void modify(const Value& base, Value& mod) {
    value = base.value + mod.value;
  }

  /// Atomically increments this value by a passed in value.
  inline bool modifyAtomic(Value& mod) {
    atomicValue.fetch_add(mod.value);
    return true;
  }

 private:
  /// The underlying 8 byte value. The atomic allows for operations when the
  /// record falls in the mutable region.
  union {
    uint64_t value;
    std::atomic<uint64_t> atomicValue;
  };
};

/// This class defines a value guarded by a cache-aligned spinlock.
///
/// The following are template arguments:
///    - S: Size of the value (excluding the lock) in bytes.
template<uint64_t S>
class ValueLocked {
 public:
  /// Constructs the value, initializing it with the passed-in argument.
  ValueLocked(uint64_t val)
   : lock(0)
   , value()
  {
    static_assert(S >= 8, "Value not large enough (< 8 Bytes");
    for (auto i = 0; i < S; i++) value[i] = 0;
    *reinterpret_cast<uint64_t*>(value) = val;
  }

  /// Default constructs the value, initializing it to zero.
  ValueLocked()
   : lock(0)
   , value()
  {
    for (auto i = 0; i < S; i++) value[i] = 0;
  }

  /// Copy constructor.
  ValueLocked(const ValueLocked& from)
  {
    lock.store(from.lock.load());
    for (auto i = 0; i < S; i++) value[i] = from.value[i];
  }

  /// Copy-assign operator.
  ValueLocked& operator=(const ValueLocked& from) {
    lock.store(from.lock.load());
    for (auto i = 0; i < S; i++) value[i] = from.value[i];
    return *this;
  }

  /// Returns the size of an instance of this class in bytes.
  inline static constexpr uint32_t size() {
    return sizeof(ValueLocked);
  }

  /// Copies the underlying byte array into a passed in value.
  inline void copy(ValueLocked* val) const {
    for (auto i = 0; i < S; i++) (val->value)[i] = value[i];
  }

  /// Atomically copies the underlying array into a passed in value.
  inline void copyAtomic(ValueLocked* val) {
    Guard g(lock);
    for (auto i = 0; i < S; i++) (val->value)[i] = value[i];
  }

  /// Copies the passed in argument into the underlying value.
  inline void store(ValueLocked& val) {
    for (auto i = 0; i < S; i++) value[i] = val.value[i];
  }

  /// Atomically copies the passed in argument into the underlying value.
  inline void storeAtomic(ValueLocked& val) {
    Guard g(lock);
    for (auto i = 0; i < S; i++) value[i] = val.value[i];
  }

  /// Adds up two passed in values and stores the result into this value.
  inline void modify(const ValueLocked& base, ValueLocked& mod) {
    auto ptr = reinterpret_cast<uint64_t*>(value);
    *ptr = *reinterpret_cast<const uint64_t*>(base.value) +
           *reinterpret_cast<uint64_t*>(mod.value);
  }

  /// Atomically increments this value by a passed in value.
  inline bool modifyAtomic(ValueLocked& mod) {
    Guard g(lock);
    auto ptr = reinterpret_cast<uint64_t*>(value);
    *ptr += *reinterpret_cast<uint64_t*>(mod.value);
    return true;
  }

 private:
  /// A simple lock guard. Acquires a spinlock on construction, releases on
  /// destruction. Requires a reference to the atomic on construction.
  class Guard {
   public:
    Guard(std::atomic<uint64_t>& l)
     : lRef(l)
    {
      while (true) {
        uint64_t e = 0;
        if (lRef.compare_exchange_weak(e, 1, std::memory_order_acquire)) break;
      }
    }

    ~Guard()
    {
      lRef.store(0, std::memory_order_release);
    }

   private:
    std::atomic<uint64_t>& lRef;
  };

  /// Cache-aligned spinlock to provide atomic access when the value falls
  /// inside FASTER's mutable region.
  alignas(64) std::atomic<uint64_t> lock;

  /// The underlying value. A simple byte-array for now.
  uint8_t value[S];
};

/// A user/ad profile with some metadata and a click count. `V` determines
/// the type on this click count.
template<class V>
class Profile {
 public:
  /// For convenience. We will use this for the profile's click count.
  typedef V counter_t;

  /// Default constructor. Initializes every element of the underlying array(s)
  /// to zero.
  Profile()
   : clicks()
   , metadata()
  {}

  /// Copy constructor. Just copies over the passed in profile.
  Profile(const Profile& from)
   : clicks()
   , metadata()
  {
    clicks[0] = from.clicks[0];
    for (auto i = 0; i < 192; i++) metadata[i] = from.metadata[i];
  }

  /// Copy-assign constructor. Again, just copies over the passed in profile.
  Profile& operator=(const Profile& from) {
    clicks[0] = from.clicks[0];
    for (auto i = 0; i < 192; i++) metadata[i] = from.metadata[i];
    return *this;
  }

  /// Returns the size of the object in bytes.
  inline static constexpr uint32_t size() {
    return sizeof(Profile);
  }

  /// Copies the counter into a passed in argument.
  inline void copy(counter_t* val) const {
    clicks[0].copy(val);
  }

  /// "Atomically" copies the counter into a passed in argument.
  inline void copyAtomic(counter_t* val) const {
    clicks[0].copyAtomic(val);
  }

  /// Stores a passed in value into the counter.
  inline void store(counter_t& val) {
    clicks[0].store(val);
  }

  /// Atomically stores a passed in value (array_value_t) into the counter.
  inline void storeAtomic(counter_t& val) {
    clicks[0].storeAtomic(val);
  }

  /// Copies a passed in profile, and then increments the counter by a value.
  inline void modify(const Profile& base, counter_t& mod) {
    for (auto i = 0; i < 192; i++) metadata[i] = base.metadata[i];

    clicks[0].modify(base.clicks[0], mod);
  }

  /// Atomically increments the counter by a passed in value.
  inline bool modifyAtomic(counter_t& mod) {
    return clicks[0].modifyAtomic(mod);
  }

 private:
  /// Number of clicks on the profile. Cache aligned. This is cache-padded to
  /// avoid false sharing. As a result, counter_t needs to be 8 Bytes wide.
  alignas(64) counter_t clicks[8];
  static_assert(counter_t::size() == 8, "Profile counter type is not 8 Bytes");

  /// Profile metadata. This is read-only and is not mutated by RMWs.
  uint8_t metadata[192];
};

} // end namespace SoFASTER.
