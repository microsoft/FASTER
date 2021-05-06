// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "core/async.h"

namespace SoFASTER {
namespace server {

using namespace FASTER::core; // For IAsyncContext.

/// A context encapsulating all information required to execute a READ
/// operation that might go asynchronous inside FASTER.
///
/// The following are template arguments.
///    K: The type on the key of each record.
///    V: The type on the value of the response.
///    R: The type on the record stored inside FASTER.
///    S: The type on the session requests will be received on.
template<class K, class V, class R, class S>
class ReadContext : public IAsyncContext {
 public:
  // Typedefs on the key and value required internally by FASTER.
  typedef K key_t;
  typedef R value_t;

  /// Constructs a ReadContext that can be passed into FASTER.
  ///
  /// \param key
  ///    The key to be read from FASTER.
  ReadContext(K key, S* session, uint64_t id)
   : session_(session)
   , pendingId_(id)
   , key_(key)
   , value_()
  {}

  /// Copy constructor. Required because the operation can go asynchronous.
  ReadContext(const ReadContext& from)
   : session_(from.session_)
   , pendingId_(from.pendingId_)
   , key_(from.key_)
   , value_(from.value_)
  {}

  /// Accessor for key. Called internally by FASTER.
  inline const K& key() const {
    return key_;
  }

  /// Accessor for value. Required when sending a response to the client that
  /// issued the read operation.
  inline const V& value() const {
    return value_;
  }

  /// Copies a passed in value into the context. Called internally by FASTER
  /// when a record falls inside the immutable region.
  inline void Get(const R& val) {
    val.copy(&value_);
  }

  /// Atomically copies a passed in value into the context. Called internally
  /// by FASTER when a record falls inside the mutable region.
  inline void GetAtomic(const R& val) {
    val.copyAtomic(&value_);
  }

 protected:
  /// Copies this context when the operation goes asynchronous inside FASTER.
  ///
  /// \param context_copy
  ///    Pointer to base context that this context will be copied into.
  FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 public:
  /// Pointer to the session this request was received on. If the request goes
  /// pending, this pointer will help us enqueue a response once it completes.
  S* session_;

  /// Identifier for the request if it goes pending. Required by the remote
  /// client since pending requests can complete out of order.
  uint64_t pendingId_;

 private:
  /// The key to be looked up inside FASTER.
  K key_;

  /// Value that the key maps to. If the read is successful, this will contain
  /// its value. Initialized to zero.
  V value_;
};

/// A context encapsulating all information required to execute an UPSERT
/// operation that might go asynchronous inside FASTER.
///
/// The following are template arguments.
///    K: The type on the key of each record.
///    V: The type on the value of the response.
///    R: The type on the record stored inside FASTER.
///    S: The type on the session requests will be received on.
template<class K, class V, class R, class S>
class UpsertContext : public IAsyncContext {
 public:
  // Typedefs on the key and value required internally by FASTER.
  typedef K key_t;
  typedef R value_t;

  /// Constructs an Upsert context.
  ///
  /// \param key
  ///    The key to Upserted. If it does not already map to a value, a new
  ///    key-value pair will be created inside FASTER.
  /// \param val
  ///    The value the key should map to after the Upsert is performed.
  UpsertContext(K key, V val, S* session, uint64_t id)
   : session_(session)
   , pendingId_(id)
   , key_(key)
   , value_(val)
  {}

  /// Copy constructor. Required for when an Upsert operation goes async
  /// inside FASTER.
  UpsertContext(const UpsertContext& from)
   : session_(from.session_)
   , pendingId_(from.pendingId_)
   , key_(from.key_)
   , value_(from.value_)
  {}

  /// Accessor for the key. Invoked from within FASTER.
  inline const K& key() const {
    return key_;
  }

  /// Returns the size of the value. Invoked from within FASTER when creating
  /// a new key-value pair (because the key did not map to a value to begin
  /// with).
  inline static constexpr uint32_t value_size() {
    return sizeof(R);
  }

  /// Stores this context's value into a passed in reference. This is
  /// typically invoked from within FASTER when a new record corresponding
  /// to the key-value pair is created at the tail of the hybrid log.
  inline void Put(R& val) {
    val.store(value_);
  }

  /// Atomically stores this context's value into a passed in reference. This
  /// is typically invoked from within FASTER when performing an Upsert on a
  /// key-value pair in the HybridLog's mutable region.
  inline bool PutAtomic(R& val) {
    val.storeAtomic(value_);
  }

 protected:
  /// Copies this context when the operation goes asynchronous inside FASTER.
  ///
  /// \param context_copy
  ///    Pointer to base context that this context will be copied into.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 public:
  /// Pointer to the session this request was received on. If the request goes
  /// pending, this pointer will help us enqueue a response once it completes.
  S* session_;

  /// Identifier for the request if it goes pending. Required by the remote
  /// client since pending requests can complete out of order.
  uint64_t pendingId_;

 private:
  /// The key the upsert must be performed against. If this key does not
  /// exist, then it will be newly inserted into FASTER.
  K key_;

  /// The value that the key should map to after the Upsert operation.
  V value_;
};

/// A context encapsulating all information required to execute an RMW
/// operation that might go asynchronous inside FASTER.
///
/// The following are template arguments.
///    K: The type on the key of each record.
///    V: The type on the value of the response.
///    R: The type on the record stored inside FASTER.
///    S: The type on the session requests will be received on.
template<class K, class V, class R, class S>
class RmwContext : public IAsyncContext {
 public:
  // Typedefs on the key and value. Required internally by FASTER.
  typedef K key_t;
  typedef R value_t;

  /// Constructs an RMW context given a key and modifier value.
  RmwContext(K key, V mod, S* session, uint64_t id)
   : session_(session)
   , pendingId_(id)
   , key_(key)
   , mod_(mod)
  {}

  /// Copy constructor for RmwContext. Required when the operation goes
  /// asynchronous inside FASTER.
  RmwContext(const RmwContext& from)
   : session_(from.session_)
   , pendingId_(from.pendingId_)
   , key_(from.key_)
   , mod_(from.mod_)
  {}

  /// Accessor for the key. Invoked from within FASTER.
  inline const K& key() const {
    return key_;
  }

  /// Returns the size of the value. Invoked from within FASTER when creating
  /// a new key-value pair (because the key did not map to a value to begin
  /// with).
  inline static constexpr uint32_t value_size() {
    return sizeof(R);
  }

  /// Returns the size of the value. Invoked from within FASTER when creating
  /// a new key-value pair (because the key was found to be in the read-only
  /// region of the HybridLog).
  inline static constexpr uint32_t value_size(const R& old) {
    return sizeof(R);
  }

  /// Stores this context's modifier into a passed in reference to a value.
  /// This method is invoked when FASTER tries to execute an rmw against a
  /// non-existent key. In this case, a new record is created at the tail of
  /// the hybrid log, and it's value is initialized to the modifier.
  inline void RmwInitial(value_t& value) {
    value.store(mod_);
  }

  /// Stores an old value incremented by this context's modifier into a passed
  /// in reference to a value. This method is invoked when FASTER performs a
  /// RMW on a record in the read-only region of the hybrid log.
  inline void RmwCopy(const value_t& old, value_t& value) {
    value.modify(old, mod_);
  }

  /// Modifies a passed in reference to a value by this context's modifier. This
  /// method is invoked when FASTER performs an RMW on a record in the mutable
  /// region of the hybrid log.
  inline bool RmwAtomic(value_t& value) {
    return value.modifyAtomic(mod_);
  }

 protected:
  /// Copies this context when the operation goes asynchronous inside FASTER.
  ///
  /// \param context_copy
  ///    Pointer to base context that this context will be copied into.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 public:
  /// Pointer to the session this request was received on. If the request goes
  /// pending, this pointer will help us enqueue a response once it completes.
  S* session_;

  /// Identifier for the request if it goes pending. Required by the remote
  /// client since pending requests can complete out of order.
  uint64_t pendingId_;

 private:
  /// The key the rmw operation must be performed against. If this key does not
  /// exist, then it will be newly inserted into FASTER with mod_ as its value.
  K key_;

  /// The amount the key's value must be modified by. If the key is being newly
  /// inserted into FASTER, then this will be its value.
  V mod_;
};

} // end namespace server.
} // end namespace SoFASTER.
