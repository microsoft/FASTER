// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <netinet/in.h>

#include "core/guid.h"
#include "core/key_hash.h"

namespace SoFASTER {
namespace wireformat {

using namespace FASTER::core;

/// The set of operations that a client can perform on a SoFASTER node.
enum class Opcode : uint8_t {
  /* Control path */

  /// The client wants to open the session with the server. Once opened, normal
  /// operations (gets, upserts, rmws) can be received and processed on
  /// the session.
  OPEN_SESSION,

  /// The client wants to re-open a session and resume issuing operations on it.
  /// This happens when a server crashed, and a client wants to reconnect to the
  /// recovered session.
  CONTINUE_SESSION,

  /// The client is done issuing operations on the session and would like to
  /// close it.
  CLOSE_SESSION,

  /* Normal operations */

  /// Reads a value from the server, and returns it to the client.
  READ,

  /// Inserts a value into the server, overriding a previous value if
  /// one existed.
  UPSERT,

  /// Atomically updates a value at the server.
  RMW,
};

/// The status of a request; whether it succeeded, failed etc.
enum class Status : uint8_t {
  /// The operation was successful. This code is used to denote that an
  /// individual request (ex: Get()) succeeded, and is also used to
  /// denote that a batch of requests was successfully executed at the
  /// server. There can be cases where a batch executed successfully, but
  /// one of the requests within the batch failed, for example, because the
  /// key was not found.
  SUCCESS,

  /// The operation was for an invalid key that does not exist on the server.
  INVALID_KEY,

  /// The operation went pending at the server, and the client will receive
  /// a response sometime in the future.
  PENDING,

  /// An operation that went pending at the server in the past completed
  /// successfully.
  PENDING_SUCCESS,

  /// An operation that went pending at the server failed because the key was
  /// invalid i.e did not exists at the server.
  PENDING_INVALID_KEY,

  /// The view changed at the server, so none of the operations in the batch
  /// were executed.
  VIEW_CHANGED,

  /// The batch of requests were executed at the server, but many went pending.
  /// The client should back-off for a while.
  RETRY_LATER,

  /// Something went wrong at the server causing the request to fail.
  INTERNAL_ERROR,

  /// Something went wrong at the server causing the request to fail after it
  /// went pending and completed.
  PENDING_INTERNAL_ERROR,
};

/// The header on every batch of requests received at the server. This
/// is a packed struct.
#pragma pack(push, 1)
struct RequestBatchHdr {
  /// The view this batch of requests was issued in.
  uint64_t view;

  /// The size of the batch in bytes.
  uint64_t size;

  RequestBatchHdr(uint64_t view, uint64_t size)
   : view(view)
   , size(size)
  {}
};
#pragma pack(pop)
static_assert(sizeof(RequestBatchHdr) == 16, "size of RequestBatchHdr != 16");

/// The header on every batch of responses received at the server. This
/// is a packed struct.
#pragma pack(push, 1)
struct ResponseBatchHdr {
  /// The status of the batch of requests. Can be "SUCCESS", "RETRY_LATER", or
  /// "VIEW_CHANGED".
  Status status;

  /// The view that the server currently is in.
  uint64_t view;

  /// LSN from which client should reissue operations in a new view. Valid only
  /// if the status field is set to "VIEW_CHANGED".
  uint64_t reIssueFromLSN;

  /// The size of the response batch in bytes.
  uint64_t size;

  ResponseBatchHdr(Status status, uint64_t view, uint64_t size, uint64_t lsn)
   : status(status)
   , view(view)
   , reIssueFromLSN(lsn)
   , size(size)
  {}
};
#pragma pack(pop)
static_assert(sizeof(ResponseBatchHdr) == 25, "size of ResponseBatchHdr != 25");

/// A simple OpenSession() request. When sent over an established connection,
/// this request opens and initializes a session at the server. This is a
/// packed struct.
#pragma pack(push, 1)
struct OpenRequest {
  /// Opcode of this request. OPEN_SESSION.
  Opcode op;

  OpenRequest()
   : op(Opcode::OPEN_SESSION)
  {}
};
#pragma pack(pop)
static_assert(sizeof(OpenRequest) == 1, "size of OpenRequest != 1");

/// A response to an OpenSession() request. If the request was successful at
/// the server, then the sessionId field will contain a unique identifier for
/// this session, and the view field will contain the view number this session
/// should operate under. This is a packed struct.
#pragma pack(push, 1)
struct OpenResponse {
  /// Opcode of the request. OPEN_SESSION.
  Opcode op;

  /// Status indicating whether the request succeeded or failed.
  Status status;

  /// View number that this session should operate under.
  uint64_t view;

  /// Unique identifier for this session.
  Guid sessionId;

  OpenResponse(Status status, uint64_t view, Guid id)
   : op(Opcode::OPEN_SESSION)
   , status(status)
   , view(view)
   , sessionId(id)
  {}
};
#pragma pack(pop)
static_assert(sizeof(OpenResponse) == 10 + sizeof(Guid),
             "size of OpenResponse != 10 + sizeof(Guid)");

/// A simple CloseSession() request. When sent on an open session, this
/// request gracefully shuts it down at the server. This is a packed struct.
#pragma pack(push, 1)
struct CloseRequest {
  /// Opcode of the request. CLOSE_SESSION.
  Opcode op;

  CloseRequest()
   : op(Opcode::CLOSE_SESSION)
  {}
};
#pragma pack(pop)
static_assert(sizeof(CloseRequest) == 1, "size of CloseRequest != 1");

/// A response to a CloseSession() request. The status field indicates whether
/// the session was successfully closed at the server. This struct is packed.
#pragma pack(push, 1)
struct CloseResponse {
  /// Opcode of the request. CLOSE_SESSION.
  Opcode op;

  /// Status indicating whether the request succeeded (SUCCESS) or failed.
  Status status;

  CloseResponse(Status status)
   : op(Opcode::CLOSE_SESSION)
   , status(status)
  {}
};
#pragma pack(pop)
static_assert(sizeof(CloseResponse) == 2, "size of CloseResponse != 2");

/// A simple Read() request that given a key, returns a value to the client.
/// This is a packed struct, and templated on the key and value type that
/// FASTER will be compiled with.
#pragma pack(push, 1)
template<class K, class V>
struct ReadRequest {
  /// Opcode of this request. READ.
  Opcode op;

  /// The key to be looked up at the server.
  K key;

  /// XXX: Garbage to make sure that all our requests (reads, upserts, and
  /// rmws) are of the same size.
  V ignore;

  ReadRequest(K key)
   : op(Opcode::READ)
   , key(key)
   , ignore()
  {}
};
#pragma pack(pop)

/// A response to a Read() request. If the request was executed successfully,
/// the value field will contain the "value" the key maps to. Otherwise, it
/// will be filled with a garbage value. This is a packed struct, and templated
/// with the value type FASTER will be compiled with.
#pragma pack(push, 1)
template<class V>
struct ReadResponse {
  /// Opcode of the request. READ.
  Opcode op;

  /// Status indicating whether the request succeeded, failed, or went pending.
  Status status;

  /// The value that the key maps to. If the request was not successful, then
  /// this will be a garbage value.
  V value;

  /// An identifier for the request if it went pending at the server. Relevant
  /// only if the above status is PENDING*.
  uint64_t pendingId;

  ReadResponse(Status status, uint64_t id)
   : op(Opcode::READ)
   , status(status)
   , value()
   , pendingId(id)
  {}
};
#pragma pack(pop)

/// An Upsert() request. Inserts a key and value pair irrespective of what the
/// key currently maps to. This is a packed struct, and templated on the key
/// and value type that FASTER will be compiled with.
#pragma pack(push, 1)
template<class K, class V>
struct UpsertRequest {
  /// Opcode of this request. UPSERT.
  Opcode op;

  /// The key to be inserted at the server.
  K key;

  /// The value that the key should map to after the insertion.
  V val;

  UpsertRequest(K key, V val)
   : op(Opcode::UPSERT)
   , key(key)
   , val(val)
  {}
};
#pragma pack(pop)

/// Response to an Upsert(). This is a packed struct.
#pragma pack(push, 1)
struct UpsertResponse {
  /// Opcode of the request. UPSERT.
  Opcode op;

  /// Status indicating whether the request succeeded, failed, or went pending.
  Status status;

  /// An identifier for the request if it went pending at the server. Relevant
  /// only if the above status is PENDING*.
  uint64_t pendingId;

  UpsertResponse(Status status, uint64_t id)
   : op(Opcode::UPSERT)
   , status(status)
   , pendingId(id)
  {}
};
#pragma pack(pop)
static_assert(sizeof(UpsertResponse) == 10, "size of UpsertResponse != 10");

/// An Rmw() request. Reads a key from the server and atomically updates it's
/// value. This is a packed struct, and templated on the key and value type that
/// FASTER will be compiled with.
#pragma pack(push, 1)
template<class K, class V>
struct RmwRequest {
  /// Opcode of this request. RMW.
  Opcode op;

  /// The key to be modified at the server.
  K key;

  /// The amount that the key's value should be updated by.
  V mod;

  RmwRequest(K key, V mod)
   : op(Opcode::RMW)
   , key(key)
   , mod(mod)
  {}
};
#pragma pack(pop)

/// Response to a RMW(). This is a packed struct.
#pragma pack(push, 1)
struct RmwResponse {
  /// Opcode of the request. RMW.
  Opcode op;

  /// Status indicating whether the request succeeded, failed, or went pending.
  Status status;

  /// An identifier for the request if it went pending at the server. Relevant
  /// only if the above status is PENDING*.
  uint64_t pendingId;

  RmwResponse(Status status, uint64_t id)
   : op(Opcode::RMW)
   , status(status)
   , pendingId(id)
  {}
};
#pragma pack(pop)
static_assert(sizeof(RmwResponse) == 10, "size of RmwResponse != 10");

} // end namespace wireformat
} // end namespace SoFASTER
