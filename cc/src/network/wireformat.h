// Copyright (c) University of Utah. All rights reserved.
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

  /// The server should migrate a range of it's hash space to another server.
  /// The server that receives this RPC is the source of the migration.
  MIGRATE,

  /// The server should prepare itself to receive ownership of a hash range.
  /// The server that receives this RPC is the target of the migration.
  PREP_FOR_TRANSFER,

  /// The server should take ownership of a hash range, and get ready to
  /// receive records belonging to the range over the network.
  TAKE_OWNERSHIP,

  /// Migration is over. The target server can go back to normal operation.
  COMPLETE_MIGRATION,

  /// The server should compact it's hybrid log upto a given log offset.
  COMPACT,

  /* Normal operations */

  /// Reads a value from the server, and returns it to the client.
  READ,

  /// Inserts a value into the server, overriding a previous value if
  /// one existed.
  UPSERT,

  /// Atomically updates a value at the server.
  RMW,

  /// Upserts a value into the server only if the hash table points to an
  /// indirection record or if the key does not exist on the server.
  CONDITIONAL_UPSERT,

  /* Migration datapath */

  /// The client (source of the migration in this case) wants to push a set of
  /// records belonging to a migrating hash range to the target.
  MIGRATION_PUSH_HASHES,
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

  /// The batch of requests weren't executed at the server because there were
  /// already many waiting requests. The client should back-off and reissue
  /// this batch at a later point of time.
  RETRY_STRICT,

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

/// A Migrate() requests. Asks a server to migrate a portion of it's hash
/// space to a target server. This is a packed struct.
#pragma pack(push, 1)
struct MigrateRequest {
  /// Opcode of this request. MIGRATE.
  Opcode op;

  /// Lower bound on the hash range to be migrated to the target.
  KeyHash firstHash;

  /// Upper bound on the hash range to be migrated to the target.
  KeyHash finalHash;

  /// Network IP address of the destination/target machine.
  in_addr_t destIP;

  /// Base port on which workers on the target are listening for
  /// incoming connections and sessions.
  uint16_t destPort;

  MigrateRequest(uint64_t lHash, uint64_t rHash,
                 in_addr_t ip, uint16_t port)
   : op(Opcode::MIGRATE)
   , firstHash(lHash)
   , finalHash(rHash)
   , destIP(ip)
   , destPort(port)
  {}
};
#pragma pack(pop)
static_assert(sizeof(MigrateRequest) == 23, "size of MigrateRequest != 23");

/// A response to a Migrate() request. The status field indicates if the
/// migration was started or not. This is a packed struct.
#pragma pack(push, 1)
struct MigrateResponse {
  /// Opcode of the request. MIGRATE.
  Opcode op;

  /// Indicates whether the migration was started at the source (SUCCESS).
  Status status;

  MigrateResponse(Status status)
   : op(Opcode::MIGRATE)
   , status(status)
  {}
};
#pragma pack(pop)
static_assert(sizeof(MigrateResponse) == 2, "size of MigrateResponse != 2");

/// A PrepForTransfer() request that informs a server of a hash range whose
/// ownership will be transferred over to it sometime in the future. This is
/// a packed struct.
#pragma pack(push, 1)
struct PrepTransferRequest {
  /// Opcode of the request. PREP_FOR_TRANSFER.
  Opcode op;

  /// Lower bound on the hash range that will be eventually owned by the server.
  KeyHash firstHash;

  /// Upper bound on the hash range that will be eventually owned by the server.
  KeyHash finalHash;

  PrepTransferRequest(uint64_t lHash, uint64_t rHash)
   : op(Opcode::PREP_FOR_TRANSFER)
   , firstHash(lHash)
   , finalHash(rHash)
  {}
};
#pragma pack(pop)
static_assert(sizeof(PrepTransferRequest) == 17,
              "size of PrepTransferRequest != 17");

/// A response to a PrepForTransfer() request. The status field indicates
/// whether it succeeded or failed at the target. This is a packed struct.
#pragma pack(push, 1)
struct PrepTransferResponse {
  /// Opcode of the request. PREP_FOR_TRANSFER.
  Opcode op;

  /// Status indicating whether the request succeeded or failed at the target.
  Status status;

  PrepTransferResponse(Status status)
   : op(Opcode::PREP_FOR_TRANSFER)
   , status(status)
  {}
};
#pragma pack(pop)
static_assert(sizeof(PrepTransferResponse) == 2,
              "size of PrepTransferResponse != 2");

/// A TakeOwnership() request that informs a target machine that it is now safe
/// to take ownership of and start servicing requests on the hash range that it
/// was previously prepared to receive. This is a packed struct.
#pragma pack(push, 1)
struct TakeOwnershipRequest {
  /// Opcode of the request. TAKE_OWNERSHIP.
  Opcode op;

  /// Lower bound on the hash range that will be owned by the server.
  KeyHash firstHash;

  /// Upper bound on the hash range that will be owned by the server.
  KeyHash finalHash;

  /// The number of bytes worth of sampled records in the RPC buffer.
  size_t sampledBytes;

  TakeOwnershipRequest(size_t size, KeyHash lHash, KeyHash rHash)
   : op(Opcode::TAKE_OWNERSHIP)
   , firstHash(lHash)
   , finalHash(rHash)
   , sampledBytes(size)
  {}
};
#pragma pack(pop)
static_assert(sizeof(TakeOwnershipRequest) == 25,
              "size of TakeOwnershipRequest != 25");

/// A response to a TakeOwnership() request. The status field indicates
/// whether it succeeded or failed at the target. This is a packed struct.
#pragma pack(push, 1)
struct TakeOwnershipResponse {
  /// Opcode of the request. TAKE_OWNERSHIP.
  Opcode op;

  /// Status indicating whether the request succeeded or failed at the target.
  Status status;

  TakeOwnershipResponse(Status status)
   : op(Opcode::TAKE_OWNERSHIP)
   , status(status)
  {}
};
#pragma pack(pop)
static_assert(sizeof(TakeOwnershipResponse) == 2,
              "size of TakeOwnershipResponse != 2");

/// A CompleteMigration() request that informs a target server that a migration
/// has completed and that it can return to normal request processing. This is
/// a packed struct.
#pragma pack(push, 1)
struct CompleteMigrationRequest {
  /// Opcode of the request. COMPLETE_MIGRATION.
  Opcode op;

  /// Lower bound on the hash range whose migration we're completing.
  KeyHash firstHash;

  /// Upper bound on the hash range whose migration we're completing.
  KeyHash finalHash;

  CompleteMigrationRequest(KeyHash lHash, KeyHash rHash)
   : op(Opcode::COMPLETE_MIGRATION)
   , firstHash(lHash)
   , finalHash(rHash)
  {}
};
#pragma pack(pop)
static_assert(sizeof(CompleteMigrationRequest) == 17,
              "size of CompleteMigrationRequest != 17");

/// A response to a CompleteMigration() request. The status field indicates
/// whether it succeeded or failed at the target. This is a packed struct.
#pragma pack(push, 1)
struct CompleteMigrationResponse {
  /// Opcode of the request. COMPLETE_MIGRATION.
  Opcode op;

  /// Status indicating whether the request succeeded or failed at the target.
  Status status;

  CompleteMigrationResponse(Status s)
   : op(Opcode::COMPLETE_MIGRATION)
   , status(s)
  {}
};
#pragma pack(pop)
static_assert(sizeof(CompleteMigrationResponse) == 2,
              "size of CompleteMigrationResponse != 2");

/// A Compact() request that indicates to a server that it must compact its
/// hybrid log upto a given address (`until`).
#pragma pack(push, 1)
struct CompactRequest {
  /// Opcode of the request. COMPACT.
  Opcode op;

  /// Log offset upto which compaction must be performed.
  uint64_t until;

  CompactRequest(uint64_t until)
   : op(Opcode::COMPACT)
   , until(until)
  {}
};
#pragma pack(pop)
static_assert(sizeof(CompactRequest) == 9,
              "size of CompactRequest != 9");

/// A response to a Compact() request. Contains a status indicating whether
/// the request succeeded or failed at the target.
#pragma pack(push, 1)
struct CompactResponse {
  /// Opcode of the request. COMPACT.
  Opcode op;

  /// Status indicating whether the request succeeded or failed at the server.
  Status status;

  CompactResponse(Status status)
   : op(Opcode::COMPACT)
   , status(status)
  {}
};
#pragma pack(pop)
static_assert(sizeof(CompactResponse) == 2,
              "size of CompactResponse != 2");

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

/// A CondUpsert() request. Inserts a key and value pair as long as the
/// server's hash table does not map the key to an indirection record.
/// This is a packed struct, and templated on the key and value type that
/// FASTER will be compiled with.
#pragma pack(push, 1)
template<class K, class V>
struct CondUpsertRequest {
  /// Opcode of this request. CONDITIONAL_UPSERT.
  Opcode op;

  /// The key to be inserted at the server.
  K key;

  /// The value that the key should map to after the insertion.
  V val;

  CondUpsertRequest(K key, V val)
   : op(Opcode::CONDITIONAL_UPSERT)
   , key(key)
   , val(val)
  {}
};
#pragma pack(pop)

/// Response to a CondUpsert(). This is a packed struct.
#pragma pack(push, 1)
struct CondUpsertResponse {
  /// Opcode of the request. CONDITIONAL_UPSERT.
  Opcode op;

  /// Status indicating whether the request succeeded, failed, or went pending.
  Status status;

  /// An identifier for the request if it went pending at the server. Relevant
  /// only if the above status is PENDING*.
  uint64_t pendingId;

  CondUpsertResponse(Status status, uint64_t id)
   : op(Opcode::CONDITIONAL_UPSERT)
   , status(status)
   , pendingId(id)
  {}
};
#pragma pack(pop)
static_assert(sizeof(CondUpsertResponse) == 10,
             "sizeof CondUpsertResponse != 10");

/// A PushHashes() migration request. Moves a batch of records from a source
/// server (the client of the request) to a target server. This struct
/// is packed.
#pragma pack(push, 1)
struct PushHashesRequest {
  /// Opcode of the request. MIGRATION_PUSH_HASHES.
  Opcode op;

  /// Lower bound on the hash range that will be eventually owned by the server.
  KeyHash firstHash;

  /// Upper bound on the hash range that will be eventually owned by the server.
  KeyHash finalHash;

  /// The total number of bytes worth of records in this request, starting
  /// immediately after this header.
  uint32_t size;

  PushHashesRequest(KeyHash lHash, KeyHash rHash, uint32_t size)
   : op(Opcode::MIGRATION_PUSH_HASHES)
   , firstHash(lHash)
   , finalHash(rHash)
   , size(size)
  {}
};
#pragma pack(pop)
static_assert(sizeof(PushHashesRequest) == 21, "size of PushHashesRequest != 21");

/// A response to a PushHashes() request. The status field indicates whether
/// the request succeeded or failed at the server.
#pragma pack(push, 1)
struct PushHashesResponse {
  /// Opcode of the request. MIGRATION_PUSH_HASHES.
  Opcode op;

  /// Status indicating whether the request succeeded (SUCCESS) or failed.
  Status status;

  PushHashesResponse(Status status)
   : op(Opcode::MIGRATION_PUSH_HASHES)
   , status(status)
  {}
};
#pragma pack(pop)
static_assert(sizeof(PushHashesResponse) == 2,
              "size of PushHashesResponse != 2");

} // end namespace wireformat
} // end namespace SoFASTER
