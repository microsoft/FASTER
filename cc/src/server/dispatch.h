// Copyright (c) University of Utah. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <map>

#include "contexts.h"
#include "callbacks.h"

#include "core/status.h"
#include "core/constants.h"
#include "core/record.h"

#include "session/status.h"
#include "network/wireformat.h"

#include "client/sofaster.h"

/// Uncomment below statement to drop any received migrated data.
/// #define DROP_MIGRATION_DATA 1

namespace SoFASTER {
namespace server {

using namespace SoFASTER::wireformat;

/// Thread local serial number. Counts the number of operations issued by
/// this thread to the FASTER instance.
thread_local uint64_t serial = 0;

/// If true, the server refuses to execute requests on a session once its
/// waiting queue size crosses a certain threshold.
bool strict = false;

/// If greater than zero, then performs a hash validation against every key
/// before executing it against FASTER.
int splits = 0;

/// Handles a single read operation.
///
/// \param session
///    The session the read was issued on.
/// \param req
///    The header for the read request. Contains the key to be looked up.
///
/// \param[out] res
///    The header for the read response. Will contain the status of the read.
///
/// The following are template arguments.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of each read. Required to correctly unpack
///       RPCs received from a client.
///    session_t: The type on the session RPCs are received on.
template<class K, class V, class R, class session_t>
static void handleRead(session_t* session,
                       ReadContext<K, V, R, session_t>& context,
                       struct ReadResponse<V>* res)
{
  // Issue the read to FASTER. If it succeeds, then write the value into the
  // response buffer. Else, set the appropriate status on the response.
  switch (session->service->Read(context, readCallback<K, V, R, session_t>,
                                 ++serial))
  {
  case FASTER::core::Status::Ok:
    res->status = Status::SUCCESS;
    res->value = context.value();
    break;

  case FASTER::core::Status::Pending:
    res->status = Status::PENDING;
    session->nPendings++;
    break;

  case FASTER::core::Status::NotFound:
    res->status = Status::INVALID_KEY;
    break;

  case FASTER::core::Status::NotReady: {
    // Looks like this key belongs to a migrating hash range. Enqueue it on
    // the session's waiting list for now.
    IAsyncContext* copy = nullptr;
    if (context.DeepCopy(copy) != FASTER::core::Status::Ok) {
      res->status = Status::INTERNAL_ERROR;
      break;
    };
    session->waiting.emplace_back(std::make_tuple((uint8_t) Opcode::READ,
                                  copy));

    res->status = Status::PENDING;
    break;
  }

  default:
    res->status = Status::INTERNAL_ERROR;
    break;
  }
}

/// Issues a single Upsert operation to FASTER. Sets the fields on the
/// response header appropriately.
///
/// \param session
///    The session the upsert was issued on.
/// \param req
///    The header for the upsert request. Contains the key and the value.
///
/// \param[out] res
///    The header for the upsert response. Will contain the status of the
///    upsert (ex: success, pending etc).
///
/// The following are template arguments.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of each upsert. Required to correctly unpack
///       RPCs received from a client.
///    session_t: The type on the session RPCs are received on.
template<class K, class V, class R, class session_t>
static void handleUpsert(session_t* session,
                         UpsertContext<K, V, R, session_t>& context,
                         struct UpsertResponse* res)
{
  // Issue the Upsert to FASTER. Based on the outcome, set the status on
  // the response appropriately.
  switch (session->service->Upsert(context, upsertCallback<K, V, R, session_t>,
                                   ++serial))
  {
  case FASTER::core::Status::Ok:
    res->status = Status::SUCCESS;
    break;

  case FASTER::core::Status::Pending:
    res->status = Status::PENDING;
    session->nPendings++;
    break;

  case FASTER::core::Status::NotReady: {
    // Looks like this key belongs to a migrating hash range. Enqueue it on
    // the session's waiting list for now.
    IAsyncContext* copy = nullptr;
    if (context.DeepCopy(copy) != FASTER::core::Status::Ok) {
      res->status = Status::INTERNAL_ERROR;
      break;
    };
    session->waiting.emplace_back(std::make_tuple((uint8_t) Opcode::UPSERT,
                                  copy));

    res->status = Status::PENDING;
    break;
  }

  default:
    res->status = Status::INTERNAL_ERROR;
    break;
  }
}

/// Issues a single Rmw operation to FASTER. Sets the fields on the
/// response header appropriately.
///
/// \param session
///    The session the rmw was issued on.
/// \param req
///    The header for the rmw request. Contains the key and the modifier.
///
/// \param[out] res
///    The header for the rmw response. Will contain the status of the
///    rmw (ex: success, pending etc).
///
/// The following are template arguments.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of each rmw. Required to correctly unpack
///       RPCs received from a client.
///    session_t: The type on the session RPCs are received on.
template<class K, class V, class R, class session_t>
static void handleRmw(session_t* session, RmwContext<K, V, R, session_t>& context,
                      struct RmwResponse* res)
{
  // Issue the RMW to FASTER. Based on the outcome, set the status on
  // the response appropriately.
  switch (session->service->Rmw(context, rmwCallback<K, V, R, session_t>,
                                ++serial))
  {
  case FASTER::core::Status::Ok:
    res->status = Status::SUCCESS;
    break;

  case FASTER::core::Status::Pending:
    res->status = Status::PENDING;
    session->nPendings++;
    break;

  case FASTER::core::Status::NotReady: {
    // Looks like this key belongs to a migrating hash range. Enqueue it on
    // the session's waiting list for now.
    IAsyncContext* copy = nullptr;
    if (context.DeepCopy(copy) != FASTER::core::Status::Ok) {
      res->status = Status::INTERNAL_ERROR;
      break;
    };
    session->waiting.emplace_back(std::make_tuple((uint8_t) Opcode::RMW, copy));

    res->status = Status::PENDING;
    break;
  }

  default:
    res->status = Status::INTERNAL_ERROR;
    break;
  }
}

/// Issues a single Conditional Upsert operation to FASTER. Sets the fields on
/// the response header appropriately.
///
/// \param session
///    The session the request was issued on.
/// \param req
///    The header for the request. Contains the key and the value.
///
/// \param[out] res
///    The header for the response. Will contain the status of the
///    upsert (ex: success, pending etc).
///
/// The following are template arguments.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of each upsert. Required to correctly unpack
///       RPCs received from a client.
///    session_t: The type on the session RPCs are received on.
template<class K, class V, class R, class session_t>
static void handleCondUpsert(session_t* session,
                             CondUpsertContext<K, V, R, session_t>& context,
                             struct CondUpsertResponse* res)
{
  // Issue the CondUpsert to FASTER. Based on the outcome, set the status on
  // the response appropriately.
  switch (session->service->CondUpsert(context,
                                       upsertCallback<K, V, R, session_t>,
                                       ++serial))
  {
  case FASTER::core::Status::Ok:
    res->status = Status::SUCCESS;
    break;

  case FASTER::core::Status::Pending:
    res->status = Status::PENDING;
    session->nPendings++;
    break;

  default:
    res->status = Status::INTERNAL_ERROR;
    break;
  }
}

/// Inserts a batch of records migrated from a source server into FASTER.
///
/// \param session
///    The session the rmw was issued on.
/// \param req
///    The header for the PushHashes request. Contains the number of bytes
///    worth of records that have been pushed by the source.
///
/// \param[out] res
///    The header for the PushHashes response. Will contain the status of
///    the request i.e. whether it succeeded or failed.
///
/// The following are template arguments.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of each record. Required to correctly unpack
///       RPCs received from a client.
///    session_t: The type on the session RPCs are received on.
template<class K, class V, class session_t>
static void handlePushHashes(session_t* session, struct PushHashesRequest* req,
                             struct PushHashesResponse* res)
{
  auto lHash = req->firstHash;
  auto rHash = req->finalHash;

  uint32_t size = req->size;
  uint8_t* buff = reinterpret_cast<uint8_t*>(req) + sizeof(PushHashesRequest);

#ifndef DROP_MIGRATION_DATA
  // The callback associated with data transfer.
  auto dCtx = malloc(sizeof(uint64_t));
  auto dCCb = [](void* ctxt) {};
  auto dTup = std::make_tuple(dCtx, targetData, dCCb);

  // The callback associated with ownership transfer. Not used by the target.
  auto oTup = std::make_tuple(nullptr, nullptr);

  // The callback associated with the target's control path.
  auto cTup = std::make_tuple(nullptr, nullptr);

  // The callback associated with the io path is empty for now.
  auto iTup = std::make_tuple(nullptr, nullptr);

  if (!session->service->Receive(buff, size, lHash, rHash, oTup, dTup,
                                 cTup, iTup))
  {
    res->status = Status::INTERNAL_ERROR;
    return;
  }
#endif

  // If we reached here, then it means that all received records were
  // successfully inserted into the server. Mark the request as successful.
  res->status = Status::SUCCESS;
}

/// Handles a request to open the session. Initializes the session, and sets
/// relevant metadata on the passed in response header so that the client can
/// correctly issue requests in the future.
///
/// \param session
///    The session the request was issued on.
/// \param req
///    The header for the OPEN_SESSION request.
///
/// \param[out] res
///    The header for the response. Will contain the view number and ID for
///    the session.
///
/// The following are template arguments.
///    session_t: The type on the session RPCs are received on.
template<class session_t>
static void handleOpen(session_t* session, struct OpenRequest* req,
                       struct OpenResponse* res)
{
  session->init();

  res->status = Status::SUCCESS;
  res->view = session->currentView;
  res->sessionId = session->sessionUID;
}

/// Tries to complete any requests that are waiting on the session.
///
/// \param responses, size
///    Buffer on which responses can be written, and the maximum number of
///    bytes that can be written.
///
/// The following are template arguments.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of each operation. Required to correctly unpack
///       RPCs received from a client.
///    session_t: The type on the session RPCs are received on.
template<class K, class V, class R, class session_t>
static uint32_t handleWaiting(session_t* session, uint8_t* responses,
                              uint32_t size)
{
  uint32_t written = 0;

  // Bound the number of waiting requests we iterate over. This way, we avoid
  // starving other sessions if the number of waiting operations are too high.
  size_t chances = std::min(session->waiting.size(), (size_t) 16);
  size = std::min(size, 1024U);
  while (chances > 0 && session->waiting.size() > 0) {
    // For each waiting request, first check if there is enough space to hold
    // its response in the buffer. If there is, then re-issue the request. If
    // it executes, then well and good. If it went pending, then ignore the
    // response and move on to the next request. This is because we have already
    // told the client that this request went pending the first time it was
    // executed by the dispatch method.
    auto req = session->waiting.front();
    switch (std::get<0>(req)) {
    case static_cast<int>(Opcode::READ): {
      if (size < sizeof(ReadResponse<K>)) return written;

      auto res = reinterpret_cast<ReadResponse<V>*>(responses);
      auto ctx = reinterpret_cast<ReadContext<K, V, R, session_t>*&>(
                                  std::get<1>(req));
      handleRead<K, V, R, session_t>(session, *ctx, res);
      if (res->status == Status::PENDING) {
        chances--;
        break;
      }

      // Status has to reflect the fact that this was a pending operation that
      // completed successfully at the server.
      if (res->status == Status::SUCCESS) res->status = Status::PENDING_SUCCESS;

      // Status has to reflect the fact that this was a pending operation that
      // failed at the server because the key was not found.
      if (res->status == Status::INVALID_KEY)
        res->status = Status::PENDING_INVALID_KEY;

      // Status has to reflect the fact that this was a pending operation that
      // failed at the server due to some internal error.
      if (res->status == Status::INTERNAL_ERROR)
        res->status = Status::PENDING_INTERNAL_ERROR;

      // Explicitly construct this class. This way, the heap allocation gets
      // freed once it goes out of scope. We do this only if the waiting request
      // did not go pending again. If it did, then we still need the heap
      // allocated context to hang around until the request completes.
      CallbackContext<ReadContext<K, V, R, session_t>> context(std::get<1>(req));

      res->op = Opcode::READ;
      res->pendingId = context->pendingId_;

      responses += sizeof(ReadResponse<K>);
      size -= sizeof(ReadResponse<K>);
      written += sizeof(ReadResponse<K>);
      break;
    }

    case static_cast<int>(Opcode::UPSERT): {
      if (size < sizeof(UpsertResponse)) return written;

      auto res = reinterpret_cast<UpsertResponse*>(responses);
      auto ctx = reinterpret_cast<UpsertContext<K, V, R, session_t>*&>(
                                  std::get<1>(req));
      handleUpsert<K, V, R, session_t>(session, *ctx, res);
      if (res->status == Status::PENDING) {
        chances--;
        break;
      }

      // Status has to reflect the fact that this was a pending operation that
      // completed successfully at the server.
      if (res->status == Status::SUCCESS) res->status = Status::PENDING_SUCCESS;

      // Status has to reflect the fact that this was a pending operation that
      // failed at the server due to some internal error.
      if (res->status == Status::INTERNAL_ERROR)
        res->status = Status::PENDING_INTERNAL_ERROR;

      // Explicitly construct this class. This way, the heap allocation gets
      // freed once it goes out of scope. We do this only if the waiting request
      // did not go pending again. If it did, then we still need the heap
      // allocated context to hang around until the request completes.
      CallbackContext<UpsertContext<K, V, R, session_t>> context(std::get<1>(req));

      res->op = Opcode::UPSERT;
      res->pendingId = context->pendingId_;

      responses += sizeof(UpsertResponse);
      size -= sizeof(UpsertResponse);
      written += sizeof(UpsertResponse);
      break;
    }

    case static_cast<int>(Opcode::RMW): {
      if (size < sizeof(RmwResponse)) return written;

      auto res = reinterpret_cast<RmwResponse*>(responses);
      auto ctx = reinterpret_cast<RmwContext<K, V, R, session_t>*&>(
                                  std::get<1>(req));
      handleRmw<K, V, R, session_t>(session, *ctx, res);
      if (res->status == Status::PENDING) {
        chances--;
        break;
      }

      // Status has to reflect the fact that this was a pending operation that
      // completed successfully at the server.
      if (res->status == Status::SUCCESS) res->status = Status::PENDING_SUCCESS;

      // Status has to reflect the fact that this was a pending operation that
      // failed at the server due to some internal error.
      if (res->status == Status::INTERNAL_ERROR)
        res->status = Status::PENDING_INTERNAL_ERROR;

      // Explicitly construct this class. This way, the heap allocation gets
      // freed once it goes out of scope. We do this only if the waiting request
      // did not go pending again. If it did, then we still need the heap
      // allocated context to hang around until the request completes.
      CallbackContext<RmwContext<K, V, R, session_t>> context(std::get<1>(req));

      res->op = Opcode::RMW;
      res->pendingId = context->pendingId_;

      responses += sizeof(RmwResponse);
      size -= sizeof(RmwResponse);
      written += sizeof(RmwResponse);
      break;
    }

    default:
      break;
    }

    // Remove the request from the queue. If it went pending above, then the
    // relevant method would have anyway re-enqueued it.
    session->waiting.pop_front();
  }

  return written;
}

/// Context with all state required to dispatch requests received on a session.
///
/// The following are template arguments.
///    session_t: The type on the session RPCs are received on.
template<class session_t>
struct DispatchContext {
  /// The view number that the worker thread is currently operating in.
  uint64_t workerView;

  /// Pointer to the session the requests were received on.
  session_t* session;

  /// Opaque pointer to a map of hash ranges. Used for benchmarking only.
  void* ranges;

  DispatchContext(session_t* session, uint64_t view, void* maps)
   : workerView(view)
   , session(session)
   , ranges(maps)
  {}
};

/// Executes a batch of operations against FASTER.
///
/// \param context
///    Opaque pointer, casted by this method into a session pointer.
/// \param requests
///    Buffer of requests to be processed.
/// \param responses
///    Buffer that responses should be written into.
///
/// \return
///    The total number of bytes in the response.
///
/// The following are template arguments.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of each rmw and upsert. Required to correctly
///       unpack RPCs received from a client.
///    session_t: The type on the session RPCs are received on.
template<class K, class V, class R, class session_t>
static uint32_t dispatch(void* context, uint8_t* requests, uint8_t* responses)
{
  // Typedef on client library. Required for when we want to benchmark server
  // side hash validation.
  typedef Sofaster<K, V, typename session_t::transport_t> sofaster_t;
  typedef std::map<typename sofaster_t::HashRange,
                   size_t,
                   typename sofaster_t::CompareRanges> mapping_t;

  // Retrieve the session these requests were received on. Also retrieve
  // the view number of the worker thread they were received on.
  auto dCtxt = reinterpret_cast<DispatchContext<session_t>*>(context);
  auto session = dCtxt->session;
  auto curView = dCtxt->workerView;
  auto ranges = reinterpret_cast<mapping_t*>(dCtxt->ranges);

  // Retrieve the header from the request buffer. The header gives us the
  // view these requests were generated in, and the total size of all the
  // requests in bytes.
  auto hdr = reinterpret_cast<RequestBatchHdr*>(requests);
  uint64_t size = hdr->size;

  auto respHdr = reinterpret_cast<ResponseBatchHdr*>(responses);
  respHdr->status = Status::SUCCESS;
  respHdr->view = 0;
  respHdr->size = 0;
  respHdr->reIssueFromLSN = 0;

  // If the session status is "INITIALIZING", then the first request
  // should be an OPEN_SESSION request. Handle it and immediately return
  // a response to the client.
  if (session->status == session::Status::INITIALIZING) {
    auto req = reinterpret_cast<OpenRequest*>(requests +
                                              sizeof(RequestBatchHdr));
    auto res = reinterpret_cast<OpenResponse*>(responses +
                                               sizeof(ResponseBatchHdr));

    // We did not receive an OPEN_SESSION, return an error to the client.
    if (req->op != Opcode::OPEN_SESSION) {
      respHdr->status = Status::INTERNAL_ERROR;
      return sizeof(ResponseBatchHdr);
    }

    res->op = Opcode::OPEN_SESSION;
    handleOpen(session, req, res);

    respHdr->size = sizeof(OpenResponse);
    return respHdr->size + sizeof(ResponseBatchHdr);
  }

  // Check for a view change. If there was one, inform the client of the
  // LSN from which it will need to reissue operations.
  if (curView > hdr->view) {
    respHdr->status = Status::VIEW_CHANGED;
    respHdr->view = curView;
    respHdr->reIssueFromLSN = session->lastServicedLSN;

    return respHdr->size + sizeof(ResponseBatchHdr);
  }

  // Snapshot the number of waiting requests before we started.
  size_t orgWaiting = session->waiting.size();

  // Iterate through and handle every request in the batch. The first byte
  // is always the opcode. Use this to identify which operation to perform.
  requests += sizeof(RequestBatchHdr);
  responses += sizeof(ResponseBatchHdr);

  // We're being strict about this waiting set size. Try to execute a few,
  // but ask the client to resend this batch after a back-off.
  if (orgWaiting > 64 && strict) {
    respHdr->status = Status::RETRY_STRICT;

    auto limit = session_t::maxTxBytes - sizeof(ResponseBatchHdr);
    if (respHdr->size < limit) {
      auto b = handleWaiting<K, V, R, session_t>(session, responses,
                                                 limit - respHdr->size);
      responses += b;
      respHdr->size += b;
    }

    return respHdr->size + sizeof(ResponseBatchHdr);
  }

  while (size) {
    switch (*requests) {
    // READ request. Read a key from FASTER. If it exists, write its value
    // into the response buffer.
    case static_cast<int>(Opcode::READ): {
      auto req = reinterpret_cast<ReadRequest<K, V>*>(requests);
      auto ctx = ReadContext<K, V, R, session_t>(req->key, session,
                                                 session->nextPendingId);
      auto res = reinterpret_cast<ReadResponse<V>*>(responses);
      res->op = Opcode::READ;
      res->pendingId = session->nextPendingId;
      handleRead<K, V, R, session_t>(session, ctx, res);
      session->lastServicedLSN += 1;

      // If the request went pending, then update the identifier that
      // the next pending operation will have.
      if (res->status == Status::PENDING) session->nextPendingId += 1;

      requests += sizeof(ReadRequest<K, V>);
      size -= sizeof(ReadRequest<K, V>);
      responses += sizeof(ReadResponse<V>);
      respHdr->size += sizeof(ReadResponse<V>);
      break;
    }

    // UPSERT request. Insert a key-value pair into FASTER irrespective of
    // what the key originally mapped to.
    case static_cast<int>(Opcode::UPSERT): {
      auto req = reinterpret_cast<UpsertRequest<K, V>*>(requests);
      auto ctx = UpsertContext<K, V, R, session_t>(req->key, req->val, session,
                                                   session->nextPendingId);
      auto res = reinterpret_cast<UpsertResponse*>(responses);
      res->op = Opcode::UPSERT;
      res->pendingId = session->nextPendingId;
      handleUpsert<K, V, R, session_t>(session, ctx, res);
      session->lastServicedLSN += 1;

      // If the request went pending, then update the identifier that
      // the next pending operation will have.
      if (res->status == Status::PENDING) session->nextPendingId += 1;

      requests += sizeof(UpsertRequest<K, V>);
      size -= sizeof(UpsertRequest<K, V>);
      responses += sizeof(UpsertResponse);
      respHdr->size += sizeof(UpsertResponse);
      break;
    }

    // RMW request. Read a key from FASTER. If it exists, update its value,
    // else, insert a new key-value pair into FASTER.
    case static_cast<int>(Opcode::RMW): {
      auto req = reinterpret_cast<RmwRequest<K, V>*>(requests);

      // If configured, validate ownership before issuing operation. Used
      // for benchmarking purposes only.
      if (splits > 0) {
        auto hash = req->key.GetHash();
        if (ranges->at(typename sofaster_t::HashRange(hash, hash)) != 0) {
          logMessage(Lvl::ERROR, "Hash validation on %lu failed",
                     hash.control());
        }
      }

      auto ctx = RmwContext<K, V, R, session_t>(req->key, req->mod, session,
                                                session->nextPendingId);
      auto res = reinterpret_cast<RmwResponse*>(responses);
      res->op = Opcode::RMW;
      res->pendingId = session->nextPendingId;
      handleRmw<K, V, R, session_t>(session, ctx, res);
      session->lastServicedLSN += 1;

      // If the request went pending, then update the identifier that
      // the next pending operation will have.
      if (res->status == Status::PENDING) session->nextPendingId += 1;

      requests += sizeof(RmwRequest<K, V>);
      size -= sizeof(RmwRequest<K, V>);
      responses += sizeof(RmwResponse);
      respHdr->size += sizeof(RmwResponse);
      break;
    }

    // CondUpsert request. Upserts a key only if faster maps it to an
    // indirection record or a tombstone record.
    case static_cast<int>(Opcode::CONDITIONAL_UPSERT): {
      auto req = reinterpret_cast<CondUpsertRequest<K, V>*>(requests);
      auto ctx = CondUpsertContext<K, V, R, session_t>(req->key, req->val,
                                                       session,
                                                       session->nextPendingId);
      auto res = reinterpret_cast<CondUpsertResponse*>(responses);
      res->op = Opcode::CONDITIONAL_UPSERT;
      res->pendingId = session->nextPendingId;
      handleCondUpsert<K, V, R, session_t>(session, ctx, res);
      session->lastServicedLSN += 1;

      // If the request went pending, then update the identifier that
      // the next pending operation will have.
      if (res->status == Status::PENDING) session->nextPendingId += 1;

      requests += sizeof(CondUpsertRequest<K, V>);
      size -= sizeof(CondUpsertRequest<K, V>);
      responses += sizeof(CondUpsertResponse);
      respHdr->size += sizeof(CondUpsertResponse);
      break;
    }

    // Migration PushHashes() request. Upsert all received records into FASTER.
    case static_cast<int>(Opcode::MIGRATION_PUSH_HASHES): {
      auto req = reinterpret_cast<PushHashesRequest*>(requests);
      auto res = reinterpret_cast<PushHashesResponse*>(responses);
      res->op = Opcode::MIGRATION_PUSH_HASHES;
      handlePushHashes<K, V, session_t>(session, req, res);

      requests += sizeof(PushHashesRequest) + req->size;
      size -= sizeof(PushHashesRequest) + req->size;
      responses += sizeof(PushHashesResponse);
      respHdr->size += sizeof(PushHashesResponse);
      break;
    }

    // Unknown opcode. Just break out of the loop and stop processing this
    // batch of requests.
    default:
      size = 0;
      break;
    }
  }

  // Too many requests waiting. Process the batch, but ask the client to
  // Back off for a bit. Do so only if this batch added more than 64 waiting
  // requests to the session.
  if ((session->waiting.size() - orgWaiting) > 64) {
    respHdr->status = Status::RETRY_LATER;
  }

  // If there is space on the response buffer, then try to handle any waiting
  // requests on the session before returning to the client.
  auto limit = session_t::maxTxBytes - sizeof(ResponseBatchHdr);
  if ((respHdr->size < limit) && (respHdr->status != Status::RETRY_LATER)) {
    auto b = handleWaiting<K, V, R, session_t>(session, responses,
                                               limit - respHdr->size);
    responses += b;
    respHdr->size += b;
  }

  // If there is space on the response buffer, then add in responses to
  // requests that went pending earlier but have now completed.
  auto space = limit - respHdr->size;
  space = (space / sizeof(RmwResponse)) * sizeof(RmwResponse);
  if (space > 0 && session->pendingResponses.size() > 0) {
    auto b = std::min(space, session->pendingResponses.size());
    memcpy(responses, &(session->pendingResponses[0]), b);
    responses += b;
    respHdr->size += b;

    auto s = session->pendingResponses.begin();
    session->pendingResponses.erase(s, s + b);
  }

  return respHdr->size + sizeof(ResponseBatchHdr);
}

} // end namespace server.
} // end namespace SoFASTER.
