// Copyright (c) Microsoft Corporation. All rights reserved.
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

namespace SoFASTER {
namespace server {

using namespace SoFASTER::wireformat;

/// Thread local serial number. Counts the number of operations issued by
/// this thread to the FASTER instance.
thread_local uint64_t serial = 0;

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

  default:
    res->status = Status::INTERNAL_ERROR;
    break;
  }
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

  DispatchContext(session_t* session, uint64_t view)
   : workerView(view)
   , session(session)
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
  // Retrieve the session these requests were received on. Also retrieve
  // the view number of the worker thread they were received on.
  auto dCtxt = reinterpret_cast<DispatchContext<session_t>*>(context);
  auto session = dCtxt->session;
  auto curView = dCtxt->workerView;

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

  // Iterate through and handle every request in the batch. The first byte
  // is always the opcode. Use this to identify which operation to perform.
  requests += sizeof(RequestBatchHdr);
  responses += sizeof(ResponseBatchHdr);

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

    // Unknown opcode. Just break out of the loop and stop processing this
    // batch of requests.
    default:
      size = 0;
      break;
    }
  }

  // If there is space on the response buffer, then add in responses to
  // requests that went pending earlier but have now completed.
  auto space = limit - respHdr->size;
  space = (space / sizeof(ReadResponse)) * sizeof(ReadResponse);
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
