// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "contexts.h"

#include "core/async.h"
#include "core/thread.h"
#include "core/status.h"

#include "common/log.h"

#include "network/wireformat.h"

namespace SoFASTER {
namespace server {

using namespace wireformat;

/// Callback invoked when a read that went async inside FASTER completes.
template <class K, class V, class R, class session_t>
void readCallback(IAsyncContext* ctxt, FASTER::core::Status result) {
  // If the result says NotReady, then it means that the record was not found,
  // but it belongs to a migrating hash range. Add it to the session's waiting
  // queue and return. Don't need to heap allocate again here.
  if (result == FASTER::core::Status::NotReady) {
    auto context = reinterpret_cast<ReadContext<K, V, R, session_t>*>(ctxt);
    context->session_->waiting.emplace_back(std::make_tuple(
                                            (uint8_t) Opcode::READ, context));
    return;
  }

  // If we're sure that the request does not need to wait, then typecast so
  // that the destructor frees up the heap allocation.
  CallbackContext<ReadContext<K, V, R, session_t>> context(ctxt);

  // Construct an appropriate response for the client with the correct status.
  // Include the pending id so that the client can invoke the appropriate
  // callback with the correct context.
  ReadResponse<V> res(Status::PENDING_SUCCESS, context->pendingId_);
  switch (result) {
  case FASTER::core::Status::Ok:
    res.value = context->value();
    break;

  case FASTER::core::Status::NotFound:
    res.status = Status::PENDING_INVALID_KEY;
    break;

  default:
    res.status = Status::INTERNAL_ERROR;
    break;
  }

  // Enqueue the response onto the session's pending array, decrement
  // the number of pendings.
  uint8_t* temp = reinterpret_cast<uint8_t*>(&res);
  for (auto byte = 0; byte < sizeof(ReadResponse<V>); byte++) {
    context->session_->pendingResponses.emplace_back(temp[byte]);
  }
  context->session_->nPendings--;
}

/// Callback invoked when an upsert that went async inside FASTER completes.
template <class K, class V, class R, class session_t>
void upsertCallback(IAsyncContext* ctxt, FASTER::core::Status result) {
  // If the result says NotReady, then it means that the record was not found,
  // but it belongs to a migrating hash range. Add it to the session's waiting
  // queue and return. Don't need to heap allocate again here.
  if (result == FASTER::core::Status::NotReady) {
    auto context = reinterpret_cast<UpsertContext<K, V, R, session_t>*>(ctxt);
    context->session_->waiting.emplace_back(std::make_tuple(
                                            (uint8_t) Opcode::UPSERT, context));
    return;
  }

  // If we're sure that the request does not need to wait, then typecast so
  // that the destructor frees up the heap allocation.
  CallbackContext<UpsertContext<K, V, R, session_t>> context(ctxt);

  // Construct an appropriate response for the client with the correct status.
  // Include the pending id so that the client can invoke the appropriate
  // callback with the correct context.
  UpsertResponse res(Status::PENDING_SUCCESS, context->pendingId_);
  if (result != FASTER::core::Status::Ok)
    res.status = Status::PENDING_INTERNAL_ERROR;

  // Enqueue the response onto the session's pending array.
  uint8_t* temp = reinterpret_cast<uint8_t*>(&res);
  for (auto byte = 0; byte < sizeof(UpsertResponse); byte++) {
    context->session_->pendingResponses.emplace_back(temp[byte]);
  }
  context->session_->nPendings--;
}

/// Callback invoked when an rmw that went async inside FASTER completes.
template <class K, class V, class R, class session_t>
void rmwCallback(IAsyncContext* ctxt, FASTER::core::Status result) {
  // If the result says NotReady, then it means that the record was not found,
  // but it belongs to a migrating hash range. Add it to the session's waiting
  // queue and return. Don't need to heap allocate again here.
  if (result == FASTER::core::Status::NotReady) {
    auto context = reinterpret_cast<RmwContext<K, V, R, session_t>*>(ctxt);
    context->session_->waiting.emplace_back(std::make_tuple(
                                            (uint8_t) Opcode::RMW, context));
    return;
  }

  // If we're sure that the request does not need to wait, then typecast so
  // that the destructor frees up the heap allocation.
  CallbackContext<RmwContext<K, V, R, session_t>> context(ctxt);

  // Construct an appropriate response for the client with the correct status.
  // Include the pending id so that the client can invoke the appropriate
  // callback with the correct context.
  RmwResponse res(Status::PENDING_SUCCESS, context->pendingId_);
  if (result != FASTER::core::Status::Ok)
    res.status = Status::PENDING_INTERNAL_ERROR;

  // Enqueue the response onto the session's pending array.
  uint8_t* temp = reinterpret_cast<uint8_t*>(&res);
  for (auto byte = 0; byte < sizeof(RmwResponse); byte++) {
    context->session_->pendingResponses.emplace_back(temp[byte]);
  }
  context->session_->nPendings--;
}

} // end of namespace server
} // end of namespace SoFASTER
