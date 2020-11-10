// Copyright (c) University of Utah. All rights reserved.
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

/// Context with all state required to run the control path of migration on the
/// source machine only.
template <class worker_t>
class CtrlPathCtxt {
 public:
  /// Constructs a context with state to issue control path RPCs from within
  /// the FASTER key-value store.
  ///
  /// \param workers
  ///    A list of worker threads that will be involved in the migration.
  CtrlPathCtxt(worker_t** workers)
   : workers(workers)
  {}

  /// Empty destructor for CtrlPathCtxt.
  ~CtrlPathCtxt() {}

  /// The list of worker threads involved in the migration. Required so that
  /// we can call up to the worker and issue the relevant control RPC.
  worker_t** workers;
};

/// Callback invoked along the source's control path. Tries to add a migration
/// and issue the relevant control RPC to the target.
template <class worker_t>
void srcCtrlPath(void* ctxt, KeyHash lHash, KeyHash rHash, std::string& ip,
                 uint16_t basePort, int phase)
{
  auto context = reinterpret_cast<CtrlPathCtxt<worker_t>*>(ctxt);

  auto id = Thread::id() - 1;
  worker_t* worker = context->workers[id];

  worker->addMigration(ip, basePort + id, lHash, rHash);

  switch(phase) {
  case 0:
    worker->migration->prepTarget();
    break;

  case 1:
    worker->migration->transfer();
    break;

  case 2:
    worker->migration->complete();
    break;

  default:
    logMessage(Lvl::ERROR, "Unknown migration phase on source control path");
    break;
  }
};

/// Context containing all state required to transfer ownership of the
/// hash range away from a worker thread. Required by the source.
template <class worker_t>
class OwnershipTfr {
 public:
  /// Constructor for OwnershipTfr.
  ///
  /// \param workers
  ///    A list of worker threads that will be involved in the migration.
  /// \param newView
  ///    The view that the worker thread should be moved to.
  OwnershipTfr(worker_t** workers, uint64_t newView)
   : workers(workers)
   , newView(newView)
  {}

  /// Empty destructor for OwnershipTfr.
  ~OwnershipTfr() {}

  /// The list of worker threads involved in ownership transfer. Required
  /// so that we can update their view number.
  worker_t** workers;

  /// The view number that the worker should be moved to when it transfers
  /// ownership away.
  uint64_t newView;
};

/// Callback invoked when a worker thread transfers ownership. This
/// basically changes the view of the worker thread. Required by the
/// source of a migration.
template <class worker_t>
void txOwnership(void* ctxt) {
  auto context = reinterpret_cast<OwnershipTfr<worker_t>*>(ctxt);

  auto id = Thread::id() - 1;
  worker_t* worker = context->workers[id];
  worker->changeView(context->newView);
};

/// Context containing all state required to initiate and terminate data
/// transfer to the target on a worker thread at the source.
template <class worker_t>
class DataMovtCtxt {
 public:
  /// Constructor for DataMovtCtxt.
  ///
  /// \param workers
  ///    A list of worker threads that will be involved in the migration.
  DataMovtCtxt(worker_t** workers)
   : workers(workers)
  {}

  /// Empty destructor for DataMovtCtxt.
  ~DataMovtCtxt() {}

  /// The list of worker threads that will be involved in data transfer.
  /// Required so that we can create migration objects on them.
  worker_t** workers;
};

/// Callback invoked when a worker thread on the source starts moving data to
/// the target.
template <class worker_t>
void dataMovtStart(void* ctxt, KeyHash lHash, KeyHash rHash,
                   std::string& ip, uint16_t basePort)
{
  auto context = reinterpret_cast<DataMovtCtxt<worker_t>*>(ctxt);

  // Add a migration to the worker.
  auto id = Thread::id() - 1;
  worker_t* worker = context->workers[id];

  // This worker might not have been involved in the control path. As a
  // result, need to add a migration here.
  worker->addMigration(ip, basePort + id, lHash, rHash);
  worker->migration->enableDataPath();
};

/// Callback invoked when a worker thread on the source ceases moving data to
/// the target.
template <class worker_t>
void dataMovtCease(void* ctxt) {
  auto context = reinterpret_cast<DataMovtCtxt<worker_t>*>(ctxt);

  // Remove the migration from the worker.
  auto id = Thread::id() - 1;
  worker_t* worker = context->workers[id];
  worker->delMigration();
};

/// Empty callback for the start of the target's data path.
void targetData(void* ctxt, KeyHash l, KeyHash r, std::string& ip,
                uint16_t port)
{
  return;
}

/// Context passed into FASTER's compact method. Contains all state required
/// to correctly determine the owner of a key.
template <class sofaster_t>
class CompactionCtxt {
 public:
  /// Constructs a context given a pointer to a client library and the IP
  /// address of this server.
  CompactionCtxt(sofaster_t* lib, std::string& ip,
                 std::tuple<KeyHash, KeyHash, std::string, uint16_t>* migr,
                 bool insert=true)
   : sofaster(lib)
   , myIPAddr(ip)
   , migration(migr)
   , skipInsert(!insert)
  {}

  /// Destroys the context. Nothing interesting here.
  ~CompactionCtxt() {}

  /// Sofaster library that can be used to determine the owner of a particular
  /// key and transmit it if required.
  sofaster_t* sofaster;

  /// The IP address of this sofaster server. Required to determine if the
  /// server owns a particular key during compaction.
  std::string myIPAddr;

  /// XXX: This lets us keep track of a migration so that we can correctly
  /// transmit records during compaction for the purpose of benchmarking.
  std::tuple<KeyHash, KeyHash, std::string, uint16_t>* migration;

  /// Determines if the algorithm should insert records owned by the server
  /// back into the log (false) or not (true).
  bool skipInsert;
};

/// A callback passed into FASTER's Compact() method that validates whether
/// a particular key is owned by a server. If it isn't, then it is migrated
/// to the correct server.
template <class sofaster_t, class record_t>
bool validate_ownership(void* ctxt, uint8_t* raw) {
  auto context = reinterpret_cast<CompactionCtxt<sofaster_t>*>(ctxt);
  auto record = reinterpret_cast<record_t*>(raw);

  auto key = const_cast<typename sofaster_t::key_t&>(record->key());
  auto val = const_cast<typename sofaster_t::val_t&>(record->value());

  // No prior migration. This means that we're benchmarking regular
  // compaction. Just return true over here.
  if (context->migration == nullptr) return true;

  // There was a prior migration. Check if the keys hash range falls
  // inside of it. If it doesn't, then no need to send this out.
  auto lHash = std::get<0>(*(context->migration));
  auto rHash = std::get<1>(*(context->migration));
  bool migrt = key.GetHash() >= lHash && key.GetHash() <= rHash;
  if (!migrt) return !(context->skipInsert);

  // Turns out we don't own this key anymore. Upsert it into the client
  // library. This library will take care of transmitting it to the
  // correct sofaster server. Return false, indicating to the compaction
  // algorithm that it must discard this record.
  typename sofaster_t::UpsertContext upsertCtxt(key, val);
  auto callback = [](FASTER::core::IAsyncContext* ctxt,
                       FASTER::core::Status result)
  {
    // Need to construct this class so that the heap allocated
    // context (ctxt) is freed correctly.
    CallbackContext<typename sofaster_t::UpsertContext> context(ctxt);
    assert(result == FASTER::core::Status::Ok);
  };
  context->sofaster->condUpsert(upsertCtxt, callback);
  return false;
}

} // end of namespace server
} // end of namespace SoFASTER
