// Copyright (c) University of Utah. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <tuple>
#include <thread>
#include <utility>
#include <arpa/inet.h>

#include "worker.h"
#include "callbacks.h"

#include "core/thread.h"
#include "core/key_hash.h"
#include "core/record.h"

#include "network/wireformat.h"

#include "client/sofaster.h"

namespace SoFASTER {
namespace server {

using namespace wireformat;
using namespace FASTER::core;

/// Admin takes in the following template arguments:
///    F: The type on the FASTER instance used by Worker to serve reads,
///       upserts, and read-modify-writes.
///    T: The type on the transport layer that will be used to receive RPCs
///       and send out responses.
template<class F, class K, class V, class R, class T>
class Admin {
 public:
  // For convenience. Defines the type on a worker thread.
  typedef Worker<F, K, V, R, T> worker_t;

  // The type signature on Sofaster's client library. Required for compaction.
  typedef SoFASTER::Sofaster<K, V, T> sofaster_t;

  // The type signature on a record. Required for compaction.
  typedef FASTER::core::Record<K, V> record_t;

  // For convenience. Defines the type on the FASTER key value store.
  typedef F faster_t;

  // For convenience. Defines the type on the transport layer (ex: Infrc).
  typedef T transport_t;

  // For convenience. Defines the type on the connection exposed
  // by the transport layer.
  typedef typename T::Connection connection_t;

  // For convenience. Defines the type on the connection handler exposed
  // by the transport layer.
  typedef typename T::ConnectHandler conn_poller_t;

  // For convenience. Defines the type on the RPC poller exposed by the
  // transport layer.
  typedef typename T::Poller rpc_poller_t;

  /// Constructs an Admin capable of receiving and issuing control operations
  /// (ex: Migration, Checkpointing) to FASTER.
  ///
  /// \param workers
  ///    The list of all worker threads that are actively serving operations
  ///    against FASTER on the server.
  /// \param faster
  ///    Pointer to an instance of FASTER that is capable of servicing reads,
  ///    upserts, and read-modify-writes. This class will only issue control
  ///    operations against this class.
  /// \param lPort
  ///    The network port on which this admin will listen for incoming
  ///    connections.
  /// \param lIP
  ///    The IP address this admin will listen on for connections.
  Admin(worker_t** workers, faster_t* faster, uint16_t lPort, std::string& lIP)
   : workers(workers)
   , faster(faster)
   , incomingConns()
   , transport()
   , connPoller()
   , rpcPoller()
   , myIP(lIP)
   , sofaster(1, 0, 4096)
   , migration(nullptr)
  {
    connPoller = std::move(conn_poller_t(lPort, lIP, &transport));
    rpcPoller = std::move(rpc_poller_t(&transport));
  }

  /// Destructor for Admin.
  ~Admin() {
    if (migration) delete migration;
  }

  /// Disallow copy and copy-assign constructors.
  Admin(const Admin& from) = delete;
  Admin& operator=(const Admin& from) = delete;

  /// Checks for and completes any pending work.
  void poll() {
    // First, poll for an incoming connection. If one was received, then
    // enqueue on the list of incoming connections.
    connection_t* conn = connPoller.poll();
    if (conn) incomingConns.emplace_back(conn);

    // If there aren't any incoming connections, then just return.
    if (incomingConns.empty()) return;

    // Next, try to receive incoming RPCs. The RPC poller will take care of
    // enqueuing any received RPC on the appropriate connection.
    rpcPoller.poll();

    // For each incoming connection, try to make progress on any incoming
    // control RPCs. If the connection has been disconnected, then release
    // it to the transport layer.
    for (auto conn = incomingConns.begin(); conn != incomingConns.end(); ) {
      try {
        (*conn)->receive(reinterpret_cast<void*>(this), adminDispatch);
        conn++;
      } catch (std::runtime_error&) {
        (*conn)->tryMarkDead();
        conn = incomingConns.erase(conn);
      }
    }
  }

 private:
  /// This method is the top-level handler for all control RPCs received by
  /// the Admin class. It dispatches these RPCs to relevant methods based on
  /// the opcode on each RPC.
  ///
  /// \param context
  ///    A pointer to the Admin object typecast into a void*.
  /// \param request
  ///    The request buffer containing the control operation to be processed.
  /// \param response
  ///    The response buffer that any responses should be written to. This
  ///    buffer will be sent out the network once this method returns.
  static uint32_t adminDispatch(void* context, uint8_t* request,
                                uint8_t* response)
  {
    // Retrieve a pointer to the admin from the passed in context.
    auto admin = reinterpret_cast<Admin*>(context);

    auto respHdr = reinterpret_cast<ResponseBatchHdr*>(response);
    respHdr->status = Status::SUCCESS;
    respHdr->size = 0;

    request += sizeof(RequestBatchHdr);
    response += sizeof(ResponseBatchHdr);

    // The total size of the response to this request. Required so that the
    // transport layer knows how much data to send out the network.
    uint32_t respSize = sizeof(ResponseBatchHdr);

    // We assume that control requests are never batched. Each request consists
    // of one and only one control operation to be performed at the server.
    switch (*request) {
    // A request to migrate a hash range out of this server.
    case static_cast<int>(Opcode::MIGRATE): {
      auto req = reinterpret_cast<MigrateRequest*>(request);
      auto res = reinterpret_cast<MigrateResponse*>(response);
      res->op = Opcode::MIGRATE;
      handleMigrate(admin, req, res);

      respSize += sizeof(MigrateResponse);
      respHdr->size = sizeof(MigrateResponse);
      break;
    }

    // A request to prepare the server to receive ownership of a hash range.
    case static_cast<int>(Opcode::PREP_FOR_TRANSFER): {
      auto req = reinterpret_cast<PrepTransferRequest*>(request);
      auto res = reinterpret_cast<PrepTransferResponse*>(response);
      res->op = Opcode::PREP_FOR_TRANSFER;
      handlePrepTransfer(admin, req, res);

      respSize += sizeof(PrepTransferResponse);
      respHdr->size = sizeof(PrepTransferResponse);
      break;
    }

    // A request to inform the server that it can take ownership of a hash
    // range that it was previously prepared to receive.
    case static_cast<int>(Opcode::TAKE_OWNERSHIP): {
      auto req = reinterpret_cast<TakeOwnershipRequest*>(request);
      auto res = reinterpret_cast<TakeOwnershipResponse*>(response);
      res->op = Opcode::TAKE_OWNERSHIP;
      handleTakeOwnership(admin, req, res);

      respSize += sizeof(TakeOwnershipResponse);
      respHdr->size = sizeof(TakeOwnershipResponse);
      break;
    }

    // A request to inform the server that migration has completed and that
    // it can now return to normal operation.
    case static_cast<int>(Opcode::COMPLETE_MIGRATION): {
      auto req = reinterpret_cast<CompleteMigrationRequest*>(request);
      auto res = reinterpret_cast<CompleteMigrationResponse*>(response);
      res->op = Opcode::COMPLETE_MIGRATION;
      handleCompleteMigration(admin, req, res);

      respSize += sizeof(CompleteMigrationResponse);
      respHdr->size = sizeof(CompleteMigrationResponse);
      break;
    }

    // A request asking the server to compact it's hybrid log upto a
    // particular log offset.
    case static_cast<int>(Opcode::COMPACT): {
      auto req = reinterpret_cast<CompactRequest*>(request);
      auto res = reinterpret_cast<CompactResponse*>(response);
      res->op = Opcode::COMPACT;
      handleCompact(admin, req, res);

      respSize += sizeof(CompactResponse);
      respHdr->size = sizeof(CompactResponse);
      break;
    }

    // Unidentified opcode. Do nothing for now. Ideally, would want to throw
    // an exception over the network to the client (like RAMCloud).
    default:
      break;
    }

    return respSize;
  }

  /// Handles a Migrate() request. Calls down to FASTER, initiating a migration
  /// of a hash range to a target server.
  ///
  /// \param admin
  ///    Pointer to the Admin object that received the request. Required so that
  ///    we can call down to faster and pass in appropriate callbacks.
  /// \param req
  ///    Request header on the Migrate() RPC. Contains the hash range to be
  ///    migrated to the target.
  /// \param res
  ///    Response header for the Migrate() RPC. Will contain the status of the
  ///    request i.e. whether the migration was started successfully or not.
  static void handleMigrate(Admin* admin, MigrateRequest* req,
                            MigrateResponse* res)
  {
    // Retrieve the migrating hash range from the request header.
    auto lHash = req->firstHash;
    auto rHash = req->finalHash;
    auto dPort = req->destPort;

    struct in_addr dIP;
    dIP.s_addr = req->destIP;
    auto dest = std::string(inet_ntoa(dIP));

    // Add the migration to admin too so that we can get compaction to work.
    if (admin->migration) {
      logMessage(Lvl::ERROR,
        "Overwriting previous migration at admin. Compaction might not work");
      delete admin->migration;
    }
    admin->migration =
      new std::tuple<KeyHash, KeyHash, std::string, uint16_t>(lHash, rHash,
                                                              dest, dPort);
    admin->sofaster.addHashRange(lHash.control(), rHash.control(),
                                 dest, std::to_string(dPort));

    /// Create a tuple with everything required to execute the control path
    /// from within FASTER.
    auto cCtx = reinterpret_cast<CtrlPathCtxt<worker_t>*>(
                    malloc(sizeof(CtrlPathCtxt<worker_t>)));
    memset((void*) cCtx, 0, sizeof(CtrlPathCtxt<worker_t>));
    cCtx->workers = admin->workers;
    auto control = std::make_tuple(reinterpret_cast<void*>(cCtx),
                                   srcCtrlPath<worker_t>);

    // Create a tuple with all state required by a worker thread to initiate
    // ownership transfer of the hash range.
    auto oCtx = reinterpret_cast<OwnershipTfr<worker_t>*>(
                    malloc(sizeof(OwnershipTfr<worker_t>)));
    memset((void*) oCtx, 0, sizeof(OwnershipTfr<worker_t>));
    oCtx->workers = admin->workers;
    oCtx->newView = 1;
    auto owner = std::make_tuple(reinterpret_cast<void*>(oCtx),
                                 txOwnership<worker_t>);

    // Create a tuple containing all state and callbacks needed to physically
    // move the hash range to the target.
    auto ctxt = reinterpret_cast<DataMovtCtxt<worker_t>*>(
                    malloc(sizeof(DataMovtCtxt<worker_t>)));
    memset((void*) ctxt, 0, sizeof(DataMovtCtxt<worker_t>));
    ctxt->workers = admin->workers;
    auto dataMovt = std::make_tuple(reinterpret_cast<void*>(ctxt),
                                    dataMovtStart<worker_t>,
                                    dataMovtCease<worker_t>);

    // Create a tuple containing all state and callbacks needed to physically
    // move records on disk to the target. Then, call down to FASTER starting
    // the migration. Set the status on the response header based on whether
    // the migration was started successfully or not.
    auto iCtx = reinterpret_cast<CompactionCtxt<sofaster_t>*>(
                    malloc(sizeof(CompactionCtxt<sofaster_t>)));
    memset((void*) iCtx, 0, sizeof(CompactionCtxt<sofaster_t>));
    iCtx->sofaster = &(admin->sofaster);
    iCtx->myIPAddr = admin->myIP;
    iCtx->migration = admin->migration;
    iCtx->skipInsert = true;
    auto iopath = std::make_tuple(reinterpret_cast<void*>(iCtx),
                                  validate_ownership<sofaster_t, record_t>);

    if (!admin->faster->Migrate(lHash, rHash, true, owner, dataMovt,
                                control, iopath, dest, dPort))
    {
      res->status = Status::INTERNAL_ERROR;
      return;
    }

    res->status = Status::SUCCESS;
    return;
  }

  /// Handles the PrepForTransfer() request. Calls down to FASTER, preparing
  /// it to receive ownership of a hash range.
  ///
  /// \param admin
  ///    Pointer to the Admin object that received the request. Required so that
  ///    we can call down to faster and pass in appropriate callbacks.
  /// \param req
  ///    Request header on the PrepForTransfer() RPC. Contains the hash range
  ///    that will be received by target.
  /// \param res
  ///    Response header for the PrepForTransfer() RPC. Will contain the status
  ///    of the request i.e. whether the target has started preparing itself.
  static void handlePrepTransfer(Admin* admin, PrepTransferRequest* req,
                                 PrepTransferResponse* res)
  {
    auto lHash = req->firstHash;
    auto rHash = req->finalHash;

    // Setup a tuple for the target's control path.
    auto cTup = cntrlpath_cb_t();

    // The callback associated with ownership transfer is empty for now.
    auto oTup = ownership_cb_t();

    // The callback associated with the io path is empty for now.
    auto iTup = iopath_cb_t();

    // The callback associated with data transfer.
    auto dCtx = malloc(sizeof(uint64_t));
    auto dCCb = [](void* ctxt) {};
    auto dTup = std::make_tuple(dCtx, targetData, dCCb);

    // Setup the migration on the target.
    if (!admin->faster->Migrate(lHash, rHash, false, oTup, dTup, cTup, iTup,
                                "", 0))
    {
      res->status = Status::INTERNAL_ERROR;
      return;
    }

    logMessage(Lvl::DEBUG,
               "Serviced PrepForTransfer() on hash range [%lu, %lu]",
               lHash.control(), rHash.control());
    res->status = Status::SUCCESS;
    return;
  }

  /// Handles the TakeOwnership() request. Calls down to FASTER, handing
  /// off ownership of a hash range that this server was previously prepared
  /// to accept through the PrepForTransfer() request.
  ///
  /// \param admin
  ///    Pointer to the Admin object that received the request. Required so that
  ///    we can call down to faster.
  /// \param req
  ///    Request header on the TakeOwnership() RPC. Contains the size of the
  ///    sampled set of records in bytes.
  /// \param res
  ///    Response header for the TakeOwnership() RPC. Will contain the status
  ///    of the request i.e. whether the target has taken ownership.
  static void handleTakeOwnership(Admin* admin, TakeOwnershipRequest* req,
                                  TakeOwnershipResponse* res)
  {
    auto lHash = req->firstHash;
    auto rHash = req->finalHash;

    auto buff = reinterpret_cast<uint8_t*>(req) + sizeof(TakeOwnershipRequest);
    auto size = req->sampledBytes;

    // Setup a tuple for the target's control path.
    auto cTup = cntrlpath_cb_t();

    // The callback associated with ownership transfer is empty for now.
    auto oTup = ownership_cb_t();

    // The callback associated with the io path is empty for now.
    auto iTup = iopath_cb_t();

    // The callback associated with data transfer.
    auto dCtx = malloc(sizeof(uint64_t));
    auto dCCb = [](void* ctxt) {};
    auto dTup = std::make_tuple(dCtx, targetData, dCCb);

    // Call down to FASTER, handing off the buffer of sampled records.
    if (!admin->faster->ReceiveSampled(buff, size, lHash, rHash, oTup,
                                       dTup, cTup, iTup))
    {
      res->status = Status::INTERNAL_ERROR;
      return;
    }

    logMessage(Lvl::DEBUG,
      "Serviced TakeOwnership() on range [%lu, %lu] with %.2f KB of samples",
      lHash.control(), rHash.control(), ((double) size) / 1024);
    res->status = Status::SUCCESS;
    return;
  }

  /// Handles the CompleteMigration() request. Calls down to FASTER, completing
  /// the migration, returning the system to the REST phase.
  ///
  /// \param admin
  ///    Pointer to the Admin object that received the request. Required so that
  ///    we can call down to faster.
  /// \param req
  ///    Request header on the CompleteMigration() RPC. Contains the hash range
  ///    of the migration being completed.
  /// \param res
  ///    Response header for the CompleteMigration() RPC. Will contain the status
  ///    of the request i.e. whether the target has completed migration.
  static void handleCompleteMigration(Admin* admin,
                                      CompleteMigrationRequest* req,
                                      CompleteMigrationResponse* res)
  {
    auto lHash = req->firstHash;
    auto rHash = req->finalHash;

    // Call down to FASTER, completing the migration.
    if (!admin->faster->CompleteMigration(lHash, rHash))
    {
      res->status = Status::INTERNAL_ERROR;
      return;
    }

    logMessage(Lvl::DEBUG,
      "Serviced CompleteMigration() on range [%lu, %lu]", lHash, rHash);
    res->status = Status::SUCCESS;
    return;
  }

  /// Handles the Compact() RPC. Creates an instance of a client library, calls
  /// down to FASTER invoking the compaction algorithm, returns a response.
  ///
  /// \param admin
  ///    Pointer to the Admin object that received the request. Required so that
  ///    we can call down to faster and retrieve the server's IP address.
  /// \param req
  ///    Header on the Compact() request. Contains the address upto which we
  ///    must compact this server's log.
  /// \param res
  ///    Response to the Compact() request. Indicates whether it succeeded.
  static void handleCompact(Admin* admin, CompactRequest* req,
                            CompactResponse* res)
  {
    // Setup a client library for records whose ownership changed but were
    // physically retained on this server. These records will be moved to
    // the correct server during compaction.
    uint64_t until = req->until;

    CompactionCtxt<sofaster_t> ctxt(&(admin->sofaster), admin->myIP,
                                    admin->migration);
    auto ownership = std::make_tuple(reinterpret_cast<void*>(&ctxt),
                                     validate_ownership<sofaster_t, record_t>);

    // Call down to FASTER asking it to compact it's hybrid log.
    logMessage(Lvl::DEBUG, "Received Compact() with untilAddress = %lu", until);
    if (!admin->faster->Compact(until, ownership)) {
      res->status = Status::INTERNAL_ERROR;
      return;
    }

    // Make sure all transmissions over the client library complete, marking
    // the end of compaction.
    admin->sofaster.clearSessions();
    logMessage(Lvl::DEBUG, "Serviced Compact() with untilAddress = %lu", until);
    res->status = Status::SUCCESS;
    return;
  }

  /// A list of threads that have opened sessions to FASTER and are actively
  /// servicing operations (reads, upserts, and rmws). Required by certain
  /// operations such as migration/scale-out etc.
  worker_t** workers;

  /// A pointer to an instance of FASTER. This instance is also shared by the
  /// above set of worker threads.
  faster_t* faster;

  /// A vector of incoming connections that control msgs will be received on.
  std::vector<connection_t*> incomingConns;

  /// The transport layer that will be used to send and receive control msgs.
  transport_t transport;

  /// Connection poller. All incoming client connections will be received here.
  conn_poller_t connPoller;

  /// RPC poller. All incoming RPC requests will be received and enqueued on
  /// the appropriate connection here.
  rpc_poller_t rpcPoller;

  /// IP address this admin instance is listening on.
  std::string myIP;

  /// An instance of the client library. Required for compaction; we might
  /// encounter migrated records that need to be sent to their owner.
  sofaster_t sofaster;

  /// XXX: This lets us keep track of a migration so that we can correctly
  /// transmit records during compaction for the purpose of benchmarking.
  std::tuple<KeyHash, KeyHash, std::string, uint16_t>* migration;
};

} // end of namespace server.
} // end of namespace SoFASTER.
