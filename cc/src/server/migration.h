// Copyright (c) University of Utah. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "core/faster.h"
#include "core/key_hash.h"
#include "core/migration.h"

#include "common/log.h"
#include "session/client.h"
#include "client/sofaster.h"

namespace SoFASTER {
namespace server {

using namespace wireformat;
using namespace FASTER::core;

template<class F, class K, class V, class T>
class Migration {
 public:
  // For convenience. Defines the type on the session that data will be migrated
  // to the target over.
  typedef session::ClientSession<T> session_t;

  // For convenience. Defines the type on the faster instance that data will be
  // migrated out from.
  typedef F faster_t;

  // For convenience. Defines the type on the connection handler exposed by the
  // transport layer.
  typedef typename T::ConnectHandler connect_t;

  // For convenience. Defines the type on the connection exposed by the
  // transport layer.
  typedef typename T::Connection connection_t;

  // For convenience. Defines the type on the poller exposed by the transport.
  typedef typename T::Poller poller_t;

  /// Constructs a Migration. Refer to the constructor for ClientSession under
  /// src/session/client.h for parameter/argument documentation.
  ///
  /// \param faster
  ///    Pointer to the underlying faster instance. The calling thread is
  ///    assumed to have already opened a session to this instance.
  /// \param samples
  ///    The number of samples (in bytes) to move to the target during migration
  Migration(connect_t& handler, poller_t* rpcPoller,
            const std::string& targetIp,
            const std::string& targetPort,
            faster_t* faster, KeyHash lHash,
            KeyHash rHash, uint32_t samples)
   : startDataTx(false)
   , target(handler, targetIp, targetPort, rpcPoller, 1)
   , faster(faster)
   , state()
   , connHandler(&handler)
   , targetIp(targetIp)
   , control(nullptr)
   , recvCtrlAction(ControlAction::NONE)
   , lHash(lHash)
   , rHash(rHash)
   , sampleLimit(samples)
  {
    // Open up a session to the target server.
    target.open(Sofaster<K, V, T>::sendOpen, Sofaster<K, V, T>::recvOpen);
  }

  /// Destroys a Migration.
  ~Migration() {
    // TODO: Close the session to the target here.

    if (control) control->tryMarkDead();
  }

  /// Delete copy and copy-assign constructors.
  Migration(const Migration& from) = delete;
  Migration& operator=(const Migration& from) = delete;

  /// Tries to make progress in moving/migrating data to the target.
  void poll() {
    // First, poll for responses on the control path if needed.
    pollCtrl();

    // Check if we can make progress on the datapath. If not, return.
    if (!startDataTx) return;

    // If there aren't any in-progress RPCs to the target, then issue one.
    // Otherwise, check if a response to the previous RPC has been received.
    if (target.inProgress < target.maxInFlight) {
      SendContext context(faster, &state, &target, lHash, rHash);
      target.send(sendFn, &context);
    } else {
      target.tryReceive(recvFn, reinterpret_cast<void*>(this));
    }
  }

  /// Enables the datapath, allowing state to be shipped to the target.
  void enableDataPath() {
    startDataTx = true;
  }

  /// Returns true if there are no in-progress migration RPCs. Else, tries
  /// to complete them.
  bool completeRecv() {
    if (!target.inProgress && state.migrationIOs == 0 &&
        recvCtrlAction == ControlAction::NONE) return true;

    if (recvCtrlAction != ControlAction::NONE) pollCtrl();

    if (target.inProgress) {
      target.tryReceive(recvFn, reinterpret_cast<void*>(this));
    }

    return false;
  }

  /// Sets the hash range on this migration object. Required because we might
  /// want to pre-initialize migration for certain experiments and might not
  /// know the actual hash range when doing so. Once we do know the hash range,
  /// we can just invoke this method.
  void setHashRange(KeyHash l, KeyHash r) {
    lHash = l;
    rHash = r;
  }

  /// Makes forward progress on the control path. Sends out the
  /// PrepForTransfer RPC.
  void prepTarget() {
    // Open a connection for the control path if required. It is not
    // guaranteed that the same thread is going to issue all control
    // RPCs to the target.
    if (!control) {
      control = connHandler->connect(targetIp, std::to_string(50000));
    }

    /// Context with all state required to send out a PrepForTransfer() RPC
    /// to the target server.
    struct PrepCtxt {
      // Lower bound on the hash range whose ownership will be transferred.
      KeyHash lHash;

      // Upper bound on the hash range whose ownership will be transferred.
      KeyHash rHash;

      PrepCtxt(KeyHash firstHash, KeyHash finalHash)
       : lHash(firstHash)
       , rHash(finalHash)
      {}
    };
    auto context = PrepCtxt(lHash, rHash);

    /// Callback that constructs the PrepForTransfer() RPC on a transmit buffer.
    auto sendPrep = [](void* ctxt, uint8_t* buffer) {
      auto range = reinterpret_cast<PrepCtxt*>(ctxt);

      // Add in a generic RPC header so that this works with TCP.
      auto hdr = reinterpret_cast<RequestBatchHdr*>(buffer);
      hdr->size = sizeof(PrepTransferRequest);
      buffer += sizeof(RequestBatchHdr);

      auto req = reinterpret_cast<PrepTransferRequest*>(buffer);
      req->op = Opcode::PREP_FOR_TRANSFER;
      req->firstHash = range->lHash;
      req->finalHash = range->rHash;

      return (uint32_t) (sizeof(PrepTransferRequest) + sizeof(RequestBatchHdr));
    };
    control->sendRpc(reinterpret_cast<void*>(&context), sendPrep);

    // We need to poll for a response to this RPC on this object's poll loop.
    recvCtrlAction = ControlAction::PREPARE;
  }

  /// Makes forward progress on the control path. Sends out the
  /// TransferOwnership() RPC.
  void transfer() {
    // Open a connection for the control path if required. It is not
    // guaranteed that the same thread is going to issue all control
    // RPCs to the target. For example, a different thread might have
    // issued the PrepForTransfer() RPC to the target.
    if (!control) {
      control = connHandler->connect(targetIp, std::to_string(50000));
    }

    /// Context with all state required to send out a TakeOwnership() request.
    struct TransferCtxt {
      /// Lower bound on the hash range whose ownership will be transferred.
      KeyHash lHash;

      /// Upper bound on the hash range whose ownership will be transferred.
      KeyHash rHash;

      /// Pointer to FASTER from which sampled set can be retrieved.
      faster_t* faster;

      /// The maximum size of the sampled set in bytes that can be moved over
      /// to the target during migration.
      uint32_t sampleLimit;

      TransferCtxt(KeyHash firstHash, KeyHash finalHash,
                   faster_t* store, uint32_t samples)
       : lHash(firstHash)
       , rHash(finalHash)
       , faster(store)
       , sampleLimit(samples)
      {}
    };
    auto context = TransferCtxt(lHash, rHash, faster, sampleLimit);

    /// Callback that sets up the TakeOwnership() request on an RPC buffer.
    auto sendTransfer = [](void* ctxt, uint8_t* buffer) {
      auto context = reinterpret_cast<TransferCtxt*>(ctxt);

      // Add in a generic RPC header so that this works with TCP.
      auto hdr = reinterpret_cast<RequestBatchHdr*>(buffer);
      hdr->size = sizeof(TakeOwnershipRequest);
      buffer += sizeof(RequestBatchHdr);

      auto req = reinterpret_cast<TakeOwnershipRequest*>(buffer);
      req->op = Opcode::TAKE_OWNERSHIP;
      req->firstHash = context->lHash;
      req->finalHash = context->rHash;

      buffer += sizeof(TakeOwnershipRequest);
      auto size = std::min(context->sampleLimit, T::bufferSize() -
                                       (uint32_t) sizeof(TakeOwnershipRequest) -
                                       (uint32_t) sizeof(RequestBatchHdr));
      req->sampledBytes = context->faster->CollectSamples(buffer, size);
      hdr->size += req->sampledBytes;

      return (uint32_t) (sizeof(TakeOwnershipRequest) + req->sampledBytes +
                         sizeof(RequestBatchHdr));
    };
    control->sendRpc(reinterpret_cast<void*>(&context), sendTransfer);

    // We need to poll for a response to this RPC on this object's poll loop.
    // In some cases, this thread might also have issued a PrepForTransfer()
    // RPC, but might not have received a response to it yet.
    if (recvCtrlAction == ControlAction::NONE) {
      recvCtrlAction = ControlAction::TRANSFER;
    } else {
      recvCtrlAction = ControlAction::PREPARE_AND_TRANSFER;
    }
  }

  /// Sends out a CompleteMigration() RPC to the target that finishes migration
  /// and returns the system to normal operation.
  void complete() {
    // Open a connection for the control path if required. It is not
    // guaranteed that the same thread is going to issue all control
    // RPCs to the target. For example, different threads might have
    // issued the PrepForTransfer() and TransferOwnership() RPCs to
    // the target.
    if (!control) {
      control = connHandler->connect(targetIp, std::to_string(50000));
    }

    /// Context with all state required to send out a CompleteMigration()
    /// RPC request.
    struct CompleteCtxt {
      /// Lower bound on the hash range whose migration will be completed.
      KeyHash lHash;

      /// Upper bound on the hash range whose migration will be completed.
      KeyHash rHash;

      CompleteCtxt(KeyHash firstHash, KeyHash finalHash)
       : lHash(firstHash)
       , rHash(finalHash)
      {}
    };
    auto context = CompleteCtxt(lHash, rHash);

    /// Callback that sets up the CompleteMigration() request on an RPC buffer.
    auto sendComplete = [](void* ctxt, uint8_t* buffer) {
      auto context = reinterpret_cast<CompleteCtxt*>(ctxt);

      // Add in a generic RPC header so that this works with TCP.
      auto hdr = reinterpret_cast<RequestBatchHdr*>(buffer);
      hdr->size = sizeof(CompleteMigrationRequest);
      buffer += sizeof(RequestBatchHdr);

      auto req = reinterpret_cast<CompleteMigrationRequest*>(buffer);
      req->op = Opcode::COMPLETE_MIGRATION;
      req->firstHash = context->lHash;
      req->finalHash = context->rHash;

      return (uint32_t) (sizeof(CompleteMigrationRequest) +
                         sizeof(RequestBatchHdr));
    };
    control->sendRpc(reinterpret_cast<void*>(&context), sendComplete);

    if (recvCtrlAction == ControlAction::NONE) {
      recvCtrlAction = ControlAction::COMPLETE;
    } else {
      logMessage(Lvl::ERROR,
        (std::string("Sending complete() RPC when we have other control ") +
        "RPCs in-flight. This corner case isn't handled too well. " +
        "Expect bugs").c_str());
    }
  }

 private:
  /// Checks for responses to any control messages that were sent out from
  /// this thread to the target server.
  void pollCtrl() {
    ControlContext ctxt(lHash, rHash, &recvCtrlAction);

    switch (recvCtrlAction) {
    // We need to poll for a response to the PrepForTransfer() control RPC.
    case ControlAction::PREPARE:
    case ControlAction::PREPARE_AND_TRANSFER:
      control->receive(reinterpret_cast<void*>(&ctxt), recvPrep);
      break;

    // We need to poll for a response to the TakeOwnership() control RPC.
    case ControlAction::TRANSFER:
      control->receive(reinterpret_cast<void*>(&ctxt), recvTfer);
      break;

    // We need to poll for a response to the CompleteMigration() control RPC.
    case ControlAction::COMPLETE:
      control->receive(reinterpret_cast<void*>(&ctxt), recvComp);
      break;

    // No responses to poll for.
    default:
      break;
    }
  }

  /// Processes a response to a PrepForTransfer() RPC.
  ///
  /// \param context
  ///    Opaque context. Cast into a control action by this method. Will be
  ///    updated to the next control action to be taken by this thread.
  /// \param request
  ///    Buffer containing the original request.
  /// \param response
  ///    Buffer containing the target's response to the request.
  ///
  /// \return
  ///    0. No real significance.
  static uint32_t recvPrep(void* context, uint8_t* request, uint8_t* response) {
    auto ctxt = reinterpret_cast<ControlContext*>(context);
    auto action = ctxt->action;

    // Advance past the generic RPC response header. The server handler for
    // this RPC currently does not set any of its fields.
    response += sizeof(ResponseBatchHdr);

    auto res = reinterpret_cast<PrepTransferResponse*>(response);
    if (res->status != Status::SUCCESS) {
      logMessage(Lvl::ERROR,
                 "Target failed to prepare for ownership transfer");
    }

    // If we're waiting for a response to a TakeOwnership() RPC too, then make
    // sure the new control action reflects that. Else, no more work to do.
    if (*action == ControlAction::PREPARE) {
      *action = ControlAction::NONE;
    } else {
      *action = ControlAction::TRANSFER;
    }

    logMessage(Lvl::DEBUG,
               "Received response to PrepForTransfer() on range [%lu, %lu]",
               ctxt->lHash.control(), ctxt->rHash.control());

    return (uint32_t) 0;
  }

  /// Processes a response to a TakeOwnership() RPC.
  ///
  /// \param context
  ///    Opaque context. Cast into a control action by this method. Will be
  ///    updated to the next control action to be taken by this thread.
  /// \param request
  ///    Buffer containing the original request.
  /// \param response
  ///    Buffer containing the target's response to the request.
  ///
  /// \return
  ///    0. No real significance.
  static uint32_t recvTfer(void* context, uint8_t* request, uint8_t* response) {
    auto ctxt = reinterpret_cast<ControlContext*>(context);
    auto action = ctxt->action;

    // Advance past the generic RPC response header. The server handler for
    // this RPC currently does not set any of its fields.
    response += sizeof(ResponseBatchHdr);

    auto res = reinterpret_cast<TakeOwnershipResponse*>(response);
    if (res->status != Status::SUCCESS) {
      logMessage(Lvl::ERROR,
                 "Target failed to take ownership of hash range");
    }

    // No more responses to poll for.
    *action = ControlAction::NONE;

    logMessage(Lvl::DEBUG,
               "Received response to TakeOwnership() on range [%lu, %lu]",
               ctxt->lHash.control(), ctxt->rHash.control());

    return (uint32_t) 0;
  }

  /// Processes a response to a CompleteMigration() RPC.
  ///
  /// \param context
  ///    Opaque context. Cast into a control action by this method. Will be
  ///    updated to the next control action to be taken by this thread.
  /// \param request
  ///    Buffer containing the original request.
  /// \param response
  ///    Buffer containing the target's response to the request.
  ///
  /// \return
  ///    0. No real significance.
  static uint32_t recvComp(void* context, uint8_t* request, uint8_t* response) {
    auto ctxt = reinterpret_cast<ControlContext*>(context);
    auto action = ctxt->action;

    // Advance past the generic RPC response header. The server handler for
    // this RPC currently does not set any of its fields.
    response += sizeof(ResponseBatchHdr);

    auto res = reinterpret_cast<CompleteMigrationResponse*>(response);
    if (res->status != Status::SUCCESS) {
      logMessage(Lvl::ERROR,
                 "Target failed to complete migration of hash range");
    }

    // No more responses to poll for.
    *action = ControlAction::NONE;

    logMessage(Lvl::DEBUG,
               "Received response to CompleteMigration() on range [%lu, %lu]",
               ctxt->lHash.control(), ctxt->rHash.control());

    return (uint32_t) 0;
  }

  /// A context passed in to the sessions layer every time an attempt is made
  /// to send/migrate some data to the target.
  struct SendContext {
    /// A pointer to faster. Required so that we can collect a bunch of hashes
    /// into the transmit buffer and send them to the target.
    faster_t* faster;

    /// A pointer to state that will help faster determine from where in it's
    /// hash table it should start/continue collecting data from.
    MigrationState::MigrationContext* state;

    /// A pointer to the session over which this data will be migrated. Required
    /// to correctly set the header on the RPC.
    session_t* target;

    /// Lower bound on the hash range to be migrated to the target.
    KeyHash lHash;

    /// Upper bound on the hash range to be migrated to the target.
    KeyHash rHash;

    SendContext(faster_t* f, MigrationState::MigrationContext* s, session_t* t,
                KeyHash lHash, KeyHash rHash)
     : faster(f)
     , state(s)
     , target(t)
     , lHash(lHash)
     , rHash(rHash)
    {}
  };

  /// This method is invoked when sending out a migration RPC to the target.
  ///
  /// \param context
  ///    A pointer to a SendContext typecast into a void* that contains all
  ///    state required to migrate a batch of records.
  /// \param rpcBuffer
  ///    The transmit buffer that records to be migrated need to be written to.
  static uint32_t sendFn(void* context, uint8_t* rpcBuffer) {
    auto ctx = reinterpret_cast<SendContext*>(context);
    auto s = ctx->state;

    // First, setup the batch header for the RPC.
    auto hdr = reinterpret_cast<RequestBatchHdr*>(rpcBuffer);
    hdr->view = ctx->target->currentView;
    hdr->size = 0;
    rpcBuffer += sizeof(RequestBatchHdr);

    // Next, setup the header for the PushHashes() RPC. There will be only one
    // such "request" on this batch.
    auto req = reinterpret_cast<PushHashesRequest*>(rpcBuffer);
    req->op = Opcode::MIGRATION_PUSH_HASHES;
    req->firstHash = ctx->lHash;
    req->finalHash = ctx->rHash;
    hdr->size += sizeof(PushHashesRequest);

    // Finally, call down into FASTER, collecting a batch of records to be
    // migrated into the RPC buffer. This buffer will be sent out after this
    // method/function returns.
    uint8_t* buffer = rpcBuffer + sizeof(PushHashesRequest);
    auto bytes = session_t::maxBufferedBytes * 3 - sizeof(RequestBatchHdr) -
                 sizeof(PushHashesRequest);
    req->size = ctx->faster->Collect(ctx->state, buffer, bytes);
    hdr->size += req->size;
    buffer += req->size;

    // If we have any pending ios belonging to the hash range, add them in.
    auto limit = session_t::maxTxBytes - sizeof(RequestBatchHdr);
    auto space = limit - hdr->size;
    if (s->onDisk.size() < space) {
      auto b = s->onDisk.size();
      memcpy(buffer, &(s->onDisk[0]), b);
      hdr->size += b;
      s->onDisk.clear();
    }

    return hdr->size + sizeof(RequestBatchHdr);
  }

  /// This method is invoked when a response to a migration RPC is received
  /// from the target server.
  ///
  /// \param context
  ///    A pointer to the Migration class on the worker thread typecast to void*
  ///    Required to update the number of in-progress RPCs.
  /// \param requests
  ///    A pointer to the buffer containing the original PullHashes() request.
  ///    Unused by this function.
  /// \param responses
  ///    A pointer to the buffer containing the target's response.
  ///
  /// \return
  ///    0. Of no real significance.
  static uint32_t recvFn(void* context, uint8_t* requests, uint8_t* responses) {
    auto migration = reinterpret_cast<Migration*>(context);

    // Check the status on the response batch header. No real reason for it to
    // fail. If it did, then just throw an exception for now.
    auto hdr = reinterpret_cast<ResponseBatchHdr*>(responses);
    if (hdr->status != Status::SUCCESS) {
      logMessage(Lvl::ERROR, "PushHashes() failed at the target (BatchHdr)");
    }

    // Check the status on the response to the PushHashes() request. Again, no
    // real reason for it to fail. If it did, then throw an exception for now.
    responses += sizeof(ResponseBatchHdr);
    auto res = reinterpret_cast<PushHashesResponse*>(responses);
    if (res->status != Status::SUCCESS) {
      logMessage(Lvl::ERROR, "PushHashes() failed at the target (RespHdr)");
    }

    // Since the RPC completed successfully, update the num of in-progress RPCs.
    migration->target.inProgress -= 1;
    return 0;
  }

  /// A boolean flag indicating whether we should start data transfer.
  bool startDataTx;

  /// A session to the target server that will be used to migrate data.
  session_t target;

  /// The underlying faster instance that data will be migrated out from.
  faster_t* faster;

  /// Context required by faster to determine where in the hash table it
  /// should continue/start migrating data from.
  MigrationState::MigrationContext state;

  /// A pointer to a connection handler that can be used to establish a
  /// connection to the target.
  connect_t* connHandler;

  /// Ip address of the target machine.
  std::string targetIp;

  /// A connection to the target that all control messages will be sent out on.
  connection_t* control;

  /// This class represents the next action that should performed
  /// out on the control path of the migration.
  enum class ControlAction : uint8_t {
    /// There are currently no control path actions to be taken.
    NONE,

    /// The action associated with PrepForTransfer() should be taken on the
    /// control path. This means that we should poll for a response to an RPC
    /// that was previously sent out.
    PREPARE,

    /// The action associated with TransferOwnership() should be taken on the
    /// control path. This means that we should poll for a response to an RPC
    /// that was previously sent out.
    TRANSFER,

    /// The action associated with both PrepForTransfer() and TransferOwnership()
    /// should be taken on the control path. This means that we should poll for
    /// a response to both of these RPCs.
    PREPARE_AND_TRANSFER,

    /// The action associated with CompleteMigration() should be take on the
    /// control path. This means that we should poll for a response to an RPC
    /// that was previously sent out.
    COMPLETE,
  };

  /// A context with all state required to successfully process a response to
  /// a control message (PrepForTransfer(), TakeOwnership()).
  struct ControlContext {
    // Lower bound on the hash range for which the RPC was sent out.
    KeyHash lHash;

    // Upper bound on the hash range for which the RPC was sent out.
    KeyHash rHash;

    // Pointer to the thread's control action.
    ControlAction* action;

    ControlContext(KeyHash firstHash, KeyHash finalHash, ControlAction* ctrl)
     : lHash(firstHash)
     , rHash(finalHash)
     , action(ctrl)
    {}
  };

  /// The control RPC whose response we have to poll for.
  ControlAction recvCtrlAction;

  /// Lower bound on the hash range to be migrated to the target.
  KeyHash lHash;

  /// Upper bound on the hash range to be migrated to the target.
  KeyHash rHash;

  /// The maximum size of the sampled set in bytes that can be moved over to
  /// the target during migration.
  uint32_t sampleLimit;
};

} // end of namespace server
} // end of namespace SoFASTER
