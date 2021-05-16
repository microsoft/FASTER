// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <vector>

#include "dispatch.h"
#include "session/server.h"

#include "core/guid.h"

namespace SoFASTER {
namespace server {

/// Worker takes in the following template arguments:
///    F: The type on the FASTER instance used by Worker to serve reads,
///       upserts, and read-modify-writes.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of rmws and upserts. Required to correctly
///       unpack RPCs received from a client.
///    T: The type on the transport layer that will be used to receive RPCs
///       and send out responses.
template<class F, class K, class V, class R, class T>
class Worker {
 public:
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

  // For convenience. Defines the type on the sessions layer that requests will
  // be received on.
  typedef session::ServerSession<transport_t, faster_t> session_t;

  /// Constructs a Worker.
  ///
  /// \param faster
  ///    Pointer to an instance of FASTER that is capable of servicing reads,
  ///    upserts, and read-modify-writes.
  /// \param lPort
  ///    The network port on which this worker will listen for incoming
  ///    sessions.
  /// \param lIP
  ///    The IP address the this worker must listen to for connections.
  /// \param view
  ///    The view number this worker should operate in.
  Worker(faster_t* faster, uint16_t lPort, std::string& lIP, uint64_t view=0)
   : faster(faster)
   , guid()
   , incomingSessions()
   , currentView(view)
   , connPoller()
   , rpcPoller()
   , transport()
  {
    connPoller = std::move(conn_poller_t(lPort, lIP, &transport));
    rpcPoller = std::move(rpc_poller_t(&transport));
  }

  /// Destroys a Worker.
  ~Worker() {
    faster->StopSession();
  }

  /// Disallow copy and copy-assign constructors.
  Worker(const Worker& from) = delete;
  Worker& operator=(const Worker& from) = delete;

  /// Starts/Opens a session to FASTER.
  void init() {
    guid = faster->StartSession();
  }

  /// Checks for and completes any pending work.
  void poll() {
    // First, poll for an incoming connection. If a connection was received,
    // then create a new incoming session on the worker.
    connection_t* conn = connPoller.poll();
    if (conn) {
      incomingSessions.emplace_back(currentView, faster, conn);
    }

    // Next, try to receive incoming RPCs. The RPC poller will take care of
    // enqueuing any received RPC on the appropriate session/connection.
    if (incomingSessions.size() > 0) rpcPoller.poll();

    // Refresh the epoch before processing requests on each session.
    faster->Refresh();
    faster->CompletePending();

    // Next, for every incoming session, make progress on all received RPCs.
    auto it = incomingSessions.begin();
    while (it != incomingSessions.end()) {
      // Delete disconnected/closed sessions on which there aren't any pendings.
      if (it->status != session::Status::INITIALIZING &&
          it->status != session::Status::OPEN)
      {
        if (it->nPendings == 0) {
          logMessage(Lvl::DEBUG, "Deleting disconnected session %s",
                     it->sessionUID.ToString().c_str());
          it = incomingSessions.erase(it);
        } else {
          it++;
        }

        continue;
      }

      try {
        DispatchContext<session_t> context(&(*it), currentView);
        it->poll(dispatch<K, V, R, session_t>,
                 reinterpret_cast<void*>(&context));
      } catch (std::runtime_error&) {
        logMessage(Lvl::DEBUG, "Detected disconnected session %s",
                   it->sessionUID.ToString().c_str());
      }

      it++;
    }
  }

 private:
  /// Pointer to an instance of FASTER that will be used to service reads,
  /// upserts and read-modify-writes.
  faster_t* faster;

  /// Unique identifier for the worker. Received on opening a session to
  /// the FASTER key-value store.
  FASTER::core::Guid guid;

  /// Vector of incoming sessions that RPCs (requests) will be received on.
  std::vector<session_t> incomingSessions;

  /// The cluster view number that this worker is currently operating in.
  uint64_t currentView;

 private:
  /// Connection poller. All incoming client connections will be received here.
  conn_poller_t connPoller;

  /// RPC poller. All incoming RPC requests will be received and enqueued on
  /// the appropriate connection here.
  rpc_poller_t rpcPoller;

  /// Transport layer used by this worker.
  transport_t transport;
};

} // end namespace server
} // end namespace SoFASTER
