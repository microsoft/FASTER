// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <vector>

#include "status.h"

#include "core/guid.h"
#include "core/async.h"

namespace session {

using namespace FASTER::core;

/// Server side session.
///
/// Template arguments on ServerSession:
///    T: The transport layer that RPCs will be received on and responses
///       will be sent out on.
///    F: The type on the underlying hash-partitioned stateful service.
template<class T, class F>
class ServerSession {
 public:
  // For convenience. Defines the service that executes the requests
  // (ex: FASTER).
  typedef F service_t;

  // For convenience. Defines the type on the transport layer used.
  typedef T transport_t;

  // For convenience. Defines the type on the connection exposed by the
  // transport layer.
  typedef typename T::Connection connection_t;

  // For convenience. Defines the function that gets called when requests
  // are received on this session.
  typedef uint32_t (*R)(void*, uint8_t*, uint8_t*);

  // The maximum number of bytes that can be written to a transmit buffer.
  static constexpr uint32_t maxTxBytes = T::bufferSize();

  /// Constructs a server-side session.
  ///
  /// \param view
  ///    The cluster view that this session will operate under.
  /// \param server
  ///    Pointer to the service that will actually handle incoming requests.
  /// \param conn
  ///    Pointer to connection over which requests will be received and
  ///    responses will be sent out.
  ServerSession(uint64_t view, service_t* server,
                connection_t* conn)
   : service(server)
   , currentView(view)
   , lastServicedLSN(0)
   , latestCPROffset(0)
   , safeCPROffset(0)
   , sessionUID()
   , status(Status::INITIALIZING)
   , nextPendingId(0)
   , pendingResponses()
   , nPendings(0)
   , connection(conn)
  {}

  /// Destroys a ServerSession.
  ~ServerSession() {}

  /// Move constructor.
  ServerSession(ServerSession&& from) {
    service = from.service;
    from.service = nullptr;

    currentView = std::move(from.currentView);
    lastServicedLSN = std::move(from.lastServicedLSN);
    latestCPROffset = std::move(from.latestCPROffset);
    safeCPROffset = std::move(from.safeCPROffset);

    sessionUID = from.sessionUID;
    from.sessionUID = FASTER::core::Guid();

    status = std::move(from.status);
    nextPendingId = std::move(from.nextPendingId);
    pendingResponses = std::move(from.pendingResponses);
    nPendings = std::move(from.nPendings);

    connection = from.connection;
    from.connection = nullptr;
  }

  /// Move assign operator.
  ServerSession& operator=(ServerSession&& from) {
    this->service = from.service;
    from.service = nullptr;

    this->currentView = std::move(from.currentView);
    this->lastServicedLSN = std::move(from.lastServicedLSN);
    this->latestCPROffset = std::move(from.latestCPROffset);
    this->safeCPROffset = std::move(from.safeCPROffset);

    this->sessionUID = from.sessionUID;
    from.sessionUID = FASTER::core::Guid();

    this->status = std::move(from.status);
    this->nextPendingId = std::move(from.nextPendingId);
    this->pendingResponses = std::move(from.pendingResponses);
    this->nPendings = std::move(from.nPendings);

    this->connection = from.connection;
    from.connection = nullptr;

    return *this;
  }

  /// Disallow copy and copy-assign constructors.
  ServerSession(const ServerSession& from) = delete;
  ServerSession& operator=(const ServerSession& from) = delete;

  /// Checks for and makes progress on any incoming requests on this session.
  ///
  /// \param recv
  ///    A function to be invoked over the received requests if any.
  /// \param context
  ///    An opaque argument to be passed into the above function.
  inline void poll(R recv, void* context) {
    // Should not be polling on a disconnected session. Log error and throw.
    if (status != Status::INITIALIZING && status != Status::OPEN) {
      logMessage(Lvl::ERR, "Trying to poll on disconnected session %s",
                 sessionUID.ToString().c_str());
      throw std::runtime_error("Trying to poll on a disconnected session");
    }

    // Try to receive data over the connection. If we receive an exception,
    // then mark the session as DISCONNECTED preventing future calls to this
    // method from going through, and release the connection to the transport.
    try {
      connection->receive(context, recv);
    } catch (std::runtime_error&) {
      // Don't need to check for Status::CLOSED. This code path is not reached
      // if the session was cleanly closed.
      status = Status::DISCONNECTED;
      connection->tryMarkDead();
      connection = nullptr;
      throw;
    }
  }

  /// The underlying hash-partitioned stateful service (FASTER) that will
  /// process requests.
  service_t* service;

  /// Initializes the session.
  inline void init() {
    sessionUID = FASTER::core::Guid::Create();
    status = Status::OPEN;
    logMessage(Lvl::DEBUG, "Opened session %s", sessionUID.ToString().c_str());
  }

  /// Closes the session and releases the underlying connection to the
  /// transport layer.
  inline void close() {
    status = Status::CLOSED;
    connection->tryMarkDead();
    connection = nullptr;
    logMessage(Lvl::DEBUG, "Closed session %s", sessionUID.ToString().c_str());
  }

  /// The server local view number that this session is currently operating in.
  /// All incoming requests are validated against this number, ensuring that
  /// the server and client session ends are in-sync wrt the hash ranges owned
  /// by the server/store.
  uint64_t currentView;

  /// The LSN of the last operation that was serviced on this session. Required
  /// for when the view changes on the server. The client will be informed of
  /// this number so that it can reissue requests to the correct server.
  uint64_t lastServicedLSN;

  /// The most recent CPR offset that the store has informed us of. It is not
  /// safe for the client end to recover from this offset. This is because the
  /// store might not have taken a checkpoint of everything prior to it yet.
  uint64_t latestCPROffset;

  /// The CPR offset that the client end can safely recover from. All operations
  /// prior to this offset are durable.
  uint64_t safeCPROffset;

  /// Unique identifier for this session.
  FASTER::core::Guid sessionUID;

  /// The current status of the session ex: open, closed etc.
  Status status;

  /// Identifier of the next operation that goes pending on the session.
  /// Required so that when this operation completes, the client knows
  /// which callback to invoke and which context to pass into the callback.
  uint64_t nextPendingId;

  /// A buffer of responses to operations that went pending and have now
  /// completed execution at the server.
  std::vector<uint8_t> pendingResponses;

  /// The number of pending requests on this session. Required for session
  /// tear down. We can garbage collect a session only when this is zero.
  uint64_t nPendings;

 private:
  /// The underlying connection exposed by the transport that will be used to
  /// receive requests and send out responses.
  connection_t* connection;
};

} // end namespace session
