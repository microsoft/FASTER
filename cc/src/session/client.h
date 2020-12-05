// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <deque>
#include <chrono>
#include <utility>
#include <stdlib.h>
#include <uuid/uuid.h>
#include <unordered_map>

#include "status.h"
#include "buffer.h"

#include "core/guid.h"
#include "core/async.h"
#include "core/light_epoch.h"

namespace session {

using namespace std::chrono;

/// Client side session.
///
/// Template arguments on ClientSession:
///    T: The transport layer that RPCs will be received on and responses
///       will be sent out on.
template<class T>
class ClientSession {
 public:
  // For convenience. Defines the type on the connection exposed by the
  // transport layer.
  typedef typename T::Connection connection_t;

  // For convenience. Defines the type on the connection handler exposed by the
  // transport layer.
  typedef typename T::ConnectHandler connect_t;

  // For convenience. Defines the type on the poller exposed by the transport.
  typedef typename T::Poller poller_t;

  // For convenience. Defines the function that gets called when sending RPCs
  // on this session.
  typedef uint32_t (*S)(void*, uint8_t*);

  // For convenience. Defines the function that gets called when responses are
  // received on this session.
  typedef uint32_t (*R)(void*, uint8_t*, uint8_t*);

  // For convenience. Defines the type on the buffer that requests are enqueued
  // on before they are sent out on the network.
  typedef SessionBuffer session_buffer_t;

  // The maximum number of bytes that can be written to a transmit buffer.
  static constexpr uint32_t maxTxBytes = T::bufferSize();

  /// Constructs a client-side session.
  ///
  /// \param handler
  ///    Reference to a connection handler that can be used to connect to
  ///    a server.
  /// \param ip
  ///    IP address of server to connect to.
  /// \param port
  ///    Port of the server to connect to.
  /// \param rpcPoller
  ///    Pointer to a poller exposed by the transport layer. This will be
  ///    used to receive responses to RPCs issued to a server.
  ClientSession(connect_t& handler, const std::string& ip,
                const std::string& port, poller_t* rpcPoller,
                uint64_t pipelineDepth=2, uint32_t txBytes=4096)
   : contexts()
   , pendingContexts()
   , currentView(0)
   , latestCPROffset(0)
   , sessionUID()
   , status(Status::INITIALIZING)
   , reIssueLSN(0)
   , maxInFlight(pipelineDepth)
   , inProgress(0)
   , nResponses(0)
   , buffer(txBytes, txBytes * 1024)
   , backOffUntil()
   , connection(nullptr)
   , poller(rpcPoller)
   , serverIP(ip)
   , serverPort(port)
  {
    // Open a connection to the server.
    connection = handler.connect(ip, port);
  }

  /// Destroys a client side session.
  ~ClientSession() {}

  /// Disallow copy and copy-assign constructors.
  ClientSession(const ClientSession& from) = delete;
  ClientSession& operator=(const ClientSession& from) = delete;

  /// Move constructor for ClientSession.
  ClientSession(ClientSession&& from)
   : contexts(std::move(from.contexts))
   , pendingContexts(std::move(from.pendingContexts))
   , currentView(from.currentView)
   , latestCPROffset(from.latestCPROffset)
   , sessionUID(std::move(from.sessionUID))
   , status(from.status)
   , reIssueLSN(from.reIssueLSN)
   , maxInFlight(from.maxInFlight)
   , inProgress(from.inProgress)
   , nResponses(from.nResponses)
   , buffer(std::move(from.buffer))
   , backOffUntil(from.backOffUntil)
   , connection(from.connection)
   , poller(from.poller)
   , serverIP(from.serverIP)
   , serverPort(from.serverPort)
  {
    from.connection = nullptr;
    from.poller = nullptr;
  }

  /// Contacts the server and opens the session. This call is synchronous.
  /// Invokes a passed in function 'send' before sending the request, and
  /// invokes a second passed in function 'recv' on receiving a response.
  ///
  /// \throw
  ///    std::runtime_error if we failed to open the session.
  inline void open(S send, R recv) {
    if (status != Status::INITIALIZING) {
      logMessage(Lvl::ERR, "Trying to open a session already opened before");
      throw std::runtime_error("Cannot open session; status != INITIALIZING");
    }

    try {
      // Send out an open session request to the server.
      connection->sendRpc(reinterpret_cast<void*>(this), send);

      // Timeout if the server does not respond in 10 seconds.
      auto start = high_resolution_clock::now();
      auto stop = start + seconds(10);

      // As long as the session status is not OPEN and we haven't timed out,
      // poll for a response.
      while (status != Status::OPEN && high_resolution_clock::now() < stop) {
        poller->poll();
        connection->receive(reinterpret_cast<void*>(this), recv);
      }
    } catch (std::runtime_error& err) {
      logMessage(Lvl::ERR,
                 "Transport exception when opening session to %s port, %s IP",
                 serverPort.c_str(), serverIP.c_str());
      throw;
    }

    // We timed out while waiting for the server to respond.
    if (status != Status::OPEN) {
      throw std::runtime_error("Timed out while trying to open session");
    }

    // The session was opened. We can now open up our persistent buffer.
    buffer.open(sessionUID.ToString());
  }

  /// Initializes the session with a passed in view number and ID.
  inline void init(uint64_t view, Guid id) {
    currentView = view;
    sessionUID = id;
    status = Status::OPEN;
    logMessage(Lvl::DEBUG,
               "Opened session %s to server with port %s, IP %s",
               id.ToString().c_str(), serverPort.c_str(),
               serverIP.c_str());
  }

  /// Sends requests out the session. Invokes a passed in function 'S'
  /// before doing so.
  ///
  /// \param send
  ///    A function to be invoked before performing the send.
  /// \param context
  ///    An opaque context to be passed in to the above function.
  void send(S send, void* context) {
    // If the session has not been opened, log a message and throw exception.
    if (status != Status::OPEN) {
      logMessage(Lvl::ERR,
                 "Tried to send RPC on session %s that isn't open",
                 sessionUID.ToString().c_str());
      throw std::runtime_error("Tried to send on session that isn't open");
    }

    // Try to send a request on this session. If the connection
    // throws an exception, mark the session as DISCONNECTED and
    // release the connection to the transport layer.
    try {
      connection->sendRpc(context, send);
      inProgress++;
    } catch (std::runtime_error&) {
      status = Status::DISCONNECTED;
      connection->tryMarkDead();
      connection = nullptr;
      throw;
    }
  }

  /// Checks for responses to RPCs issued on this session. Invokes a
  /// passed in function 'R' with a passed in context as an argument
  /// if any responses were found.
  void tryReceive(R recv, void* context) {
    // If the session has not been opened, then throw an exception.
    if (status != Status::OPEN) {
      logMessage(Lvl::ERR,
                 "Tried to receive on session %s that isn't open",
                 sessionUID.ToString().c_str());
      throw std::runtime_error("Tried to recv on session that isn't open");
    }

    poller->poll();

    // Try to receive a response on the session. If the connection
    // throws an exception, mark the session as DISCONNECTED and
    // release the connection to the transport layer.
    try {
      connection->receive(context, recv);
    } catch (std::runtime_error&) {
      status = Status::DISCONNECTED;
      connection->tryMarkDead();
      connection = nullptr;
      throw;
    }
  }

  /// Returns true if the session is open and the underlying connection
  /// has not been disconnected.
  bool isAlive() {
    // Call down to the connection asking if it is alive. If not, mark the
    // session as DISCONNECTED and release the connection to the transport.
    bool alive = connection->isAlive();
    if (!alive) {
      status = Status::DISCONNECTED;
      connection->tryMarkDead();
      connection = nullptr;
    }

    return (status == Status::OPEN) && alive;
  }

  /// Queue of opaque contexts visible to functions 'R' and 'S'.
  std::deque<std::pair<FASTER::core::IAsyncContext*,
                       FASTER::core::AsyncCallback>> contexts;

  /// Enqueues the passed in context and callback on the session.
  inline void enqueueContext(FASTER::core::IAsyncContext& ctxt,
                             FASTER::core::AsyncCallback cb)
  {
    // First, heap allocate the passed in context. Then enqueue it along with
    // the callback within this session.
    FASTER::core::IAsyncContext* copy = nullptr;
    if (ctxt.DeepCopy(copy) != FASTER::core::Status::Ok) {
      throw::std::runtime_error("Failed to heap allocate context!");
    }

    contexts.emplace_back(std::make_pair(copy, cb));
  }

  /// Executes and drops the oldest callback enqueued within the session.
  ///
  /// \param res
  ///    The second argument to be passed into the callback.
  inline void execEarliestCb(FASTER::core::Status res) {
    auto ctx = contexts.front();
    ctx.second(ctx.first, res);

    contexts.pop_front();
  }

  /// A map of contexts and callbacks for operations that went pending at the
  /// server. Indexed by an 8 byte unsigned identifier.
  std::unordered_map<uint64_t,
                     std::pair<FASTER::core::IAsyncContext*,
                               FASTER::core::AsyncCallback>> pendingContexts;

  /// Executes and drops a callback associated with an operation that went
  /// pending (identified by 'id').
  inline void execPendingCb(uint64_t id, FASTER::core::Status res) {
    auto ctx = pendingContexts.at(id);
    ctx.second(ctx.first, res);

    pendingContexts.erase(id);
  }

  /// Retrieves the oldest enqueued context on the session and inserts into
  /// the set of pending contexts against a passed-in identifier ('id').
  inline void moveEarliestToPending(uint64_t id) {
    auto ctx = contexts.front();

    pendingContexts.insert(std::make_pair(id, ctx));
    contexts.pop_front();
  }

  /// Clear out all contexts and callbacks on the session.
  inline void clearContexts() {
    size_t num = contexts.size();

    for (auto i = 0; i < num; i++) {
      auto ctx = contexts.front().first;
      CallbackContext<IAsyncContext> context(ctx);
      contexts.pop_front();
    }
  }

  /// The current view that the session is currently operating in.
  uint64_t currentView;

  /// The most recent CPR offset that the store has informed us of. This is
  /// purely for garbage collection purposes. All requests below this offset
  /// are durable at the server, and need not be kept around for recovery.
  uint64_t latestCPROffset;

  /// Unique identifier for this session. Received from the server on opening
  /// a session to it.
  Guid sessionUID;

  /// Current status of the session (ex: OPEN, or INITIALIZING etc.).
  Status status;

  /// LSN from which we need to re-issue operations that we sent out on this
  /// session. Valid only if status is "VIEW_CHANGED".
  uint64_t reIssueLSN;

  /// The maximum number of in-progress RPCs on this session. Can be used to
  /// throttle requests along with inProgress.
  const uint64_t maxInFlight;

  /// The total number of in-progress RPCs on this session. Can be used to
  /// throttle requests being sent out on this session.
  uint64_t inProgress;

  /// The number of responses received on this session so far. Required mainly
  /// for statistics, esp. during scale-out/migration.
  uint64_t nResponses;

  /// Persistent buffer on which requests will be enqueued before they are sent
  /// out the network.
  session_buffer_t buffer;

  /// If the status of the session is BACK_OFF, then this field is a time-stamp
  /// after which we should resume sending requests.
  time_point<high_resolution_clock> backOffUntil;

 private:
  /// Underlying connection exposed by the transport that will be used to send
  /// RPCs and receive responses to them.
  connection_t* connection;

  /// Pointer to poller exposed by the transport that will be used to receive
  /// responses to any RPCs that were sent out on the session.
  poller_t* poller;

 public:
  /// IP address of the server this session is (will be) connected to.
  std::string serverIP;

  /// Network port of the server thread this session is (will be) connected to.
  std::string serverPort;
};

} // end namespace session
