// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <deque>
#include <chrono>
#include <vector>
#include <string>
#include <stdexcept>
#include <unordered_map>

#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "common.h"
#include "common/log.h"

namespace transport {

using namespace std::chrono;

/// This class is *NOT* thread-safe.
class InfrcTransport {
 public:
  /// Constructs an InfrcTransport. All shared resources belonging to this
  /// layer will be lazily initialized on receiving the first connection.
  InfrcTransport()
   : pd(nullptr)
   , serverRxCq(nullptr)
   , clientRxCq(nullptr)
   , commonTxCq(nullptr)
   , serverSrq(nullptr)
   , clientSrq(nullptr)
   , incomingConns()
   , outgoingConns()
   , rxBuffers()
   , txBuffers()
   , freeTxBuffers()
  {}

  /// Destroys an InfrcTransport. Deallocates all shared resources.
  ~InfrcTransport() {
    // Free all shared resources - srq, completion queues, and the
    // protection domain.
    if (pd) {
      ibv_destroy_srq(serverSrq);
      ibv_destroy_srq(clientSrq);
      ibv_destroy_cq(serverRxCq);
      ibv_destroy_cq(clientRxCq);
      ibv_destroy_cq(commonTxCq);

      // Free all registered receive buffers. There are twice the number of
      // receive buffers than transmit buffers. Half for client RPCs and the
      // other half for server RPCs.
      for (int i = 0; i < 2 * MAX_SHARED_QUEUE_DEPTH; i++) {
        rxBuffers.pop_back();
      }

      // Free all registered transmit buffers.
      for (int i = 0; i < MAX_SHARED_QUEUE_DEPTH; i++) {
        txBuffers.pop_back();
      }

      ibv_dealloc_pd(pd);
    }
  }

  /// Move constructor for InfrcTransport.
  InfrcTransport(InfrcTransport&& from)
   : pd(from.pd)
   , serverRxCq(from.serverRxCq)
   , clientRxCq(from.clientRxCq)
   , commonTxCq(from.commonTxCq)
   , serverSrq(from.serverSrq)
   , clientSrq(from.clientSrq)
   , incomingConns(std::move(from.incomingConns))
   , outgoingConns(std::move(from.outgoingConns))
   , rxBuffers(std::move(from.rxBuffers))
   , txBuffers(std::move(from.txBuffers))
   , freeTxBuffers(std::move(from.freeTxBuffers))
  {
    from.pd = nullptr;
  }

  /// Disallow copy and copy-assign constructors.
  InfrcTransport(const InfrcTransport& from) = delete;
  InfrcTransport& operator=(const InfrcTransport& from) = delete;

 private:
  /// This class represents a buffer registered with the NIC that can be
  /// used to send and receive RPCs.
  class BufferDescriptor {
   public:
    /// Creates a BufferDescriptor. The underlying buffer is aligned to
    /// 4KB and registered with the NIC.
    ///
    /// \param pd
    ///    Protection domain that the created buffer must belong to.
    /// \param length
    ///    The size of the buffer in bytes.
    ///
    /// \throw
    ///    std::runtime_error if initialization failed.
    BufferDescriptor(ibv_pd* pd, uint32_t length)
     : buffer(nullptr)
     , size(length)
     , basePtr(nullptr)
     , mr(nullptr)
    {
      // First allocate the buffer aligned to 4KB. If this fails, throw an
      // exception to the caller.
      int r = posix_memalign(&basePtr, 4096, length);
      if (r != 0) {
        logMessage(Lvl::ERR, "Failed to allocate Infrc buffer (errno %d)", r);
        throw std::runtime_error("Failed to allocate memory for Infrc buffer");
      }

      // Next, register this buffer with the NIC's protection domain.
      mr = ibv_reg_mr(pd, basePtr, length,
                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
      if (!mr) {
        logMessage(Lvl::ERR, "Failed to register Infrc buffer with the NIC");
        throw std::runtime_error("Failed to register Infrc buff with the NIC");
      }

      buffer = static_cast<uint8_t*>(basePtr);
    }

    /// Destroys a BufferDescriptor. This method de-registers and frees
    /// the underlying buffer.
    ~BufferDescriptor() {
      // Do not free resources if allocation failed or if there was a move.
      if (!basePtr) return;

      // De-register and free the buffer.
      ibv_dereg_mr(mr);
      free(basePtr);
    }

    /// Move constructor.
    BufferDescriptor(BufferDescriptor&& from) {
      buffer = std::move(from.buffer);
      size = std::move(from.size);
      basePtr = std::move(from.basePtr);
      mr = std::move(from.mr);

      from.basePtr = nullptr;
    }

    /// Disallow copy and copy-assign constructors.
    BufferDescriptor(const BufferDescriptor& from) = delete;
    BufferDescriptor& operator=(const BufferDescriptor& from) = delete;

    /// Pointer to the buffer registered with the NIC. This buffer is `size'
    /// bytes long.
    uint8_t* buffer;

    /// Size of the above buffer in bytes.
    uint32_t size;

    /// Same as `buffer', except that this is a void*.
    void* basePtr;

    /// Pointer to the memory region of the buffer.
    ibv_mr* mr;
  };

  /// This class represents an RPC request received at a server.
  class ServerRpc {
   public:
    /// Constructor for ServerRpc.
    ///
    /// \param request
    ///    Buffer descriptor containing the RPC request.
    /// \param response
    ///    Buffer descriptor that will hold the response.
    /// \param connId
    ///    Transport identifier for the connection the RPC was received on.
    /// \param t
    ///    Pointer to transport layer the RPC was received on.
    ServerRpc(BufferDescriptor* request, BufferDescriptor* response,
              rdma_cm_id* connId, InfrcTransport* t)
     : request(request)
     , response(response)
     , sentReply(false)
     , connId(connId)
     , transport(t)
    {}

    /// Destructor for ServerRpc.
    ~ServerRpc() {
      // If moved out, then do nothing.
      if (!connId) return;

      // Post the buffer in which the RPC was received (`request') to the
      // NIC as part of a receive request.
      transport->postReceive(true, request);

      // If the response was not sent out, then return the response buffer
      // to the list of free tx buffers. Otherwise, the transmitted buffer
      // will be recycled separately.
      if (!sentReply) {
        transport->recycleTxBuffer(response);
      }
    }

    /// Move constructor.
    ServerRpc(ServerRpc&& from) {
      request = std::move(from.request);
      response = std::move(from.response);
      sentReply = std::move(from.sentReply);
      connId = std::move(from.connId);
      transport = std::move(from.transport);

      from.connId = nullptr;
    }

    /// Disallow copy and copy-assign constructors.
    ServerRpc(const ServerRpc& from) = delete;
    ServerRpc& operator=(const ServerRpc& from) = delete;

    /// Sends out the response to the client.
    ///
    /// \param size
    ///    The total size of the response in bytes.
    ///
    /// \throw
    ///    std::runtime_error if the send failed.
    void sendResponse(uint32_t size) {
      transport->send(connId, response, size);
      sentReply = true;
    }

    /// NIC registered buffer that contains the received RPC request.
    BufferDescriptor* request;

    /// NIC registered buffer that will hold the response.
    BufferDescriptor* response;

   private:
    /// Flag indicating whether a response to this RPC has been sent out. If true,
    /// then a response has been sent. False otherwise.
    bool sentReply;

    /// RDMA connection identifier over which the RPC was received.
    rdma_cm_id* connId;

    /// Transport layer over which the RPC was received.
    InfrcTransport* transport;
  };

  /// This class represents an RPC that will be sent to a server.
  class ClientRpc {
   public:
    /// Constructs a ClientRpc.
    ///
    /// \param request
    ///    Pointer to NIC registered buffer that will hold the RPC request
    ///    to be sent out the network.
    /// \param conn
    ///    RDMA connection id over which the RPC should be sent out.
    /// \param transport
    ///    Transport layer over which the RPC should be sent out.
    ClientRpc(BufferDescriptor* request, rdma_cm_id* conn,
              InfrcTransport* transport)
     : request(request)
     , response(nullptr)
     , sent(false)
     , connId(conn)
     , transport(transport)
    {}

    /// Destructor for ClientRpc.
    ~ClientRpc() {
      // If moved out, then do nothing.
      if (!connId) return;

      // If the RPC was never sent out, recycle the transmit/request buffer.
      // If it was sent out, then this buffer will be recycled by the transport.
      if (!sent) {
        transport->recycleTxBuffer(request);
      }

      // If a response to the RPC was received, then post a new receive request
      // with the response buffer to the NIC.
      if (response) {
        transport->postReceive(false, response);
      }
    }

    /// Move constructor.
    ClientRpc(ClientRpc&& from) {
      request = std::move(from.request);
      response = std::move(from.response);
      sent = std::move(from.sent);
      connId = std::move(from.connId);
      transport = std::move(from.transport);

      from.connId = nullptr;
    }

    /// Disallow copy and copy-assign constructors.
    ClientRpc(const ClientRpc& from) = delete;
    ClientRpc& operator=(const ClientRpc& from) = delete;

    /// Sends the RPC out the network.
    ///
    /// \param size
    ///    The size of the RPC in bytes.
    ///
    /// \throw
    ///    std::runtime_error if the send fails.
    void sendRpc(uint32_t size) {
      transport->send(connId, request, size);
      sent = true;
    }

    /// Pointer to NIC registered buffer that contains the RPC request to be
    /// sent out the network.
    BufferDescriptor* request;

    /// Pointer to NIC registered buffer holding the RPC response. NULL until
    /// a response to this RPC is received.
    BufferDescriptor* response;

   private:
    /// Flag indicating whether the RPC was sent out. True if the RPC was sent.
    bool sent;

    /// RDMA connection identifier over which the RPC will be sent out.
    rdma_cm_id* connId;

    /// Transport layer over which the RPC will be sent out.
    InfrcTransport* transport;
  };

 public:
  /// This class represents a connection between two machines. It can
  /// be used to send and receive data (RPCs) between them.
  class Connection {
   public:
    /// Type signature of the function to be invoked when an RPC request
    /// or response has to be sent on the connection.
    typedef uint32_t (S)(void*, uint8_t*);

    /// Type signature of the function to be invoked when an RPC request
    /// or response is received on the connection.
    typedef uint32_t (R)(void*, uint8_t*, uint8_t*);

    /// Constructs a connection. This method assumes that the handshake between
    /// the client and server has already been performed.
    ///
    /// \param connId
    ///    RDMA connection identifier for the connection. A queue-pair should
    ///    have been allocated over this identifier.
    /// \param transport
    ///    Pointer to infiniband transport layer over which this connection was
    ///    established.
    /// \param channel
    ///    Pointer to event channel over which control messages will be
    ///    received. Can be NULL if no event channel is to be used.
    /// \param server
    ///    Flag indicating whether this end of the connection is a server (true)
    ///    or client (false).
    Connection(rdma_cm_id* connId, InfrcTransport* transport,
               rdma_event_channel* channel, bool server)
     : status(Status::ALIVE)
     , connId(connId)
     , transport(transport)
     , channel(channel)
     , server(server)
     , incomingRpcs()
     , outgoingRpcs()
     , nextResp(0)
    {
      // If there was an event channel passed in, then make it non blocking.
      if (channel) {
        int flags = fcntl(channel->fd, F_GETFL);
        if (flags == -1) {
          logMessage(Lvl::ERR, "Failed to retrieve channel status (errno %d)",
                     errno);
          throw std::runtime_error("Error when trying to read channel status");
        }

        if (fcntl(channel->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
          logMessage(Lvl::ERR,
                     "Failed to make channel non-blocking (errno %d)",
                     errno);
          throw std::runtime_error("Error when making channel non-blocking");
        }
      }
    }

    /// Destructor for Connection.
    ~Connection()
    {
      // Destroy the channel if it exists.
      if (channel) rdma_destroy_event_channel(channel);

      // Destroy any pending incoming RPCs.
      for (size_t i = 0; i < incomingRpcs.size(); i++) {
        incomingRpcs.pop_front();
      }

      // Destroy any pending outgoing RPCs.
      for (size_t i = 0; i < outgoingRpcs.size(); i++) {
        outgoingRpcs.pop_front();
      }
    }

    /// Disallow copy and copy-assign constructors.
    Connection(const Connection& from) = delete;
    Connection& operator=(const Connection& from) = delete;

    /// Sends an RPC out the active (client) end of a connection.
    ///
    /// \param context
    ///    An opaque parameter that will be passed into the sender function S
    ///    before the RPC is sent out.
    /// \param cb
    ///    Function to be invoked before sending out the RPC.
    ///
    /// \throw
    ///    std::runtime_error if the send fails or the connection is dead.
    void sendRpc(void* context, S cb) {
      // If the connection is not alive, then throw an exception to the session.
      if (status != Status::ALIVE) {
        logMessage(Lvl::ERR, "Trying to send RPC on a dead Infrc connection");
        throw std::runtime_error("Cannot send RPC on a dead Infrc connection");
      }

      // Retrieve a transmit buffer and invoke the supplied sender function.
      BufferDescriptor* tx = transport->getTxBuffer();
      uint32_t size = cb(context, tx->buffer);

      // Construct and send out the RPC.
      outgoingRpcs.emplace_back(ClientRpc(tx, connId, transport));
      outgoingRpcs.back().sendRpc(size);
    }

    /// Receives on a connection.
    ///
    /// When called on the active (client) end of a connection, this method will
    /// receive responses to outgoing RPCs. When called on the passive (server)
    /// end of a connection, this method will check for an incoming RPC, handle
    /// it using the receive function R, and return a response 
    ///
    /// \param context
    ///    An opaque parameter that will be passed into the receiver function R
    ///    on receiving an RPC request (server) or response (client).
    /// \param cb
    ///    Function to be invoked if there was something received.
    ///
    /// \throw
    ///    std::runtime_error if the connection is dead.
    void receive(void* context, R cb) {
      // If the connection is not alive, then throw an exception to the session.
      if (status != Status::ALIVE) {
        logMessage(Lvl::DEBUG, "Trying to receive on a dead Infrc connection");
        throw std::runtime_error("Can't recv on a dead Infrc connection");
      }

      if (server) {
        recvServer(context, cb);
      } else {
        recvClient(context, cb);
      }
    }

    /// Disables send and receive on the connection. In order to be able to
    /// garbage collect a connection, this method needs to be called twice.
    /// Once by the transport when it receives a RDMA_CM_EVENT_DISCONNECTED
    /// on the appropriate channel, and once by the session once it has
    /// released its pointer to this connection.
    void tryMarkDead() {
      switch (status) {
      case Status::ALIVE:
        status = Status::DISCONNECTED;
        break;

      case Status::DISCONNECTED:
        status = Status::DEAD;
        break;

      default:
        break;
      }
    }

    /// Returns true if the connection is alive. Generally invoked by the
    /// sessions layer to check if this connection can still send or receive.
    /// Useful when it comes to implementing RPC timeouts.
    bool isAlive() {
      // If this is the client end of a connection, check the event channel
      // for a disconnect event.
      pollChannel();

      return (status == Status::ALIVE);
    }

    /// Returns true if the connection is dead. Generally invoked by the
    /// transport layer to check if this connection can be garbage collected.
    bool isDead() {
      // If this is the client end of a connection, check the event channel
      // for a disconnect event.
      pollChannel();

      return (status == Status::DEAD);
    }

   private:
    /// Receives, processes, and responds to incoming RPC requests at the
    /// server/passive end of a connection.
    ///
    /// Refer to receive() for parameter documentation.
    void recvServer(void* context, R cb) {
      // No incoming RPCs at the moment.
      if (incomingRpcs.empty()) return;

      // Retrieve waiting rpcs and invoke the supplied receive function.
      ServerRpc& rpc = incomingRpcs.front();
      uint32_t respSize = cb(context, rpc.request->buffer,
                             rpc.response->buffer);

      // Send the response out the network.
      rpc.sendResponse(respSize);
      incomingRpcs.pop_front();
    }

    /// Receives and processes responses to outgoing RPC requests at the
    /// client/active end of a connection.
    ///
    /// Refer to receive() for parameter documentation.
    void recvClient(void* context, R cb) {
      // No outgoing RPCs at the moment.
      if (outgoingRpcs.empty()) return;

      // Check if a response to the earliest sent RPC has been received.
      // Assuming FIFO, we can return if it hasn't.
      ClientRpc& rpc = outgoingRpcs.front();
      if (!rpc.response) return;

      // If a response is available, invoke the supplied receive function.
      cb(context, rpc.request->buffer, rpc.response->buffer);
      outgoingRpcs.pop_front();
      nextResp--;
    }

    /// Polls the event channel on the active side of a connection.
    inline void pollChannel() {
      if (!channel) return;

      // Check for an event on the channel. Return if there isn't one.
      struct rdma_cm_event* event;
      if (rdma_get_cm_event(channel, &event) < 0) return;

      // If the connection has been disconnected, try to mark it as dead.
      if (event->event != RDMA_CM_EVENT_DISCONNECTED) {
        logMessage(Lvl::ERR, "Received unexpected Infrc event on channel: %s",
                   rdma_event_str(event->event));
        return;
      }

      tryMarkDead();
    }

    /// The current status of the Connection. Takes up a single byte.
    enum class Status : uint8_t {
      /// The connection is up and running. It can be used for send and receive.
      ALIVE,

      /// The connection has been disconnected but cannot be garbage collected
      /// by the transport layer yet. This is because the sessions layer might
      /// still be holding a pointer to it.
      DISCONNECTED,

      /// The connection has been disconnected and can be garbage collected by
      /// the transport layer. The sessions layer no longer holds a pointer.
      DEAD,
    };

    /// Current status of the connection indicating whether it can be used for
    /// send and receive, or should be garbage collected.
    Status status;

    /// RDMA CM identifier for this connection.
    rdma_cm_id* connId;

    /// Pointer to infrc transport layer so that this class can be used to
    /// send and receive RPC requests and responses.
    InfrcTransport* transport;

    /// Non-blocking event channel that can be polled for control messages.
    rdma_event_channel* channel;

    /// Flag indicating whether this end is a server (true) or client (false).
    bool server;

    /// A queue of incoming RPC requests on this connection that still need
    /// to be serviced.
    std::deque<ServerRpc> incomingRpcs;

    /// A queue of outgoing RPC requests on this connection that are waiting
    /// for a response.
    std::deque<ClientRpc> outgoingRpcs;

    /// Index into outgoingRpcs at which the next response must be enqueued.
    int nextResp;

    /// Allow Poller to enqueue RPC requests onto this connection.
    friend class InfrcTransport;
  };

  /// This class is responsible for listening for incoming connections
  /// from remote machines, and setting up connections to remote machines.
  class ConnectHandler {
   public:
    /// Constructs a ConnectHandler.
    ///
    /// \param lPort
    ///    The port that this instance must listen to for incoming
    ///    connections. If zero, then listening will be disabled.
    /// \param lIP
    ///    The IP address the this instance must listen to for connections.
    /// \param transport
    ///    Pointer to infrc transport layer containing all shared resources
    ///    required to accept and create connections.
    ///
    /// \throw
    ///    std::runtime_error if initialization failed.
    ConnectHandler(uint16_t lPort, std::string& lIP, InfrcTransport* transport)
     : listen(false)
     , listener(nullptr)
     , lChannel(nullptr)
     , transport(transport)
     , interval(milliseconds(1))
     , next(high_resolution_clock::now())
    {
      // If lPort is 0, then this instance will not be able to listen for
      // incoming connections.
      if (lPort == 0) {
        return;
      }

      // Setup the listener so that we can receive and accept incoming
      // connections from clients. Listening is done using an event channel
      // based rdma connection identifier. The channel helps us listen
      // asynchronously.
      listen = true;

      lChannel = rdma_create_event_channel();
      if (!lChannel) {
        auto err = std::string("Failed to create Infrc listener ") +
                   "on port %lu, IP %s (errno %d). Do you have " +
                   "Infiniband configured on the machine?";
        logMessage(Lvl::ERR, err.c_str(), lPort, lIP.c_str(), errno);
        throw std::runtime_error("Error when creating Infrc listener");
      }

      int flags = fcntl(lChannel->fd, F_GETFL);
      if (flags == -1) {
        logMessage(Lvl::ERR,
                   "Failed to retrieve listner channel status (errno %d)",
                   errno);
        throw std::runtime_error("Error when trying to read channel status");
      }

      if (fcntl(lChannel->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        logMessage(Lvl::ERR,
                   "Failed to make listner channel non-blocking (errno %d)",
                   errno);
        throw std::runtime_error("Error when making channel non-blocking");
      }

      if (rdma_create_id(lChannel, &listener, nullptr, RDMA_PS_TCP) == -1) {
        logMessage(Lvl::ERR,
                   "Failed to create RDMA connection id for listener (errno %d)",
                   errno);
        throw std::runtime_error("Failed to create RDMA conn id for listener");
      }

      // The address to listen on for incoming connections.
      struct sockaddr_in sin = {
        .sin_family = AF_INET,
        .sin_port = htons(lPort),
        .sin_addr = {
          .s_addr = inet_addr(lIP.c_str()),
        },
      };

      if (rdma_bind_addr(listener, (struct sockaddr*)&sin) == -1) {
        auto err = std::string("Error when trying to bind Infrc listener to ") +
                   "port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), lPort, lIP.c_str(), errno);
        throw std::runtime_error("Error when binding Infrc listener");
      }

      if (rdma_listen(listener, 8) == -1) {
        auto err = std::string("Error when marking channel as listener on ") +
                   "port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), lPort, lIP.c_str(), errno);
        throw std::runtime_error("Error when marking channel as listener");
      }
    }

    /// Default constructor.
    ConnectHandler()
     : listen(false)
     , listener(nullptr)
     , lChannel(nullptr)
     , transport(nullptr)
     , interval(milliseconds(1))
     , next(high_resolution_clock::now())
    {}

    /// Destroys a ConnectHandler.
    ~ConnectHandler() {
      // Tear down the listener if configured.
      if (listen) {
        rdma_destroy_id(listener);
        rdma_destroy_event_channel(lChannel);
      }
    }

    /// Disallow copy and copy-assign constructors.
    ConnectHandler(const ConnectHandler& from) = delete;
    ConnectHandler& operator=(const ConnectHandler& from) = delete;

    /// Move constructor.
    ConnectHandler(ConnectHandler&& from)
     : listen(from.listen)
     , listener(from.listener)
     , lChannel(from.lChannel)
     , transport(from.transport)
     , interval(from.interval)
     , next(from.next)
    {
      from.listen = false;
      from.transport = nullptr;
    }

    /// Move assign operator.
    ConnectHandler& operator=(ConnectHandler&& from) {
      listen = from.listen;
      from.listen = false;

      listener = from.listener;
      lChannel = from.lChannel;

      transport = from.transport;
      from.transport = nullptr;
    }

    /// Checks for and sets up (if any) incoming connections.
    ///
    /// \return
    ///    A pointer to the incoming connection if there was one.
    ///
    /// \throw
    ///    std::runtime_error if there was an error while setting up
    ///    a connection.
    Connection* poll() {
      // Cannot check for incoming connections if the listener isn't configured.
      if (!listen) {
        logMessage(Lvl::ERR,
                   "Infrc listener not configured. Can't poll for connections");
        throw std::runtime_error("Infrc listener not configured");
      }

      // Check the current time, and determine whether we should poll for
      // a connection.
      auto curr = high_resolution_clock::now();
      if (curr < next) return nullptr;

      // We poll again after the next interval.
      next = curr + interval;

      // Check for a connection request on the listener's event channel. If
      // there is one, then create a connection identifier.
      struct rdma_cm_event* event;
      if (rdma_get_cm_event(lChannel, &event) < 0) {
        return nullptr;
      }

      // A connection request was received. Allocate a queue-pair, accept the
      // connection and create a connection object at the transport layer.
      if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
        struct rdma_cm_id* connId = event->id;
        rdma_ack_cm_event(event);

        // Next, setup and accept the connection. To do so, first create a
        // queue pair on the connection id.
        transport->allocQueuePair(connId, true);

        struct rdma_conn_param param;
        memset(&param, 0, sizeof(param));
        if (rdma_accept(connId, &param) == -1) {
          logMessage(Lvl::ERR,
                     "Failed to accept incoming Infrc connection (errno %d)",
                     errno);
          throw std::runtime_error("Failed to accept incoming Infrc conn");
        }

        // Create and return a Connection object.
        transport->incomingConns.emplace(std::piecewise_construct,
                      std::forward_as_tuple(connId->qp->qp_num),
                      std::forward_as_tuple(connId, transport, nullptr, true));

        return &(transport->incomingConns.at(connId->qp->qp_num));
      }
      // Received an ACK that the client side of a connection has also created
      // it's queue-pair. Nothing to do here.
      else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
        struct rdma_cm_id* connId = event->id;
        rdma_ack_cm_event(event);
        return nullptr;
      }
      // Unexpected event. Handle separately and return a nullptr since it does
      // not correspond to a connection request.
      else {
        handleUnexpectedEvent(event);
        rdma_ack_cm_event(event);
        return nullptr;
      }
    }

    /// Connects to a server.
    ///
    /// \param ip
    ///    IP address of the server.
    /// \param port
    ///    Port that the server is listening on.
    ///
    /// \return
    ///    A pointer to a connection.
    ///
    /// \throw
    ///    std::runtime_error if something went wrong while connection.
    Connection* connect(const std::string& ip, const std::string& port) {
      // Setup an event channel and identifier for the connection. The channel
      // is left as blocking until the connection has been fully established.
      struct rdma_event_channel* channel = rdma_create_event_channel();
      if (!channel) {
        auto err = std::string("Failed to create Infrc event channel for ") +
                   "connection to port %s, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Failed to create event channel for conn");
      }

      struct rdma_cm_id* connId;
      if (rdma_create_id(channel, &connId, nullptr, RDMA_PS_TCP) == -1) {
        auto err = std::string("Failed to create Infrc identifier for ") +
                   "connection to port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Failed to create Infrc id for connection");
      }

      // Translate the server's address from the passed in arguments.
      struct addrinfo hints;
      memset(&hints, 0, sizeof(addrinfo));
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      struct addrinfo *server;
      auto ret = getaddrinfo(ip.c_str(), port.c_str(), &hints, &server);
      if (ret != 0) {
        auto msg = std::string("Failed to parse address for Infrc ") +
                   "connection to port %s, IP %s: " + gai_strerror(ret);
        logMessage(Lvl::ERR, msg.c_str(), port.c_str(), ip.c_str());
        throw std::runtime_error("Error when parsing server Infrc address.");
      }

      // Resolve the server's address into an RDMA address. The timeout for
      // this operation is set to 5000 milliseconds. Check the channel for an
      // event corresponding the same.
      if (rdma_resolve_addr(connId, nullptr, server->ai_addr, 5000) == -1) {
        auto err = std::string("Failed to resolve server address when ") +
                   "connecting to port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Failed to resolve server address");
      }

      struct rdma_cm_event* event;
      if (rdma_get_cm_event(channel, &event) == -1) {
        auto err = std::string("Failed to retrieve event when ") +
                   "connecting to port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Failed to retrieve event when connecting");
      }

      if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        logMessage(Lvl::ERR, "Received unexpected Infrc event: %s",
                   rdma_event_str(event->event));
        throw std::runtime_error(rdma_event_str(event->event));
      }
      rdma_ack_cm_event(event);

      // Resolve a route to the server. The timeout for this operation is set to
      // 5000 milliseconds. Check the channel for a corresponding event.
      if (rdma_resolve_route(connId, 5000) == -1) {
        auto err = std::string("Failed to resolve route when ") +
                   "connecting to port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Failed to resolve route to the server");
      }

      if (rdma_get_cm_event(channel, &event) == -1) {
        auto err = std::string("Failed to retrieve event when ") +
                   "connecting to port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Failed to retrieve event when connecting");
      }

      if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        logMessage(Lvl::ERR, "Received unexpected Infrc event: %s",
                   rdma_event_str(event->event));
        throw std::runtime_error(rdma_event_str(event->event));
      }
      rdma_ack_cm_event(event);

      // Allocate a queue pair for the connection.
      transport->allocQueuePair(connId, false);

      // Connect to the server. Check the channel for a corresponding event.
      struct rdma_conn_param param;
      memset(&param, 0, sizeof(param));
      if (rdma_connect(connId, &param) == -1) {
        auto err = std::string("Failed to connect Infrc connection-id to ") +
                   "port %s, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Error when connecting Infrc id to server");
      }

      if (rdma_get_cm_event(channel, &event) == -1) {
        auto err = std::string("Failed to retrieve event when ") +
                   "connecting to port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Failed to retrieve event when connecting");
      }

      if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        logMessage(Lvl::ERR, "Received unexpected Infrc event: %s",
                   rdma_event_str(event->event));
        throw std::runtime_error(rdma_event_str(event->event));
      }
      rdma_ack_cm_event(event);

      // Create and return a new connection.
      transport->outgoingConns.emplace(std::piecewise_construct,
                      std::forward_as_tuple(connId->qp->qp_num),
                      std::forward_as_tuple(connId, transport, channel, false));

      return &(transport->outgoingConns.at(connId->qp->qp_num));
    }

   private:
    /// Handles any unexpected events that occur when trying to
    /// receive connections.
    void handleUnexpectedEvent(rdma_cm_event* event) {
      // For now, handle only disconnected. Throw a reasonably verbose
      // exception in all other cases.
      if (event->event != RDMA_CM_EVENT_DISCONNECTED) {
        logMessage(Lvl::ERR, "Received unexpected Infrc event: %s",
                   rdma_event_str(event->event));
        throw std::runtime_error(rdma_event_str(event->event));
      }

      // First, get a reference to the disconnected connection. Then, mark
      // it as dead. It will be asynchronously garbage collected later.
      auto qp_num = event->id->qp->qp_num;

      // Check if the connection was an incoming connection. We do not handle
      // outgoing connections in this method.
      auto inConn = transport->incomingConns.find(qp_num);
      if (inConn != transport->incomingConns.end()) {
        inConn->second.tryMarkDead();
      }
    }

    /// Flag indicating if this class can listen for incoming connections.
    /// If false, then it cannot.
    bool listen;

    /// Communication identifier of a listener. This will be used to listen
    /// for and accept incoming connections.
    struct rdma_cm_id* listener;

    /// Event channel used to receive events related to the listener.
    struct rdma_event_channel* lChannel;

    /// Pointer to infrc transport layer. The transport consists of all shared
    /// resources such as the protection domain, completion queues, and the
    /// shared receive queue. They are required to create connections.
    InfrcTransport* transport;

    /// Time interval at which we should poll for a new connection.
    milliseconds interval;

    /// The next point in time at which we should poll for a new connection.
    time_point<high_resolution_clock> next;
  };

  /// This class checks for and constructs incoming RPCs, and checks for
  /// responses to outgoing RPCs.
  class Poller {
   public:
    /// Constructs a Poller.
    ///
    /// \param transport
    ///    Pointer to transport layer that RPC requests and responses will
    ///    be received on.
    Poller(InfrcTransport* transport)
     : transport(transport)
    {}

    /// Default constructor.
    Poller()
     : transport(nullptr)
    {}

    /// Destroys the Poller.
    ~Poller() {}

    /// Disallow copy and copy-assign constructors.
    Poller(const Poller& from) = delete;
    Poller& operator=(const Poller& from) = delete;

    /// Move constructor.
    Poller(Poller&& from)
     : transport(from.transport)
    {
      from.transport = nullptr;
    }

    /// Move assign operator.
    Poller& operator=(Poller&& from) {
      transport = from.transport;
      from.transport = nullptr;
    }

    /// Polls for incoming RPC requests and responses. Enqueues them on the
    /// correct connection if there are any.
    void poll() {
      // Pre-allocated array that will hold all of the work completions.
      static const int MAX_COMPLETIONS = 64;
      ibv_wc wc[MAX_COMPLETIONS];

      // First, check for any incoming RPCs. Do this by polling on the
      // server-receive-completion-queue (serverRxCq).
      int n = ibv_poll_cq(transport->serverRxCq, MAX_COMPLETIONS, wc);
      if (n < 0) {
        logMessage(Lvl::ERR, "Failed to poll server rx completion queue");
        throw std::runtime_error("Failed to poll server rx completion queue");
      }

      for (int i = 0; i < n; i++) {
        ibv_wc* request = &wc[i];
        if (request->status != IBV_WC_SUCCESS) {
          logMessage(Lvl::ERR,
                     "Failed to receive RPC request: %s",
                     ibv_wc_status_str(request->status));
          throw std::runtime_error("Failed to receive RPC request");
        }

        // Find the connection, and enqueue a ServerRpc on it.
        Connection& conn = transport->incomingConns.at(request->qp_num);
        conn.incomingRpcs.emplace_back(
            ServerRpc(
              reinterpret_cast<BufferDescriptor*>(request->wr_id), // Request
              transport->getTxBuffer(),                            // Response
              conn.connId,                                         // Conn Id
              transport)                                           // Transport
        );
      }

      // Next, check for any responses to outgoing RPCs. Do this by polling on
      // the client-receive-completion-queue (clientRxCq).
      int m = ibv_poll_cq(transport->clientRxCq, MAX_COMPLETIONS, wc);
      if (m < 0) {
        logMessage(Lvl::ERR, "Failed to poll client rx completion queue");
        throw std::runtime_error("Failed to poll client rx completion queue");
      }

      for (int i = 0; i < m; i++) {
        ibv_wc* request = &wc[i];
        if (request->status != IBV_WC_SUCCESS) {
          logMessage(Lvl::ERR,
                     "Failed to receive RPC response: %s",
                     ibv_wc_status_str(request->status));
          throw std::runtime_error("Failed to receive RPC response");
        }

        // Find the connection, and set the response buffer on the corresponding
        // RPC. Assuming responses are received in the order RPCs were sent out.
        Connection& conn = transport->outgoingConns.at(request->qp_num);
        conn.outgoingRpcs[conn.nextResp++].response =
                reinterpret_cast<BufferDescriptor*>(request->wr_id);
      }

      // Check for completed transmits, and reclaim buffers if we're
      // running low on transmit buffers.
      if (transport->freeTxBuffers.size() < 3) transport->reapTxBuffers();
    }

   private:
    /// Pointer to infrc transport layer. Contains shared receive queues
    /// required to receive RPC requests and responses.
    InfrcTransport* transport;
  };

  /// Allocates shared resources required for transmit and receive over
  /// connections.
  ///
  /// \param ctxt
  ///    An ibv_context within which resources should be allocated.
  ///
  /// \throw
  ///    std::runtime_exception if allocation failed.
  void allocSharedResources(ibv_context* ctxt) {
    // Allocate a protection domain over the passed in context.
    pd = ibv_alloc_pd(ctxt);

    // Allocate completion queues.
    serverRxCq = ibv_create_cq(ctxt, MAX_SHARED_QUEUE_DEPTH,
                              nullptr, nullptr, 0);
    clientRxCq = ibv_create_cq(ctxt, MAX_SHARED_QUEUE_DEPTH,
                              nullptr, nullptr, 0);
    commonTxCq = ibv_create_cq(ctxt, MAX_SHARED_QUEUE_DEPTH,
                              nullptr, nullptr, 0);

    // Allocate the shared receive queues.
    struct ibv_srq_init_attr sia = {
      .srq_context = nullptr,
      .attr = {
        .max_wr = MAX_SHARED_QUEUE_DEPTH,
        .max_sge = MAX_SHARED_QUEUE_SGE,
      },
    };
    serverSrq = ibv_create_srq(pd, &sia);
    clientSrq = ibv_create_srq(pd, &sia);

    // Throw an exception if any of the above allocations failed.
    bool err = !pd || !serverSrq || !clientSrq || !serverRxCq || !clientRxCq ||
               !commonTxCq;
    if (err) {
      logMessage(Lvl::ERR, "Failed to allocate shared resources for Infrc");
      throw std::runtime_error("Failed to allocate shared resources for Infrc");
    }

    // Allocate and register receive buffers. Half are to receive RPC requests,
    // and the other half are to receive RPC responses.
    // XXX: Need to explicitly provide the type of NIC_BUFFER_SIZE (uint32_t) so
    // that the compiler passes it in by value and not reference.
    for (int i = 0; i < 2 * MAX_SHARED_QUEUE_DEPTH; i++) {
      rxBuffers.emplace_back(pd, (uint32_t) NIC_BUFFER_SIZE);
    }

    // Post half the allocated receive buffers to the NIC inside receive
    // requests. These will be used to receive new RPC requests.
    for (int i = 0; i < MAX_SHARED_QUEUE_DEPTH; i++) {
      postReceive(true, &(rxBuffers[i]));
    }

    // Post the other half of allocated receive buffers to the NIC inside
    // receive requests too. These will be used to receive RPC responses.
    for (int i = MAX_SHARED_QUEUE_DEPTH; i < 2 * MAX_SHARED_QUEUE_DEPTH; i++) {
      postReceive(false, &(rxBuffers[i]));
    }

    // Allocate and register transmit buffers.
    // XXX: Need to explicitly provide the type of NIC_BUFFER_SIZE (uint32_t) so
    // that the compiler passes it in by value and not reference.
    for (int i = 0; i < MAX_SHARED_QUEUE_DEPTH; i++) {
      txBuffers.emplace_back(pd, (uint32_t) NIC_BUFFER_SIZE);
    }

    // To begin with, all allocated transmit buffers are free to be used to
    // send RPC requests and responses.
    for (int i = 0; i < MAX_SHARED_QUEUE_DEPTH; i++) {
      freeTxBuffers.push_back(&(txBuffers[i]));
    }
  }

  /// Allocates a queue-pair.
  ///
  /// \param connId
  ///    The RDMA connection id over which to allocate the queue-pair.
  /// \param server
  ///    Flag indicating whether the queue pair is for a server or client.
  ///    If true, then it is for a server.
  ///
  /// \throw
  ///    std::runtime_exception if allocation failed.
  void allocQueuePair(rdma_cm_id* connId, bool server) {
    // If required, initialize all shared verbs data structures.
    if (!pd) {
      allocSharedResources(connId->verbs);
    }

    // Setup queue pair attributes.
    struct ibv_qp_init_attr attr = {
      .qp_context = nullptr,
      .send_cq = commonTxCq,
      .recv_cq = server ? serverRxCq : clientRxCq,
      .srq = server ? serverSrq : clientSrq,
      .cap = {
        .max_send_wr = MAX_SHARED_QUEUE_DEPTH,
        .max_recv_wr = MAX_SHARED_QUEUE_DEPTH,
        .max_send_sge = MAX_SHARED_QUEUE_SGE,
        .max_recv_sge = MAX_SHARED_QUEUE_SGE,
        .max_inline_data = 0,
      },
      .qp_type = IBV_QPT_RC,
      .sq_sig_all = 0,
    };

    if (rdma_create_qp(connId, pd, &attr) == -1) {
      logMessage(Lvl::ERR, "Error on trying to create Infrc qp (errno %d)",
                 errno);
      throw std::runtime_error("Failed to create Infrc queue pair");
    }
  }

  /// Posts a receive request to a shared receive queue.
  ///
  /// \param server
  ///    Boolean indicating which receive queue to post the request too.
  ///    If true, posts to the server queue. If false, post to the client queue.
  /// \param rxBuffer
  ///    Pointer to buffer that data will be received into.
  ///
  /// \throw
  ///    std::runtime_error if there was an error while posting a receive.
  void postReceive(bool server, BufferDescriptor* rxBuffer) {
    // Scatter gather entry with the buffer that the RPC request/response
    // will be received into.
    struct ibv_sge sge = {
      .addr = (uint64_t) rxBuffer->basePtr,
      .length = rxBuffer->size,
      .lkey = rxBuffer->mr->lkey,
    };

    // Post a work request informing the NIC that we're expecting to receive
    // something on the queue pair's shared receive queue. In order to be able
    // to re-use the buffer later, we piggyback a pointer to it on the work
    // request id.
    struct ibv_recv_wr wr = {
      .wr_id = reinterpret_cast<uint64_t>(rxBuffer),
      .next = nullptr,
      .sg_list = &sge,
      .num_sge = 1,
    };
    struct ibv_recv_wr *badWr;
    int err = ibv_post_srq_recv(server ? serverSrq : clientSrq, &wr, &badWr);
    if (err != 0) {
      logMessage(Lvl::ERR,
                 "Failed to post receive to Infrc srq (errno %d)", err);
      throw std::runtime_error("Error when posting receive to Infrc srq");
    }
  }

  /// Checks for completed transmissions/sends. If any, the transmit buffer is
  /// recycled for future use.
  ///
  /// \throw
  ///    std::runtime_error if there was an error while polling the completion
  ///    queue, or if a prior transmit was unsuccessful.
  void reapTxBuffers() {
    static const uint32_t MAX_COMPLETIONS = 64;
    ibv_wc wc[MAX_COMPLETIONS];

    // Poll the transmit completion queue. For each transmit that completed,
    // the buffer descriptor can be recycled.
    int n = ibv_poll_cq(commonTxCq, MAX_COMPLETIONS, wc);
    if (n < 0) {
      logMessage(Lvl::ERR,
                 "Error on polling for tx completions on Infrc queue (ret: %d)",
                 n);
      throw std::runtime_error("Error on polling tx completion queue");
    }

    for (int i = 0; i < n; i++) {
      ibv_wc* tx = &(wc[i]);
      if (tx->status != IBV_WC_SUCCESS) {
        logMessage(Lvl::ERR,
                   "Failed to complete previously issued Infrc tx (qp %lu): %s",
                   tx->qp_num, ibv_wc_status_str(tx->status));
        throw std::runtime_error("Failed to complete Infrc tx");
      }

      // The pointer to the tx buffer was piggybacked on the work request id.
      BufferDescriptor* buf = reinterpret_cast<BufferDescriptor*>(tx->wr_id);
      freeTxBuffers.push_back(buf);
    }

    // Garbage collect all dead incoming connections.
    for (auto c = incomingConns.begin(); c != incomingConns.end(); c++) {
      if (c->second.isDead()) incomingConns.erase(c);
    }

    // Garbage collect all dead outgoing connections.
    for (auto c = outgoingConns.begin(); c != outgoingConns.end(); c++) {
      if (c->second.isDead()) outgoingConns.erase(c);
    }
  }

  /// Retrieves a free transmit buffer. If required, checks for completed
  /// transmissions and recycles corresponding transmit buffers.
  ///
  /// \return
  ///    A pointer to a transmit buffer that can be used to send an RPC
  ///    request or response.
  ///
  /// \throw
  ///    std::runtime_error if a buffer could not be retrieved.
  BufferDescriptor* getTxBuffer() {
    // If there aren't any free transmit buffers at the moment, then try to
    // recycle those issued as part of prior transmissions.
    while (freeTxBuffers.empty()) {
      reapTxBuffers();
    }

    BufferDescriptor* buf = freeTxBuffers.back();
    freeTxBuffers.pop_back();

    return buf;
  }

  /// Returns a transmit buffer to the transport layer. There should not
  /// be any pending work request on the buffer when this method is called.
  ///
  /// \param buf
  ///    Pointer to the buffer to be returned/recycled.
  void recycleTxBuffer(BufferDescriptor* buf) {
    freeTxBuffers.push_back(buf);
  }

  /// Sends a transmit buffer out the network. Once the transmission completes,
  /// a completion will be generated on the transmit completion queue.
  ///
  /// \param connId
  ///    RDMA connection identifier over which the transmission should occur.
  /// \param txBuffer
  ///    Transmit buffer containing data to be sent out the network.
  /// \param bytes
  ///    The number of bytes to send out the network.
  ///
  /// \throw
  ///    std::runtime_error if the transmission failed.
  void send(rdma_cm_id* connId, BufferDescriptor* txBuffer, uint32_t bytes) {
    // Scatter gather entry with the buffer containing the data to
    // be transmitted.
    struct ibv_sge sge = {
      .addr = (uint64_t) txBuffer->basePtr,
      .length = bytes,
      .lkey = txBuffer->mr->lkey,
    };

    // Work request to be posted to the NIC. The work request is configured
    // to create a completion on the transmit completion queue.
    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(ibv_send_wr));
    wr.wr_id = reinterpret_cast<uint64_t>(txBuffer);
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr* badWr;
    int err = ibv_post_send(connId->qp, &wr, &badWr);
    if (err != 0) {
      logMessage(Lvl::ERR,
                 "Failed to send %lu bytes on Infrc qp %lu (errno %d)",
                 bytes, connId->qp->qp_num, err);
      throw std::runtime_error("Error when trying to send over Infrc.");
    }
  }

  /// Returns the maximum number of bytes a buffer can hold.
  static constexpr uint32_t bufferSize() {
    return NIC_BUFFER_SIZE;
  }

 private:
  /// The maximum size of a shared queue (completion, receive, and transmit).
  static const uint32_t MAX_SHARED_QUEUE_DEPTH = 256 - 1;

  /// The maximum number of scatter-gather entries that can be posted to a
  /// send/receive queue in a single work request.
  static const uint32_t MAX_SHARED_QUEUE_SGE = 1;

  /// The size of each buffer registered with the NIC (both tx and rx).
  static const uint32_t NIC_BUFFER_SIZE = 128 * 1024;

  /// Protection domain that all allocated resources belong to.
  struct ibv_pd* pd;

  /// Completion queue used for RPCs incoming to a server. Shared across
  /// all connections.
  struct ibv_cq* serverRxCq;

  /// Completion queue used for responses to all outgoing RPCs. Shared
  /// across all connections.
  struct ibv_cq* clientRxCq;

  /// Completion queue used to send RPCs to a server. Shared across all
  /// connections.
  struct ibv_cq* commonTxCq;

  /// A shared receive queue. All server/passive queue-pairs created by
  /// this class will share this receive queue.
  struct ibv_srq* serverSrq;

  /// A shared receive queue. All client/active queue-pairs created by
  /// this class will share this receive queue.
  struct ibv_srq* clientSrq;

  /// A map of incoming connections. When we receive an RPC request, lookup
  /// the queue-pair number inside the map to identify the connection.
  std::unordered_map<uint32_t, Connection> incomingConns;

  /// A map of outgoing connections. When we receive an RPC response, lookup
  /// the queue-pair number inside the map to identify the connection.
  std::unordered_map<uint32_t, Connection> outgoingConns;

  /// A list of buffers that can be used for receive requests. These
  /// buffers are pre-registered with the NIC on construction.
  std::vector<BufferDescriptor> rxBuffers;

  /// A list of buffers that can be used for send requests. These
  /// buffers are pre-registered with the NIC on construction.
  std::vector<BufferDescriptor> txBuffers;

  /// A list of free transmit buffers that can be used to send RPCs to
  /// a server and send responses to a client.
  std::vector<BufferDescriptor*> freeTxBuffers;

  /// ConnectHandler needs to be able to create queue pairs that use the
  /// above shared resources.
  friend class ConnectHandler;

  /// Poller needs to be able to receive RPC requests and responses.
  friend class Poller;
};

} // end namespace transport
