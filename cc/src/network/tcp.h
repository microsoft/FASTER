// Copyright (c) University of Utah. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <chrono>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>

#include "common.h"
#include "wireformat.h"

#include "common/log.h"

namespace transport {

using namespace std::chrono;
using namespace SoFASTER;
using namespace SoFASTER::wireformat;

/// This class is *NOT* thread-safe.
class TcpTransport {
 public:
  /// Constructs a TCP based transport layer. This layer mostly manages
  /// shared resources such as transmit and receive buffers. Other transport
  /// functionality is handled by Connection and ConnectHandler.
  TcpTransport()
   : incomingConns()
   , outgoingConns()
   , txBuffers()
   , freeTxBuffers()
  {
    // Allocate all shared buffers on construction.
    for (auto i = 0; i < NUM_SHARED_BUFF; i++) {
      txBuffers.emplace_back((uint32_t) NIC_BUFFER_SIZE);
      freeTxBuffers.emplace_back(&txBuffers.back());
    }
  }

  /// Destroys the TCP layer and deallocates shared resources (if required).
  ~TcpTransport() {}

  /// Move constructor for TcpTransport.
  TcpTransport(TcpTransport&& from)
   : incomingConns(std::move(from.incomingConns))
   , outgoingConns(std::move(from.outgoingConns))
   , txBuffers(std::move(from.txBuffers))
   , freeTxBuffers(std::move(from.freeTxBuffers))
  {}

  /// Disallow copy and copy-assign constructors.
  TcpTransport(const TcpTransport& from) = delete;
  TcpTransport& operator=(const TcpTransport& from) = delete;

 private:
  /// This class represents a buffer that can be used to send and receive RPCs.
  class BufferDescriptor {
   public:
    /// Creates a BufferDescriptor. Allocates a 4KB aligned buffer.
    /// \param length
    ///    The size of the buffer in bytes.
    ///
    /// \throw
    ///    std::runtime_error if initialization failed.
    BufferDescriptor(uint32_t length)
     : buffer(nullptr)
     , size(length)
     , basePtr(nullptr)
    {
      // First allocate the buffer aligned to 4KB. If this fails, throw an
      // exception to the caller.
      int r = posix_memalign(&basePtr, 4096, length);
      if (r != 0) {
        logMessage(Lvl::ERROR, "Failed to allocate TCP buffer (errno %d)", r);
        throw std::runtime_error("Failed to allocate memory for TCP buffer");
      }

      buffer = static_cast<uint8_t*>(basePtr);
    }

    /// Destroys a BufferDescriptor. This method frees the underlying buffer
    /// if needed.
    ~BufferDescriptor() {
      // Do not free resources if allocation failed or if there was a move.
      if (!basePtr) return;
      free(basePtr);
    }

    /// Move constructor.
    BufferDescriptor(BufferDescriptor&& from) {
      buffer = std::move(from.buffer);
      size = std::move(from.size);
      basePtr = std::move(from.basePtr);

      from.basePtr = nullptr;
    }

    /// Disallow copy and copy-assign constructors.
    BufferDescriptor(const BufferDescriptor& from) = delete;
    BufferDescriptor& operator=(const BufferDescriptor& from) = delete;

    /// Pointer to the underlying buffer. This buffer is `size' bytes long.
    uint8_t* buffer;

    /// Size of the above buffer in bytes.
    uint32_t size;

    /// Same as `buffer', except that this is a void*.
    void* basePtr;
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

    /// Constructs a connection. This method assumes that a socket
    /// has already been estabilished with the other end.
    ///
    /// \param fd
    ///    Pre-estabilished TCP socket to the other end of the connection.
    /// \param server
    ///    Flag indicating whether this end of the connection is a server (true)
    ///    or client (false).
    /// \param transport
    ///    Pointer to tcp transport layer over which this connection was
    ///    established.
    Connection(int fd, bool server, TcpTransport* transport)
     : rx(TcpTransport::bufferSize())
     , serverTx(TcpTransport::bufferSize())
     , outgoingRpcs()
     , socket(fd)
     , server(server)
     , alivef(true)
     , transport(transport)
    {
      int flags = fcntl(fd, F_GETFL);
      if (flags == -1) {
        logMessage(Lvl::ERROR, "Failed to retrieve socket status (errno %d)",
                   errno);
        throw std::runtime_error("Error when trying to read socket status");
      }

      if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        logMessage(Lvl::ERROR, "Failed to make socket non-blocking (errno %d)",
                   errno);
        throw std::runtime_error("Error when making socket non-blocking");
      }
    }

    /// Destroys a connection.
    ~Connection() {
      // Return all sent transmit buffers to the transport layer.
      for (int i = 0; i < outgoingRpcs.size(); i++) {
        transport->recycleTxBuffer(outgoingRpcs[i]);
        outgoingRpcs[i] = nullptr;
      }

      // Close this socket.
      close(socket);
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
    ///    std::runtime_error if the send fails.
    void sendRpc(void* context, S cb) {
      // If the connection is not alive, then log an error message and return.
      if (!alivef) {
        logMessage(Lvl::ERROR, "Trying to send RPC on a dead TCP connection");
        throw std::runtime_error("Cannot send RPC on a dead TCP connection");
      }

      // First, retrieve a transmit buffer and invoke the supplied
      // sender function.
      BufferDescriptor* tx = transport->getTxBuffer();
      uint32_t size = cb(context, tx->buffer);

      // Send out the RPC, enqueue the transmit buffer on the connection.
      // The buffer will be released to the transport layer once a response
      // has been received and processed.
      transport->send(socket, tx->basePtr, size);
      outgoingRpcs.emplace_back(tx);
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
    void receive(void* context, R cb) {
      // If the connection is not alive, then log an error message and return.
      if (!alivef) {
        logMessage(Lvl::ERROR, "Trying to receive on a dead TCP connection");
        throw std::runtime_error("Can't recv on a dead TCP connection");
      }

      if (server) {
        recvServer(context, cb);
      } else {
        recvClient(context, cb);
      }
    }

    /// Marks the connection as dead. Typically invoked by the session once it
    /// has released its pointer to this connection.
    void tryMarkDead() {
      alivef = false;
    }

    /// Returns true if the connection is alive. Generally invoked by the
    /// sessions layer to check if this connection can still send or receive.
    /// Useful when it comes to implementing RPC timeouts.
    bool isAlive() {
      return alivef;
    }

    /// Returns true if the connection is dead. Generally invoked by the
    /// transport layer to check if this connection can be garbage collected.
    bool isDead() {
      return !alivef;
    }

   private:
    /// Receives, processes, and responds to incoming RPC requests at the
    /// server/passive end of a connection.
    ///
    /// Refer to receive() for parameter documentation.
    void recvServer(void* context, R cb) {
      // Since we're the server end of a connection, try to receive the
      // header on a batch of requests.
      void* buff = rx.basePtr;
      if (!transport->recv(socket, buff, sizeof(RequestBatchHdr))) return;

      // Based on the `size` field on the received header, receive the
      // full RPC request into the buffer.
      uint64_t bytes = reinterpret_cast<RequestBatchHdr*>(buff)->size;
      buff = reinterpret_cast<void*>(rx.buffer + sizeof(RequestBatchHdr));
      while (!transport->recv(socket, buff, bytes)) return;

      // Invoke the callback over the request and send out the response.
      uint32_t respB = cb(context, rx.buffer, serverTx.buffer);
      transport->send(socket, serverTx.basePtr, respB);
    }

    /// Receives and processes responses to outgoing RPC requests at the
    /// client/active end of a connection.
    ///
    /// Refer to receive() for parameter documentation.
    void recvClient(void* context, R cb) {
      // Since we're the client end of a connection, try to receive the
      // header on a batch of responses.
      void* buff = rx.basePtr;
      if (!transport->recv(socket, buff, sizeof(ResponseBatchHdr))) return;

      // Based on the `size` field on the received header, receive the
      // full RPC response into the buffer.
      uint64_t bytes = reinterpret_cast<ResponseBatchHdr*>(buff)->size;
      buff = reinterpret_cast<void*>(rx.buffer + sizeof(ResponseBatchHdr));
      while (!transport->recv(socket, buff, bytes)) return;

      // Retrieve the RPC request, invoke the callback and return the
      // original transmit buffer.
      auto tx = outgoingRpcs.front();
      cb(context, tx->buffer, rx.buffer);
      transport->recycleTxBuffer(&(*tx));
      outgoingRpcs.pop_front();
    }

    /// Buffer into which this connection will receive RPC requests (server)
    /// or responses (client).
    BufferDescriptor rx;

    /// Buffer that this connection will use to send RPC responses to
    /// a client. Used only when this is the server end of a connection.
    BufferDescriptor serverTx;

    /// List of outgoing RPCs on this connection. Required so that when we
    /// receive a response, we can send the original request along with the
    /// response to the sessions layer. This allows us to pipeline RPCs onto
    /// a single connection.
    std::deque<BufferDescriptor*> outgoingRpcs;

    /// Socket (file descriptor) that this connection will use to send and
    /// receive RPC requests and responses.
    int socket;

    /// Flag indicating whether this end is a server (true) or client (false).
    bool server;

    /// Flag indicating whether this connection is alive or dead.
    bool alivef;

    /// Pointer to transport layer this connection was created under. Required
    /// if this is the client end of a connection; transmit buffers will be
    /// picked up from the transport, allowing multiple RPCs to be pipelined
    /// on the connection.
    TcpTransport* transport;
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
    ///    Pointer to transport layer that will own all created connections.
    ///
    /// \throw
    ///    std::runtime_error if initialization failed.
    ConnectHandler(uint16_t lPort, std::string& lIP, TcpTransport* transport)
     : listenf(false)
     , listener(-1)
     , transport(transport)
     , interval(milliseconds(1))
     , next(high_resolution_clock::now())
    {
      // If lPort is 0, then this instance will not be able to listen for
      // incoming connections.
      if (lPort == 0) return;

      // Setup the listener so that we can receive and accept incoming
      // connections from clients.
      listenf = true;
      listener = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
      if (listener == -1) {
        auto err = std::string("Error when trying to create TCP listener on ") +
                   "port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERROR, err.c_str(), lPort, lIP.c_str(), errno);
        throw std::runtime_error("Error when creating TCP listener");
      }

      // The address to listen on for incoming connections.
      struct sockaddr_in in = {
        .sin_family = AF_INET,
        .sin_port = htons(lPort),
        .sin_addr = {
          .s_addr = inet_addr(lIP.c_str()),
        },
      };
      socklen_t len = sizeof(struct sockaddr_in);

      if (bind(listener, (struct sockaddr*)&in, len) == -1) {
        auto err = std::string("Error when trying to bind TCP listener to ") +
                   "port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERROR, err.c_str(), lPort, lIP.c_str(), errno);
        throw std::runtime_error("Error when binding TCP listener");
      }

      if (listen(listener, 8) == -1) {
        auto err = std::string("Error when marking socket as listener on ") +
                   "port %lu, IP %s (errno %d)";
        logMessage(Lvl::ERROR, err.c_str(), lPort, lIP.c_str(), errno);
        throw std::runtime_error("Error when marking socket as listener");
      }
    }

    /// Default constructor.
    ConnectHandler()
     : listenf(false)
     , listener(-1)
     , transport(nullptr)
     , interval(milliseconds(1))
     , next(high_resolution_clock::now())
    {}

    /// Destroys a ConnectHandler.
    ~ConnectHandler() {
      // Tear down the listener if configured.
      if (listenf) close(listener);
    }

    /// Disallow copy and copy-assign constructors.
    ConnectHandler(const ConnectHandler& from) = delete;
    ConnectHandler& operator=(const ConnectHandler& from) = delete;

    /// Move constructor.
    ConnectHandler(ConnectHandler&& from)
     : listenf(from.listenf)
     , listener(from.listener)
     , transport(from.transport)
     , interval(from.interval)
     , next(from.next)
    {
      from.listenf = false;
      from.listener = -1;
      from.transport = nullptr;
    }

    /// Move assign operator.
    ConnectHandler& operator=(ConnectHandler&& from) {
      listenf = from.listenf;
      from.listenf = false;

      listener = from.listener;
      from.listener = -1;

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
      if (!listenf) {
        logMessage(Lvl::ERROR,
                   "TCP listener not configured. Cannot poll for connections");
        throw std::runtime_error("TCP listener not configured");
      }

      // Check the current time, and determine whether we should poll for
      // a connection.
      auto curr = high_resolution_clock::now();
      if (curr < next) return nullptr;

      // We poll again after the next interval.
      next = curr + interval;

      int fd = accept(listener, nullptr, 0);
      if (fd == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) return nullptr;
      if (fd == -1) {
        logMessage(Lvl::ERROR,
                   "Failure when polling for new TCP connections (errno %d)",
                   errno);
        if (errno == EMFILE) {
          logMessage(Lvl::ERROR,
                     "Too many file descriptors open. Try setting ulimit");
        }
        throw std::runtime_error("Failed when polling for new TCP connections");
      }

      // Create and return a Connection object.
      transport->incomingConns.emplace(std::piecewise_construct,
                    std::forward_as_tuple(fd),
                    std::forward_as_tuple(fd, true, transport));

      return &(transport->incomingConns.at(fd));
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
    ///    std::runtime_error if something went wrong while connecting.
    Connection* connect(const std::string& ip, const std::string& port) {
      // Translate the server's address from the passed in arguments.
      struct addrinfo hints;
      memset(&hints, 0, sizeof(addrinfo));
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      struct addrinfo *server;
      auto ret = getaddrinfo(ip.c_str(), port.c_str(), &hints, &server);
      if (ret != 0) {
        auto msg = std::string("Failed to parse address for TCP ") +
                   "connection to port %s, IP %s: " + gai_strerror(ret);
        logMessage(Lvl::ERROR, msg.c_str(), port.c_str(), ip.c_str());
        throw std::runtime_error("Error when parsing server TCP address.");
      }

      // Open a Tcp socket and connect to the remote server. We don't mark the
      // socket as non-blocking yet because we'd like to synchronously perform
      // the TCP handshake with the server below.
      int fd = socket(AF_INET, SOCK_STREAM, 0);
      if (fd == -1) {
        auto err = std::string("Failed to create TCP socket to ") +
                   "port %s, IP %s (errno %d)";
        logMessage(Lvl::ERROR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Error when creating socket for connection.");
      }

      if (::connect(fd, server->ai_addr, server->ai_addrlen) == -1) {
        auto err = std::string("Failed to connect TCP socket to ") +
                   "port %s, IP %s (errno %d)";
        logMessage(Lvl::ERROR, err.c_str(), port.c_str(), ip.c_str(), errno);
        throw std::runtime_error("Error when connecting socket to server.");
      }

      // Create and return a new connection.
      transport->outgoingConns.emplace(std::piecewise_construct,
                      std::forward_as_tuple(fd),
                      std::forward_as_tuple(fd, false, transport));

      return &(transport->outgoingConns.at(fd));
    }

   private:
    /// Flag indicating if this class can listen for incoming connections.
    /// If false, then it cannot.
    bool listenf;

    /// Socket that will be used to listen for and accept incoming connections.
    int listener;

    /// Pointer to underlying transport that will own all created connections.
    TcpTransport* transport;

    /// Time interval at which we should poll for a new connection.
    milliseconds interval;

    /// The next point in time at which we should poll for a new connection.
    time_point<high_resolution_clock> next;
  };

  /// Since we're on kernel TCP, this class just periodically checks for and
  /// garbage collects any dead connections.
  class Poller {
   public:
    /// Constructs a Poller.
    ///
    /// \param transport
    ///    Pointer to transport layer that RPC requests and responses will
    ///    be received on.
    Poller(TcpTransport* transport)
     : transport(transport)
     , nextGC(GC_INTERVAL)
    {}

    /// Default constructor.
    Poller()
     : transport(nullptr)
     , nextGC(GC_INTERVAL)
    {}

    /// Destroys the Poller.
    ~Poller() {}

    /// Disallow copy and copy-assign constructors.
    Poller(const Poller& from) = delete;
    Poller& operator=(const Poller& from) = delete;

    /// Move constructor.
    Poller(Poller&& from)
     : transport(from.transport)
     , nextGC(from.nextGC)
    {
      from.transport = nullptr;
    }

    /// Move assign operator.
    Poller& operator=(Poller&& from) {
      transport = from.transport;
      nextGC = from.nextGC;
      from.transport = nullptr;
    }

    /// Periodically checks for and deletes dead connections.
    void poll() {
      // If we haven't reached the gc interval yet, then just return. Otherwise,
      // try to perform garbage collection.
      nextGC--;
      if (nextGC > 0) return;

      // Garbage collect all dead incoming connections.
      for (auto c = transport->incomingConns.begin();
            c != transport->incomingConns.end(); c++)
      {
        if (c->second.isDead()) transport->incomingConns.erase(c->first);
      }

      // Garbage collect all dead outgoing connections.
      for (auto c = transport->outgoingConns.begin();
            c != transport->outgoingConns.end(); c++)
      {
        if (c->second.isDead()) transport->outgoingConns.erase(c->first);
      }

      nextGC = GC_INTERVAL;
    }

   private:
    /// Polling frequency at which we will check for dead connections.
    static const uint64_t GC_INTERVAL = 1 * 1000 * 1000;

    /// Pointer to the underlying Tcp transport layer. Required so that
    /// we can garbage collect dead connections.
    TcpTransport* transport;

    /// Number of poll() iterations after which we should run one round
    /// of garbage collection on the tcp transport.
    uint64_t nextGC;
  };

  /// Returns a buffer that can be used to transmit an RPC.
  BufferDescriptor* getTxBuffer() {
    // If there aren't any free transmit buffers at the moment, then
    // create one on demand and return a pointer to the caller.
    if (freeTxBuffers.empty()) {
      txBuffers.emplace_back(TcpTransport::bufferSize());
      return &txBuffers.back();
    }

    BufferDescriptor* buf = freeTxBuffers.back();
    freeTxBuffers.pop_back();

    return buf;
  }

  /// Returns a transmit buffer (`buf`) back to the transport layer.
  void recycleTxBuffer(BufferDescriptor* buf) {
    freeTxBuffers.push_back(buf);
  }

  /// Sends a transmit buffer out the network.
  ///
  /// \param socket
  ///    Socket to send the data over.
  /// \param txBuffer
  ///    Transmit buffer containing data to be sent out the network.
  /// \param bytes
  ///    The number of bytes to send out the network.
  ///
  /// \throw
  ///    std::runtime_error if the transmission failed.
  void send(int socket, void* tx, uint32_t bytes) {
    // Since we're using TCP which happens to be stream based, we need
    // to keep invoking send() until all data has been transmitted.
    while (bytes > 0) {
      ssize_t r = ::send(socket, tx, bytes, MSG_NOSIGNAL);

      // Looks like the socket buffer in the kernel might be full; poll
      // until we can send on the socket once again.
      if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        struct pollfd p;
        p.fd = socket;
        p.events = POLLOUT;
        if (::poll(&p, 1, -1) == -1) {
          logMessage(Lvl::ERROR,
                     "Failed to poll socket (fd %d) on send (%lu B) (errno %d)",
                     socket, bytes, errno);
          throw std::runtime_error("Error when polling TCP socket for send");
        }
        continue;
      }

      // Looks like the transmit just failed. Just throw an exception.
      if (r < 0) {
        logMessage(Lvl::ERROR,
                   "Failed to send %lu B on TCP socket (fd %d) (errno %d)",
                   bytes, socket, errno);
        throw std::runtime_error("Error when sending on TCP socket");
      }

      bytes -= r;
      tx = reinterpret_cast<void*>(reinterpret_cast<uint8_t*>(tx) + r);
    }
  }

  /// Attempts to receive data over a socket. This method is non-blocking.
  ///
  /// \param socket
  ///    Socket to receive data over.
  /// \param rx
  ///    Receive buffer containing data to be received over the network.
  /// \param bytes
  ///    The number of bytes to be received over the socket.
  ///
  /// \return
  ///    True if data was received into the passed in buffer. False otherwise.
  ///
  /// \throw
  ///    std::runtime_error if the receive operation failed.
  bool recv(int socket, void* rx, uint32_t bytes) {
    // Keep calling recv() until we've either received all the bytes we
    // wanted, or there is no data to be received (in which case we
    // simply return false to the caller. The sync flag below helps us
    // determine whether we should continue polling the socket despite
    // not receiving data (true) or whether we should return (false).
    bool sync = false;
    while (bytes > 0) {
      ssize_t r = ::recv(socket, rx, bytes, 0);

      if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (sync) {
          continue;
        } else {
          return false;
        }
      }

      if (r == 0) throw std::runtime_error("Connection disconnected");

      if (r < 0) {
        logMessage(Lvl::ERROR,
                   "Failed to recv %lu B on TCP socket (fd %d) (errno %d)",
                   bytes, socket, errno);
        throw std::runtime_error("Error when receiving on TCP socket");
      }

      bytes -= r;
      rx = reinterpret_cast<void*>(reinterpret_cast<uint8_t*>(rx) + r);

      // We received some data but not all the data we were expecting. Set
      // the sync flag to true to ensure we receive everything before returning.
      if (bytes > 0 && !sync) sync = true;
    }

    return true;
  }

  /// Returns the maximum number of bytes a buffer can hold.
  static constexpr uint32_t bufferSize() {
    return NIC_BUFFER_SIZE;
  }

 private:
  /// The size of each tx and rx buffer.
  static const uint32_t NIC_BUFFER_SIZE = 128 * 1024;

  /// The number of buffers that will be shared by connections to transmit
  /// RPC requests to servers.
  static const uint32_t NUM_SHARED_BUFF = 256 - 1;

  /// A map of incoming connections. Owned by the transport layer. Required so
  /// that we can safely garbage collect a dead connection once the sessions
  /// layer has released it.
  std::unordered_map<int, Connection> incomingConns;

  /// A map of outgoing connections. Owned by the transport layer. Required so
  /// that we can safely garbage collect a dead connection once the sessions
  /// layer has released it.
  std::unordered_map<int, Connection> outgoingConns;

  /// A list of all transmit buffers that will be shared by connections
  /// created on this transport layer.
  std::vector<BufferDescriptor> txBuffers;

  /// A list of free transmit buffers that can be used by connections to send
  /// pipelined RPC requests to servers.
  std::vector<BufferDescriptor*> freeTxBuffers;
};

} // end namespace transport
