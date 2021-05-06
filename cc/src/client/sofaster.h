// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <map>
#include <utility>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "core/async.h"
#include "core/thread.h"
#include "core/status.h"
#include "core/key_hash.h"
#include "common/log.h"
#include "session/client.h"
#include "session/buffer.h"
#include "network/wireformat.h"

namespace SoFASTER {

using namespace wireformat;
using namespace FASTER::core;

using namespace std::chrono;

/// A counter of the number of operations that have gone pending on this
/// thread. Required primarily for stats and graphs.
thread_local uint64_t pending[8];
thread_local uint64_t pendingT[8];

/// A flag indicating whether we should measure latency on this thread.
thread_local bool latency = false;

/// Vector holding timestamps to make latency measurements.
thread_local std::vector<time_point<high_resolution_clock>> startTs;
thread_local std::vector<time_point<high_resolution_clock>> finisTs;

/// Sofaster takes the following arguments:
///    K: Defines the type on the key of records indexed by SoFASTER. Required
///       to correctly pack requests into RPCs.
///    V: Defines the type on the value of records indexed by SoFASTER. Required
///       to correctly pack requests into RPCs.
///    T: The type on the transport layer that will be used to communicate with
///       Sofaster instances.
template<class K, class V, class T>
class Sofaster {
 public:
  /// Defines the type on the transport layer that will be used to send and
  /// receive requests and responses over the network.
  typedef T transport_t;

  /// Defines the type on the connection exposed by the transport layer.
  typedef typename T::Connection connection_t;

  /// Defines the type on the connection handler that will be used to create
  /// sessions to SoFASTER instances.
  typedef typename T::ConnectHandler conn_handle_t;

  /// Defines the type on the response poller that will be used to check for
  /// responses to outgoing RPCs. Used internally by the session layer.
  typedef typename T::Poller resp_poller_t;

  /// Defines the type on the session that requests will be sent out on.
  typedef session::ClientSession<transport_t> session_t;

  /// Defines the type on the key used by SoFASTER to index records.
  typedef K key_t;

  /// Type on the hash of a key for convenience.
  typedef FASTER::core::KeyHash hash_t;

  /// Defines the type on the values indexed by SoFASTER.
  typedef V val_t;

  /// Defines the type on read, upsert, and rmw requests that goes on an RPC.
  typedef ReadRequest<K, V> read_t;
  typedef UpsertRequest<K, V> upsert_t;
  typedef RmwRequest<K, V> rmw_t;

  /// Constructs a client-side instance of Sofaster. This can be used to send
  /// requests to a cluster of SoFASTER instances.
  Sofaster(uint64_t maxInProgress, uint64_t id, uint32_t transmitBytes)
   : transport(nullptr)
   , connHandle()
   , respPoller()
   , sessions()
   , sessionMap()
   , pipelineDepth(maxInProgress)
   , id(id)
   , transmitBytes(transmitBytes)
  {
    std::string lIP("");
    transport = new transport_t();
    connHandle = new conn_handle_t(0, lIP, transport);
    respPoller = new resp_poller_t(transport);

    if (latency) {
      startTs.reserve(1000 * 1000 * 3);
      finisTs.reserve(1000 * 1000 * 3);
    }
  }

  /// Destroys a client-side instance of Sofaster.
  ~Sofaster() {
    if (transport) {
      // Flush out all remaining requests enqueued on all sessions.
      clearSessions();

      delete respPoller;
      delete connHandle;
      delete transport;

      if (latency) {
        std::vector<high_resolution_clock::duration> latencies;
        for (auto i = 0; i < startTs.size(); i++) {
          latencies.push_back(finisTs[i] - startTs[i]);
        }

        std::sort(latencies.begin(), latencies.end());

        logMessage(Lvl::INFO, "Median Latency: %.2f microseconds",
                   ((double) latencies[latencies.size() / 2].count()) / 1000);
      }
    }
  }

  /// Disallow copy and copy-assign constructors.
  Sofaster(const Sofaster& from) = delete;
  Sofaster& operator=(const Sofaster& from) = delete;

  /// Move constructor for Sofaster.
  Sofaster(Sofaster&& from)
   : transport(from.transport)
   , connHandle(from.connHandle)
   , respPoller(from.respPoller)
   , sessions(std::move(from.sessions))
   , sessionMap(std::move(from.sessionMap))
   , pipelineDepth(from.pipelineDepth)
   , id(from.id)
   , transmitBytes(from.transmitBytes)
  {
    from.transport = nullptr;
  }

  /// Creates a session to a server that owns a particular hash range.
  ///
  /// \param first
  ///    The first hash in the range owned by the server.
  /// \param last
  ///    The last hash in the range owned by the server.
  /// \param ip
  ///    The server's IP address. Required so that a session can be
  ///    opened to it.
  /// \param port
  ///    The port on which the server is listening for incoming sessions.
  inline void addHashRange(uint64_t first, uint64_t last,
                          std::string ip, std::string port,
                          int index=-1)
  {
    if (index < 0) {
      // For us to be able to buffer and send out requests from a
      // session, we currently need these requests to be of same size.
      static_assert(sizeof(rmw_t) == sizeof(read_t), "RPC sizes don't match");
      static_assert(sizeof(rmw_t) == sizeof(upsert_t), "RPC sizes don't match");
      auto batch =
        ((transmitBytes - sizeof(RequestBatchHdr)) / sizeof(rmw_t)) *
        sizeof(rmw_t);

      sessions.emplace_back(*connHandle, ip, port, respPoller, pipelineDepth,
                            batch);
      sessions.back().open(sendOpen, recvOpen);
      index = sessions.size() - 1;
    }

    sessionMap.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(hash_t(first), hash_t(last)),
      std::forward_as_tuple(index)
      );
  }

  /// Adds and opens a session to a SoFASTER server. This session will not be
  /// indexed by the library's metadata map.
  inline void addSession(std::string ip, std::string port) {
    // For us to be able to buffer and send out requests from a
    // session, we currently need these requests to be of same size.
    static_assert(sizeof(rmw_t) == sizeof(read_t), "RPC sizes don't match");
    static_assert(sizeof(rmw_t) == sizeof(upsert_t), "RPC sizes don't match");
    auto batch =
      ((transmitBytes - sizeof(RequestBatchHdr)) / sizeof(rmw_t)) *
      sizeof(rmw_t);

    sessions.emplace_back(*connHandle, ip, port, respPoller, pipelineDepth,
                          batch);
    sessions.back().open(sendOpen, recvOpen);
  }

  /// Returns the IP address of the server that owns a particular key.
  inline std::string& getOwner(K& key) {
    return getSession(key).serverIP;
  }

  /// Given a reference to a key, returns a reference to a session. This session
  /// can be then used to send requests (reads, upserts, rmws) to a SoFASTER
  /// instance.
  inline session_t& getSession(K& key) {
    auto hash = key.GetHash();
    return sessions[sessionMap.at(HashRange(hash, hash))];
  };

  /// A context containing all state required to issue a read to a SoFASTER
  /// instance, and complete the read once a response has been received.
  class ReadContext : public IAsyncContext {
   public:
    /// Constructs a ReadContext given a reference to a key.
    ReadContext(key_t& key)
     : key_(key)
     , value_()
    {}

    /// Destroys a ReadContext.
    ~ReadContext() {}

    /// Returns a reference to an underlying key.
    key_t& key() {
      return key_;
    }

    /// Copies a passed in reference into the underlying value.
    void setValue(val_t& value) {
      value_ = value;
    }

    /// Returns a reference to the underlying value.
    val_t& val() {
      return value_;
    }

   protected:
    /// Copies this context into a passed in pointer.
    FASTER::core::Status DeepCopy_Internal(IAsyncContext*& copy) {
      return IAsyncContext::DeepCopy_Internal(*this, copy);
    }

   private:
    /// The key to be looked up at the SoFASTER instance.
    key_t key_;

    /// The value that the key maps to. Set once a response is received from
    /// the SoFASTER instance.
    val_t value_;
  };

  /// Enqueues a read request onto a passed in session.
  ///
  /// \param ctxt
  ///    Context for the read. Will be passed in as an argument to the callback
  ///    once a response to the read is received.
  /// \param cb
  ///    Callback function to be invoked once a response to the request is
  ///    received from SoFASTER.
  inline void read(ReadContext& ctxt, AsyncCallback cb)
  {
    // Enqueue a context and callback. The callback will be invoked with the
    // context as an argument when a response to this request is received.
    session_t& session = getSession(ctxt.key());
    session.enqueueContext(ctxt, cb);

    // Buffer the request onto the correct session and try to transmit.
    auto req = read_t(ctxt.key());
    bool retry = true;
    while (retry) {
      retry = !session.buffer.bufferRequest(reinterpret_cast<uint8_t*>(&req),
                                            sizeof(read_t));
      if (!flushSession(session)) handleViewChange(ctxt.key());
    }
  }

  /// A context containing all state required to issue an upsert to a SoFASTER
  /// instance, and complete the upsert once a response has been received.
  class UpsertContext : public IAsyncContext {
   public:
    /// Constructs an Upsert context from references to a key and value.
    UpsertContext(key_t& key, val_t& value)
     : key_(key)
     , val_(value)
    {}

    /// Destroys an UpsertContext.
    ~UpsertContext() {}

    /// Returns a reference to the underlying key.
    key_t& key() {
      return key_;
    }

    /// Returns a reference to the underlying value.
    val_t& val() {
      return val_;
    }

   protected:
    /// Copies this context into a passed in pointer.
    FASTER::core::Status DeepCopy_Internal(IAsyncContext*& copy) {
      return IAsyncContext::DeepCopy_Internal(*this, copy);
    }

   private:
    /// The key for the record that must be written/updated into SoFASTER.
    key_t key_;

    /// The value for the record that must be written/updated into SoFASTER.
    val_t val_;
  };

  /// Enqueues an upsert request onto a passed in session.
  ///
  /// \param ctxt
  ///    Context for the upsert. Will be passed in as an argument to the
  ///    callback once a response to the upsert is received.
  /// \param cb
  ///    Callback function to be invoked once a response to the request is
  ///    received from SoFASTER.
  inline void upsert(UpsertContext& ctxt, AsyncCallback cb)
  {
    // Enqueue a context and callback. The callback will be invoked with the
    // context as an argument when a response to this request is received.
    session_t& session = getSession(ctxt.key());
    session.enqueueContext(ctxt, cb);

    // Buffer the request on the session and try to transmit.
    auto req = upsert_t(ctxt.key(), ctxt.val());
    bool retry = true;
    while (retry) {
      retry = !session.buffer.bufferRequest(reinterpret_cast<uint8_t*>(&req),
                                            sizeof(upsert_t));
      if (!flushSession(session)) handleViewChange(ctxt.key());
    }
  }

  /// A context containing all state required to issue an rmw to a SoFASTER
  /// instance, and complete the rmw once a response has been received.
  class RmwContext : public IAsyncContext {
   public:
    /// Constructs an Rmw context from references to a key and modifier.
    RmwContext(key_t& key, val_t& mod)
     : key_(key)
     , mod_(mod)
    {}

    /// Destroys an RMW context.
    ~RmwContext() {}

    /// Returns a reference to the underlying key.
    key_t& key() {
      return key_;
    }

    /// Returns a reference to the underlying modifier.
    val_t& mod() {
      return mod_;
    }

   protected:
    /// Copies this context into a passed in pointer.
    FASTER::core::Status DeepCopy_Internal(IAsyncContext*& copy) {
      return IAsyncContext::DeepCopy_Internal(*this, copy);
    }

   private:
    /// The key for the record that the RMW must be issued against.
    key_t key_;

    /// The value by which the record should be modified. If the record
    /// does not exist, it will be initialized with this value.
    val_t mod_;
  };

  /// Enqueues an rmw request onto a passed in session.
  ///
  /// \param ctxt
  ///    Context for the rmw. Will be passed in as an argument to the
  ///    callback once a response to the rmw is received.
  /// \param cb
  ///    Callback function to be invoked once a response to the request is
  ///    received from SoFASTER.
  inline void rmw(RmwContext& ctxt, AsyncCallback cb) {
    // Enqueue a context and callback. The callback will be invoked with the
    // context as an argument when a response to this request is received.
    session_t& session = getSession(ctxt.key());
    session.enqueueContext(ctxt, cb);

    // Buffer the request on the session and try to transmit.
    auto req = rmw_t(ctxt.key(), ctxt.mod());
    bool retry = true;
    while (retry) {
      retry = !session.buffer.bufferRequest(reinterpret_cast<uint8_t*>(&req),
                                            sizeof(rmw_t));
      if (!flushSession(session)) handleViewChange(ctxt.key());
    }
  }

  /// Sends out requests buffered on the passed in session. Returns false
  /// if we encountered a view change on the session.
  inline bool flushSession(session_t& session, bool lbound=true) {
    // First, check if we need to back-off for a while on this session. If
    // we do, then return so that other sessions aren't blocked by this one.
    if (session.status == session::Status::BACK_OFF) {
      if (high_resolution_clock::now() < session.backOffUntil) return true;
      session.status = session::Status::OPEN;
    }

    // If the session's pipeline is full, then return without sending out more.
    if (session.inProgress == session.maxInFlight) {
      recvSession(session);
      return true;
    }

    // If there's been a view change on the session, then handle it first,
    // and continue trying to flush out requests buffered on the session.
    if (session.status == session::Status::VIEW_CHANGED) {
      if (id == 1) logMessage(Lvl::DEBUG, "Received view change on session %s",
                              session.sessionUID.ToString().c_str());
      return false;
    }

    // Back-off if necessary. Don't send out any requests, but allow other
    // sessions to proceed forward with sending requests.
    if (session.status == session::Status::BACK_OFF) return true;

    // Issue an RPC only if we have enough requests within our session buffer.
    auto batch =
      ((transmitBytes - sizeof(RequestBatchHdr)) / sizeof(rmw_t)) *
      sizeof(rmw_t);
    if (lbound && std::get<1>(session.buffer.getTx()) < batch) return true;

    session.send(sendFn, &session);

    // If we reached here, then there was no change on the status of the session
    // during the flush. As a result, we can safely return true here.
    return true;
  }

 private:
  /// Tries to receive responses on the passed in session.
  inline void recvSession(session_t& session, bool wait=false) {
    // Try receiving on the session. If not waiting, then do so once, else,
    // do so until all responses have been received.
    do {
      session.tryReceive(recvFn, &session);
    } while (wait && session.inProgress);
  }

  /// Handles a view change on a session. Takes care of opening a new session
  /// and rearranging buffers on the existing session.
  inline void handleViewChange(key_t& key) {
    // Retrieve the index of the session inside the sessions list.
    auto hash = key.GetHash();
    auto sIdx = sessionMap.at(HashRange(hash, hash));

    // Next, set the source session to open again. Buffered requests will
    // now be sent out in the new view, and responses will be processed.
    session_t& session = sessions[sIdx];
    if (id == 1) logMessage(Lvl::DEBUG, "Finished view change on session %s",
                            session.sessionUID.ToString().c_str());
    session.status = session::Status::OPEN;
  }

 public:
  /// Flushes every session and blocks until all in-flight and buffered
  /// requests have completed on all of them.
  inline void clearSessions() {
    for (auto it = sessions.begin(); it != sessions.end(); it++) {
      while (std::get<1>(it->buffer.getTx()) != 0) {
        flushSession(*it, false);
        recvSession(*it, true);
      }
    }
  }

  /// This function is passed in to the sessions layer when trying to open a
  /// newly constructed session. It writes the appropriate header into the
  /// passed in request buffer.
  ///
  /// \param cookie
  ///    Opaque parameter. Cast by this function into a session pointer.
  /// \param rpcBuffer
  ///    Pointer to request buffer that will be sent out the network once this
  ///    method returns.
  ///
  /// \return
  ///    The number of bytes to be sent out the network.
  inline static uint32_t sendOpen(void* cookie, uint8_t* rpcBuffer) {
    // Retrieve the session from the passed in opaque (oatmeal raisin?) cookie.
    auto session = reinterpret_cast<session_t*>(cookie);

    // First, setup the header that will go on the RPC.
    auto hdr = reinterpret_cast<RequestBatchHdr*>(rpcBuffer);
    hdr->view = session->currentView;
    hdr->size = sizeof(OpenRequest);

    // Setup the open-session request header.
    rpcBuffer += sizeof(RequestBatchHdr);
    auto req = reinterpret_cast<OpenRequest*>(rpcBuffer);
    req->op = Opcode::OPEN_SESSION;

    return sizeof(RequestBatchHdr) + hdr->size;
  }

  /// This function is passed in to the sessions layer when trying to open a
  /// newly constructed session. It parses the server's response to the request,
  /// and sets up the session appropriately.
  ///
  /// \param cookie
  ///    Opaque parameter. Cast by this function into a session pointer.
  /// \param request
  ///    Pointer to original request buffer containing the open session request.
  /// \param response
  ///    Pointer to buffer containing the server's response.
  ///
  /// \return
  ///    0. No real significance.
  inline static uint32_t recvOpen(void* cookie, uint8_t* request,
                                  uint8_t* response)
  {
    // Retrieve the session from the passed in opaque (snickerdoodle?) cookie.
    auto session = reinterpret_cast<session_t*>(cookie);

    // Look at the response header. If the request batch was not successful, or
    // if the response batch was malformed, do nothing.
    auto hdr = reinterpret_cast<ResponseBatchHdr*>(response);
    if (hdr->status != Status::SUCCESS || hdr->size != sizeof(OpenResponse)) {
      return 0;
    }

    response += sizeof(ResponseBatchHdr);
    auto res = reinterpret_cast<OpenResponse*>(response);
    if (res->status != Status::SUCCESS) return 0;

    // Initialize the session with the contents of the server's response.
    session->init(res->view, res->sessionId);
    return 0;
  }

 private:
  /// This function is passed in to the sessions layer when it is time to send
  /// all buffered requests out the network. It copies all requests buffered
  /// inside a session into a passed in request buffer.
  ///
  /// \param cookie
  ///    Opaque parameter. Cast by this function into a session pointer.
  /// \param rpcBuffer
  ///    Pointer to request buffer that will be sent out the network once this
  ///    method returns.
  ///
  /// \return
  ///    The number of bytes to be sent out the network.
  inline static uint32_t sendFn(void* cookie, uint8_t* rpcBuffer) {
    // Retrieve the session from the passed in opaque (chocolate chip?) cookie.
    auto session = reinterpret_cast<session_t*>(cookie);

    // First, setup the header that will go on the RPC.
    auto hdr = reinterpret_cast<RequestBatchHdr*>(rpcBuffer);
    hdr->view = session->currentView;

    auto tx = session->buffer.getTx();
    hdr->size = std::get<1>(tx);

    // Next, copy all buffered requests into the RPC buffer.
    rpcBuffer += sizeof(RequestBatchHdr);
    memcpy(reinterpret_cast<void*>(rpcBuffer),
           reinterpret_cast<void*>(std::get<0>(tx)), hdr->size);
    session->buffer.advanceTx(hdr->size);

    if (latency) startTs.push_back(high_resolution_clock::now());

    return hdr->size + sizeof(RequestBatchHdr);
  }

  /// This method is passed into the sessions layer when trying to receive
  /// responses to requests that were previously sent out. If responses were
  /// received, then this function is invoked.
  ///
  /// \param cookie
  ///    Opaque parameter. Cast by this function into a session pointer.
  /// \param request
  ///    Pointer to original request buffer containing the requests for which
  ///    responses have been just received.
  /// \param response
  ///    Pointer to buffer containing responses.
  ///
  /// \return
  ///    0. No real significance.
  inline static uint32_t recvFn(void* cookie, uint8_t* request,
                                uint8_t* response)
  {
    // Retrieve the session from the passed in opaque (peanut butter?) cookie.
    auto session = reinterpret_cast<session_t*>(cookie);

    // The request has been processed fully and there is now on lesser in-flight
    // request to the server.
    assert(session->inProgress > 0);
    if (session->inProgress > 0) session->inProgress -= 1;

    // Retrieve the header on this set of responses. If they were executed
    // successfully, proceed with request processing.
    auto hdr = reinterpret_cast<ResponseBatchHdr*>(response);

    // View changed on the server. Set the session's state and then return.
    if (hdr->status == Status::VIEW_CHANGED) {
      if (hdr->view <= session->currentView) return 0;

      session->status = session::Status::VIEW_CHANGED;
      session->reIssueLSN = hdr->reIssueFromLSN;
      session->currentView = hdr->view;
      return 0;
    }

    // Server asked us to back-off; set the status on the session and process
    // this batch normally.
    if ((hdr->status == Status::RETRY_LATER) &&
        session->status != session::Status::BACK_OFF)
    {
      session->status = session::Status::BACK_OFF;
      session->backOffUntil = high_resolution_clock::now() + microseconds(100);
    }

    if (hdr->status != Status::SUCCESS && hdr->status != Status::RETRY_LATER) {
      logMessage(Lvl::ERR, "Request batch did not succeed on session %s",
                 session->sessionUID.ToString().c_str());
      throw std::runtime_error("Request batch did not succeed.");
    }

    auto bytes = hdr->size;
    response += sizeof(ResponseBatchHdr);

    // Iterate through each response. Based on the opcode, dispatch to the
    // appropriate handler.
    while (bytes) {
      switch (*response) {
      case static_cast<int>(Opcode::READ): {
        auto res = reinterpret_cast<ReadResponse<V>*>(response);
        handleRead(session, res);

        bytes -= sizeof(ReadResponse<V>);
        response += sizeof(ReadResponse<V>);
        break;
      }

      case static_cast<int>(Opcode::UPSERT): {
        auto res = reinterpret_cast<UpsertResponse*>(response);
        handleUpsert(session, res);

        bytes -= sizeof(UpsertResponse);
        response += sizeof(UpsertResponse);
        break;
      }

      case static_cast<int>(Opcode::RMW): {
        auto res = reinterpret_cast<RmwResponse*>(response);
        handleRmw(session, res);

        bytes -= sizeof(RmwResponse);
        response += sizeof(RmwResponse);
        break;
      }

      default:
        return 0;
      }
    }

    uint64_t reqBytes = reinterpret_cast<RequestBatchHdr*>(request)->size;
    session->buffer.advanceHead(reqBytes);

    if (latency) finisTs.push_back(high_resolution_clock::now());

    return 0;
  }

  /// Handles a response to a read request that was issued to SoFASTER.
  ///
  /// \param session
  ///    Pointer to the session the response was received on.
  /// \param response
  ///    Pointer to the read response.
  inline static void handleRead(session_t* session, ReadResponse<V>* response) {
    switch (response->status) {
    // The read request executed successfully at the server. Retrieve the value
    // from the response buffer, copy it into the request's context, and then
    // invoke the enqueued callback function with the context as an argument.
    case Status::SUCCESS: {
      auto ctx = session->contexts.front();

      auto state = reinterpret_cast<ReadContext*>(ctx.first);
      state->setValue(response->value);

      session->execEarliestCb(FASTER::core::Status::Ok);
      session->nResponses++;
      break;
    }

    // The server does not contain the key that was requested for. Just invoke
    // the earliest callback with the appropriate status.
    case Status::INVALID_KEY:
      session->execEarliestCb(FASTER::core::Status::NotFound);
      session->nResponses++;
      break;

    // The operation went pending at the server. Move the context to the
    // pending queue.
    case Status::PENDING:
      pending[0]++;
      pendingT[0]++;
      session->moveEarliestToPending(response->pendingId);
      break;

    // An operation that went pending earlier completed successfully. Retrieve
    // the value and copy it into the appropriate context, and then invoke the
    // callback to complete the request.
    case Status::PENDING_SUCCESS: {
      pending[0]--;
      auto ctx = session->pendingContexts[response->pendingId];

      auto state = reinterpret_cast<ReadContext*>(ctx.first);
      state->setValue(response->value);

      session->execPendingCb(response->pendingId,
                             FASTER::core::Status::Pending);
      session->nResponses++;
      break;
    }

    // An operation went pending, but the key was never found. Invoke the
    // callback with the appropriate status.
    case Status::PENDING_INVALID_KEY:
      pending[0]--;
      session->execPendingCb(response->pendingId,
                             FASTER::core::Status::NotFound);
      session->nResponses++;
      break;

    default:
      logMessage(Lvl::ERR, "Read failed on session %s with unexpected status",
                 session->sessionUID.ToString().c_str());
      assert(false);
      break;
    }
  }

  /// Handles a response to an upsert request that was issued to SoFASTER.
  ///
  /// \param session
  ///    Pointer to the session the response was received on.
  /// \param response
  ///    Pointer to the upsert response.
  inline static void handleUpsert(session_t* session, UpsertResponse* response)
  {
    switch (response->status) {
    // The upsert request executed successfully at the server. Retrieve the
    // requests context and callback from the session. Invoke the callback with
    // the context passed in as an argument.
    case Status::SUCCESS:
      session->execEarliestCb(FASTER::core::Status::Ok);
      session->nResponses++;
      break;

    // The operation went pending at the server. Move the context to the
    // pending queue.
    case Status::PENDING:
      pending[0]++;
      pendingT[0]++;
      session->moveEarliestToPending(response->pendingId);
      break;

    // An operation that went pending at the server completed successfully.
    // Execute the appropriate callback and return.
    case Status::PENDING_SUCCESS:
      pending[0]--;
      session->execPendingCb(response->pendingId,
                             FASTER::core::Status::Pending);
      session->nResponses++;
      break;

    default:
      logMessage(Lvl::ERR, "Upsert failed on session %s, unexpected status",
                 session->sessionUID.ToString().c_str());
      assert(false);
      break;
    }
  }

  /// Handles a response to an rmw request that was issued to SoFASTER.
  ///
  /// \param session
  ///    Pointer to the session the response was received on.
  /// \param response
  ///    Pointer to the rmw response.
  inline static void handleRmw(session_t* session, RmwResponse* response)
  {
    switch (response->status) {
    // The rmw request executed successfully at the server. Retrieve the
    // requests context and callback from the session. Invoke the callback with
    // the context passed in as an argument.
    case Status::SUCCESS:
      session->execEarliestCb(FASTER::core::Status::Ok);
      session->nResponses++;
      break;

    // The operation went pending at the server. Move the context to the
    // pending queue.
    case Status::PENDING:
      pending[0]++;
      pendingT[0]++;
      session->moveEarliestToPending(response->pendingId);
      break;

    // An operation that went pending at the server completed successfully.
    // Execute the appropriate callback and return.
    case Status::PENDING_SUCCESS:
      pending[0]--;
      session->execPendingCb(response->pendingId,
                             FASTER::core::Status::Pending);
      session->nResponses++;
      break;

    default:
      logMessage(Lvl::ERR, "Rmw failed on session %s with unexpected status",
                 session->sessionUID.ToString().c_str());
      assert(false);
      break;
    }
  }

  /// The underlying transport layer that requests will be sent out and
  /// responses will be received on.
  transport_t* transport;

  /// A connection handle exposed by the transport layer. This will be used
  /// to open connections to SoFASTER server threads.
  conn_handle_t* connHandle;

  /// A response poller exposed by the transport layer. This will be used to
  /// check for responses to RPCs sent out on a session.
  resp_poller_t* respPoller;

 public:
  /// Defines a range of hashes owned by a particular SoFASTER instance.
  struct HashRange {
    /// The lower bound on the hash range (inclusive).
    hash_t lHash;

    /// The upper bound on the hash range (exclusive).
    hash_t rHash;

    HashRange(hash_t left, hash_t right)
     : lHash(left)
     , rHash(right)
    {}
  };

  /// Compares two hash ranges. Required by std::map to determine an ordering
  /// between the two hash ranges.
  struct CompareRanges {
    bool operator() (const HashRange& left, const HashRange& right) const {
      // Need to make a non-const copy to get this to work with KeyHash.
      HashRange left_ = left;

      // If the upper bound on the first range is lesser than the lower bound
      // on the second range, then the first range is smaller.
      if (left_.rHash < right.lHash) return true;

      // If the lower bound on the first range is greater than the upper bound
      // on the second range, then the first range is greater.
      if (left_.lHash >= right.rHash) return false;

      // XXX: For now, this return ensures that lookups work.
      return false;
    }
  };

  /// The list of sessions created by this client thread.
  std::vector<session_t> sessions;

 private:
  /// Maps a hash range to a session that can accept requests on keys belonging
  /// to the hash range. The mapped value is an index into the vector above.
  std::map<HashRange, size_t, CompareRanges> sessionMap;

  /// The maximum number of in-flight RPCs on every session created.
  uint64_t pipelineDepth;

  /// Identifier for this instance of the library.
  uint64_t id;

  /// Number of bytes worth of requests to pack into each RPC.
  uint32_t transmitBytes;
};

} // end of namespace SoFASTER.
