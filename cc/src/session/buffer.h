// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

namespace session {

using namespace SoFASTER;
using namespace FASTER::core;

class SessionBuffer {
 public:
  /// A buffer for requests before they are sent out the network. The buffer
  /// can hold `totalSize` bytes and will allow transmits upto `batchSize`
  /// bytes at a time.
  SessionBuffer(uint32_t batchSize,
                uint32_t totalSize)
   : bufferSize(totalSize)
   , buffer(nullptr)
   , head(0)
   , tail(0)
   , transmitted(0)
   , maxTxBytes(batchSize)
  {
    // Allocate and zero out the circular buffer of requests.
    buffer = new uint8_t [bufferSize];
    std::memset(buffer, 0, bufferSize);
  }

  /// Destructor for SessionBuffer. Frees up the circular buffer.
  ~SessionBuffer() {
    if (buffer) delete[] buffer;
  }

  /// Disallow copy and copy-assign constructors.
  SessionBuffer(const SessionBuffer& from) = delete;
  SessionBuffer& operator=(const SessionBuffer& from) = delete;

  /// Move constructor.
  SessionBuffer(SessionBuffer&& from)
   : bufferSize(from.bufferSize)
   , buffer(from.buffer)
   , head(from.head)
   , tail(from.tail)
   , transmitted(from.transmitted)
   , maxTxBytes(from.maxTxBytes)
  {
    from.buffer = nullptr;
  }

  /// Buffers a request (Read/Upsert/Rmw) inside the session's buffer. Returns
  /// true if this was successful, false if the buffer is currently full.
  bool bufferRequest(uint8_t* request, uint32_t size)
  {
    // Adding this request to the buffer would result in an overflow. Indicate
    // to the client that it must flush pending requests and make progress on
    // any in-progress RPCs.
    if (tail - head + size > bufferSize) return false;

    // Write in the request and return to the caller, incrementing the tail.
    memcpy(getPointer(tail), request, size);
    tail += size;
    return true;
  }

  /// Returns a tuple consisting of a pointer and number of bytes that can
  /// be transmitted out the network.
  inline std::tuple<uint8_t*, uint64_t> getTx() {
    // First, we check if the tail has wrapped around but transmitted hasn't.
    // In this case, we only transmit upto the end of the buffer so that the
    // caller can safely memcpy a contiguous region into the tx buffer.
    auto size = tail - transmitted;
    if (tail % bufferSize < transmitted % bufferSize) {
      size = bufferSize - (transmitted % bufferSize);
    }

    // Next, we make sure that transmits are limited to `maxTxBytes`.
    size = std::min(size, maxTxBytes);

    return std::make_tuple(getPointer(transmitted), size);
  }

  /// Advances the transmitted offset by a given number of bytes. Typically
  /// called once data to be transmitted has been copied into a tx buffer.
  inline void advanceTx(uint64_t bytes) {
    transmitted += bytes;
  }

  /// Advances the head by a given number of bytes. Called once an RPC has
  /// been successfully acked by a server.
  inline void advanceHead(uint64_t bytes) {
    head += bytes;
  }

  /// Sets the transmitted address to the last address that was acked by
  /// the server. Used during a view change when requests need to be
  /// re-issued because of changes in ownership mappings.
  inline void resetTx() {
    transmitted = head;
  }

  /// Returns the logical address of this buffer's tail. Again, typically
  /// invoked when handling a view change on a session. The return value
  /// tells us upto what logical address we need to re-shuffle and re-isssue
  /// operations to the SoFASTER cluster.
  inline uint64_t getTailAddr() {
    return tail;
  }

  /// Returns the logical address of this buffer's transmitted address. Again,
  /// typically invoked when handling a view change on a session. The return
  /// value tells us upto what logical address we need to re-shuffle and
  /// re-isssue operations to the SoFASTER cluster.
  inline uint64_t getTxAddr() {
    return transmitted;
  }

  /// Given a logical address, returns a pointer within the circular buffer.
  inline uint8_t* getPointer(uint64_t addr) {
    return &(buffer[addr % bufferSize]);
  }

 private:
  /// The total size of the in-memory buffer in bytes.
  const uint32_t bufferSize;

  /// The in-memory circular buffer of pages.
  uint8_t* buffer;

  /// Logical address (in bytes) lesser than which requests have been acked
  /// by the server. Required during view changes.
  uint64_t head;

  /// Logical address (in bytes) of the next request that will be buffered.
  uint64_t tail;

  /// Logical address (in bytes) less than which requests have been transmitted
  /// out the network to the server. Required to determine from where in the
  /// buffer to transmit next.
  uint64_t transmitted;

  /// The maximum number of bytes that will be transmitted from this buffer
  /// in a single tx operation.
  uint64_t maxTxBytes;
};

} // end of namespace session
