// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cmath>

#include "core/alloc.h"
#include "core/async.h"
#include "core/status.h"
#include "core/persistent_memory_malloc.h"

#include "common/log.h"

namespace session {

using namespace SoFASTER;
using namespace FASTER::core;

/// The following are template arguments.
///    - D: The type on the filesystem used for durability.
template<class D>
class PersistentSessionBuffer {
 public:
  /// Defines the type on the filesystem under which requests sent out on
  /// the session will be persisted.
  typedef D disk_t;

  /// For convenience. Defines the type on the durable file that buffered
  /// requests will be periodically flushed to.
  typedef typename D::file_t session_file_t;

  /// A buffer for requests before they are sent out the network. The buffer
  /// can hold `totalSize` bytes and will allow transmits upto `batchSize`
  /// bytes at a time.
  PersistentSessionBuffer(uint32_t batchSize,
                          uint32_t totalSize)
   : bufferSize(totalSize)
   , buffer(nullptr)
   , head(0)
   , tail(0)
   , transmitted(0)
   , durableFile()
   , disk()
   , maxTxBytes(batchSize)
  {
    // Allocate and zero out the circular buffer of requests.
    buffer = new uint8_t [bufferSize];
    std::memset(buffer, 0, bufferSize);
  }

  /// Destructor for PersistentSessionBuffer. Frees up the circular buffer.
  ~PersistentSessionBuffer() {
    if (!buffer) return;

    durableFile.Close();
    if (buffer) free(buffer);
  }

  /// Disallow copy and copy-assign constructors.
  PersistentSessionBuffer(const PersistentSessionBuffer& from) = delete;
  PersistentSessionBuffer& operator=(const PersistentSessionBuffer& from)
    = delete;

  /// Move constructor.
  PersistentSessionBuffer(PersistentSessionBuffer&& from)
   : bufferSize(from.bufferSize)
   , buffer(from.buffer)
   , head(from.head)
   , tail(from.tail)
   , transmitted(from.transmitted)
   , durableFile(std::move(from.durableFile))
   , disk(std::move(from.disk))
   , maxTxBytes(from.maxTxBytes)
  {
    from.buffer = nullptr;

    // Need to close and re-open file because the IO handler got moved
    // out when we moved out disk.
    durableFile.Close();
    durableFile.Open(&disk.handler());
  }

  /// Opens up the durable buffer. Need an explicit method since we don't have
  /// the session guid when the session is constructed.
  void open(std::string sessionUID) {
    // Allocate and open the file into which buffered requests will be flushed.
    durableFile = disk.NewFile(sessionUID);
    durableFile.Open(&disk.handler());
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
  /// Defines a context for asynchronously flushing a buffer page to disk.
  class FlushContext : public IAsyncContext {
   public:
    /// Returns a context that can be used to asynchronously flush a page
    /// from the circular buffer to disk.
    FlushContext() {}

    /// Empty destructor for now.
    ~FlushContext() {}

   protected:
    /// Required for when this operation goes asynchronous at the filesystem.
    /// Copies this context from the stack to the heap along with the parent.
    FASTER::core::Status DeepCopy_Internal(IAsyncContext*& copy) {
      return IAsyncContext::DeepCopy_Internal(*this, copy);
    }
  };

  /// Callback invoked once an asynchronous flush of a page to disk has
  /// completed. If successful, the page will be marked as open.
  static void FlushCallback(IAsyncContext* ctxt, FASTER::core::Status result,
                            size_t bytesFlushed)
  {
    CallbackContext<FlushContext> context(ctxt);

    if (result != FASTER::core::Status::Ok) return;
  };

  /// Issues an asynchronous write of the buffer to the durable file.
  inline void flush() {}

 private:
  /// The total size of the in-memory buffer in bytes.
  const uint32_t bufferSize;

  /// The in-memory circular buffer of pages.
  uint8_t* buffer;

  /// Logical address (in bytes) lesser than which requests are buffered on
  /// disk. Required during recovery or during a view change due migration.
  uint64_t head;

  /// Logical address (in bytes) of the next request that will be buffered.
  uint64_t tail;

  /// Logical address (in bytes) less than which requests have been transmitted
  /// out the network to the server. Required to determine from where in the
  /// buffer to transmit next.
  uint64_t transmitted;

  /// The persistent file into which buffered operations will be flushed to.
  session_file_t durableFile;

  /// The filesystem the above file was allocated under.
  disk_t disk;

  /// The maximum number of bytes that will be transmitted from this buffer
  /// in a single tx operation.
  uint64_t maxTxBytes;
};

} // end of namespace session
