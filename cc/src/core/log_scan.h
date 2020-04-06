// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>

#include "alloc.h"
#include "record.h"
#include "persistent_memory_malloc.h"

namespace FASTER {
namespace core {

/// Represents the buffering mode used when scanning pages on the log.
enum class Buffering : uint8_t {
  /// Pages on persistent storage will not be buffered by ScanIterator.
  /// When scanning such a page, the iterator will synchronously block
  /// until it has been fetched from the persistent storage layer.
  UN_BUFFERED = 0,

  /// Every time the iterator begins scanning a page, it will issue an
  /// asynchronous read for the next one. If the read hasn't completed
  /// by the time it reaches the next page the iterator will block.
  SINGLE_PAGE = 1,

  /// Every time the iterator begins scanning a page, it will issue an
  /// asynchronous read for the next two. If these reads have not completed
  /// by the time it reaches the end of what's already been read the iterator
  /// will block.
  DOUBLE_PAGE = 2,
};

/// Helps scan through records on a Log.
/// `F` is the type signature on the FASTER instance using this iterator.
template <class F>
class ScanIterator {
 public:
  /// For convenience. Typedef type signatures on records and the hybrid log.
  typedef Record<typename F::key_t, typename F::value_t> record_t;
  typedef typename F::hlog_t hlog_t;
  typedef typename F::disk_t disk_t;

  /// Constructs and returns an Iterator given a pointer to a log.
  ///
  /// \param log
  ///    Pointer to the log that this iterator must scan over.
  /// \param mode
  ///    Buffering mode to be used when scanning the log. Refer to the
  ///    `Buffering` enum class for more details.
  /// \param begin
  ///    Logical log address at which to start scanning.
  /// \param end
  ///    Logical log address at which to stop scanning.
  /// \param disk
  ///    Pointer to the disk the log was allocated under. Required for
  ///    when we need to issue IO requests and make sure they complete.
  ScanIterator(hlog_t* log, Buffering mode, Address begin, Address end,
               disk_t* disk)
   : hLog(log)
   , numFrames(0)
   , frames(nullptr)
   , start(begin)
   , until(end)
   , current(start)
   , currentFrame(0)
   , completedIOs(0)
   , disk(disk)
  {
    // Allocate one extra frame than what was requested so that we can
    // hold the page that we're currently scanning.
    switch (mode) {
    case Buffering::SINGLE_PAGE:
      numFrames = 2;
      break;

    case Buffering::DOUBLE_PAGE:
      numFrames = 3;
      break;

    case Buffering::UN_BUFFERED:
    default:
      numFrames = 1;
      break;
    }

    frames = new uint8_t* [numFrames];
    for (auto i = 0; i < numFrames; i++) {
      frames[i] = reinterpret_cast<uint8_t*>(aligned_alloc(hLog->sector_size,
                                                           hlog_t::kPageSize));
    }
  }

  /// Destroys the iterator freeing up memory allocated for the buffer.
  ~ScanIterator() {
    for (auto i = 0; i < numFrames; i++) {
      aligned_free(reinterpret_cast<void*>(frames[i]));
    }

    delete frames;
  }

  /// Disallow copy and copy-assign constructors.
  ScanIterator(const ScanIterator& from) = delete;
  ScanIterator& operator=(const ScanIterator& from) = delete;

  /// Returns a pointer to the next record.
  record_t* GetNext() {
    // We've exceeded the range over which we had to perform our scan.
    // No work to do over here other than returning a nullptr.
    if (current >= until) return nullptr;

    // If we're within the in-memory region, then just lookup the address,
    // increment it and return a pointer to the record.
    if (current >= hLog->head_address.load()) {
      auto record = reinterpret_cast<record_t*>(hLog->Get(current));
      current += record->size();
      return record;
    }

    // If we're over here then we need to issue reads to persistent storage.
    return blockAndLoad();
  }

 private:
  /// Loads pages from persistent storage if needed, and returns a pointer
  /// to the next record in the log.
  record_t* blockAndLoad() {
    // We are at the first address of the first frame in the buffer. Issue
    // an IO to the persistent layer and then wait for it to complete.
    if (currentFrame == 0 && current.offset() == 0) {
      auto cb = [](IAsyncContext* ctxt, Status result, size_t bytes) {
        assert(result == Status::Ok);
        assert(bytes == hlog_t::kPageSize);

        CallbackContext<Context> context(ctxt);
        context->counter->fetch_add(1);
      };

      // Issue reads to fill up the buffer and wait for them to complete.
      for (auto i = 0; i < numFrames; i++) {
        auto ctxt = Context(&completedIOs);
        auto addr = current.control() + (i * hlog_t::kPageSize);
        hLog->file->ReadAsync(addr,
                              reinterpret_cast<void*>(frames[i]),
                              hlog_t::kPageSize, cb, ctxt);
      }

      while (completedIOs.load() < numFrames) disk->TryComplete();
      completedIOs.store(0);
    }

    // We have the corresponding page in our buffer. Look it up, increment
    // the current address and current frame (if necessary), return a
    // pointer to the record.
    auto record = reinterpret_cast<record_t*>(frames[currentFrame] +
                                              current.offset());
    current += record->size();
    if (current.offset() == 0) currentFrame = (currentFrame + 1) % numFrames;
    return record;
  }

  /// Passed in to the persistent layer when reading pages. Contains a pointer
  /// to an atomic counter. We read `numFrames` pages at a time in
  /// blockAndLoad(). When an IO completes, the callback increments the counter,
  /// allowing ScanIterator to keep track of the number of completed IOs.
  class Context : public IAsyncContext {
   public:
    /// Constructs a context given a pointer to an atomic counter keeping
    /// track of the number of IOs that have completed so far.
    Context(std::atomic<uint64_t>* ctr)
     : counter(ctr)
    {}

    /// Destroys a Context.
    ~Context() {}

   protected:
    /// Copies this context into a passed in pointer.
    Status DeepCopy_Internal(IAsyncContext*& copy) {
      return IAsyncContext::DeepCopy_Internal(*this, copy);
    }

   public:
    /// Pointer to an atomic counter. Counts the number of IOs that have
    /// completed so far.
    std::atomic<uint64_t>* counter;
  };

  /// The underlying hybrid log to scan over.
  hlog_t* hLog;

  /// The number of page frames within the buffer.
  int numFrames;

  /// Buffer for pages that need to be scanned but are currently on disk.
  uint8_t** frames;

  /// Logical address within the log at which to start scanning.
  Address start;

  /// Logical address within the log at which to stop scanning.
  Address until;

  /// Logical address within the log at which we are currently scanning.
  Address current;

  /// Current frame within the buffer we're scanning through for records
  /// on persistent storage.
  uint64_t currentFrame;

  /// The number of read requests to the persistent storage layer that
  /// have completed so far. Refreshed every numFrames.
  std::atomic<uint64_t> completedIOs;

  /// Pointer to the disk hLog was allocated under. Required so that we
  /// can make sure that reads issued to the persistent layer complete.
  disk_t* disk;
};

} // namespace core
} // namespace FASTER
