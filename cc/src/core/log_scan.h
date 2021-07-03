// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>

#include "alloc.h"
#include "record.h"
#include "persistent_memory_malloc.h"

namespace FASTER {
namespace core {

// forward declarations
template<class F>
class LogPageIterator;
template<class F>
class LogPage;

enum class LogPageStatus : uint8_t {
  Unitialized = 0,
  Ready = 1,
  Pending = 2,
  Unavailable = 3,
};

/// Context used when issuing async page read requests
template<class F>
class LogPageReadContext : public IAsyncContext {
 public:
  LogPageReadContext(LogPage<F>* page_, uint64_t bytes_to_read_)
    : page{ page_ }
    , bytes_to_read{ bytes_to_read_ }
  {}

 protected:
  /// Copies this context into a passed in pointer.
  Status DeepCopy_Internal(IAsyncContext*& copy) {
    return IAsyncContext::DeepCopy_Internal(*this, copy);
  }

 public:
  LogPage<F>* page;
  uint64_t bytes_to_read;
};


template<class F>
class LogPage {
 public:
  typedef typename F::hlog_t hlog_t;
  typedef Record<typename F::key_t, typename F::value_t> record_t;

  LogPage(uint32_t sector_size)
    : current_addr{ Address::kInvalidAddress }
    , until_addr{  Address::kInvalidAddress }
    , status{ LogPageStatus::Unitialized }
  {
      // Allocate a page in memory
      page = alloc_aligned<uint8_t>(sector_size, hlog_t::kPageSize);
  }
  // non-copyable, non-copy assignable
  LogPage& operator=(const LogPage& other) = delete;
  LogPage(const LogPage&) = delete;
  // needed for std::vector
  LogPage(LogPage&& other) noexcept
    : status{ other.status.load() }
    , page{ std::move(other.page) }
    , current_addr{ other.current_addr }
    , until_addr{ other.until_addr }
  {}

  record_t* GetNext(Address& record_addr) {
    if (page == nullptr) return nullptr;

    if (current_addr >= until_addr ||
        current_addr.offset() >= Address::kMaxOffset) {
      record_addr = Address::kInvalidAddress;
      return nullptr;
    }

    record_t* record = reinterpret_cast<record_t*>(page.get() + current_addr.offset());
    if (!record->header.IsNull()) {
      record_addr = current_addr;
      current_addr += record->size();
      return record;
    }

    // likely the record didn't fit in this page -- no more entries in this page
    current_addr = Address::kMaxAddress;
    record_addr = Address::kInvalidAddress;
    return nullptr;
  }
  inline void Update(Address current, Address until) {
    current_addr = current;
    until_addr = until;
  }

  inline uint8_t* buffer() const {
    return page.get();
  }

 public:
  std::atomic<LogPageStatus> status;
 private:
  aligned_unique_ptr_t<uint8_t> page;
  Address current_addr;
  Address until_addr;
};


template <class F>
class LogPageCollection {
 public:
  constexpr static uint16_t kDefaultNumPages = 3; // double buffering

  typedef Record<typename F::key_t, typename F::value_t> record_t;

  LogPageCollection(uint16_t num_pages, uint32_t sector_size)
    : current_page_{ 0 }
    , num_pages_{ num_pages }
  {
    for (auto i = 0; i < num_pages; i++) {
      pages_.emplace_back(sector_size);
    }
  }
  // non-copyable
  LogPageCollection(LogPageCollection&) = delete;

  inline record_t* GetNextRecord(Address& record_address) {
    return pages_[current_page_].GetNext(record_address);
  }
  inline void MoveToNext() {
    current_page_ = (current_page_ + 1) % num_pages_;
  }


  inline LogPage<F>& current() {
    return pages_[current_page_];
  }
  inline LogPage<F>& next() {
    return pages_[(current_page_ + 1) % num_pages_];
  }

 private:
  std::vector<LogPage<F>> pages_;
  uint16_t current_page_;
  uint16_t num_pages_;
};

/// Helps scan through records on a Log.
/// `F` is the type signature on the FASTER instance using this iterator.
template <class F>
class LogPageIterator {
 public:
  /// For convenience. Typedef type signatures on records and the hybrid log.
  typedef Record<typename F::key_t, typename F::value_t> record_t;
  typedef typename F::hlog_t hlog_t;
  typedef typename F::disk_t disk_t;

  /// Constructs and returns an Iterator given a pointer to a log.
  ///
  /// \param log
  ///    Pointer to the log that this iterator must scan over.
  /// \param begin
  ///    Logical log address at which to start scanning.
  /// \param end
  ///    Logical log address at which to stop scanning.
  /// \param disk
  ///    Pointer to the disk the log was allocated under. Required for
  ///    when we need to issue IO requests and make sure they complete.
  LogPageIterator(hlog_t* log, Address begin, Address end, disk_t* disk)
   : hlog{ log }
   , until{ end }
   , current{ begin }
   , disk{ disk }
  {}

  /// Disallow copy constructor and copy assigner.
  LogPageIterator(const LogPageIterator& from) = delete;
  LogPageIterator& operator=(const LogPageIterator& from) = delete;

  bool GetNextPage(LogPage<F>& page) {
    Address page_addr, start_addr;
    if (!GetNextPageAddress(page_addr, start_addr)) {
      return false; // no more pages
    }
    IssueReadAsyncRequest(page, page_addr, start_addr);

    // Wait until page is loaded to memory
    while (page.status.load() != LogPageStatus::Ready)  {
      disk->TryComplete();
      std::this_thread::yield();
    }
    return true;
  }

  bool GetNextPage(LogPageCollection<F>& pages) {
    Address page_addr, start_addr;

    while (true) {
      if (GetNextPageAddress(page_addr, start_addr)) {
        // Issue read async request for the current page (either finished or uninitialized)
        IssueReadAsyncRequest(pages.current(), page_addr, start_addr);
      } else {
        // no more new pages available
        pages.current().status = LogPageStatus::Unavailable;
      }
      // Check if next page is available
      LogPage<F>& next_page = pages.next();
      LogPageStatus status = next_page.status;

      if (status == LogPageStatus::Pending) {
        // Wait until next page is loaded to memory
        while (next_page.status.load() != LogPageStatus::Ready)  {
          disk->TryComplete();
          std::this_thread::yield();
        }
      }
      else if (status == LogPageStatus::Unitialized) {
        // issue async read request for next page
        pages.MoveToNext();
        continue;
      }
      else if (status == LogPageStatus::Unavailable) {
        return false; // no more pages are stored
      }

      // Move to next page
      assert(next_page.status == LogPageStatus::Ready);
      pages.MoveToNext();
      return true;
    }
    return false; // not reachable
  }

 private:
  /// Computes are returns the next page address (if not reached end)
  inline bool GetNextPageAddress(Address& page_addr, Address& start_addr) {
    while (true) {
      Address current_addr = current.load();
      if (current_addr >= until) {
        return false;
      }
      // Try to advance current to next page
      Address desired(current_addr.page() + 1, 0);
      if (current.compare_exchange_strong(current_addr, desired)) {
        page_addr = Address(current_addr.page(), 0);
        start_addr = current_addr;
        break;
      }
    }
    return true;
  }

  inline void IssueReadAsyncRequest(LogPage<F>& page, Address& page_addr, Address& start_addr) {
    uint64_t bytes_to_read = (page_addr + hlog_t::kPageSize <= until)
                                ? hlog_t::kPageSize
                                : (until - page_addr).control();

    if (start_addr >= hlog->head_address.load()) {
      // Copy page contents from memory
      // NOTE: this is needed, since pages stored in memory may be flushed to disk
      memcpy(page.buffer(), hlog->Page(page_addr.page()), bytes_to_read);
      page.status.store(LogPageStatus::Ready);
    }
    else {
      // block until we load pages into the memory
      // Issue an IO to the persistent layer and then wait for it to complete.
      auto cb = [](IAsyncContext* ctxt, Status result, size_t bytes) {
        assert(result == Status::Ok);

        CallbackContext<LogPageReadContext<F>> context{ ctxt };
        assert(bytes == context->bytes_to_read);
        context->page->status.store(LogPageStatus::Ready);
      };
      // Issue reads to fill up the buffer and wait for them to complete.
      page.status.store(LogPageStatus::Pending);
      auto ctxt = LogPageReadContext<F>(&page, bytes_to_read);
      hlog->file->ReadAsync(page_addr.control(),
                            reinterpret_cast<void*>(page.buffer()),
                            bytes_to_read, cb, ctxt);
    }
    page.Update(start_addr, until);
  }

  /// The underlying hybrid log to scan over.
  hlog_t* hlog;
  /// Logical address within the log at which to stop scanning.
  Address until;
  /// Logical address within the log at which we are currently scanning.
  AtomicAddress current;
  /// Pointer to the disk hLog was allocated under. Required so that we
  /// can make sure that reads issued to the persistent layer complete.
  disk_t* disk;
};

///////////////////////////////////////////////////////////
/// [OLD] Classes used by the old compaction method
///////////////////////////////////////////////////////////

/// Represents the buffering mode used when scanning pages on the log.
enum class Buffering : uint8_t {
  /// Pages on persistent storage will not be buffered by LogIterator.
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
class LogRecordIterator {
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
  LogRecordIterator(hlog_t* log, Buffering mode, Address begin, Address end,
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
  ~LogRecordIterator() {
    for (auto i = 0; i < numFrames; i++) {
      aligned_free(reinterpret_cast<void*>(frames[i]));
    }

    delete frames;
  }

  /// Disallow copy and copy-assign constructors.
  LogRecordIterator(const LogRecordIterator& from) = delete;
  LogRecordIterator& operator=(const LogRecordIterator& from) = delete;

  /// Returns a pointer to the next record.
  record_t* GetNext(Address& record_address) {
    std::lock_guard<std::mutex> lock(mutex);

    for (int iters = 0; ; iters++) {
      // should not execute more than 2 iters
      if (iters == 2) assert(false);
      // We've exceeded the range over which we had to perform our scan.
      // No work to do over here other than returning a nullptr.
      if (current >= until) {
        record_address = Address::kInvalidAddress;
        return nullptr;
      }

      record_t * record;
      bool addr_in_memory = current >= hLog->head_address.load();
      if (!addr_in_memory) {
        // in persistent storage
        if ((currentFrame == 0 && current.offset() == 0) || (current == start)) {
          // block until we load pages into the memory
          // TODO: further optimize this (i.e. async request for next page when we move to next frame)
          BlockAndLoad();
        }
        record = reinterpret_cast<record_t*>(frames[currentFrame] + current.offset());
      }
      else {
        record = reinterpret_cast<record_t*>(hLog->Get(current));
      }

      if (!record->header.IsNull()) {
        // valid record -- update next record pointer and return
        record_address = current;
        current += record->size();
        return record;
      }

      // likely this record didn't fit in this page -- move to next page
      current = Address(current.page() + 1, 0);
      if (!addr_in_memory) {
        // switch to the next prefetched frame
        currentFrame = (currentFrame + 1) % numFrames;
      }
    }
  }

  /// Returns a pointer to the next record, along with its (logical) address
  record_t* GetNext() {
    Address record_address;
    return GetNext(record_address);
  }

 private:
  /// Loads pages from persistent storage
  void BlockAndLoad() {
    // Issue an IO to the persistent layer and then wait for it to complete.
    auto cb = [](IAsyncContext* ctxt, Status result, size_t bytes) {
      assert(result == Status::Ok);
      assert(bytes == hlog_t::kPageSize);

      CallbackContext<Context> context(ctxt);
      context->counter->fetch_add(1);
    };

    // Issue reads to fill up the buffer and wait for them to complete.
    Address load_addr = Address(current.page(), 0); // read page from start
    for (auto i = 0; i < numFrames; i++) {
      auto ctxt = Context(&completedIOs);
      auto addr = load_addr.control() + (i * hlog_t::kPageSize);
      hLog->file->ReadAsync(addr,
                            reinterpret_cast<void*>(frames[i]),
                            hlog_t::kPageSize, cb, ctxt);
    }

    while (completedIOs.load() < numFrames) {
      disk->TryComplete();
    }
    completedIOs.store(0); // reset
  }

  /// Passed in to the persistent layer when reading pages. Contains a pointer
  /// to an atomic counter. We read `numFrames` pages at a time in
  /// blockAndLoad(). When an IO completes, the callback increments the counter,
  /// allowing LogRecordIterator to keep track of the number of completed IOs.
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

  /// Mutex to protect concurrent threads accessing
  std::mutex mutex;
};

} // namespace core
} // namespace FASTER
