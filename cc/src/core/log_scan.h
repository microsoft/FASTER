// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>

#include "alloc.h"
#include "auto_ptr.h"
#include "light_epoch.h"
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
    uint32_t bytes_to_read = hlog_t::kPageSize;
    if (page_addr + hlog_t::kPageSize > until) {
      bytes_to_read = static_cast<uint32_t>((until - page_addr).control());
      // Keep consistent with the device alignment
      bytes_to_read += hlog->sector_size - (bytes_to_read % hlog->sector_size);
    }
    assert(bytes_to_read % hlog->sector_size == 0);

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
/// Concurrent Log Iterator (supports multiple threads and uses epoch-based framework)
///////////////////////////////////////////////////////////

enum class ReadStatus : uint8_t {
  Read,
  InProgress,
};

// State transitions
// ============================================================
// { Read,      Closed }  -> default/initial state
// { InProgress,Closed }  -> read async request issued
// { Read,      Open }    -> page read and ready for processing
// { Read,      Closed }  -> all records processed in this page
struct ConcurrentLogPageStatus {
  ConcurrentLogPageStatus()
    : read{ ReadStatus::Read }
    , close{ CloseStatus::Closed } {
  }

  ConcurrentLogPageStatus(ReadStatus read_, CloseStatus close_)
    : read{ read_ }
    , close{ close_ } {
  }

  ConcurrentLogPageStatus(uint16_t control_)
    : control{ control_ } {
  }

  // Is page ready for use?
  inline bool Ready() const {
    return read == ReadStatus::Read && close == CloseStatus::Open;
  }
  // Can this page be populated with new data?
  inline bool Empty() const {
    return read == ReadStatus::Read && close == CloseStatus::Closed;
  }

  union {
    struct {
      ReadStatus read;
      CloseStatus close;
    };
    uint16_t control;
  };
};


template <class K, class V>
class ConcurrentLogPage {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;

  ConcurrentLogPage()
    : page_{ nullptr }
    , page_size_{ Address::kMaxOffset + 1 }
    , page_address_{ Address::kInvalidAddress } {
      // default state
      status.store(ConcurrentLogPageStatus{ ReadStatus::Read, CloseStatus::Closed });
      current_offset_.store(0);
  }
  ~ConcurrentLogPage() {
    auto* page = page_.release();
    if (page) {
      aligned_free(page);
    }
  }
  // non-copyable, non-copy assignable
  ConcurrentLogPage& operator=(const ConcurrentLogPage& other) = delete;
  ConcurrentLogPage(const ConcurrentLogPage&) = delete;

  record_t* GetNextRecord(Address& record_addr) {
    uint32_t idx = current_offset_++;
    if (idx >= record_offsets_.size()) {
      // no more records in this page
      // NOTE: thread should then call iter->GetNextPage()
      record_addr = Address::kInvalidAddress;
      return nullptr;
    }

    // return next record
    uint32_t offset = record_offsets_[idx];
    record_addr = Address{ page_address_.page(), offset };
    return reinterpret_cast<record_t*>(&page_.get()[offset]);
  }

  inline uint32_t id() const {
    return page_address_.page();
  }
  inline bool Ready() const {
    return status.load().Ready();
  }
  inline bool Empty() const {
    return status.load().Empty();
  }

  inline void Allocate(uint32_t sector_size) {
    page_.reset(reinterpret_cast<uint8_t*>(aligned_alloc(sector_size, page_size_)));
    Reset();
  }

  inline void Reset() {
    std::memset(page_.get(), 0, page_size_);
    record_offsets_.clear();
    page_address_ = Address::kInvalidAddress;
  }

  inline void Initialize(Address start_address, Address until) {
    uint32_t offset = start_address.offset();
    uint32_t until_offset = (start_address.page() == until.page())
                              ? until.offset()
                              : Address::kMaxOffset;

    while(offset <= until_offset) {
      record_t* record = reinterpret_cast<record_t*>(&page_[offset]);
      if (RecordInfo{ record->header }.IsNull()) {
        break;
      }
      record_offsets_.push_back(offset);

      if (offset + record->size() > Address::kMaxAddress) {
        break;
      }
      offset += record->size();
    }
    page_address_ = start_address;
    current_offset_.store(0);

    log_debug("\t # record offsets: %u [first=%d]", record_offsets_.size(), record_offsets_.front());
  }

  inline uint8_t* buffer() const {
    return page_.get();
  }

 public:
  std::atomic<ConcurrentLogPageStatus> status;
 private:
  std::unique_ptr<uint8_t[]> page_;
  uint64_t page_size_;
  std::vector<uint32_t> record_offsets_;
  std::atomic<uint32_t> current_offset_;
  Address page_address_;
};


/// Helps scan through records on a Log.
/// `F` is the type signature on the FASTER instance using this iterator.
template <class F>
class ConcurrentLogPageIterator {
 public:
  constexpr static uint16_t kDefaultNumPages = 3; // double buffering

  /// For convenience. Typedef type signatures on records and the hybrid log.
  typedef F faster_t;
  typedef typename F::key_t key_t;
  typedef typename F::value_t value_t;
  typedef typename F::hlog_t hlog_t;
  typedef typename F::disk_t disk_t;
  typedef Record<key_t, value_t> record_t;
  typedef ConcurrentLogPage<key_t, value_t> concurrent_log_page_t;
  typedef ConcurrentLogPageIterator<faster_t> log_page_iter_t;

  static_assert(hlog_t::kPageSize == Address::kMaxOffset + 1);

  ConcurrentLogPageIterator(hlog_t* log, disk_t* disk, LightEpoch* epoch,
                            Address begin, Address until,
                            uint16_t num_pages = kDefaultNumPages)
   : hlog_{ log }
   , disk_{ disk }
   , epoch_{ epoch }
   , head_address_{ begin }
   , tail_address_{ begin }
   , until_{ until }
   , num_pages_{ num_pages } {
    // Allocate pages
    pages_ = new concurrent_log_page_t[num_pages];
    for (uint16_t idx = 0; idx < num_pages; ++idx) {
      pages_[idx].Allocate(log->sector_size);
    }
    // Issue first read requests
    PrefetchNextPages();
  }

  ~ConcurrentLogPageIterator() {
    if (pages_) {
      delete[] pages_;
    }
    pages_ = nullptr;
  }

  /// Disallow copy constructor and copy assigner.
  ConcurrentLogPageIterator(const ConcurrentLogPageIterator& from) = delete;
  ConcurrentLogPageIterator& operator=(const ConcurrentLogPageIterator& from) = delete;

  /// Action to be performed for when all threads have agreed that a page is processed.
  class OnPageProcessed_Context : public IAsyncContext {
   public:
    OnPageProcessed_Context(log_page_iter_t* iterator_,
                            Address old_head_address_)
      : iterator{ iterator_ }
      , old_head_address{ old_head_address_ } {
    }

    /// The deep-copy constructor.
    OnPageProcessed_Context(const OnPageProcessed_Context& other)
      : iterator{ other.iterator }
      , old_head_address{ other.old_head_address } {
    }

   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   public:
    log_page_iter_t* iterator;
    Address old_head_address;
  };

  /// Action to be performed for when all threads have agreed that a page is read.
  class OnPageRead_Context : public IAsyncContext {
   public:
    OnPageRead_Context(log_page_iter_t* iterator_, Address start_address_,
                      Address until_address_, size_t bytes_to_read_)
      : iterator{ iterator_ }
      , start_address{ start_address_ }
      , until_address{ until_address_ }
      , bytes_to_read{ bytes_to_read_ } {
    }

    /// The deep-copy constructor.
    OnPageRead_Context(const OnPageRead_Context& other)
      : iterator{ other.iterator }
      , start_address{ other.start_address }
      , until_address{ other.until_address }
      , bytes_to_read{ other.bytes_to_read } {
    }

   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   public:
    log_page_iter_t* iterator;
    Address start_address;
    Address until_address;
    size_t bytes_to_read;
  };

  inline concurrent_log_page_t& Page(uint32_t page) {
    assert(page <= Address::kMaxPage);
    return pages_[page % num_pages_];
  }

  concurrent_log_page_t* GetNextPage(uint32_t old_page, bool& pages_available) {
    assert(old_page <= Address::kMaxPage);

    PrefetchNextPages();

    Address head_address = head_address_.load();

    uint32_t next_page = old_page + 1;
    pages_available = (old_page != head_address.page()) || (Address{ next_page, 0 } < until_);
    if (!pages_available) {
      // No more pages..
      return nullptr;
    }

    if (old_page != head_address.page()) {
      // new thread that requests page for first time
      // or thread that was working on a previous page
      auto& page = Page(head_address.page());
      if (page.Ready()) {
        return &page;
      } else {
        // retry...
        return nullptr;
      }
    }

    // Try to return next page (if read from disk)
    auto& page = Page(next_page);
    if (!page.Ready()) {
      // Can't access the next page yet
      disk_->TryComplete();
      return nullptr;
    }

    ShiftHeadAddress(head_address);
    return &page;
  }

  static void OnPageProcessed(IAsyncContext* ctxt) {
    CallbackContext<OnPageProcessed_Context> context{ ctxt };

    // Mark page as closed
    auto& old_page = context->iterator->Page(context->old_head_address.page());
    old_page.Reset();

    ConcurrentLogPageStatus expected{ ReadStatus::Read, CloseStatus::Open };
    bool success = old_page.status.compare_exchange_strong(expected,
      ConcurrentLogPageStatus{ ReadStatus::Read, CloseStatus::Closed }
    );
    if (!success) {
      throw std::runtime_error{ "not possible" };
    }
  }

  static void OnPageRead(IAsyncContext* ctxt, Status result, size_t bytes) {
    assert(result == Status::Ok);

    CallbackContext<OnPageRead_Context> context{ ctxt };
    assert(bytes == context->bytes_to_read);

    auto& page = context->iterator->Page(context->start_address.page());
    page.Initialize(context->start_address, context->until_address);

    ConcurrentLogPageStatus expected{ ReadStatus::InProgress, CloseStatus::Closed };
    bool success = page.status.compare_exchange_strong(expected,
      ConcurrentLogPageStatus{ ReadStatus::Read, CloseStatus::Open }
    );
    if (!success) {
      throw std::runtime_error{ "not possible" };
    }
  }

 private:
  inline void ShiftHeadAddress(Address head_address) {
    Address desired_head_address{ head_address.page() + 1, 0 };
    bool won_cas = head_address_.compare_exchange_strong(head_address, desired_head_address);
    if (won_cas) {
      OnPageProcessed_Context context{ this, head_address };
      IAsyncContext* context_copy;
      Status result = context.DeepCopy(context_copy);
      assert(result == Status::Ok);
      epoch_->BumpCurrentEpoch(OnPageProcessed, context_copy);
    }
  }

  inline void PrefetchNextPages() {
    while (true) {
      Address tail_address = tail_address_.load();
      if (tail_address >= until_) {
        break;
      }
      // Has page been processed?
      auto& page = Page(tail_address.page());
      if (!page.Empty()) {
        break;
      }
      // Try to increment tail address...
      bool won_cas = tail_address_.compare_exchange_strong(tail_address, Address{ tail_address.page() + 1, 0 });
      if (won_cas) {
        // Issue read request for this page
        ConcurrentLogPageStatus expected{ ReadStatus::Read, CloseStatus::Closed };
        bool success = page.status.compare_exchange_strong(expected,
          ConcurrentLogPageStatus{ ReadStatus::InProgress, CloseStatus::Closed }
        );
        if (!success) {
          throw std::runtime_error{ "not possible" };
        }
        IssueReadAsyncRequest(Page(tail_address.page()), tail_address);
      }
    }
    disk_->TryComplete();
  }

  inline void IssueReadAsyncRequest(concurrent_log_page_t& page, Address& start_address) {
    assert(!page.Empty() && !page.Ready());

    uint32_t bytes_to_read = hlog_t::kPageSize;
    uint32_t buffer_offset = 0;

    // Account for when starting from inside a page
    if (start_address.offset() != 0) {
      bytes_to_read -= start_address.offset();
      // Keep consistent with the device alignment
      uint32_t delta = hlog_->sector_size - (bytes_to_read % hlog_->sector_size);
      bytes_to_read += delta;
      buffer_offset = start_address.offset() - delta;
    }
    // Account for when stopping inside a page
    else if (start_address + hlog_t::kPageSize > until_) {
      bytes_to_read = static_cast<uint32_t>((until_ - start_address).control());
      // Keep consistent with the device alignment
      bytes_to_read += hlog_->sector_size - (bytes_to_read % hlog_->sector_size);
    }
    assert(bytes_to_read % hlog_->sector_size == 0);
    assert(buffer_offset % hlog_->sector_size == 0);

    log_debug("[%u] %lu %lu SA=%llu", page.id(), buffer_offset, bytes_to_read, start_address.control());

    if (start_address >= hlog_->head_address.load()) {
      // Copy page contents from memory
      // NOTE: this is needed, since pages stored in memory may be soon flushed to disk
      memcpy(page.buffer() + buffer_offset, hlog_->Page(start_address.page()) + buffer_offset, bytes_to_read);
      // Initialize page
      page.Initialize(start_address, until_);
      // mark page ready
      page.status.store(ConcurrentLogPageStatus{ ReadStatus::Read, CloseStatus::Open });
    }
    else {
      // Issue an IO to the persistent layer in an async fashion
      OnPageRead_Context context{ this, start_address, until_, bytes_to_read };
      hlog_->file->ReadAsync(Address{ start_address.page(), buffer_offset }.control(),
                            reinterpret_cast<void*>(page.buffer() + buffer_offset),
                            bytes_to_read, OnPageRead, context);
    }
  }

 private:
  /// The underlying hybrid log to scan over.
  hlog_t* hlog_;
  /// Pointer to the disk hLog was allocated under. Required so that we
  /// can make sure that reads issued to the persistent layer complete.
  disk_t* disk_;
  ///
  LightEpoch* epoch_;
  // if page < head_address_.page(), it will be dropped from the circular buffer
  AtomicAddress head_address_;
  /// Logical address within the log which we will scan next.
  AtomicAddress tail_address_;
  /// Logical address within the log at which to stop scanning.
  Address until_;
  /// Page collection
  concurrent_log_page_t* pages_;
  uint16_t num_pages_;
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
