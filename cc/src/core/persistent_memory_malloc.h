// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <thread>

#include "device/file_system_disk.h"
#include "address.h"
#include "async_result_types.h"
#include "gc_state.h"
#include "light_epoch.h"
#include "native_buffer_pool.h"
#include "recovery_status.h"
#include "status.h"

namespace FASTER {
namespace core {

/// The log allocator, used by FASTER to store records.

enum class FlushStatus : uint8_t {
  Flushed,
  InProgress
};

enum class CloseStatus : uint8_t {
  Closed,
  Open
};

/// Pack flush- and close-status into a single 16-bit value.
/// State transitions are:
/// { Flushed, Closed } (default state)
/// --> { InProgress, Open } (when issuing the flush to disk)
/// --> either { . , Closed} (when moving the head address forward)
///     or     { Flushed, . } (when the flush completes).
struct FlushCloseStatus {
  FlushCloseStatus()
    : flush{ FlushStatus::Flushed }
    , close{ CloseStatus::Closed } {
  }

  FlushCloseStatus(FlushStatus flush_, CloseStatus close_)
    : flush{ flush_ }
    , close{ close_ } {
  }

  FlushCloseStatus(uint16_t control_)
    : control{ control_ } {
  }

  /// Is the page ready for use?
  inline bool Ready() const {
    return flush == FlushStatus::Flushed && close == CloseStatus::Open;
  }

  union {
      struct {
        FlushStatus flush;
        CloseStatus close;
      };
      uint16_t control;
    };
};
static_assert(sizeof(FlushCloseStatus) == 2, "sizeof(FlushCloseStatus) != 2");

/// Atomic version of FlushCloseStatus. Can set and get flush- and close- status, together,
/// atomically.
class AtomicFlushCloseStatus {
 public:
  AtomicFlushCloseStatus()
    : status_{} {
  }

  inline void store(FlushStatus flush, CloseStatus close) {
    // Sets flush and close statuses, atomically.
    FlushCloseStatus status{ flush, close };
    control_.store(status.control);
  }

  inline FlushCloseStatus load() const {
    // Gets flush and close statuses, atomically.
    return FlushCloseStatus{ control_.load() };
  }

  inline bool compare_exchange_weak(FlushCloseStatus& expected, FlushCloseStatus value) {
    uint16_t expected_control = expected.control;
    bool result = control_.compare_exchange_weak(expected_control, value.control);
    expected.control = expected_control;
    return result;
  }
  inline bool compare_exchange_strong(FlushCloseStatus& expected, FlushCloseStatus value) {
    uint16_t expected_control = expected.control;
    bool result = control_.compare_exchange_strong(expected_control, value.control);
    expected.control = expected_control;
    return result;
  }

  union {
      FlushCloseStatus status_;
      std::atomic<uint16_t> control_;
    };
};
static_assert(sizeof(AtomicFlushCloseStatus) == 2, "sizeof(FlushCloseStatus) != 2");

struct FullPageStatus {
  FullPageStatus()
    : LastFlushedUntilAddress{ 0 }
    , status{} {
  }

  AtomicAddress LastFlushedUntilAddress;
  AtomicFlushCloseStatus status;
};
static_assert(sizeof(FullPageStatus) == 16, "sizeof(FullPageStatus) != 16");

/// Page and offset of the tail of the log. Can reserve space within the current page or move to a
/// new page.
class PageOffset {
 public:
  PageOffset(uint32_t page, uint64_t offset)
    : offset_{ offset }
    , page_{ page } {
    assert(page <= Address::kMaxPage);
  }

  PageOffset(uint64_t control)
    : control_{ control } {
  }

  PageOffset(const Address& address)
    : offset_{ address.offset() }
    , page_{ address.page() } {
  }

  /// Accessors.
  inline uint64_t offset() const {
    return offset_;
  }
  inline uint32_t page() const {
    return static_cast<uint32_t>(page_);
  }
  inline uint64_t control() const {
    return control_;
  }

  /// Conversion operator.
  inline operator Address() const {
    assert(offset_ <= Address::kMaxOffset);
    return Address{ page(), static_cast<uint32_t>(offset()) };
  }

 private:
  /// Use 41 bits for offset, which gives us approximately 2 PB of overflow space, for
  /// Reserve().
  union {
      struct {
        uint64_t offset_ : 64 - Address::kPageBits;
        uint64_t page_ : Address::kPageBits;
      };
      uint64_t control_;
    };
};
static_assert(sizeof(PageOffset) == 8, "sizeof(PageOffset) != 8");

/// Atomic page + offset marker. Can Reserve() space from current page, or move to NewPage().
class AtomicPageOffset {
 public:
  AtomicPageOffset()
    : control_{ 0 } {
  }

  AtomicPageOffset(uint32_t page, uint64_t offset)
    : control_{ PageOffset{ page, offset } .control() } {
  }

  AtomicPageOffset(const Address& address) {
    PageOffset page_offset{ address };
    control_.store(page_offset.control());
  }

  /// Reserve space within the current page. Can overflow the page boundary (so result offset >
  /// Address::kMaxOffset).
  inline PageOffset Reserve(uint32_t num_slots) {
    assert(num_slots <= Address::kMaxOffset);
    PageOffset offset{ 0, num_slots };
    return PageOffset{ control_.fetch_add(offset.control()) };
  }

  /// Move to the next page. The compare-and-swap can fail. Returns "true" if some thread advanced
  /// the thread; sets "won_cas" = "true" if this thread won the CAS, which means it has been
  /// chosen to set up the new page.
  inline bool NewPage(uint32_t old_page, bool& won_cas) {
    assert(old_page < Address::kMaxPage);
    won_cas = false;
    PageOffset expected_page_offset = load();
    if(old_page != expected_page_offset.page()) {
      // Another thread already moved to the new page.
      assert(old_page < expected_page_offset.page());
      return true;
    }
    PageOffset new_page{ old_page + 1, 0 };
    uint64_t expected = expected_page_offset.control();
    // Try to move to a new page.
    won_cas = control_.compare_exchange_strong(expected, new_page.control());
    return PageOffset{ expected } .page() > old_page;
  }

  inline PageOffset load() const {
    return PageOffset{ control_.load() };
  }
  inline void store(Address address) {
    PageOffset page_offset{ address.page(), address.offset() };
    control_.store(page_offset.control());
  }

 private:
  union {
      /// Atomic access to the page+offset.
      std::atomic<uint64_t> control_;
    };
};
static_assert(sizeof(AtomicPageOffset) == 8, "sizeof(AtomicPageOffset) != 8");

/// The main allocator.
template <class D>
class PersistentMemoryMalloc {
 public:
  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef typename D::log_file_t log_file_t;
  typedef PersistentMemoryMalloc<disk_t> alloc_t;

  /// Each page in the buffer is 2^25 bytes (= 32 MB).
  static constexpr uint64_t kPageSize = Address::kMaxOffset + 1;

  /// The first 4 HLOG pages should be below the head (i.e., being flushed to disk).
  static constexpr uint32_t kNumHeadPages = 4;

  PersistentMemoryMalloc(bool has_no_backing_storage, uint64_t log_size, LightEpoch& epoch, disk_t& disk_, log_file_t& file_,
                         Address start_address, double log_mutable_fraction, bool pre_allocate_log)
    : sector_size{ static_cast<uint32_t>(file_.alignment()) }
    , epoch_{ &epoch }
    , disk{ &disk_ }
    , file{ &file_ }
    , read_buffer_pool{ 1, sector_size }
    , io_buffer_pool{ 1, sector_size }
    , read_only_address{ start_address }
    , safe_read_only_address{ start_address }
    , head_address{ start_address }
    , safe_head_address{ start_address }
    , flushed_until_address{ start_address }
    , begin_address{ start_address }
    , tail_page_offset_{ start_address }
    , buffer_size_{ 0 }
    , pages_{ nullptr }
    , page_status_{ nullptr }
    , pre_allocate_log_{ pre_allocate_log } {
    assert(start_address.page() <= Address::kMaxPage);

    if(log_size % kPageSize != 0) {
      throw std::invalid_argument{ "Log size must be a multiple of 32 MB" };
    }
    if(log_size / kPageSize > UINT32_MAX) {
      throw std::invalid_argument{ "Log size must be <= 128 PB" };
    }
    buffer_size_ = static_cast<uint32_t>(log_size / kPageSize);

    if(buffer_size_ <= kNumHeadPages + 1) {
      throw std::invalid_argument{ "Must have at least 2 non-head pages" };
    }
    // The latest N pages should be mutable.
    num_mutable_pages_ = static_cast<uint32_t>(log_mutable_fraction * buffer_size_);
    if(num_mutable_pages_ <= 1) {
      // Need at least two mutable pages: one to write to, and one to open up when the previous
      // mutable page is full.
      throw std::invalid_argument{ "Must have at least 2 mutable pages" };
    }
    // Make sure we have at least 'kNumHeadPages' immutable pages.
    // Otherwise, we will not be able to dump log to disk when our in-memory log is full.
    // If the user is certain that we will never need to dump anything to disk
    // (this is the case in compaction), skip this check.
    if(!has_no_backing_storage && buffer_size_ - num_mutable_pages_ < kNumHeadPages) {
      throw std::invalid_argument{ "Must have at least 'kNumHeadPages' immutable pages" };
    }

    page_status_ = new FullPageStatus[buffer_size_];

    pages_ = new uint8_t* [buffer_size_];
    for(uint32_t idx = 0; idx < buffer_size_; ++idx) {
      if (pre_allocate_log_) {
        pages_[idx] = reinterpret_cast<uint8_t*>(aligned_alloc(sector_size, kPageSize));
        std::memset(pages_[idx], 0, kPageSize);
        // Mark the page as accessible.
        page_status_[idx].status.store(FlushStatus::Flushed, CloseStatus::Open);
      } else {
        pages_[idx] = nullptr;
      }
    }

    PageOffset tail_page_offset = tail_page_offset_.load();
    AllocatePage(tail_page_offset.page());
    AllocatePage(tail_page_offset.page() + 1);
  }

  PersistentMemoryMalloc(bool has_no_backing_storage, uint64_t log_size, LightEpoch& epoch, disk_t& disk_, log_file_t& file_,
                         double log_mutable_fraction, bool pre_allocate_log)
    : PersistentMemoryMalloc(has_no_backing_storage, log_size, epoch, disk_, file_, Address{ 0 }, log_mutable_fraction, pre_allocate_log) {
    /// Allocate the invalid page. Supports allocations aligned up to kCacheLineBytes.
    uint32_t discard;
    Allocate(Constants::kCacheLineBytes, discard);
    assert(discard == UINT32_MAX);
    /// Move the head and read-only address past the invalid page.
    Address tail_address = tail_page_offset_.load();
    begin_address.store(tail_address);
    read_only_address.store(tail_address);
    safe_read_only_address.store(tail_address);
    head_address.store(tail_address);
    safe_head_address.store(tail_address);
  }

  ~PersistentMemoryMalloc() {
    if(pages_) {
      for(uint32_t idx = 0; idx < buffer_size_; ++idx) {
        if(pages_[idx]) {
          aligned_free(pages_[idx]);
        }
      }
      delete[] pages_;
    }
    if(page_status_) {
      delete[] page_status_;
    }
  }

  inline const uint8_t* Page(uint32_t page) const {
    assert(page <= Address::kMaxPage);
    return pages_[page % buffer_size_];
  }
  inline uint8_t* Page(uint32_t page) {
    assert(page <= Address::kMaxPage);
    return pages_[page % buffer_size_];
  }

  inline const FullPageStatus& PageStatus(uint32_t page) const {
    assert(page <= Address::kMaxPage);
    return page_status_[page % buffer_size_];
  }
  inline FullPageStatus& PageStatus(uint32_t page) {
    assert(page <= Address::kMaxPage);
    return page_status_[page % buffer_size_];
  }

  inline uint32_t buffer_size() const {
    return buffer_size_;
  }

  /// Read the tail page + offset, atomically, and convert it to an address.
  inline Address GetTailAddress() const {
    PageOffset tail_page_offset = tail_page_offset_.load();
    return Address{ tail_page_offset.page(), std::min(Address::kMaxOffset,
                    static_cast<uint32_t>(tail_page_offset.offset())) };
  }

  inline const uint8_t* Get(Address address) const {
    return Page(address.page()) + address.offset();
  }
  inline uint8_t* Get(Address address) {
    return Page(address.page()) + address.offset();
  }

  /// Key function used to allocate memory for a specified number of items. If the current page is
  /// full, returns Address::kInvalidAddress and sets closed_page to the current page index. The
  /// caller should Refresh() the epoch and call NewPage() until successful, before trying to
  /// Allocate() again.
  inline Address Allocate(uint32_t num_slots, uint32_t& closed_page);

  /// Tries to move the allocator to a new page; used when the current page is full. Returns "true"
  /// if the page advanced (so the caller can try to allocate, again).
  inline bool NewPage(uint32_t old_page);

  /// Invoked by users to obtain a record from disk. It uses sector aligned memory to read
  /// the record efficiently into memory.
  inline void AsyncGetFromDisk(Address address, uint32_t num_records, AsyncIOCallback callback,
                               AsyncIOContext& context);

  /// Used by applications to make the current state of the database immutable quickly
  Address ShiftReadOnlyToTail();

  void Truncate(GcState::truncate_callback_t callback);

  /// Action to be performed for when all threads have agreed that a page range is closed.
  class OnPagesClosed_Context : public IAsyncContext {
   public:
    OnPagesClosed_Context(alloc_t* allocator_,
                          Address new_safe_head_address_,
                          bool replace_with_clean_page_)
      : allocator{ allocator_ }
      , new_safe_head_address{ new_safe_head_address_ }
      , replace_with_clean_page{ replace_with_clean_page_ } {
    }

    /// The deep-copy constructor.
    OnPagesClosed_Context(const OnPagesClosed_Context& other)
      : allocator{ other.allocator }
      , new_safe_head_address{ other.new_safe_head_address }
      , replace_with_clean_page{ other.replace_with_clean_page } {
    }

   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   public:
    alloc_t* allocator;
    Address new_safe_head_address;
    bool replace_with_clean_page;
  };

  static void OnPagesClosed(IAsyncContext* ctxt);

  /// Seal: make sure there are no longer any threads writing to the page
  /// Flush: send page to secondary store
  class OnPagesMarkedReadOnly_Context : public IAsyncContext {
   public:
    OnPagesMarkedReadOnly_Context(alloc_t* allocator_,
                                  Address new_safe_read_only_address_,
                                  bool wait_for_pending_flush_complete_)
      : allocator{ allocator_ }
      , new_safe_read_only_address{ new_safe_read_only_address_ }
      , wait_for_pending_flush_complete{ wait_for_pending_flush_complete_ } {
    }

    /// The deep-copy constructor.
    OnPagesMarkedReadOnly_Context(const OnPagesMarkedReadOnly_Context& other)
      : allocator{ other.allocator }
      , new_safe_read_only_address{ other.new_safe_read_only_address }
      , wait_for_pending_flush_complete{ other.wait_for_pending_flush_complete } {
    }

   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   public:
    alloc_t* allocator;
    Address new_safe_read_only_address;
    bool wait_for_pending_flush_complete;
  };

  static void OnPagesMarkedReadOnly(IAsyncContext* ctxt);

 private:
  inline void GetFileReadBoundaries(Address read_offset, uint32_t read_length,
                                    uint64_t& begin_read, uint64_t& end_read, uint32_t& offset,
                                    uint32_t& length) const {
    assert(sector_size > 0);
    assert(Utility::IsPowerOfTwo(sector_size));
    assert(sector_size <= UINT32_MAX);
    size_t alignment_mask = sector_size - 1;
    // Align read to sector boundary.
    begin_read = read_offset.control() & ~alignment_mask;
    end_read = (read_offset.control() + read_length + alignment_mask) & ~alignment_mask;
    offset = static_cast<uint32_t>(read_offset.control() & alignment_mask);
    assert(end_read - begin_read <= UINT32_MAX);
    length = static_cast<uint32_t>(end_read - begin_read);
  }

  /// Allocate memory page, in sector aligned form
  inline void AllocatePage(uint32_t index);

  /// Used by several functions to update the variable to newValue. Ignores if newValue is smaller
  /// than the current value.
  template <typename A, typename T>
  inline bool MonotonicUpdate(A& variable, T new_value,
                              T& old_value) {
    old_value = variable.load();
    while(old_value < new_value) {
      if(variable.compare_exchange_strong(old_value, new_value)) {
        return true;
      }
    }
    return false;
  }

  Status AsyncFlushPages(uint32_t start_page, Address until_address,
                         bool serialize_objects = false);

 public:
  Status AsyncFlushPagesToFile(uint32_t start_page, Address until_address, file_t& file,
                               std::atomic<uint32_t>& flush_pending);

  /// Recovery.
  Status AsyncReadPagesFromLog(uint32_t start_page, uint32_t num_pages,
                               RecoveryStatus& recovery_status);
  Status AsyncReadPagesFromSnapshot(file_t& snapshot_file, uint32_t file_start_page,
                                    uint32_t start_page, uint32_t num_pages,
                                    RecoveryStatus& recovery_status);

  Status AsyncFlushPage(uint32_t page, RecoveryStatus& recovery_status,
                        AsyncCallback caller_callback, IAsyncContext* caller_context);
  void RecoveryReset(Address begin_address_, Address head_address_, Address tail_address);

 private:
  template <class F>
  Status AsyncReadPages(F& read_file, uint32_t file_start_page, uint32_t start_page,
                        uint32_t num_pages, RecoveryStatus& recovery_status);
  inline void PageAlignedShiftHeadAddress(uint32_t tail_page);
  inline void PageAlignedShiftReadOnlyAddress(uint32_t tail_page);

  /// Every async flush callback tries to update the flushed until address to the latest value
  /// possible
  /// Is there a better way to do this with enabling fine-grained addresses (not necessarily at
  /// page boundaries)?
  inline void ShiftFlushedUntilAddress() {
    Address current_flushed_until_address = flushed_until_address.load();
    uint32_t page = current_flushed_until_address.page();

    bool update = false;
    Address page_last_flushed_address = PageStatus(page).LastFlushedUntilAddress.load();
    while(page_last_flushed_address >= current_flushed_until_address) {
      current_flushed_until_address = page_last_flushed_address;
      update = true;
      ++page;
      page_last_flushed_address = PageStatus(page).LastFlushedUntilAddress.load();
    }

    if(update) {
      Address discard;
      MonotonicUpdate(flushed_until_address, current_flushed_until_address, discard);
    }
  }

 public:
  uint32_t sector_size;

 private:
  LightEpoch* epoch_;
  disk_t* disk;

 public:
  log_file_t* file;
  // Read buffer pool
  NativeSectorAlignedBufferPool read_buffer_pool;
  NativeSectorAlignedBufferPool io_buffer_pool;

  /// Every address < ReadOnlyAddress is read-only.
  AtomicAddress read_only_address;
  /// The minimum ReadOnlyAddress that every thread has seen.
  AtomicAddress safe_read_only_address;

  /// The circular buffer can drop any page < HeadAddress.page()--must read those pages from disk.
  AtomicAddress head_address;
  /// The minimum HeadPage that every thread has seen.
  AtomicAddress safe_head_address;

  AtomicAddress flushed_until_address;

  /// The address of the true head of the log--everything before this address has been truncated
  /// by garbage collection.
  AtomicAddress begin_address;

 private:
  uint32_t buffer_size_;
  bool pre_allocate_log_;

  /// -- the latest N pages should be mutable.
  uint32_t num_mutable_pages_;

  // Circular buffer definition
  uint8_t** pages_;

  // Array that indicates the status of each buffer page
  FullPageStatus* page_status_;

  // Global address of the current tail (next element to be allocated from the circular buffer)
  AtomicPageOffset tail_page_offset_;

};

/// Implementations.
template <class D>
inline void PersistentMemoryMalloc<D>::AllocatePage(uint32_t index) {
  index = index % buffer_size_;
  if (!pre_allocate_log_) {
    assert(pages_[index] == nullptr);
    pages_[index] = reinterpret_cast<uint8_t*>(aligned_alloc(sector_size, kPageSize));
    std::memset(pages_[index], 0, kPageSize);
    // Mark the page as accessible.
    page_status_[index].status.store(FlushStatus::Flushed, CloseStatus::Open);
  }
}

template <class D>
inline Address PersistentMemoryMalloc<D>::Allocate(uint32_t num_slots, uint32_t& closed_page) {
  closed_page = UINT32_MAX;
  PageOffset page_offset = tail_page_offset_.Reserve(num_slots);

  if(page_offset.offset() + num_slots > kPageSize) {
    // The current page is full. The caller should Refresh() the epoch and wait until
    // NewPage() is successful before trying to Allocate() again.
    closed_page = page_offset.page();
    return Address::kInvalidAddress;
  } else {
    assert(Page(page_offset.page()));
    return static_cast<Address>(page_offset);
  }
}

template <class D>
inline bool PersistentMemoryMalloc<D>::NewPage(uint32_t old_page) {
  assert(old_page < Address::kMaxPage);
  PageOffset new_tail_offset{ old_page + 1, 0 };
  // When the tail advances to page k+1, we clear page k+2.
  if(old_page + 2 >= safe_head_address.page() + buffer_size_) {
    // No room in the circular buffer for a new page; try to advance the head address, to make
    // more room available.
    disk->TryComplete();
    PageAlignedShiftReadOnlyAddress(old_page + 1);
    PageAlignedShiftHeadAddress(old_page + 1);
    return false;
  }
  FlushCloseStatus status = PageStatus(old_page + 1).status.load();
  if(!status.Ready()) {
    // Can't access the next page yet; try to advance the head address, to make the page
    // available.
    disk->TryComplete();
    PageAlignedShiftReadOnlyAddress(old_page + 1);
    PageAlignedShiftHeadAddress(old_page + 1);
    return false;
  }
  bool won_cas;
  bool retval = tail_page_offset_.NewPage(old_page, won_cas);
  if(won_cas) {
    // We moved the tail to (page + 1), so we are responsible for moving the head and
    // read-only addresses.
    PageAlignedShiftReadOnlyAddress(old_page + 1);
    PageAlignedShiftHeadAddress(old_page + 1);
    if(!Page(old_page + 2)) {
      // We are also responsible for allocating (page + 2).
      AllocatePage(old_page + 2);
    }
  }
  return retval;
}

template <class D>
inline void PersistentMemoryMalloc<D>::AsyncGetFromDisk(Address address, uint32_t num_records,
    AsyncIOCallback callback, AsyncIOContext& context) {
  uint64_t begin_read, end_read;
  uint32_t offset, length;
  GetFileReadBoundaries(address, num_records, begin_read, end_read, offset, length);
  context.record = read_buffer_pool.Get(length);
  context.record.valid_offset = offset;
  context.record.available_bytes = length - offset;
  context.record.required_bytes = num_records;

  file->ReadAsync(begin_read, context.record.buffer(), length, callback, context);
}

template <class D>
Address PersistentMemoryMalloc<D>::ShiftReadOnlyToTail() {
  Address tail_address = GetTailAddress();
  Address old_read_only_address;
  if(MonotonicUpdate(read_only_address, tail_address, old_read_only_address)) {
    OnPagesMarkedReadOnly_Context context{ this, tail_address, false };
    IAsyncContext* context_copy;
    Status result = context.DeepCopy(context_copy);
    assert(result == Status::Ok);
    epoch_->BumpCurrentEpoch(OnPagesMarkedReadOnly, context_copy);
  }
  return tail_address;
}

template <class D>
void PersistentMemoryMalloc<D>::Truncate(GcState::truncate_callback_t callback) {
  assert(sector_size > 0);
  assert(Utility::IsPowerOfTwo(sector_size));
  assert(sector_size <= UINT32_MAX);
  size_t alignment_mask = sector_size - 1;
  // Align read to sector boundary.
  uint64_t begin_offset = begin_address.control() & ~alignment_mask;
  file->Truncate(begin_offset, callback);
}

template <class D>
void PersistentMemoryMalloc<D>::OnPagesClosed(IAsyncContext* ctxt) {
  CallbackContext<OnPagesClosed_Context> context{ ctxt };
  Address old_safe_head_address;
  if(context->allocator->MonotonicUpdate(context->allocator->safe_head_address,
                                         context->new_safe_head_address,
                                         old_safe_head_address)) {
    for(uint32_t idx = old_safe_head_address.page(); idx < context->new_safe_head_address.page();
        ++idx) {
      FlushCloseStatus old_status = context->allocator->PageStatus(idx).status.load();
      FlushCloseStatus new_status;
      do {
        new_status = FlushCloseStatus{ old_status.flush, CloseStatus::Closed };
      } while(!context->allocator->PageStatus(idx).status.compare_exchange_weak(old_status,
              new_status));

      if(old_status.flush == FlushStatus::Flushed) {
        // We closed the page after it was flushed, so we are responsible for clearing and
        // reopening it.
        std::memset(context->allocator->Page(idx), 0, kPageSize);
        context->allocator->PageStatus(idx).status.store(FlushStatus::Flushed, CloseStatus::Open);
      }
    }
  }
}

template <class D>
void PersistentMemoryMalloc<D>::OnPagesMarkedReadOnly(IAsyncContext* ctxt) {
  CallbackContext<OnPagesMarkedReadOnly_Context> context{ ctxt };
  Address old_safe_read_only_address;
  if(context->allocator->MonotonicUpdate(context->allocator->safe_read_only_address,
                                         context->new_safe_read_only_address,
                                         old_safe_read_only_address)) {
    context->allocator->AsyncFlushPages(old_safe_read_only_address.page(),
                                        context->new_safe_read_only_address);
  }
}

template <class D>
Status PersistentMemoryMalloc<D>::AsyncFlushPages(uint32_t start_page, Address until_address,
    bool serialize_objects) {
  class Context : public IAsyncContext {
   public:
    Context(alloc_t* allocator_, uint32_t page_, Address until_address_)
      : allocator{ allocator_ }
      , page{ page_ }
      , until_address{ until_address_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other)
      : allocator{ other.allocator }
      , page{ other.page }
      , until_address{ other.until_address } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    alloc_t* allocator;
    uint32_t page;
    Address until_address;
  };

  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<Context> context{ ctxt };
    if(result != Status::Ok) {
      fprintf(stderr, "AsyncFlushPages(), error: %u\n", static_cast<uint8_t>(result));
    }
    context->allocator->PageStatus(context->page).LastFlushedUntilAddress.store(
      context->until_address);
    //Set the page status to flushed
    FlushCloseStatus old_status = context->allocator->PageStatus(context->page).status.load();
    FlushCloseStatus new_status;
    do {
      new_status = FlushCloseStatus{ FlushStatus::Flushed, old_status.close };
    } while(!context->allocator->PageStatus(context->page).status.compare_exchange_weak(
              old_status, new_status));
    if(old_status.close == CloseStatus::Closed) {
      // We finished flushing the page after it was closed, so we are responsible for clearing and
      // reopening it.
      std::memset(context->allocator->Page(context->page), 0, kPageSize);
      context->allocator->PageStatus(context->page).status.store(FlushStatus::Flushed,
          CloseStatus::Open);
    }
    context->allocator->ShiftFlushedUntilAddress();
  };

  uint32_t num_pages = until_address.page() - start_page;
  if(until_address.offset() > 0) {
    ++num_pages;
  }
  assert(num_pages > 0);

  for(uint32_t flush_page = start_page; flush_page < start_page + num_pages; ++flush_page) {
    Address page_start_address{ flush_page, 0 };
    Address page_end_address{ flush_page + 1, 0 };

    Context context{ this, flush_page, std::min(page_end_address, until_address) };

    //Set status to in-progress
    FlushCloseStatus old_status = PageStatus(flush_page).status.load();
    FlushCloseStatus new_status;
    do {
      new_status = FlushCloseStatus{ FlushStatus::InProgress, old_status.close };
    } while(!PageStatus(flush_page).status.compare_exchange_weak(old_status, new_status));
    PageStatus(flush_page).LastFlushedUntilAddress.store(0);

    RETURN_NOT_OK(file->WriteAsync(Page(flush_page), kPageSize * flush_page, kPageSize, callback,
                                   context));
  }
  return Status::Ok;
}

template <class D>
Status PersistentMemoryMalloc<D>::AsyncFlushPagesToFile(uint32_t start_page, Address until_address,
    file_t& file, std::atomic<uint32_t>& flush_pending) {
  class Context : public IAsyncContext {
   public:
    Context(std::atomic<uint32_t>& flush_pending_)
      : flush_pending{ flush_pending_ } {
    }
    /// The deep-copy constructor
    Context(Context& other)
      : flush_pending{ other.flush_pending } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    std::atomic<uint32_t>& flush_pending;
  };

  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<Context> context{ ctxt };
    if(result != Status::Ok) {
      fprintf(stderr, "AsyncFlushPagesToFile(), error: %u\n", static_cast<uint8_t>(result));
    }
    assert(context->flush_pending > 0);
    --context->flush_pending;
  };

  uint32_t num_pages = until_address.page() - start_page;
  if(until_address.offset() > 0) {
    ++num_pages;
  }
  assert(num_pages > 0);
  flush_pending = num_pages;

  for(uint32_t flush_page = start_page; flush_page < start_page + num_pages; ++flush_page) {
    Address page_start_address{ flush_page, 0 };
    Address page_end_address{ flush_page + 1, 0 };
    Context context{ flush_pending };
    RETURN_NOT_OK(file.WriteAsync(Page(flush_page), kPageSize * (flush_page - start_page),
                                  kPageSize, callback, context));
  }
  return Status::Ok;
}

template <class D>
Status PersistentMemoryMalloc<D>::AsyncReadPagesFromLog(uint32_t start_page, uint32_t num_pages,
    RecoveryStatus& recovery_status) {
  return AsyncReadPages(*file, 0, start_page, num_pages, recovery_status);
}

template <class D>
Status PersistentMemoryMalloc<D>::AsyncReadPagesFromSnapshot(file_t& snapshot_file,
    uint32_t file_start_page, uint32_t start_page, uint32_t num_pages,
    RecoveryStatus& recovery_status) {
  return AsyncReadPages(snapshot_file, file_start_page, start_page, num_pages, recovery_status);
}

template <class D>
template <class F>
Status PersistentMemoryMalloc<D>::AsyncReadPages(F& read_file, uint32_t file_start_page,
    uint32_t start_page, uint32_t num_pages, RecoveryStatus& recovery_status) {
  class Context : public IAsyncContext {
   public:
    Context(std::atomic<PageRecoveryStatus>& page_status_)
      : page_status{ &page_status_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other)
      : page_status{ other.page_status } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    std::atomic<PageRecoveryStatus>* page_status;
  };

  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<Context> context{ ctxt };
    if(result != Status::Ok) {
      fprintf(stderr, "Error: %u\n", static_cast<uint8_t>(result));
    }
    assert(context->page_status->load() == PageRecoveryStatus::IssuedRead);
    context->page_status->store(PageRecoveryStatus::ReadDone);
  };

  for(uint32_t read_page = start_page; read_page < start_page + num_pages; ++read_page) {
    if(!Page(read_page)) {
      // Allocate a new page.
      AllocatePage(read_page);
    } else {
      // Clear an old used page.
      std::memset(Page(read_page), 0, kPageSize);
    }
    assert(recovery_status.page_status(read_page) == PageRecoveryStatus::NotStarted);
    recovery_status.page_status(read_page).store(PageRecoveryStatus::IssuedRead);
    PageStatus(read_page).LastFlushedUntilAddress.store(Address{ read_page + 1, 0 });
    Context context{ recovery_status.page_status(read_page) };
    RETURN_NOT_OK(read_file.ReadAsync(kPageSize * (read_page - file_start_page), Page(read_page),
                                      kPageSize, callback, context));
  }
  return Status::Ok;
}

template <class D>
Status PersistentMemoryMalloc<D>::AsyncFlushPage(uint32_t page, RecoveryStatus& recovery_status,
    AsyncCallback caller_callback, IAsyncContext* caller_context) {
  class Context : public IAsyncContext {
   public:
    Context(std::atomic<PageRecoveryStatus>& page_status_, AsyncCallback caller_callback_,
            IAsyncContext* caller_context_)
      : page_status{ &page_status_ }
      , caller_callback{ caller_callback_ }
      , caller_context{ caller_context_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other, IAsyncContext* caller_context_copy)
      : page_status{ other.page_status }
      , caller_callback{ other.caller_callback }
      , caller_context{ caller_context_copy } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      if(caller_callback) {
        return IAsyncContext::DeepCopy_Internal(*this, caller_context, context_copy);
      } else {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }
    }
   public:
    std::atomic<PageRecoveryStatus>* page_status;
    AsyncCallback caller_callback;
    IAsyncContext* caller_context;
  };

  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<Context> context{ ctxt };
    if(result != Status::Ok) {
      fprintf(stderr, "Error: %u\n", static_cast<uint8_t>(result));
    }
    assert(context->page_status->load() == PageRecoveryStatus::IssuedFlush);
    context->page_status->store(PageRecoveryStatus::FlushDone);
    if(context->caller_callback) {
      context->caller_callback(context->caller_context, result);
    }
  };

  assert(recovery_status.page_status(page) == PageRecoveryStatus::ReadDone);
  recovery_status.page_status(page).store(PageRecoveryStatus::IssuedFlush);
  PageStatus(page).LastFlushedUntilAddress.store(Address{ page + 1, 0 });
  Context context{ recovery_status.page_status(page), caller_callback, caller_context };
  return file->WriteAsync(Page(page), kPageSize * page, kPageSize, callback, context);
}

template <class D>
void PersistentMemoryMalloc<D>::RecoveryReset(Address begin_address_, Address head_address_,
    Address tail_address) {
  begin_address.store(begin_address_);
  tail_page_offset_.store(tail_address);
  // issue read request to all pages until head lag
  head_address.store(head_address_);
  safe_head_address.store(head_address_);

  flushed_until_address.store(Address{ tail_address.page(), 0 });
  read_only_address.store(tail_address);
  safe_read_only_address.store(tail_address);

  uint32_t start_page = head_address_.page();
  uint32_t end_page = tail_address.offset() == 0 ? tail_address.page() : tail_address.page() + 1;
  if(!Page(end_page)) {
    AllocatePage(end_page);
  }
  if(!Page(end_page + 1)) {
    AllocatePage(end_page + 1);
  }

  for(uint32_t idx = 0; idx < buffer_size_; ++idx) {
    PageStatus(idx).status.store(FlushStatus::Flushed, CloseStatus::Open);
  }
}

template <class D>
inline void PersistentMemoryMalloc<D>::PageAlignedShiftHeadAddress(uint32_t tail_page) {
  //obtain local values of variables that can change
  Address current_head_address = head_address.load();
  Address current_flushed_until_address = flushed_until_address.load();

  if(tail_page <= (buffer_size_ - kNumHeadPages)) {
    // Desired head address is <= 0.
    return;
  }

  Address desired_head_address{ tail_page - (buffer_size_ - kNumHeadPages), 0 };

  if(current_flushed_until_address < desired_head_address) {
    desired_head_address = Address{ current_flushed_until_address.page(), 0 };
  }

  Address old_head_address;
  if(MonotonicUpdate(head_address, desired_head_address, old_head_address)) {
    OnPagesClosed_Context context{ this, desired_head_address, false };
    IAsyncContext* context_copy;
    Status result = context.DeepCopy(context_copy);
    assert(result == Status::Ok);
    epoch_->BumpCurrentEpoch(OnPagesClosed, context_copy);
  }
}

template <class D>
inline void PersistentMemoryMalloc<D>::PageAlignedShiftReadOnlyAddress(uint32_t tail_page) {
  Address current_read_only_address = read_only_address.load();
  if(tail_page <= num_mutable_pages_) {
    // Desired read-only address is <= 0.
    return;
  }

  Address desired_read_only_address{ tail_page - num_mutable_pages_, 0 };
  Address old_read_only_address;
  if(MonotonicUpdate(read_only_address, desired_read_only_address, old_read_only_address)) {
    OnPagesMarkedReadOnly_Context context{ this, desired_read_only_address, false };
    IAsyncContext* context_copy;
    Status result = context.DeepCopy(context_copy);
    assert(result == Status::Ok);
    epoch_->BumpCurrentEpoch(OnPagesMarkedReadOnly, context_copy);
  }
}

}
} // namespace FASTER::core
