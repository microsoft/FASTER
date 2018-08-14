// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <deque>
#include <thread>

#include "alloc.h"
#include "light_epoch.h"

namespace FASTER {
namespace core {

/// The allocator used for the hash table's overflow buckets.

/// Address into a fixed page.
struct FixedPageAddress {
  static constexpr uint64_t kInvalidAddress = 0;

  /// A fixed-page address is 8 bytes.
  /// --of which 48 bits are used for the address. (The remaining 16 bits are used by the hash
  /// table, for control bits and the tag.)
  static constexpr uint64_t kAddressBits = 48;
  static constexpr uint64_t kMaxAddress = ((uint64_t)1 << kAddressBits) - 1;

  /// --of which 20 bits are used for offsets into a page, of size 2^20 = 1 million items.
  static constexpr uint64_t kOffsetBits = 20;
  static constexpr uint64_t kMaxOffset = ((uint64_t)1 << kOffsetBits) - 1;

  /// --and the remaining 28 bits are used for the page index, allowing for approximately 256
  /// million pages.
  static constexpr uint64_t kPageBits = kAddressBits - kOffsetBits;
  static constexpr uint64_t kMaxPage = ((uint64_t)1 << kPageBits) - 1;

  FixedPageAddress()
    : control_{ 0 } {
  }
  FixedPageAddress(uint64_t control)
    : control_{ control } {
  }

  bool operator==(const FixedPageAddress& other) const {
    assert(reserved == 0);
    assert(other.reserved == 0);
    return control_ == other.control_;
  }
  bool operator<(const FixedPageAddress& other) const {
    assert(reserved == 0);
    assert(other.reserved == 0);
    return control_ < other.control_;
  }
  bool operator>(const FixedPageAddress& other) const {
    assert(reserved == 0);
    assert(other.reserved == 0);
    return control_ > other.control_;
  }
  bool operator>=(const FixedPageAddress& other) const {
    assert(reserved == 0);
    assert(other.reserved == 0);
    return control_ >= other.control_;
  }
  FixedPageAddress operator++() {
    return FixedPageAddress{ ++control_ };
  }

  uint32_t offset() const {
    return static_cast<uint32_t>(offset_);
  }
  uint64_t page() const {
    return page_;
  }
  uint64_t control() const {
    return control_;
  }

  union {
      struct {
        uint64_t offset_ : kOffsetBits;        // 20 bits
        uint64_t page_ : kPageBits;            // 28 bits
        uint64_t reserved : 64 - kAddressBits; // 16 bits
      };
      uint64_t control_;
    };
};
static_assert(sizeof(FixedPageAddress) == 8, "sizeof(FixedPageAddress) != 8");

/// Atomic address into a fixed page.
class AtomicFixedPageAddress {
 public:
  AtomicFixedPageAddress(const FixedPageAddress& address)
    : control_{ address.control_ } {
  }

  /// Atomic access.
  inline FixedPageAddress load() const {
    return FixedPageAddress{ control_.load() };
  }
  void store(FixedPageAddress value) {
    control_.store(value.control_);
  }
  FixedPageAddress operator++(int) {
    return FixedPageAddress{ control_++ };
  }


 private:
  /// Atomic access to the address.
  std::atomic<uint64_t> control_;
};
static_assert(sizeof(AtomicFixedPageAddress) == 8, "sizeof(AtomicFixedPageAddress) != 8");

struct FreeAddress {
  FixedPageAddress removed_addr;
  uint64_t removal_epoch;
};

template <typename T>
class FixedPage {
 public:
  typedef T item_t;
  static constexpr uint64_t kPageSize = FixedPageAddress::kMaxOffset + 1;

  /// Accessors.
  inline const item_t& element(uint32_t offset) const {
    assert(offset <= FixedPageAddress::kMaxOffset);
    return elements_[offset];
  }
  inline item_t& element(uint32_t offset) {
    assert(offset <= FixedPageAddress::kMaxOffset);
    return elements_[offset];
  }

 private:
  /// The page's contents.
  item_t elements_[kPageSize];
  static_assert(alignof(item_t) <= Constants::kCacheLineBytes,
                "alignof(item_t) > Constants::kCacheLineBytes");
};

template <typename T>
class FixedPageArray {
 public:
  typedef T item_t;
  typedef FixedPage<T> page_t;
  typedef FixedPageArray<T> array_t;

 protected:
  FixedPageArray(uint64_t alignment_, uint64_t size_, const array_t* old_array)
    : alignment{ alignment_ }
    , size{ size_ } {
    assert(Utility::IsPowerOfTwo(size));
    uint64_t idx = 0;
    if(old_array) {
      assert(old_array->size < size);
      for(; idx < old_array->size; ++idx) {
        page_t* page;
        page = old_array->pages()[idx].load(std::memory_order_acquire);
        while(page == nullptr) {
          std::this_thread::yield();
          page = old_array->pages()[idx].load(std::memory_order_acquire);
        }
        pages()[idx] = page;
      }
    }
    for(; idx < size; ++idx) {
      pages()[idx] = nullptr;
    }
  }

 public:
  static FixedPageArray* Create(uint64_t alignment, uint64_t size, const array_t* old_array) {
    void* buffer = std::malloc(sizeof(array_t) + size * sizeof(std::atomic<page_t*>));
    return new(buffer) array_t{ alignment, size, old_array };
  }

  static void Delete(array_t* arr, bool owns_pages) {
    assert(arr);
    if(owns_pages) {
      for(uint64_t idx = 0; idx < arr->size; ++idx) {
        page_t* page = arr->pages()[idx].load(std::memory_order_acquire);
        if(page) {
          page->~FixedPage();
          aligned_free(page);
        }
      }
    }
    arr->~FixedPageArray();
    std::free(arr);
  }

  /// Used by allocator.Get().
  inline page_t* Get(uint64_t page_idx) {
    assert(page_idx < size);
    return pages()[page_idx].load(std::memory_order_acquire);
  }

  /// Used by allocator.Allocate().
  inline page_t* GetOrAdd(uint64_t page_idx) {
    assert(page_idx < size);
    page_t* page = pages()[page_idx].load(std::memory_order_acquire);
    while(page == nullptr) {
      page = AddPage(page_idx);
    }
    return page;
  }

  inline page_t* AddPage(uint64_t page_idx) {
    assert(page_idx < size);
    void* buffer = aligned_alloc(alignment, sizeof(page_t));
    page_t* new_page = new(buffer) page_t{};
    page_t* expected = nullptr;
    if(pages()[page_idx].compare_exchange_strong(expected, new_page, std::memory_order_release)) {
      return new_page;
    } else {
      new_page->~page_t();
      aligned_free(new_page);
      return expected;
    }
  }

 private:
  /// Accessors, since zero-length arrays at the ends of structs aren't standard in C++.
  const std::atomic<page_t*>* pages() const {
    return reinterpret_cast<const std::atomic<page_t*>*>(this + 1);
  }
  std::atomic<page_t*>* pages() {
    return reinterpret_cast<std::atomic<page_t*>*>(this + 1);
  }

 public:
  /// Alignment at which each page is allocated.
  const uint64_t alignment;
  /// Maximum number of pages in the array; fixed at time of construction.
  const uint64_t size;
  /// Followed by [size] std::atomic<> pointers to (page_t) pages. (Not shown here.)
};

class alignas(Constants::kCacheLineBytes) FreeList {
 public:
  std::deque<FreeAddress> free_list;
};

template <typename T, class D>
class MallocFixedPageSize {
 public:
  typedef T item_t;
  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef FixedPage<T> page_t;
  typedef FixedPageArray<T> array_t;
  typedef MallocFixedPageSize<T, disk_t> alloc_t;

  MallocFixedPageSize()
    : alignment_{ UINT64_MAX }
    , count_{ 0 }
    , epoch_{ nullptr }
    , page_array_{ nullptr }
    , disk_{ nullptr }
    , pending_checkpoint_writes_{ 0 }
    , pending_recover_reads_{ 0 }
    , checkpoint_pending_{ false }
    , checkpoint_failed_{ false }
    , recover_pending_{ false }
    , recover_failed_{ false } {
  }

  ~MallocFixedPageSize() {
    if(page_array_.load() != nullptr) {
      array_t::Delete(page_array_.load(), true);
    }
  }

  inline void Initialize(uint64_t alignment, LightEpoch& epoch) {
    if(page_array_.load() != nullptr) {
      array_t::Delete(page_array_.load(), true);
    }
    alignment_ = alignment;
    count_.store(0);
    epoch_ = &epoch;
    disk_ = nullptr;
    pending_checkpoint_writes_ = 0;
    pending_recover_reads_ = 0;
    checkpoint_pending_ = false;
    checkpoint_failed_ = false;
    recover_pending_ = false;
    recover_failed_ = false;

    array_t* page_array = array_t::Create(alignment, 2, nullptr);
    page_array->AddPage(0);
    page_array_.store(page_array, std::memory_order_release);
    // Allocate the null pointer.
    Allocate();
  }

  inline void Uninitialize() {
    if(page_array_.load() != nullptr) {
      array_t::Delete(page_array_.load(), true);
      page_array_.store(nullptr);
    }
  }

  inline item_t& Get(FixedPageAddress address) {
    page_t* page = page_array_.load(std::memory_order_acquire)->Get(address.page());
    assert(page);
    return page->element(address.offset());
  }
  inline const item_t& Get(FixedPageAddress address) const {
    page_t* page = page_array_.load(std::memory_order_acquire)->Get(address.page());
    assert(page);
    return page->element(address.offset());
  }

  FixedPageAddress Allocate();

  void FreeAtEpoch(FixedPageAddress addr, uint64_t removed_epoch) {
    free_list().push_back(FreeAddress{ addr, removed_epoch });
  }

  /// Checkpointing and recovery.
  Status Checkpoint(disk_t& disk, file_t&& file, uint64_t& size);
  Status CheckpointComplete(bool wait);

  Status Recover(disk_t& disk, file_t&& file, uint64_t file_size, FixedPageAddress count);
  Status RecoverComplete(bool wait);

  std::deque<FreeAddress>& free_list() {
    return free_list_[Thread::id()].free_list;
  }
  const std::deque<FreeAddress>& free_list() const {
    return free_list_[Thread::id()].free_list;
  }

  FixedPageAddress count() const {
    return count_.load();
  }

 private:
  /// Checkpointing and recovery.
  class AsyncIoContext : public IAsyncContext {
   public:
    AsyncIoContext(alloc_t* allocator_)
      : allocator{ allocator_ } {
    }

    /// The deep-copy constructor
    AsyncIoContext(AsyncIoContext& other)
      : allocator{ other.allocator } {
    }

   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   public:
    alloc_t* allocator;
  };

  array_t* ExpandArray(array_t* expected, uint64_t new_size);

 private:
  /// Alignment at which each page is allocated.
  uint64_t alignment_;
  /// Array of all of the pages we've allocated.
  std::atomic<array_t*> page_array_;
  /// How many elements we've allocated.
  AtomicFixedPageAddress count_;

  LightEpoch* epoch_;

  /// State for ongoing checkpoint/recovery.
  disk_t* disk_;
  file_t file_;
  std::atomic<uint64_t> pending_checkpoint_writes_;
  std::atomic<uint64_t> pending_recover_reads_;
  std::atomic<bool> checkpoint_pending_;
  std::atomic<bool> checkpoint_failed_;
  std::atomic<bool> recover_pending_;
  std::atomic<bool> recover_failed_;

  FreeList free_list_[Thread::kMaxNumThreads];
};

/// Implementations.
template <typename T, class F>
Status MallocFixedPageSize<T, F>::Checkpoint(disk_t& disk, file_t&& file, uint64_t& size) {
  constexpr uint32_t kWriteSize = page_t::kPageSize * sizeof(item_t);

  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<AsyncIoContext> context{ ctxt };
    if(result != Status::Ok) {
      context->allocator->checkpoint_failed_ = true;
    }
    if(--context->allocator->pending_checkpoint_writes_ == 0) {
      result = context->allocator->file_.Close();
      if(result != Status::Ok) {
        context->allocator->checkpoint_failed_ = true;
      }
      context->allocator->checkpoint_pending_ = false;
    }
  };

  disk_ = &disk;
  file_ = std::move(file);
  size = 0;
  checkpoint_failed_ = false;
  array_t* page_array = page_array_.load();
  FixedPageAddress count = count_.load();

  uint64_t num_levels = count.page() + (count.offset() > 0 ? 1 : 0);
  assert(!checkpoint_pending_);
  assert(pending_checkpoint_writes_ == 0);
  checkpoint_pending_ = true;
  pending_checkpoint_writes_ = num_levels;
  for(uint64_t idx = 0; idx < num_levels; ++idx) {
    AsyncIoContext context{ this };
    RETURN_NOT_OK(file_.WriteAsync(page_array->Get(idx), idx * kWriteSize, kWriteSize, callback,
                                   context));
  }
  size = count.control_ * sizeof(item_t);
  return Status::Ok;
}

template <typename T, class F>
Status MallocFixedPageSize<T, F>::CheckpointComplete(bool wait) {
  disk_->TryComplete();
  bool complete = !checkpoint_pending_.load();
  while(wait && !complete) {
    disk_->TryComplete();
    complete = !checkpoint_pending_.load();
    std::this_thread::yield();
  }
  if(!complete) {
    return Status::Pending;
  } else {
    return checkpoint_failed_ ? Status::IOError : Status::Ok;
  }
}

template <typename T, class F>
Status MallocFixedPageSize<T, F>::Recover(disk_t& disk, file_t&& file, uint64_t file_size,
    FixedPageAddress count) {
  constexpr uint64_t kReadSize = page_t::kPageSize * sizeof(item_t);

  auto callback = [](IAsyncContext* ctxt, Status result, size_t bytes_transferred) {
    CallbackContext<AsyncIoContext> context{ ctxt };
    if(result != Status::Ok) {
      context->allocator->recover_failed_ = true;
    }
    if(--context->allocator->pending_recover_reads_ == 0) {
      result = context->allocator->file_.Close();
      if(result != Status::Ok) {
        context->allocator->recover_failed_ = true;
      }
      context->allocator->recover_pending_ = false;
    }
  };

  assert(file_size % sizeof(item_t) == 0);
  disk_ = &disk;
  file_ = std::move(file);
  recover_failed_ = false;

  // The size reserved by recovery is >= the size checkpointed to disk.
  FixedPageAddress file_end_addr{ file_size / sizeof(item_t) };
  uint64_t num_file_levels = file_end_addr.page() + (file_end_addr.offset() > 0 ? 1 : 0);
  assert(num_file_levels > 0);
  assert(count >= file_end_addr);
  uint64_t num_levels = count.page() + (count.offset() > 0 ? 1 : 0);
  assert(num_levels > 0);

  array_t* page_array = page_array_.load();
  // Ensure that the allocator has enough pages.
  if(page_array->size < num_levels) {
    uint64_t new_size = next_power_of_two(num_levels);
    page_array = ExpandArray(page_array, new_size);
  }
  count_.store(count);
  assert(!recover_pending_);
  assert(pending_recover_reads_.load() == 0);
  recover_pending_ = true;
  pending_recover_reads_ = num_file_levels;
  for(uint64_t idx = 0; idx < num_file_levels; ++idx) {
    //read a full page
    AsyncIoContext context{ this };
    RETURN_NOT_OK(file_.ReadAsync(idx * kReadSize, page_array->GetOrAdd(idx), kReadSize, callback,
                                  context));
  }
  return Status::Ok;
}

template <typename T, class F>
Status MallocFixedPageSize<T, F>::RecoverComplete(bool wait) {
  disk_->TryComplete();
  bool complete = !recover_pending_.load();
  while(wait && !complete) {
    disk_->TryComplete();
    complete = !recover_pending_.load();
    std::this_thread::yield();
  }
  if(!complete) {
    return Status::Pending;
  } else {
    return recover_failed_ ? Status::IOError : Status::Ok;
  }
}

template <typename T, class F>
FixedPageArray<T>* MallocFixedPageSize<T, F>::ExpandArray(array_t* expected, uint64_t new_size) {
  class Delete_Context : public IAsyncContext {
   public:
    Delete_Context(array_t* arr_)
      : arr{ arr_ } {
    }
    /// The deep-copy constructor.
    Delete_Context(const Delete_Context& other)
      : arr{ other.arr } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    array_t* arr;
  };

  auto delete_callback = [](IAsyncContext* ctxt) {
    CallbackContext<Delete_Context> context{ ctxt };
    array_t::Delete(context->arr, false);
  };

  assert(Utility::IsPowerOfTwo(new_size));
  do {
    array_t* new_array = array_t::Create(alignment_, new_size, expected);
    if(page_array_.compare_exchange_strong(expected, new_array, std::memory_order_release)) {
      // Have to free the old array, under epoch protection.
      Delete_Context context{ expected };
      IAsyncContext* context_copy;
      Status result = context.DeepCopy(context_copy);
      assert(result == Status::Ok);
      epoch_->BumpCurrentEpoch(delete_callback, context_copy);
      return new_array;
    } else {
      new_array->~array_t();
      std::free(new_array);
    }
  } while(expected->size < new_size);
  return expected;
}

template <typename T, class F>
inline FixedPageAddress MallocFixedPageSize<T, F>::Allocate() {
  if(!free_list().empty()) {
    // Check the head of the free list.
    if(free_list().front().removal_epoch <= epoch_->safe_to_reclaim_epoch.load()) {
      FixedPageAddress removed_addr = free_list().front().removed_addr;
      free_list().pop_front();
      return removed_addr;
    }
  }
  // Determine insertion page_index.
  FixedPageAddress addr = count_++;
  array_t* page_array = page_array_.load(std::memory_order_acquire);
  if(addr.page() >= page_array->size) {
    // Need to resize the page array.
    page_array = ExpandArray(page_array, next_power_of_two(addr.page() + 1));
  }
  if(addr.offset() == 0 && addr.page() + 1 < page_array->size) {
    // Add the next page early, to try to avoid blocking other threads.
    page_array->AddPage(addr.page() + 1);
  }
  page_array->GetOrAdd(addr.page());
  return addr;
}

}
} // namespace FASTER::core
