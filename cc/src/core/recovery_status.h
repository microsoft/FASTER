// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>

namespace FASTER {
namespace core {

/// Used by FASTER to track status, during recovery action.

enum class PageRecoveryStatus {
  NotStarted = 0,
  IssuedRead,
  ReadDone,
  IssuedFlush,
  FlushDone
};

class RecoveryStatus {
 public:
  RecoveryStatus(uint32_t start_page_, uint32_t end_page_)
    : start_page{ start_page_ }
    , end_page{ end_page_ }
    , page_status_{ nullptr } {
    assert(end_page >= start_page);
    uint32_t buffer_size = end_page - start_page;
    page_status_ = new std::atomic<PageRecoveryStatus>[buffer_size];
    std::memset(page_status_, 0, sizeof(std::atomic<PageRecoveryStatus>) * buffer_size);
  }

  ~RecoveryStatus() {
      delete[] page_status_;
  }

  const std::atomic<PageRecoveryStatus>& page_status(uint32_t page) const {
    assert(page >= start_page);
    assert(page < end_page);
    return page_status_[page - start_page];
  }
  std::atomic<PageRecoveryStatus>& page_status(uint32_t page) {
    assert(page >= start_page);
    assert(page < end_page);
    return page_status_[page - start_page];
  }

  uint32_t start_page;
  uint32_t end_page;

 private:
  std::atomic<PageRecoveryStatus>* page_status_;
};

}
} // namespace FASTER::core
