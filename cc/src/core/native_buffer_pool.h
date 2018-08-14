// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>

#include "alloc.h"
#include "utility.h"

#ifdef _WIN32
#include <intrin.h>
#pragma intrinsic(_BitScanReverse)

/// Microsoft's concurrency::concurrent_queue is based on Intel's tbb::concurrent_queue.
#include <concurrent_queue.h>
template <typename T>
using concurrent_queue = concurrency::concurrent_queue<T>;
#else
namespace FASTER {
/// Convert GCC's __builtin_clz() to Microsoft's _BitScanReverse.
inline uint8_t _BitScanReverse(unsigned long* index, uint32_t mask) {
  bool found = mask > 0;
  *index = 31 - __builtin_clz(mask);
  return found;
}
}

#include <tbb/concurrent_queue.h>
template <typename T>
using concurrent_queue = tbb::concurrent_queue<T>;
#endif

namespace FASTER {
namespace core {

/// A buffer pool used for file I/Os.

class NativeSectorAlignedBufferPool;

/// A sector-aligned memory block, along with offsets into the block.
class SectorAlignedMemory {
 public:
  /// Default constructor.
  SectorAlignedMemory()
    : buffer_{ nullptr }
    , valid_offset{ 0 }
    , required_bytes{ 0 }
    , available_bytes{ 0 }
    , level_{ 0 }
    , pool_{ nullptr } {
  }
  SectorAlignedMemory(uint8_t* buffer, uint32_t level, NativeSectorAlignedBufferPool* pool)
    : buffer_{ buffer }
    , valid_offset{ 0 }
    , required_bytes{ 0 }
    , available_bytes{ 0 }
    , level_{ level }
    , pool_{ pool } {
  }
  /// No copy constructor.
  SectorAlignedMemory(const SectorAlignedMemory&) = delete;
  /// Move constructor.
  SectorAlignedMemory(SectorAlignedMemory&& other)
    : buffer_{ other.buffer_ }
    , valid_offset{ other.valid_offset }
    , required_bytes{ other.required_bytes }
    , available_bytes{ other.available_bytes }
    , level_{ other.level_ }
    , pool_{ other.pool_ } {
    other.buffer_ = nullptr;
    other.pool_ = nullptr;
  }

  inline ~SectorAlignedMemory();

  /// Move assignment operator.
  inline SectorAlignedMemory& operator=(SectorAlignedMemory&& other);

  inline void CopyValidBytesToAddress(uint8_t* pt) const {
    std::memcpy(pt, &buffer_[valid_offset], required_bytes);
  }
  inline uint8_t* GetValidPointer() {
    return &buffer_[valid_offset];
  }
  inline uint8_t* buffer() {
    return buffer_;
  }

 private:
  uint8_t* buffer_;
 public:
  uint32_t valid_offset;
  uint32_t required_bytes;
  uint32_t available_bytes;
 private:
  uint32_t level_;
  NativeSectorAlignedBufferPool* pool_;
};
static_assert(sizeof(SectorAlignedMemory) == 32, "sizeof(SectorAlignedMemory) != 32");

/// Aligned buffer pool is a pool of memory.
/// Internally, it is organized as an array of concurrent queues where each concurrent
/// queue represents a memory of size in particular range. queue_[i] contains memory
/// segments each of size (2^i * sectorSize).
class NativeSectorAlignedBufferPool {
 private:
  static constexpr uint32_t kLevels = 32;

 public:
  NativeSectorAlignedBufferPool(uint32_t recordSize, uint32_t sectorSize)
    : record_size_{ recordSize }
    , sector_size_{ sectorSize } {
  }

  inline void Return(uint32_t level, uint8_t* buffer) {
    assert(level < kLevels);
    queue_[level].push(buffer);
  }
  inline SectorAlignedMemory Get(uint32_t numRecords);

 private:
  uint32_t Level(uint32_t sectors) {
    assert(sectors > 0);
    if(sectors == 1) {
      return 0;
    }
    // BSR returns the page_index k of the most-significant 1 bit. So 2^(k+1) > (sectors - 1) >=
    // 2^k, which means 2^(k+1) >= sectors > 2^k.
    unsigned long k;
    _BitScanReverse(&k, sectors - 1);
    return k + 1;
  }

  uint32_t record_size_;
  uint32_t sector_size_;
  /// Level 0 caches memory allocations of size (sectorSize); level n+1 caches allocations of size
  /// (sectorSize) * 2^n.
  concurrent_queue<uint8_t*> queue_[kLevels];
};

/// Implementations.
inline SectorAlignedMemory& SectorAlignedMemory::operator=(SectorAlignedMemory&& other) {
  if(buffer_ == other.buffer_) {
    // Self-assignment is a no-op.
    return *this;
  }
  if(buffer_ != nullptr) {
    // Return our buffer to the pool, before taking ownership of a new buffer.
    pool_->Return(level_, buffer_);
  }
  buffer_ = other.buffer_;
  valid_offset = other.valid_offset;
  required_bytes = other.required_bytes;
  available_bytes = other.available_bytes;
  level_ = other.level_;
  pool_ = other.pool_;

  // We own the buffer now; other SectorAlignedMemory does not.
  other.buffer_ = nullptr;
  other.pool_ = nullptr;
  return *this;
}

inline SectorAlignedMemory::~SectorAlignedMemory() {
  if(buffer_) {
    pool_->Return(level_, buffer_);
  }
}

inline SectorAlignedMemory NativeSectorAlignedBufferPool::Get(uint32_t numRecords) {
  // How many sectors do we need?
  uint32_t sectors_required = (numRecords * record_size_ + sector_size_ - 1) / sector_size_;
  uint32_t level = Level(sectors_required);
  uint8_t* buffer;
  if(queue_[level].try_pop(buffer)) {
    return SectorAlignedMemory{ buffer, level, this };
  } else {
    uint8_t* buffer = reinterpret_cast<uint8_t*>(aligned_alloc(sector_size_,
                      sector_size_ * (1 << level)));
    return SectorAlignedMemory{ buffer, level, this };
  }
}

}
} // namespace FASTER::core