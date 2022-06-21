// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>

#include "address.h"
#include "async.h"
#include "key_hash.h"
#include "native_buffer_pool.h"
#include "../index/hash_bucket.h"

#ifdef _WIN32
#include <concurrent_queue.h>

template <typename T>
using concurrent_queue = concurrency::concurrent_queue<T>;
#endif

using namespace FASTER::index;

namespace FASTER {
namespace core {

class AsyncIOContext : public IAsyncContext {
 public:
  AsyncIOContext(void* faster_, Address address_,
                 IAsyncContext* caller_context_,
                 concurrent_queue<AsyncIOContext*>* thread_io_responses_,
                 uint64_t io_id_)
    : faster{ faster_ }
    , address{ address_ }
    , caller_context{ caller_context_ }
    , thread_io_responses{ thread_io_responses_ }
    , io_id{ io_id_ } {
  }
  /// No copy constructor.
  AsyncIOContext(const AsyncIOContext& other) = delete;
  /// The deep-copy constructor.
  AsyncIOContext(AsyncIOContext& other, IAsyncContext* caller_context_)
    : faster{ other.faster }
    , address{ other.address }
    , caller_context{ caller_context_ }
    , thread_io_responses{ other.thread_io_responses }
    , record{ std::move(other.record) }
    , io_id{ other.io_id } {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, caller_context, context_copy);
  }
 public:
  void* faster;
  Address address;
  IAsyncContext* caller_context;
  concurrent_queue<AsyncIOContext*>* thread_io_responses;
  uint64_t io_id;

  SectorAlignedMemory record;
};

class AsyncIndexIOContext : public IAsyncContext {
 public:
  AsyncIndexIOContext(IAsyncContext* caller_context_,
                      concurrent_queue<AsyncIndexIOContext*>* thread_io_responses_,
                      uint64_t io_id_)
    : caller_context{ caller_context_ }
    , thread_io_responses{ thread_io_responses_ }
    , io_id{ io_id_ }
    , result{ Status::Corruption }
    , entry{ HashBucketEntry::kInvalidEntry } {
  }
  /// No copy constructor.
  AsyncIndexIOContext(const AsyncIOContext& other) = delete;
  /// The deep-copy constructor.
  AsyncIndexIOContext(AsyncIndexIOContext& other, IAsyncContext* caller_context_)
    : caller_context{ caller_context_ }
    , thread_io_responses{ other.thread_io_responses }
    , io_id{ other.io_id }
    , result{ other.result }
    , entry{ other.entry } {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, caller_context, context_copy);
  }

 public:
  IAsyncContext* caller_context;
  /// Queue where finished pending requests are pushed
  concurrent_queue<AsyncIndexIOContext*>* thread_io_responses;
  /// Unique id for I/O request
  uint64_t io_id;

  /// Result of the index operation
  Status result;
  /// Entry found in the index
  HashBucketEntry entry;
};

}
} // namespace FASTER::core