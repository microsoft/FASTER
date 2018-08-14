// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "thread.h"

namespace FASTER {
namespace core {

/// The first thread will have index 0.
std::atomic<uint32_t> Thread::next_index_{ 0 };

/// No thread IDs have been used yet.
std::atomic<bool> Thread::id_used_[kMaxNumThreads] = {};

#ifdef COUNT_ACTIVE_THREADS
std::atomic<int32_t> Thread::current_num_threads_ { 0 };
#endif

/// Give the new thread an ID. (In this implementation, threads get IDs when they are created, and
/// release them when they are freed. We will eventually merge chkulk's improvements, from another
/// branch, and then threads will get IDs on their first call to FasterKv::StartSession(), while
/// still releasing IDs when they are freed.)
thread_local Thread::ThreadId Thread::id_{};

}
} // namespace FASTER::core
