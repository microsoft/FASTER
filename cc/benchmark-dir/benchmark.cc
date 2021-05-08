// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <atomic>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>

#include "file.h"

#include "core/auto_ptr.h"
#include "core/faster.h"
#include "device/null_disk.h"

using namespace std::chrono_literals;
using namespace FASTER::core;

/// Basic YCSB benchmark.

enum class Op : uint8_t {
  Insert = 0,
  Read = 1,
  Upsert = 2,
  Scan = 3,
  ReadModifyWrite = 4,
};

enum class Workload {
  A_50_50 = 0,
  RMW_100 = 1,
  A_100_0 = 2,
};

static constexpr uint64_t kInitCount = 250000000;
static constexpr uint64_t kTxnCount = 1000000000;
static constexpr uint64_t kChunkSize = 3200;
static constexpr uint64_t kRefreshInterval = 64;
static constexpr uint64_t kCompletePendingInterval = 1600;

static_assert(kInitCount % kChunkSize == 0, "kInitCount % kChunkSize != 0");
static_assert(kTxnCount % kChunkSize == 0, "kTxnCount % kChunkSize != 0");
static_assert(kCompletePendingInterval % kRefreshInterval == 0,
              "kCompletePendingInterval % kRefreshInterval != 0");

static constexpr uint64_t kNanosPerSecond = 1000000000;

static constexpr uint64_t kMaxKey = 268435456;
static constexpr uint64_t kRunSeconds = 30;
static constexpr uint64_t kCheckpointSeconds = 0;

aligned_unique_ptr_t<uint64_t> init_keys_;
aligned_unique_ptr_t<uint64_t> txn_keys_;
std::atomic<uint64_t> idx_{ 0 };
std::atomic<bool> done_{ false };
std::atomic<uint64_t> total_duration_{ 0 };
std::atomic<uint64_t> total_reads_done_{ 0 };
std::atomic<uint64_t> total_writes_done_{ 0 };

class ReadContext;
class UpsertContext;
class RmwContext;

/// This benchmark stores 8-byte keys in key-value store.
class Key {
 public:
  Key(uint64_t key)
    : key_{ key } {
  }

  /// Methods and operators required by the (implicit) interface:
  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Key));
  }
  inline KeyHash GetHash() const {
    return KeyHash{ Utility::GetHashCode(key_) };
  }

  /// Comparison operators.
  inline bool operator==(const Key& other) const {
    return key_ == other.key_;
  }
  inline bool operator!=(const Key& other) const {
    return key_ != other.key_;
  }

 private:
  uint64_t key_;
};

/// This benchmark stores an 8-byte value in the key-value store.
class Value {
 public:
  Value()
    : value_{ 0 } {
  }

  Value(const Value& other)
    : value_{ other.value_ } {
  }

  Value(uint64_t value)
    : value_{ value } {
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Value));
  }

  friend class ReadContext;
  friend class UpsertContext;
  friend class RmwContext;

 private:
  union {
    uint64_t value_;
    std::atomic<uint64_t> atomic_value_;
  };
};

/// Class passed to store_t::Read().
class ReadContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  ReadContext(uint64_t key)
    : key_{ key } {
  }

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }

  // For this benchmark, we don't copy out, so these are no-ops.
  inline void Get(const value_t& value) { }
  inline void GetAtomic(const value_t& value) { }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
};

/// Class passed to store_t::Upsert().
class UpsertContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  UpsertContext(uint64_t key, uint64_t input)
    : key_{ key }
    , input_{ input } {
  }

  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_{ other.key_ }
    , input_{ other.input_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

  /// Non-atomic and atomic Put() methods.
  inline void Put(value_t& value) {
    value.value_ = input_;
  }
  inline bool PutAtomic(value_t& value) {
    value.atomic_value_.store(input_);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  uint64_t input_;
};

/// Class passed to store_t::RMW().
class RmwContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  RmwContext(uint64_t key, uint64_t incr)
    : key_{ key }
    , incr_{ incr } {
  }

  /// Copy (and deep-copy) constructor.
  RmwContext(const RmwContext& other)
    : key_{ other.key_ }
    , incr_{ other.incr_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  const Key& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  inline static constexpr uint32_t value_size(const value_t& old_value) {
    return sizeof(value_t);
  }

  /// Initial, non-atomic, and atomic RMW methods.
  inline void RmwInitial(value_t& value) {
    value.value_ = incr_;
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    value.value_ = old_value.value_ + incr_;
  }
  inline bool RmwAtomic(value_t& value) {
    value.atomic_value_.fetch_add(incr_);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  uint64_t incr_;
};

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::QueueIoHandler handler_t;
//typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif
typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;
using store_t = FasterKv<Key, Value, disk_t>;

inline Op ycsb_a_50_50(std::mt19937& rng) {
  if(rng() % 100 < 50) {
    return Op::Read;
  } else {
    return Op::Upsert;
  }
}

inline Op ycsb_a_100_0(std::mt19937& rng) {
    return Op::Read;
}

inline Op ycsb_rmw_100(std::mt19937& rng) {
  return Op::ReadModifyWrite;
}

/// Affinitize to hardware threads on the same core first, before
/// moving on to the next core.
void SetThreadAffinity(size_t core) {

  // For now, assume 36 cores. (Set this correctly for your test system.)
  constexpr size_t kCoreCount = 36;
#ifdef _WIN32
  HANDLE thread_handle = ::GetCurrentThread();
  GROUP_AFFINITY group;
  group.Group = WORD(core / kCoreCount);
  group.Mask = KAFFINITY(0x1llu << (core - kCoreCount * group.Group));
  ::SetThreadGroupAffinity(thread_handle, &group, nullptr);
#else
  // On our 28-core test system, we see CPU 0, Core 0 assigned to 0, 28;
  //                                    CPU 1, Core 0 assigned to 1, 29; etc.
  cpu_set_t mask;
  CPU_ZERO(&mask);
#ifdef NUMA
  switch(core % 4) {
  case 0:
    // 0 |-> 0
    // 4 |-> 2
    // 8 |-> 4
    core = core / 2;
    break;
  case 1:
    // 1 |-> 28
    // 5 |-> 30
    // 9 |-> 32
    core = kCoreCount + (core - 1) / 2;
    break;
  case 2:
    // 2  |-> 1
    // 6  |-> 3
    // 10 |-> 5
    core = core / 2;
    break;
  case 3:
    // 3  |-> 29
    // 7  |-> 31
    // 11 |-> 33
    core = kCoreCount + (core - 1) / 2;
    break;
  }
#else
  switch(core % 2) {
  case 0:
    // 0 |-> 0
    // 2 |-> 2
    // 4 |-> 4
    core = core;
    break;
  case 1:
    // 1 |-> 28
    // 3 |-> 30
    // 5 |-> 32
    core = (core - 1) + kCoreCount;
    break;
  }
#endif
  CPU_SET(core, &mask);

  ::sched_setaffinity(0, sizeof(mask), &mask);
#endif
}

void load_files(const std::string& load_filename, const std::string& run_filename) {
  constexpr size_t kFileChunkSize = 131072;

  auto chunk_guard = alloc_aligned<uint64_t>(512, kFileChunkSize);
  uint64_t* chunk = chunk_guard.get();

  FASTER::benchmark::File init_file{ load_filename };

  printf("loading keys from %s into memory...\n", load_filename.c_str());

  init_keys_ = alloc_aligned<uint64_t>(64, kInitCount * sizeof(uint64_t));
  uint64_t count = 0;

  uint64_t offset = 0;
  while(true) {
    uint64_t size = init_file.Read(chunk, kFileChunkSize, offset);
    for(uint64_t idx = 0; idx < size / 8; ++idx) {
        init_keys_.get()[count] = chunk[idx];
      ++count;
    }
    if(size == kFileChunkSize) {
      offset += kFileChunkSize;
    } else {
      break;
    }
  }
  if(kInitCount != count) {
    printf("Init file load fail!\n");
    exit(1);
  }

  printf("loaded %" PRIu64 " keys.\n", count);

  FASTER::benchmark::File txn_file{ run_filename };

  printf("loading txns from %s into memory...\n", run_filename.c_str());

  txn_keys_ = alloc_aligned<uint64_t>(64, kTxnCount * sizeof(uint64_t));

  count = 0;
  offset = 0;

  while(true) {
    uint64_t size = txn_file.Read(chunk, kFileChunkSize, offset);
    for(uint64_t idx = 0; idx < size / 8; ++idx) {
        txn_keys_.get()[count] = chunk[idx];
      ++count;
    }
    if(size == kFileChunkSize) {
      offset += kFileChunkSize;
    } else {
      break;
    }
  }
  if(kTxnCount != count) {
    printf("Txn file load fail!\n");
    exit(1);
  }
  printf("loaded %" PRIu64 " txns.\n", count);
}

void thread_setup_store(store_t* store, size_t thread_idx) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
    assert(result == Status::Ok);
  };

  SetThreadAffinity(thread_idx);

  Guid guid = store->StartSession();

  uint64_t value = 42;
  for(uint64_t chunk_idx = idx_.fetch_add(kChunkSize); chunk_idx < kInitCount;
      chunk_idx = idx_.fetch_add(kChunkSize)) {
    for(uint64_t idx = chunk_idx; idx < chunk_idx + kChunkSize; ++idx) {
      if(idx % kRefreshInterval == 0) {
        store->Refresh();
        if(idx % kCompletePendingInterval == 0) {
          store->CompletePending(false);
        }
      }

      UpsertContext context{ init_keys_.get()[idx], value };
      store->Upsert(context, callback, 1);
    }
  }

  store->CompletePending(true);
  store->StopSession();
}

void setup_store(store_t* store, size_t num_threads) {
  idx_ = 0;
  std::deque<std::thread> threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(&thread_setup_store, store, thread_idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  init_keys_.reset();

  printf("Finished populating store.\n");
}


static std::atomic<int64_t> async_reads_done{ 0 };
static std::atomic<int64_t> async_writes_done{ 0 };

template <Op(*FN)(std::mt19937&)>
void thread_run_benchmark(store_t* store, size_t thread_idx) {
  SetThreadAffinity(thread_idx);

  std::random_device rd{};
  std::mt19937 rng{ rd() };

  auto start_time = std::chrono::high_resolution_clock::now();

  uint64_t upsert_value = 0;
  int64_t reads_done = 0;
  int64_t writes_done = 0;

  Guid guid = store->StartSession();

  while(!done_) {
    uint64_t chunk_idx = idx_.fetch_add(kChunkSize);
    while(chunk_idx >= kTxnCount) {
      if(chunk_idx == kTxnCount) {
        idx_ = 0;
      }
      chunk_idx = idx_.fetch_add(kChunkSize);
    }
    for(uint64_t idx = chunk_idx; idx < chunk_idx + kChunkSize; ++idx) {
      if(idx % kRefreshInterval == 0) {
        store->Refresh();
        if(idx % kCompletePendingInterval == 0) {
          store->CompletePending(false);
        }
      }
      switch(FN(rng)) {
      case Op::Insert:
      case Op::Upsert: {
        auto callback = [](IAsyncContext* ctxt, Status result) {
          CallbackContext<UpsertContext> context{ ctxt };
        };

        UpsertContext context{ txn_keys_.get()[idx], upsert_value };
        Status result = store->Upsert(context, callback, 1);
        ++writes_done;
        break;
      }
      case Op::Scan:
        printf("Scan currently not supported!\n");
        exit(1);
        break;
      case Op::Read: {
        auto callback = [](IAsyncContext* ctxt, Status result) {
          CallbackContext<ReadContext> context{ ctxt };
        };

        ReadContext context{ txn_keys_.get()[idx] };

        Status result = store->Read(context, callback, 1);
        ++reads_done;
        break;
      }
      case Op::ReadModifyWrite:
        auto callback = [](IAsyncContext* ctxt, Status result) {
          CallbackContext<RmwContext> context{ ctxt };
        };

        RmwContext context{ txn_keys_.get()[idx], 5 };
        Status result = store->Rmw(context, callback, 1);
        if(result == Status::Ok) {
          ++writes_done;
        }
        break;
      }
    }
  }

  store->CompletePending(true);
  store->StopSession();

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::nanoseconds duration = end_time - start_time;
  total_duration_ += duration.count();
  total_reads_done_ += reads_done;
  total_writes_done_ += writes_done;
  printf("Finished thread %" PRIu64 " : %" PRIu64 " reads, %" PRIu64 " writes, in %.2f seconds.\n",
         thread_idx, reads_done, writes_done, (double)duration.count() / kNanosPerSecond);
}

template <Op(*FN)(std::mt19937&)>
void run_benchmark(store_t* store, size_t num_threads) {
  idx_ = 0;
  total_duration_ = 0;
  total_reads_done_ = 0;
  total_writes_done_ = 0;
  done_ = false;
  std::deque<std::thread> threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(&thread_run_benchmark<FN>, store, thread_idx);
  }

  static std::atomic<uint64_t> num_checkpoints;
  num_checkpoints = 0;

  if(kCheckpointSeconds == 0) {
    std::this_thread::sleep_for(std::chrono::seconds(kRunSeconds));
  } else {
    auto callback = [](Status result, uint64_t persistent_serial_num) {
      if(result != Status::Ok) {
        printf("Thread %" PRIu32 " reports checkpoint failed.\n",
               Thread::id());
      } else {
        ++num_checkpoints;
      }
    };

    auto start_time = std::chrono::high_resolution_clock::now();
    auto last_checkpoint_time = start_time;
    auto current_time = start_time;

    uint64_t checkpoint_num = 0;

    while(current_time - start_time < std::chrono::seconds(kRunSeconds)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      current_time = std::chrono::high_resolution_clock::now();
      if(current_time - last_checkpoint_time >= std::chrono::seconds(kCheckpointSeconds)) {
        Guid token;
        bool success = store->Checkpoint(nullptr, callback, token);
        if(success) {
          printf("Starting checkpoint %" PRIu64 ".\n", checkpoint_num);
          ++checkpoint_num;
        } else {
          printf("Failed to start checkpoint.\n");
        }
        last_checkpoint_time = current_time;
      }
    }
  }

  done_ = true;

  for(auto& thread : threads) {
    thread.join();
  }

  printf("Finished benchmark: %" PRIu64 " thread checkpoints completed;  %.2f ops/second\n",
         num_checkpoints.load(),
         ((double)num_threads * ((double)total_reads_done_ + (double)total_writes_done_)) / ((double)total_duration_ /
             kNanosPerSecond));
  printf("# %.2f\n",
      ((double)num_threads * ((double)total_reads_done_ + (double)total_writes_done_)) / ((double)total_duration_ /
          kNanosPerSecond));
}

void run(Workload workload, size_t num_threads) {
  // FASTER store has a hash table with approx. kInitCount / 2 entries and a log of size 16 GB
  //size_t init_size = next_power_of_two(kInitCount / 2);
  size_t init_size = next_power_of_two(kInitCount / 4); // 16B per key

  store_t store{ init_size, 17179869184, "storage" }; // large log
  //store_t store{ init_size, 1048576 * 192ULL, "D:\\storage" , 0.4 /*mutable_fraction*/ }; // small log - IOPS bound

  //store_t store{ init_size, 1048576 * 24ULL, "D:\\storage" , 0.4 /*mutable_fraction*/ }; // Use with kOffsetBits=22 in address.h

  // increase disk throttle limit when using > 16 threads
  if (num_threads > 16)
      store.throttle_limit *= 2;

  printf("Populating the store...\n");

  setup_store(&store, num_threads);

  store.DumpDistribution();

  printf("Running benchmark on %" PRIu64 " threads...\n", num_threads);
  switch(workload) {
  case Workload::A_50_50:
    run_benchmark<ycsb_a_50_50>(&store, num_threads);
    break;
  case Workload::A_100_0:
      run_benchmark<ycsb_a_100_0>(&store, num_threads);
      break;
  case Workload::RMW_100:
    run_benchmark<ycsb_rmw_100>(&store, num_threads);
    break;
  default:
    printf("Unknown workload!\n");
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  constexpr size_t kNumArgs = 4;
  if(argc != kNumArgs + 1) {
    printf("Usage: benchmark.exe <workload> <# threads> <load_filename> <run_filename>\n");
    exit(0);
  }

  Workload workload = static_cast<Workload>(std::atol(argv[1]));
  size_t num_threads = ::atol(argv[2]);
  std::string load_filename{ argv[3] };
  std::string run_filename{ argv[4] };
  load_files(load_filename, run_filename);

  run(workload, num_threads);

  return 0;
}
