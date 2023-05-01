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
#include "core/faster_hc.h"
#include "core/utility.h"
#include "device/null_disk.h"
#include "../test/test_types.h"

#include <libconfig.h++>
#define LOOKUP_VALUE(config, path, value) {                                           \
  if (!config.lookupValue(path, value)) {                                             \
    std::string msg = std::string("Config lookup failed: ") + std::string(path);      \
    throw std::runtime_error(msg);                                                    \
  }                                                                                   \
}
#define to_MiB(x) (static_cast<uint64_t>(x) * (1 << 20));
#define to_GiB(x) (static_cast<uint64_t>(x) * (1 << 30));

using namespace std::chrono_literals;
using namespace libconfig;
using namespace FASTER::core;

using GenLock = FASTER::test::GenLock;
using AtomicGenLock = FASTER::test::AtomicGenLock;

/// Basic YCSB benchmark.
enum class Op : uint64_t {
  Invalid = 0,

  Read = 1,
  Upsert = 2,
  RMW = 3,
  Insert = 4,
  Scan = 5,
  Delete = 6,
};
std::string OpNames[] = {
  "INVALID", "READ", "UPSERT", "RMW", "INSERT", "SCAN", "DELETE",
};

// Global config
libconfig::Config config;

uint64_t kInitCount;
uint64_t kTxnCount;

struct BenchmarkInfo {
  enum Type {
    TIME_BASED,
    OPS_BASED,
  } type;

  unsigned warmup, run;
};
BenchmarkInfo benchmarkInfo;

static constexpr uint64_t kKeySize = 8;
static constexpr uint64_t kValueSize = 108;

// System-related
std::string sNodeType;
std::string sDiskDevice;
unsigned kTotalCoreCount;

// FASTER-related
unsigned kRefreshInterval;
unsigned kCompletePendingInterval;
uint64_t kColdIndexMemSize;
double dColdIndexMutableFraction;

// Misc
static constexpr uint64_t kChunkSize = 3'200;
static constexpr uint64_t kNanosPerSecond = 1'000'000'000;

std::atomic<uint64_t> idx_{ 0 };
std::atomic<bool> done_{ false };
std::atomic<uint64_t> total_duration_{ 0 };
std::atomic<uint64_t> total_inserts_done_{ 0 }; // only when populating store
std::atomic<uint64_t> total_reads_done_{ 0 };
std::atomic<uint64_t> total_writes_done_{ 0 };
uint64_t total_bytes_read_{ 0 };
uint64_t total_bytes_written_{ 0 };

template<class K, class V>
class ReadContext;
template<class K, class V>
class UpsertContext;
template<class K, class V>
class RmwContext;

typedef FasterHashHelper<uint64_t> HashFn;

/// This benchmark stores 8-byte keys in key-value store.
class Key {
 public:
  Key(uint64_t key)
    : key_{ key } {
  }
  Key(const Key& other)
    : key_{ other.key_ } {
  }

  /// Methods and operators required by the (implicit) interface:
  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Key));
  }
  inline KeyHash GetHash() const {
    HashFn hash;
    return KeyHash{ hash(key_) };
  }

  /// Comparison operators.
  inline bool operator==(const Key& other) const {
    return key_ == other.key_;
  }
  inline bool operator!=(const Key& other) const {
    return key_ != other.key_;
  }

 public:
  uint64_t key_;
};
static_assert(sizeof(Key) == 8, "sizeof(Key) != 8");
static_assert(sizeof(Key) == kKeySize, "sizeof(Key) != kKeySize");

class Value {
 public:
  Value() {
    std::memset(&bytes, 0, kValueSize);
    gen.store(0);
  }
  Value(const Value& other) {
    std::memcpy(bytes, other.bytes, sizeof(bytes));
    gen.store( other.gen.load() );
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Value));
  }
  void operator=(const Value& other) {
    std::memcpy(bytes, other.bytes, sizeof(bytes));
    gen.store( other.gen.load() );
  }

  inline void GetAtomic(const Value& from) {
    GenLock before, after;
    do {
      before = from.gen.load();
      std::memcpy(bytes, from.bytes, sizeof(bytes));
      after = from.gen.load();
    } while (before.gen_number != after.gen_number);
  }
  inline bool PutAtomic(Value& from) {
    bool replaced;
    while(!from.gen.try_lock(replaced) && !replaced) {
      std::this_thread::yield();
    }
    if (replaced) {
      // Some other thread replaced this record
      return false;
    }
    std::memcpy(bytes, from.bytes, sizeof(bytes));
    from.gen.unlock(false);
    return true;
  }
  inline void Rmw(const Value& old_value, Value& incr) {
    std::memcpy(bytes, incr.bytes, sizeof(bytes));
  }
  inline bool RmwAtomic(Value& incr) {
    bool replaced;
    while(!gen.try_lock(replaced) && !replaced) {
      std::this_thread::yield();
    }
    if (replaced) {
      // Some other thread replaced this record
      return false;
    }
    std::memcpy(bytes, incr.bytes, sizeof(bytes));
    gen.unlock(false);
    return true;
  }

  AtomicGenLock gen;
  uint8_t bytes[kValueSize];
};
static_assert(sizeof(Value) == kValueSize + sizeof(AtomicGenLock) + 4, "sizeof(Value) != kValueSize");

template<class K, class V>
class ReadContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  ReadContext(key_t key)
    : key_{ key } {
  }
  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ }
    , thread_reads_count_{ other.thread_reads_count_ } {
  }

  inline void set_benchmark_info(int64_t* thread_reads_count) {
    thread_reads_count_ = thread_reads_count;
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const key_t& key() const {
    return key_;
  }

  inline void Get(const value_t& value) {
    value_ = value;
  }
  inline void GetAtomic(const value_t& value) {
    value_.GetAtomic(value);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  key_t key_;
  value_t value_;
 public:
  int64_t* thread_reads_count_;
};

/// Class passed to store_t::Upsert().
template <class K, class V>
class UpsertContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  UpsertContext(key_t key, value_t new_value)
    : key_{ key }
    , new_value_{ new_value } {
  }
  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_{ other.key_ }
    , new_value_{ other.new_value_ }
    , thread_writes_count_{ other.thread_writes_count_ } {
  }

  inline void set_benchmark_info(int64_t* thread_writes_count) {
    thread_writes_count_ = thread_writes_count;
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const key_t& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

  /// Non-atomic and atomic Put() methods.
  inline void Put(value_t& value) {
    value = new_value_;
  }
  inline bool PutAtomic(value_t& value) {
    return value.PutAtomic(new_value_);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  key_t key_;
  value_t new_value_;
 public:
  int64_t* thread_writes_count_;
};

/// Class passed to store_t::Rmw().
template <class K, class V>
class RmwContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;

  RmwContext(key_t key, value_t incr)
    : key_{ key }
    , incr_{ incr } {
  }
  /// Copy (and deep-copy) constructor.
  RmwContext(const RmwContext& other)
    : key_{ other.key_ }
    , incr_{ other.incr_ }
    , thread_writes_count_{ other.thread_writes_count_ } {
  }

  inline void set_benchmark_info(int64_t* thread_writes_count) {
    thread_writes_count_ = thread_writes_count;
  }

  /// The implicit and explicit interfaces require a key() accessor.
  const key_t& key() const {
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
    value = incr_;
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    value.Rmw(old_value, incr_);
  }
  inline bool RmwAtomic(value_t& value) {
    return value.RmwAtomic(incr_);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  key_t key_;
  value_t incr_;
 public:
  int64_t* thread_writes_count_;
};

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif
typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;

/// Affinitize to hardware threads on different core first, before
/// moving on to the same cores (HyperThreading cores).
void SetThreadAffinity(size_t thread_idx) {
  if (thread_idx >= kTotalCoreCount * 2) {
    throw std::runtime_error{ "Too many threads to pin to cores" };
  }
  size_t core;
  cpu_set_t mask;
  CPU_ZERO(&mask);

  if (sNodeType == "r650") {
    core = thread_idx * 2;
  } else if (sNodeType == "c6525-100g") {
    core = thread_idx;
  } else {
    throw std::invalid_argument{ std::string("Unsupported node type: ") + sNodeType };
  }

  CPU_SET(core, &mask);
  fprintf(stderr, "[Thread: %3lu] -> [Core: %3lu, total: %3u]\n",
          thread_idx, core, kTotalCoreCount);

  ::sched_setaffinity(0, sizeof(mask), &mask);
}

bool get_disk_stats(uint64_t& bytes_read, uint64_t& bytes_written) {
  char* buffer;
  size_t buffer_size = 8192;
  char key[128], value[128];

  buffer = (char*)malloc(buffer_size * sizeof(char));
  if (!buffer) return false;

  FILE* fin = fopen("/proc/self/io", "r");
  if (!fin) return false;

  bytes_read = bytes_written = 0;
  while (!feof(fin)) {
    getline(&buffer, &buffer_size, fin);
    sscanf(buffer, "%[^:]: %s", key, value);

    if (strcmp(key, "read_bytes") == 0) {
      bytes_read = static_cast<uint64_t>(strtoull(value, NULL, 10));
    } else if (strcmp(key, "write_bytes") == 0) {
      bytes_written = static_cast<uint64_t>(strtoull(value, NULL, 10));
    }
  }
  return (bytes_read > 0 && bytes_written > 0) ? true : false;
}

template <class K, class V>
class LoadRequest {
 public:
  typedef K key_t;
  typedef V value_t;

  LoadRequest(key_t key_)
    : key{ key_ } {
  }

  key_t key;
};
static_assert(sizeof(LoadRequest<Key, Value>) == 8, "sizeof(LoadRequest) != 8");

template <class K, class V>
class TxtRequest {
 public:
  typedef K key_t;
  typedef V value_t;

  TxtRequest(Op operation_, key_t key_)
    : operation{ operation_ }
    , key{ key_ } {
  }

  Op operation;
  key_t key;
};
static_assert(sizeof(TxtRequest<Key, Value>) == 16, "sizeof(TxtRequest) != 16");

typedef LoadRequest<Key, Value> load_request_t;
typedef TxtRequest<Key, Value> txt_request_t;

aligned_unique_ptr_t<load_request_t> load_requests;
aligned_unique_ptr_t<txt_request_t> txt_requests;

template <class ReqType>
void load_file(const std::string& filename, aligned_unique_ptr_t<ReqType>& buffer, uint64_t num_requests) {
  constexpr size_t kFileChunkSize = 4096 * sizeof(ReqType);
  static_assert(kFileChunkSize % 512 == 0);

  auto chunk_guard = alloc_aligned<ReqType>(512, kFileChunkSize);
  ReqType* chunk = chunk_guard.get();

  printf("Loading keys from %s into memory...\n", filename.c_str());
  FASTER::benchmark::File file{ filename };
  buffer = alloc_aligned<ReqType>(64, num_requests * sizeof(ReqType));

  uint64_t count = 0;
  uint64_t offset = 0;
  while(true) {
    uint64_t size = file.Read(chunk, kFileChunkSize, offset);
    for(uint64_t idx = 0; idx < size / sizeof(ReqType); ++idx) {
      buffer.get()[count] = chunk[idx];
      ++count;
    }
    if(size == kFileChunkSize) {
      offset += kFileChunkSize;
    } else {
      break;
    }
  }
  if(num_requests != count) {
    printf("File %s load fail! [read %lu vs indicated %lu]\n",
          filename.c_str(), count, num_requests);
    exit(1);
  }
  printf("loaded %" PRIu64 " keys/ops.\n", count);
}

class RandomValueGenerator {
 public:
  typedef __uint128_t state_t;
  typedef uint64_t rand_t;
  constexpr static size_t rand_t_sz = sizeof(rand_t);
  static_assert(sizeof(state_t) == 16 && sizeof(rand_t) == 8);

  RandomValueGenerator(uint64_t seed, uint32_t length)
    : state_{ seed }
    , length_{ length } {
  }

  template <class V>
  void Update(V& value) {
    uint32_t div = length_ / rand_t_sz, remainder = length_ % rand_t_sz;

    auto bytes = reinterpret_cast<rand_t*>(value.bytes);
    for (uint32_t idx = 0; idx < div; idx++) {
      next_rand();
      bytes[idx] = rand_;
    }
    if (remainder > 0) {
      next_rand();
      std::memcpy(bytes + div, state_bytes_, remainder);
    }
  }

 private:
  inline void next_rand() {
    state_ *= 0xda942042e4dd58b5UL;
  }

  union {
    state_t state_;
    uint64_t rand_;
    char state_bytes_[sizeof(state_t)];
  };

  uint32_t length_;
};

template <class K, class V, class S>
void thread_setup_store(S* store, size_t thread_idx) {
  typedef K key_t;
  typedef V value_t;
  typedef UpsertContext<key_t, value_t> upsert_context_t;

  SetThreadAffinity(thread_idx);

  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<upsert_context_t> context{ ctxt };
    if (result == Status::Ok) {
      ++(*(context->thread_writes_count_));
    } else {
      throw std::runtime_error{ "thread_setup_store: Upsert was not successfull" };
    }
  };

  RandomValueGenerator rng{ thread_idx, kValueSize };
  value_t value;
  int64_t inserts_done = 0;

  Guid guid = store->StartSession();

  for(uint64_t chunk_idx = idx_.fetch_add(kChunkSize); chunk_idx < kInitCount;
      chunk_idx = idx_.fetch_add(kChunkSize)) {
    for(uint64_t idx = chunk_idx; idx < chunk_idx + kChunkSize; ++idx) {
      if(idx % kRefreshInterval == 0) {
        store->Refresh();
        if(idx % kCompletePendingInterval == 0) {
          store->CompletePending(false);
        }
      }

      rng.Update(value);
      upsert_context_t context{ load_requests.get()[idx].key, value };
      context.set_benchmark_info(&inserts_done);

      Status status = store->Upsert(context, callback, 1);
      if (status == Status::Ok || status == Status::Pending) {
        ++inserts_done;
      } else {
        throw std::runtime_error{ "thread_setup_store: Upsert was not successfull" };
      }
    }
  }

  store->CompletePending(true);
  store->StopSession();

  total_inserts_done_ += inserts_done;
}

template <class K, class V, class S>
void setup_store(S* store, size_t num_threads) {
  total_inserts_done_ = 0;

  auto start_time = std::chrono::high_resolution_clock::now();
  if (num_threads != kTotalCoreCount) {
    printf("[INFO]: Using %u threads to populate store...\n", kTotalCoreCount);
    num_threads = kTotalCoreCount;
  }

  idx_ = 0;
  std::deque<std::thread> threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(&thread_setup_store<K, V, S>, store, thread_idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  // Wait for all auto compactions to finish
  do {
    std::this_thread::sleep_for(2s);
  } while (store->NumActiveSessions() > 0);

  std::chrono::nanoseconds duration = std::chrono::high_resolution_clock::now() - start_time;

  load_requests.reset();
  printf("Finished populating store: contains %lu elements.\n", total_inserts_done_.load());
  printf("\tTook %.2lf seconds\n", static_cast<double>(duration.count()) / kNanosPerSecond);
  printf("\tDB size: %.2lf GB\n", static_cast<double>(store->Size()) / (1 << 30));
}

static std::atomic<int64_t> async_reads_done{ 0 };
static std::atomic<int64_t> async_writes_done{ 0 };

struct alignas(64) TimeSampleInfo {
  int64_t reads_done;
  int64_t writes_done;
};
TimeSampleInfo* thread_samples_info;

template <class K, class V, class S>
void thread_run_benchmark(S* store, size_t thread_idx, uint64_t max_ops) {
  typedef K key_t;
  typedef V value_t;
  typedef UpsertContext<K, V> upsert_context_t;
  typedef ReadContext<K, V> read_context_t;
  typedef RmwContext<K, V> rmw_context_t;

  SetThreadAffinity(thread_idx);

  auto start_time = std::chrono::high_resolution_clock::now();

  int64_t& reads_done = thread_samples_info[thread_idx].reads_done;
  int64_t& writes_done = thread_samples_info[thread_idx].writes_done;
  reads_done = 0;
  writes_done = 0;

  Guid guid = store->StartSession();

  RandomValueGenerator rng{ thread_idx, kValueSize };
  value_t value;

  while(!done_) {
    uint64_t chunk_idx = idx_.fetch_add(kChunkSize);
    while(chunk_idx >= kTxnCount) {
      if(chunk_idx == kTxnCount) {
        idx_ = 0;
      }
      chunk_idx = idx_.fetch_add(kChunkSize);
    }
    if (chunk_idx >= max_ops) {
      // All threads collectively have done max number of ops
      done_ = true;
      break;
    }

    for(uint64_t idx = chunk_idx; idx < chunk_idx + kChunkSize; ++idx) {
      if(idx % kRefreshInterval == 0) {
        store->Refresh();
        if(idx % kCompletePendingInterval == 0) {
          store->CompletePending(false);
        }
      }

      auto& request = txt_requests.get()[idx];
      switch(request.operation) {
        case Op::Insert:
        case Op::Upsert: {
          auto callback = [](IAsyncContext* ctxt, Status result) {
            CallbackContext<upsert_context_t> context{ ctxt };
            if (result == Status::Ok) {
              ++(*(context->thread_writes_count_));
            }
          };

          rng.Update(value);
          upsert_context_t context{ request.key, value };
          context.set_benchmark_info(&writes_done);
          Status result = store->Upsert(context, callback, 1);
          if (result == Status::Ok) {
            ++writes_done;
          }
          break;
        }
        case Op::Read: {
          auto callback = [](IAsyncContext* ctxt, Status result) {
            CallbackContext<read_context_t> context{ ctxt };
            ++(*(context->thread_reads_count_));
          };

          read_context_t context{ request.key };
          context.set_benchmark_info(&reads_done);
          Status result = store->Read(context, callback, 1);
          if (result == Status::Ok || result == Status::NotFound) {
            ++reads_done;
          } else if (result != Status::Pending) {
            throw std::runtime_error{ "A.." };
          }
          break;
        }
        case Op::RMW: {
          auto callback = [](IAsyncContext* ctxt, Status result) {
            CallbackContext<rmw_context_t> context{ ctxt };
            if (result == Status::Ok) {
              ++(*(context->thread_writes_count_));
            }
          };

          rng.Update(value);
          rmw_context_t context{ request.key, value };
          context.set_benchmark_info(&writes_done);
          Status result = store->Rmw(context, callback, 1);
          if(result == Status::Ok) {
            ++writes_done;
          }
          break;
        }
        case Op::Scan:
          printf("Scan currently not supported!\n");
          exit(1);
          break;
        default:
          throw std::runtime_error{ "Invalid Operation "};
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

template <class K, class V, class S>
void run_benchmark(S* store, size_t num_threads, bool warmup) {
  if(!get_disk_stats(total_bytes_read_, total_bytes_written_)) {
    printf("[WARN] Could *NOT* find disk stats\n");
  }

  idx_ = 0;
  total_duration_ = 0;
  total_reads_done_ = 0;
  total_writes_done_ = 0;
  done_ = false;
  thread_samples_info = new TimeSampleInfo[num_threads];

  uint64_t max_ops = (benchmarkInfo.type == benchmarkInfo.OPS_BASED)
        ? (warmup ? benchmarkInfo.warmup : benchmarkInfo.run)
        : std::numeric_limits<uint64_t>::max();

  std::deque<std::thread> threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(
      &thread_run_benchmark<K, V, S>, store, thread_idx, max_ops);
  }

  static std::atomic<uint64_t> num_checkpoints;
  num_checkpoints = 0;

  // Decide run duration based on benchmark type (TIME vs OPS)
  std::chrono::seconds run_duration;
  if (benchmarkInfo.type == benchmarkInfo.TIME_BASED) {
    run_duration = warmup ? std::chrono::seconds(benchmarkInfo.warmup)
                          : std::chrono::seconds(benchmarkInfo.run);
  } else {
    run_duration = std::chrono::seconds(600); // set to 15 minutes max
  }

  auto start_time = std::chrono::high_resolution_clock::now();
  auto end_time = start_time + run_duration;

  auto next = start_time + 1s;
  std::vector<TimeSampleInfo> reqs_per_sec;
  std::vector<TimeSampleInfo> total_reqs;

  reqs_per_sec.push_back(TimeSampleInfo{0, 0});
  total_reqs.push_back(TimeSampleInfo{0, 0});

  // Wait 1 second
  std::this_thread::sleep_until(next);

  while (true) {
    auto now = std::chrono::high_resolution_clock::now();
    next = now + 1s;

    // Keep track of completed read/write requests
    int64_t total_read_requests = 0, total_write_requests = 0;
    for (int i = 0; i < num_threads; i++) {
      total_read_requests += thread_samples_info[i].reads_done;
      total_write_requests += thread_samples_info[i].writes_done;
    }
    // delta from previous
    int64_t read_requests = total_read_requests - total_reqs.back().reads_done;
    int64_t write_requests = total_write_requests - total_reqs.back().writes_done;

    total_reqs.push_back(TimeSampleInfo{ total_read_requests, total_write_requests });
    reqs_per_sec.push_back(TimeSampleInfo{ read_requests, write_requests });

    std::chrono::nanoseconds since_start = now - start_time;
    fprintf(stderr, "[%.3lf secs] READS = %12lu | WRITES = %12lu\n",
            static_cast<double>(since_start.count()) / 1000000000ULL,
            read_requests, write_requests);

    if (done_) {
      // If max ops have been executed, threads will set done_ to true
      assert(benchmarkInfo.type == benchmarkInfo.OPS_BASED);
      fprintf(stderr, "Max ops [%lu] have been reached!\n", max_ops);
      break;
    }

    if (next < end_time) {
      std::this_thread::sleep_until(next);
      continue;
    }

    std::this_thread::sleep_until(end_time);
    if (benchmarkInfo.type == benchmarkInfo.OPS_BASED) {
      fprintf(stderr, "Max run duration [%ld seconds] reached!\n", run_duration.count());
    }
    break;
  };

  // Wait until all threads have joined...
  done_ = true;
  for(auto& thread : threads) {
    thread.join();
  }
  delete[] thread_samples_info;

  if (warmup) {
    // Quickly go to benchmark round!
    return;
  }

  // Capture Disk statistics after round
  uint64_t bytes_read_prev = total_bytes_read_;
  uint64_t bytes_written_prev = total_bytes_written_;
  bool disk_stat_success = get_disk_stats(total_bytes_read_, total_bytes_written_);
  if(!disk_stat_success) {
    printf("[WARN] Could *NOT* find disk stats\n");
  } else {
    total_bytes_read_ -= bytes_read_prev;
    total_bytes_written_ -= bytes_written_prev;
  }

  // Print Perf statistics
  printf("Finished benchmark: %" PRIu64 " thread checkpoints completed;  %.2f ops/second/thread\n",
         num_checkpoints.load(),
         ((double)total_reads_done_ + (double)total_writes_done_) / ((double)total_duration_ /
             kNanosPerSecond));
  printf("Single thread\t: %.3lf Kops/sec\n",
    (static_cast<double>(total_reads_done_) + static_cast<double>(total_writes_done_)) /
    (static_cast<double>(total_duration_) / kNanosPerSecond));
  printf("All threads [%lu]: %.3lf Kops/sec\n", num_threads,
    (static_cast<double>(total_reads_done_) + static_cast<double>(total_writes_done_)) /
    (static_cast<double>(total_duration_) / num_threads / kNanosPerSecond));

  // Print Disk statistics
  if (disk_stat_success) {
    printf("Reads from Disk [MB]\t: %.3lf\n", static_cast<double>(total_bytes_read_) / (1 << 20));
    printf("Read  Amplification\t: %.2lf\n", static_cast<double>(total_bytes_read_) / (total_reads_done_ * (kKeySize + kValueSize)));
    printf("Writes to Disk [MB]\t: %.3lf\n", static_cast<double>(total_bytes_written_) / (1 << 20));
    printf("Write  Amplification\t: %.2lf\n", static_cast<double>(total_bytes_written_) / ((total_writes_done_) * (kKeySize + kValueSize)));
  } else {
    printf("** ERROR when getting disk stats **\n");
  }
}

// FASTER
using store_t = FasterKv<Key, Value, disk_t>;
// FASTER-HC
using store_hc_t = FasterKvHC<Key, Value, disk_t>;

template<class S, bool HC = false>
struct FasterKvHelper {
  static void OnSetupStoreInit(S* store) {
  }
  static void OnSetupStoreDone(S* store) {
  }
};

template<class S>
struct FasterKvHelper<S, true> {
  static void OnSetupStoreInit(S* store) {
  }
  static void OnSetupStoreDone(S* store) {
    // Reduce cold-index buffer size
    printf("Performing UnsafeBufferResize...[%.2lf, %.2lf]\n",
      static_cast<double>(kColdIndexMemSize) / (1 << 20), dColdIndexMutableFraction);
    store->cold_store.hash_index_.store_->StartSession();
    //store->cold_store.hash_index_.store_->hlog.UnsafeBufferResize(512_MiB, 0.8);
    store->cold_store.hash_index_.store_->hlog.UnsafeBufferResize(kColdIndexMemSize, dColdIndexMutableFraction);
    store->cold_store.hash_index_.store_->StopSession();
    printf("Performing UnsafeBufferResize: Done!\n");
  }
};

template<class S, bool LMHC = std::is_same<S, store_hc_t>::value>
void run(S* store, size_t num_threads) {
  // Setup store -- load data
#ifdef STATISTICS
  store->DisableStatsCollection();
#endif
  FasterKvHelper<S, LMHC>::OnSetupStoreInit(store);

  printf("Populating the store...\n");
  printf("\tKey   Size: %lu\n", kKeySize);
  printf("\tValue Size: %lu\n", kValueSize);
  setup_store<Key, Value, S>(store, num_threads);

  while (store->AutoCompactionScheduled()) {
    printf("Compaction in progress -- # Active Sessions: %u\n", store->NumActiveSessions());
    std::this_thread::sleep_for(2s);
  }
  store->DumpDistribution();

  FasterKvHelper<S, LMHC>::OnSetupStoreDone(store);

  printf("Sleeping for 5 seconds...\n");
  std::this_thread::sleep_for(5s);

  // Warm-up round
  printf("Running warmup-round on %" PRIu64 " threads...\n", num_threads);
  run_benchmark<Key, Value, S>(store, num_threads, true);

  while (store->AutoCompactionScheduled()) {
    printf("Compaction in progress -- # Active Sessions: %u\n", store->NumActiveSessions());
    std::this_thread::sleep_for(2s);
  }

  // Benchmark round
#ifdef STATISTICS
  store->EnableStatsCollection();
#endif

  printf("Running benchmark on %" PRIu64 " threads...\n", num_threads);
  run_benchmark<Key, Value, S>(store, num_threads, false);

  store->SignalStopAutoCompaction();

  #ifdef STATISTICS
  store->PrintStats();

  // Wait until all compaction-related ops are finished
  while (store->AutoCompactionScheduled()) {
    printf("Compaction in progress -- # Active Sessions: %u\n", store->NumActiveSessions());
    std::this_thread::sleep_for(2s);
  }

  store->PrintStats();
  #endif
}

void ParseConfig(std::string& config_filepath) {
  printf("Config: %s\n", config_filepath.c_str());
  fprintf(stderr, "Config: %s\n", config_filepath.c_str());

  config.readFile(config_filepath);
  unsigned init_count, txn_count;
  LOOKUP_VALUE(config, "init_count", init_count);
  kInitCount = init_count;
  LOOKUP_VALUE(config, "txn_count", txn_count);
  kTxnCount = txn_count;

  assert(kInitCount % kChunkSize == 0);
  assert(kTxnCount % kChunkSize == 0);

  // Benchmark run info (e.g., time-based or ops-based)
  unsigned warmup, run;
  try {
    LOOKUP_VALUE(config, "benchmark_info.warmup_seconds", warmup);
    LOOKUP_VALUE(config, "benchmark_info.run_seconds", run);

    benchmarkInfo.type = benchmarkInfo.TIME_BASED;
    benchmarkInfo.warmup = warmup;
    benchmarkInfo.run = run;
  } catch (...) {
    LOOKUP_VALUE(config, "benchmark_info.warmup_ops", warmup);
    LOOKUP_VALUE(config, "benchmark_info.run_ops", run);

    if (run > kTxnCount) {
      throw std::invalid_argument{ "Run ops should be *NO MORE* than total txns found in file!" };
    }

    benchmarkInfo.type = benchmarkInfo.OPS_BASED;
    benchmarkInfo.warmup = warmup;
    benchmarkInfo.run = run;
  }

  // System-related options
  LOOKUP_VALUE(config, "machine.node_type", sNodeType);
  LOOKUP_VALUE(config, "machine.disk_device", sDiskDevice);
  LOOKUP_VALUE(config, "machine.core_count", kTotalCoreCount);

  // FASTER-related global options
  LOOKUP_VALUE(config, "faster_lmhc.refresh_interval", kRefreshInterval);
  LOOKUP_VALUE(config, "faster_lmhc.complete_pending_inverval", kCompletePendingInterval);
  assert(kCompletePendingInterval % kRefreshInterval == 0);

  // Sanity check
  unsigned key_size, value_size;
  LOOKUP_VALUE(config, "key_size", key_size);
  assert(key_size == kKeySize);
  LOOKUP_VALUE(config, "value_size", value_size);
  assert(value_size == kValueSize);
}

int main(int argc, char* argv[]) {
  if(argc != 2 && argc != 3) {
    printf("Usage: benchmark.exe <config_filepath> [num_threads]\n");
    exit(0);
  }

  load_requests.reset();
  txt_requests.reset();

  // Parse config file
  std::string config_filepath{ argv[1] };
  ParseConfig(config_filepath);

  unsigned int num_threads;
  LOOKUP_VALUE(config, "num_threads", num_threads);
  if (argc == 3) {
    num_threads = std::atoi(argv[2]);
    fprintf(stderr, "[WARN]: Overriding num_threads: %u\n", num_threads);
  }

  std::string load_filename, run_filename;
  LOOKUP_VALUE(config, "load_filename", load_filename);
  LOOKUP_VALUE(config, "run_filename", run_filename);

  load_file<load_request_t>(load_filename, load_requests, kInitCount);
  load_file<txt_request_t>(run_filename, txt_requests, kTxnCount);


  // FasterKvHC
  {
    unsigned mem_size, size_budget;

    // read-cache
    ReadCacheConfig rc_config;
    LOOKUP_VALUE(config, "faster_lmhc.read_cache.enabled", rc_config.enabled);
    LOOKUP_VALUE(config, "faster_lmhc.read_cache.mem_size_mb", mem_size);
    rc_config.mem_size = to_MiB(mem_size);
    LOOKUP_VALUE(config, "faster_lmhc.read_cache.mutable_fraction", rc_config.mutable_fraction);
    // hot-cold compaction
    HCCompactionConfig hc_compaction_config;
    LOOKUP_VALUE(config, "faster_lmhc.hc_compaction.hot_store_size_budget_gb", size_budget);
    hc_compaction_config.hot_store.hlog_size_budget = to_GiB(size_budget);
    LOOKUP_VALUE(config, "faster_lmhc.hc_compaction.cold_store_size_budget_gb", size_budget);
    hc_compaction_config.cold_store.hlog_size_budget = to_GiB(size_budget);

    // hot-store
    LOOKUP_VALUE(config, "faster_lmhc.hot_store.mem_size_mb", mem_size);
    uint64_t hot_store_mem_size = to_MiB(mem_size);
    double hot_store_mutable_frac;
    LOOKUP_VALUE(config, "faster_lmhc.hot_store.mutable_fraction", hot_store_mutable_frac);
    // cold-log
    LOOKUP_VALUE(config, "faster_lmhc.cold_store.mem_size_mb", mem_size);
    uint64_t cold_store_mem_size =  to_MiB(mem_size);
    double cold_store_mutable_frac;
    LOOKUP_VALUE(config, "faster_lmhc.cold_store.mutable_fraction", cold_store_mutable_frac);
    // cold-index
    LOOKUP_VALUE(config, "faster_lmhc.cold_store.index_mem_size_mb", mem_size);
    kColdIndexMemSize = to_MiB(mem_size);
    LOOKUP_VALUE(config, "faster_lmhc.cold_store.index_mutable_fraction", dColdIndexMutableFraction);


    unsigned hot_table_size;
    LOOKUP_VALUE(config, "faster_lmhc.hot_store.table_size", hot_table_size);
    printf("Hot Table size: %u\n", hot_table_size);

    unsigned cold_table_size;
    LOOKUP_VALUE(config, "faster_lmhc.cold_store.table_size", cold_table_size);

    if (cold_table_size != 1048576) {
      //cold_table_size = 4194304;  //   64B-256MB
      //cold_table_size = 2097152;  //  128B-128MB
      cold_table_size = 1048576;  //  256B-64MB
      //cold_table_size = 524288;   //  512B-32MB
      //cold_table_size = 262144;   // 1024B-16MB
      //cold_table_size = 131072;   // 2048B-8MB
      //cold_table_size = 65536;    // 4096B-4MB

      fprintf(stderr, "OVERRIDE COLD-LOG INDEX # CHUNKS: %u\n", cold_table_size);
      printf("OVERRIDE COLD-LOG INDEX # CHUNKS: %u\n", cold_table_size);
    }
    printf("Cold Table size: %u\n", cold_table_size);

    store_hc_t store{
      hot_table_size, hot_store_mem_size, "storage-hc/hot_",
      cold_table_size, cold_store_mem_size, "storage-hc/cold_",
      hot_store_mutable_frac, cold_store_mutable_frac,
      rc_config, hc_compaction_config };

    run(&store, num_threads);
  }

  // FasterKvHC -- in-mem cold index
  /*
  {
    unsigned mem_size, size_budget;

    // read-cache
    ReadCacheConfig rc_config;
    LOOKUP_VALUE(config, "faster.read_cache.enabled", rc_config.enabled);
    LOOKUP_VALUE(config, "faster.read_cache.mem_size_mb", mem_size);
    rc_config.mem_size = to_MiB(mem_size);
    LOOKUP_VALUE(config, "faster.read_cache.mutable_fraction", rc_config.mutable_fraction);
    // hot-cold compaction
    HCCompactionConfig hc_compaction_config;
    LOOKUP_VALUE(config, "faster.hc_compaction.hot_store_size_budget_gb", size_budget);
    hc_compaction_config.hot_store.hlog_size_budget = to_GiB(size_budget);
    LOOKUP_VALUE(config, "faster.hc_compaction.cold_store_size_budget_gb", size_budget);
    hc_compaction_config.cold_store.hlog_size_budget = to_GiB(size_budget);

    // hot-store
    LOOKUP_VALUE(config, "faster.hot_store.mem_size_mb", mem_size);
    uint64_t hot_store_mem_size = to_MiB(mem_size);
    double hot_store_mutable_frac;
    LOOKUP_VALUE(config, "faster.hot_store.mutable_fraction", hot_store_mutable_frac);
    // cold-log
    LOOKUP_VALUE(config, "faster.cold_store.mem_size_mb", mem_size);
    uint64_t cold_store_mem_size =  to_MiB(mem_size);
    double cold_store_mutable_frac;
    LOOKUP_VALUE(config, "faster.cold_store.mutable_fraction", cold_store_mutable_frac);

    //size_t hot_table_size = next_power_of_two(kInitCount / 32);
    unsigned hot_table_size;
    LOOKUP_VALUE(config, "faster.hot_store.table_size", hot_table_size);
    printf("Hot Table size: %u\n", hot_table_size);

    unsigned cold_table_size;
    LOOKUP_VALUE(config, "faster.cold_store.table_size", cold_table_size);
    printf("Cold Table size: %u\n", cold_table_size);

    store_hc_t store{
      hot_table_size, hot_store_mem_size, "storage-hc/hot_",
      cold_table_size, cold_store_mem_size, "storage-hc/cold_",
      hot_store_mutable_frac, cold_store_mutable_frac,
      rc_config, hc_compaction_config };

    run(&store, num_threads);
  }
  */
  // FasterKv
  /*
  {
    unsigned mem_size, size_budget;

    // read-cache
    ReadCacheConfig rc_config;
    LOOKUP_VALUE(config, "faster.read_cache.enabled", rc_config.enabled);
    LOOKUP_VALUE(config, "faster.read_cache.mem_size_mb", mem_size);
    rc_config.mem_size = to_MiB(mem_size);
    LOOKUP_VALUE(config, "faster.read_cache.mutable_fraction", rc_config.mutable_fraction);

    // hot log (i.e. single log)
    LOOKUP_VALUE(config, "faster.hot_store.mem_size_mb", mem_size);
    uint64_t hot_store_mem_size = to_MiB(mem_size);
    double hot_store_mutable_frac;
    LOOKUP_VALUE(config, "faster.hot_store.mutable_fraction", hot_store_mutable_frac);

    //size_t table_size = next_power_of_two(kInitCount / 512);
    unsigned hot_table_size;
    LOOKUP_VALUE(config, "faster.hot_store.table_size", hot_table_size);
    printf("Hot Table size: %u\n", hot_table_size);

    HlogCompactionConfig compaction_config;
    compaction_config.enabled = true;
    compaction_config.max_compacted_size = 512_MiB;
    LOOKUP_VALUE(config, "faster.compaction.hot_store_size_budget_gb", size_budget);
    compaction_config.hlog_size_budget = to_GiB(size_budget);

    store_t store{ hot_table_size, hot_store_mem_size, "storage/", hot_store_mutable_frac,
                    rc_config, compaction_config };
    run(&store, num_threads);
  }
  */

  return 0;
}
