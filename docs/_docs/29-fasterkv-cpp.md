---
title: "FasterKV C++ Port"
permalink: /docs/fasterkv-cpp/
excerpt: "FasterKV C++ Port"
last_modified_at: 2025-04-08
toc: true
---

## Building C++ FASTER

The C++ version of FASTER uses CMake for builds. To build C++ FASTER, create
one or more build directories and use CMake to set up build scripts for your
target OS. Once CMake has generated the build scripts, it will try to update
them, as needed, during ordinary build.

### Building on Windows

Create new directory "build" off the root directory (FASTER\cc). From the new
"build" directory, execute:

```sh
cmake .. -G "<MSVC compiler>"
```

To see a list of supported MSVC compiler versions, just run "cmake -G". As of
this writing, we're using Visual Studio 2022, so you would execute:

```sh
cmake .. -G "Visual Studio 17 2022"
```

That will create build scripts inside your new "build" directory, including
a "FASTER.sln" file that you can use inside Visual Studio. CMake will add several
build profiles to FASTER.sln, including Debug/x64 and Release/x64.

### Building on Linux

The Linux build requires several packages (both libraries and header files);
see "CMakeFiles.txt" in the root directory (FASTER/cc) for the list of libraries
being linked to, on Linux.

As of this writing, the required libraries are:

- stdc++fs : for <experimental/filesytem>, used for cross-platform directory
             creation.
- uuid : support for GUIDs.
- tbb : Intel's Thread Building Blocks library, used for concurrent_queue.
- gcc (any version supporting C++17 )
- aio : Kernel Async I/O, used by QueueFile / QueueIoHandler.
- stdc++
- pthread : thread library.

Also, CMake on Linux, for the gcc compiler, generates build scripts for either
Debug or Release build, but not both; so you'll have to run CMake twice, in two
different directories, to get both Debug and Release build scripts.

Create new directories "build/Debug" and "build/Release" off the root directory
(FASTER/cc). From "build/Debug", run:

```sh
cmake -DCMAKE_BUILD_TYPE=Debug ../..
```

--and from "build/Release", run:

```sh
cmake -DCMAKE_BUILD_TYPE=Release ../..
```

Then you can build Debug or Release binaries by running "make" inside the
relevant build directory.

### Other options

You can try other generators (compilers) supported by CMake. The main CMake
build script is the CMakeLists.txt located in the root directory (FASTER/cc).

## FasterKv

Before instantiating the FasterKv store, you need to first define the *Key*, *Value* and *Disk* types at compile-time. For instance, below we define a 8-byte Key, a 100-byte Value, and a Disk class for local storage. You can find more key/value example type definitions (e.g., variable-size key and value definitions) in [`test_types.h`](https://github.com/microsoft/FASTER/blob/main/cc/test/test_types.h) (as part of the testing suite).

```cpp
// Define key type
struct Key {
    uint64_t key;

    Key(uint64_t key_) : key{ key_ } { }

    // Required for the hash table
    inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(Key));
    }
    // Hash Function for the key
    inline KeyHash GetHash() const {
        return KeyHash{
            FasterHashHelper<uint64_t>::compute(key_) };
    }
    // Comparison operators.
    inline bool operator==(const Key& other) const {
        return key_ == other.key_;
    }
    inline bool operator!=(const Key& other) const {
        return key_ != other.key_;
    }
};
// Define value type
struct Value {
    uint8_t value[100];

    Value(const uint8_t* value_) {
        std::memcpy(value, value_, 100);
    }
    // Required for the records log
    inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(Value));
    }
};

// I/O handler class definition
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif
constexpr static uint64_t kFileSegmentSize = (1 << 30ULL); // 1GiB file segments
typedef FASTER::device::FileSystemDisk<handler_t, kFileSegmentSize> Disk;
```

### Constructors

Now, you can easily instantiate a FasterKv store by directly passing a small set of options into the constructor:

```cpp
// Create FasterKv instance with:
// - 1M hash table entries
// - 1GB log size
// - 90% mutable fraction
// - No pre-allocation
FasterKv<Key, Value, Disk> store{
    2 * (1 << 20),        // Hash table size (2M entries)
    4 * (1 << 30),        // In-memory Log size (4 GiB)
    "/path/to/hlog-dir",  // Log path
    0.9,                  // Log Mutable fraction
};
```

Alternatively, there is also a constructor that accepts a single `FasterKvConfig` class instance that contains per-component configuration classes, enabling full FasterKv customization
(their definitions can be found in [config.h](https://github.com/microsoft/FASTER/blob/main/cc/src/core/config.h)):

```cpp
struct FasterKvConfig {
  IndexConfig index_config;   // Hash index
  HlogConfig hlog_config;     // Hybrid log
  ReadCacheConfig rc_config;  // Read-cache
  HlogCompactionConfig hlog_compaction_config; // Compaction policy
  std::string filepath;       // Path on disk
}
```

By default, both read-cache and automatic HybridLog compactions are disabled.

Below, we show an example of instantiating a FasterKv store with read-cache and automatic log compactions enabled:

```cpp
// Configure hash index
typedef FasterKv<Key, Value, Device>::IndexConfig IndexConfig;
IndexConfig index_config {
  .num_buckets = 2 * (1 << 20);  // 2M buckets
};

// Configure hybrid log
HlogConfig log_config {
  .in_mem_size = 4*(1 << 30),  // 4 GiB in-mem log size
  .mutable_fraction = 0.9,     // 90% mutable
  .pre_allocate = false        // Do not pre-allocate log
};

// Configure read cache
ReadCacheConfig rc_config {
  .mem_size = 512 * (1 << 20),  // 512 MiB cache
  .mutable_fraction = 0.5,      // 50%
  .pre_allocate = false,        // Do not pre-allocate
  .enabled = true               // Enable read-cache
};

// Configure hlog compaction policy
HlogCompactionConfig hc_config{
  .check_interval = 250ms,              // check size every 250ms
  .trigger_pct = 0.8,                   // trigger compact at 80% of budget
  .compact_pct = 0.2,                   // compact 20% of log
  .hlog_size_budget = 10 * (1 << 30ULL),// hlog size budget
  .max_compacted_size = 1 * (1 << 30),  // limit max compacted size
  .num_threads = 4,                     // number of compaction threads
  .enabled = true                       // Enabled automatic compactions
};

// Create FasterKv instance
typedef FasterKv<Key, Value, Device>::Config Config;
FasterKv<Key, Value, device_type> store{
  Config{ index_config, log_config, "hlog", rc_config, hc_config }
};
```


### Usage Example

Here's a basic example of using FasterKv:

```cpp
#include "faster.h"

using namespace FASTER::core;

// Define key and value types
struct Key { ... }
struct Value { ... };

// Define operation contexts for
// Read, Upsert, and RMW operations
class ReadContext : public IAsyncContext { ... };
class UpsertContext : public IAsyncContext { ... };
class RmwContext : public IAsyncContext { ... };

int main() {
    // Initialize FasterKv setting
    uint64_t hash_table_size = 1 << 20;    // 1 M hash table entries
    uint64_t log_size = 4 * (1 << 30);     // 4 GiB log size
    const char* log_path = "/path/to/hlog";
    double mutable_fraction = 0.9;         // 90% mutable region

    // Create FasterKv instance
    FasterKv<Key, Value, FASTER::device::NullDisk> store{
        hash_table_size, log_size, log_path, mutable_fraction
    };

    // Callback for async operations
    auto callback = [](IAsyncContext* ctxt, Status result) {
        if (result != Status::Ok) {
            std::cerr << "Error in async operation!" << std::endl;
        }
    };

    // Start a session
    auto session_id = store.StartSession();

    // Perform operations
    {
        // Upsert
        UpsertContext upsert_context{ Key{1}, Value{100} };
        store.Upsert(upsert_context, callback, 1);

        // Read
        ReadContext read_context{ Key{1} };
        Status result = store.Read(read_context, callback, 2);
        if (result == Status::Ok) {
            std::cout << "Read value: " << read_context.value << std::endl;
        }

        // RMW (increment by 50)
        RmwContext rmw_context{ Key{1}, Value{50} };
        store.Rmw(rmw_context, callback, 3);

        // Complete any pending operations
        store.CompletePending(true);

        // Read updated value
        ReadContext read_context2{ Key{1} };
        result = store.Read(read_context2, callback, 4);
        if (result == Status::Ok) {
            std::cout << "Updated value: " << read_context2.value << std::endl;
        }
    }

    // Stop session
    store.StopSession();

    return 0;
}
```


## F2Kv

F2Kv extends FasterKv by introducing a *two-tier* storage architecture that automatically manages data movement between hot and cold stores.
While FasterKv uses a single hybrid log, F2Kv maintains two separate FasterKv instances: a *hot* store for frequently accessed data and a *cold* store for less active data.
To reduce the in-memory overhead of the latter, F2Kv also uses a two-level indexing structure that spans both memory and disk (i.e., *cold-index*).
Finally, a read-cache can be optionally be integrated with the hot store, to serve disk-resident read-hot records either hot or cold store.

This hot-cold separation provides several advantages:

1. The hot store can be optimized for fast access and frequent updates (i.e., larger in-memory mutable region) and read caching.
2. The cold store can be optimized for storage efficiency.
3. Data automatically migrates between tiers based on observed access patterns.
4. Each tier can have its own compaction policy, allowing for more efficient space management.
5. Memory usage is optimized by keeping only frequently accessed data in the hot store.

### User Operations

Upserts always go to the hot store. When reading data, F2Kv first checks the hot store, then falls back to the cold store, if necessary.
Similarly, for updates (i.e., RMW), F2Kv first checks the hot store, performing an RMW there (if record exists). Otherwise, it reads the latest record from the cold-store and inserts the updated value to the hot store.
F2Kv guarantees system consistency, even when multiple threads operate on the same record(s).

### Background Operations

F2Kv handles data movement between tiers automatically, using multi-threaded *lookup*-based compaction processes.
Specifically, it employs a *hot-cold* compaction process to move (older) hot store records to the cold store, once the hot store size budget is reached. Similarly, it triggers a cold-cold compaction (once the cold store size budget is reached), which completely removes deleted records from the system.
F2Kv ensures that compactions can be performed concurrently to user operations in a safe manner.

### Indexing Infrequently-accessed records

To minimize the memory overhead of indexing infrequently-accessed records residing in the cold store, F2Kv employs a new two-level index that spans both memory and disk, effectively reducing the indexing overhead to 1 byte per key.
Specifically, F2Kv first groups multiple hash buckets together, to create *hash chunks* (e.g., 32 entries per hash chunks), and then indexes these hash chunks in-memory.

F2Kv implements this two-level index design, using a dedicated FasterKv instance that stores hash chunks mostly on disk, but indexes them in-memory.
The user can configure (1) the number of entries per hash chunks (i.e., hash-chunk size), and (2) the total number of hash chunks.


### Basic Usage Example

```cpp
#include "faster.h"

// Define key, value and device types
struct Key { /* ... */ };
struct Value { /* ... */ };

// Create hot and cold store configurations
typename F2Kv<Key, Value, Disk>::HotIndexConfig HotIndexConfig
typename F2Kv<Key, Value, Disk>::ColdIndexConfig ColdIndexConfig;

// Index configuration for hot, cold stores
HotIndexConfig hot_index_config {
  .table_size = (1 << 30)   // 1M hot index hash table size
};
ColdIndexConfig cold_index_config {
  .table_size = (1 << 30),       // 1M cold index buckets (8M entries)
  .in_mem_size = 192 * (1 << 20),// 192 MiB in-memory region
  .mutable_fraction = 0.4        // 40% mutable
};

// Enable in-memoy read-cache (default size: 256 MiB)
ReadCacheConfig rc_config = DEFAULT_READ_CACHE_CONFIG;
rc_config.enabled = true;

// Enable automatic compactions for both hot, cold stores
//   - Hot log total budget: 2 GiB
//   - Cold log total budget 8 GiB
F2CompactionConfig compaction_config = DEFAULT_F2_COMPACTION_CONFIG;
compaction_config.hot_store.hlog_size_budget  = 2 * (1 << 30UL);
compaction_config.cold_store.hlog_size_budget = 8 * (1 << 30UL);

// Initialize F2Kv store
F2Kv<Key, Value, Disk> store(
  hot_index_config,         // Hot store index configuration
  (1 << 30),                // Hot store memory size (1 GiB)
  "/path/to/disk/hot-log",  // Hot store log dir
  cold_config,              // Cold store index configuration
  192 * (1 << 20),          // Cold store memory size (192 MiB)
  "/path/to/disk/cold-log", // Cold store log dir
  0.9,                      // Hot store mutable fraction
  0.0                       // Cold store mutable fraction
  rc_config,                // Read-cache configuration
  compaction_config         // Hlogs compaction configuration
);
```

The user can issue Upsert/Read/RMW operations to F2Kv, in a similar fashion to FasterKv:

```cpp
...
// F2Kv<Key, Value, Disk> store{ ... }

// Define operation contexts for
// Read, Upsert, and RMW operations
class ReadContext : public IAsyncContext { ... };
class UpsertContext : public IAsyncContext { ... };
class RmwContext : public IAsyncContext { ... };

// Callback for async operations
auto callback = [](IAsyncContext* ctxt, Status result) {
  if (result != Status::Ok) {
    std::cerr << "Error in async operation!" << std::endl;
  }
};

// Start a session
auto session_id = store.StartSession();

// Perform operations
{
  // Upsert
  UpsertContext upsert_context{ Key{1}, Value{100} };
  store.Upsert(upsert_context, callback, 1);

  // Read
  ReadContext read_context{ Key{1} };
  Status result = store.Read(read_context, callback, 2);
  if (result == Status::Ok) {
    std::cout << "Read value: " << read_context.value << std::endl;
  }

  // RMW (increment by 50)
  RmwContext rmw_context{ Key{1}, Value{50} };
  store.Rmw(rmw_context, callback, 3);

  // Complete any pending operations
  store.CompletePending(true);

  // Read updated value
  ReadContext read_context2{ Key{1} };
  result = store.Read(read_context2, callback, 4);
  if (result == Status::Ok) {
    std::cout << "Updated value: " << read_context2.value << std::endl;
  }
}

// Stop session
store.StopSession();
```

## Advanced Configuration Example

Below, we show how to configure every possible aspect of F2Kv, including separate compaction behavior for hot and cold stores, and non-default cold-index hash chunk size:

```cpp
// Define your types
struct Key { /* ... */ };
struct Value { /* ... */ };

// Hot Index class
using HI = MemHashIndex<Disk>;
// Cold Index class: Use 2^6 (=64 byte) sized hash chunks
using CI = ColdIndex<Disk, ColdLogHashIndexDefinition<6>>;
// F2Kv specialized class
using store_t = F2Kv<Key, Value, Disk, HI, CI>;

// Create hot and cold store configurations
// Create hot store configuration
typename store_t::HotIndexConfig HotIndexConfig
typename store_t::hot_faster_store_config_t hot_config;
hot_config.filepath = "/path/to/disk/hot-store/";

// Configure hot store's index
hot_config.index_config = HotIndexConfig{
  .table_size = (1 << 30)  // 1M hot index hash table size
}
// Configure hot store's hybrid log
hot_config.hlog_config = HlogConfig{
  .in_mem_size = (1 << 30UL),  // 1 GiB memory size
  .mutable_fraction = 0.6,     // 60% mutable region
  .pre_allocate = true         // Pre-allocate memory
};
// Configure hot store compaction
hot_config.hlog_copaction_config = HlogCompactionConfig {
  .check_interval = 500ms,                // Check every 0.5 second
  .trigger_pct = 0.8,
  .compact_pct = 0.2,
  .max_compacted_size = 512 * (1 << 20),  // Compact up to 512 MiB
  .hlog_size_budget = 2 * (1 << 30),      // 2 GiB total disk budget
  .num_threads = 8,                       // Use 8 threads
  .enabled = true
};
// Configure (hot store) read cache
hot-config.rc_config = ReadCacheConfig{
  .mem_size = 256_MiB,
  .mutable_fraction = 0.5,
  .pre_allocate_log = false,
  .enabled = true
};

// Create cold store configuration
typename store_t::cold_faster_store_config_t cold_config;
typename store_t::ColdIndexConfig ColdIndexConfig;
cold_config.filepath = "/path/to/disk/cold-store/";

// Configure cold store's index
cold_config.index_config {
  .table_size = (1 << 30),       // 1M cold index buckets (8M entries)
  .in_mem_size = 192 * (1 << 20),// 192 MiB in-memory region
  .mutable_fraction = 0.4        // 40% mutable
};
// Configure cold store's hybrid log
cold_config.hlog_config = HlogConfig{
  .in_mem_size = (192 << 20UL), // 192 MiB memory size
  .mutable_fraction = 0.4,      // 40% mutable region
  .pre_allocate = false         // Do not pre-allocate memory
};
// Configure cold store compaction
cold_config.hlog_copaction_config = HlogCompactionConfig {
  .check_interval = 2000ms,             // Check every 2 seconds
  .trigger_pct = 0.9,
  .compact_pct = 0.1,
  .max_compacted_size = (1 << 30),      // Compact up to 1 GiB
  .hlog_size_budget = 16 * (1 << 30UL), // 16 GiB total disk budget
  .num_threads = 4,                     // Use 4 threads
  .enabled = true
};

// Create F2Kv instance
store_t store{ hot_config, cold_config };

// Insert/Read/Update records...
...
```

### API Reference

F2Kv's tiered log implementation is located in  [`f2.h`](https://github.com/microsoft/FASTER/blob/main/cc/src/core/f2.h). F2Kv's cold-index implementation can be found in [`cold_index.h`](https://github.com/microsoft/FASTER/blob/main/cc/src/index/cold_index.h).

Below we provide current F2Kv's API, which should look very familiar for someone who has used FasterKv before:


#### Session Management

- `StartSession()`
Initiates a new session and returns a session identifier.

- `ContinueSession(const Guid& guid)`
Resumes an existing session identified by the provided GUID.

- `StopSession()`
Ends the current session.

#### Core Opeations

- `Read(RC& context, AsyncCallback callback, uint64_t monotonic_serial_num)`
Retrieves a value from the store. Searches hot store first, then cold store if not found.

- `Upsert(UC& context, AsyncCallback callback, uint64_t monotonic_serial_num)`
Inserts or updates a value in the hot store.

- `Rmw(MC& context, AsyncCallback callback, uint64_t monotonic_serial_num)`
Executes an atomic read-modify-write operation. If latest record resides in the cold log, it reads that first, before appending the new updating value to hot store.

- `Delete(DC& context, AsyncCallback callback, uint64_t monotonic_serial_num)`
Removes a key-value pair from the store.

#### Checkpointing Operations

- `Checkpoint(HybridLogPersistenceCallback callback, Guid& token, bool lazy = true)`
Creates a checkpoint of both stores. Lazy specifies whether the system will wait until the next hot-cold compaction to trigger a cold-store checkpoint.

- `Recover(const Guid& token, uint32_t& version, std::vector<Guid>& session_ids)`
Restores the store from a checkpoint.

#### Compaction Operations

- `CompactHotLog(uint64_t until_address, bool shift_begin_address, int n_threads)`
Compacts the hot store's log up to the specified address, using the multithreaded lookup-based compaction algorithm.

- `CompactColdLog(uint64_t until_address, bool shift_begin_address, int n_threads)`
Compacts the cold store's log up to the specified address, using the multi-threaded lookup-based compaction algorithm.

#### Monitoring Functions

- `Size()`
Returns the total size of both stores combined.

- `DumpDistribution()`
Outputs the distribution statistics for both stores.

- `NumActiveSessions()`
Returns the count of current active sessions.

- `AutoCompactionScheduled()`
Checks if automatic compaction is pending.

## Examples

There are several unit tests in [FASTER/cc/test](https://github.com/Microsoft/FASTER/tree/master/cc/test), for both FasterKv and F2Kv.

Sum-store, located in [FASTER/cc/playground/sum_store-dir](https://github.com/Microsoft/FASTER/tree/master/cc/playground/sum_store-dir), is a good example of
checkpointing and recovery.

There's a basic YCSB test driver in [FASTER/cc/benchmark-dir](https://github.com/Microsoft/FASTER/tree/master/cc/benchmark-dir):

- For FasterKv, check [benchmark.cc](https://github.com/Microsoft/FASTER/tree/master/cc/benchmark-dir/benchmark.cc).
- For F2Kv, check [benchmark_f2.cc](https://github.com/Microsoft/FASTER/tree/master/cc/benchmark-dir/benchmark_f2.cc)

## Remote Device

The C++ version's persistent layer can be extended from local storage to a remote tier using the
[`StorageDevice`](https://github.com/Microsoft/FASTER/tree/master/cc/src/device/storage.h). This
device stores data across a set of files locally, and transparently flushes them to a remote tier.
Reads are also transparently served from the appropriate tier too. This device is templated with
the remote tier, and we currently support [Azure blob storage](https://github.com/Microsoft/FASTER/tree/master/cc/src/device/azure.h).
An example along with unit tests can be found under [FASTER/cc/test/blobs](https://github.com/Microsoft/FASTER/tree/master/cc/test/blobs).

This device depends on  `cpprestsdk` and `azure-storage-cpp`. On Linux, these
can be installed using the helper script at
[FASTER/cc/scripts/linux/azure/blob.sh](https://github.com/Microsoft/FASTER/tree/master/cc/scripts/linux/azure/blob.sh).
On windows, use [vcpkg](https://github.com/microsoft/vcpkg) to install the `azure-storage-cpp:x64-windows` library, and
then run `vcpkg integrate install` to satisfy these dependencies.

To compile the unit tests, pass in `-DUSE_BLOBS=ON`
when generating build scripts with `cmake`. To run the unit tests, you will need an instance
of [Azurite](https://github.com/Azure/Azurite) running for Linux, or the
[Azure storage emulator](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator) running
for Windows.

If you run into issues while trying to setup this device, please refer to FASTER's
[CI](https://github.com/Microsoft/FASTER/tree/master/azure-pipelines.yml); it contains rules that setup and test
this layer on both, Linux and Windows.
