# Supplementary Material for "F2: Designing a Key-Value Store for Large Skewed Workloads"

An instance of F<sup>2</sup> can be created by using the `FasterKvHC` class, located in `core/faster_hc.h`. This class provides a straightforward interface to the user to perform `Read`, `Upsert`, `RMW` and `Delete` operations.

Core F<sup>2</sup> functionality has been implemented in the following files:

* The *HybridLog* implementation can be found in `core/faster.h`. This file also contains our new *lookup-based* compaction implementation (i.e., `CompactWithLookup` method), which utilize our new primitive (i.e., `ConditionalInsert`) to enable safe, concurrent, latch-free operation.
* The *Hot-log index* implementation can be found in `index/mem_index.h`.
* The *Cold-log index* implementation (i.e., *two-level design*) can be found in `index/faster_index.h` and `index/faster_index_contexts.h`.
* The *Read-Cache* code is located in `core/read_cache.h`.

A usage example that benchmarks F<sup>2</sup> can be found [here](https://github.com/kkanellis/FASTER/blob/cc-lmhc-v2/cc/benchmark-dir/ycsb_benchmark.cc).

### Build Instructions

To build F<sup>2</sup> tests, navigate to the root (i.e., `cc/` directory), and run the following:

```
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make -j hot_cold
```

To build the benchmark, run the following:

```
cmake -DCMAKE_BUILD_TYPE=Release .. && make -j ycsb_benchmark
```

**NOTE**: CMake 3.2.2+ and compatible C++14 compiler required. Tested on Ubuntu Linux with GCC v11.
