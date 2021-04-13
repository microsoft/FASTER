---
layout: splash
permalink: /
hidden: true
header:
  overlay_color: "#5e616c"
  overlay_image: /assets/images/faster-banner.png
  actions:
    - label: "Get Started"
      url: "/docs/quick-start-guide/"
excerpt: >
  A fast concurrent persistent key-value store and log, in C# and C++.<br />
  <small><a href="https://github.com/microsoft/FASTER/releases/tag/v1.8.2">Latest release v1.8.2</a></small>
features:
  - image_path: /assets/images/faster-feature-1.png
    alt: "feature1"
    title: "Feature 1"
    excerpt: "Feature 1 excerpt"
  - image_path: /assets/images/faster-feature-2.png
    alt: "feature2"
    title: "Feature 2"
    excerpt: "Feature 1 excerpt"
  - image_path: /assets/images/faster-feature-3.png
    alt: "feature3"
    title: "Feature 3"
    excerpt: "Feature 3 excerpt"
---

Managing large application state easily, resiliently, and with high performance is one of the hardest
problems in the cloud today. The FASTER project offers two artifacts to help tackle this problem.

* **FASTER Log** is a high-performance concurrent persistent recoverable log, iterator, and random 
reader library in C#. It supports very frequent commit operations at low latency, and can quickly saturate 
disk bandwidth. It supports both sync and async interfaces, handles disk errors, and supports checksums. Learn 
more about using the FASTER Log in C# [here](docs/fasterlog-basics/).

* **FASTER KV** is a concurrent key-value store + cache (available in C# and C++) that is designed for point 
lookups and heavy updates. FASTER supports data larger than memory, by leveraging fast external 
storage (local or cloud). It also supports consistent recovery using a new checkpointing technique that lets 
applications trade-off performance for commit latency. Learn more about the FASTER KV in C# 
[here](docs/fasterkv-basics/). For the FASTER C++ port, check [here](docs/fasterkv-cpp/).

# Key Features

1. Latch-free cache-optimized index, in FASTER KV.
2. A fast persistent recoverable append-only log based on fine-grained epoch protection for concurrency, 
in FASTER Log.
3. Unique “hybrid record log” design in FASTER KV, that combines the above log with in-place updates, to 
shape the memory working set and retain performance.
4. Architecture as a component that can be embedded in multi-threaded cloud apps. 
5. Asynchronous non-blocking recovery model based on group commit.
6. A rich extensible storage device abstraction called `IDevice`, with implementations for local
storage, cloud storage, tiered storage, and sharded storage.
7. A new high performance remote interface, making the store accessible via TCP from remote clients.

For standard benchmarks where the working set fits in main memory, we found FASTER KV to achieve
significantly higher throughput than current systems, and match or exceed the performance of pure 
in-memory data structures while offering more functionality. See our [research papers](docs/td-research-papers/)
for more details. We also have a detailed analysis of C# FASTER KV performance in a wiki page 
[here](https://github.com/Microsoft/FASTER/wiki/Performance-of-FASTER-in-C%23). The performance of the 
C# and C++ versions of FASTER is very similar. FASTER Log is also extremely fast, capable of saturating modern
NVMe SSDs using less than a core of CPU, and scaling well in a multi-threaded setting.

# News and Updates

* A high-performance remote (TCP) interface to FasterKV is now available! It provides linear server scalability with increasing client sessions, with similar server throughput as embedded FasterKV. We provide a default C# client as well as an ability to extend to other protocols. Learn more [here](docs/remote-basics/).

* Async API support has been improved, with async versions of `Upsert` and `Delete` added. Spin-wait has been significantly reduced in the codebase.

* We support variable-length keys and values in FasterKV C# via `Memory<byte>` and more generally `Memory<T> where T : unmanaged` as key/value/input types. We also added
a new type called `SpanByte` to represent variable-length keys and values. See the sample [here](https://github.com/Microsoft/FASTER/tree/master/cs/samples/StoreVarLenTypes) for details on these capabilities. This is in addition to the existing object-log support for class types.

* We support C# async in FASTER KV (and FASTER Log). See the guides for [FasterKV](docs/fasterkv-basics/) and [FasterLog](docs/fasterlog-basics/) 
for more information. Also, check out the samples [here](https://github.com/Microsoft/FASTER/tree/master/cs/samples).


# Getting Started

* Quick-Start Guide and Docs: [here](docs/quick-start-guide/)
* Source Code: [https://github.com/microsoft/FASTER](https://github.com/microsoft/FASTER)
* C# Samples: [https://github.com/microsoft/FASTER/tree/master/cs/samples](https://github.com/microsoft/FASTER/tree/master/cs/samples)
* C# NuGet binary feed:
  * [Microsoft.FASTER.Core](https://www.nuget.org/packages/Microsoft.FASTER.Core/)
  * [Microsoft.FASTER.Devices.AzureStorage](https://www.nuget.org/packages/Microsoft.FASTER.Devices.AzureStorage/)
* Checkpointing and Recovery: [link](docs/fasterkv-basics/#checkpointing-and-recovery)
* Research papers: [link](docs/td-research-papers/)
* Project roadmap: [here](docs/roadmap)

Thanks to [Minimal Mistakes](https://github.com/mmistakes/minimal-mistakes) for their amazing website theme.

# Embedded key-value store sample

```cs
public static void Main()
{
  using var log = Devices.CreateLogDevice("hlog.log"); // backing storage device
  using var store = new FasterKV<long, long>(1L << 20, // hash table size (number of 64-byte buckets)
     new LogSettings { LogDevice = log } // log settings (devices, page size, memory size, etc.)
     );

  // Create a session per sequence of interactions with FASTER
  using var s = store.NewSession(new SimpleFunctions<long, long>());
  long key = 1, value = 1, input = 10, output = 0;
  
  // Upsert and Read
  s.Upsert(ref key, ref value);
  s.Read(ref key, ref output);
  Debug.Assert(output == value);
  
  // Read-Modify-Write (add input to value)
  s.RMW(ref key, ref input);
  s.RMW(ref key, ref input);
  s.Read(ref key, ref output);
  Debug.Assert(output == value + 20);
}
```
