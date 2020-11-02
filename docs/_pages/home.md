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
  <small><a href="https://github.com/microsoft/FASTER/releases/tag/v1.7.4">Latest release v1.7.4</a></small>
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

Managing large application state easily and with high performance is one of the hardest problems
in the cloud today. FASTER is a concurrent key-value store + cache that is designed for point 
lookups and heavy updates. FASTER supports data larger than memory, by leveraging fast external 
storage. It also supports consistent recovery using a non-blocking checkpointing technique that lets 
applications trade-off performance for commit latency.

{% include feature_row_small id="features" %}


The following features of FASTER differentiate it from a technical perspective:

1. Latch-free cache-optimized index.
2. Unique “hybrid record log” design that combines a traditional persistent append-only log with in-place updates (cache), to shape the memory working set and retain performance.
3. Architecture as a component that can be embedded in multi-threaded cloud apps.
4. Asynchronous checkpoint/recovery support, based on group commit.

For standard benchmarks where the working set fits in main memory, we found FASTER to achieve
significantly higher throughput than current systems, and match or exceed the performance of pure 
in-memory data structures while offering more functionality. See [the SIGMOD paper](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf) for more details. We also have a detailed analysis of C# FASTER performance in a wiki page 
[here](https://github.com/Microsoft/FASTER/wiki/Performance-of-FASTER-in-C%23). The performance of the 
C# and C++ versions of FASTER are very similar.

# Getting Started

* Visit our [research page](http://aka.ms/FASTER) for technical details and papers.
* Start reading about FASTER C# [here](cs).
* FASTER C# binaries are available via NuGet:
  * [Microsoft.FASTER.Core](https://www.nuget.org/packages/Microsoft.FASTER.Core/)
  * [Microsoft.FASTER.Devices.AzureStorage](https://www.nuget.org/packages/Microsoft.FASTER.Devices.AzureStorage/)
* Start reading about FASTER C++ [here](cc).


# Embedded key-value store sample in C#

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

