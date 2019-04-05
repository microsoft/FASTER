[![NuGet](https://img.shields.io/nuget/v/FASTER.svg)](https://www.nuget.org/packages/FASTER/)
[![Build Status](https://dev.azure.com/ms/FASTER/_apis/build/status/Microsoft.FASTER)](https://dev.azure.com/ms/FASTER/_build/latest?definitionId=8)
[![Gitter](https://badges.gitter.im/Microsoft/FASTER.svg)](https://gitter.im/Microsoft/FASTER?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

# Introduction

Managing large application state easily and with high performance is one of the hardest problems
in the cloud today. FASTER is a concurrent key-value store + cache that is designed for point 
lookups and heavy updates. FASTER supports data larger than memory, by leveraging fast external 
storage. It also supports consistent recovery using a new checkpointing technique that lets 
applications trade-off performance for commit latency.

The following features of FASTER differentiate it from a technical perspective:
1. Latch-free cache-optimized index.
2. Unique “hybrid record log” design that combines a traditional persistent append-only log with in-place updates, to shape the memory working set and retain performance.
3. Architecture as a component that can be embedded in multi-threaded cloud apps. 
4. **NEW**: Asynchronous recovery model based on group commit (called [CPR](#Recovery-in-FASTER)).

For standard benchmarks where the working set fits in main memory, we found FASTER to achieve
significantly higher throughput than current systems, and match or exceed the performance of pure 
in-memory data structures while offering more functionality. See [the SIGMOD paper](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf) for more details. We also have a detailed
analysis of C# FASTER performance in a wiki page 
[here](https://github.com/Microsoft/FASTER/wiki/Performance-of-FASTER-in-C%23). The performance of the 
C# and C++ versions of FASTER are very similar.

# Getting Started

Visit our [research website](http://aka.ms/FASTER) for technical details and papers.

:new: For FASTER usage and getting started information, head over to our 
[GitHub Pages](https://microsoft.github.io/FASTER) website. A detailed guide to getting started with FASTER 
C# is also available in the repository at [this link](https://github.com/Microsoft/FASTER/blob/master/cs/README.md). FASTER 
C# binaries are available via [NuGet](https://www.nuget.org/packages/FASTER/).

:new: You can take a look at the project roadmap [here](https://microsoft.github.io/FASTER/roadmap).

# Build and Test

For C#, click [here](https://github.com/Microsoft/FASTER/tree/master/cs).

For C++, click [here](https://github.com/Microsoft/FASTER/tree/master/cc).

# Recovery in FASTER

Both the C# and C++ version of FASTER support asynchronous checkpointing and recovery, based on a new
recovery model called Concurrent Prefix Recovery (CPR for short). You can read more about CPR in our research
paper [here](https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf) (to appear in 
SIGMOD 2019). Briefly, CPR is based on (periodic) group commit. However, instead of using an expensive 
write-ahead log (WAL) which can kill FASTER's high performance, CPR: (1) provides a semantic description of committed
operations, of the form “all operations until offset Ti in session i”; and (2) uses asynchronous 
incremental checkpointing instead of a WAL to implement group commit in a scalable bottleneck-free manner.

CPR is available in the C# and C++ versions of FASTER. More documentation on recovery in the C# version is
[here](https://github.com/Microsoft/FASTER/tree/master/cs#checkpointing-and-recovery). For C++, we only
have examples in code right now. The sum-store, located [here](https://github.com/Microsoft/FASTER/tree/master/cc/playground/sum_store-dir), is a good example of checkpointing and recovery.

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
