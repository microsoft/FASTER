# Introduction

Managing large application state easily and with high performance is one of the hardest problems
in the cloud today. FASTER is a concurrent key-value store + cache that is designed for point 
lookups and heavy updates. FASTER supports data larger than memory, by leveraging fast external 
storage. It also supports consistent recovery using a new checkpointing technique that lets 
applications trade-off performance for commit latency.

What differentiates FASTER from a technical perspective are (1) its latch-free cache-optimized index; 
(2) its unique “hybrid record log” design that combines a traditional persistent append-only log with 
in-place updates, to shape the memory working set and retain performance; (3) its architecture as a 
component that can be embedded in multi-threaded cloud apps; and (4) its asynchronous recovery model 
based on group commit.

For standard benchmarks where the working set fits in main memory, we found FASTER to achieve
significantly higher throughput than current systems, and match or exceed the performance of pure 
in-memory data structures while offering more functionality. See [the SIGMOD paper](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf) for more details. We also have a detailed
analysis of C# FASTER performance in a wiki page 
[here](https://github.com/Microsoft/FASTER/wiki/Performance-of-FASTER-in-C%23).

# Getting Started

Go to [our website](http://aka.ms/FASTER) for more details and papers.

# Build and Test

For C#, see [here](https://github.com/Microsoft/FASTER/tree/master/cs).

For C++, see [here](https://github.com/Microsoft/FASTER/tree/master/cc).

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
