# Introduction

Managing large application state easily and with high performance is one of the hardest problems
in the cloud today. We present FASTER, a new concurrent key-value store designed for point lookups 
and heavy updates. FASTER supports data larger than memory, by leveraging fast external storage. 
What differentiates FASTER are its cache-optimized index that achieves very high performance — up
to 160 million operations per second when data fits in memory; its unique “hybrid record log” design
that combines a traditional persistent log with in-place updates, to shape the memory working set 
and retain performance; and its architecture as an component that can be embedded in cloud apps. FASTER
achieves higher throughput than current systems, by more than two orders of magnitude, and scales better 
than current pure in-memory data structures, for in-memory working sets. FASTER also offers a new consistent
recovery scheme that achieves better performance at the expense of slightly higher commit latency.

# Getting Started

Go to [our website](http://aka.ms/FASTER) for more details and papers.

# Build and Test in C#

Clone the repo, open /cs/src/FASTER.sln, build using VS 2017.

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