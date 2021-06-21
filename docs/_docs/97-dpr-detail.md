---
title: "Technical Details - DPR"
permalink: /docs/dpr-details/
excerpt: "Technical Details - DPR"
last_modified_at: 2021-06-18
toc: false
classes: wide
---

We cover advanced topics in libDPR here. For a complete overview you can check out [our paper](https://tli2.github.io/assets/pdf/dpr-sigmod2021.pdf).

## Full StateObject API
`SimpleStateObject` is good enough for many cases, but may be undesirable as it assumes an underlying state object
that has no notion of versioning, and imposes a versioning scheme on top. Because `SimpleStateObject` views the
underlying storage as a black box, it has to play it safe and prevent checkpointing when batches are being
processed. This is unnecessary for sophisticated state objects, such as the FASTER K-V store.

`IStateObject` is an API designed for such state objects -- it assumes that the state object will perform
its own versioning, and that checkpointing and recovery calls are asynchronous. `SimpleStateObject` extends
`IStateObject` to hide this complexity from, well, simpler state objects.

To use the full `IStateObject` interface, simply invoke the correct method in `DprWorkerCallbacks` at appropriate
points in the state object's execution. When using `DprServer`, it is up to the user to fill in the version tracker
with correct versions as it executes batches of requests.

## Graph DPR Finder

The Graph DPR Finder is our default option for DPR finder service in a small to medium sized cluster. It achieves
fault-tolerance by persisting its state on storage before exposing results to the external world. On failure, Graph
DPR Finder can simply be restarted on the same storage, and the rest of the cluster will not observe any anomalies.

Underneath the hood, Graph DPR Finder performs exact dependency tracking as described by the paper, but keeps the 
precedence graph completely in memory. Only the DPR cut itself is written to storage. On failure, the in-memory
graph will be lost, but the restarted Graph DPR Finder will use a fallback mechanism of assuming a densely connected
graph, which allows it to still commit versions in the lost portion of the graph. 

You can find the implementation under `dprfinder/graph`

