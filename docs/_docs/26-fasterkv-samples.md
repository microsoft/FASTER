---
title: "FasterKV Samples"
permalink: /docs/fasterkv-samples/
excerpt: "FasterKV Samples"
last_modified_at: 2020-11-08
toc: false
classes: wide
---

* Main samples: [github](https://github.com/Microsoft/FASTER/tree/master/cs/samples).
* Playground with advanced samples: [github](https://github.com/Microsoft/FASTER/tree/master/cs/playground).
* Benchmarking code is present at [cs/benchmark](https://github.com/Microsoft/FASTER/tree/master/cs/benchmark).
* Unit tests are a useful resource to see how FASTER is used as well. They are in [/cs/test](https://github.com/Microsoft/FASTER/tree/master/cs/test).
* The sum-store, located [here](https://github.com/Microsoft/FASTER/tree/master/cc/playground/sum_store-dir), is a good example of checkpointing and recovery.
* FasterKV supports variable-length keys and values via `Memory<byte>` and more generally `Memory<T> where T : unmanaged` as key/value/input types. We also 
support a predefined type called `SpanByte`. See the sample 
[here](https://github.com/Microsoft/FASTER/tree/master/cs/samples/StoreVarLenTypes) for details on these capabilities. Both of these represent inlined variable-length
keys and values without a separate object-log on disk, and are a high-performance alternative to the standard C# class type support using a separate object-log.
