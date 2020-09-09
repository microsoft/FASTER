---
layout: default
title: Index
parent: Tuning FASTER
nav_order: 2
permalink: /tuning/index
---

# Managing Index Size

FASTER consists of a hash index (pure in-memory) that stores pointers to data, and logs that
store the actual key-value data itself (spanning memory and storage. This document discusses 
the hash index size. In FASTER, the number of main hash buckets is set by the constructor 
argument, called _index size_.

In C#:
```cs
store = new FasterKV<K, V, I, O, C, F>(indexSize, ...);
```
In C++:
```cpp
FasterKv<Key, Value, disk_t> store{ index_size, ... };
```

Each hash bucket is of size 64 bytes (one cache line). It holds 8 _hash entries_, each of size 8 bytes.
The 8th entry is a pointer to an overflow bucket (allocated separately at page granularity in memory). Each
overflow bucket is sized at 8 entries, similar to the main bucket. Each entry itself is 8 bytes long.

The invariant is that there will be at most one hash entry in a bucket, per _hash tag_, where hash tag stands
for the 14 most significant bits in the 64-bit hash code of the key. All colliding keys having the same bucket 
and hash tag are organized as a (reverse) linked list starting from from the hash entry.

This means that one hash bucket may have up to 2^14 = 16384 hash entries (spread across the main and overflow 
buckets). Thus, when the main hash table is sized too small, we may have lots of overflow buckets because of many
distinct tags. In the worst case, we might have 2^14 hash entries per hash bucket when many keys map to the same
hash bucket, but with different hash tag values. Said differently, the **worst case** space utilization of the 
hash index is around 2^14 * (index size in #buckets) * 8 bytes.

You can check the “shape and fill” of the hash index by calling fht.DumpDistribution() after the keys are loaded.
If you find many entries per bucket, and you can afford the memory space for the index, it is usually better to have
sized the main hash table (_index size_) larger to begin with.

If you want to control the total memory used by the index, fortunately, the maximum number of entries per hash bucket
can be controlled easily, by bounding the number of distinct tags in the hash code. For example, if we set the most 
significant 9 bits of the hash code to zero, there will be only 2^5 = 32 distinct tags<sup id="a1">[1](#f1)</sup>. 
This means that we will have at most 32 hash entries per bucket, resulting in each main bucket having at most four 
overflow buckets in the worst case. This will lead to a smaller upper bound on memory used by the hash index. The 
downside is more collisions and longer hash chains, leading to potentially slower lookup performance.


<b id="f1">[1]</b> In FASTER C++, the tag bits are read from bits 48-61 of the 64-bit hash value, instead of bits 50-63 
in FASTER C#. Here, bit 0 stands for the least significant bit of the number.
