---
title: "Tuning FasterKV"
permalink: /docs/fasterkv-tuning/
excerpt: "Tuning FasterKV"
last_modified_at: 2020-11-08
toc: false
classes: wide
---

## Configuring the Hybrid Log

Recall that FASTER consists of the hash index that connects to a hybrid log spanning storage and main 
memory. The hybrid log may be configured using the `LogSettings` input to the FasterKV constructor.

```cs
var fht = new FasterKV<Key, Value>(1L << 20, new LogSettings { ... }, ...);
```

The following log settings are available for configuration:

* `LogDevice`: This is an instance of the storage interface (device/path/file) that will be used for the
main log. It is a class that implements the `IDevice` interface. FASTER provides several device implementations 
out of the box, such as `LocalStorageDevice` and `ManagedLocalStorageDevice` for local (or attached) storage.
You can use our extension method to easily create an instance of a local device:
    ```cs
    var log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log");
    ```
    When using FASTER in memory, you can set the device to a null device (`NullDevice`) by providing an empty
    string as parameter to the method above. You are responsible for disposing the device via `IDisposable`.

* `ObjectLogDevice`: If you are using C# class objects as FASTER keys and/or values, you need to also
provide storage for the object log, similar to the main log described above. You are responsible for disposing 
the device via `IDisposable`.

* `PageSizeBits`: This field (P) is used to indicate the size of each page. It is provided in terms of the number
of bits. You get the actual size of the page by a simple computation of two raised to the power of the number
specified (2<sup>P</sup>). For example, when `PageSizeBits` P is set to 12, it represents pages of size 4KB 
(since 2<sup>P</sup> = 2<sup>10</sup> = 4096 = 4KB). Generally you should not need to adjust page size from its
default value of 25 (= 32MB). You cannot use a page size smaller than the device sector size (512 bytes by 
default), which translates to a `PageSizeBits` value of at least 9.

* `MemorySizeBits`: This field (M) indicates the total size of memory used by the log. As before, for a setting
of M, 2<sup>M</sup> is the number of bytes used totally by the log. Since each page is of size 2<sup>P</sup>, the 
number of pages in memory is simply 2<sup>M-P</sup>. FASTER requires at least 2 pages in memory, so M should be 
set to at least P+1.

* `MutableFraction`: This field (F) indicates the fraction of the memory that will be treated as _mutable_, i.e.,
updates are performed in-place instead of creating a new version on the tail of the log. A value of 0.9 (the
default) indicates that 90% of memory will be mutable.

* `SegmentSize`: On disk, the data is written to files in coarse-grained chunks called _segments_. We can size 
each chunk independently of pages, as one segment typically consists of many pages. For instance, if we want
each file on disk to be 1GB (the default), we can set `SegmentSizeBits` S to 30, since 2<sup>30</sup> = 1GB.

* `CopyReadsToTail`: This boolean setting indicates whether reads should be copied to the tail of the log. This
is useful when reads are infrequent, but will be followed by an update, or subsequent reads.

* `ReadCacheSettings`: This setting is used to enable our new feature, a separate read cache. If reads are
frequent, we recommend the read cache instead of `CopyReadsToTail`, as the latter can inflate
log growth unnecessarily.

## Computing Total FASTER Memory

The total memory footprint of FASTER can be computed as:

```
store.IndexSize * 64 + store.OverflowBucketCount * 64 + store.Log.MemorySizeBytes
```

If the read cache is enabled, add `store.ReadCache.MemorySizeBytes`.


## Managing Log Size with C# objects

The FASTER in-memory portion consists of `store.Log.BufferSize` pages in a circular buffer. The
buffer size is equal to 2<sup>X</sup>, where X = `MemorySizeBits` - `PageSizeBits`.

When FASTER stores C# class key or value objects, the `GenericAllocator` is used for the 
log (and read cache), and these buffer pages contain pointers to reference data which may take different 
sizes, making the control of total memory footprint of FASTER difficult in this scenario. 
This is because the total number of pointers in memory remains fixed, based on total 
memory size.

For example, when `PageSizeBits` is 14 (i.e., each page is 2<sup>14</sup> = 16KB) and 
`MemorySizeBits` is 25 (i.e., 2<sup>25</sup> = 32MB total memory), we have a `BufferSize` of
2048 (i.e., 2<sup>25-14</sup>) pages in memory. With a total memory size of 32MB, with C# objects 
as keys and values, since each record takes up 24 bytes (8 byte record header + 8 byte key 
pointer + 8 byte value pointer), the buffer stores a fixed number of 32M / 24 = ~1.39M key-value 
pairs. These could take up an arbitrary amount of total memory, depending on sizes of the 
stored objects.

FASTER has two capabilities to help manage state:

1. One can accurately track the total memory used by FASTER, using a cache size [tracker](https://github.com/microsoft/FASTER/blob/master/cs/samples/MemOnlyCache/CacheSizeTracker.cs) that lets `IFunctions` notify it of record additions and deletions, and by subscribing to evictions from the head of the in-memory log. Details are in the  [MemOnlySample](https://github.com/microsoft/FASTER/tree/master/cs/samples/MemOnlyCache) sample, where we show how to track FASTER's total memory usage (including the heap objects, log, hash table, and overflow buckets) very accurately.
2. In order to control the number of key-value pairs in memory dynamically, FASTER exposes a knob, called `store.Log.EmptyPageCount`, that indicates how many pages are kept empty in memory in the circular buffer. By adjusting this knob (default 0), we can reduce the effective number of pages that hold objects in memory (and therefore the overall memory utilization) below `BufferSize`. This knob can vary between 0 (full buffer is used) and `BufferSize-1` (only tail page is used). In the [MemOnlySample](https://github.com/microsoft/FASTER/tree/master/cs/samples/MemOnlyCache) sample,  we allow the application to dynamically adjust the total memory utilization of FASTER, by exploiting this `EmptyPageCount` knob in the cache size [tracker](https://github.com/microsoft/FASTER/blob/master/cs/samples/MemOnlyCache/CacheSizeTracker.cs) module. This knob is only useful when C# class key or value types are used (`GenericAllocator`).


## Configuring the Read Cache

FASTER supports an immutable read cache that sits between the index and the main log. The read cache itself is organized
exactly like the FASTER log, and if fact uses the same log (with a null device) as its implementation. It may
be configured independently of the main log. Any prefix of a hash chain may reside in the read cache. Entries in
the FASTER index may point to the read cache, which in turn may point to earlier entries in the read cache or
eventually to the main log. As items age out of the read cache, they are simply dropped (to the null device). Hash
chain pointers are adjusted before invalidation so that the hash chains (starting from the index, going to the read
cache, and finally to the main log) stay consistent. Since hash index entries may now point to read cache objects
rather than the main log, checkpointing FASTER when the read cache is enabled, is not yet supported.

The following settings are available to configure with the read cache:

* `PageSizeBits`: Similar to `PageSizeBits` used in the main log, described above.
* `MemorySizeBits`: Similar to `MemorySizeBits` used in the main log, described above. This represents the total
size of the read cache.
* `SecondChanceFraction`: Similar to `MutableFraction` used in the main log, described above. However, it does
not indicate the fraction of memory that is mutable (since the entire read cache is immutable). Instead, it
identifies the prefix of the read cache where reads are copied to the tail of the read cache instead of being
served directly. This mechanism allows a second chance for read hot items to get copied back to the tail of
the read cache, before being invalidated by aging out of the circular read cache buffer.

## Managing Hash Index Size

FasterKV consists of a hash index (pure in-memory) that stores pointers to data, and logs that
store the actual key-value data itself (spanning memory and storage. This document discusses 
the hash index size. The number of main hash buckets is set by the constructor 
argument, called _index size_.

In C#:
```cs
store = new FasterKV<Key, Value>(indexSize, ...);
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
