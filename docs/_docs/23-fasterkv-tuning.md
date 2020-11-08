---
title: "Tuning FasterKV"
permalink: /docs/fasterkv-tuning/
excerpt: "Tuning FasterKV"
last_modified_at: 2020-11-07
toc: true
---

## Configuring the Hybrid Log

Recall that FASTER consists of the hash index that connects to a hybrid log spanning storage and main 
memory. The hybrid log may be configured using the `LogSettings` input to the FasterKV constructor.

```cs
var fht = new FasterKV<...>(1L << 20, new Funcs(), new LogSettings { ... }, ...);
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
    string as parameter to the method above.

* `ObjectLogDevice`: If you are using C# class objects as FASTER keys and/or values, you need to also
provide storage for the object log, similar to the main log described above.

* `PageSizeBits`: This field (P) is used to indicate the size of each page. It is provided in terms of the number
of bits. You get the actual size of the page by a simple computation of two raised to the power of the number
specified (2<sup>P</sup>). For example, when `PageSizeBits` P is set to 12, it represents pages of size 4KB 
(since 2<sup>P</sup> = 2<sup>10</sup> = 4096 = 4KB). Generally you should not need to adjust page size from its
default value of 25 (= 32MB).

* `MemorySizeBits`: This field (M) indicates the total size of memory used by the log. As before, for a setting
of M, 2<sup>M</sup> is the number of bytes used totally by the log. Since each page is of size 2<sup>P</sup>, the 
number of pages in memory is simply 2<sup>M-P</sup>. FASTER requires at least 16 pages in memory, so M should be 
set to at least P+4.

* `MutableFraction`: This field (F) indicates the fraction of the memory that will be treated as _mutable_, i.e.,
updates are performed in-place instead of creating a new version on the tail of the log. A value of 0.9 (the
default) indicates that 90% of memory will be mutable.

* `SegmentSize`: On disk, the data is written to files in coarse-grained chunks called _segments_. We can size 
each chunk independently of pages, as one segment typically consists of many pages. For instance, if we want
each file on disk to be 1GB (the default), we can set `SegmentSizeBits` S to 30, since 2<sup>30</sup> = 1GB.

* `CopyReadsToTail`: This boolean setting indicates whether reads should be copied to the tail of the log. This
is useful when reads are infrequent, but will be followed by an update, or subsequent reads.

* `ReadCache`: This setting is used to enable our new feature, a separate read cache. If reads are
frequent, we recommend instead the read cache instead of `CopyReadsToTail`, as the latter can inflate
log growth unnecessarily.

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
