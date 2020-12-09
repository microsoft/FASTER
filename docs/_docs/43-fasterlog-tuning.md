---
title: "Tuning FasterLog"
permalink: /docs/fasterlog-tuning/
excerpt: "Tuning FasterLog"
last_modified_at: 2020-11-08
toc: false
classes: wide
---

# Tuning FasterLog

FasterLog may be configured using the `FasterLogSettings` input to the FasterLog constructor.

```cs
var log = new FasterLog(new FasterLogSettings { ... });
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

* `PageSizeBits`: This field (P) is used to indicate the size of each page. It is provided in terms of the number
of bits. You get the actual size of the page by a simple computation of two raised to the power of the number
specified (2<sup>P</sup>). For example, when `PageSizeBits` P is set to 12, it represents pages of size 4KB 
(since 2<sup>P</sup> = 2<sup>10</sup> = 4096 = 4KB). Generally you should not need to adjust page size from its
default value of 25 (= 32MB).

* `MemorySizeBits`: This field (M) indicates the total size of memory used by the log. As before, for a setting
of M, 2<sup>M</sup> is the number of bytes used totally by the log. Since each page is of size 2<sup>P</sup>, the 
number of pages in memory is simply 2<sup>M-P</sup>. FASTER requires at least 2 pages in memory, so M should be 
set to at least P+1.

* `SegmentSize`: On disk, the data is written to files in coarse-grained chunks called _segments_. We can size 
each chunk independently of pages, as one segment typically consists of many pages. For instance, if we want
each file on disk to be 1GB (the default), we can set `SegmentSizeBits` S to 30, since 2<sup>30</sup> = 1GB.

* `MutableFraction`: This field (F) indicates the fraction of the memory that is marked as _mutable_, i.e.,
uncommitted (default 0). A value of 0.5 indicates that we will only automatically flush log pages in the first
50% of in-memory pages. Note that a commit will flush all data regardless of this setting, i.e., this setting
only applies to automatic log flushing without a commit.

* `LogCommitManager`: An (optional) custom log commit manager provided by user to override the default one.

* `LogCommitFile`: Use specified directory for storing and retrieving checkpoints. This is simply a shortcut to
providing the following: `FasterLogSettings.LogCommitManager = new LocalLogCommitManager(LogCommitFile);`.

* `GetMemory`: User callback to allocate memory for read entries (optional).

* `LogChecksum`: Type of checksum to add to log entries.

* `ReadOnlyMode`: Use FasterLog as a read-only iterator/viewer of log being committed by another instance, 
possibly on a different process or machine.
