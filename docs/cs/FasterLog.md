---
layout: default
title: FasterLog - a fast concurrent write-ahead log and tailing iterator
parent: Tuning FASTER
nav_order: 3
---

# Introducing FasterLog

FasterLog is a blazing fast, persistent, and recoverable concurrent log for C#. You can perform
both appends and iteration. Tailing iteration, where the iterator can continue to read newly
added (committed) entries, is also supported. You can kill and recover FasterLog at any time.

By default, FasterLog commits at page boundaries, where the page size is configurable. You can
also force-commit the log as frequently as you need, e.g., every 5ms. The typical use cases of
FasterLog are captured in our sample [here](https://github.com/microsoft/FASTER/blob/master/cs/playground/FasterLogSample/Program.cs).

## Create the Log

```cs
  var device = Devices.CreateLogDevice("D:\\logs\\hlog.log");
  var log = new FasterLog(new FasterLogSettings { LogDevice = device });
```

We first create a 'device' (file prefix) that will store the persisted data. The commit information
will be periodically written into a file in the same path, called 'hlog.log.commit'. There are
other settings that can be provided, discussed later in this document.

## Append Operation

FasterLog supports highly concurrent appends to the log. You have several options:

* Append: Blocking append
* TryAppend: Try to append, useful to implement throttling by user
* AppendAsync: Async task-based version of Append (lower performance)

You can use these options to append byte arrays (byte[]) and spans (Span<byte>). The maximum size of an
entry is goverened by the page size chosen when configuring FasterLog. You may also atomically commit
batches of entries to FasterLog, by providing an `ISpanBatch` as argument to the append operation. In
this case, all or none of the entries in the batch will commit atomically.

## Iteration Operation

FasterLog supports scan (iteration) over the log. You can have multiple simultaneous iterators active
over the log at the same time. You specify a begin and end address for the scan. An example of
scanning a fixed region (first 100MB) of the log follows:

```cs
  using (var iter = log.Scan(0, 100_000_000))
  {
    while (iter.GetNext(out Span<byte> result))
    {
      // Process record
    }
  }
```

For tailing iteration, you specify `long.MaxValue` as the end address for the scan, and use CommitTask
to pause iteration when we have no more records available.

```cs
  using (var iter = log.Scan(0, long.MaxValue))
  {
    while (true)
    {
      // Cache commit task before checking for next record availability
      var commitTask = log.CommitTask;
      while (iter.GetNext(out Span<byte> result))
      {
        // Process record
        commitTask = log.CommitTask;
      }
      await commitTask;
    }
  }
```


## Truncation

FasterLog support log truncation until any prefix of the log. Here is an example, where we truncate the
log to limit it to the last 100GB:

```cs
  log.TruncateUntil(log.CommittedUntilAddress - 100_000_000_000L);
```


# Configuring FasterLog

The following log settings are available for configuration:

* `LogDevice`: This is an instance of the storage interface (device/path/file) that will be used for the
main log. It is a class that implements the `IDevice` interface. FASTER provides several device implementations 
out of the box, such as `LocalStorageDevice` and `ManagedLocalStorageDevice` for local (or attached) storage.
You can use our extension method to easily create an instance of a local device:
```cs
  var log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log");
```

* `PageSizeBits`: This field (P) is used to indicate the size of each page. It is provided in terms of the number
of bits. You get the actual size of the page by a simple computation of two raised to the power of the number
specified (2<sup>P</sup>). For example, when `PageSizeBits` P is set to 12, it represents pages of size 4KB 
(since 2<sup>P</sup> = 2<sup>10</sup> = 4096 = 4KB). Generally you should not need to adjust page size from its
default value of 25 (= 32MB).

* `MemorySizeBits`: This field (M) indicates the total size of memory used by the log. As before, for a setting
of M, 2<sup>M</sup> is the number of bytes used totally by the log. Since each page is of size 2<sup>P</sup>, the 
number of pages in memory is simply 2<sup>M-P</sup>. FasterLog requires at least 2 pages in memory, so M should be 
set to at least P+1.

* `MutableFraction`: This field (F) indicates the fraction of the memory that will be treated as _mutable_, i.e.,
updates are performed in-place instead of creating a new version on the tail of the log. A value of 0.9 (the
default) indicates that 90% of memory will be mutable.

* `SegmentSize`: On disk, the data is written to files in coarse-grained chunks called _segments_. We can size 
each chunk independently of pages, as one segment typically consists of many pages. For instance, if we want
each file on disk to be 1GB (the default), we can set `SegmentSizeBits` S to 30, since 2<sup>30</sup> = 1GB.

* `LogCommitManager': Users can fine tune the commit of log metadata by extending or replacing the default log
commit manager provided (`LocalLogCommitManager`). For instance, users may commit to a database instead of the
local file system. They may also perform various pre-commit and post-commit operations if needed.

* `LogCommitFile`: The file used to store log commit info, defaults to the log file name with '.commit' suffix.
This is just a shortcut to setting `LogCommitManager` to `new LocalLogCommitManager(LogCommitFile)`.
