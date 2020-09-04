---
layout: default
title: FasterLog persistent log in C#
nav_order: 3
permalink: /cs/fasterlog
---

# Introducing FasterLog

FasterLog is a blazing fast, persistent, concurrent, and recoverable log for C#. You can perform
appends, iteration, and log truncation. Tailing iteration, where the iterator can continue to read newly
added (committed) entries, is also supported. You can kill and recover FasterLog at any time, and resume
iteration as well. FasterLog may be used either synchronously or using `async` Task interfaces.

Underlying FasterLog is a global 64-bit logical log address space starting from 0. The log is split into
segments corresponding to files on disk. Each segment consists of a fixed number of pages. Both segment
and pages sizes are configurable during construction of FasterLog. By default, 
FasterLog commits at page boundaries. You can also force-commit the log as frequently as you need, e.g., every 
5ms. The typical use cases of FasterLog are captured in our extremely detailed commented sample [here](https://github.com/microsoft/FASTER/blob/master/cs/playground/FasterLogSample/Program.cs). FasterLog
works with .NET Standard 2.0, and can be used on a broad range of machines and devices. We have tested
it on both Windows and Linux-based machines.

## Creating the Log

```cs
  var device = Devices.CreateLogDevice("D:\\logs\\hlog.log");
  var log = new FasterLog(new FasterLogSettings { LogDevice = device });
```

We first create a 'device' (with specified file name prefix) that will store the persisted data. FasterLog is
based on the FASTER `IDevice` abstraction, and we have various device implementations available for use. The 
commit information is frequently written into a file in the same path, called 'hlog.log.commit'. There are 
other settings that can be provided, discussed later in this document.

## Operations on FasterLog

### Enqueue

An enqueue operation over FasterLog is the action of inserting an entry into the log, in memory. By itself, it
does not guarantee persistence (persistence is achieved using commit, discussed next). FasterLog supports highly
concurrent enqueues to the log, and supports several variants of enqueue:

* Enqueue: Blocking enqueue, returns when we successfully enqueue the entry into the log (in memory)
* TryEnqueue: Try to append, returns false if there is no space in the log. This is useful to implement throttling by users.
* EnqueueAsync: Async version of Enqueue, completed when entry is successfully enqueued (to memory)

You can use these options to append byte arrays (byte[]) or spans (Span<byte>). The maximum size of an
entry is goverened by the page size chosen when configuring FasterLog. You may also atomically enqueue 
batches of entries to FasterLog, by providing an `ISpanBatch` as an argument to the enqueue operation. In
this case, all or none of the entries in the batch will eventually commit atomically. Only the `byte[]`
variants are shown below.

```cs
long Enqueue(byte[] entry)
bool TryEnqueue(byte[] entry, out long logicalAddress)
async ValueTask<long> EnqueueAsync(byte[] entry)
```
### Commit

By default, FasterLog commits data every time a page gets filled up. We usually need to commit more frequently
than this, for e.g., every 5ms or immediately after an item is enqueued. This is achieved by the `Commit`
operation. We support sync and async versions of commit. The commit includes the committed begin and tail
addresses of the log, as well as any ongoing named iterators.

```cs
void Commit(bool spinWait = false)
async ValueTask CommitAsync()
```

### Wait-for-Commit

FasterLog supports the `WaitForCommit` operation, that will wait until the most recent enqueue (or all enqueues 
until a specified log address) has been committed successfully. The operation itself does not issue a commit 
request. As before, we support sync and async versions of this interface. A common usage pattern is to perform 
a sequence of enqueues on a thread, followed by a `WaitForCommit` to wait until all the enqueues are persisted.

```cs
void WaitForCommit(long untilAddress = 0) // spin-wait
async ValueTask WaitForCommitAsync(long untilAddress = 0)
```

### Helper: enqueue and wait for commit

For ease of use, we provide helper methods that enqueue an entry and returns only after the entry
has been committed to storage.

```cs
long EnqueueAndWaitForCommit(byte[] entry)
async ValueTask<long> EnqueueAndWaitForCommitAsync(byte[] entry)
```

As before, the operation does not itself issue the commit. We do not provide an out-of-the-box helper
to enqueue-commit-and-wait-for-commit in order to encourage users to view commits as a separate 
operation from enqueues, so that they can benefit from the performance boost of batched 
\textit{group commit} in the back-end. But, if this is needed, it may be implemented as follows:
* await EnqueueAsync
* await CommitAsync

### Iteration

FasterLog supports scan (iteration) over the log. You can have multiple simultaneous iterators active 
over the log at the same time. You specify a begin and end address for the scan. An example of scanning
a fixed region (first 100MB) of the log follows:

Scan using `IAsyncEnumerable`:

```cs
using (iter = log.Scan(log.BeginAddress, 100_000_000))
    await foreach ((byte[] result, int length, long currentAddress, long nextAddress) in iter.GetAsyncEnumerable())
    {
       // Process record
    }
```

Scan using traditional GetNext. Note that GetNext may return false either because we have reached the
end of iteration, or because we are waiting for a page read or commit to complete.

```cs
  using (var iter = log.Scan(0, 100_000_000))
    while (true)
    {
       while (!iter.GetNext(out byte[] result, out int entryLength, out long currentAddress, out long nextAddress))
       {
          if (currentAddress >= 100_000_000) return;
          await iter.WaitAsyc();
       }
       // Process record
    }
```

For tailing iteration, you simply specify `long.MaxValue` as the end address for the scan. You are guaranteed
to see only committed entries during a tailing iteration.

An iterator is associated with a `CompletedUntilAddress` which indicates until what address the iteration has been
completed. Users update the `CompletedUntilAddress` as follows:

```cs
iter.CompleteUntil(long address);
```

The specified address needs to be a valid log address, similar to the `TruncateUntil` call on the log (described below).

You can persist iterators (or more precisely, their `CompletedUntilAddress`) as part of a commit by simply naming 
them during their creation. On recovery, if an iterator with the specified name exists, it will be initialized with
its last-committed `CompletedUntilAddress` as the current iterator pointer.

```cs
using (var iter = log.Scan(0, long.MaxValue, name: "foo"))
```

You may also force an iterator to start at the specified begin address, i.e., without recovering:

```cs
using (var iter = log.Scan(0, long.MaxValue, name: "foo", recover: false))
```

### Iteration for Uncommitted Data

By default, scan iterates over committted data only, awaiting (in case of `IAsyncEnumerable`) or returning `false` (in 
case of `GetNext`) if we have returned the last committed entry. You can instead allow scans to proceed and read
uncommitted data as follows:

```cs
log.Scan(0, long.MaxValue, scanUncommitted: true)
```

This option allows readers to read beyond the committed address in the log. As long as readers keep up with writers, the
log can stay entirely in memory and avoid being written out to disk. During transient overload, when readers are unable to
keep up with writers, the logs will start getting written to disk. In effect, FasterLog in this setting behaves like an 
unbounded channel with bounded memory usage.

<img src="https://raw.githubusercontent.com/microsoft/FASTER/master/img/scan-uncommitted.png" width="400" />

With uncommitted scan, the tail of the log is generally not safe to consume: multiple threads inserting into the tail at
the same time means that there may be holes in the log until the threads complete their respective enqueues. To handle 
this, we add a method called `RefreshUncommitted` (with async variants). Similar to `Commit`, this call exposes the tail of
the log to consumers in a safe way. Since it is slightly more expensive, we do not automatically perform this operation after
every enqueue, and instead expose to users to call on demand. You use `RefreshUncommitted` similarly to `Commit`, either call
it from the enqueue thread (e.g., after a batch of enqueues) or have a separate thread/task that periodically calls it.

For optimal performance, we suggest using more than one page in memory for FasterLog used with uncommitted scans (e.g., 2 or 4 pages), 
and set mutable fraction (`MutableFraction` in log settings) to say 0.5. This will ensure that pages get auto-committed only when the
in-memory log of 2 of 4 pages is 50% full. This will allow pages sufficient time for records to be consumed by readers before the 
auto-commit tries to push them to disk. You may also commit manually as usual. The example [here](https://github.com/microsoft/FASTER/tree/master/cs/playground/FasterLogPubSub) 
shows how to use this feature.


### Log Head Truncation

FasterLog support log head truncation, until any prefix of the log. Truncation updates the log begin address and
deletes truncated parts of the log from disk, if applicable. A truncation is persisted via commit, similar 
to tail address commit.

There are two variants: `TruncateUntilPageStart` truncates until the start of the page corresponding to the 
specified address. This is safe to invoke with any address, as the page start is always a valid pointer to the
log. The second variant, called `TruncateUntil`, truncates exactly until the specified address. Here, the user 
needs to be careful and provide a valid address that points to a log entry. For example, any address returned by 
an iterator is a valid location that one could truncate until.

Here is an example, where we truncate the log to limit it to the last 100GB:

```cs
  log.TruncateUntilPageStart(log.CommittedUntilAddress - 100_000_000_000L);
```

### Random Read

You can issue random reads on any valid address in the log. This is useful to build, for example, an index over the
log. The user is responsible for requesting a valid address that points to the beginning of a valid entry in the
log.

```cs
(result, length) = await log.ReadAsync(iter.CurrentAddress);
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

* `LogCommitManager`: Users can fine tune the commit of log metadata by extending or replacing the default log
commit manager provided (`LocalLogCommitManager`). For instance, users may commit to a database instead of the
local file system. They may also perform various pre-commit and post-commit operations if needed.

* `LogCommitFile`: The file used to store log commit info, defaults to the log file name with a '.commit' suffix.
This is just a shortcut to setting `LogCommitManager` to `new LocalLogCommitManager(LogCommitFile)`.

* `GetMemory`: This delegate, if provided, is used by FasterLog to allocate memory for copying results over from
the log, for consumption by the application. GetMemory requests memory of a specified minimum size, and may
return a larger byte array to enable pooling.

* `LogChecksum`: Specifies whether we store a per-entry checksum in the log. This takes up an additional 8 bytes
of space per entry, in addition to the 4 byte header storing entry length.

* `MutableFraction`: Fraction of pages in memory that are left uncommitted by default (when explicit commits are
not called). If set to 0 (the default), a page commits as soon as it is filled up.


# Full API Reference

```cs
// Enqueue log entry (to memory) with spain-wait

long Enqueue(byte[] entry)
long Enqueue(ReadOnlySpan<byte> entry)
long Enqueue(IReadOnlySpanBatch readOnlySpanBatch)

// Try to enqueue log entry (to memory)

bool TryEnqueue(byte[] entry, out long logicalAddress)
bool TryEnqueue(ReadOnlySpan<byte> entry, out long logicalAddress)
bool TryEnqueue(IReadOnlySpanBatch readOnlySpanBatch, out long logicalAddress)

// Async enqueue log entry (to memory)

async ValueTask<long> EnqueueAsync(byte[] entry)
async ValueTask<long> EnqueueAsync(ReadOnlyMemory<byte> entry)
async ValueTask<long> EnqueueAsync(IReadOnlySpanBatch readOnlySpanBatch)

// Wait for commit

void WaitForCommit(long untilAddress = 0) // spin-wait
async ValueTask WaitForCommitAsync(long untilAddress = 0)

// Commit

void Commit(bool spinWait = false)
async ValueTask CommitAsync()

// Helper: enqueue log entry and spin-wait for commit

long EnqueueAndWaitForCommit(byte[] entry)
long EnqueueAndWaitForCommit(ReadOnlySpan<byte> entry)
long EnqueueAndWaitForCommit(IReadOnlySpanBatch readOnlySpanBatch)

// Helper: enqueue log entry and async wait for commit

async ValueTask<long> EnqueueAndWaitForCommitAsync(byte[] entry)
async ValueTask<long> EnqueueAndWaitForCommitAsync(ReadOnlyMemory<byte> entry)
async ValueTask<long> EnqueueAndWaitForCommitAsync(IReadOnlySpanBatch readOnlySpanBatch)

// Truncate log (from head)

void TruncateUntilPageStart(long untilAddress)
void TruncateUntil(long untilAddress)

// Scan interface

FasterLogScanIterator Scan(long beginAddress, long endAddress, string name = null, bool recover = true, 
   ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false)

// FasterLogScanIterator interface

void CompleteUntil(long address)
bool GetNext(out byte[] entry, out int entryLength, out long currentAddress, out long nextAddress)
bool GetNext(MemoryPool<byte> pool, out IMemoryOwner<byte> entry, out int entryLength, out long currentAddress, out long nextAddress)
async ValueTask WaitAsync()

// IAsyncEnumerable interface to FasterLogScanIterator
async IAsyncEnumerable<(byte[] entry, int entryLength, long currentAddress, long nextAddress)> GetAsyncEnumerable()
async IAsyncEnumerable<(IMemoryOwner<byte>, int entryLength, long currentAddress, long nextAddress)> GetAsyncEnumerable(MemoryPool<byte> pool)

// Random read

async ValueTask<(byte[], int)> ReadAsync(long address, int estimatedLength = 0)
```
