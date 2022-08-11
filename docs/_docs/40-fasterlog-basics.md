---
title: "FasterLog Basics"
permalink: /docs/fasterlog-basics/
excerpt: "FasterLog Basics"
last_modified_at: 2020-11-08
toc: true
---

## Introduction to FasterLog C#

FasterLog is a blazing fast, persistent, concurrent, and recoverable log for C#. You can perform
appends, commits, iteration, and log truncation. Tailing iteration, where the iterator can continue to read newly
added (committed or uncommitted) entries, is supported. You can kill and recover FasterLog at any time, and resume
iteration as well. FasterLog may be used either synchronously or using `async` Task interfaces.

Underlying FasterLog is a global 64-bit logical log address space starting from 0. The log is split into
segments corresponding to files on disk. Each segment consists of a fixed number of pages. Both segment
and pages sizes are configurable during construction of FasterLog. You can commit the log as frequently 
as you need, e.g., every 5ms. The typical use cases of FasterLog are captured in our extremely detailed 
commented sample [here](https://github.com/microsoft/FASTER/blob/master/cs/samples/FasterLogSample/Program.cs).
FasterLog works with all versions of .NET, and has been tested on both Windows and Linux machines.

## Creating the Log

```cs
  using var config = new FasterLogSettings("d:/fasterlog");
  using var log = new FasterLog(config);
```

We first create an instance of the config, `FasterLogSettings`, using an overload that automatically creates
a log device at the specified local disk path. The commit information written into a file in the same 
path, under the `log-commits` folder. The `using` ensures that the automatically created `IDevice` is
disposed at the end. You can also explicitly create an `IDevice` and assign it to the `LogDevice` field
of `FasterLogSettings`. In this case, you are responsible for disposing the device when done. We have 
various device implementations available for use, such as local and Azure storage devices. There are 
other settings that can be provided, discussed later in this guide.

## Operations on FasterLog

### Enqueue

An enqueue operation over FasterLog is the action of inserting an entry into the log, in memory. By itself, it
does not guarantee persistence (persistence is achieved using commit, discussed next). FasterLog supports highly
concurrent enqueues to the log, and supports several variants of enqueue:

* Enqueue: Blocking enqueue, returns when we successfully enqueue the entry into the log (in memory)
* TryEnqueue: Try to append, returns false if there is no space in the log. This is useful to implement throttling by users.
* EnqueueAsync: Async version of Enqueue, completed when entry is successfully enqueued (to memory)

You can use these options to append byte arrays (`byte[]`) or spans (`Span<byte>`). The maximum size of an
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

Users are expected to call the `Commit` operation to force data to disk and create recoverable points on the log. We support
sync and async versions of commit. The commit includes the committed begin and tail addresses of the log, as well as any ongoing named
iterators. See the section for advanced users [below](#advanced-features) for an overview of the different ways commits can be customized.

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
using (var iter = log.Scan(log.BeginAddress, 100_000_000))
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
          await iter.WaitAsync();
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

### Iteration for Uncommitted Data (Publish/Subscribe)

By default, scan allows iteration over log entries that are committed (persisted on an I/O device) only, awaiting 
(in case of `IAsyncEnumerable`) or returning `false` (in case of `GetNext`) if we have returned the last 
committed entry. This is a desired behaviour in most cases, but sometimes consumers do not care whether the data 
is committed or not, but just wish to read and process log entries as soon as possible, similar to a regular Channel.

You can allow scans to proceed and read uncommitted data by setting `scanUncommitted` to `true`, as follows:

```cs
log.Scan(0, long.MaxValue, scanUncommitted: true)
```

This option allows readers to read beyond the committed address in the log. This mode has a useful side-effect: as long
as readers keep up with writers, the log can stay entirely in memory and avoid being written out to disk. During transient
overload, when readers are unable to keep up with writers, the logs will start getting written to disk. In effect, FasterLog
in this setting behaves like an _unbounded channel with bounded memory usage_.

<img src="https://raw.githubusercontent.com/microsoft/FASTER/master/img/scan-uncommitted.png" width="400" />

With uncommitted scan, the tail of the log is generally not safe to consume: multiple threads inserting into the tail at
the same time means that there may be holes in the log until the threads complete their respective enqueues. To handle 
this, we add a method called `RefreshUncommitted` (with async variants). Similar to `Commit`, this call exposes the tail of
the log to consumers in a safe way. Since it is slightly more expensive, we do not automatically perform this operation after
every enqueue, and instead expose to users to call on demand. You use `RefreshUncommitted` similarly to `Commit`, either call
it from the enqueue thread (e.g., after every enqueue or a batch of enqueues) or have a separate thread/task that periodically 
calls it. Note that while `RefreshUncommitted` incurs a small CPU overhead to the write operation, it does not perform any 
I/O operation. The example in [playground](https://github.com/microsoft/FASTER/tree/master/cs/playground/FasterLogPubSub)
shows how to use this feature.


### Log Head Truncation

FasterLog support log head truncation, until any prefix of the log. Truncation updates the log begin address. At the
next commit, we persist the new begin address and delete truncated parts of the log from disk after the commit.

There are two variants: `TruncateUntilPageStart` truncates until the start of the page corresponding to the 
specified address. This is safe to invoke with any address, as the page start is always a valid pointer to the
log. The second variant, called `TruncateUntil`, truncates exactly until the specified address. Here, the user 
needs to be careful and provide a valid address that points to a log entry. For example, any address returned by 
an iterator is a valid location that one could truncate until.

Here is an example, where we truncate the log to limit it to the last 100GB:

```cs
  log.TruncateUntilPageStart(log.CommittedUntilAddress - 100_000_000_000L);
```

### Log Completion

A log can be explicitly marked as **completed**. A completed log simply signals that log producer (callers of ``Enqueue``)
have finished adding entries, and will no longer grow the log. This is useful when coordinating with log consumers (e.g., a thread
iterating through the log with an async iterator, waiting for new entries) to stop waiting for more data. Current entries in the
log can still be read as normal.

To complete a log:
```cs
log.CompleteLog(spinWait: true);
```

After this call, the log will commit all entries enqueued so far, and future ``Enqueues`` will throw an exception. Users can check for
log completion by calling:
```cs
log.LogCompleted;
```

During iteration, they can check for the end of iteration due to either log completion or iteration reaching the requested end
address, as:

```cs
iterator.Ended
```

### Random Read

You can issue random reads on any valid address in the log. This is useful to build, for example, an index over the
log. The user is responsible for requesting a valid address that points to the beginning of a valid entry in the
log.

```cs
(result, length) = await log.ReadAsync(iter.CurrentAddress);
```

# Advanced Features

### Commit behavior customization

FasterLog gives the user to customize the commit process both for performance and for richer semantics associated with commits:
- ``LogCommitPolicy``: sometimes it is convenient to call ``Commit()`` liberally in the code base, but that can come with performance overhead --- more commits usually mean smaller, more frequent
writes to disk that can reduce throughput significantly. By using a custom ``LogCommitPolicy``, users can effectively coalesce multiple commit requests in the background to improve
overall performance. Aside from a default policy that allows every non-redundant commit through, we add two more advanced policies --- ``MaxParallel`` and ``RateLimit``.
The former will only issue up to ``k`` (customizable as a parameter) parallel I/Os, forcing later commits to wait and coalesce into the next ``k`` writes; the latter similarly throttles 
commit requests, but instead enforces that at least some amount of time has passed or some of the data has been written since the last
commit. Note that regardless of the commit policy chosen, ``Commit()`` exhibits the same behavior transparently, and will never block; instead policy decisions are made
in the background to only admit certain commit requests into actual I/O operations. In short, commit policies are transparent ways for application developers to tune their
commit logic for latency and throughput without changing the way they write programs.
  
- Strong Commits: Most of the time, FasterLog users simply want entries written to disk. But other times, a commit can be associated with additional information and the performance-centric optimizations of normal
``Commit`` calls can get in the way. For these users, we introduce the concept of strong commits. Strong commits are special commit requests that ignore commit policies and other performance optimizations to always 
represent a physical recoverable point on the log. Users can:
  1. specify a unique commit number that identifies exactly the committed point on log, which can be recovered to later exactly regardless of later commits
  2. associate a piece of custom metadata with the commit called the cookie. The cookie is an arbitrary byte array that will be atomically written as part of the commit and recovered later with the commit.
  3. associate a callback with the commit to be invoked when the commit is persistent, instead of spinning or waiting externally.
In exchange for the rich semantics, strong commits are less scalable than normal commits augmented with commit policies, and we advise users to avoid issuing too many such calls (e.g., calling commit after every operation)

- Fast Commits: Normally, because FasterLog attempts to write to disk pages in parallel, we need an additional metadata file write to record a commit point, which results in an additional I/O. Now, if log-level checksum is enabled,
we support a special mode that bypasses this metadata write and instead uses special commit records to store commit information on the log itself. Fast commits are faster on the commit critical path, but may be slower to recover
due to the need to scan the log itself for commit records. Fast commit can be turned on in the log setting.
  
### Error behavior
Normally, FasterLog expects a failure-free underlying storage implementation. When faced with faulty or flaky storage, users may choose to implement a custom ``IDevice`` to mask underlying failures
by retrying or remapping on failure. However, in the unfortunate scenario where a unhandled failure propagates into FasterLog, FasterLog will cease to function and throw an exception to protect against
data corruption. Implicitly, FasterLog will only mark an entry as committed if all of its predecessors in the log are committed, in a best-effort attempt to preserve application-level causality.
When a log range ``(x, y)`` has errored, no entries with address after ``x`` will commit. Subsequent calls to the ``FasterLog`` will result in an exception, until ``FasterLog`` is recovered on a 
non-faulty device. Purely in-memory enqueus are the exception to this, and FasterLog may still function as a purely in-memory pub/sub system despite log failures. 

This may be undesirable for some advanced users who can tolerate data loss. In this case, sych users can turn on ``TolerateDeviceFailure`` in faster log setting. When this flag
is set, FasterLog will still throw an exception on a device failure, but will continue to commit future operations, ignoring the errored range afterwards. Doing so is not safe and is 
not advisable in the general case. 

# Configuring FasterLog

The following log settings are available for configuration:

* `LogDevice`: This is an instance of the storage interface (device/path/file) that will be used for the
main log. Use it if you do not use the constructor overload of `FasterLogSettings` that takes a local path
and creates a local device automatically. Make sure to eventually dispose a log device that you create 
yourself. FASTER provides several device implementations out of the box, such as `LocalStorageDevice` 
and `ManagedLocalStorageDevice` for local (or attached) storage. You can use our extension method to 
easily create an instance of a local device:
```cs
  var log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log");
```

* `PageSizeBits`: This field (P) is used to indicate the size of each page. It is provided in terms of the number
of bits. You get the actual size of the page by a simple computation of two raised to the power of the number
specified (2<sup>P</sup>). For example, when `PageSizeBits` P is set to 12, it represents pages of size 4KB 
(since 2<sup>P</sup> = 2<sup>12</sup> = 4096 = 4KB). Generally you should not need to adjust page size from its
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
commit manager provided (`DeviceLogCommitCheckpointManager`). For instance, users may commit to a database instead 
of the local file system. They may also perform various pre-commit and post-commit operations if needed. You can 
also use the older (legacy) commit manager as follows: `new LocalLogCommitManager(LogCommitFile)`.

* `GetMemory`: This delegate, if provided, is used by FasterLog to allocate memory for copying results over from
the log, for consumption by the application. GetMemory requests memory of a specified minimum size, and may
return a larger byte array to enable pooling.

* `LogChecksum`: Specifies whether we store a per-entry checksum in the log. This takes up an additional 8 bytes
of space per entry, in addition to the 4 byte header storing entry length.

* `MutableFraction`: Fraction of pages in memory that are left uncommitted by default (when explicit commits are
not called). If set to 0 (the default), a page commits as soon as it is filled up.

* `RemoveOutdatedCommits`: This setting will delete all outdated commits other than the latest commit. During 
recovery, it will likewise remove all commits other than the one recovered to. 

* `TryRecoverLatest`: If true (default), we will try to recover the log from the latest valid commit, during 
instantiation or when calling `FasterLog.CreateAsync(...)`.

* `FastCommitMode` and `LogCommitPolicy`: These settings are used for fast commit and for defining how commits are
gated into the system. See the [advanced features](#advanced-features) section for more details.

# Full API Reference

```cs
// Enqueue log entry (to memory) with spain-wait

long Enqueue(byte[] entry)
long Enqueue(ReadOnlySpan<byte> entry)
long Enqueue(IReadOnlySpanBatch readOnlySpanBatch)

// Log Completion
void CompleteLog(bool spinWait = false)

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
bool CommitStrongly(out long commitTail, out long actualCommitNum, bool spinWait = false, byte[] cookie = null, long proposedCommitNum = -1, Action callback = null)
async ValueTask CommitAsync()
async ValueTask<(bool success, long commitTail, long actualCommitNum)> CommitStronglyAsync(byte[] cookie = null, long proposedCommitNum = -1, CancellationToken token = default)

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

// Refreshing uncommitted entries

void RefreshUncommitted(bool spinWait = false)

```
