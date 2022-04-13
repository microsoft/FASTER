---
title: "FasterKV Basics"
permalink: /docs/fasterkv-basics/
excerpt: "FasterKV Basics"
last_modified_at: 2022-03-04
toc: true
---

## Introduction to FasterKV C#

The FasterKV key-value store and cache in C# works in .NET Framework and .NET core, and can be used in both a 
single-threaded and highly concurrent setting. It has been tested to work on both Windows and Linux. It exposes 
an API that allows one to performs a mix of Reads, Blind Updates (Upserts), and atomic Read-Modify-Write 
operations. It supports data larger than memory, and accepts an `IDevice` implementation for storing logs on
storage. We have provided `IDevice` implementations for local file system and Azure Page Blobs, but one may create
new devices as well. We also offer meta-devices that can group device instances into sharded and tiered configurations.

FASTER  may be used as a high-performance replacement for traditional concurrent data structures such as the 
.NET ConcurrentDictionary, and additionally supports larger-than-memory data. It also supports checkpointing of the 
data structure - both incremental and non-incremental. Operations on FASTER can be issued synchronously or 
asynchronously, i.e., using the C# `async` interface.

## Getting FASTER

### Building From Sources
Clone the Git repo, open cs/FASTER.sln in Visual Studio, and build.

### NuGet
You can install FASTER binaries using Nuget, from Nuget.org. Right-click on your project, manage NuGet packages, browse 
for Microsoft.FASTER.Core. Here is a [direct link](https://www.nuget.org/packages/Microsoft.FASTER.Core).

## Basic Concepts

### FASTER Operations

FASTER supports three basic operations:
1. **Read**: Read data from the key-value store
2. **Upsert**: Blind upsert of values into the store (does not check for prior values)
3. **Read-Modify-Write**: Update values in store atomically, used to implement operations such as Sum and Count.

### Constructor

You instantiate the FasterKV store with provided settings for various parameters. The store requires a key
type and a value type. For example, an instance of FasterKV over `long` keys and `string` values, with data
stored in "c:/temp", and all other parameters as default, is created as follows

```cs
using var settings = new FasterKVSettings<long, string>("c:/temp");
using var store = new FasterKV<long, string>(settings);
```

You can also run FasterKV purely in memory, using a special `null` path:

```cs
using var settings = new FasterKVSettings<long, string>(null);
using var store = new FasterKV<long, string>(settings);
```


#### Explicit device specification

You can also separately create your storage devices and provide them via `FasterKVSettings`. If you are using value
(blittable) types such as `long`, `int`, and structs with value-type members, or special variable-length value 
types such as our `SpanByte` type, you only need one log device:

```cs
using var log = Devices.CreateLogDevice("c:/temp/hlog.log");
```

If your key or values are serializable C# objects such as classes and strings, you 
need to create a separate object log device as well:

```cs
using var objlog = Devices.CreateLogDevice("c:/temp/hlog.obj.log");
```

For pure in-memory operation, you can just use a special `new NullDevice()` instead.

```cs
using var settings = new FasterKVSettings<long, long> { LogDevice = log, ObjectLogDevice = objlog };
using var store = new FasterKV<long, string>(settings);
```


#### FasterKVSettings

`FasterKVSettings` allows you to customize various parameters related to the store. Some of them are described below.

1. `IndexSize`: Size of main hash index, in bytes (rounds down to a power of 2). Minimum size is 64 bytes.
2. `MemorySize`: Denotes the size of the in-memory part of the hybrid log (rounds down to a power of 2). 
Note that if the log points to class key or value objects, this size only includes the 8-byte reference to 
the object. The older part of the log is spilled to storage.
3. Other log settings: These are several settings related to the log, such as `PageSize` for the size of pages.
4. `ReadCacheEnabled`: Whether a separate read cache is provisioned and enabled for the store.
5. Checkpoint settings: These are settings related to checkpoints, such as `CheckpointDir` for the checkpoint folder,
and `TryRecoverLatest` to determine whether we try to recover to the latest checkpoint at startup. These are covered 
in the section on [checkpointing and recovery](/FASTER/docs/fasterkv-recovery).
6. Serializers: A user can provide custom serializers for class key and value types. Serializers implement 
`IObjectSerializer<Key>` for keys and `IObjectSerializer<Value>` for values. *These are only needed for 
non-blittable types such as C# class objects.*
7. `EqualityComparer`: Used for providing a customized comparer for keys, implements `IFasterEqualityComparer<Key>`.

The total in-memory footprint of FASTER is controlled by `IndexSize` and `MemorySize`. Read more about managing memory
in FASTER in the [tuning](/FASTER/docs/fasterkv-tuning) guide.

### Callback Functions

#### IFunctions

For session operations, the user provides an instance of a type that implements `IFunctions<Key, Value, Input, Output, Context>`, or one of its corresponding abstract base classes (see [FunctionsBase.cs](https://github.com/microsoft/FASTER/blob/main/cs/src/core/Index/Interfaces/FunctionsBase.cs)):
- `FunctionsBase<Key, Value, Input, Output, Context>`
- `SimpleFunctions<Key, Value, Context>`, a subclass of `FunctionsBase<Key, Value, Input, Output, Context>` that uses Value for the Input and Output types.
- `SimpleFunctions<Key, Value>`, a subclass of `SimpleFunctions<Key, Value, Context>` that uses the `Empty` struct for Context.

Apart from Key and Value, the IFunctions interface is defined on three additional types:
1. `Input`: This is the type of input provided to FASTER when calling Read or RMW. It may be regarded as a parameter for the Read or RMW operation. For example, with RMW, it may be the delta being accumulated into the value.
2. `Output`: This is the type of the output of a Read or RMW operation. The reader or updater copies the relevant parts of the Value to Output.
3. `Context`: User-defined context for the operation. Use `Empty` if there is no context necesssary.

`IFunctions<>` encapsulates all callbacks made by FASTER back to the caller, which are described next:

1. SingleReader and ConcurrentReader: These are used to read from the store values and copy them to Output. Single reader can assume that there are no concurrent operations on the record.
2. SingleWriter and ConcurrentWriter: These are used to write values to the store, from a source value. Single writer can assume that there are no concurrent operations on the record.
3. Completion callbacks: Called by FASTER when various operations complete after they have gone "pending" due to requiring IO.
4. RMW Updaters: There are three updaters that the user specifies, InitialUpdater, InPlaceUpdater, and CopyUpdater. Together, they are used to implement the RMW operation and return the Output to the caller. There is also a NeedCopyUpdate() method that is called before appending a copied-and-updated record to the tail of the log; if it returns false, the record is not copied.
5. Locking: There is one property and two methods; if the DisableLocking property is false (the default), then FASTER will call Lock and Unlock within a try/finally in the four concurrent callback methods: ConcurrentReader, ConcurrentWriter, ConcurrentDeleter, and InPlaceUpdater. FunctionsBase illustrates the default implementation of Lock and Unlock as an exclusive lock using a bit in RecordInfo.

Callbacks for in-place updates receive the logical address of the record, which can be useful for applications such as indexing, and a reference to the `RecordInfo` header of the record, for use with the new locking calls.
ReadCompletionCallback receives the `RecordInfo` of the record that was read.

##### Return Values of IFunctions Data Operations
IFunctions calls receive data through reference parameters to Keys and their associated Values. As well, they receive a parameter specific to the operation, describing additional information about the operation and its context:
- **ReadInfo**: Contains the session Version, session type, and logical address
- **UpsertInfo**, **RMWInfo**, **DeleteInfo**: Contains the session Version, session type, and logical address, as well as "output" variables for expiring records and canceling the operation:
  - **DeleteRecord**: Always False coming in from FASTER; may be set True by the app if, for example, expiration has been detected. In this case, FASTER will set the Tombstone. It will assume deletion logic has already been done; that is, it will not call SingleDeleter or ConcurrentDeleter.
  - **CancelOperation**: Always False coming in from FASTER; may be set True by the app if it detects a condition in the callback that makes the operation no longer desired.

Note that the return value decision of Found vs. NotFound refers to whether the record was found. For Read, this is a thorough search. For RMW, it refers to the Read portion of the RMW (whether the record was found). For Upsert and Delete it refers to whether we found the record in the in-memory portion of the log (these do not go to disk; they "blindly" add a new record in that case.

**Read** (`ReadInfo`):
- `SingleReader`:
  - ***Expiration***: The app sets readInfo.Action to ReadAction.DeleteRecord and returns false. FASTER will perform a logical delete of the record by forcing a CopyToTail of the current record with the Tombstone set, and Read will return (status.Found | status.Record.Created | status.Record.Expired).
  - ***Cancellation***: The app sets readInfo.Action to ReadAction.CancelOperation and returns false. Read() returns (status.Canceled).
  - ***Default false return***: Read() returns (status.NotFound).
- `ConcurrentReader`
  - ***Expiration***: The app sets readInfo.Action to ReadAction.DeleteRecord and returns false. FASTER will perform a logical delete of the record by in-place update to set the tombstone, and Read will return (status.Found | status.Record.InPlaceUpdated | status.Record.Expired).
  - ***Cancellation***: The app sets readInfo.Action to ReadAction.CancelOperation and returns false. Read() returns (status.Canceled).
  - ***Other false return***: Read() returns (status.NotFound).

**RMW** (`RMWInfo`)
  - `InPlaceUpdater`
    - ***Expiration***: The app sets rmwInfo.Action to one of the following and returns false:
      - **ReadAction.ExpireAndResume**: FASTER will first try to reinitialize the record in place, and if successful, RMW will return (status.Found | status.Record.InPlaceUpdated | status.Record.Expired). If the in-place update fails, RMW will set the tombstone true via an in-place update, and then will enter the `NeedInitialUpdater` path, returning the same as if the record had been deleted on entry to RMW, (status.NotFound | status.Record.Created).
      - **ReadAction.ExpireAndStop** FASTER will set recordInfo.Tombstone to true and will return (status.Found | status.Record.InPlaceUpdated | status.Record.Expired).
      - The app may do its own internal "expire and reset" logic within the `InPlaceUpdater` call and return true, which to FASTER is indistinguishable from any other successful `InPlaceUpdater` call.
    - ***Cancellation***: The app sets rmwInfo.Action to RMWAction.CancelOperation and returns false. RMW() returns (status.Canceled).
    - ***Default false return***: Goes to `InitialUpdater` path.
  - *InitialUpdater*
    - `NeedInitialUpdate`
      - ***Expiration***: Irrelevant because there is no existing record.
      - ***Cancellation***: The app sets rmwInfo.Action to RMWAction.CancelOperation and returns false. RMW() returns (status.Canceled).
      - ***Default false return***: Returns (status.NotFound).
    - `InitialUpdater`
      - ***Expiration***: Irrelevant because there is no existing record.
      - ***Cancellation***: The app sets rmwInfo.Action to RMWAction.CancelOperation and returns false.  RMW() returns (status.Canceled).
      - ***Other false return***: Returns (status.NotFound) (but no status.Record.Created).
    - `PostInitialUpdater`
      - This is void because the record has already been CAS'd into the hash table, so it must perform operations that do not fail.
  - *CopyUpdater*
    - `NeedCopyUpdate`
      - ***Expiration***: This is ignored from NeedCopyUpdate, because we do not have the new record yet to place a Tombstone into, and is treated as a default false return, returning (status.Found).
      - ***Cancellation***: The app sets rmwInfo.CancelOperation True and returns false. RMW() returns (status.Canceled).
      - ***Default false return***: Returns (status.Found).
    - `CopyUpdater`
      - ***Expiration***: The app sets rmwInfo.Action to one of the following and returns false. FASTER will perform a logical delete of the record by setting the record's Tombstone and continuing with the CAS, and RMW will return (status.NotFound && status.Record.CreatedRecord).
        - **ReadAction.ExpireAndResume**: FASTER will first try to reinitialize the new CU record, and if successful, RMW will return (status.Found | status.Record.CopyUpdated | status.Record.Expired). If the reinitialization fails, FASTER will perform a logical delete of the record by setting the record's Tombstone and continuing with the CAS. FASTER will then return RETRY_NOW, which will (probably) see the Tombstoned record and enter the `NeedInitialUpdater` path, returning the same as if the record had been deleted on entry to RMW, (status.NotFound | status.Record.Created).
        - **ReadAction.ExpireAndStop** FASTER will set recordInfo.Tombstone to true in the new CU record and will continue with the CAS of that record, returning (status.Found | status.Record.InPlaceUpdated | status.Record.Expired).
      - ***Cancellation***: The app sets rmwInfo.Action to RMWAction.CancelOperation and returns false. RMW() returns (status.Canceled).
      - ***Default false return***: Returns (status.Found) (but no status.Recored.Created).
    - `PostCopyUpdater`
      - This is void because the record has already been CAS'd into the hash table, so it must perform operations that do not fail.

**Upsert** (`UpsertInfo`)
  - `ConcurrentWriter`
    - ***Expiration***: Irrelevant because it is a blind upsert.
    - ***Cancellation***: The app sets upsertInfo.Action to UpsertAction.CancelOperation and returns false. Upsert() returns (status.Canceled).
    - ***Default false return***: Moves to `SingleWriter` path.
  - `SingleWriter`
    - ***Expiration***: Irrelevant because there is no existing record.
    - ***Cancellation***: The app sets upsertInfo.Action to UpsertAction.CancelOperation and returns false. Upsert() returns (status.Canceled).
    - ***Other false return***: Returns (status.NotFound) (but no status.Recored.Created).
  - `PostSingleWriter`
    - This is void because the record has already been CAS'd into the hash table, so it must perform operations that do not fail.

**Delete** (`DeleteInfo`)
  - `ConcurrentDeleter`
    - ***Expiration***: Irrelevant because it is a Delete operation.
    - ***Cancellation***: The app sets deleteInfo.Action to DeleteAction.CancelOperation and returns false. Delete() returns (status.Canceled).
  - `SingleDeleter`
    - ***Expiration***: Irrelevant because it is a Delete operation.
    - ***Cancellation***: The app sets deleteInfo.Action to DeleteAction.CancelOperation and returns false. Delete() returns (status.Canceled).
  - `PostSingleDeleter`
    - This is void because the record has already been CAS'd into the hash table, so it must perform operations that do not fail.

### Sessions

Once FASTER is instantiated, one issues operations to FASTER by creating logical sessions. A session represents a "mono-threaded" sequence of operations issued to FASTER. There is no concurrency within a session, but different sessions may execute concurrently. Sessions do not need to be affinitized to threads, but if they are, FASTER can leverage the same (covered later). You create a session as follows:

```cs
using var session = store.NewSession(new Functions());
```

An equivalent, but more optimized API requires you to specify the Functions type a second time (it allows us to avoid accessing the session via an interface call):

```cs
using var session = store.For(new Functions()).NewSession<Functions>();
```

You can then perform a sequence of read, upsert, and RMW operations on the session. FASTER supports both synchronous and async versions of all operations. While all methods exist in an async form, only read and RMW are generally expected to go async; upserts and deletes will only go async when it is necessary to wait on flush operations when appending records to the log. The basic forms of these operations are described below; additional overloads are available.

#### Read

```cs
// Sync
var status = session.Read(ref key, ref output);
var status = session.Read(ref key, ref input, ref output, context, serialNo);

// Async
var (status, output) = (await session.ReadAsync(key, input)).Complete();
```

#### Upsert

```cs
// Sync
var status = session.Upsert(ref key, ref value);
var status = session.Upsert(ref key, ref value, context, serialNo);

// Async with sync operation completion
var status = (await s1.UpsertAsync(ref key, ref value)).Complete();

// Fully async (completions may themselves need to go async)
var r = await session.UpsertAsync(ref key, ref value);
while (r.Status.IsPending)
   r = await r.CompleteAsync();
```

#### RMW

```cs
// Sync
var status = session.RMW(ref key, ref input);
var status = session.RMW(ref key, ref input, ref output);
var status = session.RMW(ref key, ref input, context, serialNo);

// Async with sync operation completion (completion may rarely go async)
var status = (await session.RMWAsync(ref key, ref input)).Complete();

// Fully async (completion may rarely go async and require multiple iterations)
var r = await session.RMWAsync(ref key, ref input);
while (r.Status.IsPending)
   r = await r.CompleteAsync();
Console.WriteLine(r.Output);
```

#### Delete

```cs
// Sync
var status = session.Delete(ref key);
var status = session.Delete(ref key, context, serialNo);

// Async
var status = (await s1.DeleteAsync(ref key)).Complete();

// Fully async
var r = await session.DeleteAsync(ref key);
while (r.Status.IsPending)
   r = await r.CompleteAsync();
```

### Pending Operations

The sync form of `Read`, `Upsert`, `RMW`, and `Delete` may go pending due to IO operations. When a `Status.IsPending` is returned, you can call `CompletePending()` to wait for the results to arrive. It is generally most performant to issue many of these operations and call `CompletePending()` periodically or upon completion of a batch. An optional `wait` parameter allows you to wait until all pending operations issued on the session until that point are completed before this call returns. A second optional parameter, `spinWaitForCommit` allows you to further wait until all operations until that point are committed by a parallel checkpointing thread.

Pending Read or RMW operations call the appropriate completion callback on the functions object: `ReadCompletionCallback` or `RMWCompletionCallback`, respectively, will be called.

For ease of retrieving outputs from the calling code, there is also a `CompletePendingWithOutputs()` and a `CompletePendingWithOutputsAsync()` that return an iterator over the `Output`s that were completed.

```cs
// Sync API
session.CompletePending(wait: true);
session.CompletePendingWithOutputs(out var completedOutputs, wait: true);

// Async API
await session.CompletePendingAsync();
var completedOutputs = await session.CompletePendingWithOutputsAsync();
```

Using the `*WithOutputs` variant, you can iterate over the completed outputs on the
calling thread itself (instead of using the completion callbacks), as follows:

```cs
while (completedOutputs.Next())
{
   ref var key = ref completedOutputs.Current.Key;
   ref var output = ref completedOutputs.Current.Output;
}
completedOutputs.Dispose();
```

### Disposing

If you have not used `using` statements for auto-dispose, you can now manually dispose the created objects. 
First, the session is disposed:

```cs
session.Dispose();
```

When all sessions are done operating on FASTER, you finally dispose the FasterKV instance and the settings:

```cs
store.Dispose();
settings.Dispose();
```

## Larger-Than-Memory Data Support

FasterKV consists of an in-memory hash table that points to records in a hybrid log that spans storage and main memory. When you 
instantiate a new FasterKV instance, no log is created on disk. Records stay in main memory part of the hybrid log, whose size is
configured via `FasterKVSettings.MemorySize` when calling the constructor. Specifically, the memory portion of the log takes up a size of
`MemorySize` bytes of space. As long as all records fit in this space, nothing will be spilled to storage. Note that for C# class types, the 
log only holds references (pointers) to heap data (discussed [here](/FASTER/docs/fasterkv-tuning#managing-log-size-with-c-objects)).

Once the mutable portion of main memory is full, pages become immutable and start getting flushed to storage and you will see the
log size grow on disk. Reads of flushed records will be served by going to disk, returning a status of `Status.IsPending`, as discussed
above.

If you need recoverability, you need to take a checkpoint of FASTER. This will cause all data in the memory portion of the log to
be proactively written to disk, along with checkpoint metadata that allows you to subsequently recover. Briefy, you can checkpoint 
FASTER in two ways:

1. Flush the hybrid log to disk, i.e., make everything read-only:
   ```cs
   await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
   ```
2. Take a "snapshot" of all in-memory data into a _separate_ file, and continue in-place updates in the mutable log:
   ```cs
   await store.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot);
   ```

Recovery is simple; after instantiating a new store, you call:
```cs
store.Recover();
```

You can also checkpoint the hash index for faster recovery. Learn more about checkpoint-recovery on a dedicated page [here](/FASTER/docs/fasterkv-recovery/).


## Quick End-To-End Sample

Following is a simple end-to-end sample where all data is in memory, so we do not worry about pending 
I/O operations. There is no checkpointing in this example as well.

```cs
public static void Test()
{
   using var settings = new FasterKVSettings<long, long>("c:/temp");
   using var store = new FasterKV<long, long>(settings);
   using var session = store.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));
   
   long key = 1, value = 1, input = 10, output = 0;
   session.Upsert(ref key, ref value);
   session.Read(ref key, ref output);
   Debug.Assert(output == value);
   session.RMW(ref key, ref input);
   session.RMW(ref key, ref input, ref output);
   Debug.Assert(output == value + 20);
}
```

We use the default out-of-the-box provided `SimpleFunctions<Key, Value>` in the above example. In these functions,
`Input` and `Output` are simply set to `Value`, while `Context` is an empty struct `Empty`.

## More Examples

Several sample projects are located in the [cs/samples](https://github.com/Microsoft/FASTER/tree/main/cs/samples) folder
of GitHub.

Advanced samples and prototypes are available in [cs/playground](https://github.com/Microsoft/FASTER/tree/main/cs/playground).

Benchmarking code is present at [cs/benchmark](https://github.com/Microsoft/FASTER/tree/main/cs/benchmark)

Unit tests are a useful resource to see how FASTER is used as well. They are in 
[/cs/test](https://github.com/Microsoft/FASTER/tree/main/cs/test).

All these call be accessed through Visual Studio via the main FASTER.sln solution file at
[/cs](https://github.com/Microsoft/FASTER/tree/main/cs).

## Key Iteration

FasterKV supports key iteration in order to get the set of distinct keys that are active (not deleted or expired) and indexed by the store. Related pull request is [here](https://github.com/microsoft/FASTER/pull/287). Usage is shown below:

```cs
using var iter = session.Iterate();
while (iter.GetNext(out var recordInfo))
{
   ref Key key = ref iter.GetKey();
   ref Value value = ref iter.GetValue();
}
```

## Log Scan

Recall that FasterKV is backed by a log of records that spans disk and main memory. We support a scan operation of the records between any two log addresses. Note that unlike key iteration, scan does not check for records with duplicate keys or eliminate deleted records. Instead, it reports all records on the log in sequential fashion. `RecordInfo` can be used to check each record's header whether it is a deleted record (`recordInfo.Tombstone` is true for a deleted record). A start address of `0` is used to denote the beginning of the log. In order to scan all read-only records (not in the mutable region), you can end iteration at `store.Log.SafeReadOnlyAddress`. To include mutable records in memory, you can end iteration at `store.Log.TailAddress`. Related pull request is [here](https://github.com/microsoft/FASTER/pull/90). Usage is shown below:

```cs
using var iter = store.Log.Scan(0, fht.Log.SafeReadOnlyAddress);
while (iter.GetNext(out var recordInfo))
{
   ref Key key = ref iter.GetKey();
   ref Value value = ref iter.GetValue();
}
```

## Log Operations

FasterKV exposes a Log interface (`store.Log`) to perform different kinds of operations on the log underlying the store. A similar endpoint is exposed for the read cache as well (`store.ReadCache`). The interface supports a rich suite of operations:

* Access various pre-defined address points: `store.Log.BeginAddress`, `store.Log.HeadAddress`, `store.Log.SafeReadOnlyAddress`, `store.Log.TailAddress`
* Truncate the log until, but not including, untilAddress: `store.Log.ShiftBeginAddress(untilAddress)`
  * You can use this to delete the database contents in bulk: `store.Log.ShiftBeginAddress(store.Log.TailAddress)`
  * Deletion of the log on disk only occurs at segment boundary (file) granularity, as per `SegmentSizeBits` defined in log settings.
* Shift log head address to prune memory foorprint of hybrid log: `store.Log.ShiftHeadAddress(untilAddress, wait: true)`
* Shift log read-only address to make records immutable and flush them to disk: `store.Log.ShiftReadOnlyAddress(untilAddress, wait: true)`
* Flush log until current tail (records are still retained in memory): `store.Log.Flush(wait: true)`
* Flush log and evict all records from memory: `store.Log.FlushAndEvict(wait: true)`
* Delete log entirely from memory as a synchronous operation (cannot allocate on the log after this point): `store.Log.DisposeFromMemory()`
* Subscribe to log records as they become read-only: `store.Log.Subscribe(observer)`
* Subscribe to log records as they are evicted from memory (at HeadAddress): `store.Log.SubscribeEvictions(observer)`
* Scan the log: see [here](#log-scan) for details
* Compact the log: see [here](#log-compaction) for details

## Handling Variable Length Keys and Values

There are several ways to handle variable length keys and values in FasterKV:

* Use `Memory<byte>` or more generally, `Memory<T> where T : unmanaged` as your value and input type, and `ReadOnlyMemory<T> where T : unmanaged` or `Memory<T> where T : unmanaged` as the key type. This will work out of the box and store the keys and values on the main FasterKV log (no object log required). This provides very high performance with a single backing hybrid log. Your `IFunctions` implementation can use or extend the default supplied base class called [MemoryFunctions](https://github.com/microsoft/FASTER/blob/main/cs/src/core/VarLen/MemoryFunctions.cs).

* Use standard C# class types (e.g., `byte[]`, `string`, and `class` objects) to store keys and values. This is easiest and also allows you to perform in-place updates of objects that are still in the mutable region of the log, like a traditional C# object cache. However, there may be performance implications due to the creation of fine-grained C# objects and the fact that we need to store objects in a separate object log.

* Use `SpanByte` our wrapper struct concept, as your key and/or value type. `SpanByte` represents a simple sequence of bytes with a 4-byte (int) header that stores the length of the rest of the payload. `SpanByte` is similar to `Memory<T>` support, but provides the highest performance for variable-length keys and values. It is only compatible with pinned memory and fixed `Span<byte>` inputs. Learn more about its use in the related PR [here](https://github.com/microsoft/FASTER/pull/349).

* Our support for `Memory<T> where T : unmanaged` and `SpanByte` both use an underlying functionality that we call _variable-length structs_, that you can use directly if `SpanByte` does not fit your needs, e.g., if you want to use only two bytes for the length header or want to store ints instead of bytes. You do this by specifying `VariableLengthStructSettings` during FasterKV construction, and `SessionVariableLengthStructSettings` during session creation.

Check out the sample [here](https://github.com/Microsoft/FASTER/tree/main/cs/samples/StoreVarLenTypes) for how to use `Memory<T> where T : unmanaged` and `SpanByte` with a single backing hybrid log. For C# class objects, check out [StoreCustomTypes](https://github.com/microsoft/FASTER/tree/main/cs/samples/StoreCustomTypes).

## Log Compaction

FASTER is backed by a record-oriented log laid out across storage and main memory, with one disk file per log segment. As the log grows, you have two options to clean up the log. The first is a simple log truncation from the beginning of the log. By doing so, you are effectively expiring all log records older than the begin address. To perform this, we shift the begin address (`BeginAddress`) of the log as follows:

```cs
  store.Log.ShiftBeginAddress(long newBeginAddress);
```

This call shifts the begin address of the log, deleting any log segment files needed.

FASTER also support true "log compaction", where the log is scanned and live records are copied to the tail so that the store does not expire any data. You can perform log compaction over a FasterKv client session (`ClientSession`) as follows:

```cs
  compactUntil = session.Compact(compactUntil, shiftBeginAddress: true);
```

This call perform synchronous compaction on the provided session until the specific `compactUntil` address, scanning and copying the live records to the tail. It returns the actual log address that the call compacted until (next nearest record boundary). You can only compact until the log's `SafeReadOnlyAddress` as the rest of the log is still mutable in-place. If you wish, you can move the read-only address to the tail by calling `store.Log.ShiftReadOnlyToTail(store.Log.TailAddress, true)` or by simply taking a fold-over checkpoint (`await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver)`).

Typically, you may compact around 20% (up to 100%) of the log, e.g., you could set `compactUntil` address to `store.Log.BeginAddress + 0.2 * (store.Log.SafeReadOnlyAddress - store.Log.BeginAddress)`. The parameter `shiftBeginAddress`, when true, causes log compaction to also automatically shift the log's begin address when the compaction is complete. However, since live records are written to the tail, directly shifting the begin address may result in data loss if the store fails immediately after the call. If you do not want to lose data, you need to trigger compaction with `shiftBeginAddress` set to false, then complete a checkpoint (either fold-over or snaphot is fine), and then shift the begin address. Finally, you can take another checkpoint to save the new begin address. This is shown below:

```cs
  long compactUntil = store.Log.BeginAddress + 0.2 * (store.Log.SafeReadOnlyAddress - store.Log.BeginAddress);
  compactUntil = session.Compact(compactUntil, shiftBeginAddress: false);
  await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
  store.Log.ShiftBeginAddress(compactUntil);
  await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
```


## Checkpointing and Recovery

FASTER can operate as a persistent store supporting asynchronous non-blocking 
checkpoint-based recovery. Go to [Checkpointing and Recovery](/FASTER/docs/fasterkv-recovery) 
for details and examples of this capability.
