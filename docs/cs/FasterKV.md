---
layout: default
title: FasterKV store + cache in C#
parent: FASTER C#
nav_order: 4
permalink: /cs/fasterkv
---

Introduction to FasterKV C#
===========================

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

Table of Contents
-----------
* [Getting FASTER](#getting-faster)
* [Basic Concepts](#basic-concepts)
* [Quick End-to-End Sample](#quick-end-to-end-sample)
* [More Examples](#more-examples)
* [Handling Variable Length Keys and Values](#handling-variable-length-keys-and-values)
* [Log Compaction](#log-compaction)
* [Checkpointing and Recovery](#checkpointing-and-recovery)

## Getting FASTER

### Building From Sources
Clone the Git repo, open cs/FASTER.sln in Visual Studio, and build.

### NuGet
You can install FASTER binaries using Nuget, from Nuget.org. Right-click on your project, manage NuGet packages, browse 
for Microsoft.FASTER.Core. Here is a [direct link](https://www.nuget.org/packages/Microsoft.FASTER.Core).

## Basic Concepts

### FASTER Operations

FASTER supports three basic operations:
1. Read: Read data from the key-value store
2. Upsert: Blind upsert of values into the store (does not check for prior values)
3. Read-Modify-Write: Update values in store atomically, used to implement operations such as Sum and Count.

### Constructor

Before instantiating the FASTER store, you need to create storage devices that FASTER will use. If you are using value
(blittable) types such as `long`, `int`, and structs with value-type members, you only need one log device. If you are 
also using objects such as classes and strings, you need to create a separate object log device.

```Csharp
var log = Devices.CreateLogDevice(@"C:\Temp\hlog.log");
```

The store constructor requires a key type and a value type. For example, an instance of FASTER over `long` keys and 
`string` values is created as follows:

```Csharp
var store = new FasterKV<long, string>(1L << 20, new LogSettings { LogDevice = log });
```

#### Constructor Parameters

1. Hash Table Size: This the number of buckets allocated to FASTER, where each bucket is 64 bytes (size of a cache line).
2. Log Settings: These are settings related to the size of the log and devices used by the log.
3. Checkpoint Settings: These are settings related to checkpoints, such as checkpoint type and folder. Covered in the 
section on checkpointing [below](#checkpointing-and-recovery).
4. Serialization Settings: Used to provide custom serializers for key and value types. Serializers implement 
`IObjectSerializer<Key>` for keys and `IObjectSerializer<Value>` for values. *These are only needed for 
non-blittable types such as C# class objects.*
5. Key Equality comparer: Used for providing a better comparer for keys, implements `IFasterEqualityComparer<Key>`.

The total in-memory footprint of FASTER is controlled by the following parameters:
1. Hash table size: This parameter (the first contructor argument) times 64 is the size of the in-memory hash table in bytes.
2. Log size: The logSettings.MemorySizeBits denotes the size of the in-memory part of the hybrid log, in bits. In other 
words, the size of the log is 2^B bytes, for a parameter setting of B. Note that if the log points to class key or value 
objects, this size only includes the 8-byte reference to the object. The older part of the log is spilled to storage.

Read more about managing memory in FASTER [here](../tuning).

### Callback Functions

The user provides an instance of a type that implements `IFunctions<Key, Value, Input, Output, Context>`, or its corresponding 
abstract base class `<Key, Value, Input, Output, Context>`. Apart from Key and Value, functions use three new types:
1. `Input`: This is the type of input provided to FASTER when calling Read or RMW. It may be regarded as a parameter for the 
Read or RMW operation. For example, with RMW, it may be the delta being accumulated into the value.
2. `Output`: This is the type of the output of a Read operation. The reader copies the relevant parts of the Value to Output.
3. `Context`: User-defined context for the operation. Use `Empty` if there is no context necesssary.


`IFunctions<>` encapsulates all callbacks made by FASTER back to the caller, and are described next:

1. SingleReader and ConcurrentReader: These are used to read from the store values and copy them to Output. Single reader can 
assume that there are no concurrent operations on the record.
2. SingleWriter and ConcurrentWriter: These are used to write values to the store, from a source value. Concurrent writer can 
assume that there are no concurrent operations on the record.
3. Completion callbacks: Called by FASTER when various operations complete.
4. RMW Updaters: There are three updaters that the user specifies, InitialUpdater, InPlaceUpdater, and CopyUpdater. Together, 
they are used to implement the RMW operation.


### Sessions

Once FASTER is instantiated, one issues operations to FASTER by creating logical sessions. A session represents a "mono-threaded" sequence 
of operations issued to FASTER. There is no concurrency within a session, but different sessions may execute concurrently. Sessions do not
need to be affinitized to threads, but if they are, FASTER can leverage the same (covered later). You create a session as follows:

```var session = store.NewSession(new Functions());```

An equivalent, but more optimized API requires you to specify the Functions type a second time (it allows us to avoid accessing the 
session via an interface call):

```cs
var session = store.For(new Functions()).NewSession<Functions>();
```

You can then perform a sequence of read, upsert, and RMW operations on the session. FASTER supports sync and async versions of 
operations. The operations are described below.

#### Read

```cs
var status = session.Read(ref key, ref output);
var status = session.Read(ref key, ref input, ref output, ref context, ref serialNo);
await session.ReadAsync(key, input);
```

#### Upsert

```cs
var status = session.Upsert(ref key, ref value);
var status = session.Upsert(ref key, ref value, ref context, ref serialNo);
await session.UpsertAsync(key, value);
```

#### RMW

```cs
var status = session.RMW(ref key, ref input);
var status = session.RMW(ref key, ref input, ref context, ref serialNo);
await session.RMWAsync(key, input);
```

### Disposing

At the end, the session is disposed:

```session.Dispose();```

When all sessions are done operating on FASTER, you finally dispose the FasterKV instance:

```store.Dispose();```


## Quick End-To-End Sample

Following is a simple end-to-end sample where all data is in memory, so we do not worry about pending 
I/O operations. There is no checkpointing in this example as well.

```cs
public static void Test()
{
  var log = Devices.CreateLogDevice("C:\\Temp\\hlog.log");
  var store = new FasterKV<long, long>(1L << 20, new Funcs(), new LogSettings { LogDevice = log });
  var s = fht.NewSession(new SimpleFunctions<long, long>);
  long key = 1, value = 1, input = 10, output = 0;
  s.Upsert(ref key, ref value);
  s.Read(ref key, ref output);
  Debug.Assert(output == value);
  s.RMW(ref key, ref input);
  s.RMW(ref key, ref input);
  s.Read(ref key, ref output);
  Debug.Assert(output == value + 20);
  s.StopSession();
  store.Dispose();
  log.Close();
}
```

We use default out-of-the-box provided `SimpleFunctions<Key,Value>` in the above example. In these functions,
`Input` and `Output` are simply set to `Value`, while `Context` is an empty struct `Empty`.

## More Examples

Several sample projects are located in the [cs/samples](https://github.com/Microsoft/FASTER/tree/master/cs/samples) folder
of GitHub.

Advanced samples and prototypes are available in [cs/playground](https://github.com/Microsoft/FASTER/tree/master/cs/playground).

Benchmarking code is present at [cs/benchmark](https://github.com/Microsoft/FASTER/tree/master/cs/benchmark)

Unit tests are a useful resource to see how FASTER is used as well. They are in 
[/cs/test](https://github.com/Microsoft/FASTER/tree/master/cs/test).

All these call be accessed through Visual Studio via the main FASTER.sln solution file at
[/cs](https://github.com/Microsoft/FASTER/tree/master/cs).

## Handling Variable Length Keys and Values

There are two ways to handle variable length keys and values in FasterKV:

* Use standard C# class types (e.g., `byte[]`, `string`, and `class` objects) to store keys and values. This is easiest, but there are performance implications due to the creation of fine-grained C# objects and the fact that we need to store objects in a separate object log.

* Use `Memory<byte>` or more generally, `Memory<T> where T : unmanaged` as your value and input type, and `ReadOnlyMemory<T> where T : unmanaged` or `Memory<T> where T : unmanaged` as the key type. This will work out of the box and store the keys and values on the main FasterKV log (no object log required). Your `IFunctions` implementation can use or extend the default supplied base class called [MemoryFunctions](https://github.com/microsoft/FASTER/blob/master/cs/src/core/VarLen/MemoryFunctions.cs).

* Use `SpanByte` our wrapper struct concept, as your key and/or value type. `SpanByte` represents a simple sequence of bytes with a 4-byte (int) header that stores the length of the rest of the payload. `SpanByte` provides the highest performance for variable-length keys and values, and is compatible with pinned memory and `Span<byte>` inputs. Learn more about its use in the related PR [here](https://github.com/microsoft/FASTER/pull/349).

* Our support for `Memory<T> where T : unmanaged` and `SpanByte` both use an underlying functionality that we call _variable-length structs_, that you can use directly if `SpanByte` does not fit your needs, e.g., if you want to use only two bytes for the length header or want to store ints instead of bytes. You do this by specifying `VariableLengthStructSettings` during FasterKV construction, and `SessionVariableLengthStructSettings` during session creation.

* Check out the sample [here](https://github.com/Microsoft/FASTER/tree/master/cs/samples/StoreVarLenTypes).

## Log Compaction

FASTER is backed by a record-oriented log laid out across storage and main memory, with one disk file per log segment. As the log grows, you have two options to clean up the log. The first is a simple log truncation from the beginning of the log. By doing so, you are effectively expiring all log records older than the begin address. To perform this, we shift the begin address (`BeginAddress`) of the log as follows:

```cs
  store.Log.ShiftBeginAddress(long newBeginAddress);
```

This call shifts the begin address of the log, deleting any log segment files needed.

FASTER also support true "log compaction", where the log is scanned and live records are copied to the tail so that the store does not expire any data. You can perform log compaction over a FasterKv client session (`ClientSession`) as follows:

```cs
  session.Compact(compactUntil, shiftBeginAddress: true);
```

This call perform synchronous compaction on the provided session until the specific `compactUntil` address, scanning and copying the live records to the tail. Typically, you may compact 10-20% of the log, e.g., you set `compactUntil` address to `store.Log.BeginAddress + (store.Log.TailAddress - store.Log.BeginAddress) / 5`. The parameter `shiftBeginAddress`, when true, causes the compation to also shift the begin address when the compaction is complete. However, since live records are written to the tail, this operation may result in data loss if the store fails immediately. If you do not want to lose data, you need to trigger compaction with `shiftBeginAddress` set to false, then complete a checkpoint (either fold-over or snaphot is fine), and then shift the begin address, as shown below:

```cs
  long compactUntil = store.Log.BeginAddress + (store.Log.TailAddress - store.Log.BeginAddress) / 5;
  session.Compact(compactUntil, shiftBeginAddress: false);
  await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
  store.Log.ShiftBeginAddress(compactUntil);
```


## Checkpointing and Recovery

FASTER supports **checkpoint-based recovery**. Every new checkpoint persists (or makes durable) additional user-operations
(Read, Upsert or RMW). FASTER allows clients to keep track of operations that have persisted and those that have not using 
a session-based API. 

Recall that each FASTER client starts a session, associated with a unique session ID (or name). All FASTER session operations
(Read, Upsert, RMW) carry a monotonic sequence number (sequence numbers are implicit in case of async calls). At any point in 
time, one may call `Checkpoint` to initiate an asynchronous checkpoint of FASTER. After calling `Checkpoint`, each FASTER 
session is (eventually) notified of a commit point. A commit point consists of (1) a sequence number, such that all operations
until, and no operations after, that sequence number, are guaranteed to be persisted as part of that checkpoint; (2) an optional
exception list of operations that were not part of the commit because they went pending and could not complete before the 
checkpoint, because the session was not active at the time of checkpointing.

The commit point information can be used by the session to clear any in-memory buffer of operations waiting to be performed. 
During recovery, sessions can continue using `ResumeSession` invoked with the same session ID. The function returns the thread-local 
sequence number until which that session hash been recovered. The new thread may use this information to replay all uncommitted 
operations since that point.

With async session operations on FASTER, operations return as soon as they complete, before commit. In order to wait for commit,
you simply issue an `await session.WaitForCommitAsync()` call. The call completes only after the operation is made persistent by
an asynchronous commit (checkpoint). The user is responsible for initiating the checkpoint asynchronously.

Below, we show a simple recovery example with asynchronous checkpointing.

```Csharp
public class PersistenceExample
{
    private FasterKV<long, long> fht;
    private IDevice log;

    public PersistenceExample()
    {
        log = Devices.CreateLogDevice("C:\\Temp\\hlog.log");
        fht = new FasterKV<long, long>(1L << 20, new LogSettings { LogDevice = log });
    }

    public void Run()
    {
        IssuePeriodicCheckpoints();
        RunSession();
    }

    public void Continue()
    {
        fht.Recover();
        IssuePeriodicCheckpoints();
        ContinueSession();
    }

    /* Helper Functions */
    private void RunSession()
    {
        using var session = fht.NewSession(new SimpleFunctions<long, long>(), "s1");
        long seq = 0; // sequence identifier

        long key = 1, input = 10;
        while (true)
        {
            key = (seq % 1L << 20);
            session.RMW(ref key, ref input, Empty.Default, seq++);
        }
    }

    private void ContinueSession()
    {
        using var session = fht.ResumeSession(new SimpleFunctions<long, long>(), "s1", out CommitPoint cp); // recovered session
        var seq = cp.UntilSerialNo + 1;

        long key = 1, input = 10;
        while (true)
        {
            key = (seq % 1L << 20);
            session.RMW(ref key, ref input, Empty.Default, seq++);
        }
    }

    private void IssuePeriodicCheckpoints()
    {
        var t = new Thread(() =>
        {
            while (true)
            {
                Thread.Sleep(10000);
                (_, _) = fht.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).GetAwaiter().GetResult();
            }
        });
        t.Start();
    }
}
```

FASTER supports two notions of checkpointing: Snapshot and Fold-Over. The former is a full snapshot of in-memory into a separate
snapshot file, whereas the latter is an _incremental_ checkpoint of the changes since the last checkpoint. Fold-Over effectively 
moves the read-only marker of the hybrid log to the tail, and thus all the data is persisted as part of the same hybrid log (there
is no separate snapshot file). All subsequent updates are written to new hybrid log tail locations, which gives Fold-Over its 
incremental nature. You can find a few basic checkpointing examples 
[here](https://github.com/Microsoft/FASTER/blob/master/cs/test/SimpleRecoveryTest.cs) and 
[here](https://github.com/Microsoft/FASTER/tree/master/cs/playground/SumStore). We plan to add more examples and details going
forward.
