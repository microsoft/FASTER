Introduction to FASTER C#
=========================

FASTER C# works in .NET Framework and .NET core, and can be used in both a single-threaded and concurrent setting. It has been tested to work on both Windows and Linux. It exposes an API that allows one to performs a mix of Reads, Blind Updates (Upserts), and Read-Modify-Write operations. It supports data larger than memory, and accepts an `IDevice` implementation for storing logs on storage. We have provided `IDevice` implementations for local file system and Azure Page Blobs, but one may create new devices as well. We also offer meta-devices that can group device instances into sharded and tiered configurations. FASTER  may be used as a high-performance replacement for traditional concurrent data structures such as the .NET ConcurrentDictionary, and additionally supports larger-than-memory data. It also supports checkpointing of the data structure - both incremental and non-incremental. Operations on FASTER can be issued synchronously or asynchronously, i.e., using the C# `async` interface.

Table of Contents
-----------
* [Getting FASTER](#getting-faster)
* [Basic Concepts](#basic-concepts)
* [Quick End-to-End Sample](#quick-end-to-end-sample)
* [More Examples](#more-examples)
* [Checkpointing and Recovery](#checkpointing-and-recovery)

## Getting FASTER

### Building From Sources
Clone the Git repo, open cs/FASTER.sln in VS 2019, and build.

### NuGet
You can install FASTER binaries using Nuget, from Nuget.org. Right-click on your project, manage NuGet packages, browse for FASTER. Here is a [direct link](https://www.nuget.org/packages/Microsoft.FASTER).

## Basic Concepts

### FASTER Operations

FASTER supports three basic operations:
1. Read: Read data from the key-value store
2. Upsert: Blind upsert of values into the store (does not check for prior values)
3. Read-Modify-Write: Update values in store, used to implement operations such as Sum and Count.

### Constructor

Before instantiating FASTER, you need to create storage devices that FASTER will use. If you are using value (blittable) types, you only need one log device. If you are also using objects, you need to create a separate object log device.

```Csharp
IDevice log = Devices.CreateLogDevice("C:\\Temp\\hybridlog_native.log");
```

Then, an instance of FASTER is created as follows:

```Csharp
fht = new FasterKV<Key, Value, Input, Output, Empty, Functions>
  (1L << 20, new Functions(), new LogSettings { LogDevice = log });
```

### Type Arguments to Constructor

There are six basic concepts, provided as generic type arguments when instantiating FASTER:
1. `Key`: This is the type of the key, e.g., `long`.
2. `Value`: This is the type of the value stored in FASTER.
3. `Input`: This is the type of input provided to FASTER when calling Read or RMW. It may be regarded as a parameter for the Read or RMW operation. For example, with RMW, it may be the delta being accumulated into the value.
4. `Output`: This is the type of the output of a Read operation. The reader copies the relevant parts of the Value to Output.
5. `Context`: User-defined context for the operation. Use `Empty` if there is no context necesssary.
6. `Functions`: These is a type that implemented `IFunctions<>` and provides all callbacks necessary to use FASTER.

### Callback Functions

The user provides an instance of a type that implements `IFunctions<>`, or its corresponding abstract base class `FunctionsBase<>`. This type encapsulates all the callbacks, which are described next:

1. SingleReader and ConcurrentReader: These are used to read from the store values and copy them to Output. Single reader can assume there are no concurrent operations.
2. SingleWriter and ConcurrentWriter: These are used to write values to the store, from a source value.
3. Completion callbacks: Called by FASTER when various operations complete.
4. RMW Updaters: There are three updaters that the user specifies, InitialUpdater, InPlaceUpdater, and CopyUpdater. Together, they are used to implement the RMW operation.

### Constructor Parameters

1. Hash Table Size: This the number of buckets allocated to FASTER, where each bucket is 64 bytes (size of a cache line).
2. Log Settings: These are settings related to the size of the log and devices used by the log.
3. Checkpoint Settings: These are settings related to checkpoints, such as checkpoint type and folder. Covered in the section on checkpointing [below](#checkpointing-and-recovery).
4. Serialization Settings: Used to provide custom serializers for key and value types. Serializers implement `IObjectSerializer<Key>` for keys and `IObjectSerializer<Value>` for values. *These are only needed for non-blittable types such as C# class objects.*
5. Key Equality comparer: Used for providing a better comparer for keys, implements `IFasterEqualityComparer<Key>`.

The total in-memory footprint of FASTER is controlled by the following parameters:
1. Hash table size: This parameter (the first contructor argument) times 64 is the size of the in-memory hash table in bytes.
2. Log size: The logSettings.MemorySizeBits denotes the size of the in-memory part of the hybrid log, in bits. In other words, the size of the log is 2^B bytes, for a parameter setting of B. Note that if the log points to class objects, this size does not include the size of objects, as this information is not accessible to FASTER. The older part of the log is spilled to storage.

### Sessions (Threads)

Once FASTER is instantiated, one issues operations to FASTER by creating logical sessions. A session represents a sequence of operations issued to FASTER. There is no concurrency within a session, but different sessions may execute concurrently. Sessions do not need to be affinitized to threads, but if they are, FASTER can leverage the same (covered later). You create a session as follows:

```var session = fht.NewSession();```

You can then perform a sequence of read, upsert, and RMW operations on the session. FASTER supports sync and async versions of operations. Examples:

```cs
var status = session.Read(ref key, ref input, ref output, ref context);
await session.ReadAsync(key, input);
```

At the end, the session is disposed:

```session.Dispose();```

When all sessions are done operating on FASTER, you finally dispose the FASTER instance:

```fht.Dispose();```


## Quick End-To-End Sample

Following is a simple end-to-end sample where all data is in memory, so we do not worry about pending 
I/O operations. There is no checkpointing in this example as well.

```CSharp
public static void Test()
{
  var log = Devices.CreateLogDevice("C:\\Temp\\hlog.log");
  var fht = new FasterKV<long, long, long, long, Empty, Funcs>
    (1L << 20, new Funcs(), new LogSettings { LogDevice = log });
  var session = fht.NewSession();
  long key = 1, value = 1, input = 10, output = 0;
  session.Upsert(ref key, ref value, Empty.Default, 0);
  session.Read(ref key, ref input, ref output, Empty.Default, 0);
  Debug.Assert(output == value);
  session.RMW(ref key, ref input, Empty.Default, 0);
  session.RMW(ref key, ref input, Empty.Default, 0);
  session.Read(ref key, ref input, ref output, Empty.Default, 0);
  Debug.Assert(output == value + 20);
  session.Dispose();
  fht.Dispose();
  log.Close();
}
```

Functions for this example:

```CSharp
public class Funcs : IFunctions<long, long, long, long, Empty>
{
  public void SingleReader(ref long key, ref long input, ref long value, ref long dst) => dst = value;
  public void SingleWriter(ref long key, ref long src, ref long dst) => dst = src;
  public void ConcurrentReader(ref long key, ref long input, ref long value, ref long dst) => dst = value;
  public void ConcurrentWriter(ref long key, ref long src, ref long dst) => dst = src;
  public void InitialUpdater(ref long key, ref long input, ref long value) => value = input;
  public void CopyUpdater(ref long key, ref long input, ref long oldv, ref long newv) => newv = oldv + input;
  public void InPlaceUpdater(ref long key, ref long input, ref long value) => value += input;
  public void UpsertCompletionCallback(ref long key, ref long value, Empty ctx) { }
  public void ReadCompletionCallback(ref long key, ref long input, ref long output, Empty ctx, Status s) { }
  public void RMWCompletionCallback(ref long key, ref long input, Empty ctx, Status s) { }
  public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
}
```

## More Examples

Several example projects are located in [cs/playground](https://github.com/Microsoft/FASTER/tree/master/cs/playground) (available through the solution). You can also check out more samples in the unit tests in [/cs/test](https://github.com/Microsoft/FASTER/tree/master/cs/test), which can be run through the solution or using NUnit-Console. 

A basic YCSB benchmark is located in [cs/performance/Benchmark](https://github.com/Microsoft/FASTER/tree/master/cs/performance/Benchmark), and a performance test configurable with multiple parameter combinations is located in [cs/performance/PerfTest](https://github.com/Microsoft/FASTER/tree/master/cs/performance/PerfTest). Both are available through the main solution.

## Checkpointing and Recovery

FASTER supports **checkpoint-based recovery**. Every new checkpoint persists (or makes durable) additional user-operations (Read, Upsert or RMW). FASTER allows clients to keep track of operations that have persisted and those that have not using a session-based API. 

Recall that each FASTER client starts a session, associated with a unique session ID (or name). All FASTER session operations (Read, Upsert, RMW) carry a monotonic sequence number (sequence numbers are implicit in case of async calls). At any point in time, one may call `Checkpoint` to initiate an asynchronous checkpoint of FASTER. After calling `Checkpoint`, each FASTER session is (eventually) notified of a commit point. A commit point consists of (1) a sequence number, such that all operations until, and no operations after, that sequence number, are guaranteed to be persisted as part of that checkpoint; (2) an optional exception list of operations that were not part of the commit because they went pending and could not complete before the checkpoint, because the session was not active at the time of checkpointing.

The commit point information can be used by the session to clear any in-memory buffer of operations waiting to be performed. During recovery, sessions can continue using `ResumeSession` invoked with the same session ID. The function returns the thread-local sequence number until which that session hash been recovered. The new thread may use this information to replay all uncommitted operations since that point.

With async session operations on FASTER, one may optionally set a boolean parameter `waitForCommit` in the calls. When set, the async call completes only after the operation is made persistent by an asynchronous checkpoint. The user is responsible for performing the checkpoint asynchronously. An async upsert which returns only after the upsert is made durable, is shown below:

```cs
await session.UpsertAsync(key, value, waitForCommit: true);
```

Below, we show a simple recovery example with asynchronous checkpointing.

```Csharp
public class PersistenceExample 
{
  private FasterKV<long, long, long, long, Empty, Funcs> fht;
  private IDevice log;
  
  public PersistenceExample() 
  {
    log = Devices.CreateLogDevice("C:\\Temp\\hlog.log");
    fht = new FasterKV<long, long, long, long, Empty, Funcs>
    (1L << 20, new Funcs(), new LogSettings { LogDevice = log });
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
    using var session = fht.NewSession("s1");
    long seq = 0; // sequence identifier
    
    long key = 1, input = 10;
    while(true) 
    {
      key = (seq % 1L << 20);
      session.RMW(ref key, ref input, Empty.Default, seq++);
    }
  }
  
  private void ContinueSession() 
  {
    using var session = fht.ResumeSession("s1", out CommitPoint cp); // recovered session
    var seq = cp.UntilSerialNo + 1;
    
    long key = 1, input = 10;
    while(true) 
    {
      key = (seq % 1L << 20);
      session.RMW(ref key, ref input, Empty.Default, seq++);
    }
  }
  
  private void IssuePeriodicCheckpoints()
  {
    var t = new Thread(() => 
    {
      while(true) 
      {
        Thread.Sleep(10000);
        fht.TakeFullCheckpoint(out Guid token);
        fht.CompleteCheckpointAsync().GetAwaiter().GetResult();
      }
    });
    t.Start();
  }
}
```

FASTER supports two notions of checkpointing: Snapshot and Fold-Over. The former is a full snapshot of in-memory into a separate snapshot file, whereas the latter is an _incremental_ checkpoint of the changes since the last checkpoint. Fold-Over effectively moves the read-only marker of the hybrid log to the tail, and thus all the data is persisted as part of the same hybrid log (there is no separate snapshot file). All subsequent updates are written to new hybrid log tail locations, which gives Fold-Over its incremental nature. You can find a few basic checkpointing examples [here](https://github.com/Microsoft/FASTER/blob/master/cs/test/SimpleRecoveryTest.cs) and [here](https://github.com/Microsoft/FASTER/tree/master/cs/playground/SumStore). We plan to add more examples and details going forward.
