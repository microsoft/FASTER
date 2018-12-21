Introduction to FASTER C#
=========================

FASTER C# works in .NET Framework and .NET core, and can be used in both a single-threaded and concurrent setting. It has been tested to work on both Windows and Linux. It exposes an API that allows one to performs a mix of Reads, Blind Updates (Upserts), and Read-Modify-Write operations. It supports data larger than memory, and accepts an `IDevice` implementation for storing logs on storage. We have provided `IDevice` implementations for local file system, but one may create new devices, for example, to write to remote file systems. Alternatively, one may mount remote storage into the local file system. FASTER  may be used as a high-performance replacement for traditional concurrent data structures such as the .NET ConcurrentDictionary, and additionally supports larger-than-memory data. It also supports checkpointing of the data structure - both incremental and non-incremental.

Table of Contents
-----------
* [Getting FASTER](#getting-faster)
* [Basic Concepts](#basic-concepts)
* [Features](#features)
* [Quick Sample](#quick-sample)
* [More Examples](#more-examples)
* [Checkpointing and Recovery](#checkpointing-and-recovery)

## Getting FASTER

### Building From Sources
Clone the Git repo, open cs/FASTER.sln in VS 2017, and build.

### NuGet
You can install FASTER binaries using Nuget, from Nuget.org. Right-click on your project, manage NuGet packages, browse for FASTER. Here is a [direct link](https://www.nuget.org/packages/FASTER).

## Basic Concepts

### FASTER Operations

FASTER supports three basic operations:
1. Read: Read data from the key-value store
2. Upsert: Blind upsert of values into the store (does not check for prior values)
3. Read-Modify-Write: Update values in store, used to implement operations such as Sum and Count.

### Constructor

Before instantiating FASTER, you need to create storage devices that FASTER will use. If you are using blittable types, you only need the hybrid log device. If you are also using objects, you need to create a separate object log device.

```IDevice log = Devices.CreateLogDevice("C:\\Temp\\hybridlog_native.log");```

Then, an instance of FASTER is created as follows:

```
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

The user provides an instance of a type that implements `IFunctions<>`. This type encapsulates all the callbacks, which are described next:

1. SingleReader and ConcurrentReader: These are used to read from the store values and copy them to Output. Single reader can assume there are no concurrent operations.
2. SingleWriter and ConcurrentWriter: These are used to write values to the store, from a source value.
3. Completion callbacks: Called when various operations complete.
4. RMW Updaters: There are three updaters that the user specifies, InitialUpdater, InPlaceUpdater, and CopyUpdater. Together, they are used to implement the RMW operation.

### Constructor Parameters

1. Hash Table Size: This the number of buckets allocated to FASTER, where each bucket is 64 bytes (size of a cache line).
2. Log Settings: These are setings related to the size of the log and devices used by the log.
3. Checkpoint Settings: These are settings related to checkpoints, such as checkpoint type and folder. Covered in the section on checkpointing [below](#checkpointing-and-recovery).
4. Serialization Settings: Used to provide custom serializers for key and value types. Serializers implement `IObjectSerializer<Key>` for keys and `IObjectSerializer<Value>` for values. *These are only needed for non-blittable types such as C# class objects.*
5. Key Equality comparer: Used for providing a better comparer for keys, implements `IFasterEqualityComparer<Key>`.

### Sessions (Threads)

Once FASTER is instantiated, threads may use FASTER by registering themselves via the concept of a Session, using the call 

```fht.StartSession();```

At the end, the thread calls:

```fht.StopSession();```

When all threads are done operating on FASTER, you finally dispose the FASTER instance:

```fht.Dispose();```


## Quick End-To-End Sample
 
```
public static void Test()
{
  var log = Devices.CreateLogDevice("C:\\Temp\\hlog.log");
  var fht = new FasterKV<long, long, long, long, Empty, Funcs>
    (1L << 20, new Funcs(), new LogSettings { LogDevice = log });
  fht.StartSession();
  long key = 1, value = 1, input = 10, output = 0;
  fht.Upsert(ref key, ref value, Empty.Default, 0);
  fht.Read(ref key, ref input, ref output, Empty.Default, 0);
  Debug.Assert(output == value);
  fht.RMW(ref key, ref input, Empty.Default, 0);
  fht.RMW(ref key, ref input, Empty.Default, 0);
  fht.Read(ref key, ref input, ref output, Empty.Default, 0);
  Debug.Assert(output == value + 20);
  fht.StopSession();
  fht.Dispose();
  log.Close();
}
```

Functions for this example:

```
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
  public void CheckpointCompletionCallback(Guid sessionId, long serialNum) { }
}
```

## More Examples

Several example projects are located in cs/playground (available through the solution). You can also check out more samples in the unit tests in /cs/test, which can be run through the solution or using NUnit-Console. A basic YCSB benchmark is located in cs/benchmark, also available through the main solution.

## Checkpointing and Recovery

