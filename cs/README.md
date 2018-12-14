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

### Constructor

An instance of FASTER is created as follows:

```var fht = new FasterKV<Key, Value, Input, Output, Context, Functions>(...);```

#### Type Arguments

There are six basic concepts, provided as generic type arguments when instantiating FASTER:
1. `Key`:
2. `Value`:
3. `Input`:
4. `Output`:
5. `Context`:
6. `Functions`:

#### Constructor Parameters

1. Hash Table Size:
2. Log Settings:
3. Checkpoint Settings:
4. Serialization Settings:
5. Key Equality comparer:

#### Log Settings

#### Checkpoint Settings

Covered in the section on checkpointing [below](#checkpointing-and-recovery).

#### Serialization Settings

### Sessions (Threads)

Once FASTER is instantiated, threads may use FASTER by registering themselves via the concept of a Session.




## Features


## Quick Sample
 
## More Examples

Several example projects are located in cs/playground (available through the solution). You can also check out more samples in the unit tests in /cs/test, which can be run through the solution or using NUnit-Console. A basic YCSB benchmark is located in cs/benchmark, also available through the main solution.

## Checkpointing and Recovery

