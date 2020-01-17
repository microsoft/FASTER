---
layout: default
title: Project Roadmap
nav_order: 2000
description: Project Roadmap
permalink: /roadmap
---

# Roadmap

_This is a living document containing the FASTER team's priorities as well as release notes
for previous releases. Items refer to FASTER C# unless indicated otherwise. For C++ info, 
scroll to [C++ Porting Notes](#c-porting-notes)._

(Scroll to [Release Notes](#release-notes))

## Past and Future Work

The following is a summary of the FASTER team's past work and backlog for the next 6 months.
Completed items are included to provide the context and progress of the work. 

### Past Work

#### General

* [x] Full Read, Upsert, RMW functionality
* [x] Bulk delete via key expiry: user-initiated truncation from head of log (ShiftBeginAddress)
* [x] Persistence support for larger-than-memory data
* [x] Scan operation support for scanning a specified range of log
* [x] Ability to resize (grow) the hash table in powers of two (user-initiated)
* [x] Separately configurable optional read cache for read-heavy workloads
* [x] Support for separate user-defined object hash/equality comparers and object serializers
* [x] Remove C# dynamic code-gen for quicker instantiation, stability, debuggability
* [x] Full support for .NET core and Linux/Mac
* [x] Individual key delete support

#### Log, Cache, and Storage

* [x] Variable sized keys and values using separate object log, see [[here](https://github.com/Microsoft/FASTER/wiki/Variable-length-values#in-c-1)]
* [x] Two allocators (specializing for Blittable and Generic key-value types), with common extensible base class
* [x] Generic IDevice abstraction with out-of-the-box implementations for local storage
* [x] Segmented log on storage (log file broken into segments of configurable size)
* [x] Allocator support for copying reads to tail, useful for update-intensive workloads
* [x] Highly configurable allocator parameters (log and read cache) with respect to memory size, page size, mutable fraction of memory, etc.
* [x] Support for runtime shifting of address markers (e.g., begin, head, read-only) for dynamic tuning of memory contents of allocators (log and read cache).
* [x] Log compaction by rolling forward older active keys
* [x] Support for subscribing to the hybrid log (push-based, as record batches become read-only): [PR](https://github.com/Microsoft/FASTER/pull/133)
* [x] Support for callback when records in hybrid log become read-only: [PR](https://github.com/microsoft/FASTER/pull/133)
* [x] Support for cloud storage, starting with Azure Page Blobs: [PR](https://github.com/Microsoft/FASTER/pull/147)
* [x] Support for tiered storage: [PR](https://github.com/Microsoft/FASTER/pull/151)
* [x] Support for sharded storage: [PR](https://github.com/microsoft/FASTER/pull/162)
* [x] Support for **FasterLog** as an independent first-class abstraction: [PR](https://github.com/microsoft/FASTER/pull/177)
* [x] Reduced log memory footprint significantly (min pages in memory reduced to 1)
* [x] Improved performance of fine-grained epoch protection
* [x] Support per-entry checksums and persistent iterators in FasterLog
* [x] Full support for async interface to FasterLog

#### Checkpoint and Recovery

* [x] CPR-based checkpointing and recovery (snapshot and fold-over modes), see [[here](https://microsoft.github.io/FASTER/#recovery-in-faster)] for details
* [x] Optional separate checkpointing of index and log (so that index may be checkpointed less frequently)
* [x] Auto-recover to most recent checkpoint
* [x] Make checkpointing use a pluggable user-specified interface for providing devices and performing metadata commit: [PR](https://github.com/microsoft/FASTER/pull/161)

### Ongoing and Future Work

* [ ] Better integration with an async/await threading model in C#: [PR](https://github.com/Microsoft/FASTER/pull/130)
* [ ] Scale-out and elasticity support
* [ ] Checksums for storage pages
* [ ] Support iteration over all and only live key-value pairs (different from log scan)
* [ ] Handle log logical addresses greater than 48 bit (up to 64 bit)
* [ ] Expose other data structures, starting with a FIFO FasterQueue

## Release Notes

#### FASTER v2019.10.31.1

* [x] Support for **FasterLog** as an independent first-class abstraction: [PR](https://github.com/microsoft/FASTER/pull/177)
* [x] Reduced log memory footprint significantly (min pages in memory reduced to 1)
* [x] Improved performance of fine-grained epoch protection
* [x] Support per-entry checksums and persistent iterators in FasterLog
* [x] Full support for async interface to FasterLog

#### FASTER v2019.8.27.1

* [x] Improved support for varlen blittable allocator (iteration, compaction): [PR](https://github.com/microsoft/FASTER/pull/164)
* [x] *BREAKING CHANGE*: change return type for InPlaceUpdater and ConcurrentWriter functions to bool. User has to return true for usual in-place-update (IPU) behavior, and return false to force retry as read-copy-update (RCU).
* [x] Support for cloud storage, starting with Azure Page Blobs: [PR](https://github.com/Microsoft/FASTER/pull/147)
* [x] Support for tiered storage: [PR](https://github.com/Microsoft/FASTER/pull/151)
* [x] Make checkpointing use a pluggable user-specified interface for providing devices and performing metadata commit: [PR](https://github.com/microsoft/FASTER/pull/161)
* [x] Support for sharded storage: [PR](https://github.com/microsoft/FASTER/pull/162)

#### FASTER v2019.7.23.1

* [x] Object log recovery bug fix: [PR](https://github.com/microsoft/FASTER/pull/158)
* [x] Option to enable file buffering for local storage device
* [x] Optimizing what is loaded to hybrid log memory during recovery (prior head address onwards only)
* [x] Removing direct call of callback when IO completes synchronously: [PR](https://github.com/microsoft/FASTER/pull/155)
* [x] Fixed checkpoint recovery bug: [PR](https://github.com/microsoft/FASTER/pull/144)
* [x] Adding FILE_SHARE_DELETE when deleteOnClose is used: [PR](https://github.com/microsoft/FASTER/pull/134)
* [x] Support for callback when records in hybrid log become read-only: [PR](https://github.com/microsoft/FASTER/pull/133)


#### FASTER v2019.4.24.4
* [x] Added support for variable sized (inline) structs without object log: [PR](https://github.com/Microsoft/FASTER/pull/120)
* [x] Removed statics from codebase to better support multiple instances: [PR](https://github.com/Microsoft/FASTER/pull/117)
* [x] Fixes related to scheduling pending operations: [PR](https://github.com/Microsoft/FASTER/pull/118)

#### FASTER v2019.4.1.1

* [x] Log compaction by rolling forward older active keys: [PR](https://github.com/Microsoft/FASTER/pull/112)
* [x] Individual key delete support: [PR](https://github.com/Microsoft/FASTER/pull/114)

#### FASTER v2019.3.16.1 (cumulative feature list)

* [x] Full Read, Upsert, RMW functionality
* [x] Bulk delete via key expiry: user-initiated truncation from head of log (ShiftBeginAddress)
* [x] Persistence support for larger-than-memory data
* [x] Scan operation support for scanning a specified range of log
* [x] Ability to resize (grow) the hash table in powers of two (user-initiated)
* [x] Separately configurable optional read cache for read-heavy workloads
* [x] Support for separate user-defined object hash/equality comparers and object serializers
* [x] Remove C# dynamic code-gen for quicker instantiation, stability, debuggability
* [x] Full support for .NET core and Linux/Mac
* [x] Experimental feature: DeleteFromMemory to delete recently added keys that are still in memory
* [x] Variable sized keys and values using separate object log, see [[here](https://github.com/Microsoft/FASTER/wiki/Variable-length-values#in-c-1)]
* [x] Two allocators (specializing for Blittable and Generic key-value types), with common extensible base class
* [x] Generic IDevice abstraction with out-of-the-box implementations for local storage
* [x] Segmented log on storage (log file broken into segments of configurable size)
* [x] Allocator support for copying reads to tail, useful for update-intensive workloads
* [x] Highly configurable allocator parameters (log and read cache) with respect to memory size, page size, mutable fraction of memory, etc.
* [x] Support for runtime shifting of address markers (e.g., begin, head, read-only) for dynamic tuning of memory contents of allocators (log and read cache).
* [x] CPR-based checkpointing and recovery (snapshot and fold-over modes), see [[here](https://microsoft.github.io/FASTER/#recovery-in-faster)] for details
* [x] Optional separate checkpointing of index and log (so that index may be checkpointed less frequently)
* [x] Auto-recover to most recent checkpoint


## C++ Porting Notes

FASTER C++ is a fairly direct port of FASTER C# using C++ based coding and style
guidelines. It supports the following features as of now:

* [x] Full Read, Upsert, RMW functionality
* [x] Persistence support for larger-than-memory data
* [x] Variable sized payloads; no separate object log, see [[here](https://github.com/Microsoft/FASTER/wiki/Variable-length-values#in-c)]
* [x] Log segments on storage, with truncation from head of log
* [x] CPR-based checkpointing and recovery (both snapshot and fold-over modes), see [[here](https://microsoft.github.io/FASTER/#recovery-in-faster)]
* [x] Ability to resize the hash table
* [x] C++: Added a new `value_size()` method to `RmwContext` for RCU operations: [PR](https://github.com/microsoft/FASTER/pull/145)
