# Roadmap

_This is a living document containing the FASTER team's priorities as well as release notes
for previous releases. Items refer to FASTER C# unless indicated otherwise. For C++ info, 
scroll to [C++ Porting Notes](#c-porting-notes)._

(Scroll to [Release Notes](#release-notes))

## Past and Future Work

The following is a summary of the FASTER team's past work and backlog for the next 6 months.
Completed items are included to provide the context and progress of the work. 

### Past Work

#### System Features
* [x] Full Read, Upsert, RMW functionality
* [x] Persistence support for larger-than-memory data
* [x] Variable sized keys and values using separate object log, see [here](https://github.com/Microsoft/FASTER/wiki/Variable-length-values#in-c-1))
* [x] Segmented log on storage, with user-initiated truncation from head of log (`ShiftBeginAddress`)
* [x] CPR-based checkpointing and recovery (both snapshot and fold-over modes), see [here](https://microsoft.github.io/FASTER/#recovery-in-faster)
* [x] Ability to resize the hash table (user-initiated)
* [x] Support for separate user-defined object hash/equality compareers and serializers
* [x] Remove C# dynamic code-gen for quicker instantiation, stability, debuggability
* [x] Provide two allocators (Blittable and Generic) with common extensible base class

### Ongoing and Future Work
* [ ] Log compaction (in progress)


## Release Notes

#### FASTER v2019.3.16.1 (cumulative feature list)


## C++ Porting Notes

FASTER C++ is a fairly direct port of FASTER C# using C++ based coding and style
guidelines. It supports the following features as of now:

* [x] Full Read, Upsert, RMW functionality
* [x] Persistence support for larger-than-memory data
* [x] Variable sized keys and values (no separate  object log as C++ supports variable sized allocations on log, see [here](https://github.com/Microsoft/FASTER/wiki/Variable-length-values#in-c))
* [x] Log segments on storage, with truncation from head of log
* [x] CPR-based checkpointing and recovery (both snapshot and fold-over modes), see 
[here](https://microsoft.github.io/FASTER/#recovery-in-faster)
* [x] Ability to resize the hash table
