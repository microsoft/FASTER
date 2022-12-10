---
title: "FasterKV Record Locking"
permalink: /docs/fasterkv-record-locking/
excerpt: "FasterKV Manual Locking"
last_modified_at: 2022-11-16
toc: true
---

## Record locking in FasterKV

There are two levels of locking in FASTER:
- Ephemeral: This locks in-memory records as needed for the duration of a data operation: Upsert, RMW, Read, or Delete. Ephemeral locks operate under epoch protection, so they will not be evicted, and therefore will not enter the lock table except when an operation such as `BlockAllocate` causes epoch refresh.
- Manual: This is from either `LockableContext` or `LockableUnsafeContext` (hereafter referred to collectively as `Lockable*Context`); the user manually locks keys.

All locks are obtained via spinning on `Interlocked.CompareExchange` and `Thread.Yield()`. Ephemeral locks have limited spin count, to avoid deadlocks; if they fail to acquire the desired lock in this time, the operation retries.

Manual locks have a longer duration, and enter the `LockTable` when either the readcache or hybrid log experiences memory pressure and must evict pages, and those pages contain locked records. FASTER has a `LockEvictionObserver` that runs in this situation.

As noted above, manual locking is done by obtaining the `Lockable*Context` instance from a `ClientSession`. There are currently 4 `*Context` implementations; all are `struct` for inlining. All `*Context` are obtained as properties on the `ClientSession` named for the type (e.g. `clientSession.LockableContext`). The characteristics of each `*Context` are:
- **`BasicContext`**: This is exactly the same as `ClientSession`, internally calling directly through to `ClientSession`'s methods and reusing `ClientSession`'s `FasterSession`. It provides safe epoch management (acquiring and releasing the epoch on each call) and ephemeral locking.
- **`UnsafeContext : IUnsafeContext`**: This provides ephemeral locking, but rather than safe epoch management, this supports "unsafe" manual epoch management from the client via `BeginUnsafe()` and `EndUnsafe`; it is the client's responsibility to make these calls correctly. `UnsafeContext` API methods call the internal ContextRead etc. methods without doing the Resume and Suspend (within try/finall) of epoch protection as is done by the "Safe" API methods.
- **`LockableContext : ILockableContext`**: This provides safe epoch management, but rather than ephemeral locking, this allows long-lived locks via `BeginLockable` and `EndLockable`. It is important that all locks are acquired before any methods accessing those keys are called.
- **`LockableUnsafeContext : ILockableContext, IUnsafeContext`**: This combines the manual epoch management and manual locking, exposing both sets of calls.

See [Ephemeral Locking Conceptual Flow](#ephemeral-locking-conceptual-flow) for additional information on ephemeral locking.

Here are some Manual-locking use cases:
- Lock key1, key2, and key3, then Read key1 and key2 values, calculate the result, write them to key3, and unlock all keys. This ensures that key3 has a consistent value based on key1 and key2 values.
- Lock key1, do a bunch of operations on other keys, then unlock key1. As long as the set of keys for this operation are partitioned by the choice for key1 and all updates on those keys are done only when the lock for key1 is held, this ensures the consistency of those keys' values.
- Executing transactions with a mix of shared and exclusive operations on any number of keys as an atomic operation.

### Considerations

All manual locking of keys must lock the keys in a deterministic order, and unlock in the reverse order, to avoid deadlocks.

The introduction of Manual Locking introduces the possibility of deadlocks even with ephemeral locking. In the following examples, LUC1 is a `Lockable*Context` and S1 is a standard `ClientSession` (which does ephemeral locking):
- In-Memory
    - LUC1 exclusively locks k1
    - S1 tries to acquire an exclusive ephemeral lock on k1, and spins while holding the epoch
    - LUC1 does an RMW on k1 resulting in a CopyUpdate; this does a BlockAllocate that finds it must flush pages from the head of the log in order to make room at the tail. 
        - LUC1 therefore calls BumpCurrentEpoch(... OnPagesClosed)
        - Because S1 holds the epoch, the OnPagesClosed() call is never drained, so we have deadlock
- `LockTable`-based
    - LUC1 exclusively locks k1
    - S1 does an Upsert, which allocates and inserts a tentative record (as described below)
    - S1 does one of:
        - Transfer locks from locktable to new record, then tries to lock new record
        - Tries to lock in the locktable
    - LUC1 does an RMW that deadlocks as above

Because of this, ephemeral locking limits the number of spins for which it attempts the lock; after this, it will return failure and the caller will RETRY_LATER (which refreshes the epoch, allowing other operations such as the OnPagesClosed() mentioned above to complete).

Ephemeral locks are never held across pending I/O operations. All the data operations' low-level implementors (`InternalRead`, `InternalUpsert`, `InternalRMW`, and `InternalDelete`--collectively known as `InternalXxx`) release the lock when the call is exited; if the operations must be retried, the locks are reacquired as part of the normal operation there. This prevents potential unnecessary long-held locks while a Flush() completes, for example.

### Examples
Here are examples of the above two use cases, taken from the unit tests in `LockableUnsafeContextTests.cs`:

Lock one key for read:
```cs
    using (var luContext = session.GetLockableUnsafeContext())
    {
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        luContext.Lock(51, LockType.Shared);

        luContext.Read(24, out var value24);
        luContext.Read(51, out var value51);
        luContext.Upsert(75, value24 + value51);

        luContext.Unlock(51, LockType.Shared);

        luContext.EndLockable();
        luContext.EndUnsafe();
```

Lock multiple keys, one for update and two for read:
```cs
    using (var luContext = session.GetLockableUnsafeContext())
    {
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        luContext.Lock(24, LockType.Shared);
        luContext.Lock(51, LockType.Shared);
        luContext.Lock(75, LockType.Exclusive);

        luContext.Read(24, out var value24);
        luContext.Read(51, out var value51);
        luContext.Upsert(75, value24 + value51);

        luContext.Unlock(24, LockType.Shared);
        luContext.Unlock(51, LockType.Shared);
        luContext.Unlock(75, LockType.Exclusive);

        luContext.EndLockable();
        luContext.EndUnsafe();
```

## Internal Design

This section covers the internal design and implementation of manual locking. Although Sealing or Invalidating a record is not strictly a lock, these are still part of this document because they are closely intertwined with [Record Transfers](#record-transfers) and other concurrency considerations.

### Terminology
For locking we use the following terminology for records:
- **MainLog** records are in the main log's mutable or immutable in-memory portion
- **ReadCache** records are in the readcache (always in memory)
- **CTT** abbreviation for CopyToTail
- **InMemory** records are either MainLog or ReadCache (see RecordSource).
- **Auxiliary** records are in the ReadCache or are entries in the LockTable for main log records below HeadAddress
- **OnDisk** records are mainlog records below HeadAddress (and may have an associated auxiliary record)
- **Splice** inserting a record into a key's hash chain in the gap between the readcache records and the mainlog records

### Relevant RecordInfo Bits

The following sections refer to the following bits in the `RecordInfo`:
- **Lock Bits**: There is one Exclusive Lock bit and 6 Shared Lock bits in the RecordInfo (allowing 64 shared locks; the thread spins if this many are already present, and eventually retries the operation if it fails to acquire the lock within a certain spin count). Because locking does not affect data, even `RecordInfo`s in the ReadOnly region may be locked and unlocked directly.
- **Tentative**: a record marked Tentative is very short-term; it indicates that the thread is performing a Tentative insertion of the record, and may make the Tentative record final by removing the Tentative bit, or may back off the insertion by setting the record to Invalid and returning RETRY_LATER.
- **Sealed**: A record marked Sealed is one for which an update is known to be in progress (or for which it has been completed). Sealing is necessary because otherwise a thread could acquire a lock after another thread has inserted an updated record for this key to the Tail of the log. A thread encountering a Sealed record should immediately return RETRY_LATER (it must be RETRY_LATER instead of RETRY_NOW, because the thread owning the Seal must do another operation, such as an Insert to tail, to bring things into a consistent state; this operation may require epoch refresh). 
    - Sealing is done via `RecordInfo.Seal`. This is only done when the `RecordInfo` is already exclusively locked, so `Seal()` does a simple bitwise operation. However, `Lock` checks the `Sealed` bit and fails if it is found; a Sealed record should never be operated on.
    - The only time we `Unseal()` a record is when we are unable to complete the operation; the thread `Unseal()`s the record and retries (again, this is done while the record is XLocked).
- **Invalid**: This is a well-known bit from v1; its behavior is that the record is to be skipped, using its `.PreviousAddress` to move along the chain. This has relevance to two areas of [Record Transfers](#record-transfers)
    - In the `ReadCache` we do not Seal records; the semantics of Seal are that the operation is restarted when a Sealed record is found. For non-`readcache` records, this causes the execution to restart at the tail of the main-log records. However, `readcache` records are at the beginning of the hash chain, *before* the main-log records; thus, restarting would start again at the hash bucket, traverse the readcache records, and hit the Sealed record again, ad infinitum. Thus, we instead set `readcache` records to Invalid when they are no longer correct; this allows the traversal to continue until the readcache chain links to the first main-log record.
    - With two-phase insert, we may occasionally find that an auxiliary record (`readcache` or `LockTable`) has been "made permanent" in a race that a newly-inserted main-log record loses. In that case, the main-log record is already in the hash chain, and must be marked Invalid. This behavior is new with this version of locking; previously, Invalid records were never found in the main-log portion of the hash chain (they existed only as a result of a failed CAS (which may still happen), so they were never in the hash chain).

### Ephemeral Locking Calls

This section provides the basic calls that implement ephemeral locking. See [Ephemeral Locking Conceptual Flow](#ephemeral-locking-conceptual-flow) to see how they're used.

`IFunctions` has been modified:
- The `DisableEphemeralLocking` flag has been moved from `IFunctions` to a `FasterKV` constructor argument. This value must be uniform across all asessions. It is only to control the ephemeral locking done by FasterKV, and as such has been renamed `DisableEphemeralLocking`; this replaces the concept of user-controlled locking that was provided within the `IFunctions` methods for concurrent record access.
- All locking methods on `IFunctions` have been removed; locking is now done internally only, using the `RecordInfo` bits, and controlled by `DisableEphemeralLocking`.

`IFasterSession` has added methods that wrap ephemeral locking, based upon whether the session context does manual locking:
- **IsManualLocking**: Allows directly querying the session to determine if it is a Lockable* type (but inlined to return a constant true or false, not having to compare the Context type).
- **DisableEphemeralLocking**: For `Lockable*` contexts, this returns true; all locking in those contexts is explicitly controlled by `Lock()` and `Unlock()` calls. For other contexts, this returns whether the FASTER-level `DisableEphemeralLocking` option is set.
- **TryLockEphemeralExclusive**: Attempts to lock a record exclusively, if `!DisableEphemeralLocking`. Fails after some number of spins if the record is Sealed, Tentative, or Invalid. For `Lockable*Context`, this asserts that the record is already exclusively locked.
- **TryLockEphemeralShared**: Attempts to add a shared lock on a record, if `!DisableEphemeralLocking`. Fails after some number of spins if the record is Sealed, Tentative, or Invalid. For `Lockable*Context`, this asserts that the record already has at least one sharelock.
- **UnLockEphemeralExclusive**: Unlocks a record that is locked exclusively, if `!DisableEphemeralLocking`. Does not fail; an exclusively locked record cannot be modified or moved.
- **TryUnLockShared**: Removes a shared lock on a record, if `!DisableEphemeralLocking`. Fails if the record is Sealed or Invalid (since it is locked, it cannot be tentative).

These methods are called in `InternalXxx`, which does all locking outside the `IFasterSession` calls. Previous `IFasterSession` private methods such as `ConcurrentWriterLock` and `ConcurrentWriterNoLock` are no longer needed; the locking is controlled outside the `IFasterSession` calls.

Ephemeral locks generally do not participate in record eviction, either from main log or from readcache. This is because `InternalXxx` releases those locks before going pending. However, because `InternalXxx` calls `BlockAllocate()` to create a new record, and `BlockAllocate()` can result in `BumpCurrentEpoch` being called, 

### Manual Locking Calls
Manual locking is done via `InternalLock()`, called by the `Lock()` and `Unlock()` methods of `LockableContext` and `LockableUnsafeContext`. We do not expose promoting SLocks to XLocks in `Lockable*Context` because that could lead to deadlock; the caller must lock it the right way the first time.

Other record operations that must consider locks are `InternalUpsert`, `InternalRead` and `InternalCompletePendingRead`, `InternalRMW` and `InternalCompletePendingRMW`, `InternalDelete`, and `Compact`. This is described in [Ephemeral Locking Conceptual Flow](#ephemeral-locking-conceptual-flow).

`InternalLock()` does not issue PENDING operations to retrieve on-disk data, and locking/unlocking is designed to avoid pending I/O operations by use of a [`LockTable`](#locktable-overview) consisting of {`TKey`, `RecordInfo`} pairs, where `TKey` is the FasterKV Key type and `RecordInfo` is used to perform the locking/unlocking. If a record to be locked is not found in memory above hlog.HeadAddress, then a record is created in the `LockTable`; if this record is subsequently read from the disk (via `InternalContinuePending(Read|RMW)`), the locks from the `LockTable` are applied (and the `LockTable` entry is removed).

### Epoch Assumptions
Normally, we can assume that hlog.HeadAddress and readcache.HeadAddress will not change for the duration of an `InternalXxx` call, including `InternalContinuePendingRead` and `InternalContinuePendingRMW`. However, there are two calls in particular that can result in an epoch refresh:
- `BlockAllocate` and `BlockAllocateReadCache`. If these have to allocate a new page, they may refresh the epoch.
- One of the `SpinWaitUntil*()` methods, as described in the next subsection.

#### SpinWaitUntilRecordIsClosed 
When a record has gone below the HeadAddress of its specified log (`AllocatorBase` subclass), it will soon be evicted. Eviction is usually done as a result of `BlockAllocate` or `BlockAllocateReadCache` having to allocate a new page that will exceed the in-memory size limit of the log. Eviction does the following:
- `ShiftHeadAddress()` is called to `MonotonicUpdate(ref HeadAddress, newHeadAddress, ...)`. If successful, it calls `BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress))`. 
    - `BumpCurrentEpoch` may drain the epoch list immediately, which means that the `BlockAllocate` call may run `OnPagesClosed()`.
- `OnPagesClosed()` adjusts addresses and performs eviction:
    - First, it sets `SafeHeadAddress` to `newHeadAddress`, which is the highest address that will be closed.
    - Then it incrementally closes pages in the range from `oldHeadAddress` to `newHeadAddress`, incrementing `ClosedUntilAddress` as each region is closed.
        - Closing a region means to call `ReadCacheEvict` and any main-log observer; in our case, we register a `LockEvictionObserver`.
        - Both these observers iterate the records being evicted and transfer any locks to the `LockTable`.

As `InternalXxx` (including pending) methods proceed, they check whether a record being operated on goes below its log's HeadAddress. If so, it must call one of the following functions, which spinwait until the record has been "closed" by going below `hlog.ClosedUntilAddress` or, if a readcache record, `readcache.ClosedUntilAddress:
- `SpinWaitUntilAddressIsClosed`: This takes an address and spins until the address is below `ClosedUntilAddress`.
- `SpinWaitUntilRecordIsClosed`: This takes an address and a `ref key` and spins until the address is below `ClosedUntilAddress` -or- until the key is found in the LockTable. This optimization reduces time spent waiting if there are earlier, as-yet-unclosed ranges on the page.

The caller tests that the record is below `HeadAddress` rather than `SafeHeadAddress` because `SafeHeadAddress` is modified within `OnPagesClosed`; HeadAddress is set *before* `OnPagesClosed` is called.

### Behavior of Ephemeral Lock Acquisition and Release
The actual semantics of "taking a lock", and to a lesser extent "releasing a lock", include more than simply the lock bits; as described in [Relevant RecordInfo Bits](#relevant-recordinfo-bits), the `Sealed` and `Invalid` bits are significant. It is important that these be set and tested atomically. In particular:
- Encountering a Sealed record in the hash chain requires a retry of the current operation. We currently do `RETRY_LATER`, which does an epoch refresh before retrying the operation. Records are Sealed when they have been superseded by another record for that key; this record is appended at the tail of the log (or at the readcache splice point).
- Encountering an Invalid record in the hash chain means that another thread had a Tentative insert into the log that had to be "rolled back". Since we cannot unlink records from the hash chain without significant concurrency mitigations, we simply mark the record as Invalid.

### Ephemeral Lock Acquisition
When we acquire a lock, we test the Sealed or Invalid condition during the CAS loop; if either is found, the loop terminates without attempting to acquire the lock. Once a record is marked Sealed or Invalid, it will never be modified or reused. While we could lock the record, then test for Sealed/Invalid and unlock if found, the fact that we will never want to operate on a Sealed or Invalid record allows us to centralize this to the `Lock()` operation, returning false if either condition is encountered.

### Ephemeral Lock Release
When operations have locked an in-memory record, that record may go below its log's HeadAddress due to `BlockAllocate` or `SpinWaitUntil*Closed`. In this case, we cannot unlock that recordInfo; the reference points to evicted memory. Instead, we must `SpinWaitUntilRecordIsClosed`, then unlock the `LockTable` record.

When Read() operations have locked a readcache record, they must check for the case that it has been set Invalid due to a CopyToTail or CopyUpdate from the immutable region. In this case, its locks have been transferred (actually just copied) to a new main-log record. To manage this, Unlock() tests the returned post-operation value from `Interlocked.Add`; if this has the Invalid bit set, `Unlock()` returns false, which lets the caller know there is a new record that was not affected by the `Unlock()`. In this case, the `Unlock()` must be retried via `InternalLock()`, to find the new record and release the lock.

## Ephemeral Locking Conceptual Flow
The flow of locking differs for Read and updaters (RMW, Upsert, Delete). 

For operations where the key exists in memory:
- Read simply locks the record and calls the appropriate `IFunctions` method. 
- Updaters lock the record during the lifetime of the operation (not simply during the `IFunctions` method call, as in previous versions). 
  - If the record is in the mutable region, the update can be done in-place and the lock is immediately released.
  - Otherwise a new record is created, and the locked record becomes the 'source'.
    - For RMW the 'source' data is actually used as a source, with its data used to create the new record.
    - For Upsert and Delete, the data currently in the record is ignored; an entirely new record is written.
    - The 'source' record is unlocked.

### Operation Data Structures
There are a number of variables necessary to track the main hash table entry information, the 'source' record as defined above (including locks), and other stack-based data relevant to the operation. The code has been refactored to place these within structs that live on the stack at the `InternalXxx` level.

#### HashEntryInfo
This is used for hash-chain traversal and CAS updates. It consists primarily of:
- The key's hash code and associated tag.
- a stable copy of the `HashBucketEntry` at the time the `HashEntryInfo` was populated.
    - This has both `Address` (which may or may not include the readcache bit) and `AbsoluteAddress` (`Address` stripped of the readcache bit) accessors.
- a pointer (in the unsafe "C/C++ *" sense) to the live `HashBucketEntry` that may be updated by other sessions as our current operation proceeds.
    - As with the stable copy, this has two address accessors: `CurrentAddress` (which may or may not include the readcache bit) and `AbsoluteCurrentAddress` (`CurrentAddress` stripped of the readcache bit).
- A method to update the stable copy of the `HashBucketEntry` with the current information from the 'live' pointer.

#### RecordSource
This is actually `RecordSource<TKey, TValue>` and carries the information identifying the source record and lock information for that record.
- Whether there is an in-memory source record, and if so: its logical and physical addresses, whether it is in the readcache or main log, and whether it is locked.
- The latest logical address of this key hash in the main log. If there are no readcache records, then this is the same as the `HashEntryInfo` Address; 
- If there are readcache records in the chain, then `RecordSource` contains the lowest readcache logical and physical addresses. These are used for 'splicing' a new main-log record into the gap between the readcache records and the main log recoreds; see [ReadCache](#readcache) below.
- The log (readcache or hlog) in which the source record, if any, resides. This is hlog unless there is a source readcache record.
- Whether a LockTable lock was acquired. This is exclusive with in-memory locks; only one should be set.

#### OperationStackContext
This contains all information on the stack that is necessary for the operation, making parameter lists much smaller. It contains:
- The `HashEntryInfo`  and `RecordSource<TKey, TValue>` for this operation. These are generally used together, and in some situations, such as when hlog.HeadAddress has changed due to an epoch refresh, `RecordSource<TKey, TValue>` is reinitialized from `HashEntryInfo` during the operation.
- the logical address of a new record created for the operation. This is passed back up the chain so the try/finally can set it invalid and non-tentative on exceptions, without having to pay the cost of a nested try/finally in the `CreateNewRecord*` method.

### ReadCache
The readcache is a cache for records that are read from disk. In the case of record that become 'hot' (read multiple times) this saves multiple IOs. It is of fixed size determined at FasterKV startup, and has no on-disk component; records in the ReadCache evicted from the head without writing to disk when new records are added to the tail.

Records in the readcache participate in locking, either because they are the record to be read, or because they are the 'source' of an update. If the key is found in the readcache, then the `RecordSource<TKey, TValue>.Log` is set to the readcache. If the operation is an update, then the readcache record is Invalidated before being unlocked.

### Read Locking Conceptual Flow
When locking for `Read()` operations, we never take an exclusive lock. This requires some checking when unlocking, as noted in [Ephemeral Lock Release](#ephemeral-lock-release), to ensure we have released the shared lock on the key.

#### Read Locking for InMemory Records
Read-locks records as soon as they are found, calls the caller's `IFunctions` callback, unlocks, and returns. The sequence is:

If the record is in the readcache, readlock it, call `IFasterSession.SingleReader`, unlock it, and return.

If the record is in the mutable region, readlock it, call `IFasterSession.ConcurrentReader`, unlock it, and return.

If the record is in the immutable region, readlock it, call `IFasterSession.SingleReader`, unlock it, and return. The caller may have directed that these records be copied to tail; if so, we have the same issue of lock transfer to the new main-log record as in [Read Locking for OnDisk Records](#read-locking-for-ondisk-records). The unlocking utility function make this transparent to `InternalRead()` (or in this case, `ReadFromImmutableRegion()`).

#### Read Locking for OnDisk Records 
This goes through the pending-read processing starting with `InternalContinuePendingRead`. 

We first look for an existing source record for the key; another session may have added that key to the log, readcache, or LockTable. If found, it is locked.

Call `IFasterSession.SingleReader`.

If the ReadCache is enabled or CopyToTail is true, call `InternalTryCopyToTail`; if successful, this transfers locks from the source record and clears the "source is locked" flag in `RecordSource<TKey, TValue>`.

Unlock the source record, if any (and if still locked), and return.

##### InternalTryCopyToTail
This is called when a pending read completes, during compaction, and when copying records from the immutable region to the readcache.

First, we see whether the key is found "later than" the expected logical address. This expected logical address is dependent upon the operation:
- If this is Read() doing a copy of a record in the immutable region, this is the logical address of the source record
- Otherwise, if this is Read(), it is from the completion of a pending Read(), and is the latestLogicalAddress from `InternalRead`.
- Otherwise, this is Compact(), this is a lower-bound address (addresses are searched from tail (high) to head (low); do not search for "future records" earlier than this).

If the key is found, we return an appropriate NOT FOUND code, as docuemnted in the code.

Otherwise, we allocate a block on either the readcache or hlog, depending on which we are copying to (readcache or CTT). Allocation may entail flush() which may entail epoch refresh, so when this is complete, ensure all of our in-memory addresses are still in memory (`VerifyInMemoryAddresses()`, which updates readcache), and return RETRY_NOW if an in-memory main-log location was evicted.

After this we know HeadAddress will not change. Initialize the new record from the passed-in Value and call `IFunctions.SingleWriter()`, then insert the new record into the chain:
- By CAS into the hash table entry, if doing readcache or there are no readcache records in the chain.
    - If this was a readcache insertion, we must do two-phase insert: check to make sure no main-log record was inserted for this readcache record, and if it was, mark the newly-inserted record as Invalid and return.
- Else splice by CAS into the gap between readcache and mainlog records.
    - Because this is done by CAS, we will fail if any other record was inserted there.
    - A race that inserts this key into the readcache on another session will subsequently see this session's main-log insertion and fail.

Call `CompleteTwoPhaseCopyToTail` (like `InternalTryCopyToTail`, this includes copying to ReadCache despite the name). Since we have a readlock, not an exclusive lock, we must transfer all read locks, either from an in-memory source record or from a LockTable entry.

### Update Locking Conceptual Flow
TODO

#### Update Locking for InMemory Records
TODO

#### Update Locking for OnDisk Records
TODO

## Implementation
TODO


### LockTable Overview

For records of keys not in memory (including those which have been evicted due to memory pressure while locked), as well as keys that have not yet been added to the log, we use a `LockTable` that records locks "as if" the key were present on the log and in memory.

We optimize the operations that access the `LockTable` for the cases where no locks are present, or no locks are present for the bucket to which a key hashes.

#### LockTable Data Structure
We implement the `LockTable` with two custom data structures; `ConcurrentDictionary` is too slow, and requires an `IHeapContainer<TKey>` allocation even for lookups. The structures are:
- `InMemKV` and related: An underlying replacement for `ConcurrentDictionary` that optimizes bucket-level locking.
- `LockTable`: Provides the locking layer that uses `InMemKV<TKey, THeapKey, TValue, TUserFunctions>` for underlying storage.

##### InMemKV
This is an in-memory key/value store that optimizes locking and reuse of C# allocations (to minimize GC overhead). In this discussion, the LockTable will be used to illustrate its classes and their methods, but it is a separate class from the LockTable to allow reuse in other parts of FASTER. It and its related classes are generic with the following type parameters:
- `TKey`: The Key type, which for the LockTable is the same as the FasterKV's.
- `THeapKey`: This is the type allocated for persistent storage of the Key, which for the LockTable is `IHeapContainer<TKey>`.
- `TValue`: The Value type, which for the LockTable is the RecordInfo. 
- `TUserFunctions`: The implmentation type for `IInMemoryKVUserFunctions<TKey, THeapKey, TValue>` as described below.

The in-memory KV class hierarchy is described here. For brevity, abbreviations such as `InMemKV<>` will be used to refer to the full `InMemKV<TKey, THeapKey, TValue, TUserFunctions` or other generic class within the hierarchy.
- `InMemKV`, without generic args: This is a static class that contains constants.
- `IInMemKVUserFunctions<TKey, THeapKey, TValue>`: This is an interface that must be implemented by the caller (such as LockTable). It provides User functions that replace IFasterEqualityComparer to allow comparison of `TKey` and `THeapKey`, as well as other methods such as creating a `THeapKey` and determining if a value `IsActive`.
- `InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>`: This is the actual entry for a key that contains:
    - `THeapKey`: The persistent storage for the key. For the LockTable, it is an `IHeapContainer<TKey>`.
    - `TValue`: The value for the key. For the LockTable, it is a RecordInfo.
- `InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions>`: This contains a chunk of allocations; it is the unit by which the bucket grows, if needed. It is a class containing:
    - The vector of `InMemKVEntry<>` structs
    - Next and Previous `InMemKVChunk<>` references.
- `InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions>`: This is the hash bucket, a vertebra of the hashtable spine. It is a struct containing:
    - Numeric constants for locking.
    - `long word`: analogous to the `word` of a `RecordInfo`; it is used for locking, including maintaining the `ExclusiveGeneration`: a counter that rolls over at 33 million. Buckets follow the protocol that any membership-modifying operations must exclusively lock the bucket; `ExclusiveGeneration` allows quick checking of whether the current thread acquired the exclusive lock without an intervening thread having potentially modified the bucket membership since the current thread's read-only pass.
    - `InMemKVEntry<> initialEntry`: The initial entry of the hash chain mapping to this bucket. We optimize for the case where there are no collisions on a bucket; if multiple colliding keys are present at the same time, we create overflow allocations. 
    - `InMemKVChunk<> LastOverflowChunk`: The last overflow chunk, if any are allocated. We store the last one instead of the first to facilitate compaction, which accesses the end of the chunk list.
- `InMemKV<TKey, THeapKey, TValue, TUserFunctions>`: The high-level In-Memory Key/Value store, containing:
    - `InMemKVBucket<>[] buckets`: The spine of the hashtable.
    - `TUserFunctions userFunctions`: The table-level user functions.

Below `InMemKV<>` in the hierarchy, all `InMemKV*` classes and struct are entirely internal, and are not used directly by production code (only by tests).

Unfortunately, the `IHeapContainer<TKey>` approach is necessary, despite the need to make interface calls instead of inlining, because we cannot specialize a generic type parameter for all the types that implement `IHeapContainer<TKey>` for the different allocator types, for a single member. Instead, we minimize the number of allocations made, and compare the `TKey` directly to the `IHeapContainer<TKey>.Get()`.

The `InMemKV<>` is designed to expose functions that take references to user-implemented interfaces implemented via structures (for inlining). This allows it to provide generic functionality for Adding, Finding, and Removing entries. Because it is optimized to minimize locking and implement on-the-fly compaction, it relies on the calling class to provide the `IsActive` method; during compaction, an entry that is not active is removed.

Following are public methods of `InMemKV<>` (the class is internal, but the "internal API" methods are marked "public"):
- **`bool IsActive**`: `InMemKV<>` maintains a count of active buckets; a bucket's active state is determined by whether the InitialEntry is active or not (Compaction ensures this accurately reflects the bucket's state, and is described below). The caller checks `IsActive` to avoid unnecessary lock operations.
 
- **`bool FindEntry<TFuncs>`**: Searches for an entry in the table and calls the appropriate method on the `where TFuncs : IFindEntryFunctions`:
    - NotFound()
    - FoundEntry()

    Returns true if the entry was found, else false. The `TFuncs` implementation retains further information to allow the caller to, for example, return the result of `RecordInfo.TryLock()`.

- **`void AddEntry<TFuncs>`**: Searches for an entry in the table and calls the single method on the `where TFuncs : IAddEntryFunctions`:
    - AddedEntry()

    For speed, `AddEntry<>` does not search for an existing entry; it is the caller's responsibility to call this only in situations where uniqueness is known. Other situations should call `FindOrAddEntry<>`.

    Returns void; always adds a new record.

- **`bool FindOrAddEntry<TFuncs>`**: Searches for an entry in the table and calls the appropriate method on the `where TFuncs : IFindEntryFunctions`:
    - AddedEntry()
    - FoundEntry()
 
    Returns true if the entry was found, else false. The `TFuncs` implementation retains further information to allow the caller to, for example, return the result of `RecordInfo.TryLock()`.

- Note that there is no `Remove<TFuncs>` method. As described, `InMemKV<>` relies on the `IsActive` method to determine whether an entry is active and available for removal by compaction. Therefore, to remove an entry, supply a `TFuncs` implementation to `FindEntry` that simply sets the entry's valid to something that `IsActive` will return false for. This `TFuncs` implementation may or may not verify that the entry was found.

##### LockTable
The LockTable uses `InMemKV<>` for its backing store. It exposes the following methods (as with `InMemKV<>`, the class is internal but "public" methods are marked 'public'):
- **`bool IsActive`**: A wrapper around `InMemKV<>.IsActive`.
- **`bool TryLockEphemeral`**: Try to lock the key for an ephemeral operation; if there is no lock, it will return without locking and let two-phase insert handle it as described below. Calls `InMemKV<>.FindEntry` with the following implementation, and returns the value of `success`:
    - `NotFound()`: success = true
    - `FoundEntry()`: success = RecordInfo.TryLock()
- **`bool TryLockManual`**: Try to acquire a manual lock (which may be tentative), either by locking an existing LockTable record or by adding a new record. Calls `InMemKV<>.FindOrAddEntry` with the following implementation, and returns the value of `success`:
    - `FoundEntry()`: success = RecordInfo.TryLock() and clear tentative state
    - `AddedEntry()`: initialize lock, store tentative state, success = true
- **`bool Unlock`**: Unlock a key. Calls `InMemKV<>.FindEntry` with the following implementation, and returns the value of `success`:
    - `NotFound()`: success = false. The caller may raise an error in this case, if the key was expected to be found.
    - `FoundEntry()`: success = RecordInfo.Unlock(). This may fail if the RecordInfo was marked invalid, e.g. prior to a transfer.
- **`bool Remove`**: Remove the key from the lockTable, if it exists. Called after a record is transferred. Calls `InMemKV<>.FindEntry` with the following implementation, and returns the value of `wasFound`:
    - `NotFound()`: wasFound = false. This is ok; we may have spinwaited for an evicted record that had no locks.
    - `FoundEntry()`: wasFound = true, RecordInfo.ClearLocks().
- **`void TransferFromLogRecord`**: Transfers locks from a mainlog or readcache record that is being evicted due to memory pressure. Calls `InMemKV<>.AddEntry` with the following implementation:
    - `AddedEntry()`: RecordInfo.TransferLocksFrom(log record)
- **`void TransferToLogRecord`**: Transfers locks from the LockTable to a new log record. Calls `InMemKV<>.FindEntry` with the following implementation:
    - `FoundEntry()`: logRecord.TransferLocksFrom(lock table record)
- **`bool ClearTentative`**: Clears the Tentative bit from the key's lock--makes it "real". Calls `InMemKV<>.FindEntry` with the following implementation, and returns the value of `success`:
    - `NotFound()`: success = false
    - `FoundEntry()`: clear tentative flag; success = true
- **`void UnlockOrRemoveTentative`**: Unlock the key, or remove a tentative entry that was added. This is called when the caller is abandoning the current attempt and will retry. Calls `InMemKV<>.FindEntry` with the following implementation, and asserts the value of `success` (the record should always be found):
    - `NotFound()`: success = false. The caller may raise an error in this case, if the key was expected to be found.
    - `FoundEntry()`: success = RecordInfo.Unlock(). This may fail if the RecordInfo was marked invalid, e.g. prior to a transfer.
- **`bool CompleteTwoPhaseInsert`**: Complete the two-phase insertion protocol: wait for any tentative lock on the key (it will be cleared by the owner) or return false if a non-tentative lock was inserted. Calls `InMemKV<>.FindEntry` with the following implementation, and returns the value of `notFound`:
    - `NotFound()`: notFound = true
    - `FoundEntry()`: notFound = false, foundTentative = recordInfo.IsTentative()
- **`bool ContainsKey`**: Test whether a key is present in the Lock Table. In production code, this is used to implement `FasterKV<>.SpinWaitUntilRecordIsClosed`. It is a wrapper around `InMemKV<>.ContainsKey`.

##### Internal InMemKV Implementation
The `LockTable` is optimized to add minimal overhead when not in use; when in use, it is optimized for the no-collisions case per-bucket.

This means that we expect the `bucket.InitialEntry` to be most often operated on:
- When locking, testing whether it contains the desired key, or is available to be populated
- When unlocking, testing whether it contains the desired key

When collisions occur, as may happen more often in memory-constrained cases with multiple transactions and numerous keys, the bucket obtains `InMemKVChunk<>`s from the C# new operator, currently in fixed chunks of size 16. Because `LockTableEntry<TKey>` contains an interface `IHeapContainer<TKey>`, the memory cannot be pinned, so we have no motivate to use another allocator.

The `bucket.LastOverflowChunk` holds the last chunk. If it is null, then there are no overflow chunks. Chunks are maintained in a linked list. We store the last chunk instead of the first because we compact on-the-fly from the end of the list over the `!IsActive` entries.

Compaction is triggered by `InMemKV<>.FindEntry` detecting that the entry is `!Active` following the `IFindEntryFunctions.FoundEntry` call. If this condition is found, then the bucket promotes to an exclusive lock. This process saves off `bucket.ExclusiveGeneration` before the exclusive lock and examines it on lock acquisition (prior to incrementing it). If this is unchanged, we know no other session has modified the bucket contents, and can do an optimized swap of the last active entry over the just-inactivated entry (this is why we hold onto the last chunk, not the first, in the bucket field). Otherwise, we must verify that the no-longer-active object is still in the list: the chunk must be in our assigned list (verifying this requires iterating the set of chunks), and the entry at the index must be inactive (but not default). Some notes about this:
    - We only insert new entries at the end of the list, never in the middle. This means our target entry will never be at a position "later" than it was when we made this call.
    - Every CompactInto() removes InActive entries at the end of the list (an InActive entry will never be swapped into a target entry). Since another thread got the XLock before us, our target entry was removed if it was at the end of the list.

When all entries have been compacted off the last chunk, it is "freed" and the the bucket field is updated. "Freeing" a chunk means it is passed off to the `InMemKV<>`'s freelist, which is limited in size, so the chunk may be dropped and garbage-collected.

The `bucket.ExclusiveGeneration` is stored in 25 bits of the `bucket.word` field, which means it will roll over after 33 million allocations, which will not happen in one iteration of the loop (this could be reduced if the bits are needed elsewhere).

The `InMemeKV<TKey>` maintains an invariant that once traversal hits an entry that is `IsDefault` (no heap key is set), traversal stops; on-the-fly compaction ensures this is consistent. This works in conjunction with locking rules. The locking rules are:
- ShareLock the bucket for operations that do not change list membership (that is, the set of keys that are present): for the LockTable, this means locking and unlocking keys that are already there. We start operations by assuming this is possible: ShareLock the bucket and search. Therefore we initially take a sharelock on the bucket if we see that the initialEntry is set (if it is not and the lock request is ephemeral, we simply return, and let two-phase insertion handle the situation as described below).
- If we are changing the list membership, then we promote the ShareLock to an ExclusiveLock. This is where the `ExclusiveGeneration` value is used. If after acquiring the promoted XLock we see that the current ExclusiveGeneration is the same as before the XLock attempt, then we know the list membership has not changed; we return this knowledge to the `PromoteSharedLockToX` caller as `out bool immediateLock`.

With these locking rules, we can implement Compaction quite quickly:
- Compaction obtains an XLock
- Most of the time we will obtain the XLock immediately, so we know nobody else has changed the list; we can simply swap in the entry from `LastActiveChunkEntryIndex` (stored in 32 bits of `bucket.word`) and then decrement `LastActiveChunkEntryIndex`. If this releases the last entry on the chunk, then we free the chunk as described above.
- Otherwise, we Compact from the beginning; the logic is the same as the single-element case but examining each entry from the beginning, until it finds an `IsDefault` (or reaches the end of the final chunk).

**END OF REVISIONS 12/09/2022**




#### LockTable Conceptual Flow
For records not found in memory, the `LockTable` is used. The semantics of `LockTable` entries are as follow. This is a conceptual view; implementation details are described in subsequent sections.
- On a `Lock` call, if the key is not found in memory, the `LockTable` is searched for the Key.
  - if the RecordInfo is in the `LockTable` it is locked as specified
  - else a new Tentative record is added and subsequently finalized as in [Insertion to LockTable due to Lock](#insertion-to-locktable-due-to-lock)
- On an `Unlock` call, if the key is not found in memory, the `LockTable` is searched for the Key.
  - If it is not found, a Debug.Fail() is issued.
  - Otherwise, the requested `LockType` is unlocked. If this leaves the `RecordInfo` unlocked, its entry is rmoved.
- When a Read or RMW obtains a record from ON-DISK, it consults the `LockTable`; if the key is found:
  - if the RecordInfo is in the `LockTable` it is Sealed, else a new Tentative record is added
  - it transfers the locks from the `LockTable` entry to the retrieved recordInfo
  - it removes the Seal from the entry in the `LockTable`, or deletes the entry if it was Tentative
- When an Upsert or Delete does not find a key in memory, it consults the `LockTable`, and if the key is found:
  - it Seals the RecordInfo in the `LockTable`
  - it transfers the locks from the `LockTable` entry to the retrieved recordInfo
  - it performs the usual "append at tail of Log" operation
  - it removes the Seal from the entry in the `LockTable`, or deletes the entry if it was Tentative
- Because `LockTable` use does not verify that the key actually exists (as it does not issue a pending operation to ensure the requested key, and not a collision, is found in the on-disk portion), it is possible that keys will exist in the `LockTable` that do not in fact exist in the log. This is fine; if we do more than `Lock` them, then they will be added to the log at that time, and the locks applied to them.

#### Insertion to LockTable due to Lock

This is the complementary side of [Insertion to LockTable due to Update](#insertion-to-locktable-due-to-update):

When a thread doing `Lock()` looks for a key in the LockTable and cannot find it, it must do a Tentative insertion into the locktable, because it is possible that another thread CAS'd that key to the Tail of the log after the current thread had passed the hash table lookup:
- If Lock() finds the key in memory:
  - if the record is Tentative, Lock() spins until the record is no longer Tentative
    - if the record is Invalid, Lock() does a RETRY_NOW loop.
  - Lock() locks that record and exits successfully.
- Otherwise:
  - We record the current TailAddress as prevTailAddress.
  - If `LockTable` has an entry for that key
    - if it is Tentative, we spin until it is no longer Tentative
      - if the record is Invalid, Lock() does a RETRY_NOW loop.
    - Lock() locks that `LockTable` entry and exits successfully.
  - Otherwise, Lock() creates a Tentative entry in the LockTable for the key
  - Lock() checks to see if the key exists on the log between current TailAddress and prevTailAddress
    - if yes:
      - if the record is Tentative, Lock() spins until the record is no longer Tentative
      - Lock() backs off the LockTable entry creation by setting it Invalid (so anyone holding it to spin-test sees it is invalid), removing it from the LockTable per [Removal from LockTable](#removal-from-locktable), and doing a RETRY_NOW loop.
    - If prevTailAddress has escaped to disk by the time we start following the hash chain from Tail to prevTailAddress, Lock() must do a RETRY_NOW loop. See the InternalTryCopyToTail scan to expectedLogicalAddress and ON_DISK as an example of this.
      - Any waiting thread sees Invalid and in this case, it must also return RETRY_NOW.
    - if no, we can set locktable entry as final by removing the Tentative bit
      - Any waiting thread proceeds normally

#### Insertion to LockTable due to Update

This is the complementary side of [Insertion to LockTable due to Lock](#insertion-to-locktable-due-to-lock) and applies to Upsert, RMW, and Delete, when any of these append a record to the tail of the log (for brevity, Update is used). It is necessary so that threads that try to Lock() the Upsert()ed record as soon as it is CAS'd into the Log will not "split" locks between the log record and a `LockTable` entry. There is a bit of Catch-22 here; we cannot CAS in the non-Tentative log record before we have transferred the locks from a LockTable entry; but we must have a record on the log so that Lock() will not try to add a new entry, or lock an existing entry, while Upsert is in the process of creating the record and possibly transferring the locks from the `LockTable`.

For performance reasons, Upsert cannot do an operation on the `LockTable` for each added record; therefore, we defer the cost until the last possible point, where we know we have to do something with the `LockTable` (which is very rare).

When Upsert must append a new record:
- Upsert CASes in a record marked Tentative
  - Note that Upsert does NOT check the locktable before its CAS, for performance reasons.
  - Any thread seeing a Tentative record will spinwait until it's no longer Tentative, so no thread will try to lock this newly-CAS'd record.
- Upsert checks the `LockTable` to see if there is an entry in it for this key.
  - If an entry is in the `LockTable`, then Upsert checks to see if it is marked Tentative.
    - If so, then we spinwait until it is no longer tentative; per [Insertion to LockTable due to Lock](#insertion-to-locktable-due-to-lock), it will be removed by the Lock() thread, or made final, depending on whether the Lock() thread saw the newly-Upserted record.
    - Otherwise, Upsert:
      - Applies the locks to its newly-CAS'd record (which is still Tentative)
      - Sets the LockTable entry Invalid and removes it
      - Clears the newly-CAS'd record's Tentative, and exits successfully

#### Removal from LockTable

Here are the sequences of operations to remove records from the Lock Table:
- Unlock
  - If the lock count goes to 0, remove from `LockTable` conditionally on IsLocked == false and Sealed == false.
    - Since only lock bits are relevant in LockTable, this is equivalent to saying RecordInfo.word == 0, which is a faster test.
- Pending Read to `ReadCache` or `CopyToTail`, Pending RMW to Tail, or Upsert or Delete of a key in the LockTable
  - The `LockTable` record is Sealed as described in [Relevant RecordInfo bits](#relevant-recordInfo-bits)
    - If this fails, the operation retries
    - Other operation threads retry upon seeing the record is sealed
  - The Insplice to the main log is done
    - If this fails, the Sealed bit is removed from the `LockTable` entry and the thread does RETRY_NOW
    - Else the record is removed from the `LockTable`
      - Note: there is no concern about other threads that did not find the record on lookup and "lag behind" the thread doing the LockTable-entry removal and arrive at the LockTable after that record has been removed, because:
        - If the lagging thread is from a pending Read operation, then that pending operation will retry due to the InternalTryCopyToTail expectedLogicalAddress check or the readcache "dual 2pc" check in [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
        - If the lagging thread is from a pending RMW operation, then that pending operation will retry due to the InternalContinuePendingRMW previousFirstRecordAddress check or the readcache "dual 2pc" check in [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
        - Upsert and Delete would find the LT entry directly

### ReadCache Overview

When the `ReadCache` is enabled, "records" from the `ReadCache` (actually simply their `RecordInfo` headers) are inserted into the chain starting at the `HashTable` (these records are identified as `ReadCache` by a combination of `FasterKV.UseReadCache` being set *and* the ReadCache bit in the `RecordInfo` is set). All `ReadCache` records come before any main log record. So (using r#### to indicate a `ReadCache` record and m#### to indicate a main log record):
- When there are no `ReadCache` entries in a hash chain, it looks like: `HashTable` -> m4000 -> m3000 -> m...
- When there are `ReadCache` entries in a hash chain, it looks like: `HashTable` -> r8000 -> r7000 -> m4000 -> m3000 -> m...

As a terminology note, the sub-chain of r#### records is referred to as the `ReadCache` prefix of that hash chain.

In FASTER v1, updates involving `ReadCache` records strip the entire `ReadCache` prefix from the chain. Additionally, the `ReadCache` prefix is stripped from the hash chain when a `ReadCache` page with that hashcode is evicted due to memory limits. In FASTER v2, because `ReadCache` records may be locked, we must not lose those locks. This is resolved in two ways:
- On record updates, `ReadCache` prefixes are preserved except for the specific record being updated, which is spliced out and transferred to a `CopyToTail` on the main log, including any locks.
- When `ReadCache` pages are evicted, their records are removed from the `ReadCache` prefix, and any with locks are transferred to the `LockTable`.

### Record Transfers

In normal FASTER operation, records are appended at the tail of the log and do not move. The `HashTable` points to these records for each distinct hash code.

Record transfers occur when a ReadCache entry must be updated, or a record is evicted from either ReadCache or the main log while it holds locks.

#### `ReadCache` Records at Tail of Log

For brevity, `ReadCache` is abbreviated RC, `CopyToTail` is abbreviated CTT, and `LockTable` is abbreviated LT. Main refers to the main log. The "final RC record" is the one at the RC->Main log boundary. As always, avoiding locking cost is a primary concern. 

For record transfers involving the ReadCache, we have the following high-level considerations:
- There is no record-transfer concern if the first record in the hash chain is not a `ReadCache` entry.
  - Otherwise, we insplice between the final RC entry and the first main-log entry; we never splice into the middle of the RC prefix chain.
- Even when there are RC entries in the hash chain, we must avoid latching because that would slow down all record-insertion operations (upsert, RMW of a new record, Delete of an on-disk record, etc.) as well as some Read situations.
- "Insplicing" occurs when a new record is inserted into the main log after the end of the ReadCache prefix string.
- "Outsplicing" occurs when a record is spliced out of the RC portion of the hash chain (main log records are never spliced out) because the value for that key must be updated, or because we are evicting records from the ReadCache. We cannot simply mark ReadCache entries as Invalid and leave them there, or the chain will grow without bound. For concurrency reasons, outsplicing is "delayed"; we mark the readcache record as Invalid during normal operations, and defer actual record removal to readcache eviction time, as described below.
  - Insplicing: For splicing into the chain, we always CAS at the final RC entry rather than at the HashTable bucket slot (we never splice into the middle of the RC prefix chain).
    - Add the new record to the tail of main by pointing to the existing tail of in its `.PreviousAddress`.
    - CAS the existing final RC record to point to the new record (set its .PreviousAddress and CAS).
    - If CAS failed, someone else inserted -or- the `ReadCacheEvict` thread outspliced the final RC record (in this case, the (formerly) final RC record will have its Invalid bit set), so RETRY_NOW 
  - For outsplicing (which only applies to RC records), we use a modified "mark and sweep" approach:
    - First mark the RC record being outspliced as Invalid via CAS loop; this ensures that the final RC record has a consistent .PreviousAddress (in the event another thread is insplicing while the final RC record is being marked Invalid). No latching is required in this mark phase.
    - The Invalid entries are finally removed during ReadCacheEvict:
      - CAS the RC record to be removed to be Sealed. This will cause any other operations to retry.
      - CAS the preceding RC record to point to the to-be-removed RC record's .PreviousAddress (standard singly-linked-list operations)
      - CAS the now-removed RC record to be Invalid.
      - We only actually transfer records from the RC prefix to the LockTable if there is an active `LockableUnsafeContext` session at the time `ReadCacheEvict` is called; otherwise there will be no locks. However, we must already traverse the `ReadCache` records, and it is possible for a new `LockableUnsafeContext` session to start during the duration of `ReadCacheEvict`, so there is no benefit to checking for the no-`LockableUnsafeContext` case (unlike [Main Log Evictions](#main-log-evictions), which can avoid page scans by checking for this).

The above covers single-record operations on the RC prefix. Two-record operations occur when we must outsplice one record and insplice another, because the value for a record in the RC prefix is updated, e.g. Upsert updating a record in the ReadOnly region or RMW doing a CopyUpdater (of mutable or readonly), or either of these operating updating a key that is in the RC prefix chain. The considerations here are:
- Updating an RC record:
  - Mark the RC record as Sealed
  - Do the insplice as described above
  - If the insplice succeeds, mark the RC entry as Invalid, else remove the Sealed marking and RETRY_NOW
- Updating an in-memory main log record:
  - Mark the main log record as Sealed
  - Do the insplice as described above
  - If the insplice succeeds, leave the main log entry as Sealed, else remove the Sealed marking and RETRY_NOW

#### Main Log Evictions

When main log pages are evicted due to memory limits, *if* there are any active `LockableUnsafeContext` sessions, then each record on those pages must be examined and any locks transferred to `LockTable` entries.

Transfers to the `LockTable` due to main log evictions are handled in the following manner:
- A new `TentativeHeadAddress` (THA) field is added next to `HeadAddress`.
- Shifting HeadAddress is now done in three steps: Update THA, handle evictions, then update HeadAddress
  - In (PageAligned)ShiftHeadAddress, we now:
    - epoch.BumpCurrentEpoch(() => OnPagesReadyToClose(oldTentativeHeadAddress, newHeadAddress));
      - OnPagesReadyToTransfer() is a new routine:
        - ReadCacheEvict (via EvictCallback)
        - epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress));
          - This actually evicts the pages

### Recovery Considerations

We must clear in-memory records' lock bits during recovery. 
- Add `RecoveryInfo.manualLockingActive`, an indication of whether any `LockableUnsafeContext` were active during the Checkpoint.
- If this indicator is true:
  - Scan pages, clearing the locks of any records
    - These pages do not need to be flushed to disk
  - Ensure random reads and scans will NOT be flummoxed by the weird lock bits

### FASTER Operations

Following are the 4 FASTER data operations and Lock/Unlock, and their flow for the various lock states.

Abbreviations:
- LockOp: The `LockOperations` instance passed to one of the InternalXxx methods.
- CTT: CopyToTail
- ITCTT: InternalTryCopyToTail
- Unfound refers to entries that are not found in memory (the hash chain passes below HeadAddress) or are not found in the Hash chain

#### Conflict Between Updates and Reading From Disk to ReadCache

One big consideration for Upsert is that it blindly upserts when a scan for a record drops below HeadAddress. This in conjunction with our two insertion points--at HT->RC and at RC->MainLog--gives rise to the following lost-update anomaly:
- We Upsert k1 to the main log, splicing it into the RC->MainLog point
- At the same time, we did a read of k1 which brought the previous k1 value from disk into the read cache, inserting it at the HT->RC point
- Thus our upsert "failed", as the chain contains the old k1 in RC (even though the chain leads eventually to the new k1 at tail of main, operations will find the one in the readcache first).

General algorithm, iff readcache entries are present: each participating thread adds a Tentative entry, then scans; if it does not find an existing record, then finalize. This is modified by ensuring that the update wins (its data is more recent). We *must* have such a two-phase operation at both ends, to ensure that whichever side completes the scan first, it will find an entry, either final or Tentative, for the other operation.
- Upsert (blind only) or RMW when reading from disk:
  - Save HT->RC record address as prevFirstRCAddress
  - Do the usual check-for-mutable:
    - Call SkipAndInvalidateReadCache
    - If the record is found in mutable and updated, return SUCCESS
  - It was not mutable, so we must insert at end of log
    - Insert at RC->MainLog boundary. This is *not* tentative, because we want Upsert to win any ties
    - SkipAndInvalidateReadCache until prevFirstRCAddress
      - We want the Upsert to win, so this pass ensures that any newly-added readcache entry for this key, whether tentative or not, is marked Invalid
    - Remove the tentative 
- Read that goes pending:
  - Saves TailAddress to pcontext.RecordInfo.PreviousAddress
  - Complete Pending Read:
    - Prior to its SkipReadCache/TracebackForKeyMatch, it sets a tentative record at the HT->RC boundary.
    - it does the scan
      - if the Tentative record is now Invalid, it means Upsert/RMW set it so for a later update; return NOTFOUND
      - else searches from current RC->MainLog record to pcontext.RecordInfo.PreviousAddress record for a match
        - if it finds this non-RC record for this key, it sets the Tentative RC record to Invalid and returns NOTFOUND
      - else it removes the Tentative flag

OPTIMIZATION: Use readcache records rather than going to disk. However, there are issues here with the record being marked Invalid/Sealed in case multiple threads do it.

#### Lock
- If the record is in readcache: 
  - Do the Lock op:
    - Tentative: spinwait; must fall through to check Sealed
    - Sealed: Yield and Retry
    - ignore/continue if the record is or becomes Invalid
- else for both mutable (and RO if MFO is active) records, if the RecordInfo is:
  - Sealed: Yield() and retry
  - Tombstone: Do the Lock op (e.g. unlock)
  - Other: 
    - Do the LockOp in ConcurrentWriter for both Mutable and RO 
- else // key is not found or hash chain goes below HeadAddress
  - Perform `LockTable` insertion as described in [Insertion to LockTable due to Lock](#insertion-to-locktable-due-to-lock)

#### Read

Note that this changes specified here, including both shared and exclusive locks in the ReadOnly region, clarifies the distinction between a data-centric view of the ReadOnly region being implicitly read-locked (because it cannot be updated), vs. a transactional view that requires explicit read locks. In a transactional view, a read lock prevents an exclusive lock; implicit readlocks based on address cannot do this in FASTER, because we can always do XLock or an RCU. Therefore, we need explicit read locks, and reads must of course block if there is an XLock. This also means that SingleReader would have to lock anyway, losing any distinction between it and ConcurrentReader. Therefore, we have consolidated ConcurrentReader and SingleReader into a single function.

- if the record is in readcache:
  - Tentative: spinwait; must fall through to check Sealed
  - Sealed: Yield and Retry
  - SingleReader: will ephemerally lock if needed
- for both mutable and RO records, if the RecordInfo is:
  - Sealed: Yield() and retry
  - Tombstone: as current
  - Other: as currently, including ephemeral locking
    - Change IFunctions.SingleReader and .ConcurrentReader to simply .Reader
- On-disk: 
  - After PENDING
    - if copying to readcache, do so in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
    - else if CopyToTail do [Removal From LockTable](#removal-from-locktable)

#### Upsert

- If the record is in readcache:
  - if it is
    - Tentative: spinwait; must fall through to check Sealed
    - Sealed: Yield and Retry
  - Invalidate it
  - insert the new value in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
- else if the record is in the mutable region and the RecordInfo is:
  - Tentative: spinwait; must fall through to check Sealed
  - Sealed: Yield and Retry
  - Tombstone: as current
  - Other: IPU (including ephemeral locks)
    - If this returns false
      - Set RecordInfo Sealed as described in [Relevant RecordInfo bits](#relevant-recordinfo-bits)
      - Insert in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
- else if the record is in ReadOnly and the RecordInfo is:
  - Sealed: Yield() and retry
  - Tombstone: as current
  - Other:
    - Seal RO record
    - Do insert in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
    - Leave the RO record Sealed
- else // key is not found or hash chain goes below HeadAddress
  - if the key is in the lock table
    - Seal it (Seal XLocks so all locks are drained; we have none to transfer)
      - If Seal() fails or the record is marked Invalid, then and RETRY_NOW (someone else did an operation that removed it)
      - Else
        - Insert new record to log
        - Transfer locks from LockTable entry
        - Remove locktable entry per [Removal From LockTable](#removal-from-locktable)
        - Unlock it
  - InitialUpdater and insert in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)

#### RMW

RMW considerations are similar to Upsert from the sealing and "encountering locks" point of view.

- If the record is in readcache:
  - if it is
    - Tentative: spinwait; must fall through to check Sealed
    - Sealed: Yield and Retry
  - Invalidate it
  - CopyUpdater and insert the new value in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
- else if the record is in the mutable region and the RecordInfo is:
  - Tentative: spinwait; must fall through to check Sealed
  - Sealed: Yield and Retry
  - Tombstone: as current
  - Other: IPU (including ephemeral locks)
    - If this returns false
      - Set RecordInfo Sealed as described in [Relevant RecordInfo bits](#relevant-recordinfo-bits)
      - Do CopyUpdater and insert in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
      - Iff this fails, Unseal RO record
- else if the record is in ReadOnly and the RecordInfo is:
  - Tentative: spinwait; must fall through to check Sealed
  - Sealed: Yield and Retry
  - Tombstone: as current
  - Other: Do CopyUpdater and insert in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
- else // key is not found or hash chain goes below HeadAddress
  - if the key is in the lock table
    - Seal it (Seal XLocks so all locks are drained; we have none to transfer)
      - If Seal() fails or the record is marked Invalid, then and RETRY_NOW (someone else did an operation that removed it)
      - Else
        - Insert new record to log
        - Transfer locks from LockTable entry
        - Remove locktable entry per [Removal From LockTable](#removal-from-locktable)
        - Unlock it
  - InitialUpdater and insert in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
- TODO: potentially replace "fuzzy" region at SafeReadOnlyAddress with Sealed, which should avoid the lost-update anomaly

#### Delete

- If the record is in readcache:
  - if it is
    - Tentative: spinwait; must fall through to check Sealed
    - Sealed: Yield and Retry
  - Invalidate it
  - insert the new deleted record in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
- else if the record is in the mutable region and the RecordInfo is:
  - Tentative: spinwait; must fall through to check Sealed
  - Sealed: Yield and Retry
  - Tombstone: as current (nothing)
  - Other: Mark as tombstone 
- else if the record is in ReadOnly and the RecordInfo is:
  - Tentative: spinwait; must fall through to check Sealed
  - Sealed: Yield and Retry
  - Tombstone: as current (nothing)
  - Other:
    - Seal RO record
    - Do insert of Tombstoned record in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
    - Leave the RO record Sealed
- else // key is not found or hash chain goes below HeadAddress
    - if the key is in the lock table
      - Seal it (Seal XLocks so all locks are drained; we have none to transfer)
        - If Seal() fails or the record is marked Invalid, then and RETRY_NOW (someone else did an operation that removed it)
        - Else
          - Insert new deleted record to log
          - Transfer locks from LockTable entry
          - Remove locktable entry per [Removal From LockTable](#removal-from-locktable)
    - Insert deleted record in accordance with [Conflict Between Updates and Reading From Disk to ReadCache](#conflict-between-updates-and-reading-from-disk-to-readcache)
