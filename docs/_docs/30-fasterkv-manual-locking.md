---
title: "FasterKV Manual Locking"
permalink: /docs/fasterkv-manual-locking/
excerpt: "FasterKV Manual Locking"
last_modified_at: 2021-11-30
toc: true
---

## Manual Record locking in FasterKV

Manual locking in FasterKV refers to the user specifying when records will be locked. This is different from the per-operation locks that ensure consistency for concurrent operations, e.g. ConcurrentReader and ConcurrentWriter. Manual locks have a longer duration.

Manual locking is done by obtaining the `ManualFasterOperations` instance from a `ClientSession`. This provides an implementation of `IFasterOperations` that:
- Does not do automatic locking (except when updated records are inserted, as described below)
- Does not do automatic epoch protection; instead, the user must call `UnsafeResumeThread` and `UnsafeSuspendThread`. In these, "Unsafe" refers to the fact it is the user's responsibility to make the correct calls.
- Exposes `Lock()` and `Unlock()` APIs.

Here are two use case examples:
- Lock key1, key2, and key3, then Read key1 and key2 values, calculate the result, write them to key3, and unlock all keys. This ensures that key3 has a consistent value based on key1 and key2 values.
- Lock key1, do a bunch of operations on other keys, then unlock key1. As long as the set of keys for this operation are partitioned by the choice for key1 and all updates on those keys are done only when the lock for key1 is held, this ensures the consistency of those keys' values.

### Considerations

All keys must be locked in a deterministic order, and unlocked in the reverse order, to avoid deadlocks.

`ManualFasterOperations` inherits from `IDisposable`. All locks must be released and `UnsafeSuspendThread` must be called before `Dispose()` is called; `Dispose()` does *not* make these calls automatically.

### Examples
Here are examples of the above two use cases, taken from the unit tests in `ManualOperationsTests.cs`:

Lock multiple keys:
```cs
    using (var manualOps = session.GetManualOperations())
    {
        manualOps.UnsafeResumeThread(out var epoch);

        LockInfo lockInfo = default;
        manualOps.Lock(24, LockType.Shared, retrieveData: true, ref lockInfo);
        manualOps.Lock(51, LockType.Shared, retrieveData: true, ref lockInfo);
        manualOps.Lock(75, LockType.Exclusive, retrieveData: false, ref lockInfo);

        manualOps.Read(24, out var value24);
        manualOps.Read(51, out var value51);
        manualOps.Upsert(75, value24 + value51);

        manualOps.Unlock(24, LockType.Shared);
        manualOps.Unlock(51, LockType.Shared);
        manualOps.Unlock(75, LockType.Exclusive);

        manualOps.UnsafeSuspendThread(out var epoch);
```

Lock multiple keys:
```cs
    using (var manualOps = session.GetManualOperations())
    {
        manualOps.UnsafeResumeThread(out var epoch);

        LockInfo lockInfo = default;
        manualOps.Lock(51, LockType.Shared, retrieveData: true, ref lockInfo);

        manualOps.Read(24, out var value24);
        manualOps.Read(51, out var value51);
        manualOps.Upsert(75, value24 + value51);

        manualOps.Unlock(51, LockType.Shared);

        manualOps.UnsafeSuspendThread(out var epoch);
```

## Internal Design

This section covers the internal design and implementation of manual locking.

Manual locking is integrated into `FASTERImpl.cs` methods, notably `InternalRead` and `InternalCompletePendingRead`, `InternalUpsert`, `InternalRMW` and `InternalCompletePendingRMW`, and `InternalDelete`. These modifications are exposed via the `Lock()` and `Unlock()` APIs on `ManualFasterOperations`. This integration is necessary because many parts are dependent upon the called routine, such as Stub and Sealed record handling and how to handle operations that reference on-disk records. Most LockOperation-specific code is protected by an `if (fasterSession.IsManualOperations)` test, which is a static bool member of the `FasterSession` implementation so the comparison should optimize out.

Because epoch protection is done by user calls, ManualFasterOperations methods call the internal ContextRead etc. methods, which are called by the API methods that do Resume and Suspend of epoch protection.

The actual implementation of `Lock()` uses only `InternalRead` and `InternalUpsert`, and the implementation of `Unlock()` uses only InternalRead.

At a high level, `Lock()` checks to see if the `retrieveData` parameter is true. If so, it calls a new overload of ContextRead(). This will cause on-disk data to be retrieved via PENDING operations, and then the lock will be applied to it. If the record is not found, or if retrieveData is false, then a new overload of ContextUpser() is called. Upsert by design does not issue PENDING operations to retrieve on-disk data; if the record is not found, a Stub record is inserted at the tail of the log and is exclusively locked.

The semantics of Stub records are:
- This is a placeholder only; it has no valid data. It should not be Read().
- When an Upsert or RMW encounters a Stub record, it checks to see if the current operation is being done from an instance of `ManualFasterOperations`. 
  - If so, it assumes the current session holds the lock, and proceeds with the update by inserting a new record (which will be locked) and unlocking the Stub.
  - Otherwise, the Stub is invalid, and the operation first issues a Lock on it in order to wait. Once the lock is acquired, if the record is still a Stub, the operation returns with a RETRY_NOW status. This allows retrying until the Stub is replaced by its actual value via Upsert or RMW, or is Deleted.

For non-stub (that is, normal) records, the lock or unlock is applied directly. This includes records in the ReadOnly region, since Locks do not alter data.

Sealed records are records for which an update operation is being done; they should not be updated directly, but rather the operation should be retried immediately; Sealed records are short-term (e.g. a single call to Upsert or RMW). Note that unlike a Stub record, the operation does not attempt to Lock a Sealed record; Sealed records are used in other Lock-transfer situations and thus locks should not be taken. Rather, the calling operation issues a `Thread.Yield()` prior to retrying.

Following are the specific operations and their flow for the various lock states.

Abbreviations:
- LockOp: The `LockOperations` instance passed to one of the InternalXxx methods.
- CTT: CopyToTail
- ITCTT: InternalTryCopyToTail

Reference to Sealed record and the LockCache will be filled out as those are implemented.

## Read

- If LockOp.IsSet
  - Stub: return NOTFOUND
  - Sealed: Yield() and retry
  - Tombstone: Do Lock op (e.g. unlock)
  - Normal: 
    - Mutable: Do LockOp in ConcReader
    - RO: Do LockOp in SingleReader
  - On-disk: Issue PENDING operation
    - Do LockOp in ITCTT -> SingleWriter
    - Do LockOp in ReadCache (wins if both RC and CTT specified) -> SingleWriter
      - TODO: Updating remove from ReadCache (without eliding entire string)
      - TODO: Unlock remove from LockCache
  - Already in readcache: Lock
      - TODO: Updating remove from ReadCache (without eliding entire string)
- LockOp is not set:
  - in-mem (mutable and RO):
  - Stub: lock + retry
  - Sealed: Yield() and retry
  - Other: as currently
  - On-disk:
    - TODO: PENDING then apply **lockcache**

## Upsert

- LockOp.IsSet (Note: Unlock doesn't happen; Upsert() is not called by Unlock())
  - Stub: Create new record and transfer locks from Stub to it
    - This is Stub promotion; locking of added record is done in SingleWriter
  - Sealed: Yield() and retry
  - Tombstone: Do Lock op (e.g. unlock)
  - Normal: 
    - Mutable and RO: Do LockOp in ConcurrentWriter
    - on-disk: doesn't happen; Upsert does not have PENDING code path
  - Already in readcache: Lock
    - Note: This adds RO region handling to Upsert(), for lock handling
- LockOp is not set:
  - in-mem (mutable and RO):
    - Stub:
      - ManualOps session: Create new record and transfer locks from Stub record
      - non-ManualOps session: lock + retry
    - Sealed: retry (no lock, bc we use this for transfer and that would confuse counts, so just Yield())
    - Other: as currently
  - on-disk: doesn't happen; Upsert does not have PENDING code path
    - TODO: apply **lockcache**
    - TODO: When updating, promote to CTT from ReadCache *without* eliding entire readcache prefix chain

## RMW

- Lock/Unlock: None. Manual locking does not use RMW
- LockOp is not set:
 - in-mem (mutable and RO):
   - Stub
     - ManualOps session: Create new record and transfer locks from Stub to it (TODO reviv: update in place)
       - This is Stub promotion: locking of added record is done in InitialUpdater
     - non-ManualOps session: lock + retry
  - Sealed: Yield() and retry
   - Tombstone:
    - Nothing here as we do not process LockOp in RMW
  - Normal: as currently
  - on-disk: PENDING and **lockcache**
     - If Stub promotion, locking of added record done in InitialUpdater out of `InternalCompletePendingRMW`
  - TODO: When updating, promote to CTT from ReadCache *without* eliding entire readcache prefix chain
  - TODO: replace "fuzzy" region with Sealed

## Delete

- Lock/Unlock: None. Manual locking does not use Delete
  - Normal:
    - in-mem (mutable and RO):
      - Stub:
        - ManualOps: Change Stub to Tombstone via ConcurrentDeleter
        - non-ManualOps: lock + retry
      - Sealed: Yield() and retry
      - Other: as currently
    - on-disk: doesn't happen; Upsert does not have PENDING code path
      - TODO: apply **lockcache**
  - TODO: When deleting, remove from ReadCache *without* eliding entire readcache prefix chain
