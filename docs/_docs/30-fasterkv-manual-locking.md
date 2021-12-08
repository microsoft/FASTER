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
        manualOps.Lock(24, LockType.Shared);
        manualOps.Lock(51, LockType.Shared);
        manualOps.Lock(75, LockType.Exclusive);

        manualOps.Read(24, out var value24);
        manualOps.Read(51, out var value51);
        manualOps.Upsert(75, value24 + value51);

        manualOps.Unlock(24, LockType.Shared);
        manualOps.Unlock(51, LockType.Shared);
        manualOps.Unlock(75, LockType.Exclusive);

        manualOps.UnsafeSuspendThread();
```

Lock multiple keys:
```cs
    using (var manualOps = session.GetManualOperations())
    {
        manualOps.UnsafeResumeThread(out var epoch);

        LockInfo lockInfo = default;
        manualOps.Lock(51, LockType.Shared);

        manualOps.Read(24, out var value24);
        manualOps.Read(51, out var value51);
        manualOps.Upsert(75, value24 + value51);

        manualOps.Unlock(51, LockType.Shared);

        manualOps.UnsafeSuspendThread();
```

TODO: Add sample with `manualOps.LocalCurrentEpoch`.

## Internal Design

This section covers the internal design and implementation of manual locking.

Manual locking is integrated into `FASTERImpl.cs` methods, notably `InternalRead` and `InternalCompletePendingRead`, `InternalUpsert`, `InternalRMW` and `InternalCompletePendingRMW`, and `InternalDelete`. These modifications are exposed via the `Lock()` and `Unlock()` APIs on `ManualFasterOperations`. LockOperation-specific code done in `InternalUpsert` and is protected by an `if (fasterSession.IsManualOperations)` test, which is a static bool member of the `FasterSession` implementation so the comparison should optimize it out.

Because epoch protection is done by user calls, ManualFasterOperations methods call the internal ContextRead etc. methods, which are called by the API methods that do Resume and Suspend of epoch protection.

At a high level, `Lock()` and `Unlock()` call `ContextUpsert()` which in turn calls `InternalUpsert()`. Upsert by design does not issue PENDING operations to retrieve on-disk data, and locking/unlocking is designed to avoid pending I/O operations by use of a [`LockTable`](#locktable-overview) consisting of {`TKey`, `RecordInfo`} pairs, where `TKey` is the FasterKV Key type and `RecordInfo` is used to perform the locking/unlocking.

Locking and unlocking use bits in the `RecordInfo` header to obtain one exclusive lock or up to 64 shared locks. Because locking does not affect data, even records in the ReadOnly region may be locked and unlocked directly.

### Relevant RecordInfo bits

The following sections refer to the following two in the `RecordInfo`:
- **Tentative**: a record marked Tentative is very short-term; it indicates that the thread is performing a Tentative insertion of the record, and may make the Tentative record final by removing the Tentative bit, or may back off the insertion by setting the record to Invalid and returning RETRY_NOW.
- **Sealed**: a record marked Sealed is one for which an update is known to be in progress. Sealed records are "visible" only short-term (e.g. a single call to Upsert or RMW, or a transfer to/from the `LockTable`). A thread encountering this should immediately return RETRY_NOW.
- **Invalid**: This is a well-known bit from v1 included here for clarity: its behavior is that the record is to be skipped, using its `.PreviousAddress` to move along the chain.

### LockTable Overview

For records not found in memory, the `LockTable` is used. The semantics of `LockTable` entries are as follow. This is a conceptual view; implementation details are described in subsequent sections:
- On a `Lock` call, if the key is not found in memory, the `LockTable` is searched for the Key.
  - If it is not found, an entry is made in the `LockTable` with an empty `RecordInfo`.
  - The requested `LockType` is then taken on the `RecordInfo` for that Key.
- On an `Unlock` call, if the key is not found in memory, the `LockTable` is searched for the Key.
  - If it is not found, a Debug.Fail() is issued.
  - Otherwise, the requested `LockType` is unlocked. If this leaves the `RecordInfo` unlocked, its entry is deleted from the `LockTable`.
- When a Read or RMW obtains a record from ON-DISK, it consults the `LockTable`; if the key is found, the locks are transferred to the retrieved recordInfo, and the `LockTable` entry is removed.
- When an Upsert (without `LockOperations`) or Delete does not find a key in memory, it consults the `LockTable`, and if the key is found:
  - it Seals the RecordInfo in the `LockTable`
  - it performs the usual "append at tail of Log" operation
  - it removes the entry from the `LockTable`
- Because `LockTable` use does not verify that the key actually exists (as it does not issue a pending operation to ensure the requested key, and not a collision, is found in the on-disk portion), it is possible that keys will exist in the `LockTable` that do not in fact exist in the log. This is fine; if we do more than `Lock` them, then they will be added to the log at that time, and the locks applied to them.

#### Insertion to LockTable due to Lock
When a thread doing `Lock()` looks for a key in the LockTable and cannot find it, it must do a tentative insertion into the locktable, because it is possible that another thread CAS'd that key to the Tail of the log after the current thread had passed the hash table lookup:
- We do not find the record in memory starting from current TailAddress, so we record that TailAddress as A.
- Locktable does not have entry so we create a tentative entry in locktable
- We check if key exists between current tail and A
  - if yes we have to back off the LockTable entry creation by setting it Invalid and returning RETRY_NOW.
    - Any thread trying an operation in the Lock Table on a Tentative record must spin until the Tentative bit is removed; this will be soon, because we are only following the hash chain back to A.
    - Any waiting thread sees Invalid and in this case, it must also return RETRY_NOW.
  - if no, we can set locktable entry as final by removing the Tentative bit
    - Any waiting thread proceeds normally

#### Removal from LockTable
Here are the sequences of operations to remove records from the Lock Table:
- Unlock
  - If the lock count goes to 0, remove from `LockTable` conditionally on IsLocked == false.
- Pending Read to `ReadCache` or `CopyToTail`, Pending RMW to Tail, or Upsert or Delete of a key in the LockTable
  - For all but Read(), we are modifying or removing the record, so we must acquire an Exclusive lock
    - This is not done for `ManualFasterOperations`, which we assume owns the lock
  - The `LockTable` record is CAS'd to Sealed.
    - Lock and Unlock must return an out bool sealedWhenLocked
      - If so, then it reverts and retries
    - Other operations retry upon seeing the record is sealed
  - The Insplice to the main log is done
    - If this fails, the Sealed bit is removed from the `LockTable` entry and the thread does RETRY_NOW
    - Else the record is removed from the `LockTable`

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
Note that this resolution is only needed if there is an active `ManualFasterOperations` session at the time `ReadCacheEvict` is called. However, we must already traverse the `ReadCache` records, and it is possible for a new `ManualFasterOperations` session to start during the duration of `ReadCacheEvict`, so we do not optimize for the no-`ManualFasterOperations` case.

For brevity, `ReadCache` is abbreviated RC, `CopyToTail` is abbreviated CTT, and `LockTable` is abbreviated LT. Main refers to the main log. The "final RC record" is the one at the RC->Main log boundary. As always, avoiding locking cost is a primary concern. 

For record transfers involving the ReadCache, we have the following high-level considerations:
- There is no record-transfer concern if the first record in the chain is not a `ReadCache` entry (normal CAS will do all necessary concurrency control as presently, and there are no outsplices).
  - Otherwise, we insplice between the final RC entry and the first main-log entry; we never splice into the middle of the RC prefix chain.
- Even when RC entries are present in the tail, we must avoid latching because it affects all insert operations (at a minimum).
- "Insplicing" occurs when a new record is inserted into the main log after the end of the ReadCache prefix string.
- "Outsplicing" occurs when a record is spliced out of the RC portion of the hash chain (main log records are never spliced out) because the value for that key must be updated, or because we are evicting records from the ReadCache. Outsplicing introduces concurrency considerations but we must support it; we cannot simply mark ReadCache entries as Invalid and leave them there, or the chain will grow without bound. 
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

When main log pages are evicted due to memory limits, *if* there are any active `ManualFasterOperations` sessions, then each record on those pages must be examined and any locks transferred to `LockTable` entries.

Transfers to the `LockTable` due to main log evictions are handled in the following manner:
- A new `TentativeHeadAddress` (THA) field is added next to `HeadAddress`.
- Shifting HeadAddress is now done in two steps: Update THA, then update HeadAddress
- The purpose of THA is:
  - No record at address < THA shall be touched for Lock operations (lock or unlock).
  - Existing locks will remain there until transferred to the lock table.
  - A thread that attempts to unlock a record < THA must wait for the lock table to get populated (details below)
    - To mitigate unlocking records that were never locked, this must abandon the wait when the record to be unlocked becomes < HeadAddress.

Here is the sequence of operations to perform record eviction with `LockTable` Transfer:
- In (PageAligned)ShiftHeadAddress:
  - TentativeHeadAddress = desired new HeadAddress
  - BumpCurrentEpoch(() => OnPagesReadyToTransfer())
    - OnPagesReadyToTransfer() is a new routine:
      - Scan from OldTentativeHeadAddress to new TentativeHeadAddress:
        - Transfer records to `LockTable`
        - This is safe because we are executing in the epoch Drain thread
      - OldTentativeHeadAddress = TentativeHeadAddress
      - HeadAddress = TentativeHeadAddress

### FASTER Operations

Following are the 4 FASTER operations and their flow for the various lock states.

Abbreviations:
- LockOp: The `LockOperations` instance passed to one of the InternalXxx methods.
- CTT: CopyToTail
- ITCTT: InternalTryCopyToTail
- Unfound refers to entries that are not found in memory (the hash chain passes below HeadAddress) or are not found in the Hash chain

#### Read

- Lock/Unlock: None. Manual locking does not use Read
- LockOp is not set:
  - in-mem (mutable and RO):
    - Sealed: Yield() and retry
    - Other: as currently
  - Unfound: After PENDING, transfer any `LockTable` entry as described above
    - Splice out from readcache prefix chain if applicable

#### Upsert

- LockOp.IsSet
  - Sealed: Yield() and retry
  - Tombstone: Do Lock op (e.g. unlock)
  - Normal: 
    - Mutable and RO: Do LockOp in ConcurrentWriter
    - on-disk: Perform `LockTable` insertion as described above
  - Already in readcache: Lock
  - Note: Locking adds ReadOnly-region handling to Upsert()
- LockOp is not set:
  - in-mem (mutable and RO):
    - Sealed: Yield() and retry
    - Other: as currently
    - Splice out from readcache prefix chain if applicable
  - Unfound: if found in `LockTable`, do [Removal From LockTable](#removal-from-locktable)

#### RMW

- Lock/Unlock: None. Manual locking does not use RMW
- LockOp is not set:
  - in-mem (mutable and RO):
    - Sealed: Yield() and retry
    - Tombstone: Nothing here as we do not process LockOp in RMW
    - Normal: as currently
    - Splice out from readcache prefix chain if applicable
  - Unfound: if found in `LockTable`, do [Removal From LockTable](#removal-from-locktable)
    - Note: Splice out from readcache prefix chain if applicable
    - TODO: potentially replace "fuzzy" region with Sealed

#### Delete

- Lock/Unlock: None. Manual locking does not use Delete
  - in-mem (mutable and RO):
    - Sealed: Yield() and retry
    - Other: as currently
    - Splice out from readcache prefix chain if applicable
  - Unfound: if found in `LockTable`, do [Removal From LockTable](#removal-from-locktable)
  - Note: Splice out from readcache prefix chain if applicable
