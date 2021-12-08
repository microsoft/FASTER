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
- **Lock Bits**: There is one Exclusive Lock bit and 6 Shared Lock bits (allowing 64 shared locks) in the RecordInfo.
- **Tentative**: a record marked Tentative is very short-term; it indicates that the thread is performing a Tentative insertion of the record, and may make the Tentative record final by removing the Tentative bit, or may back off the insertion by setting the record to Invalid and returning RETRY_NOW.
- **Sealed**: a record marked Sealed is one for which an update is known to be in progress. Sealed records are "visible" only short-term (e.g. a single call to Upsert or RMW, or a transfer to/from the `LockTable`). A thread encountering this should immediately return RETRY_NOW.
  - Sealing is done via `RecordInfo.Seal`. This is used in locking scenarios rather than a sequence of "CAS to set Sealed; test Sealed bit` because the after-Seal locking is fuzzy; we don't know whether the record was CTT'd before or after a post-Seal lock, and thus we don't know if the transferred record "owns" our lock. `RecordInfo.Seal` does a CAS with both the XLock and Seal bits, then Unlocks the XLock bit; this ensures it works whether SupportsLocking is true or false. It returns  true if successsful or false if another thread Sealed the record.
- **Invalid**: This is a well-known bit from v1 included here for clarity: its behavior is that the record is to be skipped, using its `.PreviousAddress` to move along the chain.

Additionally, the `SupportsLocking` flag has been moved from IFunctions to a `FasterKV` constructor argument. This value must be uniform across all asessions. It is only to control the locking done by FasterKV; this replaces the concept of user-controlled locking that was provided with the `IFunctions` methods for concurrent record access.

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

When a thread doing `Lock()` looks for a key in the LockTable and cannot find it, it must do a Tentative insertion into the locktable, because it is possible that another thread CAS'd that key to the Tail of the log after the current thread had passed the hash table lookup:
- We do not find the record in memory starting from current TailAddress, so we record that TailAddress as prevTailAddress.
- Locktable does not have an entry for this key so we create a Tentative entry in the LockTable for it
- We check if key exists between current TailAddress and prevTailAddress
  - if yes we have to back off the LockTable entry creation by setting it Invalid (so anyone holding it to spin-test sees it is invalid), removing it from the LockTable, and returning RETRY_NOW.
    - Any thread trying an operation in the Lock Table on a Tentative record must spin until the Tentative bit is removed; this will be soon, because we are only following the hash chain back to A.
      - If prevTailAddress has escaped to disk by the time we start following the hash chain from Tail to prevTailAddress, we must retry. See the InternalTryCopyToTail scan to expectedLogicalAddress and ON_DISK as an example of this.
    - Any waiting thread sees Invalid and in this case, it must also return RETRY_NOW.
  - if no, we can set locktable entry as final by removing the Tentative bit
    - Any waiting thread proceeds normally

#### Removal from LockTable

Here are the sequences of operations to remove records from the Lock Table:
- Unlock
  - If the lock count goes to 0, remove from `LockTable` conditionally on IsLocked == false and Sealed == false.
    - Since only lock bits are relevant in LockTable, this is equivalent to saying RecordInfo.word == 0, which is a faster test.
- Pending Read to `ReadCache` or `CopyToTail`, Pending RMW to Tail, or Upsert or Delete of a key in the LockTable
  - For all but Read(), we are modifying or removing the record, so we must acquire an Exclusive lock on the LockTable entry
    - This is not done for `ManualFasterOperations`, which we assume owns the lock
  - The `LockTable` record is Sealed as described in [Relevant RecordInfo bits](#relevant-recordInfo-bits)
    - If this fails, the operation retries
    - Other operation threads retry upon seeing the record is sealed
  - The Insplice to the main log is done
    - If this fails, the Sealed bit is removed from the `LockTable` entry and the thread does RETRY_NOW
    - Else the record is removed from the `LockTable`
      - Note: there is no concern about other threads that did not find the record on lookup and "lag behind" the thread doing the LockTable-entry removal and arrive at the LockTable after that record has been removed, because:
        - If the lagging thread is from a pending Read operation, then that pending operation will retry due to the InternalTryCopyToTail expectedLogicalAddress check or the readcache "dual 2pc" check in [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
        - If the lagging thread is from a pending RMW operation, then that pending operation will retry due to the InternalContinuePendingRMW previousFirstRecordAddress check or the readcache "dual 2pc" check in [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
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
- "Outsplicing" occurs when a record is spliced out of the RC portion of the hash chain (main log records are never spliced out) because the value for that key must be updated, or because we are evicting records from the ReadCache. Outsplicing introduces concurrency considerations but we must support it; we cannot simply mark ReadCache entries as Invalid and leave them there, or the chain will grow without bound. For concurrency reasons we defer outsplicing to readcache eviction time, when readcache records are destroyed, as described below.
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
      - We only actually transfer records from the RC prefix to the LockTable if there is an active `ManualFasterOperations` session at the time `ReadCacheEvict` is called; otherwise there will be no locks. However, we must already traverse the `ReadCache` records, and it is possible for a new `ManualFasterOperations` session to start during the duration of `ReadCacheEvict`, so there is no benefit to checking for the no-`ManualFasterOperations` case (unlike [Main Log Evictions](#main-log-evictions), which can avoid page scans by checking for this).

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
- Shifting HeadAddress is now done in three steps: Update THA, handle evictions, then update HeadAddress
  - In (PageAligned)ShiftHeadAddress, we now:
    - epoch.BumpCurrentEpoch(() => OnPagesReadyToClose(oldTentativeHeadAddress, newHeadAddress));
      - OnPagesReadyToTransfer() is a new routine:
        - ReadCacheEvict (via EvictCallback)
        - epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress));
          - This actually evicts the pages

### Recovery Considerations

We must clear in-memory records' lock bits during FoldOver recovery. 
- Add to checkpoint information an indication of whether any `ManualFasterOperations` were active during the Checkpoint.
- If this MRO indicator is true:
  - Scan pages, clearing the locks of any records
    - These pages do not need to be flushed to disk
  - Ensure random reads and scans will NOT be flummoxed by the weird lock bits

### FASTER Operations

Following are the 4 FASTER operations and their flow for the various lock states.

Abbreviations:
- LockOp: The `LockOperations` instance passed to one of the InternalXxx methods.
- CTT: CopyToTail
- ITCTT: InternalTryCopyToTail
- Unfound refers to entries that are not found in memory (the hash chain passes below HeadAddress) or are not found in the Hash chain

#### Conflict Between Upsert/RMW and Reading From Disk to ReadCache

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
- Read:
  - Prior to its SkipReadCache/TracebackForKeyMatch, it sets a tentative record at the HT->RC boundary.
  - it does the scan
    - if the Tentative record is now Invalid, it means Upsert/RMW set it so for a later update; return NOTFOUND
    - else if it found a non-RC record for this key, it sets the Tentative record to Invalid and returns NOTFOUND
    - else it removes the Tentative flag

OPTIMIZATION: Use readcache records rather than going to disk. However, there are issues here with the record being marked Invalid/Sealed in case multiple threads do it.

#### Read

Note that this changes specified here, including both shared and exclusive locks in the ReadOnly region, clarifies the distinction between a data-centric view of the ReadOnly region being implicitly read-locked (because it cannot be updated), vs. a transactional view that requires explicit read locks. In a transactional view, a read lock prevents an exclusive lock; implicit readlocks based on address cannot do this in FASTER, because we can always do XLock or an RCU. Therefore, we need explicit read locks, and reads must of course block if there is an XLock. This also means that SingleReader would have to lock anyway, losing any distinction between it and ConcurrentReader. Therefore, we have consolidated ConcurrentReader and SingleReader into a single function.

- for both mutable and RO records, if the RecordInfo is:
  - Sealed: Yield() and retry
    - If SupportsLocking, we would ephemerally readlock the record, and we can't lock Sealed records as the lock may be transferred with the record.
    - Tombstone: as current
  - Other: as currently, including ephemeral locking
    - Change IFunctions.SingleReader and .ConcurrentReader to simply .Reader
- On-disk: 
  - After PENDING
    - if copying to readcache, do so in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
    - else if CopyToTail do [Removal From LockTable](#removal-from-locktable)

#### Upsert

Note: Upsert skips RO ops if the current FasterSession is not `ManualFasterOperations` (MFO); this comparison is a static bool property of to the IFasterOperations implementation 

- If LockOp.IsSet
  - If the record is in readcache: 
    - Do the Lock op:
      - retry if the record is or becomes Sealed
      - ignore/continue if the record is or becomes Invalid
  - else for both mutable (and RO if MFO is active) records, if the RecordInfo is:
    - Sealed: Yield() and retry
    - Tombstone: Do the Lock op (e.g. unlock)
    - Other: 
      - Do the LockOp in ConcurrentWriter for both Mutable and RO 
  - else // key is not found or hash chain goes below HeadAddress
    - Perform `LockTable` insertion as described in [Insertion to LockTable due to Lock](#insertion-to-locktable-due-to-lock)
- else // LockOp is not set:
  - If the record is in readcache:
    - Invalidate it
    - insert the new value in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
  - else if the record is in the mutable region and the RecordInfo is:
    - Sealed: Yield() and retry
    - Tombstone: as current
    - Other: IPU (including ephemeral locks)
      - If this returns false
        - Set RecordInfo Sealed as described in [Relevant RecordInfo bits](#relevant-recordinfo-bits)
        - Insert in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
  - else if the record is in ReadOnly and the RecordInfo is:
    - Sealed: Yield() and retry
    - Tombstone: as current
    - Other: Do CopyUpdater and insert in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
  - else // key is not found or hash chain goes below HeadAddress
    - if the key is in the lock table
      - XLock it
        - If it is Sealed or Invalid, then RETRY_NOW (someone else did an operation that removed it)
        - Else
          - Insert new record
          - Remove locktable entry per [Removal From LockTable](#removal-from-locktable)
    - InitialUpdater and insert in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)

#### RMW

RMW considerations are similar to Upsert from the sealing and "encountering locks" point of view. It does not do lock operations.

- If the record is in readcache:
  - Invalidate it
  - CopyUpdater and insert the new value in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
- else if the record is in the mutable region and the RecordInfo is:
  - Sealed: Yield() and retry
  - Tombstone: as current
  - Other: IPU (including ephemeral locks)
    - If this returns false
      - Set RecordInfo Sealed as described in [Relevant RecordInfo bits](#relevant-recordinfo-bits)
      - Do CopyUpdater and insert in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
- else if the record is in ReadOnly and the RecordInfo is:
  - Sealed: Yield() and retry
  - Tombstone: as current
  - Other: Do CopyUpdater and insert in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
- else // key is not found or hash chain goes below HeadAddress
  - InitialUpdater and insert in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
- TODO: potentially replace "fuzzy" region at SafeReadOnlyAddress with Sealed, which should avoid the lost-update anomaly

#### Delete

- If the record is in readcache:
  - Invalidate it
  - insert the new deleted record in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
- else if the record is in the mutable region and the RecordInfo is:
  - Sealed: Yield() and retry
  - Tombstone: as current (nothing)
  - Other: Mark as tombstone 
- else if the record is in ReadOnly and the RecordInfo is:
  - Sealed: Yield() and retry
  - Tombstone: as current (nothing)
  - Other: Insert deleted record in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
- else // key is not found or hash chain goes below HeadAddress
    - if the key is in the lock table
      - XLock it
        - If it is Sealed or Invalid, then RETRY_NOW (someone else did an operation that removed it)
        - Else
          - Insert deleted record
          - Remove locktable entry per [Removal From LockTable](#removal-from-locktable)
    - Insert deleted record in accordance with [Conflict Between Upsert/RMW and Reading From Disk to ReadCache](#conflict-between-upsert-rmw-and-reading-from-disk-to-readcache)
