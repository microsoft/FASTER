---
title: "FasterKV Manual Locking"
permalink: /docs/fasterkv-revivification/
excerpt: "FasterKV Revivification"
last_modified_at: 2023-08-08
toc: true
---

## Revivification in FasterKV
Revivification in FasterKV refers to reusing ("revivifying") Tombstoned (deleted) records. This minimizes log growth (and unused space) for high-delete scenarios. It is done in the following cases:
The FreeRecordList is used for:
- An Upsert, RMW, or Delete that adds a new record
- CopyToTail from Read or RMW that did IO (or from ReadFromImmutable copying to tail)
- Revivification is *not* used for the ReadCache

There are two forms of revivification:
- **In-Chain**: Tombstoned records are left in the hash chain. A subsequent Upsert or RMW of the same key will revivify the record if it has sufficient value length.
- **FreeList**: A free list is maintained, and records that are Tombstoned and are at the tail of the hash chain (pointed to directly from the hash table) are removed from the hash chain and kept in a free list (a binned (by power of 2), multiple-segment-per-bin set of circular buffers).

These are separate from the reuse of a record due to a RETRY return from `CreateNewRecordXxx`. In that case the record address is stored in the `PendingContext` and `CreateNewRecordXxx` returns either RETRY_LATER or RETRY_NOW; when the `CreateNewRecordXxx` is attempted again, it will retrieve this record (if the length is still sufficient and it has not fallen below the minimal required address). Retry reuse is always enabled; revivification might not be.

### `RevivificationSettings`
This struct indicates whether revivification is to be active:
- `EnableRevivification`: If this is true, then at least in-chain revivification is done; otherwise, record revivification is not done (but Retry reuse still is).
- `FreeListBins`: If this array of `RevivificationBin` is non-null, then revivification will include a freelist of records, as defined below.

## Maintaining Extra Value Length
Because variable-length record Values can grow and shrink, we must store the actual length of the value in addition to the Value data. Fixed-length datatypes (including objects) do not need to store this, because their Value length does not change.

This storage of record lengths applies to both Revification and non-Revivification scenarios. `ConcurrentWriter` and `InPlaceUpdater` may change the record size, and need to know how much space they have available if they are to grow in place. Revivification needs to store the record length so we can satisfy requests from the pool. 

Storing the extra value length is done by rounding up the used value length (the current length of the value) to int boundary, and then if the allocated Value space is 4 bytes or more greater, storing the extra length at (start of Value plus rounded-up used space). Thus, the full value size is the rounded-up used value size plus the extra value size. If this extra length can be stored then the RecordInfo.Filler bit is set to indicate the extra length is present. Variable-length log allocations are 8-byte (RecordInfo size) aligned, so any extra length less than sizeof(int) can be inferred from this rounding up.

Storing the extra value length must be done carefully so we maintain the invariant that unused space is zeroed. This is necessary because log scans assume any non-zero data they land on past the previous record length is a valid RecordInfo header. If the extra length is set before the Filler bit is, or if the Filler bit is cleared before the extra length is zeroed, then a log scan could momentarily see a short length, land on the nonzero extra length, and think it is a valid RecordInfo. The other direction is safe: the filler bit may be set but the extra length is zero. In this case the scan sees a zeroed RecordInfo (`RecordInfo.IsNull()`) and assumes it is uninitialized space and steps forward by the size of a RecordInfo (8 bytes).

This invariant is also followed when variable-length values are shrunk, even in the absence of the Filler bit. When a value is shrunk, the data beyond the new (shorter) size must be zeroed before the shorter size is set.

Combining these two, when we adjust a variable-length value length, we must:
- Clear the extra value length.
- Remove Filler bit.
- Zero extra value data space past the new (shorter) size.
- Set the new (shorter) value length.
- Set the Filler bit.
- Set the extra value length to the correct new value.

The FASTER-provided variable-length implementation is `SpanByte`, and `SpanByteFunctions` variants do the right thing here.

### UpdateInfo Record Length Management
For variable-length datatypes, the `UpsertInfo`, `RMWInfo`, and `DeleteInfo` have two fields that let the relevant `IFunctions` callbacks know about the record length (without having to know the internals of value storage):
- UsedValueLength: the amount of value space that is actually in use.
- FullValueLength: full allocated space for the value; this is the requested Value size for the initial Insert of the record rounded up to 8-byte alignment.

For SpanByte, the `VariableLenghtBlittableAllocator.GetValue` overload with (physicalAddress, physicalAddress + allocatedSize) that calls the default SpanByte implementation of `IVariableLengthStructureSettings` actually initializes the value space to be a valid SpanByte that includes the requested Value length (up to the maximum number of elements that can fit). However, other variable-length data types may not do this.

## DisposeForRevivification
In general, the `IFunctions` `Dispose*` functions are intended for data that does not get written to the log (usually due to CAS failure or a failure in `SingleWriter`, `InitialUpdater`, or `CopyUpdater`). Data written to the log will be see by `OnEvictionObserver` if needed. However, revivified records are taken from the log and reused; thus they must give the application an opportunity to Dispose().

To handle this a new `DisposeForRevivification` `IFunctions` method has been added. When a record is revivified from either in-chain or freelist, or is reused from Retry, it has a Key and potentially a Value. FASTER does not call `DisposeForRevivification` at the time a record becomes eligible for revivification; this would entail zero-initializing the entire data space and then writing over it again with the new values in `SingleWriter` etc. The record may never be revivified at all, in which case this effort would be wasted. Instead, FASTER leaves the valid Key and Value in the record, and only calls `DisposeForRevivification` at the time the record is revivified (or reused), in the following way:
- Clears the extra value length and filler (the extra value length is retained in local variables).
- Calls `DisposeForRevivification` with `newKeySize` > 0 if the record was retrieved from the freelist
    - If `DisposeForRevivification` clears the Value and possibly Key, it must do so in accordance with the protocol for zeroing data after used space as described in [Maintaining Extra Value Length](#maintaining-extra-value-length). `SpanByte` does not need to clear anything as a `SpanByte` contains nothing that needs to be freed.
    - In a potentially breaking change from earlier versions, any non-`SpanByte` variable-length implementation must either implement `DisposeForRevivification` to zero-init the space or must revise its `SingleWriter`, `InitialUpdater`, and `CopyUpdater` implementations to recognize an existing value is there and handle it similarly to `ConcurrentWriter` or `InPlaceUpdater`.
        - For `SpanByte`, `SingleWriter` et al. will have a valid target value in the destination for non-revivified records, because `VariableLengthBlittableAllocator.GetAndInitializeValue` (called after record allocation) calls the default SpanByte implementation of `IVariableLengthStructureSettings` and actually initializes the value space to be a valid SpanByte that includes the entire requested value size. For newly-allocated (not revivified) records the value data initialized to zero; for revivified records this is not guaranteed; only the value space after the usedValueLength is guaranteed to be zeroed, so `SingleWriter` et al. must be prepared to shrink the destination value.

## In-Chain Revivification
If the FreeList is not active, all Tombstoned records are left in the hash chain. A subsequent Upsert or RMW of the same key will revivify the record if it has sufficient allocated value length.

In-Chain revivification is active if `RevivificationSettings.EnableRevivification` is true. It functions as follows:
- `Delete()` is done in the normal way; the Tombstone is set. If the Tombstoned record is at the tail of the tag chain (i.e. is the record in the `HashBucketEntry`) and the FreeList is enabled, then it will be moved to the FreeList. Otherwise (or if this move fails), it remains in the tag chain.
- `Upsert()` and `RMW()` will try to revivify a Tombstoned record:
    - If doing `Ephemeral` locking, we Seal the record so no other thread can access it; they will see the Seal and retry (so will not insert another record while this is going on).
    - If the record is large enough, we Reinitialize its Value by:
        - Clearing the extra value length and filler and calls `DisposeForRevivification` as described in [Maintaining Extra Value Length](#maintaining-extra-value-length).
        - Removing the Tombstone.
    - If doing `Ephemeral` locking, we Unseal the record.

## FreeList Revivification
If `RevivificationSettings.FreeBinList` is not null, this creates the freelist according to the `RevivificationBin` elements of the array. If the data types are fixed, then there must be one element of the array; otherwise there can be many.

When the FreeList is active and a record is deleted, if it is at the tail of the hash chain, is not locked, and its PreviousAddress points below BeginAddress, then it can be CASed (Compare-And-Swapped) out of the hash chain. If this succeeds, it is added to the freelist.

FreeList revivification functions as follows:
- `Delete()` checks to see if the record is at the tail of the hash chain (i.e. is the record in the `HashBucketEntry`). 
    - If it is not, then we do not try to freelist it due to complexity: TracebackForKeyMatch would need to return the next-higher record whose .PreviousAddress points to this one, *and* we would need to check whether that next-higher record had also been freelisted (and possibly revivified).
    - Otherwise, `Delete()` checks to see if its PreviousAddress points below BeginAddress. If not, then we must leave it; it may be the marker for a deleted record below it, and removing it from the tag chain would allow the earlier record to be reached erroneously.
    - Otherwise, we try to CAS the newly-Tombstoned record's `.PreviousAddress` into the `HashBucketEntry`. 
        - It is possible to fail the CAS due to a concurrent insert
    - If this succeeds, then we `Add` the record onto the freelist. This Seals it, in case other threads are traversing the tag chain.
- `Upsert()` and `RMW()` will try to revivify a freelist record if they must create a new record:
    - They call `TryTakeFreeRecord` to remove a record from the freelist.
    - If successful, `TryTakeFreeRecord` initializes the record by:
        - Clearing the extra value length and filler and calls `DisposeForRevivification` as described in [Maintaining Extra Value Length](#maintaining-extra-value-length).
        - Unsealing the record; epoch management guarantees nobody is still executing who saw this record before it went into the free record pool.

### `FreeRecordPool` Design
The FreeList hierarchy consists of:
- The `FreeRecordPool`, which maintains the bins, deciding which bin should be used for Enqueue and Dequeue.
- Multiple `FreeRecordBins`, one for each `RevivificationBin` in the `RevivificationSettings` with the corresponding number of records, max record size, and best-fit scan specification.
- Each element of a bin is a `FreeRecord`

Each bin is a separate allocation, so the pool uses a size index which is a cache-aligned separate vector of ints of the bin sizes that is sequentially searched to determine the bin size. This avoids pulling in each bin’s separate cache line to check its size.

#### `FreeRecord` Design
A `FreeRecord` contains a free log address and the epoch in which it was freed; its data is:
  - A 'long' which is the address (48 bits) and size (16 bits). If the size is > 2^16, then the record is "oversize" and its exact size is obtained from the hlog.
    - The Address is stored similarly to `RecordInfo.PreviousAddress`, with the same number of bits
    - The Size is shifted above the address, using the remaining bits. Thus it is limited to 16 bits; any free records over that size go into the oversize bin.
  - An epoch, which ensures that no threads are executing that saw this record before it went into the free record pool, so the record cannot "be pulled out from underneath". For more detail see [Bumping Epochs](#bumping-epochs).

#### `FreeRecordBin` Design

The records of the bin are of sizes between [previous bin's max record size + 8] and [current bin's max record size]. As a shortcut we refer to bins by their max record size, i.e. "the 64-byte bin" means "the bin whose min record size is 8 more than the max size of the previous bin, and whose max record size is 64."

Each bin has a cache-aligned vector of `FreeRecord` that operates as a circular buffer. Unlike FIFO queues, we don't maintain a read/write pointer, for the following reasons:
- Some records in the bin will be smaller than the size requirement for a given allocation request; this could mean skipping over a number of records before finding a fit. With the pure FIFO approach this would lose a lot of records. The only solutions that keep the read/write pointer are to re-enqueue the skipped record(s), or some variation of peeking ahead of the read pointer, which would introduce complexity such as potentially blocking subsequent writes (the read pointer could be “frozen” but with many open elements beyond it that the write pointer can’t advance to). 
- Maintaining the read and write pointers entails an interlock on every call, in addition to the Add/Remove interlock on the FreeRecord itself.

We wish to obtain a solution that has only one interlock on read and write operations, does not iterate the entire segment, and makes a best effort at best-fit. To accomplish this we use a strategy of segmenting the bin by record sizes.

##### Variable-Length Bin Segmenting

For varlen Key and Value types, we segment bins based on the range of sizes at 8-byte alignment.

First, the max number of records in the bin is rounded up to a cache-aligned size, and may be further rounded up to ensure that each segment starts on a cache boundary. `FreeRecord`s are 16 bytes as stated above so there are 4 per cache line. However, 4 elements is too small for a segment, so we will require a minimum segment size of 8 (2 cache lines). This is a defined constant `MinSegmentSize` so can be modified if desired.

We calculate bin segments by evenly dividing the number of records in the by by the range of possible 8-byte-aligned record sizes in the bin, with a minimum of two cache lines (8 items) per segment. Segments do not have read/write pointers; they are just the index to start at in the bin’s record array, calculated on the fly from the record size (if Adding) or the requested record size (if Taking).

Here are 3 examples of determining the size ranges, using bin definitions as follow:
- A 32-byte bin and a 64-byte bin, both with 1024 records
- Intervening bins we will ignore but which end with a max record size of 2k
- A bin whose max record size is 4k and has 256 records.
This example takes from the 

The following segmenting strategies are used:
- The 32-byte bin’s minimum size is 16 because it is the first bin. Because the sizes are considered in multiples of 8 or greater, it has 3 possible record sizes: 16, 24, 32. Thus we create internal segments of size 1024/3, then round this up so each segment has a record count that is a multiple of 4. Thus, each segment is 344 elements (1024/3 is 341.33...; round this up to multiple of `MinSegmentSize`), and there are 1032 total elements in the bin. These segments start at indexes 0, 344, 688.
- The 64-byte bin’s min record size is 40 (8 greater than the previous bin’s maximum). By the foregoing, it has 4 possible record sizes: 40, 48, 56, 64. 1024 is evenly divisible by 4 to `MinSegmentSize`-aligned 256, and therefore the segments start at 0, 256, 512, 768.
- The the size range of the larger bin has too many possible sizes to create one segment for each size, and therefore uses a different segment-calculation strategy. In this case the size range is 2k which divided by 8 is 256 possible record sizes within the bin. Dividing the bin record count by this yields 4 which is below the required `MinSegmentSize`, so instead we set the bin's segment count to `MinSegementSize` and divide the bin record count by that to get the number of segments (rounded up to integer), and then set the segment size increment to the size range divided by segment count. We therefore have 16 segments, and thus we divide the size range by 16 to get the record size ranges for the segments. The number of records is the segment size multiplied by the segment count. The segments are:
    - Segment 0 starts at index 0 with max record size 2k+16 (thus, the segment may contain a mix of records of the following sizes: 2k+8, 2k+16)
    - Segment 1 starts at index 8 with max record size 2k + 16*2
    - Segment 2 starts at index 16 with max record size, 2k+16*3
    - And so on
- Essentially the partitions are mini-bins that we can efficiently calculate the start offsets for based on record or request size.

An alternative to size-based partitioning would be to use the thread id, as `LightEpoch` does. However, for threadId-based partitioning there is no organization to the size; we could have a random distribution of sizes. With size-based partitioning, we will much more likely land on the size (or close to it) that we want, and overflowing will put us into the next-highest record-size partition half the time, potentially giving us near-best-fit with minimal effort. Additionally, size-based partitioning makes best-fit search for a request easier.

###### Best-Fit and First-Fit
As the names imply, we have the option of first-fit or best-fit to satisfy a request. We allow both, via `RevivificationBin.BestFitScanLimit`:
- `UseFirstFit`: The value is zero, so we do not scan; when a record is requested from the bin, it returns the first one that has a size >= the requested size, has an addedEpoch > CurrentEpoch, and has an address >= the minAddress passed to the Take call.
- `BestFitScanAll`: The value is Int.MaxValue, so we scan the entire bin (possibly wrapping), keeping track of the best fit, then try to return that (which may fail if another thread returned it first, in which case we retry). If at any point there is an exact fit, that bin attempts to return that record.
- Some other number < bin record count: Similar to `BestFitScanAll` except that it limits the number of records to scan.

##### Fixed-Length Bins

For fixed-length Key and Value datatypes there is only one bin and of course no size-based partitioning. In this case we could use thread-id-based partitioning (as could any sufficiently large partition) to determine the index within the partition to start iterating from. However, threadId-based partitioning suffers from the possibility that the writers write to different partitions than the readers read from; in the worst case the readers must wrap all the way around through all partitions to get to the records. This may be offset by the reduced cache-line ping-ponging between processor caches if the first 4 records of the partition are repeatedly updated by different threads, but this has not been tried.

#### Epoch management
As noted above, each `FreeRecord` contains an `addedEpoch` field which is the epoch during which the record was added to the pool. The record will not be eligible to Take() for revivification until BumpCurrentEpoch() and ComputeNewSafeToReclaimEpoch() have made that addedEpoch safe.

Epoch protection is needed because otherwise we could have a situation where, for example:
- A thread gets the first record of a `HashBucketEntry` that matches the key (note: Revivification does not freelist any deletion that is not the item in the `HashBucketEntry`).
- Another thread deletes and freelists the record.
- Another thread immediately revivifies the record with a different key.
- Now the first thread resumes and thinks it's still on a record of the same key, but it's not. 
    - We could mitigate this by having the first thread recheck the key after it acquires the lock, but this would be a performance penalty in the non-revivification case.

##### Bumping Epochs
We do not want to maintain a per-operation counter of records in the `FreeRecordPool` or each `FreeRecordBin` because this would be an additional interlock per `Add` or `Take`. We therefore need a worker thread that periodically iterates the bins looking for records whose addedEpoch == epoch.CurrentEpoch; these records will not be eligible for revivification until BumpCurrentEpoch() and ComputeNewSafeToReclaimEpoch() have made that addedEpoch safe.

This is done by the `BumpEpochWorker` class, which uses a separate on-demand `Task` to do the bump. It does this in a loop that performs:
- Bump: BumpCurrentEpoch
- Scan: Scan the entire pool looking for records that have an `addedEpoch` > `epoch.CurrentEpoch`, as well as tracking whether the pool currently `HasSafeRecords`. 
    - If there are no records requiring a bump, the task exits.
    - Otherwise, it calculates a duration to Sleep before the next Bump. The sleep duration is based upon the number of records requiring a Bump; for only a few records, it sleeps longer (up to 1 second max, currently); if there are a lot of records (up to 32, currently) it sleeps less, and for more records that that it will not sleep at all.
- Sleep: Sleeps for the duration calculated after the Scan, then re-loop up to restart the Bump/Scan/Sleep loop.
    - The sleep is done by a wait on an `AutoResetEvent`, because this is alertable (see following).

`BumpEpochWorker` has a Start method which is called by `FreeRecordPool` `Add` or `Take`. This Start method detects whether the `BumpEpochWorker` is currently in one of two states, and does the following if so:
- ScanOrQuiescent: Either there is no thread active or there is a task already scanning the bins as described previously.
    - In this case the Start methods launches another thread, which starts the Bump/Scan/Sleep loop. It's necessary to do this because the Scan thread may have proceeded past the point where the new record was Added to the pool; if the new Task is not issued, this record will not be detected for Bump or availability until some other thread does something (which may not happen immediately).
- BumpOrSleep: There is a thread either sleeping after Scan and before a re-Bump, or currently bumping the epoch. - In this case Start Sets() the event, which either awakens the executing Task from its Wait, or ensures the Wait is immediately satisfied as soon as the Task calls it. Following this the Task will loop back up to restart the Bump/Scan/Sleep loop.

##### Epochs as FreeRecord Latches
Because the `FreeRecordBin` has two 8-byte fields, it is not possible to atomically swap them. Therefore, we use the addedEpoch as an extremely short-term latch, as well as an empty-record indicator. This takes advantage of the fact that epochs must be >= 1.
- On Add, if the `FreeRecord.AddedEpoch` is Epoch_Empty (0), it is considered free.
    - We then CAS the Epoch_Latched (-1) value into the epoch.
    - If successful, we copy the address and *then* the epoch into the `FreeRecord`. This sequence ensures that the epoch is latched until the operation is complete.
- On Take, if the `FreeRecord.AddedEpoch` is > 0 (and other requirements are met), we know there is a valid record.
    - We try to CAS in the Epoch_Latched value
    - If successful, we re-verify size and address requirements (another thread may have updated it with the same epoch value but a smaller size or lesser address, for example).
    - If verified, we copy out the address and set it to kInvalidAddres, then set the epoch to Epoch_Empty (following the same sequence as Add to ensure the epoch is latched until the operation is complete).

#### Fixed vs. VarLen
For non-variable-length types, the record size is fixed, so the FreeRecordPool has only the one bin for it. Otherwise, it has the full range of variable-length bins.

#### Adding a Record to the Pool
When Adding a record to the pool:
- The caller calls `FreeRecordPool.TryAdd`.
- TryAdd scans the size index to find the bin index
- The bin at that index is called to Add the record
    - The bin calculates the segment offset based on the record size, then does a linear scan to look for a free space (or a space whose address is < fasterKV.ReadOnlyAddress).
    - If it finds one, the record is added with its address, size (if below 16 bits), and the currentEpoch at the time of the `TryAdd` call. `TryAdd` then returns true.
    - else `TryAdd` returns false

#### Taking a Record from the Pool
When Taking a record, we must:
- Ensure that the addedEpoch is > epoch.CurrentEpoch
- Maintain the invariant that hash chains point downward; the `HashTableEntry.Address` is passed as a minimum required address. This is either a valid lower address or an invalid address (below BeginAddress).

The operation then proceeds as:
- The caller calls `FreeRecordPool.TryTake`.
- TryTake scans the size index to find the bin index
- The bin at that index is called to try to Take the record
    - The bin calculates the segment offset based on the record size, then does a linear scan to look for a record that is >= the required size, is safe (addedEpoch is < CurrentEpoch) and has an address >= the minAddress passed to the call. See [Best-Fit and First-Fit](#best-fit-and-irst-fit) for a discussion of which viable records are returned.
    - If it finds a record:
        - It attempts to use CAS to return it  xxxxxxxx , the record is returned and `TryTake` returns true.
    - else `TryTake` returns false
