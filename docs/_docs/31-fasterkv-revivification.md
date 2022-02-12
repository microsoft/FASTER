---
title: "FasterKV Manual Locking"
permalink: /docs/fasterkv-revivification/
excerpt: "FasterKV Revivification"
last_modified_at: 2022-02-11
toc: true
---

## Revivification in FasterKV

Revivification in FasterKV refers to reusing ("revivifying") Tombstoned (deleted) records. This minimizes log growth (and unused space) for high-delete scenarios.

There are two forms of revivification:
- **In-Chain**: Tombstoned records are left in the hash chain. A subsequent Upsert or RMW of the same key will revivify the record if it has sufficient value length.
- **FreeList**: A free list is maintained, and records that are Tombstoned and are at the tail of the hash chain (pointed to directly from the hash table) are removed from the hash chain and kept in a free list (a binned (by power of 2), multiple-partition-per-bin set of circular buffers).

## In-Chain
If the FreeList is not active, Tombstoned records are left in the hash chain. A subsequent Upsert or RMW of the same key will revivify the record if it has sufficient value length.

In-Chain revivification is always on:
- A Tombstoned record may not be at the head of the hash chain.
- It is possible to fail the CAS due to a concurrent insert

## FreeList
If `FasterKVSettings.MaxFreeRecordsInBin` is nonzero, this activates the freelist.

When the FreeList is active and a record is deleted, if it is at the tail of the hash chain, is not locked, and its PreviousAddress points below BeginAddress, then it can be CASed (Compare-And-Swapped) out of the hash chain. If this succeeds, it is added to the freelist.

### Design of FreeList
The FreeList hierarchy consists of:
- The FreeRecordPool, which maintains the bins, deciding which bin should be used for Enqueue and Dequeue.
- Multiple FreeRecordBins, including one for oversize (where the length is too long to fit into the FreeRecordHeader)
  - Each FreeRecordBin is partitioned (currently the number of partitions is ProcessorCount / 2) and the ManagedThreadId is used to decide which partition to start on for both enqueueing and dequeueing. If the initial partition cannot accommodate the operation (it is full for enqueue or empty for dequeue), the next partition is done, wrapping around until all partitions have been examined.
- FreeRecords, which are syntactic sugar over a `long` value that contains the Address and Size.
  - The Address is stored similarly to `RecordInfo.PreviousAddress`, with the same number of bits
  - The Size is shifted above the address, using the remaining bits. Thus it is limited to 16 bits; any free records over that size go into the oversize bin.
    - The smallest bin is of size 16, allowing reuse of records large enough to hold an integer key and value. 

#### Fixed vs. VarLen
For non-variable-length types, the record size is fixed, so the FreeRecordPool has only the one bin for it. Otherwise, it has the full range of variable-length bins.

#### Enqueueing
Bins hold records up to their maxium size, which is a power of 2. Enqueuing will enqueue a record into the "smallest" bin it fits into.

#### Dequeueing
Records stored in a bin have lengths that are *between* the lowest and highest sizes of a bin, *not* the highest size of the bin. Therefore, for performance we have to retrieve from the next-highest bin; e.g. 48 will come from the [64-127] bin because the [32-63] bin might not have anything larger than 42, so we can't satisfy the request and it would take too much time to find that out before moving to the larger bin.

## Revivifying a record
The FreeRecordList is used for:
- An Upsert, RMW, or Delete that adds a new record
- A Pending Read or RMW that does CopyToTail (Revivification is not used for the ReadCache).

If there is a record available, it is reused; otherwise, the usual BlockAllocate is done.

### Record Length Management
There are two lengths we are concerned with: The FullValueLength (allocated space), and the UsedValueLength within that that the application has used. These are properties of the new `UpdateInfo` structure passed by reference to `IFunctions` update callbacks for Upsert, RMW, and Delete.

The used space is for a new record is equal to the full allocation space; by default, "all space is used". Note that SpanByte actually initializes itself to include space for the entire record. However, other data types may not do this.

For non-Tombstoned records, the full value (not record) length is stored in an integer immediately following (4-byte aligned) the used value space. FasterImpl.cs contains some utility functions at the top that set the full value length into and retrieve it from the value space, based upon this offset. If there is extra length in the record (UsedValueLength < FullValueLength by at least the (aligned) space of an integer), the length is written and the Filler bit is set. Otherwise, the Filler bit is cleared.

Having UsedValueLength set to FullValueLength by default means we do not write the length into the record (and keep the Filler bit cleared) unless UsedValueLength is adjusted by the application.

#### Single-Threaded operations
For single-threaded operations, the record is either not yet CASed into the hash table, or is protected by the Tentative flag. These single-threaded initial record creation operations therefore do not do locking, and length determination and setting into the value space (if applicable) are done within in InternalXxx in FasterImpl.cs.

#### Concurrent Operations
Concurrent update operations (ConcurrentWriter, InPlaceUpdater, and ConcurrentDeleter) msut lock when retrieving lengths, performing the update, and setting lengths back into the record. Therefore, these operations are done within the IFasterSession implementation, within the ephemaral lock.