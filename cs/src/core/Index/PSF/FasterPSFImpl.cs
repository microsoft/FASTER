// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

//#define PSF_TRACE

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    // PSF-related internal function implementations for FasterKV; these correspond to the similarly-named
    // functions in FasterImpl.cs.
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
        where Key : new()
        where Value : new()
    {
        internal KeyAccessor<Key> PsfKeyAccessor => this.hlog.PsfKeyAccessor;

        internal bool ImplmentsPSFs => !(this.PsfKeyAccessor is null);

        bool ScanQueryChain(ref long logicalAddress, ref KeyPointer<Key> queryKeyPointer, ref int latestRecordVersion)
        {
            long physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
            var recordAddress = this.PsfKeyAccessor.GetRecordAddressFromKeyPhysicalAddress(physicalAddress);
            if (latestRecordVersion == -1)
                latestRecordVersion = hlog.GetInfo(recordAddress).Version;

            while (true)
            {
                if (this.PsfKeyAccessor.EqualsAtKeyAddress(ref queryKeyPointer, physicalAddress))
                {
                    PsfTrace($" / {logicalAddress}");
                    return true;
                }
                logicalAddress = this.PsfKeyAccessor.GetPrevAddress(physicalAddress);
                if (logicalAddress < hlog.HeadAddress)
                    break;    // RECORD_ON_DISK or not found
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
            }
            PsfTrace($"/{logicalAddress}");
            return false;
        }

        [Conditional("PSF_TRACE")]
        private void PsfTrace(string message)
        {
            if (!this.ImplmentsPSFs) Console.Write(message);
        }

        [Conditional("PSF_TRACE")]
        private void PsfTraceLine(string message = null)
        {
            if (this.ImplmentsPSFs) Console.WriteLine(message ?? string.Empty);
        }

        // PsfKeyContainer is necessary because VarLenBlittableAllocator.GetKeyContainer will use the size of the full
        // composite key (KeyPointerSize * PsfCount), but the query key has only one KeyPointer.
        private class PsfQueryKeyContainer : IHeapContainer<Key>
        {
            private readonly SectorAlignedMemory mem;

            public unsafe PsfQueryKeyContainer(ref Key key, KeyAccessor<Key> keyAccessor, SectorAlignedBufferPool pool)
            {
                var len = keyAccessor.KeyPointerSize;
                this.mem = pool.Get(len);
                Buffer.MemoryCopy(Unsafe.AsPointer(ref key), mem.GetValidPointer(), len, len);
            }

            public unsafe ref Key Get() => ref Unsafe.AsRef<Key>(this.mem.GetValidPointer());

            public void Dispose() => this.mem.Return();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalReadKey<Input, Output, Context, FasterSession>(
                                    ref Key queryKeyPointerRefAsKeyRef, ref PSFReadArgs<Key, Value> psfArgs,
                                    ref PendingContext<Input, Output, Context> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx, long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // Note: This function is called only for the secondary FasterKV.
            var bucket = default(HashBucket*);
            var slot = default(int);
            var latestRecordVersion = -1;
            var heldOperation = LatchOperation.None;

            var psfInput = psfArgs.Input;
            var psfOutput = psfArgs.Output;
            ref KeyPointer<Key> queryKeyPointer = ref KeyPointer<Key>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);

            var hash = this.PsfKeyAccessor.GetHashCode64(ref queryKeyPointer);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            #region Trace back for record in in-memory HybridLog
            HashBucketEntry entry = default;
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
            OperationStatus status;

            // For PSFs, the addresses stored in the hash table point to KeyPointer entries, not the record header.
            PsfTrace($"ReadKey: {this.PsfKeyAccessor?.GetString(ref queryKeyPointer)} | hash {hash} |");
            long logicalAddress = Constants.kInvalidAddress;
            if (tagExists)
            {
                logicalAddress = entry.Address;
                PsfTrace($" {logicalAddress}");

#if false // TODOdcr: Support ReadCache in PSFs (must call this.PsfKeyAccessor.GetRecordAddressFromKeyLogicalAddress) 
                if (UseReadCache && ReadFromCache(ref queryKey, ref logicalAddress, ref physicalAddress, ref latestRecordVersion, psfInput))
                {
                    if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
                    {
                        status = OperationStatus.CPR_SHIFT_DETECTED;
                        goto CreatePendingContext; // Pivot thread
                    }
                    return psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress),
                                           ref readcache.GetValue(physicalAddress),
                                           hlog.GetInfo(physicalAddress).Tombstone, isConcurrent: false).Status;
                }
#endif

                if (logicalAddress >= hlog.HeadAddress)
                {
                    if (!ScanQueryChain(ref logicalAddress, ref queryKeyPointer, ref latestRecordVersion))
                        goto ProcessAddress;    // RECORD_ON_DISK or not found
                }
            }
            else
            {
                PsfTraceLine($" 0");
                return OperationStatus.NOTFOUND;    // no tag found
            }
#endregion

            if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
            {
                PsfTraceLine("CPR_SHIFT_DETECTED");
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot thread
            }

        #region Normal processing

        ProcessAddress:
            PsfTraceLine();
            if (logicalAddress >= hlog.HeadAddress)
            {
                // Mutable region (even fuzzy region is included here) is above SafeReadOnlyAddress and 
                // is concurrent; Immutable region will not be changed.
                long physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                long recordAddress = this.PsfKeyAccessor.GetRecordAddressFromKeyPhysicalAddress(physicalAddress);
                return psfOutput.Visit(psfInput.PsfOrdinal, physicalAddress,
                                      ref hlog.GetValue(recordAddress),
                                      hlog.GetInfo(recordAddress).Tombstone,
                                      isConcurrent: logicalAddress >= hlog.SafeReadOnlyAddress).Status;
            }

            // On-Disk Region
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;
                if (sessionCtx.phase == Phase.PREPARE)
                {
                    Debug.Assert(heldOperation != LatchOperation.Exclusive);
                    if (heldOperation == LatchOperation.Shared || HashBucket.TryAcquireSharedLatch(bucket))
                        heldOperation = LatchOperation.Shared;
                    else
                        status = OperationStatus.CPR_SHIFT_DETECTED;

                    if (RelaxedCPR) // don't hold on to shared latched during IO
                    {
                        if (heldOperation == LatchOperation.Shared)
                            HashBucket.ReleaseSharedLatch(bucket);
                        heldOperation = LatchOperation.None;
                    }
                }
                goto CreatePendingContext;
            }
            else
            {
                // No record found
                return OperationStatus.NOTFOUND;
            }

#endregion

#region Create pending context
            CreatePendingContext:
            {
                pendingContext.type = OperationType.PSF_READ_KEY;
                pendingContext.key = new PsfQueryKeyContainer(ref queryKeyPointerRefAsKeyRef, this.PsfKeyAccessor, this.hlog.bufferPool);
                pendingContext.input = default;
                pendingContext.output = default;
                pendingContext.userContext = default;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = this.PsfKeyAccessor.GetRecordAddressFromKeyLogicalAddress(logicalAddress, psfInput.PsfOrdinal);
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
                pendingContext.psfReadArgs = psfArgs;
            }
#endregion

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalReadAddress<Input, Output, Context, FasterSession>(
                                    ref PSFReadArgs<Key, Value> psfArgs,
                                    ref PendingContext<Input, Output, Context> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx, long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // Notes:
            //   - This function is called for both the primary and secondary FasterKV.
            //   - Because we are retrieving a specific address rather than looking up by key, we are not in a position
            //     to scan for a particular record version--and thus do not consider CPR boundaries, so latestRecordVersion
            //     is used only as a target for ScanQueryChain.
            // TODO: Support a variation of this that allows traversing from a start address -or- the hash table, and returns next start address.
            var latestRecordVersion = -1;

            var psfInput = psfArgs.Input;
            var psfOutput = psfArgs.Output;

            OperationStatus status;

#region Look up record in in-memory HybridLog
            // For PSFs, the addresses stored in the hash table point to KeyPointer entries, not the record header.
            long logicalAddress = psfInput.ReadLogicalAddress;
            PsfTrace($"  ReadAddr:        | {logicalAddress}");

#if false // TODOdcr: Support ReadCache in PSFs (must call this.PsfKeyAccessor.GetRecordAddressFromKeyLogicalAddress) 
            if (UseReadCache && ReadFromCache(ref logicalAddress, ref physicalAddress, ref latestRecordVersion))
            {
                if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
                {
                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    goto CreatePendingContext; // Pivot thread
                }
                return psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress), 
                                        ref readcache.GetValue(physicalAddress),
                                        hlog.GetInfo(physicalAddress).Tombstone, isConcurrent: false).Status;
            }
#endif

            if (logicalAddress >= hlog.HeadAddress)
            {
                if (this.ImplmentsPSFs && !ScanQueryChain(ref logicalAddress, ref KeyPointer<Key>.CastFromKeyRef(ref psfInput.QueryKeyRef), ref latestRecordVersion))
                { 
                    goto ProcessAddress;    // RECORD_ON_DISK or not found
                }
            }
#endregion

#region Normal processing

            ProcessAddress:
            PsfTraceLine();
            if (logicalAddress >= hlog.HeadAddress)
            {
                // Mutable region (even fuzzy region is included here) is above SafeReadOnlyAddress and 
                // is concurrent; Immutable region will not be changed.
                long physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                if (!this.ImplmentsPSFs)
                {
                    return psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress),
                                      ref hlog.GetValue(physicalAddress),
                                      hlog.GetInfo(physicalAddress).Tombstone,
                                      isConcurrent: logicalAddress >= hlog.SafeReadOnlyAddress).Status;
                }

                long recordAddress = this.PsfKeyAccessor.GetRecordAddressFromKeyPhysicalAddress(physicalAddress);
                return psfOutput.Visit(psfInput.PsfOrdinal, physicalAddress,
                                      ref hlog.GetValue(recordAddress),
                                      hlog.GetInfo(recordAddress).Tombstone,
                                      isConcurrent: logicalAddress >= hlog.SafeReadOnlyAddress).Status;
            }

            // On-Disk Region
            else if (logicalAddress >= hlog.BeginAddress)
            {
                // As mentioned above, we do not have a key here, so we do not worry about CPR and getting the hash, latching, etc.
                status = OperationStatus.RECORD_ON_DISK;
                goto CreatePendingContext;
            }
            else
            {
                // No record found. TODOerr: we should always find the LogicalAddress
                return OperationStatus.NOTFOUND;
            }

#endregion

#region Create pending context
            CreatePendingContext:
            {
                pendingContext.type = OperationType.PSF_READ_ADDRESS;
                pendingContext.key = this.ImplmentsPSFs 
                                        ? new PsfQueryKeyContainer(ref psfInput.QueryKeyRef, this.PsfKeyAccessor, this.hlog.bufferPool)
                                        : default;
                pendingContext.input = default;
                pendingContext.output = default;
                pendingContext.userContext = default;
                pendingContext.entry.word = default;
                pendingContext.logicalAddress = this.ImplmentsPSFs
                                                ? this.PsfKeyAccessor.GetRecordAddressFromKeyLogicalAddress(logicalAddress, psfInput.PsfOrdinal)
                                                : logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = LatchOperation.None;
                pendingContext.psfReadArgs = psfArgs;
            }
#endregion

            return status;
        }

        unsafe struct CASHelper
        {
            internal HashBucket* bucket;
            internal HashBucketEntry entry;
            internal long hash;
            internal int slot;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalInsert<Input, Output, Context, FasterSession>(
                        ref Key firstKeyPointerRefAsKeyRef, ref Value value, ref Input input,
                        ref PendingContext<Input, Output, Context> pendingContext,
                        FasterSession fasterSession,
                        FasterExecutionContext<Input, Output, Context> sessionCtx, long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var status = default(OperationStatus);
            var latestRecordVersion = -1;

            var psfInput = input as IPSFInput<Key>;
            ref CompositeKey<Key> compositeKey = ref CompositeKey<Key>.CastFromFirstKeyPointerRefAsKeyRef(ref firstKeyPointerRefAsKeyRef);

            // Update the KeyPointer links for chains with IsNullAt false (indicating a match with the
            // corresponding PSF) to point to the previous records for all keys in the composite key.
            // Note: We're not checking for a previous occurrence of the input value (the recordId) because
            // we are doing insert only here; the update part of upsert is done in PsfInternalUpdate.
            var psfCount = this.PsfKeyAccessor.KeyCount;
            CASHelper* casHelpers = stackalloc CASHelper[psfCount];
            PsfTrace($"Insert: {this.PsfKeyAccessor.GetString(ref compositeKey)} | rId {value} |");
            for (psfInput.PsfOrdinal = 0; psfInput.PsfOrdinal < psfCount; ++psfInput.PsfOrdinal)
            {
                // For RCU, or in case we had to retry due to CPR_SHIFT and somehow managed to delete
                // the previously found record, clear out the chain link pointer.
                this.PsfKeyAccessor.SetPrevAddress(ref compositeKey, psfInput.PsfOrdinal, Constants.kInvalidAddress);

                if (psfInput.IsNullAt)
                {
                    PsfTrace($" null");
                    continue;
                }

                ref CASHelper casHelper = ref casHelpers[psfInput.PsfOrdinal];
                casHelper.hash = this.PsfKeyAccessor.GetHashCode64(ref compositeKey, psfInput.PsfOrdinal);
                var tag = (ushort)((ulong)casHelper.hash >> Constants.kHashTagShift);

                if (sessionCtx.phase != Phase.REST)
                    HeavyEnter(casHelper.hash, sessionCtx, fasterSession);

#region Look up record in in-memory HybridLog
                FindOrCreateTag(casHelper.hash, tag, ref casHelper.bucket, ref casHelper.slot, ref casHelper.entry, hlog.BeginAddress);

                // For PSFs, the addresses stored in the hash table point to KeyPointer entries, not the record header.
                var logicalAddress = casHelper.entry.Address;
                if (logicalAddress >= hlog.BeginAddress)
                {
                    PsfTrace($" {logicalAddress}");

                    if (logicalAddress < hlog.BeginAddress)
                        continue;

                    if (logicalAddress >= hlog.HeadAddress)
                    {
                        // Note that we do not backtrace here because we are not replacing the value at the key; 
                        // instead, we insert at the top of the hash chain. Track the latest record version we've seen.
                        long physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                        var recordAddress = this.PsfKeyAccessor.GetRecordAddressFromKeyPhysicalAddress(physicalAddress);
                        if (hlog.GetInfo(physicalAddress).Tombstone)
                        {
                            // The chain might extend past a tombstoned record so we must include it in the chain
                            // unless its prevLink at psfOrdinal is invalid.
                            var prevAddress = this.PsfKeyAccessor.GetPrevAddress(physicalAddress);
                            if (prevAddress < hlog.BeginAddress)
                                continue;
                        }
                        latestRecordVersion = Math.Max(latestRecordVersion, hlog.GetInfo(recordAddress).Version);
                    }

                    this.PsfKeyAccessor.SetPrevAddress(ref compositeKey, psfInput.PsfOrdinal, logicalAddress);
                }
                else
                {
                    PsfTrace($" 0");
                }
#endregion
            }

#region Entry latch operation
            // No actual checkpoint locking will be done because this is Insert; only the current thread can write to
            // the record we're about to create, and no readers can see it until it is successfully inserted. However, we
            // must pivot and retry any insertions if we have seen a later version in any record in the hash table.
            if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
            {
                PsfTraceLine("CPR_SHIFT_DETECTED");
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot Thread
            }
            Debug.Assert(latestRecordVersion <= sessionCtx.version);
            goto CreateNewRecord;
#endregion

#region Create new record in the mutable region
            CreateNewRecord:
            {
                // Create the new record. Because we are updating multiple hash buckets, mark the record as invalid to start,
                // so it is not visible until we have successfully updated all chains.
                var recordSize = hlog.GetRecordSize(ref firstKeyPointerRefAsKeyRef, ref value);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx, fasterSession);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress), sessionCtx.version,
                                     final:true, tombstone: psfInput.IsDelete, invalidBit:true,
                                     Constants.kInvalidAddress);  // We manage all prev addresses within CompositeKey
                ref Key storedFirstKeyPointerRefAsKeyRef = ref hlog.GetKey(newPhysicalAddress);
                ref CompositeKey<Key> storedKey = ref CompositeKey<Key>.CastFromFirstKeyPointerRefAsKeyRef(ref storedFirstKeyPointerRefAsKeyRef);
                hlog.ShallowCopy(ref firstKeyPointerRefAsKeyRef, ref storedFirstKeyPointerRefAsKeyRef);
                hlog.ShallowCopy(ref value, ref hlog.GetValue(newPhysicalAddress));

                PsfTraceLine();
                newLogicalAddress += RecordInfo.GetLength();
                for (psfInput.PsfOrdinal = 0; psfInput.PsfOrdinal < psfCount; 
                    ++psfInput.PsfOrdinal, newLogicalAddress += this.PsfKeyAccessor.KeyPointerSize)
                {
                    var casHelper = casHelpers[psfInput.PsfOrdinal];
                    var tag = (ushort)((ulong)casHelper.hash >> Constants.kHashTagShift);

                    PsfTrace($"    ({psfInput.PsfOrdinal}): {casHelper.hash} {tag} | newLA {newLogicalAddress} | prev {casHelper.entry.word}");
                    if (psfInput.IsNullAt)
                    {
                        PsfTraceLine(" null");
                        continue;
                    }

                    var newEntry = default(HashBucketEntry);
                    newEntry.Tag = tag;
                    newEntry.Address = newLogicalAddress & Constants.kAddressMask;
                    newEntry.Pending = casHelper.entry.Pending;
                    newEntry.Tentative = false;

                    var foundEntry = default(HashBucketEntry);
                    while (true)
                    {
                        // If we do not succeed on the exchange, another thread has updated the slot, or we have done so
                        // with a colliding hash value from earlier in the current record. As long as we satisfy the
                        // invariant that the chain points downward (to lower addresses), we can retry.
                        foundEntry.word = Interlocked.CompareExchange(ref casHelper.bucket->bucket_entries[casHelper.slot],
                                                                      newEntry.word, casHelper.entry.word);
                        if (foundEntry.word == casHelper.entry.word)
                            break;

                        if (foundEntry.word < newEntry.word)
                        {
                            PsfTrace($" / {foundEntry.Address}");
                            casHelper.entry.word = foundEntry.word;
                            this.PsfKeyAccessor.SetPrevAddress(ref storedKey, psfInput.PsfOrdinal, foundEntry.Address);
                            continue;
                        }

                        // We can't satisfy the always-downward invariant, so leave the record marked Invalid and go
                        // around again to try inserting another record.
                        PsfTraceLine("RETRY_NOW");
                        status = OperationStatus.RETRY_NOW;
                        goto LatchRelease;
                    }

                    // Success
                    PsfTraceLine(" ins");
                    hlog.GetInfo(newPhysicalAddress).Invalid = false;
                }

                status = OperationStatus.SUCCESS;
                goto LatchRelease;
            }
#endregion

#region Create pending context
            CreatePendingContext:
            {
                psfInput.PsfOrdinal = Constants.kInvalidPsfOrdinal;

                pendingContext.type = OperationType.PSF_INSERT;
                pendingContext.key = hlog.GetKeyContainer(ref firstKeyPointerRefAsKeyRef);  // The Insert key has the full PsfCount of KeyPointers
                pendingContext.value = hlog.GetValueContainer(ref value);
                pendingContext.input = input;
                pendingContext.userContext = default;
                pendingContext.entry.word = default;
                pendingContext.logicalAddress = Constants.kInvalidAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
            }
#endregion

#region Latch release
            LatchRelease:
            // No actual latching was done.
#endregion

            return status == OperationStatus.RETRY_NOW
                ? PsfInternalInsert(ref firstKeyPointerRefAsKeyRef, ref value, ref input, ref pendingContext, fasterSession, sessionCtx, lsn)
                : status;
        }
   }
}