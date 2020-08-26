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
                logicalAddress = this.PsfKeyAccessor.GetPreviousAddress(physicalAddress);
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

        private IPSFFunctions<Key, Value, Input, Output> GetFunctions<Input, Output, Context>(ref Context context)
            => (context as PSFContext).Functions as IPSFFunctions<Key, Value, Input, Output>;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalReadKey<Input, Output, Context, FasterSession>(
                                    ref Key queryKeyPointerRefAsKeyRef, ref Input input, ref Output output, ref Context context,
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

            var functions = GetFunctions<Input, Output, Context>(ref context);
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
                    return functions.VisitReadCache(input, ref hlog.GetKey(physicalAddress),
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
                return functions.VisitSecondaryRead(ref hlog.GetValue(recordAddress), ref input, ref output, physicalAddress,
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
                pendingContext.input = input;
                pendingContext.output = output;
                pendingContext.userContext = default;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = this.PsfKeyAccessor.GetRecordAddressFromKeyPhysicalAddress(hlog.GetPhysicalAddress(logicalAddress));
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
            }
#endregion

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalReadAddress<Input, Output, Context, FasterSession>(
                                    ref Input input, ref Output output, ref Context context,
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
            OperationStatus status;
            var functions = GetFunctions<Input, Output, Context>(ref context);

            #region Look up record in in-memory HybridLog
            // For PSFs, the addresses stored in the hash table point to KeyPointer entries, not the record header.
            long logicalAddress = functions.ReadLogicalAddress(ref input);
            PsfTrace($"  ReadAddr:        | {logicalAddress}");

#if false // TODOdcr: Support ReadCache in PSFs (must call this.PsfKeyAccessor.GetRecordAddressFromKeyLogicalAddress) 
          // TODO: PsfInternalReadAddress should handle ReadCache for primary FKV
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
                if (this.ImplmentsPSFs && !ScanQueryChain(ref logicalAddress, ref KeyPointer<Key>.CastFromKeyRef(ref functions.QueryKeyRef(ref input)), ref latestRecordVersion))
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
                if (this.ImplmentsPSFs)
                {
                    long recordAddress = this.PsfKeyAccessor.GetRecordAddressFromKeyPhysicalAddress(physicalAddress);
                    return functions.VisitSecondaryRead(ref hlog.GetValue(recordAddress), ref input, ref output, physicalAddress,
                                                        hlog.GetInfo(recordAddress).Tombstone,
                                                        isConcurrent: logicalAddress >= hlog.SafeReadOnlyAddress).Status;
                }
                return functions.VisitPrimaryReadAddress(ref hlog.GetKey(physicalAddress), ref hlog.GetValue(physicalAddress),
                                                         ref output, isConcurrent: logicalAddress >= hlog.SafeReadOnlyAddress).Status;
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
                // No record found. TODOerr: we should not have called this function in this case.
                return OperationStatus.NOTFOUND;
            }

#endregion

#region Create pending context
            CreatePendingContext:
            {
                pendingContext.type = OperationType.PSF_READ_ADDRESS;
                pendingContext.key = this.ImplmentsPSFs 
                                        ? new PsfQueryKeyContainer(ref functions.QueryKeyRef(ref input), this.PsfKeyAccessor, this.hlog.bufferPool)
                                        : default;
                pendingContext.input = input;
                pendingContext.output = output;
                pendingContext.userContext = default;
                pendingContext.entry.word = default;
                pendingContext.logicalAddress = this.ImplmentsPSFs
                                                ? this.PsfKeyAccessor.GetRecordAddressFromKeyPhysicalAddress(hlog.GetPhysicalAddress(logicalAddress))
                                                : logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = LatchOperation.None;
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
            internal bool isNull;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalInsert<Input, Output, Context, FasterSession>(
                        ref Key firstKeyPointerRefAsKeyRef, ref Value value, ref Input input, ref Context context,
                        ref PendingContext<Input, Output, Context> pendingContext,
                        FasterSession fasterSession,
                        FasterExecutionContext<Input, Output, Context> sessionCtx, long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var status = default(OperationStatus);
            var latestRecordVersion = -1;

            var functions = GetFunctions<Input, Output, Context>(ref context);
            ref CompositeKey<Key> compositeKey = ref CompositeKey<Key>.CastFromFirstKeyPointerRefAsKeyRef(ref firstKeyPointerRefAsKeyRef);

            // Update the KeyPointer links for chains with IsNullAt false (indicating a match with the
            // corresponding PSF) to point to the previous records for all keys in the composite key.
            // Note: We're not checking for a previous occurrence of the input value (the recordId) because
            // we are doing insert only here; the update part of upsert is done in PsfInternalUpdate.
            var psfCount = this.PsfKeyAccessor.KeyCount;
            CASHelper* casHelpers = stackalloc CASHelper[psfCount];
            int startOfKeysOffset = 0;
            PsfTrace($"Insert: {this.PsfKeyAccessor.GetString(ref compositeKey)} | rId {value} |");
            for (var psfOrdinal = 0; psfOrdinal < psfCount; ++psfOrdinal)
            {
                // For RCU, or in case we had to retry due to CPR_SHIFT and somehow managed to delete
                // the previously found record, clear out the chain link pointer.
                this.PsfKeyAccessor.SetPreviousAddress(ref compositeKey, psfOrdinal, Constants.kInvalidAddress);

                this.PsfKeyAccessor.SetOffsetToStartOfKeys(ref compositeKey, psfOrdinal, startOfKeysOffset);
                startOfKeysOffset += this.PsfKeyAccessor.KeyPointerSize;

                ref CASHelper casHelper = ref casHelpers[psfOrdinal];
                if (this.PsfKeyAccessor.IsNullAt(ref compositeKey, psfOrdinal))
                {
                    casHelper.isNull = true;
                    PsfTrace($" null");
                    continue;
                }

                casHelper.hash = this.PsfKeyAccessor.GetHashCode64(ref compositeKey, psfOrdinal);
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
                            var prevAddress = this.PsfKeyAccessor.GetPreviousAddress(physicalAddress);
                            if (prevAddress < hlog.BeginAddress)
                                continue;
                        }
                        latestRecordVersion = Math.Max(latestRecordVersion, hlog.GetInfo(recordAddress).Version);
                    }

                    this.PsfKeyAccessor.SetPreviousAddress(ref compositeKey, psfOrdinal, logicalAddress);
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
                                     final:true, tombstone: functions.IsDelete(ref input), invalidBit:true,
                                     Constants.kInvalidAddress);  // We manage all prev addresses within CompositeKey
                ref Key storedFirstKeyPointerRefAsKeyRef = ref hlog.GetKey(newPhysicalAddress);
                ref CompositeKey<Key> storedKey = ref CompositeKey<Key>.CastFromFirstKeyPointerRefAsKeyRef(ref storedFirstKeyPointerRefAsKeyRef);
                hlog.ShallowCopy(ref firstKeyPointerRefAsKeyRef, ref storedFirstKeyPointerRefAsKeyRef);
                hlog.ShallowCopy(ref value, ref hlog.GetValue(newPhysicalAddress));

                PsfTraceLine();
                newLogicalAddress += RecordInfo.GetLength();
                for (var psfOrdinal = 0; psfOrdinal < psfCount; ++psfOrdinal, newLogicalAddress += this.PsfKeyAccessor.KeyPointerSize)
                {
                    var casHelper = casHelpers[psfOrdinal];
                    var tag = (ushort)((ulong)casHelper.hash >> Constants.kHashTagShift);

                    PsfTrace($"    ({psfOrdinal}): {casHelper.hash} {tag} | newLA {newLogicalAddress} | prev {casHelper.entry.word}");
                    if (casHelper.isNull)
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
                            this.PsfKeyAccessor.SetPreviousAddress(ref storedKey, psfOrdinal, foundEntry.Address);
                            continue;
                        }

                        // We can't satisfy the always-downward invariant, so leave the record marked Invalid and go
                        // around again to try inserting another record.
                        PsfTraceLine("RETRY_NOW");
                        status = OperationStatus.RETRY_NOW;
                        goto LatchRelease;
                    }

                    // Success for this PSF.
                    PsfTraceLine(" ins");
                    hlog.GetInfo(newPhysicalAddress).Invalid = false;
                }

                storedKey.ClearUpdateFlags(this.PsfKeyAccessor.KeyCount, this.PsfKeyAccessor.KeyPointerSize);
                status = OperationStatus.SUCCESS;
                goto LatchRelease;
            }
#endregion

#region Create pending context
            CreatePendingContext:
            {
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
                ? PsfInternalInsert(ref firstKeyPointerRefAsKeyRef, ref value, ref input, ref context, ref pendingContext, fasterSession, sessionCtx, lsn)
                : status;
        }
   }
}