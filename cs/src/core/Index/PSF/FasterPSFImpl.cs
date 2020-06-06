// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    // PSF-related internal function implementations for FasterKV; these correspond to the similarly-named
    // functions in FasterImpl.cs.
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions>
        : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal IChainPost<Value> chainPost;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalReadKey(
                                    ref Key queryKey, ref PSFReadArgs<Key, Value> psfArgs,
                                    ref PendingContext pendingContext, FasterExecutionContext sessionCtx, long lsn)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var physicalAddress = default(long);
            var latestRecordVersion = -1;
            var heldOperation = LatchOperation.None;

            var psfInput = psfArgs.Input;
            var psfOutput = psfArgs.Output;

            var hash = psfInput.GetHashCode64At(ref queryKey);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx);

            #region Trace back for record in in-memory HybridLog
            HashBucketEntry entry = default;
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
            OperationStatus status;
            long logicalAddress;
            if (tagExists)
            {
                logicalAddress = entry.Address;

                if (UseReadCache && ReadFromCache(ref queryKey, ref logicalAddress, ref physicalAddress,  ref latestRecordVersion, psfInput))
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

                if (logicalAddress >= hlog.HeadAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                    if (latestRecordVersion == -1)
                        latestRecordVersion = hlog.GetInfo(physicalAddress).Version;

                    if (!psfInput.EqualsAt(ref queryKey, ref hlog.GetKey(physicalAddress)))
                    {
                        logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                        TraceBackForKeyMatch(ref queryKey,
                                                logicalAddress,
                                                hlog.HeadAddress,
                                                out logicalAddress,
                                                out physicalAddress,
                                                psfInput);
                    }
                }
            }
            else
            {
                // no tag found
                return OperationStatus.NOTFOUND;
            }
            #endregion

            if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
            {
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot thread
            }

            #region Normal processing

            // Mutable region (even fuzzy region is included here)
            if (logicalAddress >= hlog.SafeReadOnlyAddress)
            {
                return psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress), 
                                      ref hlog.GetValue(physicalAddress),
                                      hlog.GetInfo(physicalAddress).Tombstone, isConcurrent:true).Status;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                return psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress),
                                      ref hlog.GetValue(physicalAddress),
                                      hlog.GetInfo(physicalAddress).Tombstone, isConcurrent: true).Status;
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
                pendingContext.key = hlog.GetKeyContainer(ref queryKey);
                pendingContext.input = default;
                pendingContext.output = default;
                pendingContext.userContext = default;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
                pendingContext.psfReadArgs = psfArgs;
            }
            #endregion

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalReadAddress(
                                    ref PSFReadArgs<Key, Value> psfArgs,
                                    ref PendingContext pendingContext, FasterExecutionContext sessionCtx, long lsn)
        {
            var physicalAddress = default(long);
            var latestRecordVersion = -1;

            var psfInput = psfArgs.Input;
            var psfOutput = psfArgs.Output;

            // TODO: Verify we should always find the LogicalAddress
            OperationStatus status;

            #region Look up record in in-memory HybridLog
            long logicalAddress = psfInput.ReadLogicalAddress;
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

            if (logicalAddress >= hlog.HeadAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                if (latestRecordVersion == -1)
                    latestRecordVersion = hlog.GetInfo(physicalAddress).Version;
            }
            #endregion

            if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
            {
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot thread
            }

            #region Normal processing

            // Mutable region (even fuzzy region is included here)
            if (logicalAddress >= hlog.SafeReadOnlyAddress)
            {
                return psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress), 
                                      ref hlog.GetValue(physicalAddress),
                                      hlog.GetInfo(physicalAddress).Tombstone, isConcurrent: true).Status;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                return psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress), 
                                      ref hlog.GetValue(physicalAddress),
                                      hlog.GetInfo(physicalAddress).Tombstone, isConcurrent: true).Status;
            }

            // On-Disk Region
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;
#if false // TODO: See above and discussion in Teams/email; need to get the key here so we can lock the hash etc.
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
#endif
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
                pendingContext.type = OperationType.PSF_READ_ADDRESS;
                pendingContext.key = default;
                pendingContext.input = default;
                pendingContext.output = default;
                pendingContext.userContext = default;
                pendingContext.entry.word = default;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = LatchOperation.None;
                pendingContext.psfReadArgs = psfArgs;
            }
#endregion

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalInsert(
                        ref Key compositeKey, ref Value value, ref Input input,
                        ref PendingContext pendingContext, FasterExecutionContext sessionCtx, long lsn)
        {
            var status = default(OperationStatus);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var latchOperation = default(LatchOperation);
            var latestRecordVersion = -1;
            var entry = default(HashBucketEntry);

            var psfInput = input as IPSFInput<Key>;

            // Update the PSFValue links for chains with IsNullAt false (indicating a match with the
            // corresponding PSF) to point to the previous records for all keys in the composite key.
            // Note: We're not checking for a previous occurrence of the PSFValue's recordId because
            // we are doing insert only here; the update part of upsert is done in PsfInternalUpdate.
            var chainHeight = this.chainPost.ChainHeight;
            long* hashes = stackalloc long[chainHeight];
            long* chainLinkPtrs = this.chainPost.GetChainLinkPtrs(ref value);
            for (var chainLinkIdx = 0; chainLinkIdx < chainHeight; ++chainLinkIdx)
            {
                // For RCU, or in case we had to retry due to CPR_SHIFT and somehow managed to delete
                // the previously found record, clear out the chain link pointer.
                long* chainLinkPtr = chainLinkPtrs + chainLinkIdx;
                *chainLinkPtr = Constants.kInvalidAddress;

                psfInput.PsfOrdinal = chainLinkIdx;
                if (psfInput.IsNullAt)
                    continue;

                var hash = psfInput.GetHashCode64At(ref compositeKey);
                *(hashes + chainLinkIdx) = hash;
                var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                if (sessionCtx.phase != Phase.REST)
                    HeavyEnter(hash, sessionCtx);

#region Trace back for record in in-memory HybridLog
                entry = default;
                var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
                if (tagExists)
                {
                    logicalAddress = entry.Address;

                    // TODO: Test this: If this fails for any TPSFKey in the composite key, we'll create the pending
                    // context and come back here on the retry and overwrite any previously-obtained logicalAddress
                    // at the chainLinkPtr.
                    if (UseReadCache && ReadFromCache(ref compositeKey, ref logicalAddress, ref physicalAddress,
                                                      ref latestRecordVersion, psfInput))
                    {
                        if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            goto CreatePendingContext; // Pivot thread
                        }
                    }

                    if (logicalAddress >= hlog.HeadAddress)
                    {
                        physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                        if (latestRecordVersion == -1)
                            latestRecordVersion = hlog.GetInfo(physicalAddress).Version;

                        if (!psfInput.EqualsAt(ref compositeKey, ref hlog.GetKey(physicalAddress)))
                        {
                            logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                            TraceBackForKeyMatch(ref compositeKey,
                                                 logicalAddress,
                                                 hlog.HeadAddress,
                                                 out logicalAddress,
                                                 out physicalAddress,
                                                 psfInput);
                        }
                    }

                    if (hlog.GetInfo(physicalAddress).Tombstone)
                    {
                        // The chain might extend past a tombstoned record so we must include it in the chain
                        // unless its link at chainLinkIdx is kInvalidAddress.
                        long* prevLinks = this.chainPost.GetChainLinkPtrs(ref hlog.GetValue(physicalAddress));
                        long* prevLink = prevLinks + chainLinkIdx;
                        if (*prevLink == Constants.kInvalidAddress)
                            continue;
                    }
                    *chainLinkPtr = logicalAddress;
                }
#endregion
            }

#region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                switch (sessionCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            if (HashBucket.TryAcquireSharedLatch(bucket))
                            {
                                // Set to release shared latch (default)
                                latchOperation = LatchOperation.Shared;
                                if (latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
                                {
                                    status = OperationStatus.CPR_SHIFT_DETECTED;
                                    goto CreatePendingContext; // Pivot Thread
                                }
                                break; // Normal Processing
                            }
                            else
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                goto CreatePendingContext; // Pivot Thread
                            }
                        }
                    case Phase.IN_PROGRESS:
                        {
                            if (latestRecordVersion != -1 && latestRecordVersion < sessionCtx.version)
                            {
                                if (HashBucket.TryAcquireExclusiveLatch(bucket))
                                {
                                    // Set to release exclusive latch (default)
                                    latchOperation = LatchOperation.Exclusive;
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreatePendingContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_PENDING:
                        {
                            if (latestRecordVersion != -1 && latestRecordVersion < sessionCtx.version)
                            {
                                if (HashBucket.NoSharedLatches(bucket))
                                {
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreatePendingContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            if (latestRecordVersion != -1 && latestRecordVersion < sessionCtx.version)
                            {
                                goto CreateNewRecord; // Create a (v+1) record
                            }
                            break; // Normal Processing
                        }
                    default:
                        break;
                }
            }
#endregion

            Debug.Assert(latestRecordVersion <= sessionCtx.version);

#region Create new record in the mutable region
            CreateNewRecord:
            {
                // Immutable region or new record
                var recordSize = hlog.GetRecordSize(ref compositeKey, ref value);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress), sessionCtx.version,
                                     final:true, tombstone: psfInput.IsDelete, invalidBit:false,
                                     Constants.kInvalidAddress);  // We manage all prev addresses within PSFValue
                hlog.ShallowCopy(ref compositeKey, ref hlog.GetKey(newPhysicalAddress));
                functions.SingleWriter(ref compositeKey, ref value, ref hlog.GetValue(newPhysicalAddress));

                for (var chainLinkIdx = 0; chainLinkIdx < chainHeight; ++chainLinkIdx)
                {
                    psfInput.PsfOrdinal = chainLinkIdx;
                    if (psfInput.IsNullAt)
                        continue;

                    var hash = *(hashes + chainLinkIdx);
                    var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);
                    entry = default;
                    FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);

                    var updatedEntry = default(HashBucketEntry);
                    updatedEntry.Tag = tag;
                    updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                    updatedEntry.Pending = entry.Pending;
                    updatedEntry.Tentative = false;

                    var foundEntry = default(HashBucketEntry);
                    foundEntry.word = Interlocked.CompareExchange(ref bucket->bucket_entries[slot],
                                                                  updatedEntry.word, entry.word);

                    if (foundEntry.word != entry.word)
                    {
                        hlog.GetInfo(newPhysicalAddress).Invalid = true;
                        status = OperationStatus.RETRY_NOW;
                        goto LatchRelease;
                    }
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
                pendingContext.key = hlog.GetKeyContainer(ref compositeKey);
                pendingContext.value = hlog.GetValueContainer(ref value);
                pendingContext.input = input;
                pendingContext.userContext = default;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
            }
            #endregion

            #region Latch release
            LatchRelease:
            {
                switch (latchOperation)
                {
                    case LatchOperation.Shared:
                        HashBucket.ReleaseSharedLatch(bucket);
                        break;
                    case LatchOperation.Exclusive:
                        HashBucket.ReleaseExclusiveLatch(bucket);
                        break;
                    default:
                        break;
                }
            }
            #endregion

            return status == OperationStatus.RETRY_NOW
                ? PsfInternalInsert(ref compositeKey, ref value, ref input, ref pendingContext, sessionCtx, lsn)
                : status;
        }
    }
}