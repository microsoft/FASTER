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
                                           ref readcache.GetValue(physicalAddress), isConcurrent: false).Status;
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
                return hlog.GetInfo(physicalAddress).Tombstone
                    ? OperationStatus.NOTFOUND
                    : psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress), 
                                      ref hlog.GetValue(physicalAddress), isConcurrent:true).Status;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                return hlog.GetInfo(physicalAddress).Tombstone
                    ? OperationStatus.NOTFOUND
                    : psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress), 
                                      ref hlog.GetValue(physicalAddress), isConcurrent:true).Status;
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

            // TODO: For the primary FasterKV, we do not have any queryKey here to get hash and tag and do latching;
            // verify this wrt RelaxedCRT
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
                                        ref readcache.GetValue(physicalAddress), isConcurrent: false).Status;
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
                return hlog.GetInfo(physicalAddress).Tombstone
                    ? OperationStatus.NOTFOUND
                    : psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress), 
                                      ref hlog.GetValue(physicalAddress), isConcurrent: true).Status;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                return hlog.GetInfo(physicalAddress).Tombstone
                    ? OperationStatus.NOTFOUND
                    : psfOutput.Visit(psfInput.PsfOrdinal, ref hlog.GetKey(physicalAddress), 
                                      ref hlog.GetValue(physicalAddress), isConcurrent: true).Status;
            }

            // On-Disk Region
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;
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

            // Update the PSFValue links for chains with nullIndicator false (indicating a match with the
            // corresponding PSF) to point to the previous records for all keys in the composite key.
            // TODO: We're not checking for a previous occurrence of the PSFValue's recordId because
            // we are doing insert only here; the update part of upsert is done in PsfInternalRMW.
            var chainHeight = this.chainPost.ChainHeight;
            long* hashes = stackalloc long[chainHeight];
            long* chainLinkPtrs = this.chainPost.GetChainLinkPtrs(ref value);
            for (var chainLinkIdx = 0; chainLinkIdx < chainHeight; ++chainLinkIdx)
            {
                psfInput.PsfOrdinal = chainLinkIdx;
                if (psfInput.IsNullAt)
                    continue;
                long* chainLinkPtr = chainLinkPtrs + chainLinkIdx;

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

                    // TODO: is this needed? If so, verify handling for multiple keys (should abandon here and restart on retry)
                    if (UseReadCache && ReadFromCache(ref compositeKey, ref logicalAddress, ref physicalAddress, ref latestRecordVersion))
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

                    if (!hlog.GetInfo(physicalAddress).Tombstone)
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
                                     final:true, tombstone:false, invalidBit:false,
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

        internal OperationStatus PsfInternalRMW(    // TODO
                                   ref Key key, ref Input input,
                                   ref Context userContext,
                                   ref PendingContext pendingContext, FasterExecutionContext sessionCtx, long lsn)
        {
            var recordSize = default(int);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var latestRecordVersion = -1;
            var status = default(OperationStatus);
            var latchOperation = LatchOperation.None;
            var heldOperation = LatchOperation.None;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx);

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            logicalAddress = entry.Address;

            // For simplicity, we don't let RMW operations use read cache
            if (UseReadCache)
                SkipReadCache(ref logicalAddress, ref latestRecordVersion);
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.HeadAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                latestRecordVersion = hlog.GetInfo(physicalAddress).Version;

                if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key, logicalAddress,
                                            hlog.HeadAddress,
                                            out logicalAddress,
                                            out physicalAddress);
                }
            }
            #endregion

            // Optimization for the most common case
            if (sessionCtx.phase == Phase.REST && logicalAddress >= hlog.ReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                if (functions.InPlaceUpdater(ref key, ref input, ref hlog.GetValue(physicalAddress)))
                {
                    return OperationStatus.SUCCESS;
                }
            }

            #region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                switch (sessionCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            Debug.Assert(pendingContext.heldLatch != LatchOperation.Exclusive);
                            if (pendingContext.heldLatch == LatchOperation.Shared || HashBucket.TryAcquireSharedLatch(bucket))
                            {
                                // Set to release shared latch (default)
                                latchOperation = LatchOperation.Shared;
                                if (latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
                                {
                                    status = OperationStatus.CPR_SHIFT_DETECTED;
                                    goto CreateFailureContext; // Pivot Thread
                                }
                                break; // Normal Processing
                            }
                            else
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                goto CreateFailureContext; // Pivot Thread
                            }
                        }
                    case Phase.IN_PROGRESS:
                        {
                            if (latestRecordVersion != -1 && latestRecordVersion < sessionCtx.version)
                            {
                                Debug.Assert(pendingContext.heldLatch != LatchOperation.Shared);
                                if (pendingContext.heldLatch == LatchOperation.Exclusive || HashBucket.TryAcquireExclusiveLatch(bucket))
                                {
                                    // Set to release exclusive latch (default)
                                    latchOperation = LatchOperation.Exclusive;
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreateFailureContext; // Go Pending
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
                                    goto CreateFailureContext; // Go Pending
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

            #region Normal processing

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                if (FoldOverSnapshot)
                {
                    Debug.Assert(hlog.GetInfo(physicalAddress).Version == sessionCtx.version);
                }

                if (functions.InPlaceUpdater(ref key, ref input, ref hlog.GetValue(physicalAddress)))
                {
                    status = OperationStatus.SUCCESS;
                    goto LatchRelease; // Release shared latch (if acquired)
                }
            }

            // Fuzzy Region: Must go pending due to lost-update anomaly
            else if (logicalAddress >= hlog.SafeReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                status = OperationStatus.RETRY_LATER;
                // Do not retain latch for pendings ops in relaxed CPR
                if (!RelaxedCPR)
                {
                    // Retain the shared latch (if acquired)
                    if (latchOperation == LatchOperation.Shared)
                    {
                        heldOperation = latchOperation;
                        latchOperation = LatchOperation.None;
                    }
                }
                goto CreateFailureContext; // Go pending
            }

            // Safe Read-Only Region: Create a record in the mutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                goto CreateNewRecord;
            }

            // Disk Region: Need to issue async io requests
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;
                // Do not retain latch for pendings ops in relaxed CPR
                if (!RelaxedCPR)
                {
                    // Retain the shared latch (if acquired)
                    if (latchOperation == LatchOperation.Shared)
                    {
                        heldOperation = latchOperation;
                        latchOperation = LatchOperation.None;
                    }
                }
                goto CreateFailureContext; // Go pending
            }

            // No record exists - create new
            else
            {
                goto CreateNewRecord;
            }

            #endregion

            #region Create new record
            CreateNewRecord:
            {
                recordSize = (logicalAddress < hlog.BeginAddress) ?
                                hlog.GetInitialRecordSize(ref key, ref input) :
                                hlog.GetRecordSize(physicalAddress);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress), sessionCtx.version,
                               true, false, false,
                               latestLogicalAddress);
                hlog.ShallowCopy(ref key, ref hlog.GetKey(newPhysicalAddress));
                if (logicalAddress < hlog.BeginAddress)
                {
                    functions.InitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress));
                    status = OperationStatus.NOTFOUND;
                }
                else if (logicalAddress >= hlog.HeadAddress)
                {
                    if (hlog.GetInfo(physicalAddress).Tombstone)
                    {
                        functions.InitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress));
                        status = OperationStatus.NOTFOUND;
                    }
                    else
                    {
                        functions.CopyUpdater(ref key, ref input,
                                                ref hlog.GetValue(physicalAddress),
                                                ref hlog.GetValue(newPhysicalAddress));
                        status = OperationStatus.SUCCESS;
                    }
                }
                else
                {
                    // ah, old record slipped onto disk
                    hlog.GetInfo(newPhysicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    goto LatchRelease;
                }
                else
                {
                    // CAS failed
                    hlog.GetInfo(newPhysicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }
            }
            #endregion

            #region Create failure context
            CreateFailureContext:
            {
                pendingContext.type = OperationType.RMW;
                pendingContext.key = hlog.GetKeyContainer(ref key);
                pendingContext.input = input;
                pendingContext.userContext = userContext;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
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
                ? PsfInternalRMW(ref key, ref input, ref userContext, ref pendingContext, sessionCtx, lsn)
                : status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalDelete( // TODO
                            ref Key key,
                            ref Context userContext,
                            ref PendingContext pendingContext, FasterExecutionContext sessionCtx, long lsn)
        {
            var status = default(OperationStatus);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var latchOperation = default(LatchOperation);
            var version = default(int);
            var latestRecordVersion = -1;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx);

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
            if (!tagExists)
                return OperationStatus.NOTFOUND;

            logicalAddress = entry.Address;

            if (UseReadCache)
                SkipAndInvalidateReadCache(ref logicalAddress, ref latestRecordVersion, ref key);
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                if (latestRecordVersion == -1)
                    latestRecordVersion = hlog.GetInfo(physicalAddress).Version;
                if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key,
                                        logicalAddress,
                                        hlog.ReadOnlyAddress,
                                        out logicalAddress,
                                        out physicalAddress);
                }
            }
            #endregion

            // NO optimization for most common case
            //if (sessionCtx.phase == Phase.REST && logicalAddress >= hlog.ReadOnlyAddress)
            //{
            //    hlog.GetInfo(physicalAddress).Tombstone = true;
            //    return OperationStatus.SUCCESS;
            //}

            #region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                switch (sessionCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            version = sessionCtx.version;
                            if (HashBucket.TryAcquireSharedLatch(bucket))
                            {
                                // Set to release shared latch (default)
                                latchOperation = LatchOperation.Shared;
                                if (latestRecordVersion != -1 && latestRecordVersion > version)
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
                            version = (sessionCtx.version - 1);
                            if (latestRecordVersion != -1 && latestRecordVersion <= version)
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
                            version = (sessionCtx.version - 1);
                            if (latestRecordVersion != -1 && latestRecordVersion <= version)
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
                            version = (sessionCtx.version - 1);
                            if (latestRecordVersion != -1 && latestRecordVersion <= version)
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

            #region Normal processing

            // Record is in memory, try to update hash chain and completely elide record
            // only if previous address points to invalid address
            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                if (entry.Address == logicalAddress && hlog.GetInfo(physicalAddress).PreviousAddress < hlog.BeginAddress)
                {
                    var updatedEntry = default(HashBucketEntry);
                    updatedEntry.Tag = 0;
                    if (hlog.GetInfo(physicalAddress).PreviousAddress == Constants.kTempInvalidAddress)
                        updatedEntry.Address = Constants.kInvalidAddress;
                    else
                        updatedEntry.Address = hlog.GetInfo(physicalAddress).PreviousAddress;
                    updatedEntry.Pending = entry.Pending;
                    updatedEntry.Tentative = false;

                    if (entry.word == Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word))
                    {
                        // Apply tombstone bit to the record
                        hlog.GetInfo(physicalAddress).Tombstone = true;

                        if (WriteDefaultOnDelete)
                        {
                            // Write default value
                            // Ignore return value, the record is already marked
                            Value v = default;
                            functions.ConcurrentWriter(ref hlog.GetKey(physicalAddress), ref v, ref hlog.GetValue(physicalAddress));
                        }

                        status = OperationStatus.SUCCESS;
                        goto LatchRelease; // Release shared latch (if acquired)
                    }
                }
            }

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                hlog.GetInfo(physicalAddress).Tombstone = true;

                if (WriteDefaultOnDelete)
                {
                    // Write default value
                    // Ignore return value, the record is already marked
                    Value v = default;
                    functions.ConcurrentWriter(ref hlog.GetKey(physicalAddress), ref v, ref hlog.GetValue(physicalAddress));
                }

                status = OperationStatus.SUCCESS;
                goto LatchRelease; // Release shared latch (if acquired)
            }

            // All other regions: Create a record in the mutable region
            #endregion

            #region Create new record in the mutable region
            CreateNewRecord:
            {
                var value = default(Value);
                // Immutable region or new record
                // Allocate default record size for tombstone
                var recordSize = hlog.GetRecordSize(ref key, ref value);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress),
                               sessionCtx.version,
                               true, true, false,
                               latestLogicalAddress);
                hlog.ShallowCopy(ref key, ref hlog.GetKey(newPhysicalAddress));

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    status = OperationStatus.SUCCESS;
                    goto LatchRelease;
                }
                else
                {
                    hlog.GetInfo(newPhysicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }
            }
            #endregion

            #region Create pending context
            CreatePendingContext:
            {
                pendingContext.type = OperationType.DELETE;
                pendingContext.key = hlog.GetKeyContainer(ref key);
                pendingContext.userContext = userContext;
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
                ? PsfInternalDelete(ref key, ref userContext, ref pendingContext, sessionCtx, lsn)
                : status;
        }
    }
}