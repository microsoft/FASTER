// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define CPR

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// This is a wrapper for checking the record's version instead of just peeking at the latest record at the tail of the bucket.
        /// By calling with the address of the traced record, we can prevent a different key sharing the same bucket from deceiving 
        /// the operation to think that the version of the key has reached v+1 and thus to incorrectly update in place.
        /// </summary>
        /// <param name="logicalAddress">The logical address of the traced record for the key</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CheckEntryVersionNew(long logicalAddress)
        {
            HashBucketEntry entry = default;
            entry.word = logicalAddress;
            return CheckBucketVersionNew(ref entry);
        }

        /// <summary>
        /// Check the version of the passed-in entry. 
        /// The semantics of this function are to check the tail of a bucket (indicated by entry), so we name it this way.
        /// </summary>
        /// <param name="entry">the last entry of a bucket</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CheckBucketVersionNew(ref HashBucketEntry entry)
        {
            // A version shift can only in an address after the checkpoint starts, as v_new threads RCU entries to the tail.
            if (entry.Address < _hybridLogCheckpoint.info.startLogicalAddress) return false;

            // Read cache entries are not in new version
            if (UseReadCache && entry.ReadCache) return false;

            // Check if record has the new version bit set
            var _addr = hlog.GetPhysicalAddress(entry.Address);
            if (entry.Address >= hlog.HeadAddress)
                return hlog.GetInfo(_addr).InNewVersion;
            else
                return false;
        }

        internal enum LatchOperation : byte
        {
            None,
            Shared,
            Exclusive
        }

        #region Read Operation

        /// <summary>
        /// Read operation. Computes the 'output' from 'input' and current value corresponding to 'key'.
        /// When the read operation goes pending, once the record is retrieved from disk, InternalContinuePendingRead
        /// function is used to complete the operation.
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="input">Input required to compute output from value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="startAddress">If not Constants.kInvalidAddress, this is the address to start at instead of a hash table lookup</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The output has been computed using current value of 'key' and 'input'; and stored in 'output'.</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk and the operation.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRead<Input, Output, Context, FasterSession>(
                                    ref Key key,
                                    ref Input input,
                                    ref Output output,
                                    long startAddress,
                                    ref Context userContext,
                                    ref PendingContext<Input, Output, Context> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx,
                                    long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var physicalAddress = default(long);
            var heldOperation = LatchOperation.None;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            #region Trace back for record in in-memory HybridLog
            HashBucketEntry entry = default;

            OperationStatus status;
            long logicalAddress;
            long prevTailAddress = hlog.GetTailAddress();
            var useStartAddress = startAddress != Constants.kInvalidAddress && !pendingContext.HasMinAddress;
            bool tagExists;
            if (!useStartAddress)
            {
                tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry) && entry.Address >= pendingContext.minAddress;
            }
            else
            {
                tagExists = startAddress >= hlog.BeginAddress;
                entry.Address = startAddress;
            }

            if (tagExists)
            {
                logicalAddress = entry.Address;

                if (UseReadCache)
                {
                    if (pendingContext.SkipReadCache || pendingContext.NoKey)
                    {
                        SkipReadCache(ref logicalAddress, out _);
                    }
                    else if (ReadFromCache(ref key, ref logicalAddress, ref physicalAddress, out status))
                    {
                        // When session is in PREPARE phase, a read-cache record cannot be new-version.
                        // This is because a new-version record insertion would have elided the read-cache entry.
                        // and before the new-version record can go to disk become eligible to enter the read-cache,
                        // the PREPARE phase for that session will be over due to an epoch refresh.

                        // This is not called when looking up by address, so we do not set pendingContext.recordInfo.
                        // ReadCache addresses are not valid for indexing etc. so pass kInvalidAddress.
                        ref RecordInfo recordInfo = ref readcache.GetInfo(physicalAddress);
                        pendingContext.recordInfo = recordInfo;
                        return fasterSession.SingleReader(ref key, ref input, ref readcache.GetValue(physicalAddress), ref output, ref pendingContext.lockOperation, ref recordInfo, Constants.kInvalidAddress)
                            ? OperationStatus.SUCCESS : OperationStatus.NOTFOUND;
                    }
                    else if (status != OperationStatus.SUCCESS)
                        return status;
                }

                if (logicalAddress >= hlog.HeadAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                    if (!pendingContext.NoKey)
                    {
                        if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                        {
                            logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                            TraceBackForKeyMatch(ref key,
                                                    logicalAddress,
                                                    hlog.HeadAddress,
                                                    out logicalAddress,
                                                    out physicalAddress);
                        }
                    } else
                    {
                        // If NoKey, we do not have the key in the call and must use the key from the record.
                        key = ref hlog.GetKey(physicalAddress);
                    }
                }
            }
            else
            {
                // no tag found
                return OperationStatus.NOTFOUND;
            }
            #endregion

            if (sessionCtx.phase == Phase.PREPARE && CheckBucketVersionNew(ref entry))
            {
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot thread
            }

#region Normal processing

            // Mutable region (even fuzzy region is included here)
            if (logicalAddress >= hlog.SafeReadOnlyAddress)
            {
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                pendingContext.recordInfo = recordInfo;
                pendingContext.logicalAddress = logicalAddress;
                ref Value recordValue = ref hlog.GetValue(physicalAddress);

                if (recordInfo.IsIntermediate(out status, useStartAddress))
                {
                    return status;
                }
                else if (!recordInfo.Tombstone
                        && fasterSession.ConcurrentReader(ref key, ref input, ref recordValue, ref output, ref pendingContext.lockOperation, ref recordInfo, logicalAddress))
                {
                    return OperationStatus.SUCCESS;
                }
                return OperationStatus.NOTFOUND;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                pendingContext.recordInfo = recordInfo;
                pendingContext.logicalAddress = logicalAddress;

                if (recordInfo.IsIntermediate(out status, useStartAddress))
                {
                    return status;
                }
                else if (!recordInfo.Tombstone
                        && fasterSession.SingleReader(ref key, ref input, ref hlog.GetValue(physicalAddress), ref output, ref pendingContext.lockOperation, ref recordInfo, logicalAddress))
                {
                    if (CopyReadsToTail == CopyReadsToTail.FromReadOnly && !pendingContext.SkipCopyReadsToTail)
                    {
                        var container = hlog.GetValueContainer(ref hlog.GetValue(physicalAddress));
                        InternalTryCopyToTail(sessionCtx, ref pendingContext, ref key, ref input, ref container.Get(), ref output, ref pendingContext.lockOperation, logicalAddress, fasterSession, sessionCtx);
                        container.Dispose();
                    }
                    return OperationStatus.SUCCESS;
                }
                return OperationStatus.NOTFOUND;
            }

            // On-Disk Region
            else if (logicalAddress >= hlog.BeginAddress)
            {
                if (hlog.IsNullDevice)
                    return OperationStatus.NOTFOUND;

                status = OperationStatus.RECORD_ON_DISK;
                if (sessionCtx.phase == Phase.PREPARE)
                {
                    Debug.Assert(heldOperation != LatchOperation.Exclusive);
                    if (useStartAddress)
                    {
                        Debug.Assert(heldOperation == LatchOperation.None);
                    }
                    else if (heldOperation == LatchOperation.Shared || HashBucket.TryAcquireSharedLatch(bucket))
                    {
                        heldOperation = LatchOperation.Shared;
                    }
                    else
                    {
                        status = OperationStatus.CPR_SHIFT_DETECTED;
                    }

                    if (RelaxedCPR) // don't hold on to shared latched during IO
                    {
                        if (heldOperation == LatchOperation.Shared)
                            HashBucket.ReleaseSharedLatch(bucket);
                        heldOperation = LatchOperation.None;
                    }
                }

                goto CreatePendingContext;
            }

            // No record found
            else
            {
                return OperationStatus.NOTFOUND;
            }

#endregion

#region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.READ;
                if (!pendingContext.NoKey && pendingContext.key == default)    // If this is true, we don't have a valid key
                    pendingContext.key = hlog.GetKeyContainer(ref key);
                if (pendingContext.input == default) pendingContext.input = fasterSession.GetHeapContainer(ref input);

                pendingContext.output = output;
                if (pendingContext.output is IHeapConvertible heapConvertible)
                    heapConvertible.ConvertToHeap();

                pendingContext.userContext = userContext;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
                pendingContext.recordInfo.PreviousAddress = prevTailAddress;
            }
#endregion

            return status;
        }
        #endregion

        #region Upsert Operation

        private enum LatchDestination
        {
            CreateNewRecord,
            CreatePendingContext,
            NormalProcessing
        }

        /// <summary>
        /// Upsert operation. Replaces the value corresponding to 'key' with provided 'value', if one exists 
        /// else inserts a new record with 'key' and 'value'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="value">value to be updated to (or inserted if key does not exist).</param>
        /// <param name="output">output where the result of the update can be placed</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully replaced(or inserted)</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalUpsert<Input, Output, Context, FasterSession>(
                            ref Key key, ref Input input, ref Value value, ref Output output,
                            ref Context userContext,
                            ref PendingContext<Input, Output, Context> pendingContext,
                            FasterSession fasterSession,
                            FasterExecutionContext<Input, Output, Context> sessionCtx,
                            long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var status = default(OperationStatus);
            var latchOperation = LatchOperation.None;
            var latchDestination = LatchDestination.NormalProcessing;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

#region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            var logicalAddress = entry.Address;
            var physicalAddress = default(long);

            long lastReadCachePhysicalAddress = Constants.kInvalidAddress;
            long prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
            if (UseReadCache)
            {
                prevFirstReadCacheLogicalAddress = logicalAddress;
                SkipReadCache(ref logicalAddress, out lastReadCachePhysicalAddress);
                if (prevFirstReadCacheLogicalAddress == logicalAddress) // if there were no readcache records
                    prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
            }
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
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

            // Optimization for the most common case
            long unsealPhysicalAddress = Constants.kInvalidAddress;
            if (sessionCtx.phase == Phase.REST)
            {
                if (logicalAddress >= hlog.ReadOnlyAddress)
                {
                    ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                    ref Value recordValue = ref hlog.GetValue(physicalAddress);
                    if (recordInfo.IsIntermediate(out status))
                        return status;

                    if (!recordInfo.Tombstone)
                    {
                        if (fasterSession.ConcurrentWriter(ref key, ref input, ref value, ref hlog.GetValue(physicalAddress), ref output, ref pendingContext.lockOperation, ref recordInfo, logicalAddress))
                        {
                            hlog.MarkPage(logicalAddress, sessionCtx.version);
                            pendingContext.recordInfo = recordInfo;
                            pendingContext.logicalAddress = logicalAddress;
                            return OperationStatus.SUCCESS;
                        }

                        // ConcurrentWriter failed (e.g. insufficient space). Another thread may come along to do this update in-place; Seal it to prevent that.
                        if (!recordInfo.Seal(fasterSession.IsManualLocking))
                            return OperationStatus.RETRY_NOW;
                        unsealPhysicalAddress = physicalAddress;
                    }
                    goto CreateNewRecord;
                }
            }

#region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                latchDestination = AcquireLatchUpsert(sessionCtx, bucket, ref status, ref latchOperation, ref entry, logicalAddress);
            }
            #endregion

            #region Normal processing

            // Mutable Region: Update the record in-place
            if (latchDestination == LatchDestination.NormalProcessing)
            {
                if (logicalAddress >= hlog.ReadOnlyAddress)
                {
                    ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                    ref Value recordValue = ref hlog.GetValue(physicalAddress);
                    if (recordInfo.IsIntermediate(out status))
                    {
                        goto LatchRelease; // Release shared latch (if acquired)
                    }

                    if (!recordInfo.Tombstone)
                    {
                        if (fasterSession.ConcurrentWriter(ref key, ref input, ref value, ref recordValue, ref output, ref pendingContext.lockOperation, ref recordInfo, logicalAddress))
                        {
                            if (sessionCtx.phase == Phase.REST)
                                hlog.MarkPage(logicalAddress, sessionCtx.version);
                            else
                                hlog.MarkPageAtomic(logicalAddress, sessionCtx.version);
                            pendingContext.recordInfo = recordInfo;
                            pendingContext.logicalAddress = logicalAddress;
                            status = OperationStatus.SUCCESS;
                            goto LatchRelease; // Release shared latch (if acquired)
                        }

                        // ConcurrentWriter failed (e.g. insufficient space). Another thread may come along to do this update in-place; Seal it to prevent that.
                        if (!recordInfo.Seal(fasterSession.IsManualLocking))
                        {
                            status = OperationStatus.RETRY_NOW;
                            goto LatchRelease; // Release shared latch (if acquired)
                        }
                        unsealPhysicalAddress = physicalAddress;
                        goto CreateNewRecord;
                    }
                }
                else if (logicalAddress >= hlog.HeadAddress)
                {
                    // Only need to go below ReadOnly here for locking and Sealing.
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                    ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                    pendingContext.recordInfo = recordInfo;
                    pendingContext.logicalAddress = logicalAddress;

                    if (recordInfo.IsIntermediate(out status))
                        goto LatchRelease; // Release shared latch (if acquired)
                    if (!recordInfo.Seal(fasterSession.IsManualLocking))
                    {
                        status = OperationStatus.RETRY_NOW;
                        goto LatchRelease; // Release shared latch (if acquired)
                    }
                    unsealPhysicalAddress = physicalAddress;
                    goto CreateNewRecord;
                }
            }

        // All other regions: Create a record in the mutable region
#endregion

#region Create new record in the mutable region
        CreateNewRecord:
            // Invalidate the entry in the read cache, as we did not do IPU.
            if (UseReadCache)
            {
                prevFirstReadCacheLogicalAddress = logicalAddress;
                if (!SkipAndInvalidateReadCache(ref logicalAddress, ref key, out lastReadCachePhysicalAddress, out OperationStatus internalStatus))
                    return internalStatus;
            }

            if (latchDestination != LatchDestination.CreatePendingContext)
            {
                // Immutable region or new record
                status = CreateNewRecordUpsert(ref key, ref input, ref value, ref output, ref pendingContext, fasterSession, sessionCtx, bucket, slot, tag, entry,
                                               latestLogicalAddress, prevFirstReadCacheLogicalAddress, lastReadCachePhysicalAddress, unsealPhysicalAddress);
                if (status != OperationStatus.SUCCESS)
                {
                    if (unsealPhysicalAddress != Constants.kInvalidAddress)
                    {
                        // Operation failed, so unseal the old record.
                        hlog.GetInfo(unsealPhysicalAddress).Unseal();
                    }
                    if (status == OperationStatus.ALLOCATE_FAILED)
                    {
                        latchDestination = LatchDestination.CreatePendingContext;
                        goto CreatePendingContext;
                    }
                }
                goto LatchRelease;
            }
            #endregion

            #region Create pending context
            CreatePendingContext:
            Debug.Assert(latchDestination == LatchDestination.CreatePendingContext, $"Upsert CreatePendingContext encountered latchDest == {latchDestination}");
            {
                pendingContext.type = OperationType.UPSERT;
                if (pendingContext.key == default) pendingContext.key = hlog.GetKeyContainer(ref key);
                if (pendingContext.input == default) pendingContext.input = fasterSession.GetHeapContainer(ref input);
                if (pendingContext.value == default) pendingContext.value = hlog.GetValueContainer(ref value);

                pendingContext.output = output;
                if (pendingContext.output is IHeapConvertible heapConvertible)
                    heapConvertible.ConvertToHeap();

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

            return status;
        }

        private LatchDestination AcquireLatchUpsert<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> sessionCtx, HashBucket* bucket, ref OperationStatus status, 
                                                                            ref LatchOperation latchOperation, ref HashBucketEntry entry, long logicalAddress)
        {
            switch (sessionCtx.phase)
            {
                case Phase.PREPARE:
                    {
                        if (HashBucket.TryAcquireSharedLatch(bucket))
                        {
                            // Set to release shared latch (default)
                            latchOperation = LatchOperation.Shared;
                            // Here (and in InternalRead, AcquireLatchRMW, and InternalDelete) we still check the tail record of the bucket (entry.Address)
                            // rather than the traced record (logicalAddress), because I'm worried that the implementation
                            // may not allow in-place updates for version v when the bucket arrives v+1. 
                            // This is safer but potentially unnecessary.
                            if (CheckBucketVersionNew(ref entry))
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                return LatchDestination.CreatePendingContext; // Pivot Thread
                            }
                            break; // Normal Processing
                        }
                        else
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            return LatchDestination.CreatePendingContext; // Pivot Thread
                        }
                    }
                case Phase.IN_PROGRESS:
                    {
                        if (!CheckEntryVersionNew(logicalAddress))
                        {
                            if (HashBucket.TryAcquireExclusiveLatch(bucket))
                            {
                                // Set to release exclusive latch (default)
                                latchOperation = LatchOperation.Exclusive;
                                return LatchDestination.CreateNewRecord; // Create a (v+1) record
                            }
                            else
                            {
                                status = OperationStatus.RETRY_LATER;
                                return LatchDestination.CreatePendingContext; // Go Pending
                            }
                        }
                        break; // Normal Processing
                    }
                case Phase.WAIT_FLUSH:
                    {
                        if (!CheckEntryVersionNew(logicalAddress))
                        {
                            return LatchDestination.CreateNewRecord; // Create a (v+1) record
                        }
                        break; // Normal Processing
                    }
                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        private OperationStatus CreateNewRecordUpsert<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Value value, ref Output output, ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                                                                             FasterExecutionContext<Input, Output, Context> sessionCtx, HashBucket* bucket, int slot, ushort tag, HashBucketEntry entry,
                                                                                             long latestLogicalAddress, long prevFirstReadCacheLogicalAddress, long lastReadCachePhysicalAddress, long unsealPhysicalAddress) 
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var (actualSize, allocateSize) = hlog.GetRecordSize(ref key, ref value);
            BlockAllocate(allocateSize, out long newLogicalAddress, sessionCtx, fasterSession, pendingContext.IsAsync);
            if (newLogicalAddress == 0)
                return OperationStatus.ALLOCATE_FAILED;
            var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
            ref RecordInfo recordInfo = ref hlog.GetInfo(newPhysicalAddress);
            RecordInfo.WriteInfo(ref recordInfo,
                           inNewVersion: sessionCtx.InNewVersion,
                           tombstone: false, dirty: true,
                           latestLogicalAddress);
            recordInfo.Tentative = true;
            hlog.Serialize(ref key, newPhysicalAddress);
            ref Value newValue = ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize);

            fasterSession.SingleWriter(ref key, ref input, ref value, ref newValue, ref output, ref pendingContext.lockOperation, ref recordInfo, newLogicalAddress);

            bool success = true;
            if (lastReadCachePhysicalAddress == Constants.kInvalidAddress)
            {
                // Insert as the first record in the hash chain.
                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word);
                success = foundEntry.word == entry.word;
            }
            else
            {
                // Splice into the gap of the last readcache/first main log entries.
                ref RecordInfo rcri = ref readcache.GetInfo(lastReadCachePhysicalAddress);
                if (rcri.PreviousAddress != latestLogicalAddress)
                    return OperationStatus.RETRY_NOW;
                
                // Splice a non-tentative record into the readcache/mainlog gap.
                success = rcri.TryUpdateAddress(newLogicalAddress);

                // Now see if we have added a readcache entry from a pending read while we were inserting; if so it is obsolete and must be Invalidated.
                InvalidateUpdatedRecordInReadCache(entry.word, ref key, prevFirstReadCacheLogicalAddress);
            }

            if (success)
            {
                if (unsealPhysicalAddress != Constants.kInvalidAddress)
                    recordInfo.CopyLocksFrom(hlog.GetInfo(unsealPhysicalAddress));
                else if (LockTable.IsActive && !LockTable.ApplyToLogRecord(ref key, ref recordInfo))
                    return OperationStatus.RETRY_NOW;
                recordInfo.Tentative = false;

                fasterSession.PostSingleWriter(ref key, ref input, ref value, ref newValue, ref output, ref pendingContext.lockOperation, ref recordInfo, newLogicalAddress);
                pendingContext.recordInfo = recordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                return OperationStatus.SUCCESS;
            }

            // CAS failed - let user dispose similar to a deleted record
            ref RecordInfo insertedRecordInfo = ref hlog.GetInfo(newPhysicalAddress);
            ref Value insertedValue = ref hlog.GetValue(newPhysicalAddress);
            ref Key insertedKey = ref hlog.GetKey(newPhysicalAddress);
            // First set Invalid to true so that ConcurrentDeleter knows to dispose key as well
            insertedRecordInfo.SetInvalid();
            fasterSession.ConcurrentDeleter(ref insertedKey, ref insertedValue, ref insertedRecordInfo, newLogicalAddress);
            if (WriteDefaultOnDelete)
            {
                insertedKey = default;
                insertedValue = default;
            }
            return OperationStatus.RETRY_NOW;
        }

#endregion

#region RMW Operation

        /// <summary>
        /// Read-Modify-Write Operation. Updates value of 'key' using 'input' and current value.
        /// Pending operations are processed either using InternalRetryPendingRMW or 
        /// InternalContinuePendingRMW.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="userContext">user context corresponding to operation used during completion callback.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated(or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRMW<Input, Output, Context, FasterSession>(
                                   ref Key key, ref Input input, ref Output output,
                                   ref Context userContext,
                                   ref PendingContext<Input, Output, Context> pendingContext,
                                   FasterSession fasterSession,
                                   FasterExecutionContext<Input, Output, Context> sessionCtx,
                                   long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var physicalAddress = default(long);
            var status = default(OperationStatus);
            var latchOperation = LatchOperation.None;
            var heldOperation = LatchOperation.None;
            var latchDestination = LatchDestination.NormalProcessing;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

#region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            var logicalAddress = entry.Address;

            long lastReadCachePhysicalAddress = Constants.kInvalidAddress;
            long prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
            if (UseReadCache)
            {
                prevFirstReadCacheLogicalAddress = logicalAddress;
                SkipReadCache(ref logicalAddress, out lastReadCachePhysicalAddress);
                if (prevFirstReadCacheLogicalAddress == logicalAddress) // if there were no readcache records
                    prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
            }
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.HeadAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key,
                                        logicalAddress,
                                        hlog.HeadAddress,
                                        out logicalAddress,
                                        out physicalAddress);
                }
            }
#endregion

            // Optimization for the most common case
            long unsealPhysicalAddress = Constants.kInvalidAddress;
            if (sessionCtx.phase == Phase.REST && logicalAddress >= hlog.ReadOnlyAddress)
            {
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                ref Value recordValue = ref hlog.GetValue(physicalAddress);
                if (recordInfo.IsIntermediate(out status))
                    return status;

                if (!recordInfo.Tombstone)
                {
                    if (fasterSession.InPlaceUpdater(ref key, ref input, ref hlog.GetValue(physicalAddress), ref output, ref recordInfo, logicalAddress))
                    {
                        hlog.MarkPage(logicalAddress, sessionCtx.version);
                        pendingContext.recordInfo = recordInfo;
                        pendingContext.logicalAddress = logicalAddress;
                        return OperationStatus.SUCCESS;
                    }

                    // ConcurrentWriter failed (e.g. insufficient space). Another thread may come along to do this update in-place; Seal it to prevent that.
                    if (!recordInfo.Seal(fasterSession.IsManualLocking))
                        return OperationStatus.RETRY_NOW;
                    unsealPhysicalAddress = physicalAddress;
                }
                goto CreateNewRecord;
            }

#region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                latchDestination = AcquireLatchRMW(pendingContext, sessionCtx, bucket, ref status, ref latchOperation, ref entry, logicalAddress);
            }
#endregion

#region Normal processing

            // Mutable Region: Update the record in-place
            if (latchDestination == LatchDestination.NormalProcessing)
            {
                if (logicalAddress >= hlog.ReadOnlyAddress)
                {
                    ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                    ref Value recordValue = ref hlog.GetValue(physicalAddress);
                    if (recordInfo.IsIntermediate(out status))
                        return status;

                    if (!recordInfo.Tombstone)
                    {
                        if (fasterSession.InPlaceUpdater(ref key, ref input, ref recordValue, ref output, ref recordInfo, logicalAddress))
                        {
                            if (sessionCtx.phase == Phase.REST) hlog.MarkPage(logicalAddress, sessionCtx.version);
                            else hlog.MarkPageAtomic(logicalAddress, sessionCtx.version);
                            pendingContext.recordInfo = recordInfo;
                            pendingContext.logicalAddress = logicalAddress;
                            status = OperationStatus.SUCCESS;
                            goto LatchRelease; // Release shared latch (if acquired)
                        }

                        // ConcurrentWriter failed (e.g. insufficient space). Another thread may come along to do this update in-place; Seal it to prevent that.
                        if (!recordInfo.Seal(fasterSession.IsManualLocking))
                            return OperationStatus.RETRY_NOW;
                        unsealPhysicalAddress = physicalAddress;
                    }
                }

                // Fuzzy Region: Must go pending due to lost-update anomaly
                else if (logicalAddress >= hlog.SafeReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone) // TODO replace with Sealed
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
                    latchDestination = LatchDestination.CreatePendingContext; // Go pending
                }

                // Safe Read-Only Region: Create a record in the mutable region
                else if (logicalAddress >= hlog.HeadAddress)
                {
                    ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                    if (recordInfo.IsIntermediate(out status))
                        goto LatchRelease; // Release shared latch (if acquired)
                    if (!recordInfo.Seal(fasterSession.IsManualLocking))
                        return OperationStatus.RETRY_NOW;
                    unsealPhysicalAddress = physicalAddress;
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
                    latchDestination = LatchDestination.CreatePendingContext; // Go pending
                }

                // No record exists - create new
                else
                {
                    goto CreateNewRecord;
                }
            }

#endregion

#region Create new record
        CreateNewRecord:
            // Invalidate the entry in the read cache, as we did not do IPU.
            if (UseReadCache)
            {
                prevFirstReadCacheLogicalAddress = logicalAddress;
                if (!SkipAndInvalidateReadCache(ref logicalAddress, ref key, out lastReadCachePhysicalAddress, out OperationStatus internalStatus))
                    return internalStatus;
            }

            if (latchDestination != LatchDestination.CreatePendingContext)
            {
                status = CreateNewRecordRMW(ref key, ref input, ref output, ref pendingContext, fasterSession, sessionCtx, bucket, slot, logicalAddress, physicalAddress, tag, entry,
                                            latestLogicalAddress, prevFirstReadCacheLogicalAddress, lastReadCachePhysicalAddress, unsealPhysicalAddress);
                if (status != OperationStatus.SUCCESS)
                {
                    if (unsealPhysicalAddress != Constants.kInvalidAddress)
                    {
                        // Operation failed, so unseal the old record.
                        hlog.GetInfo(unsealPhysicalAddress).Unseal();
                    }
                    if (status == OperationStatus.ALLOCATE_FAILED)
                    {
                        latchDestination = LatchDestination.CreatePendingContext;
                        goto CreatePendingContext;
                    }
                }
                goto LatchRelease;
            }
#endregion

#region Create failure context
            CreatePendingContext:
            Debug.Assert(latchDestination == LatchDestination.CreatePendingContext, $"RMW CreatePendingContext encountered latchDest == {latchDestination}");
            {
                pendingContext.type = OperationType.RMW;
                if (pendingContext.key == default) pendingContext.key = hlog.GetKeyContainer(ref key);
                if (pendingContext.input == default) pendingContext.input = fasterSession.GetHeapContainer(ref input);

                pendingContext.output = output;
                if (pendingContext.output is IHeapConvertible heapConvertible)
                    heapConvertible.ConvertToHeap();

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

            return status;
        }

        private LatchDestination AcquireLatchRMW<Input, Output, Context>(PendingContext<Input, Output, Context> pendingContext, FasterExecutionContext<Input, Output, Context> sessionCtx,
                                                                         HashBucket* bucket, ref OperationStatus status, ref LatchOperation latchOperation, ref HashBucketEntry entry, long logicalAddress)
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
                            if (CheckBucketVersionNew(ref entry))
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                return LatchDestination.CreatePendingContext; // Pivot Thread
                            }
                            break; // Normal Processing
                        }
                        else
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            return LatchDestination.CreatePendingContext; // Pivot Thread
                        }
                    }
                case Phase.IN_PROGRESS:
                    {
                        if (!CheckEntryVersionNew(logicalAddress))
                        {
                            Debug.Assert(pendingContext.heldLatch != LatchOperation.Shared);
                            if (pendingContext.heldLatch == LatchOperation.Exclusive || HashBucket.TryAcquireExclusiveLatch(bucket))
                            {
                                // Set to release exclusive latch (default)
                                latchOperation = LatchOperation.Exclusive;
                                if (logicalAddress >= hlog.HeadAddress)
                                    return LatchDestination.CreateNewRecord; // Create a (v+1) record
                            }
                            else
                            {
                                status = OperationStatus.RETRY_LATER;
                                return LatchDestination.CreatePendingContext; // Go Pending
                            }
                        }
                        break; // Normal Processing
                    }
                case Phase.WAIT_FLUSH:
                    {
                        if (!CheckEntryVersionNew(logicalAddress))
                        {
                            if (logicalAddress >= hlog.HeadAddress)
                                return LatchDestination.CreateNewRecord; // Create a (v+1) record
                        }
                        break; // Normal Processing
                    }
                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        private OperationStatus CreateNewRecordRMW<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output, ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                                                                          FasterExecutionContext<Input, Output, Context> sessionCtx, HashBucket* bucket, int slot, long logicalAddress, 
                                                                                          long physicalAddress, ushort tag, HashBucketEntry entry, long latestLogicalAddress,
                                                                                          long prevFirstReadCacheLogicalAddress, long lastReadCachePhysicalAddress, long unsealPhysicalAddress)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // Determine if we should allocate a new record
            if (logicalAddress >= hlog.HeadAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                if (!fasterSession.NeedCopyUpdate(ref key, ref input, ref hlog.GetValue(physicalAddress), ref output))
                    return OperationStatus.SUCCESS;
            }
            else
            {
                if (!fasterSession.NeedInitialUpdate(ref key, ref input, ref output))
                    return OperationStatus.SUCCESS;
            }

            // Allocate and initialize the new record
            var (actualSize, allocatedSize) = (logicalAddress < hlog.BeginAddress) ?
                            hlog.GetInitialRecordSize(ref key, ref input, fasterSession) :
                            hlog.GetRecordSize(physicalAddress, ref input, fasterSession);
            BlockAllocate(allocatedSize, out long newLogicalAddress, sessionCtx, fasterSession, pendingContext.IsAsync);
            if (newLogicalAddress == 0)
                return OperationStatus.ALLOCATE_FAILED;
            var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
            ref RecordInfo recordInfo = ref hlog.GetInfo(newPhysicalAddress);
            recordInfo.Tentative = true;
            RecordInfo.WriteInfo(ref recordInfo, 
                            inNewVersion: sessionCtx.InNewVersion,
                            tombstone: false, dirty: true,
                            latestLogicalAddress);
            hlog.Serialize(ref key, newPhysicalAddress);

            // Populate the new record
            OperationStatus status;
            if (logicalAddress < hlog.BeginAddress)
            {
                fasterSession.InitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                             ref pendingContext.lockOperation, ref recordInfo, newLogicalAddress);
                status = OperationStatus.NOTFOUND;
            }
            else if (logicalAddress >= hlog.HeadAddress)
            {
                if (hlog.GetInfo(physicalAddress).Tombstone)
                {
                    fasterSession.InitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                                 ref pendingContext.lockOperation, ref recordInfo, newLogicalAddress);
                    status = OperationStatus.NOTFOUND;
                }
                else
                {
                    fasterSession.CopyUpdater(ref key, ref input, ref hlog.GetValue(physicalAddress),
                                            ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                            ref output, ref recordInfo, newLogicalAddress);
                    status = OperationStatus.SUCCESS;
                }
            }
            else
            {
                // ah, old record slipped onto disk
                hlog.GetInfo(newPhysicalAddress).SetInvalid();
                return OperationStatus.RETRY_NOW;
            }

            bool success = true;
            if (lastReadCachePhysicalAddress == Constants.kInvalidAddress)
            {
                // Insert as the first record in the hash chain.
                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word);
                success = foundEntry.word == entry.word;
            }
            else
            {
                // Splice into the gap of the last readcache/first main log entries.
                ref RecordInfo rcri = ref readcache.GetInfo(lastReadCachePhysicalAddress);
                if (rcri.PreviousAddress != latestLogicalAddress)
                    return OperationStatus.RETRY_NOW;

                // Splice a non-tentative record into the readcache/mainlog gap.
                success = rcri.TryUpdateAddress(newLogicalAddress);

                // Now see if we have added a readcache entry from a pending read while we were inserting; if so it is obsolete and must be Invalidated.
                InvalidateUpdatedRecordInReadCache(entry.word, ref key, prevFirstReadCacheLogicalAddress);
            }

            if (success)
            {
                if (unsealPhysicalAddress != Constants.kInvalidAddress)
                    recordInfo.CopyLocksFrom(hlog.GetInfo(unsealPhysicalAddress));
                else if (LockTable.IsActive && !LockTable.ApplyToLogRecord(ref key, ref recordInfo))
                    return OperationStatus.RETRY_NOW;
                recordInfo.Tentative = false;

                // If IU, status will be NOTFOUND; return that.
                if (status != OperationStatus.SUCCESS)
                {
                    Debug.Assert(OperationStatus.NOTFOUND == status);
                    fasterSession.PostInitialUpdater(ref key,
                            ref input, ref hlog.GetValue(newPhysicalAddress),
                            ref output, ref recordInfo, newLogicalAddress);
                    pendingContext.recordInfo = recordInfo;
                    pendingContext.logicalAddress = newLogicalAddress;
                    return status;
                }

                // Else it was a CopyUpdater so call PCU; if PCU returns true, return success, else retry op.
                if (fasterSession.PostCopyUpdater(ref key,
                            ref input, ref hlog.GetValue(physicalAddress),
                            ref hlog.GetValue(newPhysicalAddress),
                            ref output, ref recordInfo, newLogicalAddress))
                {
                    pendingContext.recordInfo = recordInfo;
                    pendingContext.logicalAddress = newLogicalAddress;
                    return status;
                }
            }
            else
            {
                // CAS failed
                hlog.GetInfo(newPhysicalAddress).SetInvalid();
            }
            status = OperationStatus.RETRY_NOW;
            return status;
        }

#endregion

#region Delete Operation

        /// <summary>
        /// Delete operation. Replaces the value corresponding to 'key' with tombstone.
        /// If at head, tries to remove item from hash chain
        /// </summary>
        /// <param name="key">Key of the record to be deleted.</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully deleted</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalDelete<Input, Output, Context, FasterSession>(
                            ref Key key,
                            ref Context userContext,
                            ref PendingContext<Input, Output, Context> pendingContext,
                            FasterSession fasterSession,
                            FasterExecutionContext<Input, Output, Context> sessionCtx,
                            long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var status = default(OperationStatus);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var latchOperation = default(LatchOperation);
            long unsealPhysicalAddress = Constants.kInvalidAddress;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

#region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
            if (!tagExists)
                return OperationStatus.NOTFOUND;

            logicalAddress = entry.Address;

            long lastReadCachePhysicalAddress = Constants.kInvalidAddress;
            long prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
            if (UseReadCache)
            {
                prevFirstReadCacheLogicalAddress = logicalAddress;
                SkipReadCache(ref logicalAddress, out lastReadCachePhysicalAddress);
                if (prevFirstReadCacheLogicalAddress == logicalAddress) // if there were no readcache records
                    prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
            }
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
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
                                if (CheckBucketVersionNew(ref entry))
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
                            if (!CheckEntryVersionNew(logicalAddress))
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
                    case Phase.WAIT_FLUSH:
                        {
                            if (!CheckEntryVersionNew(logicalAddress))
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


            #region Normal processing

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                ref Value recordValue = ref hlog.GetValue(physicalAddress);
                if (recordInfo.IsIntermediate(out status))
                {
                    goto LatchRelease; // Release shared latch (if acquired)
                }

                if (!fasterSession.ConcurrentDeleter(ref hlog.GetKey(physicalAddress), ref recordValue, ref recordInfo, logicalAddress))
                    goto CreateNewRecord;

                if (sessionCtx.phase == Phase.REST)
                    hlog.MarkPage(logicalAddress, sessionCtx.version);
                else 
                    hlog.MarkPageAtomic(logicalAddress, sessionCtx.version);
                if (WriteDefaultOnDelete)
                    recordValue = default;

                // Try to update hash chain and completely elide record only if previous address points to invalid address
                if (!recordInfo.IsLocked && entry.Address == logicalAddress && recordInfo.PreviousAddress < hlog.BeginAddress)
                {
                    var updatedEntry = default(HashBucketEntry);
                    updatedEntry.Tag = 0;
                    if (recordInfo.PreviousAddress == Constants.kTempInvalidAddress)
                        updatedEntry.Address = Constants.kInvalidAddress;
                    else
                        updatedEntry.Address = recordInfo.PreviousAddress;
                    updatedEntry.Pending = entry.Pending;
                    updatedEntry.Tentative = false;

                    // Ignore return value; this is a performance optimization to keep the hash table clean if we can, so if we fail it just means
                    // the hashtable entry has already been updated by someone else.
                    Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word);
                }

                status = OperationStatus.SUCCESS;
                goto LatchRelease; // Release shared latch (if acquired)
            }
            else if (logicalAddress >= hlog.HeadAddress)
            {
                // Only need to go below ReadOnly here for locking and Sealing.
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                pendingContext.recordInfo = recordInfo;
                pendingContext.logicalAddress = logicalAddress;

                if (recordInfo.IsIntermediate(out status))
                    goto LatchRelease; // Release shared latch (if acquired)
                if (!recordInfo.Seal(fasterSession.IsManualLocking))
                {
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease; // Release shared latch (if acquired)
                }
                unsealPhysicalAddress = physicalAddress;
                goto CreateNewRecord;
            }

            // All other regions: Create a record in the mutable region
#endregion

#region Create new record in the mutable region
            CreateNewRecord:
            {
                var value = default(Value);
                // Immutable region or new record
                // Allocate default record size for tombstone
                var (actualSize, allocateSize) = hlog.GetRecordSize(ref key, ref value);
                BlockAllocate(allocateSize, out long newLogicalAddress, sessionCtx, fasterSession, pendingContext.IsAsync);
                if (newLogicalAddress == 0)
                {
                    if (unsealPhysicalAddress != Constants.kInvalidAddress)
                    {
                        // Operation failed, so unseal the old record.
                        hlog.GetInfo(unsealPhysicalAddress).Unseal();
                    }
                    status = OperationStatus.ALLOCATE_FAILED;
                    goto CreatePendingContext;
                }
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(newPhysicalAddress);
                recordInfo.Tentative = true;
                RecordInfo.WriteInfo(ref recordInfo,
                               inNewVersion: sessionCtx.InNewVersion,
                               tombstone: true, dirty: true,
                               latestLogicalAddress);
                hlog.Serialize(ref key, newPhysicalAddress);

                bool success = true;
                if (lastReadCachePhysicalAddress == Constants.kInvalidAddress)
                {
                    // Insert as the first record in the hash chain.
                    var updatedEntry = default(HashBucketEntry);
                    updatedEntry.Tag = tag;
                    updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                    updatedEntry.Pending = entry.Pending;
                    updatedEntry.Tentative = false;

                    var foundEntry = default(HashBucketEntry);
                    foundEntry.word = Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word);
                    success = foundEntry.word == entry.word;
                }
                else
                {
                    // Splice into the gap of the last readcache/first main log entries.
                    ref RecordInfo rcri = ref readcache.GetInfo(lastReadCachePhysicalAddress);
                    if (rcri.PreviousAddress != latestLogicalAddress)
                        return OperationStatus.RETRY_NOW;

                    // Splice a non-tentative record into the readcache/mainlog gap.
                    success = rcri.TryUpdateAddress(newLogicalAddress);

                    // Now see if we have added a readcache entry from a pending read while we were inserting; if so it is obsolete and must be Invalidated.
                    InvalidateUpdatedRecordInReadCache(entry.word, ref key, prevFirstReadCacheLogicalAddress);
                }

                if (success)
                {
                    if (unsealPhysicalAddress != Constants.kInvalidAddress)
                        recordInfo.CopyLocksFrom(hlog.GetInfo(unsealPhysicalAddress));
                    else if (LockTable.IsActive && !LockTable.ApplyToLogRecord(ref key, ref recordInfo))
                        return OperationStatus.RETRY_NOW;
                    recordInfo.Tentative = false;

                    // Note that this is the new logicalAddress; we have not retrieved the old one if it was below HeadAddress, and thus
                    // we do not know whether 'logicalAddress' belongs to 'key' or is a collision.
                    fasterSession.PostSingleDeleter(ref key, ref recordInfo, newLogicalAddress);
                    pendingContext.recordInfo = recordInfo;
                    pendingContext.logicalAddress = newLogicalAddress;
                    status = OperationStatus.SUCCESS;
                    goto LatchRelease;
                }
                else
                {
                    recordInfo.SetInvalid();
                    status = OperationStatus.RETRY_NOW;

                    if (unsealPhysicalAddress != Constants.kInvalidAddress)
                    {
                        // Operation failed, so unseal the old record.
                        hlog.GetInfo(unsealPhysicalAddress).Unseal();
                    }
                    goto LatchRelease;
                }
            }
#endregion

#region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.DELETE;
                if (pendingContext.key == default) pendingContext.key = hlog.GetKeyContainer(ref key);
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

            return status;
        }

        #endregion

        /// <summary>
        /// Manual Lock operation. Locks the record corresponding to 'key'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="lockOp">Lock operation being done.</param>
        /// <param name="lockInfo">Receives the recordInfo of the record being locked</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalLock<Input, Output, Context, FasterSession>(
                            ref Key key, LockOperation lockOp, out RecordInfo lockInfo, FasterSession fasterSession,
                            FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            var prevTailAddress = hlog.GetTailAddress();

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            var logicalAddress = entry.Address;
            var physicalAddress = default(long);

            OperationStatus status;
            if (UseReadCache)
            {
                if (LockReadCacheRecord(logicalAddress, ref key, lockOp, out lockInfo, out status))
                    return status;
            }

            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
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

            lockInfo = default;
            if (logicalAddress >= hlog.HeadAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                if (!recordInfo.IsIntermediate(out status))
                {
                    if (lockOp.LockOperationType == LockOperationType.IsLocked)
                        status = OperationStatus.SUCCESS;
                    else
                        recordInfo.HandleLockOperation(lockOp, out _);
                }
                if (lockOp.LockOperationType == LockOperationType.IsLocked)
                    lockInfo = recordInfo;
                return status;
            }

            // Not in memory. Do LockTable operations
            if (lockOp.LockOperationType == LockOperationType.IsLocked)
            {
                this.LockTable.Get(ref key, out lockInfo);
                return OperationStatus.SUCCESS;
            }

            if (lockOp.LockOperationType == LockOperationType.Unlock)
            {
                this.LockTable.Unlock(ref key, lockOp.LockType);
                return OperationStatus.SUCCESS;
            }

            // Try to lock
            bool tentativeLock;
            while (!this.LockTable.LockOrTentative(ref key, lockOp.LockType, out tentativeLock))
            {
                // Sealed by someone else, so retry
                return OperationStatus.RETRY_NOW;
            }

            // We got the lock. If we had another record with this key inserted, RETRY.
            if (FindTag(hash, tag, ref bucket, ref slot, ref entry) && entry.Address >= prevTailAddress)
                return OperationStatus.RETRY_NOW;

            // Success
            if (tentativeLock)
                this.LockTable.ClearTentative(ref key);
            return OperationStatus.SUCCESS;
        }

        #region ContainsKeyInMemory

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status InternalContainsKeyInMemory<Input, Output, Context, FasterSession>(
            ref Key key, 
            FasterExecutionContext<Input, Output, Context> sessionCtx, 
            FasterSession fasterSession, out long logicalAddress, long fromAddress = -1)
            where FasterSession : IFasterSession
        {
            if (fromAddress < hlog.HeadAddress)
                fromAddress = hlog.HeadAddress;

            var bucket = default(HashBucket*);
            var slot = default(int);
            long physicalAddress;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            HashBucketEntry entry = default;
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);

            if (tagExists)
            {
                logicalAddress = entry.Address;

                if (UseReadCache)
                    SkipReadCache(ref logicalAddress, out _);

                if (logicalAddress >= fromAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                    if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                    {
                        logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                        TraceBackForKeyMatch(ref key,
                                                logicalAddress,
                                                fromAddress,
                                                out logicalAddress,
                                                out _);
                    }

                    if (logicalAddress < fromAddress)
                    {
                        logicalAddress = 0;
                        return Status.NOTFOUND;
                    }
                    else
                        return Status.OK;
                }
                else
                {
                    logicalAddress = 0;
                    return Status.NOTFOUND;
                }
            }
            else
            {
                // no tag found
                logicalAddress = 0;
                return Status.NOTFOUND;
            }
        }
#endregion

#region Continue operations
        /// <summary>
        /// Continue a pending read operation. Computes 'output' from 'input' and value corresponding to 'key'
        /// obtained from disk. Optionally, it copies the value to tail to serve future read/write requests quickly.
        /// </summary>
        /// <param name="ctx">The thread (or session) context to execute operation in.</param>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="currentCtx"></param>
        /// <returns>
        /// <list type = "table" >
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The output has been computed and stored in 'output'.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalContinuePendingRead<Input, Output, Context, FasterSession>(
                            FasterExecutionContext<Input, Output, Context> ctx,
                            AsyncIOContext<Key, Value> request,
                            ref PendingContext<Input, Output, Context> pendingContext,
                            FasterSession fasterSession,
                            FasterExecutionContext<Input, Output, Context> currentCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(RelaxedCPR || pendingContext.version == ctx.version);

            if (request.logicalAddress >= hlog.BeginAddress)
            {
                ref RecordInfo recordInfo = ref hlog.GetInfoFromBytePointer(request.record.GetValidPointer());

                if (recordInfo.IsIntermediate(out var internalStatus))
                    return internalStatus;

                if (hlog.GetInfoFromBytePointer(request.record.GetValidPointer()).Tombstone)
                    return OperationStatus.NOTFOUND;

                // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
                // With the new overload of CompletePending that returns CompletedOutputs, pendingContext must have the key.
                if (pendingContext.NoKey && pendingContext.key == default)
                    pendingContext.key = hlog.GetKeyContainer(ref hlog.GetContextRecordKey(ref request));

                ref Key key = ref pendingContext.key.Get();
                if (!fasterSession.SingleReader(ref key, ref pendingContext.input.Get(),
                                       ref hlog.GetContextRecordValue(ref request), ref pendingContext.output, ref pendingContext.lockOperation, ref recordInfo, request.logicalAddress))
                    return OperationStatus.NOTFOUND;

                // If there is a LockTable entry for this record, we must force the CopyToTail, or the lock will be ignored.
                if (LockTable.ContainsKey(ref key)
                    || (CopyReadsToTail != CopyReadsToTail.None && !pendingContext.SkipCopyReadsToTail)
                    || pendingContext.CopyReadsToTail
                    || (UseReadCache && !pendingContext.SkipReadCache))
                {
                    InternalContinuePendingReadCopyToTail(ctx, request, ref pendingContext, fasterSession, currentCtx);
                }
            }
            else
                return OperationStatus.NOTFOUND;

            return OperationStatus.SUCCESS;
        }

        /// <summary>
        /// Copies the record read from disk to tail of the HybridLog. 
        /// </summary>
        /// <param name="opCtx"> The thread(or session) context to execute operation in.</param>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="currentCtx"></param>
        internal void InternalContinuePendingReadCopyToTail<Input, Output, Context, FasterSession>(
                                    FasterExecutionContext<Input, Output, Context> opCtx,
                                    AsyncIOContext<Key, Value> request,
                                    ref PendingContext<Input, Output, Context> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> currentCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(RelaxedCPR || pendingContext.version == opCtx.version);

            // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
            ref Key key = ref pendingContext.NoKey ? ref hlog.GetContextRecordKey(ref request) : ref pendingContext.key.Get();

            byte* physicalAddress = request.record.GetValidPointer();
            long logicalAddress = pendingContext.entry.Address;
            ref RecordInfo oldRecordInfo = ref hlog.GetInfoFromBytePointer(physicalAddress);
            
            InternalTryCopyToTail(opCtx, ref pendingContext, ref key, ref pendingContext.input.Get(), ref hlog.GetContextRecordValue(ref request), 
                                 ref pendingContext.output, ref pendingContext.lockOperation, logicalAddress, fasterSession, currentCtx);
        }

        /// <summary>
        /// Continue a pending RMW operation with the record retrieved from disk.
        /// </summary>
        /// <param name="opCtx">thread (or session) context under which operation must be executed.</param>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated(or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalContinuePendingRMW<Input, Output, Context, FasterSession>(
                                    FasterExecutionContext<Input, Output, Context> opCtx,
                                    AsyncIOContext<Key, Value> request,
                                    ref PendingContext<Input, Output, Context> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var status = default(OperationStatus);
            ref Key key = ref pendingContext.key.Get();

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            long lastReadCachePhysicalAddress = Constants.kInvalidAddress;
            long prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;

#region Trace Back for Record on In-Memory HybridLog
            while (true)
            {
                var entry = default(HashBucketEntry);
                FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
                logicalAddress = entry.Address;

                // Invalidate the entry in the read cache, as we did not do IPU.
                if (UseReadCache)
                {
                    prevFirstReadCacheLogicalAddress = logicalAddress;
                    if (!SkipAndInvalidateReadCache(ref logicalAddress, ref key, out lastReadCachePhysicalAddress, out status))
                        return status;
                    if (prevFirstReadCacheLogicalAddress == logicalAddress) // if there were no readcache records
                        prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
                }
                var latestLogicalAddress = logicalAddress;

                if (logicalAddress >= hlog.HeadAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                    if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                    {
                        logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                        TraceBackForKeyMatch(ref key,
                                                logicalAddress,
                                                hlog.HeadAddress,
                                                out logicalAddress,
                                                out physicalAddress);
                    }
                }
#endregion

                var previousFirstRecordAddress = pendingContext.entry.Address;
                if (logicalAddress > previousFirstRecordAddress)
                {
                    break;
                }

#region Create record in mutable region

                // Determine if we should allocate a new record
                if ((request.logicalAddress >= hlog.BeginAddress) && !hlog.GetInfoFromBytePointer(request.record.GetValidPointer()).Tombstone)
                {
                    if (!fasterSession.NeedCopyUpdate(ref key, ref pendingContext.input.Get(), ref hlog.GetContextRecordValue(ref request), ref pendingContext.output))
                        return OperationStatus.SUCCESS;
                }
                else
                {
                    if (!fasterSession.NeedInitialUpdate(ref key, ref pendingContext.input.Get(), ref pendingContext.output))
                        return OperationStatus.SUCCESS;
                }

                // Allocate and initialize the new record
                int actualSize, allocatedSize;
                if ((request.logicalAddress < hlog.BeginAddress) || hlog.GetInfoFromBytePointer(request.record.GetValidPointer()).Tombstone)
                {
                    (actualSize, allocatedSize) = hlog.GetInitialRecordSize(ref key, ref pendingContext.input.Get(), fasterSession);
                }
                else
                {
                    physicalAddress = (long)request.record.GetValidPointer();
                    (actualSize, allocatedSize) = hlog.GetRecordSize(physicalAddress, ref pendingContext.input.Get(), fasterSession);
                }
                BlockAllocate(allocatedSize, out long newLogicalAddress, sessionCtx, fasterSession);
                if (newLogicalAddress == 0)
                    return OperationStatus.ALLOCATE_FAILED;
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(newPhysicalAddress);
                RecordInfo.WriteInfo(ref recordInfo,
                               inNewVersion: opCtx.InNewVersion,
                               tombstone: false, dirty: true,
                               latestLogicalAddress);
                recordInfo.Tentative = true;
                hlog.Serialize(ref key, newPhysicalAddress);

                // Populate the new record
                if ((request.logicalAddress < hlog.BeginAddress) || hlog.GetInfoFromBytePointer(request.record.GetValidPointer()).Tombstone)
                {
                    fasterSession.InitialUpdater(ref key,
                                             ref pendingContext.input.Get(), ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                             ref pendingContext.output, ref pendingContext.lockOperation, ref recordInfo, newLogicalAddress);
                    status = OperationStatus.NOTFOUND;
                }
                else
                {
                    fasterSession.CopyUpdater(ref key,
                                          ref pendingContext.input.Get(), ref hlog.GetContextRecordValue(ref request),
                                          ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                          ref pendingContext.output, ref recordInfo, newLogicalAddress);
                    status = OperationStatus.SUCCESS;
                }

                bool success = true;
                if (lastReadCachePhysicalAddress == Constants.kInvalidAddress)
                {
                    // Insert as the first record in the hash chain.
                    var updatedEntry = default(HashBucketEntry);
                    updatedEntry.Tag = tag;
                    updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                    updatedEntry.Pending = entry.Pending;
                    updatedEntry.Tentative = false;

                    var foundEntry = default(HashBucketEntry);
                    foundEntry.word = Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word);
                    success = foundEntry.word == entry.word;
                }
                else
                {
                    // Splice into the gap of the last readcache/first main log entries.
                    ref RecordInfo rcri = ref readcache.GetInfo(lastReadCachePhysicalAddress);
                    if (rcri.PreviousAddress != latestLogicalAddress)
                        return OperationStatus.RETRY_NOW;

                    // Splice a non-tentative record into the readcache/mainlog gap.
                    success = rcri.TryUpdateAddress(newLogicalAddress);

                    // Now see if we have added a readcache entry from a pending read while we were inserting; if so it is obsolete and must be Invalidated.
                    InvalidateUpdatedRecordInReadCache(entry.word, ref key, prevFirstReadCacheLogicalAddress);
                }

                if (success)
                {
                    if (LockTable.IsActive && !LockTable.ApplyToLogRecord(ref key, ref recordInfo))
                        return OperationStatus.RETRY_NOW;
                    recordInfo.Tentative = false;

                    // If IU, status will be NOTFOUND; return that.
                    if (status != OperationStatus.SUCCESS)
                    {
                        Debug.Assert(OperationStatus.NOTFOUND == status);
                        fasterSession.PostInitialUpdater(ref key,
                                          ref pendingContext.input.Get(),
                                          ref hlog.GetValue(newPhysicalAddress),
                                          ref pendingContext.output, ref recordInfo, newLogicalAddress);
                        pendingContext.recordInfo = recordInfo;
                        pendingContext.logicalAddress = newLogicalAddress;
                        return status;
                    }

                    // Else it was a CopyUpdater so call PCU; if PCU returns true, return success, else retry op.
                    if (fasterSession.PostCopyUpdater(ref key,
                                          ref pendingContext.input.Get(),
                                          ref hlog.GetContextRecordValue(ref request),
                                          ref hlog.GetValue(newPhysicalAddress),
                                          ref pendingContext.output, ref recordInfo, newLogicalAddress))
                    {
                        pendingContext.recordInfo = recordInfo;
                        pendingContext.logicalAddress = newLogicalAddress;
                        return status;
                    }
                }
                else
                {
                    // CAS failed. Retry in loop.
                    hlog.GetInfo(newPhysicalAddress).SetInvalid();
                }
#endregion
            }

            OperationStatus internalStatus;
            do
                internalStatus = InternalRMW(ref pendingContext.key.Get(), ref pendingContext.input.Get(), ref pendingContext.output, ref pendingContext.userContext, ref pendingContext, fasterSession, opCtx, pendingContext.serialNum);
            while (internalStatus == OperationStatus.RETRY_NOW);
            return internalStatus;
        }

#endregion

#region Helper Functions

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="opCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="currentCtx">Current context</param>
        /// <param name="pendingContext">Internal context of the operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="status">Internal status of the trial.</param>
        /// <param name="asyncOp">When operation issued via async call</param>
        /// <param name="request">IO request, if operation went pending</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>OK</term>
        ///     <term>The operation has been completed successfully.</term>
        ///     </item>
        ///     <item>
        ///     <term>PENDING</term>
        ///     <term>The operation is still pending and will callback when done.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal Status HandleOperationStatus<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            ref PendingContext<Input, Output, Context> pendingContext,
            FasterSession fasterSession,
            OperationStatus status, bool asyncOp, out AsyncIOContext<Key, Value> request)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            request = default;

            if (status == OperationStatus.CPR_SHIFT_DETECTED)
            {
                SynchronizeEpoch(opCtx, currentCtx, ref pendingContext, fasterSession);
            }

            // RMW now suppports RETRY_NOW due to Sealed records.
            if (status == OperationStatus.CPR_SHIFT_DETECTED || status == OperationStatus.RETRY_NOW || ((asyncOp || RelaxedCPR) && status == OperationStatus.RETRY_LATER))
            {
#region Retry as (v+1) Operation
                var internalStatus = default(OperationStatus);
                do
                {
                    switch (pendingContext.type)
                    {
                        case OperationType.READ:
                            internalStatus = InternalRead(ref pendingContext.key.Get(),
                                                          ref pendingContext.input.Get(),
                                                          ref pendingContext.output,
                                                          pendingContext.recordInfo.PreviousAddress,
                                                          ref pendingContext.userContext,
                                                          ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                            break;
                        case OperationType.UPSERT:
                            internalStatus = InternalUpsert(ref pendingContext.key.Get(),
                                                            ref pendingContext.input.Get(),
                                                            ref pendingContext.value.Get(),
                                                            ref pendingContext.output,
                                                            ref pendingContext.userContext,
                                                            ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                            break;
                        case OperationType.DELETE:
                            internalStatus = InternalDelete(ref pendingContext.key.Get(),
                                                            ref pendingContext.userContext,
                                                            ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                            break;
                        case OperationType.RMW:
                            internalStatus = InternalRMW(ref pendingContext.key.Get(),
                                                         ref pendingContext.input.Get(),
                                                         ref pendingContext.output,
                                                         ref pendingContext.userContext,
                                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                            break;
                    }
                    Debug.Assert(internalStatus != OperationStatus.CPR_SHIFT_DETECTED);
                } while (internalStatus == OperationStatus.RETRY_NOW || ((asyncOp || RelaxedCPR) && internalStatus == OperationStatus.RETRY_LATER));
                // Note that we spin in case of { async op + strict CPR } which is fine as this combination is rare/discouraged

                status = internalStatus;
#endregion
            }

            if (status == OperationStatus.SUCCESS || status == OperationStatus.NOTFOUND)
            {
                return (Status)status;
            }
            else if (status == OperationStatus.RECORD_ON_DISK)
            {
                //Add context to dictionary
                pendingContext.id = opCtx.totalPending++;
                opCtx.ioPendingRequests.Add(pendingContext.id, pendingContext);

                // Issue asynchronous I/O request
                request.id = pendingContext.id;
                request.request_key = pendingContext.key;
                request.logicalAddress = pendingContext.logicalAddress;
                request.minAddress = pendingContext.minAddress;
                request.record = default;
                if (asyncOp)
                    request.asyncOperation = new TaskCompletionSource<AsyncIOContext<Key, Value>>(TaskCreationOptions.RunContinuationsAsynchronously);
                else
                    request.callbackQueue = opCtx.readyResponses;
                
                hlog.AsyncGetFromDisk(pendingContext.logicalAddress,
                                 hlog.GetAverageRecordSize(),
                                 request);

                return Status.PENDING;
            }
            else if (status == OperationStatus.RETRY_LATER)
            {
                Debug.Assert(!asyncOp);
                opCtx.retryRequests.Enqueue(pendingContext);
                return Status.PENDING;
            }
            else
            {
                return Status.ERROR;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SynchronizeEpoch<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx, 
            FasterExecutionContext<Input, Output, Context> currentCtx, 
            ref PendingContext<Input, Output, Context> pendingContext, 
            FasterSession fasterSession)
            where FasterSession : IFasterSession
        {
            var version = opCtx.version;
            Debug.Assert(currentCtx.version == version);
            Debug.Assert(currentCtx.phase == Phase.PREPARE);
            InternalRefresh(currentCtx, fasterSession);
            Debug.Assert(currentCtx.version > version);

            pendingContext.version = currentCtx.version;
        }

        private void AcquireSharedLatch(Key key)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            HashBucket.TryAcquireSharedLatch(bucket);
        }

        private void ReleaseSharedLatch(Key key)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            HashBucket.ReleaseSharedLatch(bucket);
        }

        private void HeavyEnter<Input, Output, Context, FasterSession>(long hash, FasterExecutionContext<Input, Output, Context> ctx, FasterSession session)
            where FasterSession : IFasterSession
        {
            if (ctx.phase == Phase.PREPARE_GROW)
            {
                // We spin-wait as a simplification
                // Could instead do a "heavy operation" here
                while (systemState.Phase != Phase.IN_PROGRESS_GROW)
                    Thread.SpinWait(100);
                InternalRefresh(ctx, session);
            }
            if (ctx.phase == Phase.IN_PROGRESS_GROW)
            {
                SplitBuckets(hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BlockAllocate<Input, Output, Context, FasterSession>(
                int recordSize,
                out long logicalAddress,
                FasterExecutionContext<Input, Output, Context> ctx,
                FasterSession fasterSession, bool isAsync = false)
                where FasterSession : IFasterSession
        {
            logicalAddress = hlog.TryAllocate(recordSize);
            if (logicalAddress > 0)
                return;
            SpinBlockAllocate(hlog, recordSize, out logicalAddress, ctx, fasterSession, isAsync);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BlockAllocateReadCache<Input, Output, Context, FasterSession>(
                int recordSize,
                out long logicalAddress,
                FasterExecutionContext<Input, Output, Context> currentCtx,
                FasterSession fasterSession)
                where FasterSession : IFasterSession
        {
            logicalAddress = readcache.TryAllocate(recordSize);
            if (logicalAddress > 0)
                return;
            SpinBlockAllocate(readcache, recordSize, out logicalAddress, currentCtx, fasterSession, isAsync: false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SpinBlockAllocate<Input, Output, Context, FasterSession>(
                AllocatorBase<Key, Value> allocator,
                int recordSize,
                out long logicalAddress,
                FasterExecutionContext<Input, Output, Context> ctx,
                FasterSession fasterSession, bool isAsync)
                where FasterSession : IFasterSession
        {
            var spins = 0;
            while (true)
            {
                var flushEvent = allocator.FlushEvent;
                logicalAddress = allocator.TryAllocate(recordSize);
                if (logicalAddress > 0)
                    return;
                if (logicalAddress == 0)
                {
                    if (spins++ < Constants.kFlushSpinCount)
                    {
                        Thread.Yield();
                        continue;
                    }
                    if (isAsync) return;
                    try
                    {
                        epoch.Suspend();
                        flushEvent.Wait();
                    }
                    finally
                    {
                        epoch.Resume();
                    }
                }

                allocator.TryComplete();
                InternalRefresh(ctx, fasterSession);
                Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(
                                    ref Key key,
                                    long fromLogicalAddress,
                                    long minOffset,
                                    out long foundLogicalAddress,
                                    out long foundPhysicalAddress)
        {
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minOffset)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);
                if (comparer.Equals(ref key, ref hlog.GetKey(foundPhysicalAddress)))
                {
                    return true;
                }
                else
                {
                    foundLogicalAddress = hlog.GetInfo(foundPhysicalAddress).PreviousAddress;
                    continue;
                }
            }
            foundPhysicalAddress = Constants.kInvalidAddress;
            return false;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalCopyToTail<Input, Output, Context, FasterSession>(
                                            ref Key key, ref Input input, ref Value value, ref Output output,
                                            long expectedLogicalAddress,
                                            FasterSession fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx,
                                            bool noReadCache)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        { 
            OperationStatus internalStatus;
            LockOperation dummyLockOperation = default;
            PendingContext<Input, Output, Context>  pendingContext = default;
            do
                internalStatus = InternalTryCopyToTail(currentCtx, ref pendingContext, ref key, ref input, ref value, ref output, ref dummyLockOperation, expectedLogicalAddress, fasterSession, currentCtx, noReadCache);
            while (internalStatus == OperationStatus.RETRY_NOW);
            return internalStatus;
        }

        /// <summary>
        /// Helper function for trying to copy existing immutable records (at foundLogicalAddress) to the tail,
        /// used in <see cref="InternalRead{Input, Output, Context, Functions}(ref Key, ref Input, ref Output, long, ref Context, ref PendingContext{Input, Output, Context}, Functions, FasterExecutionContext{Input, Output, Context}, long)"/>
        /// <see cref="InternalContinuePendingReadCopyToTail{Input, Output, Context, FasterSession}(FasterExecutionContext{Input, Output, Context}, AsyncIOContext{Key, Value}, ref PendingContext{Input, Output, Context}, FasterSession, FasterExecutionContext{Input, Output, Context})"/>,
        /// and <see cref="ClientSession{Key, Value, Input, Output, Context, Functions}.CopyToTail(ref Key, ref Input, ref Value, ref Output, long)"/>
        /// 
        /// Succeed only if the record for the same key hasn't changed.
        /// </summary>
        /// <param name="opCtx">
        /// The thread(or session) context to execute operation in.
        /// It's different from currentCtx only when the function is used in InternalContinuePendingReadCopyToTail
        /// </param>
        /// <param name="pendingContext"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="output"></param>
        /// <param name="lockOperation"></param>
        /// <param name="expectedLogicalAddress">
        /// The expected address of the record being copied.
        /// </param>
        /// <param name="fasterSession"></param>
        /// <param name="currentCtx"></param>
        /// <param name="noReadCache">
        /// If true, it won't clutter read cache. 
        /// Otherwise, it still checks UseReadCache to determine whether to buffer in read cache.
        /// It is useful in Compact.
        /// </param>
        /// <returns>
        /// RETRY_NOW: failed CAS, so no copy done
        /// RECORD_ON_DISK: unable to determine if record present beyond expectedLogicalAddress, so no copy done
        /// NOTFOUND: record was found in memory beyond expectedLogicalAddress, so no copy done
        /// SUCCESS: no record found beyond expectedLogicalAddress, so copy was done
        /// </returns>
        internal OperationStatus InternalTryCopyToTail<Input, Output, Context, FasterSession>(
                                        FasterExecutionContext<Input, Output, Context> opCtx, ref PendingContext<Input, Output, Context> pendingContext,
                                        ref Key key, ref Input input, ref Value value, ref Output output, ref LockOperation lockOperation,
                                        long expectedLogicalAddress,
                                        FasterSession fasterSession,
                                        FasterExecutionContext<Input, Output, Context> currentCtx,
                                        bool noReadCache = false)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            #region Trace back for record in in-memory HybridLog
            // Find the entry in the log and make sure someone didn't insert another record after we decided there wasn't one.
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            var logicalAddress = entry.Address;
            var physicalAddress = default(long);
            var prevTailAddress = pendingContext.recordInfo.PreviousAddress;

            long lastReadCachePhysicalAddress = Constants.kInvalidAddress;
            long prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
            if (UseReadCache)
            {
                prevFirstReadCacheLogicalAddress = logicalAddress;
                SkipReadCache(ref logicalAddress, out lastReadCachePhysicalAddress);
                if (prevFirstReadCacheLogicalAddress == logicalAddress) // if there were no readcache records
                    prevFirstReadCacheLogicalAddress = Constants.kInvalidAddress;
            }
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.HeadAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key,
                                            logicalAddress,
                                            hlog.HeadAddress,
                                            out logicalAddress,
                                            out physicalAddress);
                }
            }

            if (logicalAddress > expectedLogicalAddress)
            {
                // Note1: In Compact, expectedLogicalAddress may not exactly match the source of this copy operation, but instead only an upper bound.
                // Note2: In the case of ReadAtAddress, we will bail here by design; we assume anything in the readcache is the latest version.
                //        Any loop to retrieve prior versions should set ReadFlags.SkipReadCache; see ReadAddressTests.
                if (logicalAddress < hlog.HeadAddress)
                    return OperationStatus.RECORD_ON_DISK;
                else
                    return OperationStatus.NOTFOUND;
            }
            #endregion

            #region Create new copy in mutable region
            var (actualSize, allocatedSize) = hlog.GetRecordSize(ref key, ref value);

            long newLogicalAddress, newPhysicalAddress;
            bool copyToReadCache = !noReadCache && UseReadCache;
            if (copyToReadCache)
            {
                // Copy to readcache should just append at the tail (becoming the first, not last, readcache record in the chain).
                lastReadCachePhysicalAddress = Constants.kInvalidAddress;

                BlockAllocateReadCache(allocatedSize, out newLogicalAddress, currentCtx, fasterSession);
                newPhysicalAddress = readcache.GetPhysicalAddress(newLogicalAddress);
                ref RecordInfo recordInfo = ref readcache.GetInfo(newPhysicalAddress);
                RecordInfo.WriteInfo(ref recordInfo,
                                    inNewVersion: false,
                                    tombstone: false, dirty: false,
                                    entry.Address);

                // Initial readcache entry is tentative.
                recordInfo.Tentative = true;
                readcache.Serialize(ref key, newPhysicalAddress);
                fasterSession.SingleWriter(ref key, ref input, ref value,
                                        ref readcache.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                        ref lockOperation, ref recordInfo, Constants.kInvalidAddress); // We do not expose readcache addresses
            }
            else
            {
                BlockAllocate(allocatedSize, out newLogicalAddress, currentCtx, fasterSession);
                newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(newPhysicalAddress);
                RecordInfo.WriteInfo(ref recordInfo,
                                inNewVersion: opCtx.InNewVersion,
                                tombstone: false, dirty: true,
                                latestLogicalAddress);
                hlog.Serialize(ref key, newPhysicalAddress);
                fasterSession.SingleWriter(ref key, ref input, ref value,
                                        ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                        ref lockOperation, ref recordInfo, newLogicalAddress);
            }

            bool success = true;
            if (lastReadCachePhysicalAddress == Constants.kInvalidAddress)
            {
                // Insert as the first record in the hash chain--this can be either a readcache entry or a main-log entry
                // when there are no readcache records.
                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;
                updatedEntry.ReadCache = copyToReadCache;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word);
                success = foundEntry.word == entry.word;

                if (success && UseReadCache)
                {
                    // See if we have added a main-log entry from an update while we were inserting; if so, the new readcache
                    // record is obsolete and must be Invalidated.
                    ref RecordInfo rcri = ref readcache.GetInfo(newPhysicalAddress);
                    for (var la = entry.Address; la >= prevTailAddress; /* incremented in loop */)
                    {
                        var pa = hlog.GetPhysicalAddress(la);
                        if (comparer.Equals(ref key, ref hlog.GetKey(pa)))
                        {
                            rcri.SetInvalid();
                            break;
                        }
                        la = hlog.GetInfo(pa).PreviousAddress;
                    }
                    if (!rcri.Invalid)
                        rcri.Tentative = false;
                }
            }
            else
            {
                // Splice into the gap of the last readcache/first main log entries.
                ref RecordInfo rcri = ref readcache.GetInfo(lastReadCachePhysicalAddress);
                if (rcri.PreviousAddress != latestLogicalAddress)
                    return OperationStatus.RETRY_NOW;

                // Splice a non-tentative record into the readcache/mainlog gap.
                success = rcri.TryUpdateAddress(newLogicalAddress);

                // Now see if we have added a readcache entry from a pending read while we were inserting; if so it is obsolete and must be Invalidated.
                InvalidateUpdatedRecordInReadCache(entry.word, ref key, prevFirstReadCacheLogicalAddress);
            }

            if (!success)
            {
                if (!copyToReadCache) hlog.GetInfo(newPhysicalAddress).SetInvalid();
                return OperationStatus.RETRY_NOW;
            }
            else
            {
                var log = copyToReadCache ? readcache : hlog;
                ref RecordInfo recordInfo = ref log.GetInfo(newPhysicalAddress);

                if (LockTable.IsActive && !LockTable.ApplyToLogRecord(ref key, ref recordInfo))
                {
                    recordInfo.SetInvalid();
                    recordInfo.Tentative = false;
                    return OperationStatus.RETRY_NOW;
                }
                recordInfo.Tentative = false;

                pendingContext.recordInfo = recordInfo;
                pendingContext.logicalAddress = copyToReadCache ? Constants.kInvalidAddress /* We do not expose readcache addresses */ : newLogicalAddress;
                fasterSession.PostSingleWriter(ref key, ref input, ref value,
                                        ref log.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                        ref lockOperation, ref recordInfo, pendingContext.logicalAddress);
                return OperationStatus.SUCCESS;
            }
#endregion
        }

#endregion

#region Split Index
        private void SplitBuckets(long hash)
        {
            long masked_bucket_index = hash & state[1 - resizeInfo.version].size_mask;
            int offset = (int)(masked_bucket_index >> Constants.kSizeofChunkBits);
            SplitBuckets(offset);
        }

        private void SplitBuckets(int offset)
        {
            int numChunks = (int)(state[1 - resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0) numChunks = 1; // at least one chunk

            if (!Utility.IsPowerOfTwo(numChunks))
            {
                throw new FasterException("Invalid number of chunks: " + numChunks);
            }
            for (int i = offset; i < offset + numChunks; i++)
            {
                if (0 == Interlocked.CompareExchange(ref splitStatus[i & (numChunks - 1)], 1, 0))
                {
                    long chunkSize = state[1 - resizeInfo.version].size / numChunks;
                    long ptr = chunkSize * (i & (numChunks - 1));

                    HashBucket* src_start = state[1 - resizeInfo.version].tableAligned + ptr;
                    HashBucket* dest_start0 = state[resizeInfo.version].tableAligned + ptr;
                    HashBucket* dest_start1 = state[resizeInfo.version].tableAligned + state[1 - resizeInfo.version].size + ptr;

                    SplitChunk(src_start, dest_start0, dest_start1, chunkSize);

                    // split for chunk is done
                    splitStatus[i & (numChunks - 1)] = 2;

                    if (Interlocked.Decrement(ref numPendingChunksToBeSplit) == 0)
                    {
                        // GC old version of hash table
                        state[1 - resizeInfo.version] = default;
                        overflowBucketsAllocatorResize.Dispose();
                        overflowBucketsAllocatorResize = null;
                        GlobalStateMachineStep(systemState);
                        return;
                    }
                    break;
                }
            }

            while (Interlocked.Read(ref splitStatus[offset & (numChunks - 1)]) == 1)
            {
                Thread.Yield();
            }

        }

        private void SplitChunk(
                    HashBucket* _src_start,
                    HashBucket* _dest_start0,
                    HashBucket* _dest_start1,
                    long chunkSize)
        {
            for (int i = 0; i < chunkSize; i++)
            {
                var src_start = _src_start + i;

                long* left = (long*)(_dest_start0 + i);
                long* right = (long*)(_dest_start1 + i);
                long* left_end = left + Constants.kOverflowBucketIndex;
                long* right_end = right + Constants.kOverflowBucketIndex;

                HashBucketEntry entry = default;
                do
                {
                    for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                    {
                        entry.word = *(((long*)src_start) + index);
                        if (Constants.kInvalidEntry == entry.word)
                        {
                            continue;
                        }

                        var logicalAddress = entry.Address;
                        long physicalAddress = 0;

                        if (entry.ReadCache && (entry.Address & ~Constants.kReadCacheBitMask) >= readcache.HeadAddress)
                            physicalAddress = readcache.GetPhysicalAddress(entry.Address & ~Constants.kReadCacheBitMask);
                        else if (logicalAddress >= hlog.HeadAddress)
                                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                        // It is safe to always use hlog instead of readcache for some calls such
                        // as GetKey and GetInfo
                        if (physicalAddress != 0)
                        {
                            var hash = comparer.GetHashCode64(ref hlog.GetKey(physicalAddress));
                            if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == 0)
                            {
                                // Insert in left
                                if (left == left_end)
                                {
                                    var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                    *left = new_bucket_logical;
                                    left = (long*)new_bucket;
                                    left_end = left + Constants.kOverflowBucketIndex;
                                }

                                *left = entry.word;
                                left++;

                                // Insert previous address in right
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 1);
                                if ((entry.Address != Constants.kInvalidAddress) && (entry.Address != Constants.kTempInvalidAddress))
                                {
                                    if (right == right_end)
                                    {
                                        var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                        *right = new_bucket_logical;
                                        right = (long*)new_bucket;
                                        right_end = right + Constants.kOverflowBucketIndex;
                                    }

                                    *right = entry.word;
                                    right++;
                                }
                            }
                            else
                            {
                                // Insert in right
                                if (right == right_end)
                                {
                                    var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                    *right = new_bucket_logical;
                                    right = (long*)new_bucket;
                                    right_end = right + Constants.kOverflowBucketIndex;
                                }

                                *right = entry.word;
                                right++;

                                // Insert previous address in left
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 0);
                                if ((entry.Address != Constants.kInvalidAddress) && (entry.Address != Constants.kTempInvalidAddress))
                                {
                                    if (left == left_end)
                                    {
                                        var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                        *left = new_bucket_logical;
                                        left = (long*)new_bucket;
                                        left_end = left + Constants.kOverflowBucketIndex;
                                    }

                                    *left = entry.word;
                                    left++;
                                }
                            }
                        }
                        else
                        {
                            // Insert in both new locations

                            // Insert in left
                            if (left == left_end)
                            {
                                var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                *left = new_bucket_logical;
                                left = (long*)new_bucket;
                                left_end = left + Constants.kOverflowBucketIndex;
                            }

                            *left = entry.word;
                            left++;

                            // Insert in right
                            if (right == right_end)
                            {
                                var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                *right = new_bucket_logical;
                                right = (long*)new_bucket;
                                right_end = right + Constants.kOverflowBucketIndex;
                            }

                            *right = entry.word;
                            right++;
                        }
                    }

                    if (*(((long*)src_start) + Constants.kOverflowBucketIndex) == 0) break;
                    src_start = (HashBucket*)overflowBucketsAllocatorResize.GetPhysicalAddress(*(((long*)src_start) + Constants.kOverflowBucketIndex));
                } while (true);
            }
        }

        private long TraceBackForOtherChainStart(long logicalAddress, int bit)
        {
            while (true)
            {
                HashBucketEntry entry = default;
                entry.Address = logicalAddress;
                if (entry.ReadCache)
                {
                    if (logicalAddress < readcache.HeadAddress)
                        break;
                    var physicalAddress = readcache.GetPhysicalAddress(logicalAddress);
                    var hash = comparer.GetHashCode64(ref readcache.GetKey(physicalAddress));
                    if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == bit)
                    {
                        return logicalAddress;
                    }
                    logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                }
                else
                {
                    if (logicalAddress < hlog.HeadAddress)
                        break;
                    var physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                    var hash = comparer.GetHashCode64(ref hlog.GetKey(physicalAddress));
                    if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == bit)
                    {
                        return logicalAddress;
                    }
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                }
            }
            return logicalAddress;
        }
#endregion

#region Read Cache
        private bool ReadFromCache(ref Key key, ref long logicalAddress, ref long physicalAddress, out OperationStatus internalStatus)
        {
            // logicalAddress is retrieved from the main FKV's hash table.
            HashBucketEntry entry = default;
            entry.word = logicalAddress;
            internalStatus = OperationStatus.SUCCESS;
            if (!entry.ReadCache) return false;

            physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);

            while (true)
            {
                ref RecordInfo recordInfo = ref readcache.GetInfo(physicalAddress);
                if (!recordInfo.Invalid && comparer.Equals(ref key, ref readcache.GetKey(physicalAddress)))
                {
                    if ((logicalAddress & ~Constants.kReadCacheBitMask) >= readcache.SafeReadOnlyAddress)
                    {
                        // This is a valid readcache record.
                        return !recordInfo.IsIntermediate(out internalStatus);
                    }
                    Debug.Assert((logicalAddress & ~Constants.kReadCacheBitMask) >= readcache.SafeHeadAddress);
                }

                logicalAddress = recordInfo.PreviousAddress;
                entry.word = logicalAddress;
                if (!entry.ReadCache) 
                    break;
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            }

            // Not found in read cache.
            physicalAddress = 0;
            return false;
        }

        // Skip over all readcache records in this key's chain (advancing logicalAddress to the first non-readcache record we encounter).
        private void SkipReadCache(ref long logicalAddress, out long lastReadCachePhysicalAddress)
        {
            HashBucketEntry entry = default;
            entry.word = logicalAddress;
            if (!entry.ReadCache)
            {
                lastReadCachePhysicalAddress = Constants.kInvalidAddress;
                return;
            }

            var physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);

            while (true)
            {
                lastReadCachePhysicalAddress = physicalAddress;
                logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                entry.word = logicalAddress;
                if (!entry.ReadCache) 
                    return;
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            }
        }

        private bool LockReadCacheRecord(long logicalAddress, ref Key key, LockOperation lockOp, out RecordInfo lockInfo, out OperationStatus internalStatus)
        {
            HashBucketEntry entry = default;
            lockInfo = default;
            entry.word = logicalAddress;
            if (!entry.ReadCache)
            {
                internalStatus = OperationStatus.SUCCESS;
                return false;
            }

            var physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);

            while (true)
            {
                ref RecordInfo recordInfo = ref readcache.GetInfo(physicalAddress);
                if (!recordInfo.Invalid && comparer.Equals(ref key, ref readcache.GetKey(physicalAddress)))
                {
                    if ((logicalAddress & ~Constants.kReadCacheBitMask) >= readcache.SafeReadOnlyAddress)
                    {
                        // This is a valid readcache record.
                        if (!recordInfo.IsIntermediate(out internalStatus))
                        {
                            if (lockOp.LockOperationType != LockOperationType.IsLocked)
                                recordInfo.HandleLockOperation(lockOp, out _);
                            lockInfo = recordInfo;
                        }
                        return true;
                    }
                    Debug.Assert((logicalAddress & ~Constants.kReadCacheBitMask) >= readcache.SafeHeadAddress);
                }

                logicalAddress = recordInfo.PreviousAddress;
                entry.word = logicalAddress;
                if (!entry.ReadCache) 
                    break;
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            }

            internalStatus = OperationStatus.SUCCESS;
            return false;
        }

        // Skip over all readcache records in all key chains in this bucket, updating the bucket to point to the first main log record.
        // Called during checkpointing; we create a copy of the hash table page, eliminate read cache pointers from this copy, then write this copy to disk.
        private void SkipReadCacheBucket(HashBucket* bucket)
        {
            for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
            {
                HashBucketEntry* entry = (HashBucketEntry*)&bucket->bucket_entries[index];
                if (0 == entry->word)
                    continue;
                
                if (!entry->ReadCache) continue;
                var logicalAddress = entry->Address;
                var physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);

                while (true)
                {
                    logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                    entry->Address = logicalAddress;
                    if (!entry->ReadCache) 
                        break;
                    physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
                }
            }
        }

        // Skip over all readcache records in this key's chain (advancing logicalAddress to the first non-readcache record we encounter).
        // Invalidate each record we skip over that matches the key.
        private void InvalidateUpdatedRecordInReadCache(long logicalAddress, ref Key key, long untilAddress)
        {
            HashBucketEntry entry = default;
            entry.word = logicalAddress;
            if (!entry.ReadCache)
                return;

            var physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);

            while (logicalAddress != untilAddress)
            {
                // Invalidate read cache entry if key found. This is called when an updated value has been inserted to the main log tail,
                // so instead of waiting just invalidate and return.
                ref RecordInfo recordInfo = ref readcache.GetInfo(physicalAddress);
                if (comparer.Equals(ref key, ref readcache.GetKey(physicalAddress)))
                    recordInfo.SetInvalid();

                logicalAddress = recordInfo.PreviousAddress;
                entry.word = logicalAddress;
                if (!entry.ReadCache)
                    return;
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            }
        }

        private bool SkipAndInvalidateReadCache(ref long logicalAddress, ref Key key, out long lastReadCachePhysicalAddress, out OperationStatus internalStatus)
        {
            internalStatus = OperationStatus.SUCCESS;
            HashBucketEntry entry = default;
            entry.word = logicalAddress;
            if (!entry.ReadCache)
            {
                lastReadCachePhysicalAddress = Constants.kInvalidAddress;
                return true;
            }

            var physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            lastReadCachePhysicalAddress = physicalAddress;

            while (true)
            {
                // Invalidate read cache entry if key found
                ref RecordInfo recordInfo = ref readcache.GetInfo(physicalAddress);
                if (comparer.Equals(ref key, ref readcache.GetKey(physicalAddress)))
                {
                    if (recordInfo.IsIntermediate(out internalStatus))
                        return false;
                    recordInfo.LockExclusive();
                    recordInfo.SetInvalid();
                    recordInfo.UnlockExclusive();
                }

                lastReadCachePhysicalAddress = physicalAddress;
                logicalAddress = recordInfo.PreviousAddress;
                entry.word = logicalAddress;
                if (!entry.ReadCache) 
                    return true;
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            }
        }

        private void ReadCacheEvict(long fromHeadAddress, long toHeadAddress)
        {
            // fromHeadAddress and toHeadAddress are in the readCache
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);

            HashBucketEntry entry = default;
            logicalAddress = fromHeadAddress;

            // Remove readcache entries from the main FKV that are in the fromHeadAddress/toHeadAddress range in the readcache.
            while (logicalAddress < toHeadAddress)
            {
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress);
                var (actualSize, allocatedSize) = readcache.GetRecordSize(physicalAddress);
                ref RecordInfo info = ref readcache.GetInfo(physicalAddress);
                if (!info.Invalid)
                {
                    ref Key key = ref readcache.GetKey(physicalAddress);

                    // If this to-be-evicted readcache record's prevAddress points to a record in the main FKV...
                    entry.word = info.PreviousAddress;
                    if (!entry.ReadCache)
                    {
                        // Find the index entry for the key in the main FKV.
                        var hash = comparer.GetHashCode64(ref key);
                        var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                        entry = default;
                        var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);

                        // Because we call SkipReadCache on upserts, if we have a readcache entry for this hash, it will be pointed to by
                        // the hashtable; there may be other readcache entries as well, before one that is a non-readcache entry.
                        // That is, if there is a readcache entry for this hash, the chain will always be of the form:
                        //      hashtable -> zero or more readcache entries -> main FKV entry.
                        // Remove the readcache entry for this hash from the main FKV, unless somee other thread has done it for us.
                        // Note that this removes the entire leading readcache-entry set of records from the hash table pointer. 
                        while (tagExists && entry.ReadCache)
                        {
                            var updatedEntry = default(HashBucketEntry);
                            updatedEntry.Tag = tag;
                            updatedEntry.Address = info.PreviousAddress;
                            updatedEntry.Pending = entry.Pending;
                            updatedEntry.Tentative = false;

                            if (entry.word == Interlocked.CompareExchange
                                (ref bucket->bucket_entries[slot], updatedEntry.word, entry.word))
                                break;

                            tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
                        }
                    }
                }
                if ((logicalAddress & readcache.PageSizeMask) + allocatedSize > readcache.PageSize)
                {
                    logicalAddress = (1 + (logicalAddress >> readcache.LogPageSizeBits)) << readcache.LogPageSizeBits;
                    continue;
                }
                logicalAddress += allocatedSize;
            }
        }
#endregion
    }
}
