// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
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
            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(stackCtx.hei.hash, sessionCtx, fasterSession);

            #region Trace back for record in readcache and in-memory HybridLog

            // This tracks the address pointed to by the hash bucket; it may or may not be in the readcache, in-memory, on-disk, or < BeginAddress (in which
            // case we return NOTFOUND and this value is not used). InternalContinuePendingRead can stop comparing keys immediately above this address.
            long prevHighestKeyHashAddress = Constants.kInvalidAddress;

            var useStartAddress = startAddress != Constants.kInvalidAddress && !pendingContext.HasMinAddress;
            if (!useStartAddress)
            {
                if (!FindTag(ref stackCtx.hei) || (!stackCtx.hei.IsReadCache && stackCtx.hei.Address < pendingContext.minAddress))
                    return OperationStatus.NOTFOUND;
                prevHighestKeyHashAddress = stackCtx.hei.Address;
            }
            else
            {
                if (startAddress < hlog.BeginAddress)
                    return OperationStatus.NOTFOUND;
                stackCtx.hei.entry.Address = startAddress;
            }

            // We have a number of options in Read that are not present in other areas RecordSource is used, and we
            // lock -> call IFunctions -> return immediately if we find a record, so have a different structure than Internal(Updaters).
            stackCtx.SetRecordSourceToHashEntry(hlog);

            OperationStatus status;
            if (UseReadCache)
            {
                // TODO doc: DisableReadCacheReads is used by readAtAddress, e.g. to backtrack to previous versions.
                //       Verify this can be done outside the locking scheme (maybe skip ephemeral locking entirely for readAtAddress)
                if (pendingContext.DisableReadCacheReads || pendingContext.NoKey)
                {
                    SkipReadCache(ref stackCtx.hei, ref stackCtx.recSrc.LogicalAddress);
                    stackCtx.SetRecordSourceToHashEntry(hlog);
                }
                else if (ReadFromCache(ref key, ref input, ref output, ref stackCtx, ref pendingContext, fasterSession, sessionCtx, out status)
                    || status != OperationStatus.SUCCESS)
                {
                    return status;
                }
            }

            // Traceback for key match
            if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
            {
                stackCtx.recSrc.PhysicalAddress = hlog.GetPhysicalAddress(stackCtx.recSrc.LogicalAddress);
                if (!pendingContext.NoKey)
                {
                    var minAddress = pendingContext.minAddress > hlog.HeadAddress ? pendingContext.minAddress : hlog.HeadAddress;
                    TraceBackForKeyMatch(ref key, ref stackCtx.recSrc, minAddress);
                }
                else
                    key = ref hlog.GetKey(stackCtx.recSrc.PhysicalAddress);  // We do not have the key in the call and must use the key from the record.
            }
            #endregion

            if (sessionCtx.phase == Phase.PREPARE && CheckBucketVersionNew(ref stackCtx.hei.entry))
            {
                return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry
            }

            #region Normal processing

            // Mutable region (even fuzzy region is included here)
            if (stackCtx.recSrc.LogicalAddress >= hlog.SafeReadOnlyAddress)
            {
                return ReadFromMutableRegion(ref key, ref input, ref output, ref stackCtx, ref pendingContext, fasterSession, sessionCtx);
            }

            // Immutable region
            else if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
            {
                status = ReadFromImmutableRegion(ref key, ref input, ref output, ref stackCtx, ref pendingContext, fasterSession, sessionCtx);
                if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync)    // May happen due to CopyToTailFromReadOnly
                    goto CreatePendingContext;
                return status;
            }

            // On-Disk Region
            else if (stackCtx.recSrc.LogicalAddress >= hlog.BeginAddress)
            {
#if DEBUG
                SpinWaitUntilAddressIsClosed(stackCtx.recSrc.LogicalAddress, hlog);
                Debug.Assert(!fasterSession.IsManualLocking || ManualLockTable.IsLocked(ref key, ref stackCtx.hei), "A Lockable-session Read() of an on-disk key requires a LockTable lock");
#endif
                // Note: we do not lock here; we wait until reading from disk, then lock in the InternalContinuePendingRead chain.
                if (hlog.IsNullDevice)
                    return OperationStatus.NOTFOUND;

                status = OperationStatus.RECORD_ON_DISK;
                if (sessionCtx.phase == Phase.PREPARE)
                {
                    if (!useStartAddress)
                    {
                        // Failure to latch indicates CPR_SHIFT, but don't hold on to shared latch during IO
                        if (HashBucket.TryAcquireSharedLatch(stackCtx.hei.bucket))
                            HashBucket.ReleaseSharedLatch(stackCtx.hei.bucket);
                        else
                            return OperationStatus.CPR_SHIFT_DETECTED;
                    }
                }

                goto CreatePendingContext;
            }

            // No record found
            else
            {
                Debug.Assert(!fasterSession.IsManualLocking || ManualLockTable.IsLocked(ref key, ref stackCtx.hei), "A Lockable-session Read() of a non-existent key requires a LockTable lock");
                return OperationStatus.NOTFOUND;
            }

        #endregion

        #region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.READ;
                if (!pendingContext.NoKey && pendingContext.key == default)    // If this is true, we don't have a valid key
                    pendingContext.key = hlog.GetKeyContainer(ref key);
                if (pendingContext.input == default)
                    pendingContext.input = fasterSession.GetHeapContainer(ref input);

                pendingContext.output = output;
                if (pendingContext.output is IHeapConvertible heapConvertible)
                    heapConvertible.ConvertToHeap();

                pendingContext.userContext = userContext;
                pendingContext.PrevLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.PrevHighestKeyHashAddress = prevHighestKeyHashAddress;
            }
            #endregion

            return status;
        }

        private bool ReadFromCache<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output,
                                    ref OperationStackContext<Key, Value> stackCtx,
                                    ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (FindInReadCache(ref key, ref stackCtx, untilAddress: Constants.kInvalidAddress, alwaysFindLatestLA: false))
            {
                // Note: When session is in PREPARE phase, a read-cache record cannot be new-version. This is because a new-version record
                // insertion would have invalidated the read-cache entry, and before the new-version record can go to disk become eligible
                // to enter the read-cache, the PREPARE phase for that session will be over due to an epoch refresh.

                // This is not called when looking up by address, so we can set pendingContext.recordInfo.
                ref RecordInfo srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                pendingContext.recordInfo = srcRecordInfo;

                ReadInfo readInfo = new()
                {
                    SessionType = fasterSession.SessionType,
                    Version = sessionCtx.version,
                    Address = Constants.kInvalidAddress,    // ReadCache addresses are not valid for indexing etc. so pass kInvalidAddress.
                    RecordInfo = srcRecordInfo
                };

                if (!TryEphemeralOnlySLock(ref stackCtx.recSrc, ref srcRecordInfo, out status))
                    return false;

                try
                {
                    if (fasterSession.SingleReader(ref key, ref input, ref stackCtx.recSrc.GetSrcValue(), ref output, ref srcRecordInfo, ref readInfo))
                        return true;
                    status = readInfo.Action == ReadAction.CancelOperation ? OperationStatus.CANCELED : OperationStatus.NOTFOUND;
                    return false;
                }
                finally
                {
                    EphemeralSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
                }
            }
            return false;
        }

        private OperationStatus ReadFromMutableRegion<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output,
                                    ref OperationStackContext<Key, Value> stackCtx,
                                    ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // We don't copy from this source, but we do lock it.
            ref var srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
            pendingContext.recordInfo = srcRecordInfo;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

            ReadInfo readInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = sessionCtx.version,
                Address = stackCtx.recSrc.LogicalAddress,
                RecordInfo = srcRecordInfo
            };

            if (!TryEphemeralOnlySLock(ref stackCtx.recSrc, ref srcRecordInfo, out var status))
                return status;

            try
            {
                if (pendingContext.ResetModifiedBit && !srcRecordInfo.TryResetModifiedAtomic())
                    return OperationStatus.RETRY_LATER;
                if (srcRecordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;

                if (fasterSession.ConcurrentReader(ref key, ref input, ref hlog.GetValue(stackCtx.recSrc.PhysicalAddress), ref output, ref srcRecordInfo, ref readInfo))
                    return OperationStatus.SUCCESS;
                if (readInfo.Action == ReadAction.CancelOperation)
                    return OperationStatus.CANCELED;
                if (readInfo.Action == ReadAction.Expire)
                {
                    // Our IFasterSession.ConcurrentReader implementation has already set Tombstone if appropriate.
                    this.MarkPage(stackCtx.recSrc.LogicalAddress, sessionCtx);
                    return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.InPlaceUpdatedRecord | StatusCode.Expired);
                }
                return OperationStatus.NOTFOUND;
            }
            finally
            {
                EphemeralSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
            }
        }

        private OperationStatus ReadFromImmutableRegion<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output,
                                    ref OperationStackContext<Key, Value> stackCtx,
                                    ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // We don't copy from this source, but we do lock it.
            ref var srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
            pendingContext.recordInfo = srcRecordInfo;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

            ReadInfo readInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = sessionCtx.version,
                Address = stackCtx.recSrc.LogicalAddress,
                RecordInfo = srcRecordInfo
            };

            if (!TryEphemeralOnlySLock(ref stackCtx.recSrc, ref srcRecordInfo, out var status))
                return status;

            try
            {
                if (pendingContext.ResetModifiedBit && !srcRecordInfo.TryResetModifiedAtomic())
                    return OperationStatus.RETRY_LATER;
                if (srcRecordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;
                ref Value recordValue = ref stackCtx.recSrc.GetSrcValue();

                if (fasterSession.SingleReader(ref key, ref input, ref recordValue, ref output, ref srcRecordInfo, ref readInfo)
                    || readInfo.Action == ReadAction.Expire)
                {
                    if (pendingContext.CopyReadsToTailFromReadOnly || readInfo.Action == ReadAction.Expire) // Expire adds a tombstoned record to tail
                    {
                        do
                        {
                            status = InternalTryCopyToTail(sessionCtx, ref pendingContext, ref key, ref input, ref recordValue, ref output, ref stackCtx,
                                                            ref srcRecordInfo, untilLogicalAddress: stackCtx.recSrc.LatestLogicalAddress, fasterSession,
                                                            reason: WriteReason.CopyToTail, expired: readInfo.Action == ReadAction.Expire);
                        } while (HandleImmediateRetryStatus(status, sessionCtx, sessionCtx, fasterSession, ref pendingContext));

                        // No copy to tail was done
                        if (status == OperationStatus.NOTFOUND || status == OperationStatus.RECORD_ON_DISK)
                            return readInfo.Action == ReadAction.Expire
                                ? OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired)
                                : OperationStatus.SUCCESS;
                        return status;
                    }
                    return OperationStatus.SUCCESS;
                }
                return OperationStatus.NOTFOUND;
            }
            finally
            {
                // Unlock the record. If doing CopyReadsToTailFromReadOnly, then we have already copied the locks to the new record;
                // this unlocks the source (old) record; the new record may already be operated on by other threads, which is fine.
                EphemeralSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
            }
        }
    }
}
