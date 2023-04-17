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
        internal OperationStatus InternalRead<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output,
                                    long startAddress, ref Context userContext, ref PendingContext<Input, Output, Context> pendingContext,
                                    FasterSession fasterSession, long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (fasterSession.Ctx.phase != Phase.REST)
                HeavyEnter(stackCtx.hei.hash, fasterSession.Ctx, fasterSession);

            #region Trace back for record in readcache and in-memory HybridLog

            var useStartAddress = startAddress != Constants.kInvalidAddress && !pendingContext.HasMinAddress;
            if (!useStartAddress)
            {
                if (!FindTag(ref stackCtx.hei) || (!stackCtx.hei.IsReadCache && stackCtx.hei.Address < pendingContext.minAddress))
                    return OperationStatus.NOTFOUND;
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
                // DisableReadCacheReads is used by readAtAddress, e.g. to backtrack to previous versions.
                if (pendingContext.NoKey)
                {
                    SkipReadCache(ref stackCtx, out _);     // This may refresh, but we haven't examined HeadAddress yet
                    stackCtx.SetRecordSourceToHashEntry(hlog);
                }
                else if (ReadFromCache(ref key, ref input, ref output, ref stackCtx, ref pendingContext, fasterSession, out status)
                    || status != OperationStatus.SUCCESS)
                {
                    return status;
                }
            }

            // Traceback for key match
            if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
            {
                stackCtx.recSrc.SetPhysicalAddress();
                if (!pendingContext.NoKey)
                {
                    var minAddress = pendingContext.minAddress > hlog.HeadAddress ? pendingContext.minAddress : hlog.HeadAddress;
                    TraceBackForKeyMatch(ref key, ref stackCtx.recSrc, minAddress);
                }
                else
                    key = ref stackCtx.recSrc.GetKey();     // We do not have the key in the call and must use the key from the record.
            }
            #endregion

            // Track the latest searched-below addresses. They are the same if there are no readcache records.
            pendingContext.InitialEntryAddress = stackCtx.hei.Address;
            pendingContext.InitialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;

            // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
            if (fasterSession.Ctx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry

            #region Normal processing

            if (stackCtx.recSrc.LogicalAddress >= hlog.SafeReadOnlyAddress)
            {
                // Mutable region (even fuzzy region is included here)
                return ReadFromMutableRegion(ref key, ref input, ref output, useStartAddress, ref stackCtx, ref pendingContext, fasterSession);
            }
            else if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
            {
                // Immutable region
                status = ReadFromImmutableRegion(ref key, ref input, ref output, ref userContext, lsn, useStartAddress, ref stackCtx, ref pendingContext, fasterSession);
                if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync)    // May happen due to CopyToTailFromReadOnly
                    goto CreatePendingContext;
                return status;
            }
            else if (stackCtx.recSrc.LogicalAddress >= hlog.BeginAddress)
            {
                // On-Disk Region
                Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLocked(ref key, ref stackCtx.hei), "A Lockable-session Read() of an on-disk key requires a LockTable lock");

                // Note: we do not lock here; we wait until reading from disk, then lock in the InternalContinuePendingRead chain.
                if (hlog.IsNullDevice)
                    return OperationStatus.NOTFOUND;

                status = OperationStatus.RECORD_ON_DISK;
                goto CreatePendingContext;
            }
            else
            {
                // No record found
                Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLocked(ref key, ref stackCtx.hei), "A Lockable-session Read() of a non-existent key requires a LockTable lock");
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
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                pendingContext.version = fasterSession.Ctx.version;
                pendingContext.serialNum = lsn;
            }
            #endregion

            return status;
        }

        private bool ReadFromCache<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output, ref OperationStackContext<Key, Value> stackCtx,
                                    ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (FindInReadCache(ref key, ref stackCtx, minAddress: Constants.kInvalidAddress, alwaysFindLatestLA: false))
            {
                // Note: When session is in PREPARE phase, a read-cache record cannot be new-version. This is because a new-version record
                // insertion would have invalidated the read-cache entry, and before the new-version record can go to disk become eligible
                // to enter the read-cache, the PREPARE phase for that session will be over due to an epoch refresh.

                // This is not called when looking up by address, so we can set pendingContext.recordInfo.
                ref RecordInfo srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                pendingContext.recordInfo = srcRecordInfo;

                ReadInfo readInfo = new()
                {
                    Version = fasterSession.Ctx.version,
                    Address = Constants.kInvalidAddress,    // ReadCache addresses are not valid for indexing etc. so pass kInvalidAddress.
                    RecordInfo = srcRecordInfo
                };

                if (!TryTransientSLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out status))
                    return false;

                try
                {
                    if (fasterSession.SingleReader(ref key, ref input, ref stackCtx.recSrc.GetValue(), ref output, ref srcRecordInfo, ref readInfo))
                        return true;
                    status = readInfo.Action == ReadAction.CancelOperation ? OperationStatus.CANCELED : OperationStatus.NOTFOUND;
                    return false;
                }
                finally
                {
                    TransientSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
                }
            }
            return false;
        }

        private OperationStatus ReadFromMutableRegion<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output,
                                    bool useStartAddress, ref OperationStackContext<Key, Value> stackCtx,
                                    ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // We don't copy from this source, but we do lock it.
            ref var srcRecordInfo = ref stackCtx.recSrc.GetInfo();
            pendingContext.recordInfo = srcRecordInfo;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

            ReadInfo readInfo = new()
            {
                Version = fasterSession.Ctx.version,
                Address = stackCtx.recSrc.LogicalAddress,
                RecordInfo = srcRecordInfo
            };

            // If we are starting from a specified address in the immutable region, we may have a Sealed record from a previous RCW.
            // For this case, do not try to lock, TransientSUnlock will see that we do not have a lock so will not try to update it.
            OperationStatus status = OperationStatus.SUCCESS;
            if (!useStartAddress && !TryTransientSLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out status))
                return status;

            try
            {
                if (srcRecordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;

                if (fasterSession.ConcurrentReader(ref key, ref input, ref stackCtx.recSrc.GetValue(), ref output, ref srcRecordInfo, ref readInfo, out stackCtx.recSrc.ephemeralLockResult))
                    return OperationStatus.SUCCESS;
                if (stackCtx.recSrc.ephemeralLockResult == EphemeralLockResult.Failed)
                    return OperationStatus.RETRY_LATER;
                if (readInfo.Action == ReadAction.CancelOperation)
                    return OperationStatus.CANCELED;
                if (readInfo.Action == ReadAction.Expire)
                    return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired);
                return OperationStatus.NOTFOUND;
            }
            finally
            {
                TransientSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
            }
        }

        private OperationStatus ReadFromImmutableRegion<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output,
                                    ref Context userContext, long lsn, bool useStartAddress, ref OperationStackContext<Key, Value> stackCtx,
                                    ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // We don't copy from this source, but we do lock it.
            ref var srcRecordInfo = ref stackCtx.recSrc.GetInfo();
            pendingContext.recordInfo = srcRecordInfo;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

            ReadInfo readInfo = new()
            {
                Version = fasterSession.Ctx.version,
                Address = stackCtx.recSrc.LogicalAddress,
                RecordInfo = srcRecordInfo
            };

            // If we are starting from a specified address in the immutable region, we may have a Sealed record from a previous RCW.
            // For this case, do not try to lock, TransientSUnlock will see that we do not have a lock so will not try to update it.
            OperationStatus status = OperationStatus.SUCCESS;
            if (!useStartAddress && !TryTransientSLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out status))
                return status;

            try
            {
                if (srcRecordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;
                ref Value recordValue = ref stackCtx.recSrc.GetValue();

                if (fasterSession.SingleReader(ref key, ref input, ref recordValue, ref output, ref srcRecordInfo, ref readInfo))
                {
                    if (pendingContext.readCopyOptions.CopyFrom != ReadCopyFrom.AllImmutable)
                        return OperationStatus.SUCCESS;
                    if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.MainLog)
                        return ConditionalCopyToTail(fasterSession, ref pendingContext, ref key, ref input, ref recordValue, ref output, ref userContext, lsn, ref stackCtx, WriteReason.CopyToTail);
                    if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.ReadCache
                            && TryCopyToReadCache(fasterSession, ref pendingContext, ref key, ref input, ref recordValue, ref stackCtx))
                        return OperationStatus.SUCCESS | OperationStatus.COPIED_RECORD_TO_READ_CACHE;
                }
                if (readInfo.Action == ReadAction.CancelOperation)
                    return OperationStatus.CANCELED;
                if (readInfo.Action == ReadAction.Expire)
                    return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired);
                return OperationStatus.NOTFOUND;
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                TransientSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
            }
        }
    }
}
