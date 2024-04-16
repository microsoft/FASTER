// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// Continue a pending read operation. Computes 'output' from 'input' and value corresponding to 'key'
        /// obtained from disk. Optionally, it copies the value to tail to serve future read/write requests quickly.
        /// </summary>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
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
        internal OperationStatus ContinuePendingRead<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request,
                                                        ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            ref RecordInfo srcRecordInfo = ref hlog.GetInfoFromBytePointer(request.record.GetValidPointer());
            srcRecordInfo.ClearBitsForDiskImages();

            if (request.logicalAddress >= hlog.BeginAddress && request.logicalAddress >= pendingContext.minAddress)
            {
                SpinWaitUntilClosed(request.logicalAddress);

                // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
                ref Key key = ref pendingContext.NoKey ? ref hlog.GetContextRecordKey(ref request) : ref pendingContext.key.Get();
                OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

                while (true)
                {
                    if (!FindTag(ref stackCtx.hei))
                        Debug.Fail("Expected to FindTag in InternalContinuePendingRead");
                    stackCtx.SetRecordSourceToHashEntry(hlog);

                    ref var value = ref hlog.GetContextRecordValue(ref request);

                    // During the pending operation, a record for the key may have been added to the log or readcache. If we had a StartAddress we ignore this.
                    if (!pendingContext.HadStartAddress)
                    {
                        if (TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext))
                        {
                            srcRecordInfo = ref stackCtx.recSrc.GetInfo();

                            // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
                            if (fasterSession.Ctx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                                return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry
                            value = ref stackCtx.recSrc.GetValue();
                        }
                        else
                        {
                            // We didn't find a record for the key in memory, but if recSrc.LogicalAddress (which is the .PreviousAddress of the lowest record
                            // above InitialLatestLogicalAddress we could reach) is > InitialLatestLogicalAddress, then it means InitialLatestLogicalAddress is
                            // now below HeadAddress and there is at least one record below HeadAddress but above InitialLatestLogicalAddress. Reissue the Read(),
                            // using the LogicalAddress we just found as minAddress. We will either find an in-memory version of the key that was added after the
                            // TryFindRecordInMemory we just did, or do IO and find the record we just found or one above it. Read() updates InitialLatestLogicalAddress,
                            // so if we do IO, the next time we come to CompletePendingRead we will only search for a newer version of the key in any records added
                            // after our just-completed TryFindRecordInMemory.
                            if (stackCtx.recSrc.LogicalAddress > pendingContext.InitialLatestLogicalAddress
                                && (!pendingContext.HasMinAddress || stackCtx.recSrc.LogicalAddress >= pendingContext.minAddress))
                            {
                                OperationStatus internalStatus;
                                do
                                {
                                    internalStatus = InternalRead(ref key, pendingContext.keyHash, ref pendingContext.input.Get(), ref pendingContext.output,
                                        startAddress: Constants.kInvalidAddress, ref pendingContext.userContext, ref pendingContext, fasterSession, pendingContext.serialNum);
                                }
                                while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pendingContext));
                                return internalStatus;
                            }
                        }
                    }

                    if (!TryTransientSLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out var status))
                    {
                        if (HandleImmediateRetryStatus(status, fasterSession, ref pendingContext))
                            continue;
                        return status;
                    }

                    try
                    {
                        // Wait until after locking to check this.
                        if (srcRecordInfo.Tombstone)
                            goto NotFound;

                        ReadInfo readInfo = new()
                        {
                            Version = fasterSession.Ctx.version,
                            Address = request.logicalAddress,
                            RecordInfo = srcRecordInfo
                        };

                        bool success = false;
                        if (stackCtx.recSrc.HasMainLogSrc && stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress)
                        {
                            // If this succeeds, we don't need to copy to tail or readcache, so return success.
                            if (fasterSession.ConcurrentReader(ref key, ref pendingContext.input.Get(), ref value,
                                    ref pendingContext.output, ref srcRecordInfo, ref readInfo, out EphemeralLockResult lockResult))
                                return OperationStatus.SUCCESS;
                            if (lockResult == EphemeralLockResult.Failed)
                            {
                                HandleImmediateRetryStatus(OperationStatus.RETRY_LATER, fasterSession, ref pendingContext);
                                continue;
                            }
                        }
                        else
                        {
                            // This may be in the immutable region, which means it may be an updated version of the record.
                            success = fasterSession.SingleReader(ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output, ref srcRecordInfo, ref readInfo);
                        }

                        if (!success)
                        {
                            pendingContext.recordInfo = srcRecordInfo;
                            if (readInfo.Action == ReadAction.CancelOperation)
                                return OperationStatus.CANCELED;
                            if (readInfo.Action == ReadAction.Expire)
                                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired);
                            goto NotFound;
                        }

                        // See if we are copying to read cache or tail of log. If we are copying to readcache but already found the record in the readcache, we're done.
                        if (pendingContext.readCopyOptions.CopyFrom != ReadCopyFrom.None)
                        {
                            if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.MainLog)
                                status = ConditionalCopyToTail(fasterSession, ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output,
                                                               ref pendingContext.userContext, pendingContext.serialNum, ref stackCtx, WriteReason.CopyToTail);
                            else if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.ReadCache && !stackCtx.recSrc.HasReadCacheSrc
                                    && TryCopyToReadCache(fasterSession, ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref stackCtx))
                                status |= OperationStatus.COPIED_RECORD_TO_READ_CACHE;
                        }
                        else
                        {
                            pendingContext.recordInfo = srcRecordInfo;
                            return OperationStatus.SUCCESS;
                        }
                    }
                    finally
                    {
                        stackCtx.HandleNewRecordOnException(this);
                        TransientSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
                    }

                    // Must do this *after* Unlocking. Status was set by InternalTryCopyToTail.
                    if (!HandleImmediateRetryStatus(status, fasterSession, ref pendingContext))
                        return status;
                } // end while (true)
            }

        NotFound:
            pendingContext.recordInfo = srcRecordInfo;
            return OperationStatus.NOTFOUND;
        }

        /// <summary>
        /// Continue a pending RMW operation with the record retrieved from disk.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="fasterSession">Callback functions.</param>
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
        ///     <term>We need to issue an IO to continue.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus ContinuePendingRMW<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request,
                                                ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            ref Key key = ref pendingContext.key.Get();

            SpinWaitUntilClosed(request.logicalAddress);

            byte* recordPointer = request.record.GetValidPointer();
            var srcRecordInfo = hlog.GetInfoFromBytePointer(recordPointer); // Not ref, as we don't want to write into request.record
            srcRecordInfo.ClearBitsForDiskImages();

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));
            OperationStatus status;

            while (true)
            {
                FindOrCreateTag(ref stackCtx.hei, hlog.BeginAddress);
                stackCtx.SetRecordSourceToHashEntry(hlog);

                // During the pending operation, a record for the key may have been added to the log. If so, go through the full InternalRMW
                // sequence; the record in 'request' is stale.
                if (TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext))
                    break;

                // We didn't find a record for the key in memory, but if recSrc.LogicalAddress (which is the .PreviousAddress of the lowest record
                // above InitialLatestLogicalAddress we could reach) is > InitialLatestLogicalAddress, then it means InitialLatestLogicalAddress is
                // now below HeadAddress and there is at least one record below HeadAddress but above InitialLatestLogicalAddress. We must do InternalRMW.
                if (stackCtx.recSrc.LogicalAddress > pendingContext.InitialLatestLogicalAddress)
                {
                    Debug.Assert(pendingContext.InitialLatestLogicalAddress < hlog.HeadAddress, "Failed to search all in-memory records");
                    break;
                }

                if (!TryTransientXLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out status))
                    goto CheckRetry;

                try
                {
                    // Here, the input data for 'doingCU' is the from the request, so populate the RecordSource copy from that, preserving LowestReadCache*.
                    stackCtx.recSrc.LogicalAddress = request.logicalAddress;
                    stackCtx.recSrc.PhysicalAddress = (long)recordPointer;

                    status = CreateNewRecordRMW(ref key, ref pendingContext.input.Get(), ref hlog.GetContextRecordValue(ref request), ref pendingContext.output,
                                                ref pendingContext, fasterSession, ref stackCtx, ref srcRecordInfo,
                                                doingCU: request.logicalAddress >= hlog.BeginAddress && !srcRecordInfo.Tombstone);
                }
                finally
                {
                    stackCtx.HandleNewRecordOnException(this);
                    TransientXUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
                }

            // Must do this *after* Unlocking. Retries should drop down to InternalRMW
            CheckRetry:
                if (!HandleImmediateRetryStatus(status, fasterSession, ref pendingContext))
                    return status;
            } // end while (true)

            do
                status = InternalRMW(ref key, pendingContext.keyHash, ref pendingContext.input.Get(), ref pendingContext.output, ref pendingContext.userContext, ref pendingContext, fasterSession, pendingContext.serialNum);
            while (HandleImmediateRetryStatus(status, fasterSession, ref pendingContext));
            return status;
        }

        /// <summary>
        /// Continue a pending CONDITIONAL_INSERT operation with the record retrieved from disk, checking whether a record for this key was
        /// added since we went pending; in that case this operation must be adjusted to use current data.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully inserted, or was found above the specified address.</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>We need to issue an IO to continue.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus ContinuePendingConditionalCopyToTail<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request,
                                                ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // If the key was found at or above minAddress, do nothing.
            if (request.logicalAddress >= pendingContext.minAddress)
                return OperationStatus.SUCCESS;

            // Prepare to copy to tail. Use data from pendingContext, not request; we're only made it to this line if the key was not found, and thus the request was not populated.
            ref Key key = ref pendingContext.key.Get();
            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            // See if the record was added above the highest address we checked before issuing the IO.
            var minAddress = pendingContext.InitialLatestLogicalAddress + 1;
            if (TryFindRecordInMainLogForConditionalCopyToTail(ref key, ref stackCtx, minAddress, out bool needIO))
                return OperationStatus.SUCCESS;

            // HeadAddress may have risen above minAddress; if so, we need IO.
            if (needIO)
                return PrepareIOForConditionalCopyToTail(fasterSession, ref pendingContext, ref key, ref pendingContext.input.Get(), ref pendingContext.value.Get(),
                                                    ref pendingContext.output, ref pendingContext.userContext, pendingContext.serialNum, ref stackCtx, minAddress, WriteReason.Compaction);

            // No IO needed. 
            return ConditionalCopyToTail(fasterSession, ref pendingContext, ref key, ref pendingContext.input.Get(), ref pendingContext.value.Get(),
                                     ref pendingContext.output, ref pendingContext.userContext, pendingContext.serialNum, ref stackCtx, pendingContext.writeReason);
        }
    }
}