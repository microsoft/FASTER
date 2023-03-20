// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

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
        internal OperationStatus InternalContinuePendingRead<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request,
                                                        ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            ref RecordInfo srcRecordInfo = ref hlog.GetInfoFromBytePointer(request.record.GetValidPointer());
            srcRecordInfo.ClearBitsForDiskImages();

            if (request.logicalAddress >= hlog.BeginAddress)
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

                    // During the pending operation, a record for the key may have been added to the log or readcache.
                    ref var value = ref hlog.GetContextRecordValue(ref request);
                    if (TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext))
                    {
                        srcRecordInfo = ref stackCtx.recSrc.GetInfo();

                        // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
                        if (fasterSession.Ctx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                            return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry
                        value = ref stackCtx.recSrc.GetValue();
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
                        if (stackCtx.recSrc.HasMainLogSrc)
                        {
                            // If this succeeds, we obviously don't need to copy to tail or readcache, so return success.
                            if (fasterSession.ConcurrentReader(ref key, ref pendingContext.input.Get(), ref stackCtx.recSrc.GetValue(),
                                    ref pendingContext.output, ref srcRecordInfo, ref readInfo, out EphemeralLockResult lockResult))
                                return OperationStatus.SUCCESS;
                            if (lockResult == EphemeralLockResult.Failed)
                            {
                                HandleImmediateRetryStatus(OperationStatus.RETRY_LATER, fasterSession, ref pendingContext);
                                continue;
                            }
                        }
                        else
                            success = fasterSession.SingleReader(ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output, ref srcRecordInfo, ref readInfo);

                        if (!success)
                        {
                            pendingContext.recordInfo = srcRecordInfo;
                            if (readInfo.Action == ReadAction.CancelOperation)
                                return OperationStatus.CANCELED;
                            if (readInfo.Action == ReadAction.Expire)
                                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired);
                            goto NotFound;
                        }

                        // See if we are copying to read cache or tail of log.
                        // If we are copying to readcache but already found the record in the readcache, we're done.
                        if (pendingContext.CopyReadsToTail)
                        {
                            status = InternalConditionalCopyToTail(ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output,
                                                                   ref stackCtx, ref srcRecordInfo, fasterSession, WriteReason.CopyToTail);
                        }
                        else if (!stackCtx.recSrc.HasInMemorySrc && UseReadCache && !pendingContext.DisableReadCacheUpdates)
                        {
                            status = InternalTryCopyToReadCache(ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output,
                                                                ref stackCtx, fasterSession);
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
                        TransientSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
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
        /// </list>
        /// </returns>
        internal OperationStatus InternalContinuePendingRMW<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request,
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
                status = InternalRMW(ref key, ref pendingContext.input.Get(), ref pendingContext.output, ref pendingContext.userContext, ref pendingContext, fasterSession, pendingContext.serialNum);
            while (HandleImmediateRetryStatus(status, fasterSession, ref pendingContext));
            return status;
        }

        /// <summary>
        /// Continue a pending CONDITIONAL_* operation with the record retrieved from disk, checking whether a record for this key was
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
        /// </list>
        /// </returns>
        internal OperationStatus InternalContinuePendingConditional<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request,
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

            do
            {
                status = pendingContext.type switch
                {
                    OperationType.CONDITIONAL_CTT => InternalConditionalCTT(request, ref stackCtx, ref pendingContext, fasterSession),
                    OperationType.CONDITIONAL_INSERT => InternalConditionalInsert(request, ref stackCtx, ref pendingContext, fasterSession),
                    OperationType.CONDITIONAL_DELETE => InternalConditionalDelete(request, ref stackCtx, ref pendingContext, fasterSession),
                    _ => throw new FasterException("Unexpected OperationType")
                };
            } while (HandleImmediateRetryStatus(status, fasterSession, ref pendingContext));

            return status;
        }

        private OperationStatus InternalConditionalCTT<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request, ref OperationStackContext<Key, Value> stackCtx, ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession) where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // TODO: make sure the given address is less than read only, or in place updates will mess up the invariant
            throw new NotImplementedException();
        }

        private OperationStatus InternalConditionalInsert<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request, ref OperationStackContext<Key, Value> stackCtx, ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession) where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            throw new NotImplementedException();
        }

        private OperationStatus InternalConditionalDelete<Input, Output, Context, FasterSession>(AsyncIOContext<Key, Value> request, ref OperationStackContext<Key, Value> stackCtx, ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession) where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="FasterSession"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="output"></param>
        /// <param name="untilAddress">Lower-bound address (addresses are searched from tail (high) to head (low); do not search for "future records" earlier than this)</param>
        /// <param name="actualAddress">Actual address of existing key record</param>
        /// <param name="fasterSession"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalCopyToTailForCompaction<Input, Output, Context, FasterSession>(
                                            ref Key key, ref Input input, ref Value value, ref Output output,
                                            long untilAddress, long actualAddress, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is currently only called from Compaction so the epoch should be protected");
            OperationStatus status = default;
            PendingContext<Input, Output, Context> pendingContext = default;
            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            do
            {
                // A 'ref' variable must be initialized. If we find a record for the key, we reassign the reference. We don't copy from this source, but we do lock it.
                RecordInfo dummyRecordInfo = default;
                ref RecordInfo srcRecordInfo = ref dummyRecordInfo;

                // We must check both the readcache as well as transfer if the current record is in the immutable region (Compaction
                // allows copying up to SafeReadOnlyAddress). hei must be set in all cases, because ITCTT relies on it.
                if (!FindTag(ref stackCtx.hei))
                    Debug.Fail("Expected to FindTag in InternalCopyToTailForCompaction");
                stackCtx.SetRecordSourceToHashEntry(hlog);

                if (this.LockTable.IsEnabled && !fasterSession.TryLockTransientShared(ref key, ref stackCtx))
                {
                    HandleImmediateRetryStatus(OperationStatus.RETRY_LATER, fasterSession, ref pendingContext);
                    continue;
                }

                status = OperationStatus.SUCCESS;
                if (actualAddress >= hlog.BeginAddress)
                {
                    // Lookup-based compaction knows the record address.
                    if (actualAddress >= hlog.HeadAddress)
                    {
                        // Since this is for compaction, we don't need to TracebackForKeyMatch; ITCTT will catch the case where a future record was inserted for this key.
                        stackCtx.recSrc.LogicalAddress = actualAddress;
                        stackCtx.recSrc.SetPhysicalAddress();
                        stackCtx.recSrc.HasMainLogSrc = true;
                        srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                    }
                    else
                    {
                        if (TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext))
                            srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                    }
                }
                else
                {
                    // Scan compaction does not know the address, so we must traverse. This is similar to what ITCTT does, but we update untilAddress so it's not done twice.
                    Debug.Assert(actualAddress == Constants.kUnknownAddress, "Unexpected address in compaction");

                    if (TryFindRecordInMemory(ref key, ref stackCtx, hlog.HeadAddress))
                    {
                        if (stackCtx.recSrc.LogicalAddress > untilAddress)   // Same check ITCTT does
                            status = OperationStatus.NOTFOUND;
                        else
                        {
                            untilAddress = stackCtx.recSrc.LatestLogicalAddress;
                            srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                        }
                    }
                }

                if (status == OperationStatus.SUCCESS)
                {
                    try
                    {
                        status = InternalTryCopyToTail(ref pendingContext, ref key, ref input, ref value, ref output,
                                                       ref stackCtx, ref srcRecordInfo, fasterSession, WriteReason.Compaction);
                    }
                    finally
                    {
                        stackCtx.HandleNewRecordOnException(this);
                        TransientSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
                    }
                }
            } while (HandleImmediateRetryStatus(status, fasterSession, ref pendingContext));
            return status;
        }

        /// <summary>
        /// Copy a record from the immutable region of the log, from the disk, or from ConditionalCopyToTail to the tail of the log (or splice into the log/readcache boundary).
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="output"></param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">if <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>, the recordInfo to close, if transferring.</param>
        /// <param name="fasterSession"></param>
        /// <param name="reason">The reason for this operation.</param>
        /// <returns>
        ///     <list type="bullet">
        ///     <item>RETRY_NOW: failed CAS, so no copy done. This routine deals entirely with new records, so will not encounter Sealed records</item>
        ///     <item>SUCCESS: copy was done</item>
        ///     </list>
        /// </returns>
        internal OperationStatus InternalTryCopyToTail<Input, Output, Context, FasterSession>(ref PendingContext<Input, Output, Context> pendingContext,
                                        ref Key key, ref Input input, ref Value value, ref Output output, ref OperationStackContext<Key, Value> stackCtx,
                                        ref RecordInfo srcRecordInfo, FasterSession fasterSession, WriteReason reason)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            #region Create new copy in mutable region
            var (actualSize, allocatedSize) = hlog.GetRecordSize(ref key, ref value);

            #region Allocate new record and call SingleWriter

            if (!TryAllocateRecord(ref pendingContext, ref stackCtx, allocatedSize, recycle: true, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;
            ref var newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: fasterSession.Ctx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.SetNewRecord(newLogicalAddress);

            UpsertInfo upsertInfo = new()
            {
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
                RecordInfo = newRecordInfo
            };

            if (!fasterSession.SingleWriter(ref key, ref input, ref value, ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                            ref output, ref newRecordInfo, ref upsertInfo, reason))
            {
                // No SaveAlloc here as we won't retry, but TODO this record could be reused later.
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                return (upsertInfo.Action == UpsertAction.CancelOperation) ? OperationStatus.CANCELED : OperationStatus.SUCCESS;
            }

            #endregion Allocate new record and call SingleWriter

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            bool success;
            OperationStatus failStatus = OperationStatus.RETRY_NOW;     // Default to CAS-failed status, which does not require an epoch refresh
            if (DoEphemeralLocking)
                newRecordInfo.InitializeLockShared();                   // For PostSingleWriter
            if (stackCtx.recSrc.LowestReadCacheLogicalAddress == Constants.kInvalidAddress)
            {
                // ReadCache entries, and main-log records when there are no readcache records, are CAS'd in as the first entry in the hash chain.
                success = stackCtx.hei.TryCAS(newLogicalAddress);
            }
            else
            {
                // We are doing CopyToTail; we may have a source record from either main log (Compaction) or ReadCache.
                Debug.Assert(reason == WriteReason.CopyToTail || reason == WriteReason.Compaction, "Expected WriteReason.CopyToTail or .Compaction");
                success = SpliceIntoHashChainAtReadCacheBoundary(ref key, ref stackCtx, newLogicalAddress);
            }

            if (success)
                PostInsertAtTail(ref key, ref stackCtx, ref srcRecordInfo);
            else
            {
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);

                // CAS failed, so let the user dispose similar to a deleted record, and save for retry.
                fasterSession.DisposeSingleWriter(ref hlog.GetKey(newPhysicalAddress), ref input, ref value, ref hlog.GetValue(newPhysicalAddress),
                                                    ref output, ref newRecordInfo, ref upsertInfo, reason);
                SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
                return failStatus;
            }

            // Success, and any read locks have been transferred.
            pendingContext.recordInfo = newRecordInfo;
            pendingContext.logicalAddress = upsertInfo.Address;
            fasterSession.PostSingleWriter(ref key, ref input, ref value, ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                           ref newRecordInfo, ref upsertInfo, reason);
            stackCtx.ClearNewRecord();
            return OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.Found | StatusCode.CopiedRecord);
#endregion
        }

        /// <summary>
        /// Copy a record from the disk to the read cache.
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="recordValue"></param>
        /// <param name="output"></param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="fasterSession"></param>
        /// <returns>
        ///     <list type="bullet">
        ///     <item>RETRY_NOW: failed CAS, so no copy done. This routine deals entirely with new records, so will not encounter Sealed records</item>
        ///     <item>SUCCESS: copy was done</item>
        ///     </list>
        /// </returns>
        internal OperationStatus InternalTryCopyToReadCache<Input, Output, Context, FasterSession>(ref PendingContext<Input, Output, Context> pendingContext,
                                        ref Key key, ref Input input, ref Value recordValue, ref Output output, ref OperationStackContext<Key, Value> stackCtx,
                                        FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            #region Create new copy in mutable region
            var (actualSize, allocatedSize) = hlog.GetRecordSize(ref key, ref recordValue);

            #region Allocate new record and call SingleWriter
            
            if (!TryAllocateRecordReadCache(ref pendingContext, ref stackCtx, allocatedSize, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;
            ref var newRecordInfo = ref WriteNewRecordInfo(ref key, readcache, newPhysicalAddress, inNewVersion: false, tombstone: false, stackCtx.hei.Address);
            stackCtx.SetNewRecord(newLogicalAddress | Constants.kReadCacheBitMask);

            UpsertInfo upsertInfo = new()
            {
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
                Address = Constants.kInvalidAddress,        // We do not expose readcache addresses
                KeyHash = stackCtx.hei.hash,
                RecordInfo = newRecordInfo
            };

            var advancedStatusCode = StatusCode.Found | StatusCode.CopiedRecordToReadCache;

            if (!fasterSession.SingleWriter(ref key, ref input, ref recordValue, ref readcache.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                            ref output, ref newRecordInfo, ref upsertInfo, WriteReason.CopyToReadCache))
            {
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                return (upsertInfo.Action == UpsertAction.CancelOperation) ? OperationStatus.CANCELED : OperationStatus.SUCCESS;
            }

            #endregion Allocate new record and call SingleWriter

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            // It is possible that we will successfully CAS but subsequently fail validation.
            OperationStatus failStatus = OperationStatus.RETRY_NOW;     // Default to CAS-failed status, which does not require an epoch refresh

            // ReadCache entries, and main-log records when there are no readcache records, are CAS'd in as the first entry in the hash chain.
            var success = stackCtx.hei.TryCAS(newLogicalAddress | Constants.kReadCacheBitMask);
            var casSuccess = success;

            if (success && stackCtx.recSrc.LowestReadCacheLogicalAddress != Constants.kInvalidAddress)
            {
                // If someone added a main-log entry for this key from an update or CTT while we were inserting the new readcache record, then the new
                // readcache record is obsolete and must be Invalidated. (If LowestReadCacheLogicalAddress == kInvalidAddress, then the CAS would have
                // failed in this case.) If this was the first readcache record in the chain, then once we CAS'd it in someone could have spliced into
                // it, but then that splice will call ReadCacheCheckTailAfterSplice and invalidate it if it's the same key.
                success = EnsureNoNewMainLogRecordWasSpliced(ref key, stackCtx.recSrc, stackCtx.hei.Address, ref failStatus);
            }

            if (!success)
            {
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);

                if (!casSuccess)
                {
                    // Let user dispose similar to a deleted record, and save for retry, *only* if CAS failed; otherwise we must preserve it in the chain.
                    fasterSession.DisposeSingleWriter(ref readcache.GetKey(newPhysicalAddress), ref input, ref recordValue, ref readcache.GetValue(newPhysicalAddress),
                                                      ref output, ref newRecordInfo, ref upsertInfo, WriteReason.CopyToReadCache);
                    newRecordInfo.PreviousAddress = Constants.kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
                }
                return failStatus;
            }

            // Success, and any read locks have been transferred.
            pendingContext.recordInfo = newRecordInfo;
            pendingContext.logicalAddress = upsertInfo.Address;
            fasterSession.PostSingleWriter(ref key, ref input, ref recordValue, ref readcache.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                    ref newRecordInfo, ref upsertInfo, WriteReason.CopyToReadCache);
            stackCtx.ClearNewRecord();
            return OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, advancedStatusCode);
            #endregion
        }
    }
}