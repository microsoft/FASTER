// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
                        srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();

                        // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
                        if (fasterSession.Ctx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                            return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry
                        value = ref stackCtx.recSrc.GetSrcValue();
                    }

                    if (!TryTransientSLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out var status))
                    {
                        if (HandleImmediateRetryStatus(status, fasterSession, ref pendingContext))
                            continue;
                        return status;
                    }

                    var expired = false;
                    try
                    {
                        // Wait until after locking to check this.
                        if (srcRecordInfo.Tombstone)
                            goto NotFound;

                        ReadInfo readInfo = new()
                        {
                            SessionType = fasterSession.SessionType,
                            Version = fasterSession.Ctx.version,
                            Address = request.logicalAddress,
                            RecordInfo = srcRecordInfo
                        };

                        bool success = false;
                        if (stackCtx.recSrc.HasMainLogSrc)
                        {
                            // If this succeeds, we obviously don't need to copy to tail or readcache, so return success.
                            if (fasterSession.ConcurrentReader(ref key, ref pendingContext.input.Get(), ref hlog.GetValue(stackCtx.recSrc.PhysicalAddress),
                                    ref pendingContext.output, ref srcRecordInfo, ref readInfo))
                                return OperationStatus.SUCCESS;
                        }
                        else 
                            success = fasterSession.SingleReader(ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output, ref srcRecordInfo, ref readInfo);

                        if (!success)
                        {
                            if (readInfo.Action == ReadAction.CancelOperation)
                            {
                                pendingContext.recordInfo = srcRecordInfo;
                                return OperationStatus.CANCELED;
                            }
                            if (readInfo.Action != ReadAction.Expire)
                                goto NotFound;
                            expired = true;
                        }

                        // See if we are copying to read cache or tail of log. If we are copying to readcache but already found the record in the readcache, we're done.
                        var copyToTail = expired || pendingContext.CopyReadsToTail;
                        var copyToRC = !stackCtx.recSrc.HasInMemorySrc && UseReadCache && !pendingContext.DisableReadCacheUpdates;
                        if (copyToRC || copyToTail)
                        {
                            status = InternalTryCopyToTail(ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output,
                                                ref stackCtx, ref srcRecordInfo, untilLogicalAddress: pendingContext.InitialLatestLogicalAddress,
                                                fasterSession, copyToTail ? WriteReason.CopyToTail : WriteReason.CopyToReadCache);
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
                    {
                        // If no copy to tail was done.
                        if (status == OperationStatus.NOTFOUND || status == OperationStatus.RECORD_ON_DISK)
                            return expired ? OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired) : OperationStatus.SUCCESS;
                        return status;
                    }

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
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
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
                        srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                    }
                    else
                    {
                        if (TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext))
                            srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
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
                            srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                        }
                    }
                }

                if (status == OperationStatus.SUCCESS)
                {
                    try
                    {
                        status = InternalTryCopyToTail(ref pendingContext, ref key, ref input, ref value, ref output,
                                                       ref stackCtx, ref srcRecordInfo, untilAddress, fasterSession, WriteReason.Compaction);
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
        /// Helper function for trying to copy existing immutable records (at foundLogicalAddress) to the tail, used in:
        ///     <list type="bullet">
        ///     <item><see cref="InternalRead{Input, Output, Context, Functions}
        ///                             (ref Key, ref Input, ref Output, long, ref Context, ref PendingContext{Input, Output, Context}, 
        ///                             Functions, long)"/></item>
        ///     <item><see cref="InternalContinuePendingRead{Input, Output, Context, FasterSession}(
        ///                             AsyncIOContext{Key, Value}, ref PendingContext{Input, Output, Context}, 
        ///                             FasterSession)"/>,</item>
        ///     <item><see cref="ClientSession{Key, Value, Input, Output, Context, Functions}
        ///                             .CompactionCopyToTail(ref Key, ref Input, ref Value, ref Output, long, long)"/></item>
        ///     </list>
        /// Succeeds only if the record for the same key hasn't changed.
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="recordValue">The record value; may be superseded by a default value for expiration</param>
        /// <param name="output"></param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">if <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>, the recordInfo to transfer locks from.</param>
        /// <param name="untilLogicalAddress">The expected *main-log* address of the record being copied. This has different meanings depending on the operation:
        ///     <list type="bullet">
        ///         <item>If this is Read() doing a copy of a record in the immutable region, this is the logical address of the source record</item>
        ///         <item>Otherwise, if this is Read(), it is the latestLogicalAddress from the Read()</item>
        ///         <item>If this is Compact(), this is a lower-bound address (addresses are searched from tail (high) to head (low); do not search for "future records" earlier than this)</item>
        ///     </list>
        /// </param>
        /// <param name="fasterSession"></param>
        /// <param name="reason">The reason for this operation.</param>
        /// <param name="expired">If true, this is called to append an expired (Tombstoned) record</param>
        /// <returns>
        ///     <list type="bullet">
        ///     <item>RETRY_NOW: failed CAS, so no copy done. This routine deals entirely with new records, so will not encounter Sealed records</item>
        ///     <item>RECORD_ON_DISK: unable to determine if record present beyond expectedLogicalAddress, so no copy done</item>
        ///     <item>NOTFOUND: record was found in memory beyond expectedLogicalAddress, so no copy done</item>
        ///     <item>SUCCESS: no record found beyond expectedLogicalAddress, so copy was done</item>
        ///     </list>
        /// </returns>
        internal OperationStatus InternalTryCopyToTail<Input, Output, Context, FasterSession>(ref PendingContext<Input, Output, Context> pendingContext,
                                        ref Key key, ref Input input, ref Value recordValue, ref Output output, ref OperationStackContext<Key, Value> stackCtx,
                                        ref RecordInfo srcRecordInfo, long untilLogicalAddress, FasterSession fasterSession, WriteReason reason, bool expired = false)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            #region Trace back for newly-inserted record in HybridLog
            if (stackCtx.recSrc.LatestLogicalAddress > untilLogicalAddress)
            {
                // Entries exist in the log above our last-checked address; another session inserted them after our FindTag. See if there is a newer entry for this key.
                var minAddress = untilLogicalAddress < hlog.HeadAddress ? hlog.HeadAddress : untilLogicalAddress;
                TraceBackForKeyMatch(ref key, stackCtx.recSrc.LatestLogicalAddress, minAddress, out long foundLogicalAddress, out _);
                if (foundLogicalAddress > untilLogicalAddress)
                {
                    // Note: ReadAtAddress bails here by design; we assume anything in the readcache is the latest version.
                    //       Any loop to retrieve prior versions should set ReadFlags.DisableReadCache*; see ReadAddressTests.
                    return foundLogicalAddress < hlog.HeadAddress ? OperationStatus.RECORD_ON_DISK : OperationStatus.NOTFOUND;
                }

                // Update untilLogicalAddress to the latest address we've checked; recSrc.LatestLogicalAddress can be updated by VerifyReadCacheSplicePoint.
                untilLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            }
            #endregion

            #region Create new copy in mutable region
            Value defaultValue = default;
            ref Value value = ref (expired ? ref defaultValue : ref recordValue);
            var (actualSize, allocatedSize) = hlog.GetRecordSize(ref key, ref value);

            UpsertInfo upsertInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
                Address = stackCtx.recSrc.HasInMemorySrc ? stackCtx.recSrc.LogicalAddress : Constants.kInvalidAddress,
                KeyHash = stackCtx.hei.hash
            };

            StatusCode advancedStatusCode = expired ? StatusCode.Expired : StatusCode.Found;

            // A 'ref' variable must be initialized; we'll assign it to the new record we allocate.
            RecordInfo dummyRecordInfo = default;
            ref RecordInfo newRecordInfo = ref dummyRecordInfo;
            AllocatorBase<Key, Value> localLog = hlog;

            #region Allocate new record and call SingleWriter
            long newLogicalAddress, newPhysicalAddress;
            bool copyToReadCache = UseReadCache && reason == WriteReason.CopyToReadCache;
            long readcacheNewAddressBit = 0L;
            if (copyToReadCache)
            {
                localLog = readcache;
                readcacheNewAddressBit = Constants.kReadCacheBitMask;

                if (!TryAllocateRecordReadCache(ref pendingContext, ref stackCtx, allocatedSize, out newLogicalAddress, out newPhysicalAddress, out OperationStatus status))
                    return status;

                newRecordInfo = ref WriteNewRecordInfo(ref key, readcache, newPhysicalAddress, inNewVersion: false, tombstone: false, stackCtx.hei.Address);

                upsertInfo.Address = Constants.kInvalidAddress;     // We do not expose readcache addresses
                advancedStatusCode |= StatusCode.CopiedRecordToReadCache;
                reason = WriteReason.CopyToReadCache;
            }
            else
            {
                if (!TryAllocateRecord(ref pendingContext, ref stackCtx, allocatedSize, recycle: true, out newLogicalAddress, out newPhysicalAddress, out OperationStatus status))
                    return status;

                newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: fasterSession.Ctx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);

                newRecordInfo.Tombstone = expired;
                upsertInfo.Address = newLogicalAddress;
                advancedStatusCode |= StatusCode.CopiedRecord;
                if (reason == WriteReason.CopyToReadCache)
                    reason = WriteReason.CopyToTail;
            }

            stackCtx.SetNewRecord(newLogicalAddress | readcacheNewAddressBit);
            upsertInfo.RecordInfo = newRecordInfo;

            if (!fasterSession.SingleWriter(ref key, ref input, ref value, ref localLog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                            ref output, ref newRecordInfo, ref upsertInfo, reason))
            {
                // No SaveAlloc here, but TODO this record could be reused later.
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                return (upsertInfo.Action == UpsertAction.CancelOperation) ? OperationStatus.CANCELED : OperationStatus.SUCCESS;
            }

            #endregion Allocate new record and call SingleWriter

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            // It is possible that we will successfully CAS but subsequently fail validation.
            bool success = true, casSuccess = false;
            OperationStatus failStatus = OperationStatus.RETRY_NOW;     // Default to CAS-failed status, which does not require an epoch refresh
            if (copyToReadCache || (stackCtx.recSrc.LowestReadCacheLogicalAddress == Constants.kInvalidAddress))
            {
                Debug.Assert(!stackCtx.hei.IsReadCache || (readcacheNewAddressBit != 0), $"Inconsistent IsReadCache ({stackCtx.hei.IsReadCache}) vs. readcacheNewAddressBit ({readcacheNewAddressBit})");

                // ReadCache entries, and main-log records when there are no readcache records, are CAS'd in as the first entry in the hash chain.
                success = casSuccess = stackCtx.hei.TryCAS(newLogicalAddress | readcacheNewAddressBit);

                if (success && copyToReadCache && stackCtx.recSrc.LowestReadCacheLogicalAddress != Constants.kInvalidAddress)
                {
                    // If someone added a main-log entry for this key from an update or CTT while we were inserting the new readcache record, then the new
                    // readcache record is obsolete and must be Invalidated. (If LowestReadCacheLogicalAddress == kInvalidAddress, then the CAS would have
                    // failed in this case.) If this was the first readcache record in the chain, then once we CAS'd it in someone could have spliced into
                    // it, but then that splice will call ReadCacheCheckTailAfterSplice and invalidate it if it's the same key.
                    success = EnsureNoNewMainLogRecordWasSpliced(ref key, stackCtx.recSrc, untilLogicalAddress, ref failStatus);
                }
                if (success)
                    CompleteCopyToTail(ref key, ref stackCtx, ref srcRecordInfo);
            }
            else
            {
                Debug.Assert(readcacheNewAddressBit == 0, "Must not be inserting a readcache record here");

                // We are doing CopyToTail; we may have a source record from either main log (Compaction) or ReadCache, or have a LockTable lock.
                Debug.Assert(reason == WriteReason.CopyToTail || reason == WriteReason.Compaction, "Expected WriteReason.CopyToTail or .Compaction");
                success = casSuccess = SpliceIntoHashChainAtReadCacheBoundary(ref stackCtx.recSrc, newLogicalAddress);
            }

            if (!success)
            {
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);

                if (!casSuccess)
                {
                    // Let user dispose similar to a deleted record, and save for retry, *only* if CAS failed; otherwise we must preserve it in the chain.
                    fasterSession.DisposeSingleWriter(ref localLog.GetKey(newPhysicalAddress), ref input, ref value, ref localLog.GetValue(newPhysicalAddress), ref output, ref newRecordInfo, ref upsertInfo, reason);
                    newRecordInfo.PreviousAddress = Constants.kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
                    if (!copyToReadCache)
                        SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
                }
                return failStatus;
            }

            // Success, and any read locks have been transferred.
            pendingContext.recordInfo = newRecordInfo;
            pendingContext.logicalAddress = upsertInfo.Address;
            fasterSession.PostSingleWriter(ref key, ref input, ref value,
                                    ref localLog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                    ref newRecordInfo, ref upsertInfo, reason);
            if (pendingContext.ResetModifiedBit)
            {
                newRecordInfo.Modified = false;
                pendingContext.recordInfo = newRecordInfo;
            }
            stackCtx.ClearNewRecord();
            return OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, advancedStatusCode);
#endregion
        }
    }
}