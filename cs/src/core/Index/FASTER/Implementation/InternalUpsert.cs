// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
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
        internal OperationStatus InternalUpsert<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Value value, ref Output output,
                            ref Context userContext, ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession, long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var latchOperation = LatchOperation.None;
            var latchDestination = LatchDestination.NormalProcessing;

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (fasterSession.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            FindOrCreateTag(ref stackCtx.hei, hlog.BeginAddress);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            // We blindly insert if we don't find the record in the mutable region.
            RecordInfo dummyRecordInfo = new() { Valid = true };
            ref RecordInfo srcRecordInfo = ref TryFindRecordInMemory(ref key, ref stackCtx, hlog.ReadOnlyAddress)
                ? ref stackCtx.recSrc.GetInfo()
                : ref dummyRecordInfo;
            if (srcRecordInfo.IsClosed)
                return OperationStatus.RETRY_LATER;

            // Note: we do not track pendingContext.Initial*Address because we don't have an InternalContinuePendingUpsert

            UpsertInfo upsertInfo = new()
            {
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
                Address = stackCtx.recSrc.LogicalAddress,
                KeyHash = stackCtx.hei.hash
            };

            if (!TryTransientXLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out OperationStatus status))
                return status;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // Revivification or some other operation may have sealed the record while we waited for the lock.
                if (srcRecordInfo.IsClosed)
                    return OperationStatus.RETRY_LATER;

                #region Address and source record checks

                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    // Use the readcache record as the CopyUpdater source.
                    goto CreateNewRecord;
                }

                // Check for CPR consistency after checking if source is readcache.
                if (fasterSession.Ctx.phase != Phase.REST)
                {
                    latchDestination = CheckCPRConsistencyUpsert(fasterSession.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    if (latchDestination == LatchDestination.Retry)
                        goto LatchRelease;
                }

                if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress && latchDestination == LatchDestination.NormalProcessing)
                {
                    // Mutable Region: Update the record in-place. We perform mutable updates only if we are in normal processing phase of checkpointing
                    ref Value recordValue = ref stackCtx.recSrc.GetValue();
                    if (srcRecordInfo.Tombstone)
                    {
                        // Try in-place revivification of the record.
                        if (srcRecordInfo.Filler)
                        {
                            if (!this.LockTable.IsEnabled && !srcRecordInfo.TrySeal())
                                return OperationStatus.RETRY_NOW;
                            bool ok = true;
                            try
                            { 
                                if (srcRecordInfo.Tombstone && srcRecordInfo.Filler)
                                {
                                    srcRecordInfo.Tombstone = false;

                                    if (IsFixedLengthReviv)
                                        upsertInfo.UsedValueLength = upsertInfo.FullValueLength = FixedLengthStruct<Value>.Length;
                                    else
                                    {
                                        upsertInfo.FullValueLength = GetTombstonedValueLength(stackCtx.recSrc.PhysicalAddress, ref srcRecordInfo);

                                        // Upsert uses GetRecordSize because it has both the initial Input and Value
                                        var (actualSize, _) = hlog.GetRecordSize(ref key, ref input, ref value, fasterSession);
                                        (ok, upsertInfo.UsedValueLength) = TryReinitializeTombstonedValue(stackCtx.recSrc.PhysicalAddress, actualSize, ref srcRecordInfo, ref recordValue, upsertInfo.FullValueLength);
                                    }

                                    upsertInfo.RecordInfo = srcRecordInfo;
                                    if (ok && fasterSession.SingleWriter(ref key, ref input, ref value, ref recordValue, ref output, ref srcRecordInfo, ref upsertInfo, WriteReason.Upsert))
                                    {
                                        this.MarkPage(stackCtx.recSrc.LogicalAddress, fasterSession.Ctx);
                                        pendingContext.recordInfo = srcRecordInfo;
                                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                                        goto LatchRelease;
                                    }
                                }
                            }
                            finally
                            {
                                SetLiveFullValueLength(stackCtx.recSrc.PhysicalAddress, ref recordValue, ref srcRecordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);
                                srcRecordInfo.Unseal();
                                if (!ok)
                                    srcRecordInfo.Tombstone = true; // Restore tombstone on inability to update in place
                            }
                        }
                        goto CreateNewRecord;
                    }

                    upsertInfo.RecordInfo = srcRecordInfo;
                    if (fasterSession.ConcurrentWriter(stackCtx.recSrc.PhysicalAddress, ref key, ref input, ref value, ref recordValue, ref output, ref srcRecordInfo, 
                                                       ref upsertInfo, out stackCtx.recSrc.ephemeralLockResult))
                    {
                        this.MarkPage(stackCtx.recSrc.LogicalAddress, fasterSession.Ctx);
                        pendingContext.recordInfo = srcRecordInfo;
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                        goto LatchRelease;
                    }
                    if (stackCtx.recSrc.ephemeralLockResult == EphemeralLockResult.Failed)
                    {
                        status = OperationStatus.RETRY_LATER;
                        goto LatchRelease;
                    }
                    if (upsertInfo.Action == UpsertAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        goto LatchRelease;
                    }

                    // ConcurrentWriter failed (e.g. insufficient space, another thread set Tombstone, etc). Write a new record, but track that we have to seal and unlock this one.
                    goto CreateNewRecord;
                }
                else if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                {
                    goto CreateNewRecord;
                }
                else
                {
                    // Either on-disk or no record exists - create new record.
                    Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, ref stackCtx.hei), "A Lockable-session Upsert() of an on-disk or non-existent key requires a LockTable lock");
                    goto CreateNewRecord;
                }
            #endregion Address and source record checks

            #region Create new record in the mutable region
            CreateNewRecord:
                if (latchDestination != LatchDestination.CreatePendingContext)
                {
                    // Immutable region or new record
                    status = CreateNewRecordUpsert(ref key, ref input, ref value, ref output, ref pendingContext, fasterSession, ref stackCtx, ref srcRecordInfo);
                    if (!OperationStatusUtils.IsAppend(status))
                    {
                        // We should never return "SUCCESS" for a new record operation: it returns NOTFOUND on success.
                        Debug.Assert(OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS);
                        if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync)
                        {
                            latchDestination = LatchDestination.CreatePendingContext;
                            goto CreatePendingContext;
                        }
                    }
                    goto LatchRelease;
                }
                #endregion Create new record
            }
            finally
            {
                // On success, we call UnlockAndSeal. Non-success includes the source address going below HeadAddress, in which case we rely on
                // recordInfo.ClearBitsForDiskImages clearing locks and Seal.
                if (stackCtx.recSrc.ephemeralLockResult == EphemeralLockResult.HoldForSeal && stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress && srcRecordInfo.IsLocked)
                    srcRecordInfo.UnlockExclusive();
                stackCtx.HandleNewRecordOnException(this);
                TransientXUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
            }

        #region Create pending context
        CreatePendingContext:
            Debug.Assert(latchDestination == LatchDestination.CreatePendingContext, $"Upsert CreatePendingContext encountered latchDest == {latchDestination}");
            {
                pendingContext.type = OperationType.UPSERT;
                if (pendingContext.key == default) 
                    pendingContext.key = hlog.GetKeyContainer(ref key);
                if (pendingContext.input == default) 
                    pendingContext.input = fasterSession.GetHeapContainer(ref input);
                if (pendingContext.value == default) 
                    pendingContext.value = hlog.GetValueContainer(ref value);

                pendingContext.output = output;
                if (pendingContext.output is IHeapConvertible heapConvertible)
                    heapConvertible.ConvertToHeap();

                pendingContext.userContext = userContext;
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                pendingContext.version = fasterSession.Ctx.version;
                pendingContext.serialNum = lsn;
            }
        #endregion

        #region Latch release
        LatchRelease:
            {
                if (latchOperation != LatchOperation.None)
                {
                    switch (latchOperation)
                    {
                        case LatchOperation.Shared:
                            HashBucket.ReleaseSharedLatch(ref stackCtx.hei);
                            break;
                        case LatchOperation.Exclusive:
                            HashBucket.ReleaseExclusiveLatch(ref stackCtx.hei);
                            break;
                        default:
                            break;
                    }
                }
            }
            #endregion

            return status;
        }

        private LatchDestination CheckCPRConsistencyUpsert(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            if (!this.DoTransientLocking)
                return AcquireCPRLatchUpsert(phase, ref stackCtx, ref status, ref latchOperation);

            // This is AcquireCPRLatchUpsert without the bucket latching, since we already have a latch on either the bucket or the recordInfo.
            // See additional comments in AcquireCPRLatchRMW.

            switch (phase)
            {
                case Phase.PREPARE: // Thread is in V
                    if (!IsEntryVersionNew(ref stackCtx.hei.entry))
                        break; // Normal Processing; thread is in V, record is in V

                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    return LatchDestination.Retry;  // Pivot Thread for retry (do not operate on V+1 record when thread is in V)

                case Phase.IN_PROGRESS: // Thread is in V+1
                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_FLUSH:
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1
                    return LatchDestination.CreateNewRecord;    // Upsert never goes pending; always force creation of a (V+1) record

                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        private LatchDestination AcquireCPRLatchUpsert(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // See additional comments in AcquireCPRLatchRMW.

            switch (phase)
            {
                case Phase.PREPARE: // Thread is in V
                    if (HashBucket.TryAcquireSharedLatch(ref stackCtx.hei))
                    {
                        // Set to release shared latch (default)
                        latchOperation = LatchOperation.Shared;
                        if (IsEntryVersionNew(ref stackCtx.hei.entry))
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            return LatchDestination.Retry;  // Pivot Thread for retry (do not operate on V+1 record when thread is in V)
                        }
                        break; // Normal Processing; thread is in V, record is in V
                    }

                    // Could not acquire Shared latch; system must be in V+1 (or we have too many shared latches).
                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    return LatchDestination.Retry;  // Pivot Thread for retry

                case Phase.IN_PROGRESS: // Thread is in V+1
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1

                    if (HashBucket.TryAcquireExclusiveLatch(ref stackCtx.hei))
                    {
                        // Set to release exclusive latch (default)
                        latchOperation = LatchOperation.Exclusive;
                        return LatchDestination.CreateNewRecord; // Upsert never goes pending; always force creation of a (v+1) record
                    }

                    // Could not acquire exclusive latch; likely a conflict on the bucket.
                    status = OperationStatus.RETRY_LATER;
                    return LatchDestination.Retry;  // Retry after refresh

                case Phase.WAIT_INDEX_CHECKPOINT:   // Thread is in v+1
                case Phase.WAIT_FLUSH:
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1
                    return LatchDestination.CreateNewRecord;    // Upsert never goes pending; always force creation of a (V+1) record

                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        /// <summary>
        /// Create a new record for Upsert
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="input">Input to the operation</param>
        /// <param name="value">The value to insert</param>
        /// <param name="output">The result of IFunctions.SingleWriter</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="fasterSession">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/></param>
        private OperationStatus CreateNewRecordUpsert<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Value value, ref Output output,
                                                                                             ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                                                                             ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var (actualSize, allocatedSize) = hlog.GetRecordSize(ref key, ref input, ref value, fasterSession);
            if (!TryAllocateRecord(ref pendingContext, ref stackCtx, ref allocatedSize, recycle: true, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;

            ref RecordInfo newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: fasterSession.Ctx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.SetNewRecord(newLogicalAddress);

            UpsertInfo upsertInfo = new()
            {
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
                RecordInfo = newRecordInfo
            };

            ref Value newRecordValue = ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (upsertInfo.UsedValueLength, upsertInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            if (!fasterSession.SingleWriter(ref key, ref input, ref value, ref newRecordValue, ref output, ref newRecordInfo, ref upsertInfo, WriteReason.Upsert))
            {
                // TODO save allocation for reuse (not retry, because these aren't retry status codes); check other InternalXxx as well
                if (upsertInfo.Action == UpsertAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            SetLiveFullValueLength(newPhysicalAddress, ref newRecordValue, ref newRecordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            upsertInfo.RecordInfo = newRecordInfo;
            bool success = CASRecordIntoChain(ref key, ref stackCtx, newLogicalAddress, ref newRecordInfo);
            if (success)
            {
                PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo);

                fasterSession.PostSingleWriter(ref key, ref input, ref value, ref newRecordValue, ref output, ref newRecordInfo, ref upsertInfo, WriteReason.Upsert);
                if (stackCtx.recSrc.ephemeralLockResult == EphemeralLockResult.HoldForSeal)
                    srcRecordInfo.UnlockExclusiveAndSeal();
                stackCtx.ClearNewRecord();
                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            upsertInfo.RecordInfo = newRecordInfo;
            ref Value insertedValue = ref hlog.GetValue(newPhysicalAddress);
            ref Key insertedKey = ref hlog.GetKey(newPhysicalAddress);
            fasterSession.DisposeSingleWriter(ref insertedKey, ref input, ref value, ref insertedValue, ref output, ref newRecordInfo, ref upsertInfo, WriteReason.Upsert);
            insertedKey = default;
            insertedValue = default;

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}
