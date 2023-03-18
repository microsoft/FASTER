// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
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
            OperationStatus status = default;
            var latchOperation = LatchOperation.None;
            var latchDestination = LatchDestination.NormalProcessing;

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(stackCtx.hei.hash, sessionCtx, fasterSession);

            // A 'ref' variable must be initialized. If we find a record for the key, we reassign the reference. We don't copy from this source, but we do lock it.
            RecordInfo dummyRecordInfo = default;
            ref RecordInfo srcRecordInfo = ref dummyRecordInfo;

            FindOrCreateTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            // We must always scan to HeadAddress; a Lockable*Context could be activated and lock the record in the immutable region while we're scanning.
            TryFindRecordInMemory(ref key, ref stackCtx, hlog.HeadAddress);

            UpsertInfo upsertInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = sessionCtx.version,
                SessionID = sessionCtx.sessionID,
                Address = stackCtx.recSrc.LogicalAddress,
                KeyHash = stackCtx.hei.hash
            };

            #region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                latchDestination = AcquireLatchUpsert(sessionCtx, ref stackCtx.hei, ref status, ref latchOperation, stackCtx.recSrc.LogicalAddress);
                if (latchDestination == LatchDestination.Retry)
                    goto LatchRelease;
            }
            #endregion

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
            #region Address and source record checks

                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    // Use the readcache record as the CopyUpdater source.
                    goto LockSourceRecord;
                }
                else if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress && latchDestination == LatchDestination.NormalProcessing)
                {
                    // Mutable Region: Update the record in-place
                    // We perform mutable updates only if we are in normal processing phase of checkpointing
                    srcRecordInfo = ref hlog.GetInfo(stackCtx.recSrc.PhysicalAddress);
                    if (!TryEphemeralXLock<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx.recSrc, ref srcRecordInfo, out status))
                        goto LatchRelease;

                    if (srcRecordInfo.Tombstone)
                        goto CreateNewRecord;

                    if (!srcRecordInfo.IsValidUpdateOrLockSource)
                    {
                        EphemeralXUnlockAndAbandonUpdate<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx.recSrc, ref srcRecordInfo);
                        srcRecordInfo = ref dummyRecordInfo;
                        goto CreateNewRecord;
                    }

                    upsertInfo.RecordInfo = srcRecordInfo;
                    ref Value recordValue = ref hlog.GetValue(stackCtx.recSrc.PhysicalAddress);
                    if (fasterSession.ConcurrentWriter(ref key, ref input, ref value, ref recordValue, ref output, ref srcRecordInfo, ref upsertInfo))
                    {
                        this.MarkPage(stackCtx.recSrc.LogicalAddress, sessionCtx);
                        pendingContext.recordInfo = srcRecordInfo;
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                        goto LatchRelease;
                    }
                    if (upsertInfo.Action == UpsertAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        goto LatchRelease;
                    }

                    // ConcurrentWriter failed (e.g. insufficient space, another thread set Tombstone, etc). Write a new record, but track that we have to seal and unlock this one.
                    stackCtx.recSrc.HasMainLogSrc = true;
                    goto CreateNewRecord;
                }
                else if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                {
                    // Only need to go below ReadOnly for locking and Sealing.
                    stackCtx.recSrc.HasMainLogSrc = true;
                    goto LockSourceRecord;
                }
                else
                {
                    // Either on-disk or no record exists - check for lock before creating new record. First ensure any record lock has transitioned to the LockTable.
                    SpinWaitUntilRecordIsClosed(ref key, stackCtx.hei.hash, stackCtx.recSrc.LogicalAddress, hlog);
                    Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, stackCtx.hei.hash), "A Lockable-session Upsert() of an on-disk or non-existent key requires a LockTable lock");
                    if (LockTable.IsActive && !fasterSession.DisableEphemeralLocking 
                            && !LockTable.TryLockEphemeral(ref key, stackCtx.hei.hash, LockType.Exclusive, out stackCtx.recSrc.HasLockTableLock))
                    {
                        status = OperationStatus.RETRY_LATER;
                        goto LatchRelease;
                    }
                    goto CreateNewRecord;
                }
            #endregion Address and source record checks

            #region Lock source record
            LockSourceRecord:
                // This would be a local function to reduce "goto", but 'ref' variables and parameters aren't supported on local functions.
                srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                if (!TryEphemeralXLock<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx.recSrc, ref srcRecordInfo, out status))
                    goto LatchRelease;
                if (!srcRecordInfo.IsValidUpdateOrLockSource)
                {
                    EphemeralXUnlockAndAbandonUpdate<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx.recSrc, ref srcRecordInfo);
                    srcRecordInfo = ref dummyRecordInfo;
                }
                goto CreateNewRecord;
            #endregion Lock source record

            #region Create new record in the mutable region
            CreateNewRecord:
                if (latchDestination != LatchDestination.CreatePendingContext)
                {
                    // Immutable region or new record
                    status = CreateNewRecordUpsert(ref key, ref input, ref value, ref output, ref pendingContext, fasterSession,
                                                   sessionCtx, ref stackCtx, ref srcRecordInfo);
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
                stackCtx.HandleNewRecordOnError(this);
                EphemeralXUnlockAfterUpdate<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
            }

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
                pendingContext.PrevLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                pendingContext.version = sessionCtx.version;
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

        private LatchDestination AcquireLatchUpsert<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> sessionCtx, ref HashEntryInfo hei, ref OperationStatus status,
                                                                            ref LatchOperation latchOperation, long logicalAddress)
        {
            switch (sessionCtx.phase)
            {
                case Phase.PREPARE:
                    {
                        if (HashBucket.TryAcquireSharedLatch(ref hei))
                        {
                            // Set to release shared latch (default)
                            latchOperation = LatchOperation.Shared;
                            // Here (and in InternalRead, AcquireLatchRMW, and AcquireLatchDelete) we still check the tail record of the bucket (entry.Address)
                            // rather than the traced record (logicalAddress), because I'm worried that the implementation
                            // may not allow in-place updates for version v when the bucket arrives v+1. 
                            // This is safer but potentially unnecessary.
                            if (CheckBucketVersionNew(ref hei.entry))
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                return LatchDestination.Retry; // Pivot Thread on retry
                            }
                            break; // Normal Processing
                        }
                        else
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            return LatchDestination.Retry; // Pivot Thread on retry
                        }
                    }
                case Phase.IN_PROGRESS:
                    {
                        if (!CheckEntryVersionNew(logicalAddress))
                        {
                            if (HashBucket.TryAcquireExclusiveLatch(ref hei))
                            {
                                // Set to release exclusive latch (default)
                                latchOperation = LatchOperation.Exclusive;
                                return LatchDestination.CreateNewRecord; // Create a (v+1) record
                            }
                            else
                            {
                                status = OperationStatus.RETRY_LATER;
                                return LatchDestination.Retry; // Refresh and retry operation
                            }
                        }
                        break; // Normal Processing
                    }
                case Phase.WAIT_INDEX_CHECKPOINT:
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

        /// <summary>
        /// Create a new record for Upsert
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="input">Input to the operation</param>
        /// <param name="value">The value to insert</param>
        /// <param name="output">The result of IFunctions.SingleWriter</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="fasterSession">The current session</param>
        /// <param name="sessionCtx">The current session context</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/></param>
        private OperationStatus CreateNewRecordUpsert<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Value value, ref Output output,
                                                                                             ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                                                                             FasterExecutionContext<Input, Output, Context> sessionCtx,
                                                                                             ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var (actualSize, allocatedSize) = hlog.GetRecordSize(ref key, ref value);

            if (!GetAllocationForRetry(ref pendingContext, stackCtx.hei.Address, allocatedSize, out long newLogicalAddress, out long newPhysicalAddress))
            {
                // Spin to make sure newLogicalAddress is > recSrc.LatestLogicalAddress (the .PreviousAddress and CAS comparison value).
                do
                {
                    if (!BlockAllocate(allocatedSize, out newLogicalAddress, ref pendingContext, out OperationStatus status))
                        return status;
                    newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                    if (!VerifyInMemoryAddresses(ref stackCtx))
                    {
                        SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
                        return OperationStatus.RETRY_LATER;
                    }
                } while (newLogicalAddress < stackCtx.recSrc.LatestLogicalAddress);
            }

            ref RecordInfo newRecordInfo = ref WriteTentativeInfo(ref key, hlog, newPhysicalAddress, inNewVersion: sessionCtx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.newLogicalAddress = newLogicalAddress;

            UpsertInfo upsertInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = sessionCtx.version,
                SessionID = sessionCtx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
                RecordInfo = newRecordInfo
            };

            ref Value newValue = ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            if (!fasterSession.SingleWriter(ref key, ref input, ref value, ref newValue, ref output, ref newRecordInfo, ref upsertInfo, WriteReason.Upsert))
            {
                // TODO save allocation for reuse (not retry, because these aren't retry status codes); check other InternalXxx as well
                if (upsertInfo.Action == UpsertAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            upsertInfo.RecordInfo = newRecordInfo;
            bool success = CASRecordIntoChain(ref stackCtx, newLogicalAddress);
            if (success)
            {
                if (!CompleteTwoPhaseUpdate<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo, ref newRecordInfo, out var lockStatus))
                    return lockStatus;

                fasterSession.PostSingleWriter(ref key, ref input, ref value, ref newValue, ref output, ref newRecordInfo, ref upsertInfo, WriteReason.Upsert);
                stackCtx.ClearNewRecordTentativeBitAtomic(ref newRecordInfo);
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
            if (WriteDefaultOnDelete)
            {
                insertedKey = default;
                insertedValue = default;
            }

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}
