// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
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
            var latchOperation = LatchOperation.None;
            var latchDestination = LatchDestination.NormalProcessing;

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(stackCtx.hei.hash, sessionCtx, fasterSession);

            var tagExists = FindTag(ref stackCtx.hei);
            if (!tagExists)
            {
                Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, ref stackCtx.hei), "A Lockable-session Delete() of a non-existent key requires a LockTable lock");
                return OperationStatus.NOTFOUND;
            }
            stackCtx.SetRecordSourceToHashEntry(hlog);

            // Always scan to HeadAddress; this lets us find a tombstoned record in the immutable region, avoiding unnecessarily adding one.
            // We also need to lock RecordInfos in the immutable region if doing EphemeralOnly locking.
            RecordInfo dummyRecordInfo = new() { Valid = true };
            ref RecordInfo srcRecordInfo = ref TryFindRecordInMemory(ref key, ref stackCtx, hlog.HeadAddress)
                ? ref stackCtx.recSrc.GetSrcRecordInfo()
                : ref dummyRecordInfo;
            if (srcRecordInfo.IsClosed)
                return OperationStatus.RETRY_LATER;

            // If we already have a deleted record, there's nothing to do.
            if (srcRecordInfo.Tombstone)
                return OperationStatus.NOTFOUND;

            DeleteInfo deleteInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = sessionCtx.version,
                SessionID = sessionCtx.sessionID,
                Address = stackCtx.recSrc.LogicalAddress,
                KeyHash = stackCtx.hei.hash
            };

            if (!TryEphemeralXLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo, out OperationStatus status))
                return status;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                #region Entry latch operation
                if (sessionCtx.phase != Phase.REST)
                {
                    latchDestination = AcquireLatchDelete(sessionCtx, ref stackCtx.hei, ref status, ref latchOperation, stackCtx.recSrc.LogicalAddress);
                    if (latchDestination == LatchDestination.Retry)
                        goto LatchRelease;
                }
                #endregion Entry latch operation

                #region Address and source record checks

                if (latchDestination == LatchDestination.NormalProcessing)
                {
                    if (stackCtx.recSrc.HasReadCacheSrc)
                    {
                        // Use the readcache record as the CopyUpdater source.
                        goto CreateNewRecord;
                    }

                    else if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress && latchDestination == LatchDestination.NormalProcessing)
                    {
                        // Mutable Region: Update the record in-place
                        deleteInfo.RecordInfo = srcRecordInfo;
                        ref Value recordValue = ref hlog.GetValue(stackCtx.recSrc.PhysicalAddress);
                        if (fasterSession.ConcurrentDeleter(ref hlog.GetKey(stackCtx.recSrc.PhysicalAddress), ref recordValue, ref srcRecordInfo, ref deleteInfo))
                        {
                            this.MarkPage(stackCtx.recSrc.LogicalAddress, sessionCtx);
                            if (WriteDefaultOnDelete)
                                recordValue = default;

                        // Try to update hash chain and completely elide record iff previous address points to invalid address, to avoid re-enabling a prior version of this record.
                        if (stackCtx.hei.Address == stackCtx.recSrc.LogicalAddress && !fasterSession.IsManualLocking && srcRecordInfo.PreviousAddress < hlog.BeginAddress)
                        {
                            // Ignore return value; this is a performance optimization to keep the hash table clean if we can, so if we fail it just means
                            // the hashtable entry has already been updated by someone else.
                            var address = (srcRecordInfo.PreviousAddress == Constants.kTempInvalidAddress) ? Constants.kInvalidAddress : srcRecordInfo.PreviousAddress;
                            stackCtx.hei.TryCAS(address, tag: 0);
                        }

                            status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                            goto LatchRelease;
                        }
                        if (deleteInfo.Action == DeleteAction.CancelOperation)
                        {
                            status = OperationStatus.CANCELED;
                            goto LatchRelease;
                        }
                        goto CreateNewRecord;
                    }
                    else if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                    {
                        goto CreateNewRecord;
                    }
                    else
                    {
                        // Either on-disk or no record exists - check for lock before creating new record. First ensure any record lock has transitioned to the LockTable.
                        Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, ref stackCtx.hei), "A Lockable-session Delete() of an on-disk or non-existent key requires a LockTable lock");
                        goto CreateNewRecord;
                    }
                }

            #endregion Address and source record checks

            #region Create new record in the mutable region
            CreateNewRecord:
                {
                    if (latchDestination != LatchDestination.CreatePendingContext)
                    {
                        // Immutable region or new record
                        status = CreateNewRecordDelete(ref key, ref pendingContext, fasterSession, sessionCtx, ref stackCtx, ref srcRecordInfo);
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
                }
                #endregion
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                EphemeralXUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
            }

        #region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.DELETE;
                if (pendingContext.key == default) pendingContext.key = hlog.GetKeyContainer(ref key);
                pendingContext.userContext = userContext;
                pendingContext.entry.word = stackCtx.recSrc.LatestLogicalAddress;
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
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
                        HashBucket.ReleaseSharedLatch(ref stackCtx.hei);
                        break;
                    case LatchOperation.Exclusive:
                        HashBucket.ReleaseExclusiveLatch(ref stackCtx.hei);
                        break;
                    default:
                        break;
                }
            }
            #endregion

            return status;
        }

        private LatchDestination AcquireLatchDelete<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> sessionCtx, ref HashEntryInfo hei, ref OperationStatus status,
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
                            if (CheckBucketVersionNew(ref hei.entry))
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                return LatchDestination.Retry; // Pivot Thread, retry
                            }
                            break; // Normal Processing
                        }
                        else
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            return LatchDestination.Retry; // Pivot Thread, retry
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
                                return LatchDestination.Retry; // Retry after refresh
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
        /// Create a new tombstoned record for Delete
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="fasterSession">The current session</param>
        /// <param name="sessionCtx">The current session context</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/></param>
        private OperationStatus CreateNewRecordDelete<Input, Output, Context, FasterSession>(ref Key key, ref PendingContext<Input, Output, Context> pendingContext,
                                                                                             FasterSession fasterSession, FasterExecutionContext<Input, Output, Context> sessionCtx,
                                                                                             ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var value = default(Value);
            var (_, allocatedSize) = hlog.GetRecordSize(ref key, ref value);

            if (!TryAllocateRecord(ref pendingContext, ref stackCtx, allocatedSize, recycle: false, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;

            ref RecordInfo newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: sessionCtx.InNewVersion, tombstone: true, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.SetNewRecord(newLogicalAddress);

            DeleteInfo deleteInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = sessionCtx.version,
                SessionID = sessionCtx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
                RecordInfo = newRecordInfo
            };

            if (!fasterSession.SingleDeleter(ref key, ref hlog.GetValue(newPhysicalAddress), ref newRecordInfo, ref deleteInfo))
            {
                // Don't save allocation because we did not allocate a full Value.
                if (deleteInfo.Action == DeleteAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            deleteInfo.RecordInfo = newRecordInfo;
            newRecordInfo.MarkTentative();
            bool success = CASRecordIntoChain(ref stackCtx, newLogicalAddress);
            if (success)
            {
                CompleteUpdate<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo, ref newRecordInfo);

                // Note that this is the new logicalAddress; we have not retrieved the old one if it was below HeadAddress, and thus
                // we do not know whether 'logicalAddress' belongs to 'key' or is a collision.
                fasterSession.PostSingleDeleter(ref key, ref newRecordInfo, ref deleteInfo);
                newRecordInfo.ClearTentative();
                stackCtx.ClearNewRecord();
                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            deleteInfo.RecordInfo = newRecordInfo;
            ref Value insertedValue = ref hlog.GetValue(newPhysicalAddress);
            ref Key insertedKey = ref hlog.GetKey(newPhysicalAddress);
            fasterSession.DisposeSingleDeleter(ref insertedKey, ref insertedValue, ref newRecordInfo, ref deleteInfo);
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
