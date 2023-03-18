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
        internal OperationStatus InternalDelete<Input, Output, Context, FasterSession>(ref Key key, ref Context userContext,
                            ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession, long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var latchOperation = LatchOperation.None;
            var latchDestination = LatchDestination.NormalProcessing;

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (fasterSession.Ctx.phase != Phase.REST)
                HeavyEnter(stackCtx.hei.hash, fasterSession.Ctx, fasterSession);

            var tagExists = FindTag(ref stackCtx.hei);
            if (!tagExists)
            {
                Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, ref stackCtx.hei), "A Lockable-session Delete() of a non-existent key requires a LockTable lock");
                return OperationStatus.NOTFOUND;
            }
            stackCtx.SetRecordSourceToHashEntry(hlog);

            // Always scan to HeadAddress; this lets us find a tombstoned record in the immutable region, avoiding unnecessarily adding one.
            RecordInfo dummyRecordInfo = new() { Valid = true };
            ref RecordInfo srcRecordInfo = ref TryFindRecordInMemory(ref key, ref stackCtx, hlog.HeadAddress)
                ? ref stackCtx.recSrc.GetInfo()
                : ref dummyRecordInfo;
            if (srcRecordInfo.IsClosed)
                return OperationStatus.RETRY_LATER;

            // If we already have a deleted record, there's nothing to do.
            if (srcRecordInfo.Tombstone)
                return OperationStatus.NOTFOUND;

            DeleteInfo deleteInfo = new()
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
                #region Address and source record checks

                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    // Use the readcache record as the CopyUpdater source.
                    goto CreateNewRecord;
                }

                // Check for CPR consistency after checking if source is readcache.
                if (fasterSession.Ctx.phase != Phase.REST)
                {
                    latchDestination = CheckCPRConsistencyDelete(fasterSession.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    if (latchDestination == LatchDestination.Retry)
                        goto LatchRelease;
                }

                if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress && latchDestination == LatchDestination.NormalProcessing)
                {
                    // Mutable Region: Update the record in-place
                    deleteInfo.RecordInfo = srcRecordInfo;
                    ref Value recordValue = ref stackCtx.recSrc.GetValue();
                    if (fasterSession.ConcurrentDeleter(ref stackCtx.recSrc.GetKey(), ref recordValue, ref srcRecordInfo, ref deleteInfo, out bool lockFailed))
                    {
                        this.MarkPage(stackCtx.recSrc.LogicalAddress, fasterSession.Ctx);
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
                    if (lockFailed)
                    {
                        status = OperationStatus.RETRY_LATER;
                        goto LatchRelease;
                    }
                    if (deleteInfo.Action == DeleteAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        goto LatchRelease;
                    }

                    // Could not delete in place for some reason - create new record.
                    goto CreateNewRecord;
                }
                else if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                {
                    goto CreateNewRecord;
                }
                else
                {
                    // Either on-disk or no record exists - create new record.
                    Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, ref stackCtx.hei), "A Lockable-session Delete() of an on-disk or non-existent key requires a LockTable lock");
                    goto CreateNewRecord;
                }
            #endregion Address and source record checks

            #region Create new record in the mutable region
            CreateNewRecord:
                {
                    if (latchDestination != LatchDestination.CreatePendingContext)
                    {
                        // Immutable region or new record
                        status = CreateNewRecordDelete(ref key, ref pendingContext, fasterSession, ref stackCtx, ref srcRecordInfo);
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
                TransientXUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
            }

        #region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.DELETE;
                if (pendingContext.key == default) pendingContext.key = hlog.GetKeyContainer(ref key);
                pendingContext.userContext = userContext;
                pendingContext.entry.word = stackCtx.recSrc.LatestLogicalAddress;
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                pendingContext.version = fasterSession.Ctx.version;
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

        private LatchDestination CheckCPRConsistencyDelete(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // This is the same logic as Upsert; neither goes pending.
            return CheckCPRConsistencyUpsert(phase, ref stackCtx, ref status, ref latchOperation);
        }

        /// <summary>
        /// Create a new tombstoned record for Delete
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="fasterSession">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/></param>
        private OperationStatus CreateNewRecordDelete<Input, Output, Context, FasterSession>(ref Key key, ref PendingContext<Input, Output, Context> pendingContext,
                                                                                             FasterSession fasterSession, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var value = default(Value);
            var (_, allocatedSize) = hlog.GetRecordSize(ref key, ref value);

            if (!TryAllocateRecord(ref pendingContext, ref stackCtx, allocatedSize, recycle: false, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;

            ref RecordInfo newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: fasterSession.Ctx.InNewVersion, tombstone: true, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.SetNewRecord(newLogicalAddress);

            DeleteInfo deleteInfo = new()
            {
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
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
            bool success = CASRecordIntoChain(ref key, ref stackCtx, newLogicalAddress, ref newRecordInfo);
            if (success)
            {
                PostInsertAtTail(ref key, ref stackCtx, ref srcRecordInfo);

                // Note that this is the new logicalAddress; we have not retrieved the old one if it was below HeadAddress, and thus
                // we do not know whether 'logicalAddress' belongs to 'key' or is a collision.
                fasterSession.PostSingleDeleter(ref key, ref newRecordInfo, ref deleteInfo);
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
